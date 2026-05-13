#!/usr/bin/env python3
"""
多策略晨报 — 合并主策略/小盘/金叉/筹码四路信号，统一推送。

股票按覆盖策略数从多到少排列；同策略内按原始得分排序。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

from common import push_wechat

DATA = ROOT / "data"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[morning_push] 读取 {path.name} 失败: {e}")
        return None


def _is_fresh(date_str: str, fmt: str = "%Y%m%d", max_days: int = 1) -> bool:
    """昨天或今天的数据都算新鲜。"""
    if not date_str:
        return False
    try:
        d = datetime.strptime(date_str[:10].replace("-", "")[:8], "%Y%m%d")
        return (datetime.now() - d).days <= max_days
    except Exception:
        return False


# ── Data loaders ──────────────────────────────────────────────────────────────

def _load_main() -> tuple[list[dict], list[dict]]:
    """Returns (主策略picks, 小盘picks); each item: {code, name, score}."""
    d = _load(DATA / "latest_picks.json")
    if not d:
        return [], []
    ts = d.get("timestamp", "")
    if ts and not _is_fresh(ts[:10].replace("-", ""), "%Y%m%d"):
        print("[morning_push] latest_picks.json 数据已过期，跳过主/小盘策略")
        return [], []

    def _extract(lst: list) -> list[dict]:
        return [{"code": p["code"], "name": p.get("name", ""), "score": p.get("score", 0)}
                for p in lst if p.get("code")]

    return _extract(d.get("results", [])), _extract(d.get("smallcap", []))


def _load_gc() -> list[dict]:
    """Returns [{code, name, score, tier, close}, ...] for G0/G1/G2."""
    d = _load(DATA / "golden_cross_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[morning_push] golden_cross_latest.json 数据已过期，跳过金叉策略")
        return []
    picks = []
    for tier in ("G0", "G1", "G2"):
        for p in d.get("tiers", {}).get(tier, []):
            if p.get("code"):
                picks.append({"code": p["code"], "name": p.get("name", ""),
                               "score": p.get("gc_score", 0), "tier": tier,
                               "close": p.get("close")})
    return picks


def _load_chip() -> list[dict]:
    """Returns [{code, name, score, tier, close}, ...] from chip_scan_latest.json."""
    d = _load(DATA / "chip_scan_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[morning_push] chip_scan_latest.json 数据已过期，跳过筹码策略")
        return []
    picks = []
    for p in d.get("all_picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": p.get("winner_rate", 0), "tier": p.get("tier", ""),
                          "close": p.get("close")})
    return picks


# ── Merge & tag ───────────────────────────────────────────────────────────────

def _merge(
    main: list[dict],
    small: list[dict],
    gc: list[dict],
    chip: list[dict],
) -> dict[str, dict]:
    """
    Returns registry: {code: {name, tags: list[str], details: {tag: pick_dict}}}.
    Tag order: 主 小 叉 筹
    """
    registry: dict[str, dict] = {}

    def _add(tag: str, picks: list[dict]) -> None:
        for p in picks:
            code = p["code"]
            if code not in registry:
                registry[code] = {"name": p.get("name", ""), "tags": [], "details": {}}
            if tag not in registry[code]["tags"]:
                registry[code]["tags"].append(tag)
            registry[code]["details"][tag] = p

    _add("主", main)
    _add("小", small)
    _add("叉", gc)
    _add("筹", chip)

    return registry


# ── Format ────────────────────────────────────────────────────────────────────

def _tag_str(tags: list[str]) -> str:
    return "`" + ",".join(tags) + "`"


def _fmt_pick(code: str, entry: dict) -> str:
    name = entry["name"]
    tags = _tag_str(entry["tags"])
    details = entry["details"]

    # Price: prefer 叉 then 筹 (both carry close field)
    price = None
    for tag in ("叉", "筹"):
        close = details.get(tag, {}).get("close")
        if close:
            price = float(close)
            break

    price_str = f"  ¥{price:.2f}" if price else ""

    # Score annotations
    annotations = []
    if "主" in details:
        s = details["主"].get("score", 0)
        if s:
            annotations.append(f"主{s:.0f}")
    if "小" in details:
        s = details["小"].get("score", 0)
        if s:
            annotations.append(f"小{s:.0f}")
    if "叉" in details:
        t = details["叉"].get("tier", "")
        s = details["叉"].get("score", 0)
        annotations.append(f"叉{t}({s}叉)")
    if "筹" in details:
        t = details["筹"].get("tier", "")
        s = details["筹"].get("score", 0)
        annotations.append(f"筹{t}({s:.0f}%)")

    ann_str = "  " + "  ".join(annotations) if annotations else ""
    return f"**{code} {name}** {tags}{price_str}{ann_str}"


def _build_message(registry: dict[str, dict]) -> tuple[str, str]:
    if not registry:
        return "多策略晨报", "今日三路策略均无信号"

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"多策略晨报 | {now_str}"

    # Sort by tag count desc, then by tag combination alphabetically
    sorted_codes = sorted(
        registry.keys(),
        key=lambda c: (-len(registry[c]["tags"]), ",".join(registry[c]["tags"]), c),
    )

    # Group: multi-strategy (≥2 tags) vs single-strategy
    multi = [c for c in sorted_codes if len(registry[c]["tags"]) >= 2]
    single = [c for c in sorted_codes if len(registry[c]["tags"]) == 1]

    parts: list[str] = [f"*{now_str}*"]

    # ── Multi-strategy resonance block ────────────────────────────────────────
    if multi:
        parts.append(f"**【多策略共振】{len(multi)}只**")
        for code in multi:
            parts.append(_fmt_pick(code, registry[code]))

    # ── Per-strategy sections ─────────────────────────────────────────────────
    tag_order = ["主", "小", "叉", "筹"]
    tag_label = {"主": "主策略", "小": "小盘策略", "叉": "金叉", "筹": "筹码"}

    for tag in tag_order:
        tag_codes = [c for c in single if registry[c]["tags"] == [tag]]
        # Also include multi codes that aren't already covered above in per-section
        # (we show multi only in the top block, not repeated below)
        if not tag_codes:
            # Check if any multi codes belong to this tag
            in_multi_with_tag = [c for c in multi if tag in registry[c]["tags"]]
            if not in_multi_with_tag:
                continue
            else:
                continue  # already shown in multi block
        parts.append(f"<br>**【{tag_label[tag]}】{len(tag_codes)}只**")
        for code in tag_codes:
            parts.append(_fmt_pick(code, registry[code]))

    parts.append("<br>> 仅供参考，不构成投资建议")
    body = "<br>".join(parts)

    return title, body


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push",    action="store_true", help="推送微信")
    parser.add_argument("--dry-run", action="store_true", help="打印不推送")
    args = parser.parse_args()

    main_picks, small_picks = _load_main()
    gc_picks   = _load_gc()
    chip_picks = _load_chip()

    total = len(main_picks) + len(small_picks) + len(gc_picks) + len(chip_picks)
    print(f"[morning_push] 主{len(main_picks)} 小{len(small_picks)} "
          f"叉{len(gc_picks)} 筹{len(chip_picks)}  共{total}只信号")

    if total == 0:
        print("[morning_push] 无信号，退出")
        sys.exit(0)

    registry = _merge(main_picks, small_picks, gc_picks, chip_picks)
    unique = len(registry)
    multi_count = sum(1 for e in registry.values() if len(e["tags"]) >= 2)
    print(f"[morning_push] 去重后 {unique} 只（多策略共振 {multi_count} 只）")

    title, body = _build_message(registry)

    print(f"\n{'='*40}")
    print(f"{title}")
    print(body.replace("<br>", "\n"))
    print(f"{'='*40}\n")

    if args.dry_run:
        return

    if args.push:
        try:
            push_wechat(title, body)
            print("[morning_push] 微信推送完成")
        except Exception as e:
            print(f"[morning_push] 推送失败: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
