#!/usr/bin/env python3
"""
多策略晨报 — 合并主策略/小盘/金叉/筹码/低市值/热榜/横盘七路信号，统一推送。

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


def _norm_code(code: str) -> str:
    """Strip sh/sz exchange prefix; keep last 6 digits.

    gc_scan emits 'sh603486' / 'sz301377' while chip/hot/marketcap use pure 6-digit
    codes. Without normalization, set intersections silently return 0.
    """
    if not code:
        return code
    s = str(code)
    return s.lower().lstrip("shz")[-6:] if any(c.isalpha() for c in s) else s


# 科技 (TMT) 行业关键词 — 与 src/strategies/sideways_scan.py 保持同步
_TECH_KEYWORDS = (
    "半导体", "集成电路", "芯片",
    "软件", "计算机", "互联网", "信息",
    "通信",
    "元器件", "电子", "光电",
    "网络", "数据", "云", "操作系统",
    "智能", "人工智", "IT",
)


def _is_tech(industry: str) -> bool:
    if not industry:
        return False
    return any(kw in industry for kw in _TECH_KEYWORDS)


def _load_industry_map() -> dict[str, str]:
    """从 stock_names.json 读取 {6位code: industry}。"""
    p = DATA / "stock_names.json"
    if not p.exists():
        print(f"[morning_push] stock_names.json 不存在，无法过滤科技行业")
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return {ts.split(".")[0]: (info.get("industry", "") if isinstance(info, dict) else "")
                for ts, info in raw.items()}
    except Exception as e:
        print(f"[morning_push] 读取 stock_names.json 失败: {e}")
        return {}


def _filter_tech(picks: list[dict], ind_map: dict[str, str]) -> list[dict]:
    """只保留 industry 在 _TECH_KEYWORDS 中匹配的 picks。"""
    if not ind_map:
        return picks
    return [p for p in picks if _is_tech(ind_map.get(p["code"], ""))]


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
        return [{"code": _norm_code(p["code"]), "name": p.get("name", ""), "score": p.get("score", 0)}
                for p in lst if p.get("code")]

    return _extract(d.get("results", [])), _extract(d.get("smallcap", []))


def _load_gc() -> list[dict]:
    """Returns [{code, name, score, tier, close}, ...] for all tiers in tiers dict.
    Codes are normalized to 6-digit form (gc_scan emits sh/sz prefixes)."""
    d = _load(DATA / "golden_cross_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[morning_push] golden_cross_latest.json 数据已过期，跳过金叉策略")
        return []
    picks = []
    for tier, tier_picks in d.get("tiers", {}).items():
        for p in tier_picks:
            raw = p.get("code")
            if raw:
                picks.append({"code": _norm_code(raw), "name": p.get("name", ""),
                               "score": p.get("gc_score", 0), "tier": tier,
                               "close": p.get("close")})
    return picks


def _load_chip() -> list[dict]:
    """Returns [{code, name, score, tier, close}, ...] for all tiers in all_picks."""
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


def _load_hot() -> list[dict]:
    """Returns [{code, name, score, rank, close}, ...] from hot_scan_latest.json (top 30 by score)."""
    d = _load(DATA / "hot_scan_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[morning_push] hot_scan_latest.json 数据已过期，跳过热榜策略")
        return []
    picks = []
    for p in d.get("picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": p.get("score", 0), "rank": p.get("rank"),
                          "close": p.get("close")})
    return picks


def _load_sideways() -> list[dict]:
    """Returns [{code, name, score, tier, close, range_pct, avg_amt_5d_yi}, ...]
    from sideways_latest.json (8 tiers HX0~HS3, 0-based)."""
    d = _load(DATA / "sideways_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[morning_push] sideways_latest.json 数据已过期，跳过横盘策略")
        return []
    picks = []
    for p in d.get("all_picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": -float(p.get("range_pct", 100.0)),
                          "tier": p.get("tier", ""),
                          "close": p.get("close"),
                          "range_pct": p.get("range_pct"),
                          "avg_amt_5d_yi": p.get("avg_amt_5d_yi")})
    return picks


def _load_marketcap() -> list[dict]:
    """Returns [{code, name, price, marketcap_yi}, ...] from marketcap_latest.json."""
    d = _load(DATA / "marketcap_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[morning_push] marketcap_latest.json 数据已过期，跳过市值策略")
        return []
    return [
        {"code": p["code"], "name": p.get("name", ""), "score": 0,
         "price": p.get("price"), "marketcap_yi": p.get("marketcap_yi")}
        for p in d.get("picks", []) if p.get("code")
    ]


# ── Merge & tag ───────────────────────────────────────────────────────────────

def _merge(
    main: list[dict],
    small: list[dict],
    gc: list[dict],
    chip: list[dict],
    marketcap: list[dict],
    hot: list[dict],
    sideways: list[dict],
) -> dict[str, dict]:
    """
    Returns registry: {code: {name, tags: list[str], details: {tag: pick_dict}}}.
    Tag order: 主 小 叉 筹 市 热 横
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
    _add("市", marketcap)
    _add("热", hot)
    _add("横", sideways)

    return registry


# ── Format ────────────────────────────────────────────────────────────────────

def _tag_str(tags: list[str]) -> str:
    return "`" + ",".join(tags) + "`"


def _fmt_pick(code: str, entry: dict) -> str:
    name = entry["name"]
    tags = _tag_str(entry["tags"])
    details = entry["details"]

    # Price: prefer 市 then 叉 then 筹 then 热 then 横
    price = None
    if "市" in details and details["市"].get("price"):
        price = float(details["市"]["price"])
    else:
        for tag in ("叉", "筹", "热", "横"):
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
    if "市" in details:
        mv = details["市"].get("marketcap_yi")
        if mv:
            annotations.append(f"市值{mv:.1f}亿")
    if "热" in details:
        r = details["热"].get("rank")
        if r:
            annotations.append(f"热#{r}")
    if "横" in details:
        t = details["横"].get("tier", "")
        rp = details["横"].get("range_pct")
        amt = details["横"].get("avg_amt_5d_yi")
        if t and rp is not None:
            amt_s = f"/{amt:.1f}亿" if amt else ""
            annotations.append(f"横{t}({rp:.1f}%{amt_s})")

    ann_str = "  " + "  ".join(annotations) if annotations else ""
    return f"**{code} {name}** {tags}{price_str}{ann_str}"


MAX_SINGLE = 20  # per-strategy section cap (single-strategy stocks only)

_TIER_ORDER = {
    "G0": 0, "G1": 1, "G2": 2,
    "C0": 0, "C1": 1, "C2": 2,
    "HX0": 0, "HS0": 1, "HX1": 2, "HS1": 3, "HX2": 4, "HS2": 5, "HX3": 6, "HS3": 7,
}


def _build_message(registry: dict[str, dict], tech_only: bool = True) -> tuple[str, str]:
    label = "多策略晨报（科技）" if tech_only else "多策略晨报"
    if not registry:
        return label, "今日七路策略均无信号"

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"{label} | {now_str}"

    # Sort by tag count desc, then by tag combination alphabetically
    sorted_codes = sorted(
        registry.keys(),
        key=lambda c: (-len(registry[c]["tags"]), ",".join(registry[c]["tags"]), c),
    )

    # Group: multi-strategy (≥2 tags) vs single-strategy
    multi = [c for c in sorted_codes if len(registry[c]["tags"]) >= 2]
    single = [c for c in sorted_codes if len(registry[c]["tags"]) == 1]

    parts: list[str] = [f"*{now_str}*"]

    # ── Multi-strategy resonance block (no truncation) ────────────────────────
    if multi:
        parts.append(f"**【多策略共振】{len(multi)}只**")
        for code in multi:
            parts.append(_fmt_pick(code, registry[code]))

    # ── Per-strategy sections (capped at MAX_SINGLE each) ────────────────────
    tag_order = ["主", "小", "叉", "筹", "市", "热", "横"]
    tag_label = {"主": "主策略", "小": "小盘策略", "叉": "金叉", "筹": "筹码",
                 "市": "低市值", "热": "热榜", "横": "横盘"}

    for tag in tag_order:
        tag_codes = [c for c in single if registry[c]["tags"] == [tag]]
        if not tag_codes:
            in_multi_with_tag = [c for c in multi if tag in registry[c]["tags"]]
            if not in_multi_with_tag:
                continue
            else:
                continue  # already shown in multi block
        if tag in ("叉", "筹", "横"):
            tag_codes.sort(key=lambda c, _t=tag: (
                _TIER_ORDER.get(registry[c]["details"][_t].get("tier", ""), 99),
                -registry[c]["details"][_t].get("score", 0),
            ))
        elif tag in ("主", "小"):
            tag_codes.sort(key=lambda c, _t=tag: -registry[c]["details"][_t].get("score", 0))
        elif tag == "市":
            tag_codes.sort(key=lambda c: registry[c]["details"]["市"].get("marketcap_yi") or float("inf"))
        elif tag == "热":
            tag_codes.sort(key=lambda c: registry[c]["details"]["热"].get("rank") or 9999)
        shown = tag_codes[:MAX_SINGLE]
        omitted = len(tag_codes) - len(shown)
        parts.append(f"<br>**【{tag_label[tag]}】{len(tag_codes)}只**")
        for code in shown:
            parts.append(_fmt_pick(code, registry[code]))
        if omitted:
            parts.append(f"_...还有{omitted}只_")

    parts.append("<br>> 仅供参考，不构成投资建议")
    body = "<br>".join(parts)

    return title, body


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push",    action="store_true", help="推送微信")
    parser.add_argument("--dry-run", action="store_true", help="打印不推送")
    parser.add_argument("--no-tech-filter", action="store_true",
                        help="关闭科技行业过滤（默认仅展示科技/TMT 票）")
    args = parser.parse_args()

    main_picks, small_picks = _load_main()
    gc_picks       = _load_gc()
    chip_picks     = _load_chip()
    mc_picks       = _load_marketcap()
    hot_picks      = _load_hot()
    sideways_picks = _load_sideways()

    if not args.no_tech_filter:
        ind_map = _load_industry_map()
        if ind_map:
            before = (len(main_picks), len(small_picks), len(gc_picks),
                      len(chip_picks), len(mc_picks), len(hot_picks), len(sideways_picks))
            main_picks     = _filter_tech(main_picks, ind_map)
            small_picks    = _filter_tech(small_picks, ind_map)
            gc_picks       = _filter_tech(gc_picks, ind_map)
            chip_picks     = _filter_tech(chip_picks, ind_map)
            mc_picks       = _filter_tech(mc_picks, ind_map)
            hot_picks      = _filter_tech(hot_picks, ind_map)
            sideways_picks = _filter_tech(sideways_picks, ind_map)
            after = (len(main_picks), len(small_picks), len(gc_picks),
                     len(chip_picks), len(mc_picks), len(hot_picks), len(sideways_picks))
            print(f"[morning_push] 科技过滤: 主{before[0]}→{after[0]} 小{before[1]}→{after[1]} "
                  f"叉{before[2]}→{after[2]} 筹{before[3]}→{after[3]} "
                  f"市{before[4]}→{after[4]} 热{before[5]}→{after[5]} 横{before[6]}→{after[6]}")

    total = (len(main_picks) + len(small_picks) + len(gc_picks) + len(chip_picks)
             + len(mc_picks) + len(hot_picks) + len(sideways_picks))
    print(f"[morning_push] 主{len(main_picks)} 小{len(small_picks)} "
          f"叉{len(gc_picks)} 筹{len(chip_picks)} 市{len(mc_picks)} "
          f"热{len(hot_picks)} 横{len(sideways_picks)}  共{total}只信号")

    if total == 0:
        print("[morning_push] 无信号，退出")
        sys.exit(0)

    registry = _merge(main_picks, small_picks, gc_picks, chip_picks,
                       mc_picks, hot_picks, sideways_picks)
    unique = len(registry)
    multi_count = sum(1 for e in registry.values() if len(e["tags"]) >= 2)
    print(f"[morning_push] 去重后 {unique} 只（多策略共振 {multi_count} 只）")

    title, body = _build_message(registry, tech_only=not args.no_tech_filter)

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
