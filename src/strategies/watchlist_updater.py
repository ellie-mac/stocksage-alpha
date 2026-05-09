#!/usr/bin/env python3
"""
自选池自动更新 — 每晚 gc_scan 后运行（约 19:30+）

三路来源：
  A 主策略  data/latest_picks.json         top 15 by buy_score
  B 金叉    data/golden_cross_latest.json   G0/G1/G2 全部
  C 热榜    data/hot_scan_latest.json       top 10 by score

淘汰规则（每次运行时检查当前动态池）：
  - TTL 到期（A/B: 14 日, C: 7 日）
  - 入池后浮亏 ≥ 8%
  - 价格跌破 MA20（含 1% 缓冲）

手动池（alert_config.json → watchlist）永久有效，不纳入动态池，也不重复。
动态池保存在 data/watchlist_dynamic.json。

用法：
    python -X utf8 src/strategies/watchlist_updater.py
    python -X utf8 src/strategies/watchlist_updater.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

DATA              = ROOT / "data"
DYNAMIC_WL_PATH   = DATA / "watchlist_dynamic.json"
LATEST_PICKS_PATH = DATA / "latest_picks.json"
GC_LATEST_PATH    = DATA / "golden_cross_latest.json"
HOT_LATEST_PATH   = DATA / "hot_scan_latest.json"

TTL_DAYS  = {"main_scan": 14, "gc_scan": 14, "hot_scan": 7}
LIMIT_A   = 15
LIMIT_C   = 10
STOP_LOSS = -8.0


def _clean(code: str) -> str:
    return code[-6:] if len(code) > 6 else code


def _today() -> str:
    return date.today().strftime("%Y%m%d")


def _age(date_str: str) -> int:
    try:
        return (date.today() - datetime.strptime(date_str, "%Y%m%d").date()).days
    except Exception:
        return 999


def _load_dynamic() -> list[dict]:
    if not DYNAMIC_WL_PATH.exists():
        return []
    try:
        return json.loads(DYNAMIC_WL_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_dynamic(entries: list[dict]) -> None:
    DATA.mkdir(exist_ok=True)
    tmp = str(DYNAMIC_WL_PATH) + ".tmp"
    Path(tmp).write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, str(DYNAMIC_WL_PATH))


def _evict_reason(entry: dict) -> str | None:
    """Returns eviction reason or None to keep."""
    # TTL
    age = _age(entry.get("added_date", "20200101"))
    ttl = TTL_DAYS.get(entry.get("source", "main_scan"), 14)
    if age >= ttl:
        return f"TTL到期({age}日)"

    # Price checks
    try:
        import fetcher
        df = fetcher.get_price_history(entry["code"], days=30)
        if df is None or df.empty:
            return None
        price = float(df.iloc[-1]["close"])

        # Stop loss
        ep = entry.get("entry_price") or 0
        if ep > 0:
            pnl = (price - ep) / ep * 100
            if pnl <= STOP_LOSS:
                return f"止损({pnl:+.1f}%)"

        # MA20 break
        if len(df) >= 20:
            ma20 = float(df["close"].tail(20).mean())
            if price < ma20 * 0.99:
                return f"跌破MA20({price:.2f}<{ma20:.2f})"
    except Exception:
        pass

    return None


def _candidates_A() -> list[dict]:
    if not LATEST_PICKS_PATH.exists():
        return []
    try:
        picks = json.loads(LATEST_PICKS_PATH.read_text(encoding="utf-8")).get("results", [])
        picks.sort(key=lambda x: -x.get("buy_score", 0))
        return [
            {"code": _clean(p["code"]), "name": p.get("name", p["code"]),
             "source": "main_scan", "entry_price": p.get("price") or 0}
            for p in picks[:LIMIT_A]
        ]
    except Exception as e:
        print(f"[updater] A-主策略读取失败: {e}")
        return []


def _candidates_B() -> list[dict]:
    if not GC_LATEST_PATH.exists():
        return []
    try:
        tiers = json.loads(GC_LATEST_PATH.read_text(encoding="utf-8")).get("tiers", {})
        seen, result = set(), []
        for picks in tiers.values():
            for p in picks:
                code = _clean(p.get("code", ""))
                if not code or code in seen:
                    continue
                seen.add(code)
                result.append({"code": code, "name": p.get("name", code),
                                "source": "gc_scan", "entry_price": p.get("close", 0)})
        return result
    except Exception as e:
        print(f"[updater] B-金叉读取失败: {e}")
        return []


def _candidates_C() -> list[dict]:
    if not HOT_LATEST_PATH.exists():
        return []
    try:
        picks = json.loads(HOT_LATEST_PATH.read_text(encoding="utf-8")).get("picks", [])
        picks.sort(key=lambda x: -x.get("score", 0))
        return [
            {"code": _clean(p["code"]), "name": p.get("name", p["code"]),
             "source": "hot_scan", "entry_price": p.get("close", 0)}
            for p in picks[:LIMIT_C]
        ]
    except Exception as e:
        print(f"[updater] C-热榜读取失败: {e}")
        return []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg_path = ROOT / "alert_config.json"
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}

    # Manual codes — never touch these
    raw_manual = cfg.get("watchlist", [])
    manual_codes = {_clean(e["code"] if isinstance(e, dict) else e) for e in raw_manual}

    today    = _today()
    current  = _load_dynamic()

    # ── Evict ─────────────────────────────────────────────────────────────────
    kept, evicted = [], []
    for entry in current:
        reason = _evict_reason(entry)
        if reason:
            evicted.append((entry, reason))
        else:
            kept.append(entry)

    if evicted:
        print(f"[updater] 淘汰 {len(evicted)} 只:")
        for e, r in evicted:
            print(f"  - {e.get('name', e['code'])}({e['code']}) [{e.get('source','')}] {r}")

    # ── Add new candidates ────────────────────────────────────────────────────
    kept_codes = {e["code"] for e in kept}
    added: list[dict] = []
    for candidate in _candidates_A() + _candidates_B() + _candidates_C():
        code = candidate["code"]
        if code in kept_codes or code in manual_codes or code in {c["code"] for c in added}:
            continue
        candidate["added_date"] = today
        added.append(candidate)
        kept_codes.add(code)

    if added:
        src_label = {"main_scan": "主策略", "gc_scan": "金叉", "hot_scan": "热榜"}
        print(f"[updater] 新增 {len(added)} 只:")
        for e in added:
            print(f"  + {e.get('name', e['code'])}({e['code']}) [{src_label.get(e['source'], e['source'])}]")

    new_list = kept + added
    print(f"[updater] 动态池: {len(current)} → {len(new_list)} 只  (淘汰{len(evicted)} 新增{len(added)})")

    if args.dry_run:
        print("[updater] dry-run，不保存")
        return

    _save_dynamic(new_list)
    print("[updater] 已保存 → watchlist_dynamic.json")

    if evicted or added:
        try:
            from notify.notify import push_feishu_card
            src_label = {"main_scan": "主策略", "gc_scan": "金叉", "hot_scan": "热榜"}
            lines = [f"动态自选池  {today}  共{len(new_list)}只"]
            if added:
                lines.append(f"\n新增 {len(added)} 只")
                for e in added:
                    lines.append(f"  + {e.get('name', e['code'])}({e['code']})  [{src_label.get(e['source'], e['source'])}]")
            if evicted:
                lines.append(f"\n移除 {len(evicted)} 只")
                for e, r in evicted:
                    lines.append(f"  - {e.get('name', e['code'])}({e['code']})  {r}")
            push_feishu_card(f"自选池更新 +{len(added)} -{len(evicted)}", lines)
        except Exception as e:
            print(f"[updater] 飞书推送失败: {e}")


if __name__ == "__main__":
    main()
