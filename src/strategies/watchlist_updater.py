#!/usr/bin/env python3
"""
自选池自动更新 — 每晚 gc_scan 后运行（约 19:30+）

五路来源：
  A 主策略  data/latest_picks.json          top 15 by buy_score
  B 金叉    data/golden_cross_latest.json   G0/G1/G2 全部
  C 热榜    data/hot_scan_latest.json       top 10 by score
  D 横盘    data/sideways_latest.json       top 10 by range_pct asc（区间最窄优先）
  E 扶梯    data/escalator_latest.json      top 10 by R² desc（拟合度最高优先）

淘汰规则（每次运行时检查当前动态池）：
  - TTL 到期（A: 14 日, B/C: 7 日, D: 10 日, E: 10 日）
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
SIDEWAYS_LATEST_PATH  = DATA / "sideways_latest.json"
ESCALATOR_LATEST_PATH = DATA / "escalator_latest.json"

TTL_DAYS  = {"main_scan": 14, "gc_scan": 7, "hot_scan": 7, "sideways_scan": 10, "escalator_scan": 10}
LIMIT_A   = 15
LIMIT_C   = 10
LIMIT_D   = 10
LIMIT_E   = 10
STOP_LOSS = -8.0


def _clean(code: str) -> str:
    """Normalize 6-digit code; delegates to fetcher.normalize_code for SH/SZ/BJ-prefix handling."""
    import fetcher as _f
    return _f.normalize_code(code)


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


def _candidates_D() -> list[dict]:
    """横盘策略：从 sideways_latest.json all_picks 里取 range_pct 最窄的 top 10。"""
    if not SIDEWAYS_LATEST_PATH.exists():
        return []
    try:
        picks = json.loads(SIDEWAYS_LATEST_PATH.read_text(encoding="utf-8")).get("all_picks", [])
        # range_pct 越小代表区间越窄，反映横盘越紧
        picks.sort(key=lambda x: x.get("range_pct", 100.0))
        return [
            {"code": _clean(p["code"]), "name": p.get("name", p["code"]),
             "source": "sideways_scan", "entry_price": p.get("close", 0)}
            for p in picks[:LIMIT_D]
        ]
    except Exception as e:
        print(f"[updater] D-横盘读取失败: {e}")
        return []


def _candidates_E() -> list[dict]:
    """扶梯策略：从 escalator_latest.json all_picks 里取 R² 最高的 top 10（贴线最直）。"""
    if not ESCALATOR_LATEST_PATH.exists():
        return []
    try:
        picks = json.loads(ESCALATOR_LATEST_PATH.read_text(encoding="utf-8")).get("all_picks", [])
        # R² 越高代表走势越像一条直线
        picks.sort(key=lambda x: -float(x.get("r2", 0)))
        return [
            {"code": _clean(p["code"]), "name": p.get("name", p["code"]),
             "source": "escalator_scan", "entry_price": p.get("close", 0)}
            for p in picks[:LIMIT_E]
        ]
    except Exception as e:
        print(f"[updater] E-扶梯读取失败: {e}")
        return []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from common import load_config
    cfg = load_config()

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

    # ── Add new candidates / refresh existing ────────────────────────────────
    # 同票同源再次入选 → 刷新 added_date 延期 TTL（避免热度仍在的票被硬剔）
    kept_by_code: dict[str, dict] = {e["code"]: e for e in kept}
    added: list[dict] = []
    refreshed: list[dict] = []
    src_label = {"main_scan": "主策略", "gc_scan": "金叉", "hot_scan": "热榜",
                 "sideways_scan": "横盘", "escalator_scan": "扶梯"}

    for candidate in _candidates_A() + _candidates_B() + _candidates_C() + _candidates_D() + _candidates_E():
        code = candidate["code"]
        if code in manual_codes:
            continue
        # 已在池里：source 匹配 → 刷新 added_date；不匹配 → 不动（让原 TTL 走完）
        if code in kept_by_code:
            existing = kept_by_code[code]
            if (existing.get("source") == candidate["source"]
                    and existing.get("added_date") != today):
                existing["added_date"] = today
                refreshed.append(existing)
            continue
        # 不在池且本轮 added 也没添加过 → 入池
        if code in {c["code"] for c in added}:
            continue
        candidate["added_date"] = today
        added.append(candidate)

    if refreshed:
        print(f"[updater] 刷新 TTL {len(refreshed)} 只 (同源再次入选):")
        for e in refreshed:
            print(f"  ↻ {e.get('name', e['code'])}({e['code']}) [{src_label.get(e['source'], e['source'])}]")

    if added:
        print(f"[updater] 新增 {len(added)} 只:")
        for e in added:
            print(f"  + {e.get('name', e['code'])}({e['code']}) [{src_label.get(e['source'], e['source'])}]")

    new_list = kept + added
    print(f"[updater] 动态池: {len(current)} → {len(new_list)} 只  "
          f"(淘汰{len(evicted)} 新增{len(added)} 刷新{len(refreshed)})")

    if args.dry_run:
        print("[updater] dry-run，不保存")
        return

    _save_dynamic(new_list)
    print("[updater] 已保存 → watchlist_dynamic.json")

    # 飞书推送已停用（噪音过大）
    # if evicted or added:
    #     try:
    #         from notify.notify import push_feishu_card
    #         lines = [f"动态自选池  {today}  共{len(new_list)}只"]
    #         if added:
    #             lines.append(f"\n新增 {len(added)} 只")
    #             for e in added:
    #                 lines.append(f"  + {e.get('name', e['code'])}({e['code']})  [{src_label.get(e['source'], e['source'])}]")
    #         if refreshed:
    #             lines.append(f"\n刷新 TTL {len(refreshed)} 只 (同源再选)")
    #         if evicted:
    #             lines.append(f"\n移除 {len(evicted)} 只")
    #             for e, r in evicted:
    #                 lines.append(f"  - {e.get('name', e['code'])}({e['code']})  {r}")
    #         push_feishu_card(f"自选池更新 +{len(added)} -{len(evicted)} ↻{len(refreshed)}", lines)
    #     except Exception as e:
    #         print(f"[updater] 飞书推送失败: {e}")


if __name__ == "__main__":
    main()
