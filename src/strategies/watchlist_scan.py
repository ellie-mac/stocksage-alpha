#!/usr/bin/env python3
"""
自选池扫描 — 盘中每30分钟运行，检查买入信号

- 买入：对 watchlist 里的股票打分，分数超阈值则推送
- 同一股票有冷却窗口（默认 90 分钟），避免盘中重复推送

用法：
    python -X utf8 src/strategies/watchlist_scan.py
    python -X utf8 src/strategies/watchlist_scan.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import partial

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factors import weights_from_config_dict
from factors.config import REGIME_WEIGHTS
from factors import score_market_regime
import fetcher
from common import send_wechat, setup_push
from report.utils import regime_key as _regime_key, score_one_buy as _score_watchlist
from research import _FACTOR_ZH_REPORT

_ROOT           = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCAN_CACHE_PATH = os.path.join(_ROOT, "data", "watchlist_scan_latest.json")


# ── 核心扫描 ──────────────────────────────────────────────────────────────────

def scan(
    watchlist_codes: list[str],
    thresholds: dict,
    regime_score: float = 5.0,
) -> tuple[list[dict], list[dict]]:
    """Score watchlist. Returns (buy_alerts, all_scored)."""
    rk  = _regime_key(regime_score)
    fw  = weights_from_config_dict(REGIME_WEIGHTS[rk])
    _sw = partial(_score_watchlist, weights=fw)

    sell_guard = max(0, thresholds.get("sell_score_trigger", 60) - 10)
    buy_trig   = thresholds.get("buy_score_trigger", 70)
    if regime_score <= 2:
        buy_trig   = round(buy_trig * 1.25, 1)
        sell_guard = min(sell_guard, 25)
    elif regime_score <= 4:
        buy_trig = round(buy_trig * 1.15, 1)

    scored_wl: list[dict] = []
    if watchlist_codes:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_sw, c): c for c in watchlist_codes}
            for fut in as_completed(futs):
                scored_wl.append(fut.result())
    scored_wl.sort(key=lambda x: -x.get("buy_score", 0))

    buy_alerts: list[dict] = []
    top_n = thresholds.get("buy_universe_top_n", 5)
    for s in scored_wl:
        if s.get("error"):
            continue
        if s["buy_score"] < buy_trig:
            break
        if s["sell_score"] >= sell_guard:
            continue
        if (s.get("change_pct") or 0) >= 9.5:
            continue
        buy_alerts.append(s)
        if len(buy_alerts) >= top_n:
            break

    return buy_alerts, scored_wl


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.join(_ROOT, "alert_config.json")
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    thresholds = config.get("thresholds", {})
    sendkey = setup_push(config)

    raw_wl = config.get("watchlist", [])
    if raw_wl and isinstance(raw_wl[0], dict):
        watchlist_codes = [e["code"] for e in raw_wl]
    else:
        watchlist_codes = [c[-6:] if len(c) > 6 else c for c in raw_wl]

    # Merge dynamic watchlist
    dynamic_path = os.path.join(_ROOT, "data", "watchlist_dynamic.json")
    if os.path.exists(dynamic_path):
        try:
            dynamic = json.load(open(dynamic_path, encoding="utf-8"))
            dynamic_codes = [e["code"] for e in dynamic if isinstance(e, dict) and e.get("code")]
            seen = set(watchlist_codes)
            watchlist_codes += [c for c in dynamic_codes if c not in seen]
        except Exception:
            pass

    if not watchlist_codes:
        print("[watchlist_scan] 自选池为空，退出")
        return
    print(f"[watchlist_scan] watchlist={len(watchlist_codes)} (手动+动态)")

    regime_score  = 5.0
    regime_signal = "unknown"
    try:
        mkt = score_market_regime(fetcher.get_market_regime_data())
        if mkt:
            regime_score  = mkt.get("score", 5.0)
            regime_signal = mkt.get("details", {}).get("signal", "unknown")
    except Exception as e:
        print(f"[watchlist_scan] regime fetch failed: {e}")
    print(f"[watchlist_scan] regime={regime_score}/10 {regime_signal}")

    buy_alerts, all_scored = scan(watchlist_codes, thresholds, regime_score)
    print(f"[watchlist_scan] buy={len(buy_alerts)}")

    now_dt = datetime.now()

    # 保存扫描缓存
    try:
        os.makedirs(os.path.dirname(SCAN_CACHE_PATH), exist_ok=True)
        with open(SCAN_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "date":   now_dt.strftime("%Y-%m-%d"),
                "time":   now_dt.strftime("%H:%M"),
                "scored": [{k: s.get(k) for k in
                            ("code","name","buy_score","sell_score","price","bullish","bearish")}
                           for s in all_scored if not s.get("error")],
            }, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    if not buy_alerts:
        print("[watchlist_scan] 无买入信号，跳过推送")
        return

    run_time  = now_dt.strftime("%Y-%m-%d %H:%M")
    _re_emoji = "🐻" if regime_score <= 3 else ("🟡" if regime_score <= 6 else "🐂")

    rows = [f"*{run_time}*<br>市场 {_re_emoji} {regime_score:.0f}/10"]
    for ba in buy_alerts:
        p = ba.get("price") or 0
        rows.append(f"**{ba['name']} ({ba['code']})**<br>"
                    f"买入分 **{ba['buy_score']:.0f}** | 现价 **{p}**")
        if ba.get("bullish"):
            labels = [f"`{_FACTOR_ZH_REPORT.get(b['factor'], b['factor'])}`"
                      for b in ba["bullish"] if isinstance(b, dict) and b.get("factor")]
            if labels:
                rows.append("+ " + " / ".join(labels))
    rows.append("<br>> 仅供参考")
    desp = "<br>".join(rows)

    strong_b = [a for a in buy_alerts if a["buy_score"] >= 80]
    add_b    = [a for a in buy_alerts if a not in strong_b]
    parts: list[str] = []
    if strong_b: parts.append(f"✅ {len(strong_b)} 强买")
    if add_b:    parts.append(f"💡 {len(add_b)} 买入")
    title = f"自选池 {' | '.join(parts)}"

    if not args.dry_run:
        try:
            send_wechat(title, desp, sendkey, dry_run=False)
            print("[watchlist_scan] 微信推送完成")
        except Exception as e:
            print(f"[watchlist_scan] 微信推送失败: {e}")
    else:
        print(f"[watchlist_scan] dry-run:\n{title}\n{desp}")


if __name__ == "__main__":
    main()
