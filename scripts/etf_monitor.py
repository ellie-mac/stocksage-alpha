#!/usr/bin/env python3
"""
ETF T+0 监控 — 专门针对场内 ETF 的高频买卖信号监控。

与 monitor.py 的核心区别:
  - ETF 可当日反复买卖（T+0），买卖信号冷却均为 20 分钟
  - 维护当日操作记录（每只 ETF 的买入/卖出提醒次数）
  - 卖出信号无 T+1 限制（ETF 当天买当天卖完全合法）
  - 扫描间隔更短（默认 5 分钟）

ETF 清单: alert_config.json → "etf_watchlist"
  示例:
    "etf_watchlist": [
      {"code": "510050", "name": "华夏上证50ETF",    "shares": 0, "cost_price": 0},
      {"code": "510300", "name": "华泰沪深300ETF",   "shares": 0, "cost_price": 0},
      {"code": "159915", "name": "易方达创业板ETF",  "shares": 0, "cost_price": 0}
    ]
  shares/cost_price 填 0 表示未持仓（只看买入信号）；填实际值则同时看卖出信号。

Usage:
  python scripts/etf_monitor.py                  # 默认每 5 分钟扫描
  python scripts/etf_monitor.py --dry-run        # 只打印，不推送
  python scripts/etf_monitor.py --interval 3    # 每 3 分钟扫描
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))

from research import research
from factors_extended import score_market_regime
import fetcher
from common import is_trading_hours, next_session_seconds, send_wechat

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT            = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH      = os.path.join(_ROOT, "alert_config.json")
SIGNALS_LOG_PATH = os.path.join(_ROOT, "signals_log.json")

# No cooldowns for ETF: T+0 means each signal can represent a new partial trade tranche.


# ── Signals log ────────────────────────────────────────────────────────────────

def _append_signals_log(buy_alerts: list[dict], sell_alerts: list[dict],
                         run_time: str, regime_score: Optional[float] = None) -> None:
    """Append ETF buy + sell signals to the shared signals_log.json for backtesting."""
    if not buy_alerts and not sell_alerts:
        return

    def _common(s: dict) -> dict:
        return {
            "code":         s["code"],
            "name":         s.get("name", s["code"]),
            "signal_price": s.get("price"),
            "change_pct":   s.get("change_pct"),
            "buy_score":    s.get("buy_score"),
            "sell_score":   s.get("sell_score"),
            "bullish":      s.get("bullish", []),
            "bearish":      s.get("bearish", []),
        }

    entry = {
        "date":         datetime.now().strftime("%Y-%m-%d"),
        "run_time":     run_time,
        "regime_score": regime_score,
        "source":       "etf_monitor",
        "buy_signals": [_common(b) for b in buy_alerts],
        "sell_signals": [
            {
                **_common(s),
                "shares":     s.get("shares"),
                "cost_price": s.get("cost_price"),
                "pnl_pct":    s.get("pnl_pct"),
                "reasons":    s.get("reasons", []),
            }
            for s in sell_alerts
        ],
    }
    try:
        with open(SIGNALS_LOG_PATH, "r", encoding="utf-8") as f:
            log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log = []
    log.append(entry)
    with open(SIGNALS_LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"  Signals logged → signals_log.json "
          f"(buy={len(buy_alerts)}, sell={len(sell_alerts)})")


# ── Config ─────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


# ── Scoring ────────────────────────────────────────────────────────────────────

def _score_etf(etf: dict) -> dict:
    """Run full research on one ETF entry."""
    code = etf["code"]
    try:
        result = research(code)
        buy_score  = round(result.get("total_score", 0) or 0, 1)
        sell_score = round(result.get("total_sell_score", 0) or 0, 1)
        price_d    = result.get("price") or {}
        price      = price_d.get("current") or 0
        cost       = etf.get("cost_price", 0) or 0
        pnl_pct    = ((price - cost) / cost * 100) if cost > 0 else 0.0
        summary    = result.get("signals_summary", {})
        return {
            "code":       code,
            "name":       result.get("name") or etf.get("name", code),
            "shares":     etf.get("shares", 0),
            "cost_price": cost,
            "price":      price,
            "change_pct": price_d.get("change_pct"),
            "pnl_pct":    round(pnl_pct, 2),
            "buy_score":  buy_score,
            "sell_score": sell_score,
            "bullish":    summary.get("top_bullish", [])[:3],
            "bearish":    summary.get("top_bearish", [])[:3],
            "error":      None,
        }
    except Exception as e:
        return {
            "code": code, "name": etf.get("name", code),
            "shares": etf.get("shares", 0), "cost_price": etf.get("cost_price", 0),
            "price": None, "pnl_pct": 0.0,
            "buy_score": 0.0, "sell_score": 0.0, "bearish": [], "error": str(e),
        }


# ── Signal checks ──────────────────────────────────────────────────────────────

def _check_buy(scored: dict, thresholds: dict, held_codes: set,
               regime_score: float = 5.0) -> bool:
    """
    Apply the same regime gate as monitor.py:
      Bear  (≤2): buy threshold +25%, sell_score must be < 25 (bottom-fishing only)
      Caution (≤4): buy threshold +15%
    """
    buy_trigger  = thresholds.get("buy_score_trigger", 65)
    sell_trigger = thresholds.get("sell_score_trigger", 60)

    if regime_score <= 2:
        buy_trigger = round(buy_trigger * 1.25, 1)
        if scored["sell_score"] >= 25:
            return False
    elif regime_score <= 4:
        buy_trigger = round(buy_trigger * 1.15, 1)

    return (scored["buy_score"] >= buy_trigger and
            scored["sell_score"] < sell_trigger * 0.7)


def _check_sell(scored: dict, thresholds: dict) -> list[str]:
    reasons = []
    sell_trigger = thresholds.get("sell_score_trigger", 60)
    stall_score  = thresholds.get("stall_sell_score", 40)
    stop_loss    = thresholds.get("stop_loss_pct", -8.0)
    if scored.get("shares", 0) <= 0:
        return []   # not held, no sell signal

    if scored["sell_score"] >= sell_trigger:
        reasons.append(f"综合卖出评分 {scored['sell_score']:.0f}/100 ≥ {sell_trigger}")
    elif stall_score <= scored["sell_score"] < sell_trigger:
        # Soft sell: high-and-stalling — prime T+0 exit scenario
        reasons.append(
            f"逢高减仓参考: 卖出信号 **{scored['sell_score']:.0f}** 显示动能减弱"
            f"（阈值 {stall_score}–{sell_trigger}）"
        )

    if scored["pnl_pct"] <= stop_loss:
        reasons.append(f"止损触发: 浮亏 {scored['pnl_pct']:+.1f}%")
    for bf in scored.get("bearish", []):
        if bf.get("sell_score", 0) >= 7:
            reasons.append(f"[{bf.get('factor','')}] {bf.get('signal','')} "
                           f"(卖出分={bf['sell_score']:.0f})")
    return reasons


# ── WeChat formatting ──────────────────────────────────────────────────────────

def _build_desp(
    buy_alerts: list[dict],
    sell_alerts: list[dict],
    today_activity: dict,
    run_time: str,
    regime_score: float,
    regime_signal: str,
    thresholds: dict = {},
) -> str:
    lines = [f"*{run_time}*  市场体制: {regime_score}/10 — {regime_signal}\n"]

    stop_loss_pct = thresholds.get("stop_loss_pct", -8.0) if isinstance(thresholds, dict) else -8.0
    sell_trigger  = thresholds.get("sell_score_trigger", 60) if isinstance(thresholds, dict) else 60
    stall_score   = thresholds.get("stall_sell_score", 40)   if isinstance(thresholds, dict) else 40
    if sell_alerts:
        lines.append("## 卖出信号 🔴\n")
        for a in sell_alerts:
            pnl_icon = "🔴" if a["pnl_pct"] < 0 else "🟢"
            cost = a.get("cost_price") or 0
            stop_price = round(cost * (1 + stop_loss_pct / 100), 3) if cost > 0 else "—"
            if a["sell_score"] >= sell_trigger:
                pos_hint = "建议减仓参考 50–100%（强卖出信号）"
            else:
                pos_hint = "建议减仓参考 30–50%（动能减弱，逢高减仓）"
            lines.append(
                f"### {a['name']} ({a['code']})\n"
                f"现价 **{a['price']}** | 浮盈 {pnl_icon} **{a['pnl_pct']:+.1f}%**  \n"
                f"卖出评分: **{a['sell_score']:.0f}/100** | 参考止损价: **{stop_price}**  \n"
                f"*{pos_hint}*\n"
            )
            for r in a["reasons"]:
                lines.append(f"- {r}")
            lines.append("")

    if buy_alerts:
        lines.append("## 买入信号 ✅\n")
        for a in buy_alerts:
            price = a.get("price") or 0
            entry_lo = round(price * 0.998, 3)
            entry_hi = round(price * 1.003, 3)
            if a["buy_score"] >= 85:
                pos_hint = "建议仓位参考 8–12%"
            elif a["buy_score"] >= 75:
                pos_hint = "建议仓位参考 5–8%"
            else:
                pos_hint = "建议仓位参考 3–5%（轻仓试探）"
            lines.append(
                f"### {a['name']} ({a['code']})\n"
                f"现价 **{price}** | 参考区间: **{entry_lo} ~ {entry_hi}**  \n"
                f"买入评分: **{a['buy_score']:.0f}/100**  \n"
                f"*{pos_hint}*\n"
            )

    if today_activity:
        lines.append("## 今日信号统计\n")
        lines.append("| ETF | 买入提醒 | 卖出提醒 |")
        lines.append("|-----|----------|----------|")
        for code, act in today_activity.items():
            lines.append(f"| {code} | {act['buys']} | {act['sells']} |")

    lines.append("\n> T+0 / 仅供参考，不构成投资建议")
    return "\n".join(lines)


def _send(title: str, desp: str, sendkey: str, dry_run: bool) -> None:
    send_wechat(title, desp, sendkey, dry_run=dry_run)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run_loop(interval_min: int = 5, dry_run: bool = False) -> None:
    """High-frequency ETF T+0 monitoring loop."""
    config     = load_config()
    etf_list   = config.get("etf_watchlist", [])
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")

    if not etf_list:
        print("[WARN] etf_watchlist is empty in alert_config.json. Nothing to monitor.")
        print("  Add entries like: {\"code\": \"510050\", \"name\": \"上证50ETF\", "
              "\"shares\": 0, \"cost_price\": 0}")
        return

    today_activity: dict = {e["code"]: {"buys": 0, "sells": 0} for e in etf_list}
    _last_activity_date = datetime.now().date()
    _closing_date = None   # not persisted — harmless if fired twice on restart

    print(f"[ETF Monitor] interval={interval_min}min  no cooldown (T+0 partial trades)")
    print(f"  ETFs: {[e['code'] for e in etf_list]}")
    print("  Press Ctrl+C to stop.\n")

    while True:
        now = datetime.now()

        # ── Config hot-reload (picks up ETF list / threshold changes) ─────────
        try:
            config     = load_config()
            etf_list   = config.get("etf_watchlist", [])
            thresholds = config.get("thresholds", {})
            sendkey    = config.get("serverchan", {}).get("sendkey", "")
        except Exception as e:
            print(f"  [WARN] Config reload failed: {e}")

        # Reset daily activity counter at midnight
        if now.date() != _last_activity_date:
            today_activity = {e["code"]: {"buys": 0, "sells": 0} for e in etf_list}
            _last_activity_date = now.date()

        # ── Daily closing summary (15:05) ─────────────────────────────────────
        if (now.weekday() < 5
                and now.hour == 15 and 5 <= now.minute < 10
                and _closing_date != now.date()):
            _closing_date = now.date()
            rows = ["| ETF | 今日涨跌 | 买入提醒 | 卖出提醒 |",
                    "|-----|----------|----------|----------|"]
            for e in etf_list:
                code = e["code"]
                name = e.get("name", code)
                act  = today_activity.get(code, {"buys": 0, "sells": 0})
                try:
                    q   = fetcher.get_realtime_quote(code)
                    chg = q.get("change_pct") or 0.0
                    rows.append(
                        f"| {name} | {'📈' if chg >= 0 else '📉'} {chg:+.1f}% "
                        f"| {act['buys']} 次 | {act['sells']} 次 |"
                    )
                except Exception:
                    rows.append(f"| {name} | — | {act['buys']} 次 | {act['sells']} 次 |")
            closing_desp = (
                f"**{now.strftime('%Y-%m-%d')} ETF 收盘快报**\n\n"
                + "\n".join(rows)
                + "\n\n> T+0 / 仅供参考，不构成投资建议"
            )
            try:
                _send("[StockSage ETF] 今日收盘 📊", closing_desp, sendkey, dry_run)
            except Exception as e:
                print(f"  [WARN] Closing push failed: {e}")

        if not is_trading_hours():
            wait_sec = next_session_seconds()
            print(f"[{now.strftime('%H:%M')}] Outside trading hours. "
                  f"Next session in ~{wait_sec//60} min. Sleeping...")
            for _ in range(min(wait_sec, 300)):
                time.sleep(1)
            continue

        run_time = now.strftime("%Y-%m-%d %H:%M")

        # ── Market regime ──────────────────────────────────────────────────────
        regime_score  = 5.0
        regime_signal = "unknown"
        try:
            mkt = score_market_regime(fetcher.get_market_regime_data())
            if mkt:
                regime_score  = mkt.get("score", 5.0)
                regime_signal = mkt.get("details", {}).get("signal", "unknown")
        except Exception as e:
            print(f"  [WARN] Regime fetch failed: {e}")

        # ── Score all ETFs concurrently ────────────────────────────────────────
        print(f"[{run_time}] Scanning {len(etf_list)} ETFs...", end=" ", flush=True)
        scored: list[dict] = []
        with ThreadPoolExecutor(max_workers=min(len(etf_list), 8)) as ex:
            futures = {ex.submit(_score_etf, e): e for e in etf_list}
            for fut in as_completed(futures):
                scored.append(fut.result())
        print(f"done")

        held_codes = {e["code"] for e in etf_list if (e.get("shares") or 0) > 0}

        buy_alerts:  list[dict] = []
        sell_alerts: list[dict] = []

        for s in scored:
            if s["error"]:
                print(f"  [WARN] {s['code']}: {s['error']}")
                continue

            code = s["code"]

            # ── Sell check ────────────────────────────────────────────────────
            sell_reasons = _check_sell(s, thresholds)
            if sell_reasons:
                today_activity.setdefault(code, {"buys": 0, "sells": 0})["sells"] += 1
                sell_alerts.append({**s, "reasons": sell_reasons})
                print(f"  SELL: {s['name']} ({code}) score={s['sell_score']}")

            # ── Buy check ─────────────────────────────────────────────────────
            if _check_buy(s, thresholds, held_codes, regime_score=regime_score):
                today_activity.setdefault(code, {"buys": 0, "sells": 0})["buys"] += 1
                buy_alerts.append(s)
                print(f"  BUY:  {s['name']} ({code}) score={s['buy_score']}")

        # ── Push ──────────────────────────────────────────────────────────────
        if buy_alerts or sell_alerts:
            _sell_trigger = thresholds.get("sell_score_trigger", 60)
            _stall_score  = thresholds.get("stall_sell_score", 40)
            _STRONG_BUY   = 80
            strong_sells = [a for a in sell_alerts if a["sell_score"] >= _sell_trigger]
            stall_sells  = [a for a in sell_alerts if _stall_score <= a["sell_score"] < _sell_trigger]
            strong_buys  = [a for a in buy_alerts  if a["buy_score"] >= _STRONG_BUY]
            add_buys     = [a for a in buy_alerts  if a["buy_score"] < _STRONG_BUY]
            parts = []
            if strong_sells: parts.append(f"🔴 {len(strong_sells)} 强卖")
            if stall_sells:  parts.append(f"⚠️ {len(stall_sells)} 减仓参考")
            if strong_buys:  parts.append(f"✅ {len(strong_buys)} 强买")
            if add_buys:     parts.append(f"💡 {len(add_buys)} 加仓参考")
            title = f"[StockSage ETF] {' | '.join(parts)}"
            desp = _build_desp(buy_alerts, sell_alerts, today_activity,
                                run_time, regime_score, regime_signal, thresholds)
            try:
                _send(title, desp, sendkey, dry_run)
            except Exception as e:
                print(f"  [ERROR] 推送失败: {e}")
            _append_signals_log(buy_alerts, sell_alerts, run_time,
                                regime_score=regime_score)
        else:
            print(f"  No signals.")

        for _ in range(interval_min * 60):
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StockSage ETF T+0 监控")
    parser.add_argument("--dry-run",  action="store_true", help="只打印，不推送")
    parser.add_argument("--interval", type=int, default=5, help="扫描间隔（分钟），默认 5")
    args = parser.parse_args()
    run_loop(interval_min=args.interval, dry_run=args.dry_run)
