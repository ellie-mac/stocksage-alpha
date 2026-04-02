#!/usr/bin/env python3
"""
持仓监控 + 买卖信号微信提醒（Server酱）

用法:
  python scripts/monitor.py                  # 运行一次检查
  python scripts/monitor.py --dry-run        # 只打印，不推送
  python scripts/monitor.py --buy-only       # 只检查买入机会
  python scripts/monitor.py --sell-only      # 只检查卖出信号
  python scripts/monitor.py --loop           # 交易时间持续运行
  python scripts/monitor.py --loop --interval 1 --full-interval 10

配置文件:
  holdings.json      — 持仓列表
  alert_config.json  — Server酱 sendkey + 阈值

定时运行 (Windows 任务计划程序 / cron):
  每天 15:05 (收盘后5分钟):
  python C:/path/to/scripts/monitor.py
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

from serverchan_sdk import sc_send

# Windows: force UTF-8 stdout so Chinese characters don't crash cp1252
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))

from research import research
import fetcher

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOLDINGS_PATH = os.path.join(_ROOT, "holdings.json")
CONFIG_PATH   = os.path.join(_ROOT, "alert_config.json")


# ── Config loading ─────────────────────────────────────────────────────────────

def load_holdings() -> list[dict]:
    if not os.path.exists(HOLDINGS_PATH):
        print(f"[WARN] holdings.json not found at {HOLDINGS_PATH}")
        return []
    with open(HOLDINGS_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"alert_config.json not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


# ── Score computation ──────────────────────────────────────────────────────────

def _score_one(holding: dict) -> dict:
    """Research a single holding; return enriched dict with scores and signals."""
    code = holding["code"]
    try:
        result = research(code)
        buy_score  = result.get("total_score", 0) or 0
        sell_score = result.get("total_sell_score", 0) or 0
        price      = (result.get("price") or {}).get("current") or 0
        cost       = holding.get("cost_price", 0) or 0
        pnl_pct    = ((price - cost) / cost * 100) if cost > 0 else 0.0

        # Collect top sell signals from individual factors
        signals_summary = result.get("signals_summary", {})
        bearish = signals_summary.get("bearish_factors", [])

        return {
            "code":       code,
            "name":       result.get("name", holding.get("name", code)),
            "shares":     holding.get("shares", 0),
            "cost_price": cost,
            "price":      price,
            "pnl_pct":    round(pnl_pct, 2),
            "buy_score":  round(buy_score, 1),
            "sell_score": round(sell_score, 1),
            "bearish":    bearish[:3],  # top 3 sell signals
            "error":      None,
        }
    except Exception as e:
        return {
            "code":       code,
            "name":       holding.get("name", code),
            "shares":     holding.get("shares", 0),
            "cost_price": holding.get("cost_price", 0),
            "price":      None,
            "pnl_pct":    0.0,
            "buy_score":  0.0,
            "sell_score": 0.0,
            "bearish":    [],
            "error":      str(e),
        }


def _score_one_buy(code: str) -> dict:
    """Research a universe stock for buy screening."""
    try:
        result = research(code)
        return {
            "code":      code,
            "name":      result.get("name", code),
            "price":     (result.get("price") or {}).get("current"),
            "buy_score": round(result.get("total_score", 0) or 0, 1),
            "sell_score": round(result.get("total_sell_score", 0) or 0, 1),
            "error":     None,
        }
    except Exception as e:
        return {"code": code, "name": code, "price": None,
                "buy_score": 0.0, "sell_score": 0.0, "error": str(e)}


# ── Signal evaluation ──────────────────────────────────────────────────────────

def check_sell_signals(scored: dict, thresholds: dict) -> list[str]:
    """Return list of human-readable sell reasons, empty if no trigger."""
    reasons = []
    sell_trigger = thresholds.get("sell_score_trigger", 60)
    stop_loss    = thresholds.get("stop_loss_pct", -8.0)

    if scored["sell_score"] >= sell_trigger:
        reasons.append(f"综合卖出评分 {scored['sell_score']:.0f}/100 ≥ {sell_trigger}")

    if scored["pnl_pct"] <= stop_loss:
        reasons.append(f"止损触发: 浮亏 {scored['pnl_pct']:+.1f}% ≤ {stop_loss}%")

    for bf in scored.get("bearish", []):
        factor = bf.get("factor", "")
        sig    = bf.get("signal", "")
        ss     = bf.get("sell_score", 0)
        if ss >= 7:
            reasons.append(f"[{factor}] {sig} (卖出分={ss:.0f})")

    return reasons


def check_buy_signal(scored: dict, thresholds: dict, held_codes: set) -> bool:
    buy_trigger  = thresholds.get("buy_score_trigger", 65)
    sell_trigger = thresholds.get("sell_score_trigger", 60)
    if scored["code"] in held_codes:
        return False
    if scored["buy_score"] >= buy_trigger and scored["sell_score"] < sell_trigger * 0.7:
        return True
    return False


# ── Markdown formatting (Server酱 / WeChat) ────────────────────────────────────

def _fmt_sell_section_md(sell_alerts: list[dict]) -> str:
    if not sell_alerts:
        return "无卖出信号触发。\n"
    lines = []
    for s in sell_alerts:
        pnl_sign = "🔴" if s["pnl_pct"] < 0 else "🟢"
        lines.append(
            f"### ⚠️ {s['name']} ({s['code']})\n"
            f"现价 **{s['price']}** | 成本 {s['cost_price']} | "
            f"浮盈 {pnl_sign} **{s['pnl_pct']:+.1f}%**  \n"
            f"卖出评分: **{s['sell_score']:.0f}/100** | 买入评分: {s['buy_score']:.0f}/100\n"
        )
        for r in s["reasons"]:
            lines.append(f"- {r}")
        lines.append("")
    return "\n".join(lines)


def _fmt_buy_section_md(buy_alerts: list[dict]) -> str:
    if not buy_alerts:
        return "无新买入机会。\n"
    lines = []
    for b in buy_alerts:
        lines.append(
            f"### ✅ {b['name']} ({b['code']})\n"
            f"现价 **{b['price']}**  \n"
            f"买入评分: **{b['buy_score']:.0f}/100** | 卖出评分: {b['sell_score']:.0f}/100\n"
        )
    return "\n".join(lines)


def _fmt_holdings_table_md(scored_holdings: list[dict]) -> str:
    if not scored_holdings:
        return ""
    rows = ["| 股票 | 代码 | 现价 | 浮盈 | 卖出分 | 买入分 |",
            "|------|------|------|------|--------|--------|"]
    for s in scored_holdings:
        pnl = f"{s['pnl_pct']:+.1f}%"
        rows.append(
            f"| {s['name']} | {s['code']} | {s['price'] or 'N/A'} "
            f"| {pnl} | {s['sell_score']:.0f} | {s['buy_score']:.0f} |"
        )
    return "\n".join(rows)


def build_wechat_desp(
    sell_alerts: list[dict],
    buy_alerts:  list[dict],
    scored_holdings: list[dict],
    run_time: str,
) -> str:
    parts = [f"*{run_time}*\n"]
    parts.append("## 卖出信号\n")
    parts.append(_fmt_sell_section_md(sell_alerts))
    parts.append("## 买入机会（未持仓）\n")
    parts.append(_fmt_buy_section_md(buy_alerts))
    if scored_holdings:
        parts.append("## 当前持仓\n")
        parts.append(_fmt_holdings_table_md(scored_holdings))
    parts.append("\n\n> 仅供参考，不构成投资建议")
    return "\n".join(parts)


# ── Server酱推送 ───────────────────────────────────────────────────────────────

def send_wechat(title: str, desp: str, sendkey: str, dry_run: bool = False) -> None:
    if dry_run:
        print(f"[DRY-RUN] 微信推送标题: {title}")
        print(f"[DRY-RUN] 内容预览:\n{desp[:300]}{'...' if len(desp) > 300 else ''}")
        return
    resp = sc_send(sendkey, title, desp)
    if resp.get("code") == 0:
        print(f"[OK] 微信推送成功: {title}")
    else:
        print(f"[WARN] 微信推送返回: code={resp.get('code')} msg={resp.get('message')}")


# ── Trading hours ──────────────────────────────────────────────────────────────

# CST offsets (hours, minutes) for session windows
_MORNING_OPEN  = (9, 25)
_MORNING_CLOSE = (11, 35)
_AFTERNOON_OPEN  = (12, 55)
_AFTERNOON_CLOSE = (15, 5)


def _is_trading_hours() -> bool:
    """Return True if current CST time is within A-share trading windows."""
    now = datetime.now()
    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    hm = (now.hour, now.minute)
    morning   = _MORNING_OPEN   <= hm <= _MORNING_CLOSE
    afternoon = _AFTERNOON_OPEN <= hm <= _AFTERNOON_CLOSE
    return morning or afternoon


def _next_session_seconds() -> int:
    """Seconds until the next trading session opens (for sleep-until-open)."""
    now = datetime.now()
    # Try today's morning open
    today_open = now.replace(hour=_MORNING_OPEN[0], minute=_MORNING_OPEN[1],
                              second=0, microsecond=0)
    if now < today_open and now.weekday() < 5:
        return max(0, int((today_open - now).total_seconds()))
    # Try today's afternoon open
    aftn_open = now.replace(hour=_AFTERNOON_OPEN[0], minute=_AFTERNOON_OPEN[1],
                             second=0, microsecond=0)
    if now < aftn_open and now.weekday() < 5:
        return max(0, int((aftn_open - now).total_seconds()))
    # Tomorrow morning (skip weekends)
    days_ahead = 1
    while (now.weekday() + days_ahead) % 7 >= 5:
        days_ahead += 1
    from datetime import timedelta
    next_open = (now + timedelta(days=days_ahead)).replace(
        hour=_MORNING_OPEN[0], minute=_MORNING_OPEN[1], second=0, microsecond=0)
    return max(0, int((next_open - now).total_seconds()))


# ── Fast path (realtime quotes only) ───────────────────────────────────────────

def fast_check_holdings(
    holdings: list[dict],
    thresholds: dict,
    alert_state: dict,
) -> list[dict]:
    """
    Quick scan using only realtime quotes.  Returns list of alert dicts with:
      code, name, price, pnl_pct, change_pct, reasons
    Deduplicates: won't re-alert same code+reason within `cooldown_min` minutes.
    """
    cooldown_min = thresholds.get("fast_alert_cooldown_min", 30)
    stop_loss    = thresholds.get("stop_loss_pct", -8.0)
    intraday_drop_trigger = thresholds.get("intraday_drop_trigger_pct", -5.0)
    intraday_surge_trigger = thresholds.get("intraday_surge_trigger_pct", 7.0)

    alerts = []
    now = datetime.now()

    for h in holdings:
        code = h["code"]
        quote = fetcher.get_realtime_quote(code)
        if not quote or "error" in quote:
            continue

        price = quote.get("price") or 0
        cost  = h.get("cost_price", 0) or 0
        pnl_pct     = ((price - cost) / cost * 100) if cost > 0 else 0.0
        change_pct  = quote.get("change_pct") or 0.0

        reasons = []
        if pnl_pct <= stop_loss:
            reasons.append(f"止损触发: 浮亏 {pnl_pct:+.1f}%")
        if change_pct <= intraday_drop_trigger:
            reasons.append(f"日内急跌 {change_pct:+.1f}%")
        if change_pct >= intraday_surge_trigger:
            reasons.append(f"日内急涨 {change_pct:+.1f}% — 考虑止盈")

        if not reasons:
            continue

        # Cooldown: skip if same code alerted within cooldown_min
        last_alert = alert_state.get(code)
        if last_alert:
            elapsed = (now - last_alert).total_seconds() / 60
            if elapsed < cooldown_min:
                continue

        alert_state[code] = now
        alerts.append({
            "code":       code,
            "name":       quote.get("name") or h.get("name", code),
            "price":      price,
            "change_pct": change_pct,
            "pnl_pct":    round(pnl_pct, 2),
            "cost_price": cost,
            "reasons":    reasons,
        })

    return alerts


def build_fast_wechat_desp(fast_alerts: list[dict], run_time: str) -> str:
    lines = [f"*{run_time}*\n"]
    for a in fast_alerts:
        pnl_icon = "🔴" if a["pnl_pct"] < 0 else "🟢"
        chg_icon = "📉" if a["change_pct"] < 0 else "📈"
        lines.append(
            f"### ⚡ {a['name']} ({a['code']})\n"
            f"现价 **{a['price']}** | "
            f"今日 {chg_icon} **{a['change_pct']:+.1f}%** | "
            f"浮盈 {pnl_icon} **{a['pnl_pct']:+.1f}%**\n"
        )
        for r in a["reasons"]:
            lines.append(f"- {r}")
        lines.append("")
    lines.append("\n> 仅供参考，不构成投资建议")
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, buy_only: bool = False, sell_only: bool = False,
        always_send: bool = False) -> None:
    config     = load_config()
    holdings   = load_holdings()
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")
    universe   = config.get("screener_universe", [])
    run_time   = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(f"[{run_time}] StockSage Monitor starting...")

    # ── 1. Score holdings (sell signal check) ─────────────────────────────────
    scored_holdings: list[dict] = []
    if not buy_only and holdings:
        print(f"  Scoring {len(holdings)} holdings...")
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(_score_one, h): h for h in holdings}
            for fut in as_completed(futures):
                scored_holdings.append(fut.result())
        scored_holdings.sort(key=lambda x: -x["sell_score"])

    sell_alerts = []
    if not buy_only:
        for s in scored_holdings:
            if s["error"]:
                print(f"  [WARN] {s['code']}: {s['error']}")
                continue
            reasons = check_sell_signals(s, thresholds)
            if reasons:
                sell_alerts.append({**s, "reasons": reasons})
                print(f"  SELL SIGNAL: {s['name']} ({s['code']}) — {reasons[0]}")

    # ── 2. Score universe (buy signal check) ──────────────────────────────────
    buy_alerts = []
    if not sell_only and universe:
        held_codes = {h["code"] for h in holdings}
        print(f"  Screening {len(universe)} stocks for buy signals...")
        scored_universe: list[dict] = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(_score_one_buy, code): code for code in universe}
            for fut in as_completed(futures):
                scored_universe.append(fut.result())
        scored_universe.sort(key=lambda x: -x["buy_score"])

        top_n = thresholds.get("buy_universe_top_n", 5)
        for s in scored_universe[:top_n * 3]:  # check top candidates
            if check_buy_signal(s, thresholds, held_codes):
                buy_alerts.append(s)
                print(f"  BUY SIGNAL:  {s['name']} ({s['code']}) score={s['buy_score']}")
            if len(buy_alerts) >= top_n:
                break

    # ── 3. Build + send email ─────────────────────────────────────────────────
    has_signal = bool(sell_alerts or buy_alerts)
    if not has_signal and not always_send:
        print("  No signals triggered. No email sent.")
        return

    subject_parts = []
    if sell_alerts:
        subject_parts.append(f"⚠️ {len(sell_alerts)} 卖出信号")
    if buy_alerts:
        subject_parts.append(f"✅ {len(buy_alerts)} 买入机会")
    if not subject_parts:
        subject_parts.append("持仓日报")
    title = f"[StockSage] {' | '.join(subject_parts)}"

    desp = build_wechat_desp(sell_alerts, buy_alerts, scored_holdings, run_time)

    try:
        send_wechat(title, desp, sendkey, dry_run=dry_run)
    except Exception as e:
        print(f"[ERROR] 微信推送失败: {e}")


def run_loop(
    interval_min: int = 2,
    full_interval_min: int = 30,
    dry_run: bool = False,
) -> None:
    """
    High-frequency intraday loop.

    - Every `interval_min` minutes: fast check (realtime quotes only).
      Alerts on stop-loss / intraday drop ≥ 5% / intraday surge ≥ 7%.
    - Every `full_interval_min` minutes: full factor check (all factors + buy screen).
    - Automatically waits when outside trading hours.
    - Per-stock cooldown prevents repeat alerts within 30 min.
    """
    config     = load_config()
    holdings   = load_holdings()
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")

    alert_state: dict = {}          # code -> last alert datetime
    last_full_run: Optional[datetime] = None

    print(f"[StockSage Loop] interval={interval_min}min  full_every={full_interval_min}min")
    print(f"  Holdings: {[h['code'] for h in holdings]}")
    print("  Press Ctrl+C to stop.\n")

    while True:
        now = datetime.now()

        if not _is_trading_hours():
            wait_sec = _next_session_seconds()
            wait_min = wait_sec // 60
            print(f"[{now.strftime('%H:%M')}] Outside trading hours. "
                  f"Next session in ~{wait_min} min. Sleeping...")
            # Sleep in chunks so Ctrl+C is responsive
            for _ in range(min(wait_sec, 300)):
                time.sleep(1)
            continue

        run_time = now.strftime("%Y-%m-%d %H:%M")

        # ── Fast check ────────────────────────────────────────────────────────
        print(f"[{run_time}] Fast check ({len(holdings)} holdings)...", end=" ", flush=True)
        fast_alerts = fast_check_holdings(holdings, thresholds, alert_state)
        print(f"{len(fast_alerts)} alert(s)")

        if fast_alerts:
            title = f"[StockSage ⚡] {len(fast_alerts)} 实时预警"
            desp  = build_fast_wechat_desp(fast_alerts, run_time)
            try:
                send_wechat(title, desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [ERROR] 微信推送失败: {e}")

        # ── Full check (every full_interval_min) ──────────────────────────────
        need_full = (
            last_full_run is None or
            (now - last_full_run).total_seconds() >= full_interval_min * 60
        )
        if need_full:
            print(f"[{run_time}] Full factor check...")
            try:
                run(dry_run=dry_run)
            except Exception as e:
                print(f"  [ERROR] Full check failed: {e}")
            last_full_run = datetime.now()

        # ── Sleep until next fast interval ────────────────────────────────────
        sleep_sec = interval_min * 60
        for _ in range(sleep_sec):
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StockSage 持仓监控 + 买卖信号邮件提醒")
    parser.add_argument("--dry-run",     action="store_true", help="只打印，不发邮件")
    parser.add_argument("--buy-only",    action="store_true", help="只检查买入机会")
    parser.add_argument("--sell-only",   action="store_true", help="只检查卖出信号")
    parser.add_argument("--always-send", action="store_true", help="无信号时也发日报邮件")
    parser.add_argument("--loop",        action="store_true",
                        help="持续运行：交易时间每 --interval 分钟快速检查一次")
    parser.add_argument("--interval",    type=int, default=2,
                        help="快速检查间隔（分钟），默认 2")
    parser.add_argument("--full-interval", type=int, default=30,
                        help="完整因子检查间隔（分钟），默认 30")
    args = parser.parse_args()

    if args.loop:
        run_loop(
            interval_min=args.interval,
            full_interval_min=args.full_interval,
            dry_run=args.dry_run,
        )
    else:
        run(dry_run=args.dry_run, buy_only=args.buy_only,
            sell_only=args.sell_only, always_send=args.always_send)
