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
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(__file__))

from research import research
from factors_extended import score_market_regime
import fetcher
import cache
from common import (
    is_trading_hours  as _is_trading_hours,
    next_session_seconds as _next_session_seconds,
    send_wechat,
    is_etf   as _is_etf,
    is_t1_locked as _is_t1_locked_common,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOLDINGS_PATH = os.path.join(_ROOT, "holdings.json")
CONFIG_PATH   = os.path.join(_ROOT, "alert_config.json")
SIGNALS_LOG_PATH  = os.path.join(_ROOT, "data", "signals_log.json")
LATEST_PICKS_PATH = os.path.join(_ROOT, "data", "latest_picks.json")
STATE_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".monitor_state.json")


# ── State persistence ──────────────────────────────────────────────────────────

def _load_state() -> dict:
    """Load persisted loop state from disk (survives restarts)."""
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    """Persist loop state to disk (best-effort, never raises)."""
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass


# ── Config loading ─────────────────────────────────────────────────────────────

def load_holdings() -> list[dict]:
    if not os.path.exists(HOLDINGS_PATH):
        print(f"[WARN] holdings.json not found at {HOLDINGS_PATH}")
        return []
    with open(HOLDINGS_PATH, encoding="utf-8") as f:
        return json.load(f)


def _backfill_holding_names(scored_holdings: list[dict]) -> None:
    """Write real company names back to holdings.json for entries where name == code.

    Only rewrites the file if at least one name was updated, to avoid churn.
    """
    try:
        with open(HOLDINGS_PATH, encoding="utf-8") as f:
            holdings = json.load(f)
    except Exception:
        return

    name_map = {s["code"]: s["name"] for s in scored_holdings
                if s.get("name") and s["name"] != s["code"]}
    updated = False
    for h in holdings:
        if h.get("name") == h["code"] and h["code"] in name_map:
            h["name"] = name_map[h["code"]]
            updated = True

    if updated:
        with open(HOLDINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(holdings, f, ensure_ascii=False, indent=2)
        print(f"  holdings.json names updated ({sum(1 for h in holdings if h['name'] != h['code'])} resolved)")


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"alert_config.json not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)



# ── ETF / T+1 helpers ──────────────────────────────────────────────────────────

def _is_t1_locked(holding: dict) -> bool:
    """Thin wrapper — delegates to common.is_t1_locked."""
    return _is_t1_locked_common(holding)


# ── Score computation ──────────────────────────────────────────────────────────


def _compact_factor_scores(factors: dict) -> dict:
    """Distill factors dict → {name: {buy, sell}} for compact logging."""
    return {
        name: {"buy": round(f.get("score") or 0, 1),
               "sell": round(f.get("sell_score") or 0, 1)}
        for name, f in factors.items()
        if isinstance(f, dict)
    }


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

        summary  = result.get("signals_summary", {})
        price_d  = result.get("price") or {}
        basic    = result.get("basic") or {}
        val      = result.get("valuation") or {}

        return {
            "code":             code,
            "name":             result.get("name", holding.get("name", code)),
            "shares":           holding.get("shares", 0),
            "cost_price":       cost,
            "price":            price,
            "change_pct":       price_d.get("change_pct"),
            "pnl_pct":          round(pnl_pct, 2),
            "buy_score":        round(buy_score, 1),
            "sell_score":       round(sell_score, 1),
            "bullish":          summary.get("top_bullish", [])[:3],
            "bearish":          summary.get("top_bearish", [])[:3],
            # backtesting context
            "industry":         basic.get("industry", "Unknown"),
            "market_cap_b":     basic.get("market_cap_billion"),
            "pe_ttm":           val.get("pe_ttm"),
            "pb":               val.get("pb"),
            "turnover_rate":    price_d.get("turnover_rate"),
            "volume_ratio":     price_d.get("volume_ratio"),
            "volume_million":   price_d.get("volume_million"),
            "factor_scores":    _compact_factor_scores(result.get("factors") or {}),
            "error":            None,
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
        summary  = result.get("signals_summary", {})
        price_d  = result.get("price") or {}
        basic    = result.get("basic") or {}
        val      = result.get("valuation") or {}
        return {
            "code":           code,
            "name":           result.get("name", code),
            "price":          price_d.get("current"),
            "change_pct":     price_d.get("change_pct"),
            "buy_score":      round(result.get("total_score", 0) or 0, 1),
            "sell_score":     round(result.get("total_sell_score", 0) or 0, 1),
            "bullish":        summary.get("top_bullish", [])[:3],
            "bearish":        summary.get("top_bearish", [])[:3],
            # backtesting context
            "industry":       basic.get("industry", "Unknown"),
            "market_cap_b":   basic.get("market_cap_billion"),
            "pe_ttm":         val.get("pe_ttm"),
            "pb":             val.get("pb"),
            "turnover_rate":  price_d.get("turnover_rate"),
            "volume_ratio":   price_d.get("volume_ratio"),
            "volume_million": price_d.get("volume_million"),
            "factor_scores":  _compact_factor_scores(result.get("factors") or {}),
            "error":          None,
        }
    except Exception as e:
        return {"code": code, "name": code, "price": None, "change_pct": None,
                "buy_score": 0.0, "sell_score": 0.0, "bullish": [], "bearish": [],
                "error": str(e)}


# ── Signal evaluation ──────────────────────────────────────────────────────────

def check_sell_signals(scored: dict, thresholds: dict,
                       holding: Optional[dict] = None) -> list[str]:
    """Return list of human-readable sell reasons, empty if no trigger.

    If `holding` is provided and the position is T+1-locked (bought today, non-ETF),
    reasons are prefixed with a T+1 warning so the user knows they can't act today.
    """
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

    # Soft sell: momentum stalling regardless of P&L.
    # Fires when sell_score is in the [stall_threshold, sell_trigger) range —
    # i.e., model sees early weakness but hasn't hit the full sell trigger yet.
    # No profit/loss condition: a position showing weakness should be trimmed
    # whether it's up 30% or down 10%.
    stall_score = thresholds.get("stall_sell_score", 40)
    if stall_score <= scored.get("sell_score", 0) < sell_trigger:
        reasons.append(
            f"逢高减仓参考: 卖出信号 **{scored['sell_score']:.0f}** 显示动能减弱"
            f"（阈值 {stall_score}–{sell_trigger}）"
        )

    if reasons and holding and _is_t1_locked(holding):
        reasons = [f"⚠️[T+1今日买入/明日可操作] {r}" for r in reasons]

    return reasons


def check_buy_signal(scored: dict, thresholds: dict, held_codes: set) -> bool:
    buy_trigger  = thresholds.get("buy_score_trigger", 65)
    sell_trigger = thresholds.get("sell_score_trigger", 60)
    bear_sell_cap = thresholds.get("_bear_sell_cap")   # set in bear regime only
    if scored["code"] in held_codes:
        return False
    if scored["buy_score"] < buy_trigger:
        return False
    if scored["sell_score"] >= sell_trigger * 0.7:
        return False
    # Bear regime: additionally require sell_score below cap (rule out falling knives)
    if bear_sell_cap is not None and scored["sell_score"] >= bear_sell_cap:
        return False
    return True


# ── Markdown formatting (Server酱 / WeChat) ────────────────────────────────────

def _append_signals_log(buy_alerts: list[dict], sell_alerts: list[dict],
                         run_time: str, regime_score: Optional[float] = None,
                         source: str = "monitor") -> None:
    """Append one run's buy + sell signals to signals_log.json for backtesting.

    Both buy and sell entries carry the same rich fields so post-mortem analysis
    is symmetric: signal_price, scores, intraday change, top bullish/bearish
    factors, and market regime at the time of the signal.
    Never touches holdings.json.
    """
    if not buy_alerts and not sell_alerts:
        return

    def _common(s: dict) -> dict:
        return {
            "code":           s["code"],
            "name":           s.get("name", s["code"]),
            "signal_price":   s.get("price"),
            "change_pct":     s.get("change_pct"),
            "buy_score":      s.get("buy_score"),
            "sell_score":     s.get("sell_score"),
            "bullish":        s.get("bullish", []),
            "bearish":        s.get("bearish", []),
            # market context for backtesting
            "industry":       s.get("industry"),
            "market_cap_b":   s.get("market_cap_b"),
            "pe_ttm":         s.get("pe_ttm"),
            "pb":             s.get("pb"),
            "turnover_rate":  s.get("turnover_rate"),
            "volume_ratio":   s.get("volume_ratio"),
            "volume_million": s.get("volume_million"),
            "factor_scores":  s.get("factor_scores"),
        }

    entry = {
        "date":         datetime.now().strftime("%Y-%m-%d"),
        "run_time":     run_time,
        "regime_score": regime_score,
        "source":       source,
        "buy_signals": [
            _common(b)
            for b in buy_alerts
        ],
        "sell_signals": [
            {
                **_common(s),
                "shares":     s.get("shares"),
                "cost_price": s.get("cost_price"),
                "pnl_pct":    s.get("pnl_pct"),
                "t1_locked":  s.get("t1_locked", False),
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

    # Write latest_picks.json so xhs/writer.py can read results without re-running screener
    if buy_alerts:
        latest = {
            "timestamp": datetime.now().isoformat(),
            "source":    source,
            "results": [
                {
                    "code":       b["code"],
                    "name":       b.get("name", b["code"]),
                    "score":      b.get("buy_score", 0),
                    "change_pct": b.get("change_pct"),
                    "buy_score":  b.get("buy_score"),
                    "sell_score": b.get("sell_score"),
                    "bullish":    b.get("bullish", []),
                    "bearish":    b.get("bearish", []),
                }
                for b in buy_alerts
            ],
            "regime": source,
        }
        with open(LATEST_PICKS_PATH, "w", encoding="utf-8") as f:
            json.dump(latest, f, ensure_ascii=False, indent=2)

    print(f"  Signals logged → signals_log.json "
          f"(buy={len(buy_alerts)}, sell={len(sell_alerts)})")


def _fmt_sell_section_md(sell_alerts: list[dict], stop_loss_pct: float = -8.0,
                          sell_trigger: float = 60, stall_score: float = 40) -> str:
    if not sell_alerts:
        return "无卖出信号触发。\n"
    lines = []
    for s in sell_alerts:
        pnl_sign = "🔴" if s["pnl_pct"] < 0 else "🟢"
        cost = s.get("cost_price") or 0
        stop_price = round(cost * (1 + stop_loss_pct / 100), 2) if cost > 0 else "—"
        lines.append(
            f"### ⚠️ {s['name']} ({s['code']})\n"
            f"现价 **{s['price']}** | 成本 {cost} | "
            f"浮盈 {pnl_sign} **{s['pnl_pct']:+.1f}%**  \n"
            f"卖出评分: **{s['sell_score']:.0f}/100** | 买入评分: {s['buy_score']:.0f}/100  \n"
            f"参考止损价: **{stop_price}**（成本 × {1 + stop_loss_pct/100:.2f}）  \n"
            f"*{_sell_position_hint(s['sell_score'], sell_trigger, stall_score)}*\n"
        )
        for r in s["reasons"]:
            lines.append(f"- {r}")
        lines.append("")
    return "\n".join(lines)


def _buy_position_hint(score: float) -> str:
    """Rough position-size suggestion based on buy score."""
    if score >= 85:
        return "建议仓位参考 8–12%"
    if score >= 75:
        return "建议仓位参考 5–8%"
    return "建议仓位参考 3–5%（轻仓试探）"


def _sell_position_hint(sell_score: float, sell_trigger: float = 60,
                        stall_score: float = 40) -> str:
    """Rough trim-size suggestion based on sell score tier."""
    if sell_score >= sell_trigger:
        return "建议减仓参考 50–100%（强卖出信号）"
    return "建议减仓参考 30–50%（动能减弱，逢高减仓）"


def _fmt_buy_section_md(buy_alerts: list[dict]) -> str:
    if not buy_alerts:
        return "无新买入机会。\n"
    lines = []
    for b in buy_alerts:
        price = b.get("price") or 0
        entry_lo = round(price * 0.998, 2)
        entry_hi = round(price * 1.003, 2)
        lines.append(
            f"### ✅ {b['name']} ({b['code']})\n"
            f"现价 **{price}** | 参考买入区间: **{entry_lo} ~ {entry_hi}**  \n"
            f"买入评分: **{b['buy_score']:.0f}/100** | 卖出评分: {b['sell_score']:.0f}/100  \n"
            f"*{_buy_position_hint(b['buy_score'])}*\n"
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
    stop_loss_pct: float = -8.0,
    sell_trigger: float = 60,
    stall_score: float = 40,
) -> str:
    parts = [f"*{run_time}*\n"]
    parts.append("## 卖出信号\n")
    parts.append(_fmt_sell_section_md(sell_alerts, stop_loss_pct=stop_loss_pct,
                                      sell_trigger=sell_trigger, stall_score=stall_score))
    parts.append("## 买入机会（未持仓）\n")
    parts.append(_fmt_buy_section_md(buy_alerts))
    if scored_holdings:
        parts.append("## 当前持仓\n")
        parts.append(_fmt_holdings_table_md(scored_holdings))
    parts.append("\n\n> 仅供参考，不构成投资建议")
    return "\n".join(parts)


# ── Trading session scan windows ───────────────────────────────────────────────
_SCAN_WINDOWS = [
    ("morning",   (9, 25), (9, 55)),
    ("midday",    (11, 45), (12, 5)),
    ("afternoon", (14, 30), (15, 0)),
]


def _current_scan_session(now: datetime) -> Optional[str]:
    """Return session key if now falls inside a universe-scan window, else None."""
    hm = (now.hour, now.minute)
    for key, start, end in _SCAN_WINDOWS:
        if start <= hm <= end:
            return key
    return None


# ── Fast path (realtime quotes only) ───────────────────────────────────────────

def fast_check_holdings(
    holdings: list[dict],
    thresholds: dict,
    alert_state: dict,                          # 急涨 / 做T: normal cooldown
    t_trade_state: Optional[dict] = None,       # {code: {sell_price, cover_price, date}}
    urgent_alert_state: Optional[dict] = None,  # 止损 / 急跌: separate shorter cooldown
) -> list[dict]:
    """
    Quick scan using only realtime quotes.  Returns list of alert dicts with:
      code, name, price, pnl_pct, change_pct, reasons

    Two independent cooldown buckets:
      urgent_alert_state — 止损 / 日内急跌: uses `fast_urgent_cooldown_min` (default 15 min).
        These are risk-critical and must never be blocked by a prior 做T alert.
      alert_state        — 日内急涨 / 做T suggestions: uses `fast_alert_cooldown_min` (default 30 min).

    做T logic (opt-in per holding via t_trade_enabled=True):
      Phase 1 — Sell high: when intraday change_pct ≥ t_trade_sell_pct, suggest
        selling a tranche at current price and set a target buy-back price.
      Phase 2 — Buy back: when price drops back to ≤ cover_price, suggest covering.
      Requires pre-existing shares (use holdings with shares bought before today).
    """
    cooldown_min        = thresholds.get("fast_alert_cooldown_min", 30)
    urgent_cooldown_min = thresholds.get("fast_urgent_cooldown_min", 15)
    stop_loss           = thresholds.get("stop_loss_pct", -8.0)
    intraday_drop_trigger  = thresholds.get("intraday_drop_trigger_pct", -5.0)
    intraday_surge_trigger = thresholds.get("intraday_surge_trigger_pct", 7.0)
    t_sell_trigger = thresholds.get("t_trade_sell_pct", 3.0)
    t_cover_pct    = thresholds.get("t_trade_cover_pct", 1.5)  # drop % from T-sell price

    if urgent_alert_state is None:
        urgent_alert_state = {}  # fallback: won't persist across calls, but won't crash

    alerts = []
    now = datetime.now()

    for h in holdings:
        if not h.get("shares", 0):   # skip zero-share / watchlist entries
            continue
        code = h["code"]
        quote = fetcher.get_realtime_quote(code)
        if not quote or "error" in quote:
            continue

        price = quote.get("price") or 0
        cost  = h.get("cost_price", 0) or 0
        pnl_pct    = ((price - cost) / cost * 100) if cost > 0 else 0.0
        change_pct = quote.get("change_pct") or 0.0

        # ── Urgent reasons (止损 + 急跌) — own cooldown bucket ─────────────────
        urgent_reasons = []
        if pnl_pct <= stop_loss:
            urgent_reasons.append(f"止损触发: 浮亏 {pnl_pct:+.1f}%")
        if change_pct <= intraday_drop_trigger:
            urgent_reasons.append(f"日内急跌 {change_pct:+.1f}%")

        last_urgent = urgent_alert_state.get(code)
        if last_urgent and (now - last_urgent).total_seconds() / 60 < urgent_cooldown_min:
            urgent_reasons = []   # within cooldown — suppress but don't block normal reasons
        elif urgent_reasons:
            urgent_alert_state[code] = now

        # ── Normal reasons (急涨 + 做T) — shared cooldown bucket ───────────────
        normal_reasons = []
        if change_pct >= intraday_surge_trigger:
            normal_reasons.append(f"日内急涨 {change_pct:+.1f}% — 考虑止盈")

        # 做T logic
        if t_trade_state is not None and h.get("t_trade_enabled", False):
            ts = t_trade_state.get(code, {})
            if ts.get("date") != now.date():
                ts = {}   # reset each new trading day

            if not ts and change_pct >= t_sell_trigger:
                # Phase 1: intraday high → suggest T-sell
                cover_price = round(price * (1 - t_cover_pct / 100), 2)
                normal_reasons.append(
                    f"📈 做T卖出参考: 涨幅 {change_pct:+.1f}%，"
                    f"建议目标买回价 ≤ **{cover_price}**"
                )
                t_trade_state[code] = {
                    "sell_price":  price,
                    "cover_price": cover_price,
                    "date":        now.date(),
                }
            elif ts and price <= ts["cover_price"]:
                # Phase 2: price retraced to target → suggest T-buy
                drop = (price - ts["sell_price"]) / ts["sell_price"] * 100
                normal_reasons.append(
                    f"📉 做T买回参考: 现价 **{price}** 已回落至目标价 "
                    f"{ts['cover_price']}（较卖出 {drop:+.1f}%）"
                )
                t_trade_state.pop(code, None)   # reset after cover suggestion

        last_normal = alert_state.get(code)
        if last_normal and (now - last_normal).total_seconds() / 60 < cooldown_min:
            normal_reasons = []
        elif normal_reasons:
            alert_state[code] = now

        reasons = urgent_reasons + normal_reasons
        if not reasons:
            continue

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


def build_fast_wechat_desp(fast_alerts: list[dict], run_time: str,
                           stop_loss_pct: float = -8.0) -> str:
    lines = [f"*{run_time}*\n"]
    for a in fast_alerts:
        pnl_icon = "🔴" if a["pnl_pct"] < 0 else "🟢"
        chg_icon = "📉" if a["change_pct"] < 0 else "📈"
        cost = a.get("cost_price") or 0
        stop_price = round(cost * (1 + stop_loss_pct / 100), 2) if cost > 0 else "—"
        lines.append(
            f"### ⚡ {a['name']} ({a['code']})\n"
            f"现价 **{a['price']}** | "
            f"今日 {chg_icon} **{a['change_pct']:+.1f}%** | "
            f"浮盈 {pnl_icon} **{a['pnl_pct']:+.1f}%**  \n"
            f"参考止损价: **{stop_price}**\n"
        )
        for r in a["reasons"]:
            lines.append(f"- {r}")
        lines.append("")
    lines.append("\n> 仅供参考，不构成投资建议")
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def run(
    dry_run: bool = False,
    buy_only: bool = False,
    sell_only: bool = False,
    always_send: bool = False,
    universe_override: Optional[list] = None,      # pass watchlist to skip full screener_universe
    sell_alert_state: Optional[dict] = None,        # cross-tier sell dedup {code: last_datetime}
    _regime: Optional[tuple] = None,               # (score, signal) pre-fetched by run_loop
) -> None:
    config     = load_config()
    holdings   = load_holdings()
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")
    universe   = universe_override if universe_override is not None else config.get("screener_universe", [])
    run_time   = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(f"[{run_time}] StockSage Monitor starting...")

    # ── 0. Market regime ──────────────────────────────────────────────────────
    if _regime is not None:
        regime_score, regime_signal = _regime
    else:
        regime_score  = 5.0   # neutral fallback
        regime_signal = "unknown"
        try:
            mkt = score_market_regime(fetcher.get_market_regime_data())
            if mkt:
                regime_score  = mkt.get("score", 5.0)
                regime_signal = mkt.get("details", {}).get("signal", "unknown")
        except Exception as e:
            print(f"  [WARN] Regime fetch failed: {e}")
    print(f"  Market regime: {regime_score}/10 — {regime_signal}")

    # ── 1. Score holdings (sell signal check) ─────────────────────────────────
    scored_holdings: list[dict] = []
    if not buy_only and holdings:
        print(f"  Scoring {len(holdings)} holdings...")
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(_score_one, h): h for h in holdings}
            for fut in as_completed(futures):
                scored_holdings.append(fut.result())
        scored_holdings.sort(key=lambda x: -x["sell_score"])
        _backfill_holding_names(scored_holdings)

    # Build a lookup from code -> original holding dict (for T+1 check)
    holding_by_code = {h["code"]: h for h in holdings}

    sell_cooldown_min = thresholds.get("sell_alert_cooldown_min", 90)
    now_dt = datetime.now()

    sell_alerts = []
    if not buy_only:
        for s in scored_holdings:
            if s["error"]:
                print(f"  [WARN] {s['code']}: {s['error']}")
                continue
            holding = holding_by_code.get(s["code"])
            reasons = check_sell_signals(s, thresholds, holding)
            if not reasons:
                continue
            # Cross-tier dedup: skip if same stock already alerted recently
            if sell_alert_state is not None:
                last = sell_alert_state.get(s["code"])
                if last and (now_dt - last).total_seconds() / 60 < sell_cooldown_min:
                    continue
                sell_alert_state[s["code"]] = now_dt
            sell_alerts.append({**s, "reasons": reasons,
                                "t1_locked": _is_t1_locked(holding) if holding else False})
            print(f"  SELL SIGNAL: {s['name']} ({s['code']}) — {reasons[0]}")

    # ── 2. Score universe (buy signal check) ──────────────────────────────────
    buy_alerts = []
    if not sell_only and universe:
        # Regime gate: raise bar proportionally to market weakness.
        # Bear market is NOT suppressed — oversold bounces are valid bottom-fishing ops.
        # Instead, require higher conviction (score) + low sell signal (not a falling knife).
        _orig_buy = thresholds.get("buy_score_trigger", 65)
        if regime_score <= 2:
            # Bear (CSI300 < MA60): +25% threshold + sell_score must be < 25
            thresholds = {**thresholds,
                          "buy_score_trigger": round(_orig_buy * 1.25, 1),
                          "_bear_sell_cap": 25}
            print(f"  [BEAR] Regime={regime_score}/10 — buy threshold raised to "
                  f"{thresholds['buy_score_trigger']}, sell_score cap=25 (bottom-fishing only)")
        elif regime_score <= 4:
            # Caution (CSI300 < MA20): +15% threshold, no sell_score cap
            thresholds = {**thresholds, "buy_score_trigger": round(_orig_buy * 1.15, 1)}
            print(f"  [CAUTION] Regime={regime_score}/10 — buy threshold raised to "
                  f"{thresholds['buy_score_trigger']}")

    if not sell_only and universe:
        held_codes = {h["code"] for h in holdings}
        print(f"  Screening {len(universe)} stocks for buy signals...")
        # Pre-warm the realtime quote cache (no-op cache hit when called from
        # run_loop(), which pre-warms at the top of each iteration; kept here
        # as a fallback for standalone `python monitor.py` invocations).
        try:
            fetcher.get_realtime_quote("000001")
        except Exception:
            pass
        scored_universe: list[dict] = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_score_one_buy, code): code for code in universe}
            for fut in as_completed(futures):
                scored_universe.append(fut.result())
        scored_universe.sort(key=lambda x: -x["buy_score"])

        top_n = thresholds.get("buy_universe_top_n", 5)
        for s in scored_universe[:top_n * 3]:  # check top candidates
            if not check_buy_signal(s, thresholds, held_codes):
                continue
            buy_alerts.append(s)
            print(f"  BUY SIGNAL:  {s['name']} ({s['code']}) score={s['buy_score']}")
            if len(buy_alerts) >= top_n:
                break

    # ── 3. Log signals for backtesting (separate from holdings) ─────────────
    _append_signals_log(buy_alerts, sell_alerts, run_time, regime_score=regime_score)

    # ── 4. Build + send email ─────────────────────────────────────────────────
    has_signal = bool(sell_alerts or buy_alerts)
    if not has_signal and not always_send:
        print("  No signals triggered. No email sent.")
        return buy_alerts

    _sell_trigger = thresholds.get("sell_score_trigger", 60)
    _stall_score  = thresholds.get("stall_sell_score", 40)
    _STRONG_BUY   = 80   # aligns with _buy_position_hint upper tiers (75+)

    strong_sells = [s for s in sell_alerts if s["sell_score"] >= _sell_trigger]
    stall_sells  = [s for s in sell_alerts if _stall_score <= s["sell_score"] < _sell_trigger]
    strong_buys  = [b for b in buy_alerts  if b["buy_score"] >= _STRONG_BUY]
    add_buys     = [b for b in buy_alerts  if b["buy_score"] < _STRONG_BUY]

    subject_parts = []
    if strong_sells: subject_parts.append(f"🔴 {len(strong_sells)} 强卖")
    if stall_sells:  subject_parts.append(f"⚠️ {len(stall_sells)} 减仓参考")
    if strong_buys:  subject_parts.append(f"✅ {len(strong_buys)} 强买")
    if add_buys:     subject_parts.append(f"💡 {len(add_buys)} 加仓参考")
    if not subject_parts:
        subject_parts.append("持仓日报")
    title = f"[StockSage] {' | '.join(subject_parts)}"

    desp = build_wechat_desp(sell_alerts, buy_alerts, scored_holdings, run_time,
                             stop_loss_pct=thresholds.get("stop_loss_pct", -8.0),
                             sell_trigger=thresholds.get("sell_score_trigger", 60),
                             stall_score=thresholds.get("stall_sell_score", 40))

    try:
        send_wechat(title, desp, sendkey, dry_run=dry_run)
    except Exception as e:
        print(f"[ERROR] 微信推送失败: {e}")

    return buy_alerts


def run_loop(
    interval_min: int = 2,
    full_interval_min: int = 30,
    dry_run: bool = False,
) -> None:
    """
    High-frequency intraday loop.

    - Every `interval_min` minutes: fast check (realtime quotes only).
      Alerts on stop-loss / intraday drop ≥ 5% / intraday surge ≥ 7%.
    - Full factor check runs twice per day: 09:25–09:55 (morning open) and
      14:30–15:00 (afternoon close). Factor scores don't meaningfully change
      every 30 min, so fixed-interval scanning is wasteful.
    - Buy signals have a 2-hour cooldown per stock to prevent repetition.
    - Bear regime (CSI300 < MA60): buy threshold +25% and sell_score must be
      < 25 (only genuine oversold/bottom-fishing signals pass through).
    - Caution regime (CSI300 < MA20): buy threshold +15%.
    - Automatically waits when outside trading hours.
    - Every Monday pre-market: auto-refreshes screener_universe via build_universe.py.
    - Holdings are hot-reloaded if holdings.json changes (no restart needed).
    """
    config     = load_config()
    holdings   = load_holdings()
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")

    # ── Restore persisted state (survives restarts) ───────────────────────────
    _state = _load_state()

    def _restore_dt(key: str) -> Optional[datetime]:
        v = _state.get(key)
        try:
            return datetime.fromisoformat(v) if v else None
        except Exception:
            return None

    def _restore_date(key: str):
        v = _state.get(key)
        try:
            return datetime.fromisoformat(v).date() if v else None
        except Exception:
            return None

    alert_state:        dict = {}   # code -> last fast-alert datetime (急涨/做T, normal cooldown)
    urgent_alert_state: dict = {}   # code -> last fast-alert datetime (止损/急跌, short cooldown)
    sell_alert_state:   dict = {}   # code -> last sell-alert datetime (cross-tier dedup)
    t_trade_state:      dict = {}   # code -> {sell_price, cover_price, date}
    _error_notified:    dict = {}   # error_key -> last WeChat push datetime (1h rate limit)
    _xhs_triggered_today: set = set()  # slots triggered today (resets on restart)

    # Restore scanned_sessions: stored as "YYYY-MM-DD|session" strings
    _scanned_sessions: set = set()
    for _s in _state.get("scanned_sessions", []):
        _parts = _s.split("|", 1)   # maxsplit=1: never more than 2 parts
        if len(_parts) == 2:
            try:
                _scanned_sessions.add(
                    (datetime.fromisoformat(_parts[0]).date(), _parts[1])
                )
            except ValueError:
                pass   # corrupt entry — skip silently

    _heartbeat_date = _restore_date("heartbeat_date")
    _closing_date   = None   # not persisted — harmless if fired twice on restart
    last_universe_refresh_date = _restore_date("universe_refresh_date")
    _watchlist_last_scan: Optional[datetime] = _restore_dt("watchlist_last_scan")
    _premarket_scan_date: Optional[object] = _restore_date("premarket_scan_date")
    _premarket_picks: list[str] = _state.get("premarket_picks", [])
    _night_scan_date: Optional[object] = _restore_date("night_scan_date")
    _WATCHLIST_INTERVAL_MIN = 30

    # Holdings hot-reload: detect changes to holdings.json without restarting
    _holdings_mtime: float = (
        os.path.getmtime(HOLDINGS_PATH) if os.path.exists(HOLDINGS_PATH) else 0.0
    )

    _build_universe_script = os.path.join(os.path.dirname(__file__), "build_universe.py")

    # ── Startup universe check: refresh immediately if empty or stale (>7 days) ──
    _startup_age = (
        (datetime.now().date() - last_universe_refresh_date).days
        if last_universe_refresh_date else 999
    )
    if not config.get("screener_universe") or _startup_age > 7:
        print(f"[StockSage] screener_universe empty or stale ({_startup_age}d) — refreshing now...")
        try:
            result = subprocess.run(
                [sys.executable, "-X", "utf8", _build_universe_script],
                capture_output=True, text=True, encoding="utf-8", timeout=600,
            )
            if result.returncode == 0:
                config     = load_config()
                holdings   = load_holdings()
                thresholds = config.get("thresholds", {})
                last_universe_refresh_date = datetime.now().date()
                print(f"  Universe ready: {len(config.get('screener_universe', []))} stocks")
            else:
                print(f"  [WARN] build_universe failed:\n{result.stderr[:300]}")
        except Exception as e:
            print(f"  [WARN] build_universe error: {e}")

    def _notify_error(key: str, msg: str) -> None:
        """Push a WeChat error alert — rate-limited to once per hour per key."""
        last = _error_notified.get(key)
        if last and (datetime.now() - last).total_seconds() < 3600:
            return
        _error_notified[key] = datetime.now()
        try:
            send_wechat(
                f"[StockSage] ⚠️ 扫描异常",
                f"**{key}** 发生错误:\n\n```\n{msg[:400]}\n```\n\n"
                f"> {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                sendkey, dry_run=dry_run,
            )
        except Exception:
            pass  # never let error notification crash the loop

    print(f"[StockSage Loop] interval={interval_min}min  "
          f"watchlist=30min  full_scan=open+close  "
          f"buy_cooldown=120min(A股)/20min(ETF)")
    print(f"  Holdings: {[h['code'] for h in holdings]}")
    print("  Press Ctrl+C to stop.\n")

    try:
      while True:
        now = datetime.now()

        # ── Weekly universe refresh (every Monday, before market open) ───────────
        if (now.weekday() == 0  # Monday
                and last_universe_refresh_date != now.date()
                and not _is_trading_hours()):
            print(f"[{now.strftime('%H:%M')}] Monday pre-market: refreshing screener universe...")
            try:
                result = subprocess.run(
                    [sys.executable, "-X", "utf8", _build_universe_script],
                    capture_output=True, text=True, encoding="utf-8", timeout=600,
                )
                if result.returncode == 0:
                    config   = load_config()  # reload to pick up new universe
                    holdings = load_holdings()
                    thresholds = config.get("thresholds", {})
                    last_universe_refresh_date = now.date()
                    n = len(config.get('screener_universe', []))
                    print(f"  Universe refreshed: {n} stocks")
                    try:
                        send_wechat(
                            "[StockSage] 股票池已更新 🔄",
                            f"本周 screener_universe 刷新完成\n\n"
                            f"- 股票数量: **{n}** 只\n"
                            f"- 自选池: {len(config.get('watchlist', []))} 只\n\n"
                            f"> {now.strftime('%Y-%m-%d')} 周一开盘前自动刷新",
                            sendkey, dry_run=dry_run,
                        )
                    except Exception:
                        pass
                else:
                    print(f"  [WARN] build_universe failed:\n{result.stderr[:300]}")
            except Exception as e:
                print(f"  [WARN] build_universe error: {e}")

        # ── Pre-market scan (01:00 — prev-day closing data, pick candidates) ────
        if (now.weekday() < 5
                and now.hour == 1 and 0 <= now.minute < 5
                and _premarket_scan_date != now.date()):
            _premarket_scan_date = now.date()
            print(f"[{run_time}] Pre-market scan (01:00)...")
            try:
                _pm_regime = None
                try:
                    mkt = score_market_regime(fetcher.get_market_regime_data())
                    if mkt:
                        _pm_regime = (mkt.get("score", 5.0),
                                      mkt.get("details", {}).get("signal", "unknown"))
                except Exception:
                    pass
                _pm_universe = config.get("screener_universe", [])
                picked = run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                             _regime=_pm_regime, universe_override=_pm_universe)
                _premarket_picks = [b["code"] for b in (picked or [])]
                print(f"  Pre-market picks saved: {_premarket_picks}")
            except Exception as e:
                print(f"  [ERROR] Pre-market scan failed: {e}")

        # ── Night scan (22:00 — post-close, feeds xhs/writer.py night post) ─────
        if (now.weekday() < 5
                and now.hour == 22 and 0 <= now.minute < 5
                and _night_scan_date != now.date()):
            _night_scan_date = now.date()
            print(f"[{run_time}] Night scan (22:00)...")
            try:
                _nt_regime = None
                try:
                    mkt = score_market_regime(fetcher.get_market_regime_data())
                    if mkt:
                        _nt_regime = (mkt.get("score", 5.0),
                                      mkt.get("details", {}).get("signal", "unknown"))
                except Exception:
                    pass
                _nt_universe = config.get("screener_universe", [])
                run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                    _regime=_nt_regime, universe_override=_nt_universe,
                    sell_only=False)
                _trigger_xhs_post("night", dry_run)
            except Exception as e:
                print(f"  [ERROR] Night scan failed: {e}")

        # ── Daily heartbeat + cache purge (09:00, before market open) ──────────
        if (now.weekday() < 5
                and now.hour == 9 and 0 <= now.minute < 5
                and _heartbeat_date != now.date()):
            _heartbeat_date = now.date()
            # Purge expired cache entries (keeps disk usage bounded)
            try:
                deleted = cache.purge_expired()
                if deleted:
                    print(f"  [Cache] Purged {deleted} expired file(s)")
            except Exception as e:
                print(f"  [WARN] Cache purge failed: {e}")
            desp = (
                f"监控进程正常运行中\n\n"
                f"- 持仓: {len(holdings)} 只 — {[h['code'] for h in holdings]}\n"
                f"- 自选池: {len(config.get('watchlist', []))} 只\n"
                f"- ETF 监控: {len(config.get('etf_watchlist', []))} 只\n\n"
                f"> {now.strftime('%Y-%m-%d')} 开盘前自检"
            )
            try:
                send_wechat("[StockSage] 今日监控在线 ✅", desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [WARN] Heartbeat push failed: {e}")

        # ── Daily closing summary (15:05) ─────────────────────────────────────
        if (now.weekday() < 5
                and now.hour == 15 and 5 <= now.minute < 10
                and _closing_date != now.date()):
            _closing_date = now.date()
            rows = ["| 股票 | 今日涨跌 | 浮盈 |", "|------|----------|------|"]
            for h in holdings:
                try:
                    q    = fetcher.get_realtime_quote(h["code"])
                    chg  = q.get("change_pct") or 0.0
                    price = q.get("price") or 0
                    cost = h.get("cost_price") or 0
                    pnl  = ((price - cost) / cost * 100) if cost > 0 else 0.0
                    rows.append(
                        f"| {h.get('name', h['code'])} "
                        f"| {'📈' if chg >= 0 else '📉'} {chg:+.1f}% "
                        f"| {'🟢' if pnl >= 0 else '🔴'} {pnl:+.1f}% |"
                    )
                except Exception:
                    rows.append(f"| {h.get('name', h['code'])} | — | — |")
            closing_desp = (
                f"**{now.strftime('%Y-%m-%d')} 收盘快报**\n\n"
                + "\n".join(rows)
                + "\n\n> 仅供参考，不构成投资建议"
            )
            try:
                send_wechat("[StockSage] 今日收盘 📊", closing_desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [WARN] Closing summary push failed: {e}")

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

        # ── Holdings hot-reload ────────────────────────────────────────────────
        try:
            mtime = os.path.getmtime(HOLDINGS_PATH) if os.path.exists(HOLDINGS_PATH) else 0.0
            if mtime != _holdings_mtime:
                holdings = load_holdings()
                _holdings_mtime = mtime
                print(f"[{run_time}] holdings.json changed — reloaded "
                      f"({[h['code'] for h in holdings]})")
        except Exception:
            pass

        # ── Pre-warm realtime quote cache (shared by fast check + run()) ────────
        # One full-market fetch; subsequent get_realtime_quote() calls within the
        # same 30s TTL window are instant cache hits — no redundant API calls.
        try:
            fetcher.get_realtime_quote("000001")
        except Exception:
            pass

        # ── Fast check ────────────────────────────────────────────────────────
        print(f"[{run_time}] Fast check ({len(holdings)} holdings)...", end=" ", flush=True)
        fast_alerts = fast_check_holdings(
            holdings, thresholds, alert_state, t_trade_state,
            urgent_alert_state=urgent_alert_state,
        )
        print(f"{len(fast_alerts)} alert(s)")

        if fast_alerts:
            title = f"[StockSage ⚡] {len(fast_alerts)} 实时预警"
            desp  = build_fast_wechat_desp(fast_alerts, run_time,
                                           stop_loss_pct=thresholds.get("stop_loss_pct", -8.0))
            try:
                send_wechat(title, desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [ERROR] 微信推送失败: {e}")

        # ── Watchlist scan (every 30 min, medium frequency) ──────────────────
        watchlist = config.get("watchlist", [])
        need_watchlist = (
            watchlist and (
                _watchlist_last_scan is None or
                (now - _watchlist_last_scan).total_seconds() >= _WATCHLIST_INTERVAL_MIN * 60
            )
        )

        # ── Full check (session-based: morning open + afternoon close) ────────
        session_key = _current_scan_session(now)
        scan_id = (now.date(), session_key) if session_key else None
        need_full = scan_id is not None and scan_id not in _scanned_sessions

        # Fetch regime once if either scan will run this iteration
        _regime_this_iter = None
        if need_watchlist or need_full:
            try:
                mkt = score_market_regime(fetcher.get_market_regime_data())
                if mkt:
                    _regime_this_iter = (
                        mkt.get("score", 5.0),
                        mkt.get("details", {}).get("signal", "unknown"),
                    )
            except Exception:
                pass

        if need_watchlist:
            print(f"[{run_time}] Watchlist scan ({len(watchlist)} stocks)...")
            try:
                run(dry_run=dry_run, universe_override=watchlist,
                    sell_alert_state=sell_alert_state, _regime=_regime_this_iter)
                _watchlist_last_scan = now
            except Exception as e:
                print(f"  [ERROR] Watchlist scan failed: {e}")
                _notify_error("watchlist_scan", str(e))

        if need_full:
            print(f"[{run_time}] Full factor check ({session_key} session)...")

            # ── Preauction XHS trigger (fires at start of morning session) ────
            if session_key == "morning" and "preauction" not in _xhs_triggered_today:
                _xhs_triggered_today.add("preauction")
                _trigger_xhs_post("preauction", dry_run)

            try:
                _watchlist_set = set(config.get("watchlist", []))
                _screener_universe = config.get("screener_universe", [])

                # Morning open: first confirm today's 1AM pre-market picks, then
                # scan the full universe for any new signals after the auction.
                _morning_signals = []
                if (session_key == "morning"
                        and _premarket_picks
                        and _premarket_scan_date == now.date()):
                    _pm_universe = [c for c in _premarket_picks
                                    if c not in _watchlist_set]
                    print(f"  Post-auction: confirming {len(_pm_universe)} "
                          f"pre-market picks...")
                    _picked1 = run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                                   _regime=_regime_this_iter, universe_override=_pm_universe)
                    _morning_signals.extend(_picked1 or [])
                    # Then scan the rest of the universe for new signals.
                    _pm_set = set(_premarket_picks)
                    _remaining = [c for c in _screener_universe
                                  if c not in _pm_set and c not in _watchlist_set]
                    if _remaining:
                        print(f"  Post-auction: scanning {len(_remaining)} "
                              f"remaining stocks...")
                        _picked2 = run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                                       _regime=_regime_this_iter,
                                       universe_override=_remaining)
                        _morning_signals.extend(_picked2 or [])
                elif session_key == "midday":
                    _full_universe = [c for c in _screener_universe
                                      if c not in _watchlist_set]
                    picked = run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                                 _regime=_regime_this_iter, universe_override=_full_universe)
                    if picked and "midday" not in _xhs_triggered_today:
                        _xhs_triggered_today.add("midday")
                        _trigger_xhs_post("midday", dry_run)
                else:
                    _full_universe = [c for c in _screener_universe
                                      if c not in _watchlist_set]
                    run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                        _regime=_regime_this_iter, universe_override=_full_universe)
                _scanned_sessions.add(scan_id)

                # ── Post-scan XHS triggers (only if signals found) ────────────
                if (session_key == "morning"
                        and _morning_signals
                        and "morning" not in _xhs_triggered_today):
                    _xhs_triggered_today.add("morning")
                    _trigger_xhs_post("morning", dry_run)
                elif session_key == "afternoon" and "evening" not in _xhs_triggered_today:
                    _xhs_triggered_today.add("evening")
                    _trigger_xhs_post("evening", dry_run)

            except Exception as e:
                print(f"  [ERROR] Full check failed: {e}")
                _notify_error("full_scan", str(e))

        # ── Persist state (survives restarts) ────────────────────────────────
        _save_state({
            "scanned_sessions": [
                f"{d.isoformat()}|{sess}" for d, sess in _scanned_sessions
            ],
            "heartbeat_date":      _heartbeat_date.isoformat() if _heartbeat_date else None,
            "universe_refresh_date": last_universe_refresh_date.isoformat()
                                     if last_universe_refresh_date else None,
            "watchlist_last_scan": _watchlist_last_scan.isoformat()
                                   if _watchlist_last_scan else None,
            "premarket_scan_date": _premarket_scan_date.isoformat()
                                   if _premarket_scan_date else None,
            "premarket_picks":     _premarket_picks,
            "night_scan_date":     _night_scan_date.isoformat()
                                   if _night_scan_date else None,
        })

        # ── Sleep until next fast interval ────────────────────────────────────
        sleep_sec = interval_min * 60
        for _ in range(sleep_sec):
            time.sleep(1)

    except KeyboardInterrupt:
        _save_state({
            "scanned_sessions": [
                f"{d.isoformat()}|{sess}" for d, sess in _scanned_sessions
            ],
            "heartbeat_date":        _heartbeat_date.isoformat() if _heartbeat_date else None,
            "universe_refresh_date": last_universe_refresh_date.isoformat()
                                     if last_universe_refresh_date else None,
            "watchlist_last_scan":   _watchlist_last_scan.isoformat()
                                     if _watchlist_last_scan else None,
            "premarket_scan_date":   _premarket_scan_date.isoformat()
                                     if _premarket_scan_date else None,
            "premarket_picks":       _premarket_picks,
            "night_scan_date":       _night_scan_date.isoformat()
                                     if _night_scan_date else None,
        })
        print("\n[StockSage] 监控已停止（Ctrl+C）。状态已保存。")


def _trigger_xhs_post(slot: str, dry_run: bool = False) -> None:
    """Fire xhs/writer.py {slot} as a non-blocking background process."""
    print(f"[XHS] Triggering writer.py {slot}...")
    if dry_run:
        print(f"[XHS] (dry-run — skipping actual subprocess)")
        return
    try:
        writer = os.path.join(_ROOT, "xhs", "writer.py")
        subprocess.Popen(
            [sys.executable, writer, slot, "--style", "auto"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=_ROOT,
        )
    except Exception as e:
        print(f"[XHS] Failed to trigger {slot}: {e}")


def _register_scheduler_tasks(dry_run: bool = False) -> None:
    """Register Windows scheduled tasks for xhs/writer.py (wraps setup_scheduler.py logic)."""
    import subprocess as _sp
    _setup = os.path.join(_ROOT, "xhs", "setup_scheduler.py")
    if not os.path.exists(_setup):
        print("[WARN] xhs/setup_scheduler.py not found — skipping task registration")
        return
    cmd = [sys.executable, "-X", "utf8", _setup]
    if dry_run:
        cmd.append("--status")
    print("[StockSage] Registering Windows scheduled tasks...")
    result = _sp.run(cmd, encoding="utf-8", capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        stderr = result.stderr[:300]
        if "拒绝访问" in stderr or "Access is denied" in stderr or "ERROR: Access" in stderr:
            print("[WARN] 注册定时任务需要管理员权限。")
            print("       请用管理员身份运行 PowerShell，然后执行:")
            print("       python xhs/setup_scheduler.py")
        else:
            print(f"[WARN] setup_scheduler failed: {stderr}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StockSage 持仓监控 + 买卖信号邮件提醒")
    parser.add_argument("--dry-run",        action="store_true", help="只打印，不发微信")
    parser.add_argument("--buy-only",       action="store_true", help="只检查买入机会")
    parser.add_argument("--sell-only",      action="store_true", help="只检查卖出信号")
    parser.add_argument("--always-send",    action="store_true", help="无信号时也发日报")
    parser.add_argument("--loop",           action="store_true",
                        help="持续运行：交易时间每 --interval 分钟快速检查一次")
    parser.add_argument("--interval",       type=int, default=2,
                        help="快速检查间隔（分钟），默认 2")
    parser.add_argument("--full-interval",  type=int, default=30,
                        help="完整因子检查间隔（分钟），默认 30")
    parser.add_argument("--register-tasks", action="store_true",
                        help="启动前注册 Windows 定时任务（xhs/setup_scheduler.py）")
    parser.add_argument("--test-now",       action="store_true",
                        help="立即跑一轮全量扫描并推送，用于测试（不启动 loop）")
    args = parser.parse_args()

    if args.register_tasks:
        _register_scheduler_tasks(dry_run=args.dry_run)

    if args.test_now:
        print("[StockSage] --test-now: 立即跑一轮全量扫描...")
        run(dry_run=args.dry_run, always_send=True)
    elif args.loop:
        run_loop(
            interval_min=args.interval,
            full_interval_min=args.full_interval,
            dry_run=args.dry_run,
        )
    else:
        run(dry_run=args.dry_run, buy_only=args.buy_only,
            sell_only=args.sell_only, always_send=args.always_send)
