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
import threading
import time
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from research import research
from factors_extended import score_market_regime
from factors import DEFAULT_WEIGHTS, weights_from_config_dict
from factor_config import REGIME_WEIGHTS, SMALLCAP_CONFIG, REGIME_WEIGHTS_SMALLCAP, FACTOR_WEIGHTS_ETF
import fetcher
import strategy_tracker
import cache
from common import (
    is_trading_hours  as _is_trading_hours,
    next_session_seconds as _next_session_seconds,
    send_wechat,
    configure_pushplus,
    is_etf   as _is_etf,
    is_t1_locked as _is_t1_locked_common,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOLDINGS_PATH = os.path.join(_ROOT, "holdings.json")
CONFIG_PATH   = os.path.join(_ROOT, "alert_config.json")
SIGNALS_LOG_PATH  = os.path.join(_ROOT, "data", "signals_log.json")
LATEST_PICKS_PATH = os.path.join(_ROOT, "data", "latest_picks.json")
SCAN_CACHE_PATH   = os.path.join(_ROOT, "data", "watchlist_scan_latest.json")
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


def _score_one(holding: dict, weights=None) -> dict:
    """Research a single holding; return enriched dict with scores and signals."""
    code = holding["code"]
    try:
        result = research(code, weights or DEFAULT_WEIGHTS)
        if result.get("error"):
            return {
                "code": code, "name": holding.get("name", code),
                "shares": holding.get("shares", 0),
                "cost_price": holding.get("cost_price", 0),
                "price": None, "pnl_pct": 0.0,
                "buy_score": 0.0, "sell_score": 0.0,
                "bearish": [], "bullish": [],
                "error": result["error"],
            }
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


def _score_one_buy(code: str, weights=None) -> dict:
    """Research a universe stock for buy screening."""
    try:
        result = research(code, weights or DEFAULT_WEIGHTS)
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

    if scored["sell_score"] >= sell_trigger:
        reasons.append(f"综合卖出评分 {scored['sell_score']:.0f}/100 ≥ {sell_trigger}")

    for bf in scored.get("bearish", []):
        factor = bf.get("factor", "")
        sig    = bf.get("signal", "")
        ss     = bf.get("sell_score", 0)
        if ss >= 20:   # raised from 7 — filter out low-conviction single-factor noise
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
    if scored["sell_score"] >= sell_trigger * 0.85:
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
        lines.append(
            f"### ⚠️ {s['name']} ({s['code']})\n"
            f"现价 **{s['price']}** | 成本 {cost} | "
            f"浮盈 {pnl_sign} **{s['pnl_pct']:+.1f}%**  \n"
            f"卖出评分: **{s['sell_score']:.0f}/100** | 买入评分: {s['buy_score']:.0f}/100  \n"
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


def _fmt_holdings_table_md(scored_holdings: list[dict],
                           sell_trigger: float = 60,
                           buy_trigger: float = 65) -> str:
    """Compact inline format: 3 holdings per row, flagged with ✅/🔴 when threshold crossed."""
    if not scored_holdings:
        return ""
    chips = []
    for s in scored_holdings:
        name = s["name"]
        sell = s["sell_score"]
        buy  = s["buy_score"]
        flag = ""
        if sell >= sell_trigger:
            flag = "🔴"
        elif buy >= buy_trigger:
            flag = "✅"
        chips.append(f"【{name}】卖{sell:.0f}买{buy:.0f}{flag}")
    # Group 3 per row
    rows = []
    for i in range(0, len(chips), 3):
        rows.append("  ".join(chips[i:i+3]))
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
        parts.append(_fmt_holdings_table_md(scored_holdings,
                                            sell_trigger=sell_trigger,
                                            buy_trigger=65))
    parts.append("\n\n> 仅供参考，不构成投资建议")
    return "\n".join(parts)


# ── Trading session scan windows ───────────────────────────────────────────────
_SCAN_WINDOWS = [
    ("morning",   (9, 25), (9, 55)),
    ("midday",    (11, 25), (11, 50)),   # trigger at 11:30
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

        # ── Urgent reasons (急跌) — own cooldown bucket ───────────────────────
        urgent_reasons = []
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
                t_trade_state.pop(code, None)  # evict stale entry so it doesn't accumulate
                ts = {}

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


# ── Opening auction quality check ─────────────────────────────────────────────

def _check_opening_auction(
    holdings: list[dict],
    pre_picks: list[str],
    watchlist: list[str],
    sendkey: str,
    dry_run: bool = False,
    weights=None,
    thresholds: Optional[dict] = None,
) -> dict:
    """
    竞价质量检验 — 在每日 9:25 集合竞价结束后运行一次。

    对持仓、昨夜预选股、自选池三类股票分别计算：
      gap_pct   = (今日开盘 - 昨收) / 昨收 × 100
      ATR_pct   = 近20日 (最高-最低) 均值 / 昨收 × 100
      norm_gap  = gap_pct / ATR_pct   （以 ATR 为单位的开口幅度）

    判断标准：
      norm_gap ≥ 2.5×  → 大幅高开，追入成本已高，风险收益比变差
      norm_gap ≥ 1.5×  → 高开，注意成本抬升
      norm_gap ≤ -2.5× → 大幅低开，与做多信号相悖，建议观望
      norm_gap ≤ -1.5× → 低开，谨慎

    Returns: {code: {"text": str, "tag": str, "gap_pct": float, "norm_gap": float}}
    """
    hold_codes = [h["code"] for h in holdings if h.get("shares", 0) > 0]
    all_codes  = list(dict.fromkeys(hold_codes + list(pre_picks) + list(watchlist)))
    if not all_codes:
        return {}

    print(f"  [竞价检验] Checking {len(all_codes)} stocks...")

    def _one(code: str) -> Optional[dict]:
        try:
            quote = fetcher.get_realtime_quote(code) or {}
            if "error" in quote:
                return None
            open_p   = quote.get("open", 0) or 0
            prev_cls = quote.get("prev_close", 0) or 0
            if open_p <= 0 or prev_cls <= 0:
                return None
            gap_pct = (open_p - prev_cls) / prev_cls * 100

            atr_pct = None
            ph = fetcher.get_price_history(code, 30)
            if ph is not None and len(ph) >= 10 and "high" in ph.columns and "low" in ph.columns:
                atr_abs = (pd.to_numeric(ph["high"], errors="coerce") -
                           pd.to_numeric(ph["low"],  errors="coerce")).tail(20).mean()
                if atr_abs > 0:
                    atr_pct = float(atr_abs / prev_cls * 100)

            norm_gap = round(gap_pct / atr_pct, 1) if atr_pct else None
            return {
                "code":      code,
                "name":      quote.get("name", code),
                "open":      open_p,
                "prev_close": prev_cls,
                "gap_pct":   round(gap_pct, 2),
                "atr_pct":   round(atr_pct, 2) if atr_pct else None,
                "norm_gap":  norm_gap,
            }
        except Exception:
            return None

    # Run gap calculation + factor scoring in parallel
    _w = weights or DEFAULT_WEIGHTS

    def _score(code: str) -> Optional[dict]:
        try:
            return research(code, _w)
        except Exception:
            return None

    raw:    dict[str, dict] = {}
    scores: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        gap_futs   = {ex.submit(_one,   c): ("gap",   c) for c in all_codes}
        score_futs = {ex.submit(_score, c): ("score", c) for c in all_codes}
        for fut in as_completed({**gap_futs, **score_futs}):
            role, code = (gap_futs if fut in gap_futs else score_futs)[fut]
            r = fut.result()
            if r:
                if role == "gap":
                    raw[code] = r
                else:
                    scores[code] = r

    # Build verdict per code
    verdicts: dict[str, dict] = {}
    for code, r in raw.items():
        ng = r.get("norm_gap")       # may be None if ATR unavailable
        gp = r.get("gap_pct", 0)
        if ng is None:
            tag  = "normal"
            text = f"{'高开' if gp > 0 else ('低开' if gp < 0 else '平开')} {gp:+.1f}%（ATR不可用）"
        elif ng >= 2.5:
            tag  = "big_gap_up"
            text = f"大幅高开 {gp:+.1f}%（{ng}×ATR）⚠️ 追高风险"
        elif ng >= 1.5:
            tag  = "gap_up"
            text = f"高开 {gp:+.1f}%（{ng}×ATR）— 注意成本抬升"
        elif ng >= 0.3:
            tag  = "small_gap_up"
            text = f"小幅高开 {gp:+.1f}%（{ng}×ATR）"
        elif ng <= -2.5:
            tag  = "big_gap_down"
            text = f"大幅低开 {gp:+.1f}%（{ng}×ATR）⚠️ 信号可能失效"
        elif ng <= -1.5:
            tag  = "gap_down"
            text = f"低开 {gp:+.1f}%（{ng}×ATR）— 谨慎"
        elif ng <= -0.3:
            tag  = "small_gap_down"
            text = f"小幅低开 {gp:+.1f}%（{ng}×ATR）"
        else:
            tag  = "normal"
            text = f"平开 {gp:+.1f}%（{ng}×ATR）"
        verdicts[code] = {"text": text, "tag": tag, "gap_pct": gp, "norm_gap": ng}

    # ── Format WeChat message ──────────────────────────────────────────────────
    _thr        = thresholds or {}
    _buy_thr    = _thr.get("buy_score",  60.0)
    _sell_thr   = _thr.get("sell_score", 60.0)

    def _icon(tag: str) -> str:
        return {"big_gap_up": "⚠️", "gap_up": "📈", "small_gap_up": "📈",
                "big_gap_down": "⚠️", "gap_down": "📉", "small_gap_down": "📉"}.get(tag, "➡️")

    def _score_line(code: str) -> str:
        """Return score-based recommendation string."""
        s = scores.get(code)
        if not s or s.get("error"):
            return "评分不可用"
        bs = s.get("buy_score",  0)
        ss = s.get("sell_score", 0)
        if ss >= _sell_thr:
            return f"卖出信号 (买:{bs:.0f} 卖:{ss:.0f})"
        if bs >= _buy_thr:
            return f"买入信号 (买:{bs:.0f} 卖:{ss:.0f})"
        return f"中性观察 (买:{bs:.0f} 卖:{ss:.0f})"

    def _fmt_group(title: str, codes_list: list[str]) -> str:
        rows = []
        for c in codes_list:
            if c not in verdicts:
                continue
            v = verdicts[c]
            name = raw[c]["name"] if c in raw else c
            rows.append((v["gap_pct"], c,
                         f"- {_icon(v['tag'])} **{name}** ({c}): "
                         f"{v['text']}  |  {_score_line(c)}"))
        if not rows:
            return ""
        rows.sort(key=lambda x: x[0], reverse=True)
        return "## " + title + "\n" + "\n".join(r[2] for r in rows)

    sections = []
    g1 = _fmt_group("持仓开盘情况", hold_codes)
    if g1:
        sections.append(g1)

    pick_extra = [c for c in pre_picks if c not in set(hold_codes)]
    g2 = _fmt_group("预选股（夜间+凌晨）", pick_extra)
    if g2:
        sections.append(g2)

    wl_extra = [c for c in watchlist
                if c not in set(hold_codes) and c not in set(pre_picks)][:10]
    g3 = _fmt_group("热榜自选池", wl_extra)
    if g3:
        sections.append(g3)

    if not sections:
        print("  [竞价检验] 无数据可推送，跳过")
        return verdicts

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    desp = f"*{now_str}*\n\n" + "\n\n".join(sections) + "\n\n> 仅供参考，不构成投资建议"
    try:
        send_wechat("[StockSage] 开盘竞价简报 🔔", desp, sendkey, dry_run=dry_run)
        print(f"  [竞价检验] 推送完成（{len(verdicts)} 只）")
    except Exception as e:
        print(f"  [竞价检验] 推送失败: {e}")

    # Write auction check results to file for writer.py preauction slot
    try:
        _auction_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                     "data", "auction_check_latest.json")
        os.makedirs(os.path.dirname(_auction_path), exist_ok=True)
        with open(_auction_path, "w", encoding="utf-8") as _f:
            json.dump({
                "date":     datetime.now().strftime("%Y-%m-%d"),
                "time":     datetime.now().strftime("%H:%M"),
                "verdicts": verdicts,
                "scores":   {c: {"buy_score":  s.get("buy_score"),
                                 "sell_score": s.get("sell_score"),
                                 "name":       s.get("name", c)}
                             for c, s in scores.items() if not s.get("error")},
                "raw":      {c: {"name": r.get("name", c),
                                 "gap_pct": r.get("gap_pct"),
                                 "open":    r.get("open")}
                             for c, r in raw.items()},
                "groups":   {"holdings": hold_codes,
                             "picks":    pick_extra,
                             "watchlist": wl_extra},
            }, _f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return verdicts


# ── Small-cap scan ─────────────────────────────────────────────────────────────

def _run_smallcap_scan(
    held_codes: set,
    config: dict,
    score_fn,
    thresholds: dict,
) -> list[dict]:
    """Scan the full market for small-cap buy signals.

    Steps:
      1. Pull EM full-market snapshot (cached — no extra request during run_loop).
      2. Filter: market_cap < max_cap_yi, not ST/退, not suspended.
      3. Pre-filter top prefilter_n by turnover_rate (active small caps).
      4. Full research via score_fn (same as universe scan).
      5. Return top top_n meeting buy_score_trigger.
    """
    sc_cfg = {**SMALLCAP_CONFIG, **config.get("smallcap", {})}
    max_cap   = sc_cfg["max_cap_yi"] * 1e8
    prefilt_n = sc_cfg["prefilter_n"]
    top_n     = sc_cfg["top_n"]
    buy_trig  = thresholds.get("buy_score_trigger", 60)

    spot_df = fetcher._get_spot_df()
    if spot_df is None or spot_df.empty:
        return []
    if "名称" not in spot_df.columns or "总市值" not in spot_df.columns or "代码" not in spot_df.columns:
        return []

    try:
        suspended = fetcher.get_suspended_codes()
    except Exception:
        suspended = set()

    df = spot_df[~spot_df["名称"].str.contains("ST|退", na=False)].copy()
    df = df[~df["代码"].isin(suspended)]
    mktcap = pd.to_numeric(df["总市值"], errors="coerce")
    df = df[(mktcap > 0) & (mktcap <= max_cap)].copy()

    if df.empty:
        return []

    if "换手率" in df.columns:
        df["_tr"] = pd.to_numeric(df["换手率"], errors="coerce").fillna(0)
        df = df.nlargest(prefilt_n, "_tr")
    else:
        df = df.head(prefilt_n)

    candidates = df["代码"].tolist()
    print(f"  [小市值] Scanning {len(candidates)} candidates (cap ≤ {sc_cfg['max_cap_yi']}亿)...")

    scored: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(score_fn, code): code for code in candidates}
        for fut in as_completed(futures):
            scored.append(fut.result())
    scored.sort(key=lambda x: -x.get("buy_score", 0))

    results: list[dict] = []
    for s in scored:
        if s.get("error") or not s.get("buy_score"):
            continue
        if s["code"] in held_codes:
            continue
        s["_sc_signal"] = s["buy_score"] >= buy_trig   # flag threshold-crossers
        results.append(s)
        if len(results) >= top_n:
            break
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def run(
    dry_run: bool = False,
    buy_only: bool = False,
    sell_only: bool = False,
    always_send: bool = False,
    universe_override: Optional[list] = None,      # pass watchlist to skip full screener_universe
    sell_alert_state: Optional[dict] = None,        # cross-tier sell dedup {code: last_datetime}
    _regime: Optional[tuple] = None,               # (score, signal) pre-fetched by run_loop
    etf_scores: Optional[list] = None,             # top ETF scores to merge into push
) -> None:
    config     = load_config()
    holdings   = load_holdings()
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(config.get("pushplus", {}).get("token", ""))
    always_send = always_send or config.get("always_send", False)
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

    # Select regime-appropriate factor weights for scoring
    if regime_score <= 2:
        _regime_key = "BEAR"
    elif regime_score <= 4:
        _regime_key = "CAUTION"
    elif regime_score >= 7:
        _regime_key = "BULL"
    else:
        _regime_key = "NORMAL"
    _fw = weights_from_config_dict(REGIME_WEIGHTS[_regime_key])
    if _regime_key != "NORMAL":
        print(f"  Using {_regime_key} factor weights for scoring")

    from functools import partial as _partial
    _score_one_h     = _partial(_score_one,     weights=_fw)
    _score_one_buy_w = _partial(_score_one_buy, weights=_fw)

    scored_universe: list[dict] = []   # populated in step 2 if universe scan runs

    # ── 1. Score holdings (sell signal check) ─────────────────────────────────
    scored_holdings: list[dict] = []
    if not buy_only and holdings:
        print(f"  Scoring {len(holdings)} holdings...")
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(_score_one_h, h): h for h in holdings}
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

    held_codes: set = {h["code"] for h in holdings}

    if not sell_only and universe:
        print(f"  Screening {len(universe)} stocks for buy signals...")
        # Pre-warm the realtime quote cache (no-op cache hit when called from
        # run_loop(), which pre-warms at the top of each iteration; kept here
        # as a fallback for standalone `python monitor.py` invocations).
        try:
            fetcher.get_realtime_quote("000001")
        except Exception:
            pass
        # Filter suspended stocks before scoring
        try:
            suspended = fetcher.get_suspended_codes()
            if suspended:
                universe_filtered = [c for c in universe if c not in suspended]
                if len(universe_filtered) < len(universe):
                    print(f"  [Suspension] Filtered {len(universe)-len(universe_filtered)} suspended stock(s)")
                universe = universe_filtered
        except Exception:
            pass

        scored_universe: list[dict] = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_score_one_buy_w, code): code for code in universe}
            for fut in as_completed(futures):
                scored_universe.append(fut.result())
        scored_universe.sort(key=lambda x: -x["buy_score"])

        # Write scan cache for signal_tracker.py (watchlist scans only)
        if universe_override is not None:
            try:
                _data = {
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "scored": [
                        {k: s.get(k) for k in
                         ("code","name","buy_score","sell_score","price","bullish","bearish")}
                        for s in scored_universe if not s.get("error")
                    ],
                }
                os.makedirs(os.path.dirname(SCAN_CACHE_PATH), exist_ok=True)
                with open(SCAN_CACHE_PATH, "w", encoding="utf-8") as _f:
                    json.dump(_data, _f, ensure_ascii=False, indent=2)
            except Exception:
                pass

        top_n = thresholds.get("buy_universe_top_n", 5)
        for s in scored_universe[:top_n * 3]:  # check top candidates
            if not check_buy_signal(s, thresholds, held_codes):
                continue
            buy_alerts.append(s)
            print(f"  BUY SIGNAL:  {s['name']} ({s['code']}) score={s['buy_score']}")
            if len(buy_alerts) >= top_n:
                break

    # ── 3. Small-cap strategy scan ───────────────────────────────────────────
    smallcap_candidates: list[dict] = []   # top N regardless of threshold
    smallcap_alerts:    list[dict] = []   # subset that crossed buy threshold (for has_signal)
    smallcap_enabled = config.get("smallcap", {}).get("enabled", True)
    if not sell_only and smallcap_enabled:
        try:
            _sc_weights = weights_from_config_dict(REGIME_WEIGHTS_SMALLCAP[_regime_key])
            _score_sc = _partial(_score_one_buy, weights=_sc_weights)
            smallcap_candidates = _run_smallcap_scan(
                held_codes=held_codes if not sell_only else set(),
                config=config,
                score_fn=_score_sc,
                thresholds=thresholds,
            )
            smallcap_alerts = [s for s in smallcap_candidates if s.get("_sc_signal")]
            for s in smallcap_alerts:
                print(f"  [小市值] BUY: {s['name']} ({s['code']}) score={s['buy_score']}")
        except Exception as _e:
            print(f"  [WARN] Small-cap scan failed: {_e}")

    # ── 4. Log signals for backtesting (separate from holdings) ─────────────
    _append_signals_log(buy_alerts, sell_alerts, run_time, regime_score=regime_score)

    # Record daily pools for strategy performance tracking
    _today = datetime.now().strftime("%Y-%m-%d")
    try:
        strategy_tracker.record_pool(_today, "lowvol", buy_alerts or scored_universe[:10])
    except Exception:
        pass
    try:
        if smallcap_candidates or smallcap_enabled:
            strategy_tracker.record_pool(_today, "smallcap", smallcap_alerts)
    except Exception:
        pass

    # ── 5. Build + send WeChat ────────────────────────────────────────────────
    has_signal = bool(sell_alerts or buy_alerts or smallcap_alerts)
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

    def _names(stocks: list, max_n: int = 3) -> str:
        """Return comma-joined names for up to max_n stocks."""
        ns = [s.get("name") or s.get("code", "") for s in stocks[:max_n]]
        suffix = "…" if len(stocks) > max_n else ""
        return "、".join(ns) + suffix

    subject_parts = []
    if strong_sells: subject_parts.append(f"🔴 {len(strong_sells)} 强卖（{_names(strong_sells)}）")
    if stall_sells:  subject_parts.append(f"⚠️ {len(stall_sells)} 减仓（{_names(stall_sells)}）")
    if strong_buys:  subject_parts.append(f"✅ {len(strong_buys)} 强买（{_names(strong_buys)}）")
    if add_buys:     subject_parts.append(f"💡 {len(add_buys)} 加仓（{_names(add_buys)}）")
    if not subject_parts:
        subject_parts.append("持仓日报")
    title = f"[StockSage] {' | '.join(subject_parts)}"

    desp = build_wechat_desp(sell_alerts, buy_alerts, scored_holdings, run_time,
                             stop_loss_pct=thresholds.get("stop_loss_pct", -8.0),
                             sell_trigger=thresholds.get("sell_score_trigger", 60),
                             stall_score=thresholds.get("stall_sell_score", 40))

    # Always append top candidates from scored_universe (watchlist or full universe)
    if scored_universe:
        top_candidates = [s for s in scored_universe[:15] if not s.get("error") and s.get("buy_score", 0) > 0][:10]
        if top_candidates:
            label = "自选池排名" if universe_override is not None else "今日关注（低波动）"
            lines = [f"## {label}\n"]
            for s in top_candidates:
                signal = " ✅" if s in buy_alerts else ""
                lines.append(f"- **{s['name']}**（{s['code']}）买入分:{s['buy_score']:.0f}{signal}")
            desp += "\n\n" + "\n".join(lines)

    # Always append small-cap top candidates (threshold-crossers marked ✅)
    if smallcap_candidates:
        sc_lines = ["## 今日关注（小市值）\n"]
        for s in smallcap_candidates:
            cap_b = s.get("market_cap_b") or ""
            cap_str = f" {cap_b:.0f}亿" if cap_b else ""
            signal_mark = " ✅" if s.get("_sc_signal") else ""
            sc_lines.append(
                f"- **{s['name']}**（{s['code']}）"
                f"买入分:{s['buy_score']:.0f}{cap_str}{signal_mark}"
            )
        desp += "\n\n" + "\n".join(sc_lines)

    # Merge ETF top scores into push when provided by run_loop
    if etf_scores:
        _sorted_etf = sorted(etf_scores, key=lambda x: x.get("buy_score", 0), reverse=True)
        etf_lines = ["## ETF 评分\n"]
        for _se in _sorted_etf[:5]:
            _p    = _se.get("price") or 0
            _pnl  = _se.get("pnl_pct", 0)
            _pnl_str = f" | 浮盈 {_pnl:+.1f}%" if _se.get("shares", 0) > 0 else ""
            etf_lines.append(
                f"- **{_se['name']}** ({_se['code']}): "
                f"买 {_se.get('buy_score', 0):.0f} / 卖 {_se.get('sell_score', 0):.0f}"
                f" | 价 {_p}{_pnl_str}"
            )
        desp += "\n\n" + "\n".join(etf_lines)

    # Append strategy performance comparison (forward returns from past pools)
    try:
        spot_df = fetcher._get_spot_df()
        if spot_df is not None and not spot_df.empty and "代码" in spot_df.columns and "最新价" in spot_df.columns:
            price_map = dict(zip(
                spot_df["代码"].astype(str),
                pd.to_numeric(spot_df["最新价"], errors="coerce"),
            ))
            perf_section = strategy_tracker.format_perf_section(price_map)
            if perf_section:
                desp += "\n\n" + perf_section
    except Exception:
        pass

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
    configure_pushplus(config.get("pushplus", {}).get("token", ""))

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
    _etf_buy_state:     dict = {}   # code -> last ETF buy-alert datetime  (20-min cooldown)
    _etf_sell_state:    dict = {}   # code -> last ETF sell-alert datetime (20-min cooldown)
    _etf_activity:      dict = {}   # code -> {"buys": int, "sells": int}  (reset daily)
    _etf_activity_date:     Optional[object] = None
    _etf_closing_date:      Optional[object] = None
    _etf_status_last_sent:  Optional[object] = None   # last periodic ETF status push
    _etf_all_scores_latest: list = []               # last ETF scan results (passed to watchlist push)
    _ETF_COOLDOWN_MIN = 20
    _ETF_STATUS_INTERVAL_MIN = 60   # periodic standalone ETF status (main push gets ETF via etf_scores)
    _xhs_triggered_today: set = set()  # slots triggered today
    _xhs_date: Optional[object] = None              # date _xhs_triggered_today belongs to
    _auction_checked_date: Optional[object] = None   # date of last 竞价检验

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
    _night_picks: list[str] = _state.get("night_picks", [])   # 22:00 buy signals → 9:25 auction check
    _WATCHLIST_INTERVAL_MIN = 5

    # Holdings hot-reload: detect changes to holdings.json without restarting
    _holdings_mtime: float = (
        os.path.getmtime(HOLDINGS_PATH) if os.path.exists(HOLDINGS_PATH) else 0.0
    )

    _build_universe_script  = os.path.join(os.path.dirname(__file__), "build_universe.py")
    _signal_tracker_script  = os.path.join(os.path.dirname(__file__), "signal_tracker.py")
    _auto_tune_script       = os.path.join(os.path.dirname(__file__), "auto_tune.py")
    _universe_refresh_done  = threading.Event()   # background thread sets this when finished
    _universe_running       = threading.Event()   # set while refresh is in progress (duplicate guard)
    _universe_proc: Optional[subprocess.Popen] = None   # track running subprocess
    _signal_tracker_date: Optional[object] = _restore_date("signal_tracker_date")
    _auto_tune_date:      Optional[object] = _restore_date("auto_tune_date")

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

        # ── Reload config after background universe refresh completes ────────────
        if _universe_refresh_done.is_set():
            _universe_refresh_done.clear()
            config     = load_config()
            holdings   = load_holdings()
            thresholds = config.get("thresholds", {})
            print(f"  [Universe] Config reloaded: "
                  f"{len(config.get('screener_universe', []))} stocks, "
                  f"{len(config.get('watchlist', []))} watchlist")

        # ── Daily reset of XHS trigger set ────────────────────────────────────────
        if _xhs_date != now.date():
            _xhs_triggered_today = set()
            _xhs_date = now.date()

        # ── Daily universe refresh (00:00, gives 9h before market open) ─────────
        # Starts at midnight so it always finishes well before 09:25 open.
        # Fallback: also runs if loop starts any time before 09:25 and hasn't run today.
        if (last_universe_refresh_date != now.date()
                and not _is_trading_hours()):
            # Mark date immediately — prevents re-triggering every loop iteration.
            # Guard: don't start if one is already running
            if _universe_running.is_set():
                print(f"  [Universe] Already in progress, skipping duplicate.")
            else:
                last_universe_refresh_date = now.date()
                _universe_running.set()
                print(f"[{now.strftime('%H:%M')}] Daily pre-market: refreshing screener universe (background)...")
                _universe_refresh_done.clear()

                def _run_universe_refresh(_sendkey=sendkey, _dry_run=dry_run):
                    nonlocal _universe_proc
                    try:
                        _universe_proc = subprocess.Popen(
                            [sys.executable, "-X", "utf8", _build_universe_script],
                        )
                        try:
                            _universe_proc.wait(timeout=300)
                        except subprocess.TimeoutExpired:
                            _universe_proc.kill()
                            print(f"  [Universe] Timed out after 300s — killed.")
                            return
                        if _universe_proc.returncode == 0:
                            cfg = load_config()
                            n   = len(cfg.get('screener_universe', []))
                            print(f"  [Universe] Refreshed: {n} stocks")
                            _universe_refresh_done.set()
                            try:
                                wl = cfg.get('watchlist', [])
                                wl_names = cfg.get('watchlist_names', {})
                                wl_lines = "\n".join(
                                    f"  - {wl_names.get(c, c)} ({c})" for c in wl
                                ) if wl else "  （空）"
                                send_wechat(
                                    "[StockSage] 股票池已更新 🔄",
                                    f"今日 screener_universe 刷新完成\n\n"
                                    f"- 候选股票: **{n}** 只（全量扫描）\n"
                                    f"- 自选池（每30分钟扫描）:\n{wl_lines}\n\n"
                                    f"> {datetime.now().strftime('%Y-%m-%d')} 每日自动刷新",
                                    _sendkey, dry_run=_dry_run,
                                )
                            except Exception:
                                pass
                        else:
                            print(f"  [Universe] build_universe failed (exit={_universe_proc.returncode})")
                    except Exception as e:
                        print(f"  [Universe] build_universe error: {e}")
                    finally:
                        _universe_running.clear()

                threading.Thread(target=_run_universe_refresh, daemon=True).start()

        # ── Evening preview scan (18:00 — picks for tomorrow, feeds XHS night post) ─
        if (now.hour == 18 and 0 <= now.minute < 5
                and _premarket_scan_date != now.date()):
            _premarket_scan_date = now.date()
            print(f"[{run_time}] Evening preview scan (18:00)...")
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
                # Write picks to file for writer.py
                _preview_path = os.path.join(_ROOT, "data", "preview_picks.json")
                os.makedirs(os.path.dirname(_preview_path), exist_ok=True)
                with open(_preview_path, "w", encoding="utf-8") as _f:
                    json.dump({
                        "date":    datetime.now().strftime("%Y-%m-%d"),
                        "time":    datetime.now().strftime("%H:%M"),
                        "regime":  _pm_regime[1] if _pm_regime else "unknown",
                        "picks":   [b for b in (picked or [])
                                    if not b.get("error")],
                    }, _f, ensure_ascii=False, indent=2)
                print(f"  Preview picks saved: {_premarket_picks}")
                _trigger_xhs_post("night", dry_run)
            except Exception as e:
                print(f"  [ERROR] Evening preview scan failed: {e}")

        # ── Night scan (22:00 — post-close signals only, no XHS) ──────────────
        if (now.hour == 22 and 0 <= now.minute < 5
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
                _nt_picked = run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                                 _regime=_nt_regime, universe_override=_nt_universe,
                                 sell_only=False)
                _night_picks = [b["code"] for b in (_nt_picked or [])]
                print(f"  Night picks saved: {_night_picks}")
                # No XHS trigger here — night post moved to 18:00 scan
            except Exception as e:
                print(f"  [ERROR] Night scan failed: {e}")

        # ── Daily heartbeat + cache purge (09:00, before market open) ──────────
        if (now.hour == 9 and 0 <= now.minute < 5  # TEST: weekday check removed
                and _heartbeat_date != now.date()):
            _heartbeat_date = now.date()
            # Purge expired cache entries (keeps disk usage bounded)
            try:
                deleted = cache.purge_expired()
                if deleted:
                    print(f"  [Cache] Purged {deleted} expired file(s)")
            except Exception as e:
                print(f"  [WARN] Cache purge failed: {e}")
            _wl       = config.get('watchlist', [])
            _wl_names = config.get('watchlist_names', {})
            _holding_lines = "、".join(
                f"{h.get('name', h['code'])}({h['code']})" for h in holdings
            ) or "（空）"
            _wl_lines = "\n".join(
                f"  - {_wl_names.get(c, c)} ({c})" for c in _wl
            ) or "  （空）"
            desp = (
                f"监控进程正常运行中\n\n"
                f"- 持仓 {len(holdings)} 只: {_holding_lines}\n\n"
                f"- 自选池 {len(_wl)} 只:\n{_wl_lines}\n\n"
                f"- ETF 监控: {len(config.get('etf_watchlist', []))} 只\n\n"
                f"> {now.strftime('%Y-%m-%d')} 开盘前自检"
            )
            try:
                send_wechat("[StockSage] 今日监控在线 ✅", desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [WARN] Heartbeat push failed: {e}")

        # ── Daily closing summary (15:05) ─────────────────────────────────────
        if (now.hour == 15 and 5 <= now.minute < 10  # TEST: weekday check removed
                and _closing_date != now.date()):
            _closing_date = now.date()
            rows = []
            no_data = []
            for h in holdings:
                try:
                    q = fetcher.get_realtime_quote(h["code"])
                    if not q or not q.get("price"):
                        no_data.append(h.get('name') or h['code'])
                        continue
                    chg  = q.get("change_pct") or 0.0
                    name = h.get('name') or q.get('name') or h['code']
                    rows.append(
                        f"- **{name}** {'📈' if chg >= 0 else '📉'} {chg:+.1f}%"
                    )
                except Exception:
                    no_data.append(h.get('name') or h['code'])
            if no_data:
                rows.append(f"\n*无数据（{len(no_data)}只）: {', '.join(no_data[:5])}{'...' if len(no_data) > 5 else ''}*")
            # ── ETF section (merged into same push) ──────────────────────────
            etf_list_cfg = config.get("etf_watchlist", [])
            _etf_closing_date = now.date()
            etf_rows = []
            etf_no_data = []
            for _etf_entry in etf_list_cfg:
                _ec = _etf_entry if isinstance(_etf_entry, str) else _etf_entry.get("code", "")
                _en = (_etf_entry.get("name", _ec) if isinstance(_etf_entry, dict) else _ec)
                try:
                    _eq = fetcher.get_realtime_quote(_ec)
                    if not _eq or not _eq.get("price"):
                        etf_no_data.append(_en)
                        continue
                    _echg = _eq.get("change_pct") or 0.0
                    _act  = _etf_activity.get(_ec, {})
                    _buys  = _act.get("buys", 0)
                    _sells = _act.get("sells", 0)
                    _sig = ""
                    if _buys:  _sig += f" 买{_buys}次"
                    if _sells: _sig += f" 卖{_sells}次"
                    etf_rows.append(
                        f"- **{_en}** {'📈' if _echg >= 0 else '📉'} {_echg:+.1f}%{_sig}"
                    )
                except Exception:
                    etf_no_data.append(_en)
            if etf_no_data:
                etf_rows.append(
                    f"\n*ETF无数据（{len(etf_no_data)}只）: "
                    f"{', '.join(etf_no_data[:5])}{'...' if len(etf_no_data) > 5 else ''}*"
                )

            etf_section = ""
            if etf_rows:
                etf_section = "\n\n**ETF 自选**\n" + "\n".join(etf_rows)

            closing_desp = (
                f"**{now.strftime('%Y-%m-%d')} 收盘快报**\n\n"
                + "\n".join(rows)
                + etf_section
                + "\n\n> 仅供参考，不构成投资建议"
            )
            try:
                send_wechat("[StockSage] 今日收盘 📊", closing_desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [WARN] Closing summary push failed: {e}")
            # XHS evening post: today's watchlist performance summary
            if "evening" not in _xhs_triggered_today:
                _xhs_triggered_today.add("evening")
                _trigger_xhs_post("evening", dry_run)

        # ── Daily signal tracker (15:20 — after closing scan) ────────────────
        if (now.hour == 15 and 20 <= now.minute < 30
                and _signal_tracker_date != now.date()):
            _signal_tracker_date = now.date()
            print(f"[{now.strftime('%H:%M')}] Running signal tracker...")
            def _run_signal_tracker():
                try:
                    result = subprocess.run(
                        [sys.executable, "-X", "utf8", _signal_tracker_script],
                        capture_output=True, text=True, encoding="utf-8", timeout=300,
                    )
                    if result.returncode == 0:
                        print(f"  [Tracker] Done.\n{result.stdout[-500:]}")
                    else:
                        print(f"  [Tracker] Failed:\n{result.stderr[:300]}")
                except Exception as e:
                    print(f"  [Tracker] Error: {e}")
            threading.Thread(target=_run_signal_tracker, daemon=True).start()

        # ── Weekly auto-tune (Monday 08:00, if enough signal data) ───────────
        if (now.weekday() == 0 and now.hour == 8 and now.minute < 5
                and _auto_tune_date != now.date()):
            _auto_tune_date = now.date()
            print(f"[{now.strftime('%H:%M')}] Running auto-tune...")
            def _run_auto_tune():
                try:
                    result = subprocess.run(
                        [sys.executable, "-X", "utf8", _auto_tune_script, "--apply"],
                        capture_output=True, text=True, encoding="utf-8", timeout=60,
                    )
                    if result.returncode == 0:
                        print(f"  [AutoTune] Done.\n{result.stdout[-500:]}")
                    else:
                        print(f"  [AutoTune] Failed:\n{result.stderr[:300]}")
                except Exception as e:
                    print(f"  [AutoTune] Error: {e}")
            threading.Thread(target=_run_auto_tune, daemon=True).start()

        # TEST: trading-hours gate bypassed so full scan runs at any time
        # if not _is_trading_hours():
        #     wait_sec = _next_session_seconds()
        #     wait_min = wait_sec // 60
        #     print(f"[{now.strftime('%H:%M')}] Outside trading hours. "
        #           f"Next session in ~{wait_min} min. Sleeping...")
        #     _deadline = time.time() + min(wait_sec, 300)
        #     while time.time() < _deadline:
        #         time.sleep(1)
        #     continue

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
        # Try East Money full-market fetch first; if it fails, pre-warm Sina
        # batch cache so all holdings get quotes in one request (avoids
        # per-stock concurrent Sina calls that trigger rate limiting).
        try:
            fetcher.get_realtime_quote("000001")
        except Exception:
            pass
        if fetcher._spot_em_failed:
            try:
                fetcher._warm_sina_cache([h["code"] for h in holdings])
            except Exception:
                pass

        # ── Fast check (trading hours only: 09:30–15:00) ─────────────────────────
        _market_open = (now.hour > 9 or (now.hour == 9 and now.minute >= 30)) and now.hour < 15
        fast_alerts: list[dict] = []
        if _market_open:
            print(f"[{run_time}] Fast check ({len(holdings)} holdings)...")
            fast_alerts = fast_check_holdings(
                holdings, thresholds, alert_state, t_trade_state,
                urgent_alert_state=urgent_alert_state,
            )
            print(f"  {len(fast_alerts)} alert(s)")
        if fast_alerts and _market_open:
            title = f"[StockSage ⚡] {len(fast_alerts)} 实时预警"
            desp  = build_fast_wechat_desp(fast_alerts, run_time,
                                           stop_loss_pct=thresholds.get("stop_loss_pct", -8.0))
            try:
                send_wechat(title, desp, sendkey, dry_run=dry_run)
            except Exception as e:
                print(f"  [ERROR] 微信推送失败: {e}")

        # ── ETF watchlist scan (every fast-check cycle, T+0 20-min cooldown) ──
        _regime_this_iter = None
        etf_list = config.get("etf_watchlist", [])
        if etf_list and _is_trading_hours():
            # Reset daily activity counter
            if _etf_activity_date != now.date():
                _etf_activity = {e["code"]: {"buys": 0, "sells": 0} for e in etf_list}
                _etf_activity_date = now.date()

            print(f"[{run_time}] ETF scan ({len(etf_list)} ETFs)...", end=" ", flush=True)
            etf_buy_alerts: list[dict] = []
            etf_sell_alerts: list[dict] = []
            etf_all_scores: list[dict] = []   # all scored ETFs for periodic status
            _etf_regime = _regime_this_iter  # reuse if already fetched, else None

            _etf_w = weights_from_config_dict(FACTOR_WEIGHTS_ETF)
            with ThreadPoolExecutor(max_workers=min(len(etf_list), 4)) as _ex:
                _futs = {_ex.submit(_score_one_buy, e["code"], _etf_w): e for e in etf_list}
                for _fut in as_completed(_futs):
                    _etf_entry = _futs[_fut]
                    _s = _fut.result()
                    # Merge shares/cost from etf_watchlist entry
                    _s["shares"]     = _etf_entry.get("shares", 0)
                    _s["cost_price"] = _etf_entry.get("cost_price", 0)
                    cost = _s["cost_price"] or 0
                    price = _s.get("price") or 0
                    _s["pnl_pct"] = round((price - cost) / cost * 100, 2) if cost > 0 else 0.0

                    etf_all_scores.append(_s)
                    code = _s["code"]
                    _etf_activity.setdefault(code, {"buys": 0, "sells": 0})
                    _t = thresholds
                    _sell_trigger = _t.get("sell_score_trigger", 60)
                    _stall        = _t.get("stall_sell_score", 40)
                    _stop_loss    = _t.get("stop_loss_pct", -8.0)

                    # ── ETF sell check (T+0: no T+1 lock, 20-min cooldown) ─────
                    _sell_reasons: list[str] = []
                    if (_s.get("shares", 0) or 0) > 0:
                        if _s["sell_score"] >= _sell_trigger:
                            _sell_reasons.append(
                                f"综合卖出评分 {_s['sell_score']:.0f}/100 ≥ {_sell_trigger}")
                        elif _stall <= _s["sell_score"] < _sell_trigger:
                            _sell_reasons.append(
                                f"逢高减仓参考: 卖出信号 **{_s['sell_score']:.0f}**"
                                f"（阈值 {_stall}–{_sell_trigger}）")
                        if _s["pnl_pct"] <= _stop_loss:
                            _sell_reasons.append(f"止损触发: 浮亏 {_s['pnl_pct']:+.1f}%")
                    _last_sell = _etf_sell_state.get(code)
                    _sell_ok = (not _last_sell or
                                (now - _last_sell).total_seconds() >= _ETF_COOLDOWN_MIN * 60)
                    if _sell_reasons and _sell_ok:
                        _etf_sell_state[code] = now
                        _etf_activity[code]["sells"] += 1
                        etf_sell_alerts.append({**_s, "reasons": _sell_reasons})

                    # ── ETF buy check ─────────────────────────────────────────
                    _rs = _etf_regime[0] if _etf_regime else 5.0
                    _buy_trigger = thresholds.get("buy_score_trigger", 65)
                    if _rs <= 2:
                        _buy_trigger = round(_buy_trigger * 1.25, 1)
                    elif _rs <= 4:
                        _buy_trigger = round(_buy_trigger * 1.15, 1)
                    _last_buy = _etf_buy_state.get(code)
                    _buy_ok = (not _last_buy or
                               (now - _last_buy).total_seconds() >= _ETF_COOLDOWN_MIN * 60)
                    if (_s["buy_score"] >= _buy_trigger and
                            _s["sell_score"] < _sell_trigger * 0.7 and
                            _buy_ok):
                        _etf_buy_state[code] = now
                        _etf_activity[code]["buys"] += 1
                        etf_buy_alerts.append(_s)

            print(f"{len(etf_buy_alerts)} buy / {len(etf_sell_alerts)} sell")
            _etf_all_scores_latest = etf_all_scores  # persist for next watchlist push

            if (etf_buy_alerts or etf_sell_alerts) and _market_open:
                _parts = []
                _sell_trig = thresholds.get("sell_score_trigger", 60)
                _stall_s   = thresholds.get("stall_sell_score", 40)
                strong_s = [a for a in etf_sell_alerts if a["sell_score"] >= _sell_trig]
                stall_s  = [a for a in etf_sell_alerts if _stall_s <= a["sell_score"] < _sell_trig]
                strong_b = [a for a in etf_buy_alerts if a["buy_score"] >= 80]
                add_b    = [a for a in etf_buy_alerts if a["buy_score"] < 80]
                def _en(lst, n=3):
                    ns = [a.get("name") or a.get("code","") for a in lst[:n]]
                    return "、".join(ns) + ("…" if len(lst) > n else "")
                if strong_s: _parts.append(f"🔴 {len(strong_s)} 强卖（{_en(strong_s)}）")
                if stall_s:  _parts.append(f"⚠️ {len(stall_s)} 减仓（{_en(stall_s)}）")
                if strong_b: _parts.append(f"✅ {len(strong_b)} 强买（{_en(strong_b)}）")
                if add_b:    _parts.append(f"💡 {len(add_b)} 加仓（{_en(add_b)}）")
                _stop = thresholds.get("stop_loss_pct", -8.0)
                _etf_lines = []
                for _a in etf_sell_alerts:
                    _etf_lines.append(
                        f"### {_a['name']} ({_a['code']})\n"
                        f"卖出评分: **{_a['sell_score']:.0f}** | 浮盈: **{_a['pnl_pct']:+.1f}%**")
                    for _r in _a["reasons"]:
                        _etf_lines.append(f"- {_r}")
                for _a in etf_buy_alerts:
                    _p = _a.get("price") or 0
                    _etf_lines.append(
                        f"### {_a['name']} ({_a['code']})\n"
                        f"买入评分: **{_a['buy_score']:.0f}** | 现价: **{_p}**")
                _etf_lines.append("\n> T+0 / 仅供参考")
                try:
                    send_wechat(
                        f"[StockSage ETF] {' | '.join(_parts)}",
                        "\n".join(_etf_lines), sendkey, dry_run=dry_run)
                except Exception as _e:
                    print(f"  [ERROR] ETF 推送失败: {_e}")

            # ── Periodic ETF status (every 30 min, even without alerts) ──────
            _need_etf_status = (
                _market_open and etf_all_scores and (
                    _etf_status_last_sent is None or
                    (now - _etf_status_last_sent).total_seconds() >= _ETF_STATUS_INTERVAL_MIN * 60
                )
            )
            if _need_etf_status:
                _sorted_buy = sorted(etf_all_scores, key=lambda x: x.get("buy_score", 0), reverse=True)
                _top_buy = _sorted_buy[:5]
                _status_lines = ["**Top ETF 买入评分**\n"]
                for _se in _top_buy:
                    _p = _se.get("price") or 0
                    _pnl = _se.get("pnl_pct", 0)
                    _pnl_str = f" | 浮盈: {_pnl:+.1f}%" if _se.get("shares", 0) > 0 else ""
                    _status_lines.append(
                        f"- {_se['name']} ({_se['code']}): "
                        f"买入 **{_se.get('buy_score', 0):.0f}** / "
                        f"卖出 {_se.get('sell_score', 0):.0f} | 价 {_p}{_pnl_str}"
                    )
                _hold_etfs = [s for s in etf_all_scores if s.get("shares", 0) > 0]
                if _hold_etfs:
                    _status_lines.append("\n**持仓 ETF**\n")
                    for _se in sorted(_hold_etfs, key=lambda x: x.get("pnl_pct", 0), reverse=True):
                        _p = _se.get("price") or 0
                        _status_lines.append(
                            f"- {_se['name']} ({_se['code']}): "
                            f"买入 {_se.get('buy_score', 0):.0f} / 卖出 {_se.get('sell_score', 0):.0f} "
                            f"| 浮盈 {_se.get('pnl_pct', 0):+.1f}%"
                        )
                _status_lines.append(f"\n> 共扫描 {len(etf_all_scores)} 只 ETF")
                try:
                    send_wechat(
                        f"[StockSage ETF] 池子状态 ({run_time})",
                        "\n".join(_status_lines), sendkey, dry_run=dry_run)
                    _etf_status_last_sent = now
                except Exception as _e:
                    print(f"  [ERROR] ETF 状态推送失败: {_e}")

        # ── Watchlist scan (every 5 min, trading hours only) ─────────────────
        # Normalise codes: strip SH/SZ/BJ prefix so fetcher can look them up
        watchlist = [c[-6:] if len(c) > 6 else c for c in config.get("watchlist", [])]
        need_watchlist = (
            _market_open and
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
                    sell_alert_state=sell_alert_state, _regime=_regime_this_iter,
                    etf_scores=_etf_all_scores_latest if _etf_all_scores_latest else None)
                _watchlist_last_scan = now
            except Exception as e:
                print(f"  [ERROR] Watchlist scan failed: {e}")
                _notify_error("watchlist_scan", str(e))

        if need_full:
            print(f"[{run_time}] Full factor check ({session_key} session)...")

            # ── Opening auction quality check (9:25, once per day) ───────────
            if session_key == "morning" and _auction_checked_date != now.date():
                _auction_checked_date = now.date()
                try:
                    # Union of 22:00 night picks + 01:00 pre-market picks (deduped, order preserved)
                    _auction_picks = list(dict.fromkeys(_night_picks + _premarket_picks))
                    _check_opening_auction(
                        holdings   = holdings,
                        pre_picks  = _auction_picks,
                        watchlist  = config.get("watchlist", []),
                        sendkey    = sendkey,
                        dry_run    = dry_run,
                        weights    = _fw,
                        thresholds = thresholds,
                    )
                except Exception as e:
                    print(f"  [竞价检验] 异常: {e}")

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
                    if "midday" not in _xhs_triggered_today:
                        _xhs_triggered_today.add("midday")
                        _trigger_xhs_post("midday", dry_run)
                else:
                    _full_universe = [c for c in _screener_universe
                                      if c not in _watchlist_set]
                    run(dry_run=dry_run, sell_alert_state=sell_alert_state,
                        _regime=_regime_this_iter, universe_override=_full_universe)
                _scanned_sessions.add(scan_id)

                # ── Post-scan XHS triggers ────────────────────────────────────
                # morning slot: no longer triggers XHS (preauction handles 09:25)
                # evening slot: triggered at 15:05 closing summary (not here)
                # midday slot: triggered below
                pass

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
            "night_picks":         _night_picks,
            "signal_tracker_date": _signal_tracker_date.isoformat()
                                   if _signal_tracker_date else None,
            "auto_tune_date":      _auto_tune_date.isoformat()
                                   if _auto_tune_date else None,
        })

        # ── Sleep until next fast interval ────────────────────────────────────
        sleep_sec = interval_min * 60
        _deadline = time.time() + sleep_sec
        while time.time() < _deadline:
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
            "night_picks":           _night_picks,
            "signal_tracker_date":   _signal_tracker_date.isoformat()
                                     if _signal_tracker_date else None,
            "auto_tune_date":        _auto_tune_date.isoformat()
                                     if _auto_tune_date else None,
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
        cmd = [sys.executable, "-X", "utf8", writer, slot]
        # Only pass --style for slots that accept it
        if slot in ("morning", "midday", "night", "evening"):
            cmd += ["--style", "auto"]
        log_path = os.path.join(_ROOT, "logs", f"xhs_{slot}.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as _log:
            subprocess.Popen(cmd, stdout=_log, stderr=_log, cwd=_ROOT)
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
