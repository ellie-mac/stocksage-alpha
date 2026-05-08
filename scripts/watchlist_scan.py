#!/usr/bin/env python3
"""
自选池扫描 — 盘中每30分钟运行，检查买卖信号

- 卖出：对 holdings 里 shares>0 的持仓打分，触发阈值则推送
- 买入：对 watchlist 里未持仓的股票打分，分数超阈值则推送
- 同一股票有冷却窗口（默认 90 分钟），避免盘中重复推送

用法：
    python -X utf8 scripts/watchlist_scan.py
    python -X utf8 scripts/watchlist_scan.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import partial

sys.path.insert(0, os.path.dirname(__file__))

from research import research
from factors import DEFAULT_WEIGHTS, weights_from_config_dict
from factor_config import REGIME_WEIGHTS
from factors_extended import score_market_regime
import fetcher
from common import configure_pushplus, send_wechat, is_t1_locked

_ROOT           = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCAN_CACHE_PATH = os.path.join(_ROOT, "data", "watchlist_scan_latest.json")
SELL_STATE_PATH = os.path.join(_ROOT, "data", ".watchlist_sell_state.json")


# ── 市场状态 ──────────────────────────────────────────────────────────────────

def _regime_key(score: float) -> str:
    if score <= 2: return "BEAR"
    if score <= 4: return "CAUTION"
    if score >= 7: return "BULL"
    return "NORMAL"


# ── 卖出冷却状态 ──────────────────────────────────────────────────────────────

def _load_sell_state() -> dict:
    try:
        with open(SELL_STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_sell_state(state: dict) -> None:
    tmp = SELL_STATE_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, SELL_STATE_PATH)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass


# ── 评分 ──────────────────────────────────────────────────────────────────────

def _score_holding(holding: dict, weights=None) -> dict:
    code = holding["code"]
    try:
        result  = research(code, weights or DEFAULT_WEIGHTS)
        price_d = result.get("price") or {}
        price   = price_d.get("current") or 0
        cost    = holding.get("cost_price") or 0
        pnl_pct = round((price - cost) / cost * 100, 2) if cost > 0 else 0.0
        summary = result.get("signals_summary", {})
        return {
            "code":       code,
            "name":       result.get("name", holding.get("name", code)),
            "shares":     holding.get("shares", 0),
            "cost_price": cost,
            "price":      price,
            "change_pct": price_d.get("change_pct"),
            "pnl_pct":    pnl_pct,
            "buy_score":  round(result.get("total_score", 0) or 0, 1),
            "sell_score": round(result.get("total_sell_score", 0) or 0, 1),
            "bullish":    summary.get("top_bullish", [])[:3],
            "bearish":    summary.get("top_bearish", [])[:3],
            "t1_locked":  is_t1_locked(holding),
            "error":      None,
        }
    except Exception as e:
        return {"code": code, "name": holding.get("name", code),
                "shares": holding.get("shares", 0), "cost_price": holding.get("cost_price", 0),
                "price": None, "pnl_pct": 0.0, "buy_score": 0.0, "sell_score": 0.0,
                "bullish": [], "bearish": [], "t1_locked": False, "error": str(e)}


def _score_watchlist(code: str, weights=None) -> dict:
    try:
        result  = research(code, weights or DEFAULT_WEIGHTS)
        price_d = result.get("price") or {}
        summary = result.get("signals_summary", {})
        return {
            "code":       code,
            "name":       result.get("name", code),
            "price":      price_d.get("current"),
            "change_pct": price_d.get("change_pct"),
            "buy_score":  round(result.get("total_score", 0) or 0, 1),
            "sell_score": round(result.get("total_sell_score", 0) or 0, 1),
            "bullish":    summary.get("top_bullish", [])[:3],
            "bearish":    summary.get("top_bearish", [])[:3],
            "error":      None,
        }
    except Exception as e:
        return {"code": code, "name": code, "price": None, "change_pct": None,
                "buy_score": 0.0, "sell_score": 0.0, "bullish": [], "bearish": [],
                "error": str(e)}


# ── 核心扫描 ──────────────────────────────────────────────────────────────────

def scan(
    watchlist_codes: list[str],
    holdings: list[dict],
    thresholds: dict,
    regime_score: float = 5.0,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Score watchlist + holdings. Returns (sell_alerts, buy_alerts, all_wl_scored)."""
    rk  = _regime_key(regime_score)
    fw  = weights_from_config_dict(REGIME_WEIGHTS[rk])
    _sh = partial(_score_holding,  weights=fw)
    _sw = partial(_score_watchlist, weights=fw)

    sell_trig  = thresholds.get("sell_score_trigger", 60)
    stall      = thresholds.get("stall_sell_score", 40)
    stop_loss  = thresholds.get("stop_loss_pct", -8.0)
    buy_trig   = thresholds.get("buy_score_trigger", 70)
    sell_guard = max(0, sell_trig - 10)
    if regime_score <= 2:
        buy_trig   = round(buy_trig * 1.25, 1)
        sell_guard = min(sell_guard, 25)
    elif regime_score <= 4:
        buy_trig = round(buy_trig * 1.15, 1)

    held_codes = {h["code"] for h in holdings if (h.get("shares") or 0) > 0}

    # 持仓打分（卖出检查）
    scored_holdings: list[dict] = []
    if holdings:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_sh, h): h for h in holdings}
            for fut in as_completed(futs):
                scored_holdings.append(fut.result())

    # 自选池打分（买入检查，排除已持仓）
    wl_only = [c for c in watchlist_codes if c not in held_codes]
    scored_wl: list[dict] = []
    if wl_only:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_sw, c): c for c in wl_only}
            for fut in as_completed(futs):
                scored_wl.append(fut.result())
    scored_wl.sort(key=lambda x: -x.get("buy_score", 0))

    # 卖出信号
    sell_alerts: list[dict] = []
    for s in scored_holdings:
        if s.get("error") or (s.get("shares") or 0) <= 0:
            continue
        reasons: list[str] = []
        if s["sell_score"] >= sell_trig:
            reasons.append(f"综合卖出评分 {s['sell_score']:.0f}/100 ≥ {sell_trig}")
        elif stall <= s["sell_score"] < sell_trig:
            reasons.append(f"逢高减仓参考: 卖出信号 **{s['sell_score']:.0f}**"
                           f"（阈值 {stall}–{sell_trig}）")
        if (s.get("pnl_pct") or 0) <= stop_loss:
            reasons.append(f"止损触发: 浮亏 {s['pnl_pct']:+.1f}%")
        if reasons:
            prefix = "⚠️[T+1今日买入/明日可操作] " if s.get("t1_locked") else ""
            sell_alerts.append({**s, "reasons": [prefix + r for r in reasons] if prefix else reasons})

    # 买入信号
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

    return sell_alerts, buy_alerts, scored_wl


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.join(_ROOT, "alert_config.json")
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(config.get("pushplus", {}).get("token", ""))

    raw_wl = config.get("watchlist", [])
    if raw_wl and isinstance(raw_wl[0], dict):
        watchlist_codes = [e["code"] for e in raw_wl]
    else:
        watchlist_codes = [c[-6:] if len(c) > 6 else c for c in raw_wl]

    holdings_path = os.path.join(_ROOT, "holdings.json")
    holdings: list[dict] = []
    if os.path.exists(holdings_path):
        with open(holdings_path, encoding="utf-8") as f:
            holdings = json.load(f)

    if not watchlist_codes and not holdings:
        print("[watchlist_scan] 自选池和持仓均为空，退出")
        return
    print(f"[watchlist_scan] watchlist={len(watchlist_codes)} holdings={len(holdings)}")

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

    sell_alerts, buy_alerts, all_scored = scan(
        watchlist_codes, holdings, thresholds, regime_score
    )
    print(f"[watchlist_scan] sell={len(sell_alerts)} buy={len(buy_alerts)}")

    # 卖出冷却去重
    cooldown_min  = thresholds.get("sell_alert_cooldown_min", 90)
    sell_state    = _load_sell_state()
    now_dt        = datetime.now()
    deduped_sell: list[dict] = []
    for sa in sell_alerts:
        last_str = sell_state.get(sa["code"])
        if last_str:
            try:
                last_dt = datetime.fromisoformat(last_str)
                if (now_dt - last_dt).total_seconds() / 60 < cooldown_min:
                    print(f"  [dedup] 跳过 {sa['code']} (上次推送 {last_str[:16]})")
                    continue
            except Exception:
                pass
        sell_state[sa["code"]] = now_dt.isoformat()
        deduped_sell.append(sa)
    if not args.dry_run:
        _save_sell_state(sell_state)
    sell_alerts = deduped_sell

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

    if not sell_alerts and not buy_alerts:
        print("[watchlist_scan] 无信号，跳过推送")
        return

    run_time  = now_dt.strftime("%Y-%m-%d %H:%M")
    _re_emoji = "🐻" if regime_score <= 3 else ("🟡" if regime_score <= 6 else "🐂")

    rows = [f"*{run_time}*<br>市场 {_re_emoji} {regime_score:.0f}/10"]
    for sa in sell_alerts:
        pnl_sign = "🔴" if (sa.get("pnl_pct") or 0) < 0 else "🟢"
        rows.append(f"**{sa['name']} ({sa['code']})**<br>"
                    f"卖出分 **{sa['sell_score']:.0f}** | 浮盈 {pnl_sign} **{sa.get('pnl_pct', 0):+.1f}%**")
        for r in sa["reasons"]:
            rows.append(f"- {r}")
    for ba in buy_alerts:
        p = ba.get("price") or 0
        rows.append(f"**{ba['name']} ({ba['code']})**<br>"
                    f"买入分 **{ba['buy_score']:.0f}** | 现价 **{p}**")
    rows.append("<br>> 仅供参考")
    desp = "<br>".join(rows)

    sell_trig = thresholds.get("sell_score_trigger", 60)
    strong_s  = [a for a in sell_alerts if a["sell_score"] >= sell_trig]
    stall_s   = [a for a in sell_alerts if a not in strong_s]
    strong_b  = [a for a in buy_alerts  if a["buy_score"] >= 80]
    add_b     = [a for a in buy_alerts  if a not in strong_b]
    parts: list[str] = []
    if strong_s: parts.append(f"🔴 {len(strong_s)} 卖出")
    if stall_s:  parts.append(f"⚠️ {len(stall_s)} 减仓")
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
