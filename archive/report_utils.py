"""
report_utils.py — 报告生成共享工具

提供给 reporter.py 使用的通用计算函数，不包含任何推送或格式化逻辑。
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from common import get_spot_em
from research import research as _research
from factors import DEFAULT_WEIGHTS


def forward_return(
    closes: dict[str, float],
    signal_date: str,
    entry_price: float,
    n: int,
) -> float | None:
    """Calculate T+N trading day forward return (%).

    Args:
        closes: {date_str: close_price} mapping, sorted ascending
        signal_date: signal date string (YYYY-MM-DD or YYYYMMDD)
        entry_price: price at signal
        n: number of trading days forward

    Returns forward return in % or None if data insufficient.
    """
    if not closes or entry_price <= 0:
        return None
    sorted_dates = sorted(closes.keys())
    try:
        first_after = next(i for i, d in enumerate(sorted_dates) if d > signal_date)
    except StopIteration:
        return None
    target_idx = first_after + n - 1
    if target_idx >= len(sorted_dates):
        return None
    return round((closes[sorted_dates[target_idx]] - entry_price) / entry_price * 100, 4)


def calc_pick_stats(picks: list[dict], prices: dict[str, dict]) -> dict:
    """Calculate win rate and return stats for a list of picks against live prices.

    Args:
        picks:  list of dicts with at least {"code", "name"} keys
        prices: {code: {"price": float, "change_pct": float}} from get_spot_em()

    Returns dict with keys: results, n_total, n_win, win_rate, avg_ret,
                            top5, watch_up, watch_dn, nan_stocks
    """
    results = []
    for p in picks:
        code = p["code"]
        pr   = prices.get(code)
        if not pr:
            continue
        change_pct = pr.get("change_pct")
        if change_pct is None:
            continue
        results.append({
            "code":       code,
            "name":       p.get("name", code),
            "industry":   p.get("industry", ""),
            "price":      pr.get("price"),
            "change_pct": float(change_pct),
            "tier":       p.get("tier", ""),
        })

    empty = {"results": [], "n_total": 0, "n_win": 0,
             "win_rate": 0.0, "avg_ret": 0.0,
             "top5": [], "watch_up": [], "watch_dn": [], "nan_stocks": []}
    if not results:
        return empty

    nan_stocks = [r for r in results if math.isnan(r["change_pct"])]
    valid      = [r["change_pct"] for r in results if not math.isnan(r["change_pct"])]
    n_win      = sum(1 for v in valid if v > 0)
    win_rate   = n_win / len(valid) * 100 if valid else 0.0
    avg_ret    = sum(valid) / len(valid)   if valid else 0.0
    by_chg     = sorted(
        [r for r in results if not math.isnan(r["change_pct"])],
        key=lambda r: r["change_pct"], reverse=True,
    )
    return {
        "results":    results,
        "n_total":    len(results),
        "n_win":      n_win,
        "win_rate":   win_rate,
        "avg_ret":    avg_ret,
        "top5":       by_chg[:5],
        "watch_up":   [r for r in by_chg if 0 < r["change_pct"] <= 3.0],
        "watch_dn":   [r for r in by_chg if r["change_pct"] < 0][:3],
        "nan_stocks": nan_stocks,
    }


def fetch_prices_with_retry(
    codes: list[str],
    picks: list[dict],
    slot: str,
    retry_interval: int = 600,
    max_retries: int = 4,
) -> dict:
    """Fetch live prices, retrying within the time window if data is not yet available.

    Returns the result of calc_pick_stats(picks, prices).
    """
    def _fetch() -> dict[str, dict]:
        try:
            df = get_spot_em()
            if df.empty:
                return {}
            df["_code"] = df["代码"].astype(str).str.zfill(6)
            df = df[df["_code"].isin(set(codes))].copy()
            df["_price"] = pd.to_numeric(df["最新价"],  errors="coerce")
            df["_pct"]   = pd.to_numeric(df["涨跌幅"],  errors="coerce")
            df = df.dropna(subset=["_price", "_pct"])
            return dict(zip(df["_code"],
                            [{"price": p, "change_pct": c}
                             for p, c in zip(df["_price"], df["_pct"])]))
        except Exception as e:
            print(f"[report_utils] 行情获取失败: {e}")
            return {}

    for attempt in range(max_retries):
        prices = _fetch()
        stats  = calc_pick_stats(picks, prices)
        if stats["results"]:
            return stats
        if attempt < max_retries - 1:
            print(f"[{slot}] 行情未就绪，{retry_interval // 60}分钟后重试"
                  f"（第{attempt + 1}次）")
            time.sleep(retry_interval)
        else:
            break
    return calc_pick_stats(picks, {})


def load_json(path) -> dict | list:
    """Load JSON from a Path or str. Returns empty dict on missing/invalid file."""
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path, obj) -> None:
    """Atomically write obj as JSON to path."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(p) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, str(p))


# ---------------------------------------------------------------------------
# Strategy scoring helpers (shared by main_strategy, small_strategy, watchlist_scan)
# ---------------------------------------------------------------------------

def regime_key(regime_score: float) -> str:
    if regime_score <= 2: return "BEAR"
    if regime_score <= 4: return "CAUTION"
    if regime_score >= 7: return "BULL"
    return "NORMAL"


def compact_factor_scores(factors: dict) -> dict:
    return {
        name: {"buy": round(f.get("score") or 0, 1),
               "sell": round(f.get("sell_score") or 0, 1)}
        for name, f in factors.items() if isinstance(f, dict)
    }


def score_one_buy(code: str, weights=None) -> dict:
    try:
        result  = _research(code, weights or DEFAULT_WEIGHTS)
        summary = result.get("signals_summary", {})
        price_d = result.get("price") or {}
        basic   = result.get("basic") or {}
        val     = result.get("valuation") or {}
        return {
            "code":           code,
            "name":           result.get("name", code),
            "price":          price_d.get("current"),
            "change_pct":     price_d.get("change_pct"),
            "buy_score":      round(result.get("total_score", 0) or 0, 1),
            "sell_score":     round(result.get("total_sell_score", 0) or 0, 1),
            "bullish":        summary.get("top_bullish", [])[:3],
            "bearish":        summary.get("top_bearish", [])[:3],
            "industry":       basic.get("industry", "Unknown"),
            "market_cap_b":   basic.get("market_cap_billion"),
            "pe_ttm":         val.get("pe_ttm"),
            "pb":             val.get("pb"),
            "turnover_rate":  price_d.get("turnover_rate"),
            "volume_ratio":   price_d.get("volume_ratio"),
            "volume_million": price_d.get("volume_million"),
            "factor_scores":  compact_factor_scores(result.get("factors") or {}),
            "error":          None,
        }
    except Exception as e:
        return {"code": code, "name": code, "price": None, "change_pct": None,
                "buy_score": 0.0, "sell_score": 0.0, "bullish": [], "bearish": [],
                "industry": None, "market_cap_b": None, "pe_ttm": None, "pb": None,
                "turnover_rate": None, "volume_ratio": None, "volume_million": None,
                "factor_scores": {}, "error": str(e)}
