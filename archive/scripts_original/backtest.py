#!/usr/bin/env python3
"""
Portfolio backtest: long-only top-quantile strategy on A-share stocks.

At each historical cross-section (price_offset days ago):
  1. Score all stocks in the universe using multi-factor model
  2. Go long the top-N% basket (equal-weight)
  3. Track actual forward return vs. CSI 300 benchmark

Output: cumulative returns, Sharpe, max drawdown, win rate, alpha/beta.

Note: price-based factors use true historical data (no look-ahead).
Fundamental / social / revision factors use current data — they have
look-ahead bias for historical periods and will overstate real alpha.
Use group="A" for a cleaner (price-only) backtest signal.

Usage:
  python backtest.py                          # 6 periods, 20d fwd, top 20%
  python backtest.py --periods 12 --fwd 10   # 12 periods, 10d forward
  python backtest.py --top 10 --group AB     # top 10%, all factors
  python backtest.py --out results.json      # save full output
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Patch requests.Session so every akshare HTTP call gets a fast connect timeout.
# East Money endpoints are currently slow/hanging; 5s connect / 25s read allows
# legitimate APIs (163.com ~7s) to succeed while failing hung ones quickly.
import requests as _requests
_orig_session_request = _requests.Session.request

def _patched_session_request(self, method, url, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = (5, 25)
    return _orig_session_request(self, method, url, **kwargs)

_requests.Session.request = _patched_session_request

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

import fetcher
from factor_analysis import compute_stock_scores, TEST_UNIVERSE
from industry import build_industry_map
from factor_config import (FACTOR_WEIGHTS,
                           REGIME_MA_LONG,
                           REGIME_EXPOSURE, REGIME_WEIGHTS,
                           REGIME_WEIGHTS_SMALLCAP,
                           REGIME_CAUTION_THRESHOLD, REGIME_CRISIS_THRESHOLD,
                           REGIME_BULL_THRESHOLD, REGIME_EXTREME_BULL_THRESHOLD)


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

def _get_benchmark_returns(
    forward_days: int,
    n_periods: int,
    step: int,
) -> list[Optional[float]]:
    """
    Compute CSI 300 forward return for each backtest period.
    Period k uses price_offset = k * step, same convention as compute_stock_scores.
    Returns a list of length n_periods; entry is None if data is unavailable.
    """
    df = fetcher.get_market_regime_data()  # last 120 rows of CSI 300
    if df is None or "close" not in df.columns:
        return [None] * n_periods

    close = pd.to_numeric(df["close"], errors="coerce").dropna().reset_index(drop=True)
    results: list[Optional[float]] = []

    for period_idx in range(n_periods):
        price_offset = period_idx * step
        total_skip   = forward_days + price_offset
        # Need at least total_skip + 2 rows
        if len(close) < total_skip + 2:
            results.append(None)
            continue
        start_price = float(close.iloc[-(total_skip + 1)])
        end_price   = float(close.iloc[-(price_offset + 1)])
        ret = (end_price / start_price - 1) * 100
        results.append(round(ret, 3))

    return results


# ---------------------------------------------------------------------------
# Composite score
# ---------------------------------------------------------------------------

def _composite_score(stock_scores: dict,
                     weights: Optional[dict] = None) -> Optional[float]:
    """
    Compute a weighted composite score.
    `weights` defaults to FACTOR_WEIGHTS (NORMAL regime).
    Only factors listed in `weights` are used; others are ignored.
    Negative weights invert the factor (contrarian signal).
    Returns None if fewer than 3 active factors have valid data.
    """
    if weights is None:
        weights = FACTOR_WEIGHTS
    exclude = {"forward_ret", "code"}
    weighted_sum = 0.0
    weight_total = 0.0
    n_active = 0
    for k, v in stock_scores.items():
        if k in exclude or k.startswith("sell_score_"):
            continue
        w = weights.get(k)
        if w is None or w == 0.0:
            continue
        try:
            fval = float(v)
        except (TypeError, ValueError):
            continue
        if np.isnan(fval):
            continue
        weighted_sum += fval * w
        weight_total += abs(w)
        n_active += 1
    if n_active < 3 or weight_total == 0.0:
        return None
    return float(weighted_sum / weight_total)


def _interpolate_exposure(lam: float) -> float:
    """λ-smooth exposure: λ=0 → BEAR (0.15), λ=1 → NORMAL cap (0.85)."""
    return round(REGIME_EXPOSURE["BEAR"] + (REGIME_EXPOSURE["NORMAL"] - REGIME_EXPOSURE["BEAR"]) * max(0.0, min(1.0, lam)), 2)


def _compute_regime_lambda(end_px: float, ma60: float, prior_ret: float) -> float:
    """Continuous bullishness λ ∈ [0, 1]: 0 = fully bearish, 1 = fully bullish.

    Combines trend_strength (price distance from MA60) and 20d momentum.
    Mapping: raw ≤ -8 → λ=0, raw ≥ +8 → λ=1, linear in between.
    """
    trend_str = (end_px / ma60 - 1) * 100.0 if ma60 > 0 else 0.0
    raw = trend_str * 0.6 + prior_ret * 0.4
    return max(0.0, min(1.0, (raw + 8.0) / 16.0))


def _interpolate_weights(wt_a: dict, wt_b: dict, lam: float) -> dict:
    """Linear interpolation between two factor-weight dicts at position λ."""
    all_keys = set(wt_a) | set(wt_b)
    return {k: round((1.0 - lam) * wt_a.get(k, 0.0) + lam * wt_b.get(k, 0.0), 4)
            for k in all_keys}


def _detect_regime(regime_close: Optional[pd.Series],
                   price_offset: int,
                   weights_table: Optional[dict] = None,
                   use_lambda: bool = True) -> tuple[str, float, dict, float]:
    """
    Detect market regime at the cross-section date.

    Two-layer regime system:
    Layer 1 — MA60 trend filter (structural):
      CSI 300 < MA60  ->  BEAR (15% exposure, crisis weights) — stay near cash
      CSI 300 >= MA60 ->  proceed to Layer 2

    Layer 2 — prior-20d return thresholds (tactical, only when above MA60):
      prior_ret >= -3%  -> NORMAL  (100% exposure)
      prior_ret <  -3%  -> CAUTION ( 70% exposure, defensive weights)
      prior_ret <  -6%  -> CRISIS  ( 40% exposure, crisis weights)
      prior_ret > +3.5% -> BULL    ( 80% exposure, momentum weights)
      prior_ret > +6%   -> EXTREME_BULL (55% exposure, momentum weights)

    weights_table: regime-name -> factor-weights dict.  Defaults to REGIME_WEIGHTS (main strategy).
    Returns (regime_name, exposure, factor_weights, regime_lambda).
    factor_weights is λ-interpolated between BEAR and EXTREME_BULL endpoints for smooth transitions.
    """
    if weights_table is None:
        weights_table = REGIME_WEIGHTS
    default = ("NORMAL", REGIME_EXPOSURE["NORMAL"], weights_table["NORMAL"], 0.5)
    if regime_close is None:
        return default

    lookback = 20
    ma60_lookback = 60
    needed = price_offset + max(lookback, ma60_lookback) + 2
    if len(regime_close) < needed:
        return default

    end_px   = float(regime_close.iloc[-(price_offset + 1)])

    # --- Layer 1: MA60 trend filter ---
    ma60_start = max(0, len(regime_close) - (price_offset + ma60_lookback + 1))
    ma60_end   = len(regime_close) - price_offset
    ma60       = float(regime_close.iloc[ma60_start:ma60_end].mean())
    if end_px < ma60:
        if use_lambda:
            lam = _compute_regime_lambda(end_px, ma60, -10.0)
            wts = _interpolate_weights(weights_table["BEAR"], weights_table["EXTREME_BULL"], lam)
            exp = _interpolate_exposure(lam)
        else:
            lam = 0.0
            wts = weights_table["BEAR"]
            exp = REGIME_EXPOSURE["BEAR"]
        return "BEAR", exp, wts, lam

    # --- Layer 2: prior-20d return (tactical) ---
    start_px = float(regime_close.iloc[-(price_offset + lookback + 1)])
    if start_px <= 0:
        return default

    prior_ret = (end_px / start_px - 1) * 100

    if prior_ret < REGIME_CRISIS_THRESHOLD:
        r = "CRISIS"
    elif prior_ret < REGIME_CAUTION_THRESHOLD:
        r = "CAUTION"
    elif prior_ret > REGIME_EXTREME_BULL_THRESHOLD:
        r = "EXTREME_BULL"
    elif prior_ret > REGIME_BULL_THRESHOLD:
        r = "BULL"
    else:
        r = "NORMAL"

    if use_lambda:
        lam = _compute_regime_lambda(end_px, ma60, prior_ret)
        wts = _interpolate_weights(weights_table["BEAR"], weights_table["EXTREME_BULL"], lam)
        exp = _interpolate_exposure(lam)
    else:
        lam = 0.5
        wts = weights_table[r]
        exp = REGIME_EXPOSURE[r]
    return r, exp, wts, lam


def _precompute_regimes(
    regime_close: Optional[pd.Series],
    n_periods: int,
    step: int,
    weights_table: dict,
    min_duration: int = 2,
    use_lambda: bool = True,
) -> list[tuple[str, float, dict, float]]:
    """
    Pre-compute regime for every period with causal minimum-duration smoothing.

    A regime switch is only confirmed after it persists for min_duration consecutive
    periods (processed oldest→newest). This replaces the prior isolated-blip filter
    which required knowing the next period's regime — a look-ahead bias.

    Array layout: index 0 = most recent period, index n-1 = oldest.
    Causal direction: oldest→newest = high-index→low-index.
    """
    raw = [
        _detect_regime(regime_close, i * step, weights_table, use_lambda=use_lambda)
        for i in range(n_periods)
    ]
    if len(raw) <= 2:
        return raw

    smoothed       = list(raw)
    confirmed      = raw[-1]   # oldest period starts as the confirmed regime
    pending_regime = raw[-1]
    pending_count  = 1

    for i in range(len(raw) - 2, -1, -1):  # oldest→newest
        if raw[i][0] == confirmed[0]:
            smoothed[i]    = raw[i]
            pending_regime = raw[i]
            pending_count  = 1
        elif raw[i][0] == pending_regime[0]:
            pending_count += 1
            if pending_count >= min_duration:
                confirmed = raw[i]
            smoothed[i] = confirmed
        else:
            pending_regime = raw[i]
            pending_count  = 1
            smoothed[i]    = confirmed

    return smoothed


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_backtest(
    codes: list[str] = TEST_UNIVERSE,
    forward_days: int = 20,
    n_periods: int = 6,
    step: int = 20,
    top_pct: float = 0.20,      # top 20% = long basket
    txn_cost_pct: float = 0.10, # round-trip transaction cost (%)
    group: str = "A",
    max_workers: int = 8,
    use_regime: bool = True,    # apply market-regime exposure filter
    sector_neutral: bool = True, # demean scores within each sector before ranking
    weights_table: Optional[dict] = None,  # regime->weights map; None = REGIME_WEIGHTS (main)
    min_liquidity_wan: float = 0.0,  # min avg daily trading amount in 万元; 0 = disabled
    use_lambda: bool = True,    # λ-interpolated regime weights; False = step-function
) -> dict:
    """
    Run a long-only quantile portfolio backtest.

    Returns a dict with per-period results, cumulative returns,
    and aggregate performance statistics.
    """
    if weights_table is None:
        weights_table = REGIME_WEIGHTS
    n_stocks = len(codes)
    top_n    = max(1, int(n_stocks * top_pct))
    strategy_label = "smallcap" if weights_table is REGIME_WEIGHTS_SMALLCAP else "main"

    print(f"Portfolio backtest [{strategy_label}]: {n_stocks} stocks, {n_periods} periods × {step}d step")
    print(f"Forward window: {forward_days}d | Top {top_pct*100:.0f}% ({top_n} stocks) | "
          f"Txn cost: {txn_cost_pct:.2f}% | Group: {group} | "
          f"Sector-neutral: {'ON' if sector_neutral else 'OFF'}\n")

    benchmark_rets = _get_benchmark_returns(forward_days, n_periods, step)

    # Load industry map once (cached 7 days) for sector neutralization
    ind_map: dict[str, str] = {}
    if sector_neutral:
        print("Loading industry sector map (cached 7 days)...")
        ind_map = build_industry_map()
        if ind_map:
            n_covered = sum(1 for c in codes if c in ind_map)
            print(f"  Loaded {len(ind_map)} stock→sector mappings "
                  f"({n_covered}/{n_stocks} universe stocks covered)\n")
        else:
            print("  Warning: sector map unavailable, falling back to global ranking\n")

    # Load market data: DataFrame (trade_date + close) for PIT slicing,
    # and a plain close Series for regime detection.
    _market_df: Optional[pd.DataFrame] = fetcher.get_market_regime_data()
    regime_close: Optional[pd.Series] = None
    if _market_df is not None and "close" in _market_df.columns:
        regime_close = pd.to_numeric(_market_df["close"], errors="coerce").dropna().reset_index(drop=True)

    # Pre-compute (and smooth) all regimes before entering the period loop.
    if use_regime:
        pre_regimes = _precompute_regimes(regime_close, n_periods, step,
                                          weights_table or REGIME_WEIGHTS,
                                          use_lambda=use_lambda)
    else:
        wt = (weights_table or REGIME_WEIGHTS)["NORMAL"]
        pre_regimes = [("NORMAL", REGIME_EXPOSURE["NORMAL"], wt, 0.5)] * n_periods

    # Pre-fetch shared spot data (used by sector_sympathy factor)
    _spot_df = fetcher._get_spot_df()

    period_results: list[dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for period_idx in range(n_periods):
            price_offset = period_idx * step
            print(f"  Period {period_idx + 1}/{n_periods}  (price_offset={price_offset}d)")

            # ── Historicize market data and derive asof_date ─────────────────
            asof_date = ""
            market_df_period: Optional[pd.DataFrame] = None
            if _market_df is not None:
                if price_offset > 0 and len(_market_df) > price_offset:
                    market_df_period = _market_df.iloc[:-price_offset].copy()
                else:
                    market_df_period = _market_df
                if (market_df_period is not None
                        and not market_df_period.empty
                        and "trade_date" in market_df_period.columns):
                    asof_date = str(market_df_period["trade_date"].iloc[-1])

            _shared: dict = {
                "market_df":  market_df_period,
                "spot_df":    _spot_df,
                "market_ret": None,
                "asof_date":  asof_date,
            }

            futures = {
                ex.submit(compute_stock_scores, code, forward_days, group,
                          price_offset, _shared): code
                for code in codes
            }

            # Use pre-computed (smoothed) regime for this period
            regime_name, exposure, regime_wts, regime_lam = pre_regimes[period_idx]
            print(f"    Regime: {regime_name} ({exposure:.0%}) λ={regime_lam:.2f}  asof={asof_date}")

            period_rows: list[dict] = []
            per_period_timeout = len(codes) * 60  # 60s budget per stock
            try:
                done_iter = as_completed(futures, timeout=per_period_timeout)
                for future in done_iter:
                    code = futures[future]
                    try:
                        r = future.result(timeout=0)
                        if r is None:
                            continue
                        comp = _composite_score(r, regime_wts)
                        if comp is None:
                            continue
                        period_rows.append({
                            "code":              code,
                            "composite":         comp,
                            "forward_ret":       r.get("forward_ret"),
                            "_avg_daily_amt_wan": r.get("_avg_daily_amt_wan", 0.0),
                        })
                    except Exception:
                        pass
            except concurrent.futures.TimeoutError:
                print(f"    Warning: period timed out after {per_period_timeout}s, "
                      f"collected {len(period_rows)} stocks so far")

            if min_liquidity_wan > 0:
                n_before = len(period_rows)
                period_rows = [
                    r for r in period_rows
                    if (r.get("_avg_daily_amt_wan") or 0.0) >= min_liquidity_wan
                ]
                n_dropped = n_before - len(period_rows)
                if n_dropped:
                    print(f"    Liquidity filter (≥{min_liquidity_wan:.0f}万/d): "
                          f"removed {n_dropped} illiquid stocks, {len(period_rows)} remain")

            if len(period_rows) < max(10, top_n * 2):
                print(f"    Skipped — only {len(period_rows)} valid stocks\n")
                continue

            df_period = (
                pd.DataFrame(period_rows)
                .dropna(subset=["composite", "forward_ret"])
            )

            # ── Sector neutralization ──────────────────────────────────────
            # Replace raw composite score with within-sector z-score so that
            # rankings reflect stock quality relative to sector peers, not
            # cross-sector factor biases (e.g. value being high for all banks).
            if sector_neutral and ind_map:
                df_period["sector"] = df_period["code"].map(ind_map).fillna("未分类")

                def _sector_zscore(grp: pd.DataFrame) -> pd.DataFrame:
                    grp = grp.copy()
                    mu = grp["composite"].mean()
                    sd = grp["composite"].std(ddof=0)
                    if len(grp) < 2 or sd < 1e-8:
                        grp["score_adj"] = grp["composite"] - mu
                    else:
                        grp["score_adj"] = (grp["composite"] - mu) / sd
                    return grp

                df_period = (
                    df_period
                    .groupby("sector", group_keys=False)
                    .apply(_sector_zscore)
                    .sort_values("score_adj", ascending=False)
                    .reset_index(drop=True)
                )
            else:
                df_period = df_period.sort_values("composite", ascending=False).reset_index(drop=True)

            # Signals cache: save snapshot & compare with previous run for drift detection
            cache_stats = {}
            if asof_date:
                cache_stats = _signals_cache_save_and_compare(
                    asof_date, df_period, group, regime_wts, top_n
                )

            long_basket  = df_period.head(top_n)
            short_basket = df_period.tail(top_n)  # bottom quantile for reference

            if long_basket.empty:
                continue

            basket_ret = float(long_basket["forward_ret"].mean())
            # regime_name / exposure already set above (before scoring)
            regime_label = f"{regime_name} ({exposure:.0%})"
            # Partial exposure: exposure fraction in basket, (1-exposure) in cash
            port_ret  = (basket_ret - txn_cost_pct) * exposure
            bench_ret = benchmark_rets[period_idx]
            alpha     = (round(port_ret - bench_ret, 3) if bench_ret is not None else None)

            # Decile returns — split into 5 groups to check monotonicity
            n_decile = max(1, len(df_period) // 5)
            decile_rets = []
            for d in range(5):
                slice_df = df_period.iloc[d * n_decile: (d + 1) * n_decile]
                decile_rets.append(round(float(slice_df["forward_ret"].mean()), 3))

            period_results.append({
                "period":          period_idx + 1,
                "price_offset_d":  price_offset,
                "n_valid":         len(df_period),
                "exposure":        round(exposure, 2),
                "regime":          regime_name,
                "regime_lambda":   round(regime_lam, 3),
                "portfolio_ret":   round(port_ret, 3),
                "basket_ret":      round(basket_ret - txn_cost_pct, 3),
                "benchmark_ret":   bench_ret,
                "alpha":           alpha,
                "bottom_ret":      round(float(short_basket["forward_ret"].mean()), 3),
                "long_short_spread": round(
                    float(long_basket["forward_ret"].mean()) -
                    float(short_basket["forward_ret"].mean()), 3
                ),
                "decile_rets":     decile_rets,
                "top_stocks":      long_basket["code"].tolist(),
                "signal_id":       cache_stats.get("signal_id", ""),
                "cache_drift":     {k: v for k, v in cache_stats.items() if k != "signal_id"},
            })

            regime_tag = (f"  [{regime_name} exp={exposure:.0%}]"
                         if use_regime and regime_name != "NORMAL" else "")
            status = (f"port={port_ret:+.2f}%  bench={bench_ret:+.2f}%  "
                      f"alpha={alpha:+.2f}%{regime_tag}" if bench_ret is not None
                      else f"port={port_ret:+.2f}%  bench=N/A{regime_tag}")
            print(f"    {status}\n")

    if not period_results:
        return {"error": "All periods failed — insufficient data"}

    # ── Aggregate statistics ────────────────────────────────────────────────
    port_rets  = [p["portfolio_ret"]  for p in period_results]
    bench_rets = [p["benchmark_ret"]  for p in period_results if p["benchmark_ret"] is not None]
    alphas     = [p["alpha"]          for p in period_results if p["alpha"]          is not None]
    ls_spreads = [p["long_short_spread"] for p in period_results]

    stats = _compute_stats(port_rets, bench_rets, alphas, ls_spreads, forward_days)

    # Cumulative returns (compounded)
    cum_port  = _cumulative(port_rets)
    cum_bench = _cumulative([p["benchmark_ret"] for p in period_results])

    return {
        "meta": {
            "n_stocks":        n_stocks,
            "n_periods":       len(period_results),
            "forward_days":    forward_days,
            "step_days":       step,
            "top_pct":         top_pct,
            "top_n":           top_n,
            "txn_cost_pct":    txn_cost_pct,
            "group":           group,
            "use_regime":      use_regime,
            "sector_neutral":  sector_neutral,
            "strategy":        strategy_label,
            "min_liquidity_wan": min_liquidity_wan,
        },
        "period_results":      period_results,
        "cumulative_portfolio": cum_port,
        "cumulative_benchmark": cum_bench,
        "stats":               stats,
    }


def _compute_stats(
    port_rets: list[float],
    bench_rets: list[float],
    alphas: list[float],
    ls_spreads: list[float],
    forward_days: int,
) -> dict:
    """Compute aggregate performance statistics."""
    if not port_rets:
        return {}

    arr = np.array(port_rets)
    periods_per_year = 252 / forward_days

    mean_ret    = float(np.mean(arr))
    std_ret     = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    sharpe      = mean_ret / std_ret * np.sqrt(periods_per_year) if std_ret > 0 else np.nan

    # Max drawdown on cumulative returns
    cum = np.cumprod(1 + arr / 100)
    running_max = np.maximum.accumulate(cum)
    drawdowns   = (cum - running_max) / running_max * 100
    max_dd      = float(drawdowns.min())

    # Beat-benchmark win rate: fraction of periods where portfolio outperforms index
    valid_alphas = [a for a in alphas if a is not None]
    beat_bench  = np.array(valid_alphas) > 0 if valid_alphas else np.array([])
    win_rate    = float(np.mean(beat_bench)) * 100 if len(beat_bench) > 0 else 0.0

    annualized  = (float(np.prod(1 + arr / 100)) ** (periods_per_year / len(arr)) - 1) * 100

    mean_alpha  = float(np.mean(valid_alphas)) if valid_alphas else None
    mean_spread = float(np.mean(ls_spreads)) if ls_spreads else None

    # Information ratio (alpha / tracking error)
    if bench_rets and len(bench_rets) == len(port_rets):
        excess = np.array(port_rets) - np.array(bench_rets)
        ir = float(np.mean(excess) / np.std(excess, ddof=1) * np.sqrt(periods_per_year)) \
             if np.std(excess, ddof=1) > 0 else np.nan
    else:
        ir = np.nan

    return {
        "mean_period_ret_pct":      round(mean_ret, 3),
        "annualized_ret_pct":       round(annualized, 2),
        "sharpe_ratio":             round(sharpe, 3) if not np.isnan(sharpe) else None,
        "information_ratio":        round(ir, 3) if not np.isnan(ir) else None,
        "max_drawdown_pct":         round(max_dd, 2),
        "win_rate_pct":             round(win_rate, 1),
        "mean_alpha_pct":           round(mean_alpha, 3) if mean_alpha is not None else None,
        "mean_long_short_spread_pct": round(mean_spread, 3) if mean_spread is not None else None,
        "n_periods":                len(port_rets),
    }


def _cumulative(rets: list[Optional[float]]) -> list[Optional[float]]:
    """Compound a list of period returns into cumulative returns (0-based).
    None entries are treated as 0% so the series is always gap-free.
    """
    result: list[Optional[float]] = []
    cum = 1.0
    for r in rets:
        if r is not None:
            cum *= (1 + r / 100)
        result.append(round((cum - 1) * 100, 3))
    return result


# ---------------------------------------------------------------------------
# Signals cache — lightweight snapshot & drift detection
# ---------------------------------------------------------------------------
_SIGNALS_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals_cache")


def _signals_cache_save_and_compare(
    asof_date: str,
    df_sorted: "pd.DataFrame",
    group: str,
    regime_wts: dict,
    top_n: int,
) -> dict:
    """
    Save composite scores for this period to a local cache and compare with the
    previous run for the same asof_date.

    Cache file: signals_cache/{asof_date}_{group}.json
    Saved fields: signal_id, scores {code→composite}, top20, bottom20.

    Comparison (if previous exists): Spearman rank correlation + top-N Jaccard.
    Returns a dict with signal_id and comparison stats (or empty dict on error).
    """
    os.makedirs(_SIGNALS_CACHE_DIR, exist_ok=True)

    score_col = "score_adj" if "score_adj" in df_sorted.columns else "composite"
    if "code" not in df_sorted.columns or score_col not in df_sorted.columns:
        return {}

    scores_map: dict[str, float] = {
        str(row["code"]): round(float(row[score_col]), 4)
        for _, row in df_sorted.iterrows()
    }
    sorted_codes = list(df_sorted["code"].astype(str))
    top20 = sorted_codes[:min(20, top_n)]
    bottom20 = sorted_codes[max(0, len(sorted_codes) - 20):]

    # signal_id = hash(asof_date + universe + group + weights)
    universe_hash = hashlib.md5(",".join(sorted(scores_map.keys())).encode()).hexdigest()[:8]
    weights_str = json.dumps(regime_wts, sort_keys=True)
    weights_hash = hashlib.md5(weights_str.encode()).hexdigest()[:8]
    signal_id = hashlib.md5(
        f"{asof_date}|{universe_hash}|{group}|{weights_hash}".encode()
    ).hexdigest()[:12]

    cache_path = os.path.join(_SIGNALS_CACHE_DIR, f"{asof_date}_{group}.json")

    comparison: dict = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as f:
                prev = json.load(f)
            prev_scores: dict = prev.get("scores", {})
            common_codes = [c for c in scores_map if c in prev_scores]
            if len(common_codes) >= 10:
                cur_vals = pd.Series([scores_map[c] for c in common_codes])
                prv_vals = pd.Series([prev_scores[c] for c in common_codes])
                spearman = float(
                    cur_vals.rank().corr(prv_vals.rank())
                )
                prev_top20 = set(prev.get("top20", []))
                curr_top20 = set(top20)
                jaccard = (
                    len(prev_top20 & curr_top20) / len(prev_top20 | curr_top20)
                    if prev_top20 | curr_top20
                    else 1.0
                )
                comparison = {
                    "prev_signal_id": prev.get("signal_id", ""),
                    "prev_saved_at": prev.get("saved_at", ""),
                    "n_common": len(common_codes),
                    "spearman_vs_prev": round(spearman, 4),
                    "top20_jaccard_vs_prev": round(jaccard, 4),
                }
                flag = ""
                if spearman < 0.90:
                    flag = " ⚠ LARGE DRIFT"
                elif spearman < 0.95:
                    flag = " (minor drift)"
                print(f"    signals_cache [{asof_date}]: "
                      f"Spearman={spearman:.3f}  top20_overlap={jaccard:.2f}{flag}")
        except Exception:
            pass

    snapshot = {
        "signal_id": signal_id,
        "asof_date": asof_date,
        "group": group,
        "n_stocks": len(scores_map),
        "scores": scores_map,
        "top20": top20,
        "bottom20": bottom20,
        "saved_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False)
    except Exception:
        pass

    return {"signal_id": signal_id, **comparison}


# ---------------------------------------------------------------------------
# CLI output helpers
# ---------------------------------------------------------------------------

def _print_results(result: dict) -> None:
    if "error" in result:
        print(f"\nError: {result['error']}")
        return

    meta  = result["meta"]
    stats = result["stats"]

    print("\n" + "=" * 72)
    print("PORTFOLIO BACKTEST RESULTS")
    print("=" * 72)
    print(f"Universe: {meta['n_stocks']} stocks  |  "
          f"{meta['n_periods']} periods × {meta['step_days']}d step  |  "
          f"{meta['forward_days']}d forward")
    print(f"Long basket: top {meta['top_pct']*100:.0f}% ({meta['top_n']} stocks)  |  "
          f"Group: {meta['group']}  |  Txn cost: {meta['txn_cost_pct']:.2f}%/period\n")

    # Period-by-period table
    has_regime = meta.get("use_regime", False)
    hdr_regime = "  Regime" if has_regime else ""
    print(f"{'Period':>7} {'Port%':>8} {'Bench%':>8} {'Alpha%':>8} {'L/S%':>7} {'#Stk':>5}{hdr_regime}")
    print("-" * (52 + (10 if has_regime else 0)))
    for p in result["period_results"]:
        bench  = f"{p['benchmark_ret']:+.2f}" if p["benchmark_ret"] is not None else "  N/A"
        alpha  = f"{p['alpha']:+.2f}"         if p["alpha"]         is not None else "  N/A"
        regime = p.get("regime", "NORMAL")
        exp    = p.get("exposure", 1.0)
        reg_s  = f"  {regime}({exp:.0%})" if has_regime else ""
        print(f"{p['period']:>7}  {p['portfolio_ret']:>+7.2f}%  {bench:>7}%  "
              f"{alpha:>7}%  {p['long_short_spread']:>+6.2f}%  {p['n_valid']:>4}{reg_s}")

    # Cumulative
    cum_p = result["cumulative_portfolio"]
    cum_b = result["cumulative_benchmark"]
    print("\nCumulative returns:")
    cum_strs = []
    for i, (cp, cb) in enumerate(zip(cum_p, cum_b)):
        cp_s = f"{cp:+.1f}%" if cp is not None else "N/A"
        cb_s = f"{cb:+.1f}%" if cb is not None else "N/A"
        cum_strs.append(f"  P{i+1}: port={cp_s} bench={cb_s}")
    print("\n".join(cum_strs))

    # Stats summary
    print("\n" + "=" * 52)
    print("AGGREGATE STATS")
    print("=" * 52)
    for key, val in stats.items():
        if val is None:
            continue
        print(f"  {key:<35}  {val}")

    # Monotonicity check (decile returns)
    print("\nDecile returns (top->bottom, averaged across periods):")
    n_deciles = 5
    agg_deciles = [[] for _ in range(n_deciles)]
    for p in result["period_results"]:
        for d, v in enumerate(p.get("decile_rets", [])):
            agg_deciles[d].append(v)
    decile_means = [round(np.mean(d), 2) for d in agg_deciles if d]
    labels = ["Top 20%", "Q2", "Q3", "Q4", "Bot 20%"]
    for label, val in zip(labels, decile_means):
        bar = "#" * max(0, int((val + 5) * 2))
        print(f"  {label:<9} {val:>+6.2f}%  {bar}")

    _print_bias_audit(meta)


def _print_bias_audit(meta: dict) -> None:
    """Print a structured backtest integrity checklist."""
    print("\n" + "=" * 62)
    print("BACKTEST INTEGRITY AUDIT")
    print("=" * 62)

    group   = meta.get("group", "A")
    fwd     = meta.get("forward_days", 20)
    step    = meta.get("step_days", 20)
    cost    = meta.get("txn_cost_pct", 0.0)
    n_per   = meta.get("n_periods", 0)
    liq     = meta.get("min_liquidity_wan", 0)

    def _chk(ok: bool, msg: str) -> None:
        print(f"  {'[✓]' if ok else '[!]'} {msg}")

    _chk(True, "Suspension: pre-signal 0-volume lookback applied")
    _chk(True, "Signal-day limit: |change_pct| ≥ 9.5% excluded before scoring")
    _chk(liq > 0,
         f"Liquidity: {'≥' + str(int(liq)) + '万/d ADV filter applied'  if liq > 0 else 'OFF — add --min-liquidity to filter illiquid stocks'}")
    _chk(group.upper() == "A",
         f"Look-ahead (group={group}): " + (
             "price-only, no structural look-ahead"
             if group.upper() == "A"
             else "[!] fundamental/social factors use CURRENT data for all historical periods"
         ))
    _chk(step >= fwd,
         f"Period overlap: step={step}d, fwd={fwd}d — " + (
             "non-overlapping"
             if step >= fwd
             else f"[!] {fwd - step}d overlap → serial correlation in IC series"
         ))
    _chk(cost > 0,
         f"Transaction cost: {cost:.2f}%/period" + (" applied" if cost > 0 else " — [!] MISSING"))
    _chk(n_per >= 8,
         f"Period count: {n_per}" + (
             " (≥8, statistically meaningful)"
             if n_per >= 8
             else " (<8, noise-dominated)"
         ))
    print("  [!] Survivorship: fixed universe does not exclude delisted stocks")
    print("  [!] PIT proxy: mandatory disclosure deadlines, not actual announcement dates")
    print("  [!] Execution: equal-weight full-fill; no market-impact or partial-fill model")
    print("  [!] walk-forward OOS: no true train/test split — all periods use current weights")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Long-only quantile backtest for A-share multi-factor model")
    parser.add_argument("--periods",           type=int,   default=12,   help="Number of backtest periods (default 12)")
    parser.add_argument("--fwd",               type=int,   default=20,   help="Forward return window in days (default 20)")
    parser.add_argument("--step",              type=int,   default=20,   help="Days between periods (default 20)")
    parser.add_argument("--top",               type=float, default=20.0, help="Long basket size as %% of universe (default 20)")
    parser.add_argument("--n",                 type=int,   default=50,   help="Universe size from TEST_UNIVERSE (default 50, ignored if --universe set)")
    parser.add_argument("--universe",          type=str,   default="",   help="Load stock universe from JSON file (overrides --n / TEST_UNIVERSE)")
    parser.add_argument("--group",             type=str,   default="A",  help="Factor group: A (fast) or AB (all)")
    parser.add_argument("--cost",              type=float, default=0.10, help="Round-trip transaction cost %% (default 0.10)")
    parser.add_argument("--out",               type=str,   default="",   help="Save full output to JSON file")
    parser.add_argument("--workers",           type=int,   default=4,    help="Thread pool size (default 4)")
    parser.add_argument("--no-regime",         action="store_true",      help="Disable market-regime exposure filter")
    parser.add_argument("--no-sector-neutral", action="store_true",      help="Disable sector neutralization (use raw composite scores)")
    parser.add_argument("--smallcap",          action="store_true",      help="Use small-cap regime weights (REGIME_WEIGHTS_SMALLCAP)")
    parser.add_argument("--min-liquidity",     type=float, default=0.0,  help="Min avg daily trading amount in 万元 (default 0 = disabled)")
    args = parser.parse_args()

    if args.universe:
        uni_path = args.universe
        if not os.path.isabs(uni_path):
            uni_path = os.path.join(os.path.dirname(__file__), uni_path)
        with open(uni_path, encoding="utf-8") as f:
            codes = [str(c).zfill(6) for c in json.load(f)]
        print(f"Universe loaded from {uni_path}: {len(codes)} stocks")
    else:
        codes = TEST_UNIVERSE[:args.n]

    wt = REGIME_WEIGHTS_SMALLCAP if args.smallcap else REGIME_WEIGHTS

    result = run_backtest(
        codes            = codes,
        forward_days     = args.fwd,
        n_periods        = args.periods,
        step             = args.step,
        top_pct          = args.top / 100,
        txn_cost_pct     = args.cost,
        group            = args.group,
        max_workers      = args.workers,
        use_regime       = not args.no_regime,
        sector_neutral   = not args.no_sector_neutral,
        weights_table    = wt,
        min_liquidity_wan = args.min_liquidity,
    )

    _print_results(result)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\nFull results saved to {args.out}")
        # Run manifest for reproducibility
        meta = result.get("meta", {})
        manifest = {
            "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
            "params": {
                "periods": args.periods, "fwd": args.fwd, "step": args.step,
                "top_pct": args.top, "group": args.group, "cost": args.cost,
                "use_regime": not args.no_regime,
                "sector_neutral": not args.no_sector_neutral,
                "min_liquidity_wan": args.min_liquidity,
                "strategy": "smallcap" if args.smallcap else "main",
            },
            "data_stats": {
                "n_stocks": meta.get("n_stocks"),
                "n_periods_completed": meta.get("n_periods"),
            },
            "stats_summary": result.get("stats", {}),
            "signals_cache_dir": _SIGNALS_CACHE_DIR,
            "params_hash": hashlib.md5(json.dumps({
                "periods": args.periods, "fwd": args.fwd, "step": args.step,
                "top_pct": args.top, "group": args.group, "cost": args.cost,
            }, sort_keys=True).encode()).hexdigest()[:8],
        }
        manifest_path = args.out.rsplit(".", 1)[0] + ".manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"Run manifest saved to {manifest_path}")
