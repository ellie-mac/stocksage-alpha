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
                           REGIME_MA_SHORT, REGIME_MA_LONG,
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


def _detect_regime(regime_close: Optional[pd.Series],
                   price_offset: int,
                   weights_table: Optional[dict] = None) -> tuple[str, float, dict]:
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
    Returns (regime_name, exposure, factor_weights).
    """
    if weights_table is None:
        weights_table = REGIME_WEIGHTS
    default = ("NORMAL", REGIME_EXPOSURE["NORMAL"], weights_table["NORMAL"])
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
        # Structural downtrend: near-cash, use crisis weights for the small deployed portion
        return "BEAR", REGIME_EXPOSURE["BEAR"], weights_table["BEAR"]

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

    return r, REGIME_EXPOSURE[r], weights_table[r]


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

    # Load CSI 300 close series for regime filter
    regime_close: Optional[pd.Series] = None
    if use_regime:
        _rdf = fetcher.get_market_regime_data()
        if _rdf is not None and "close" in _rdf.columns:
            regime_close = pd.to_numeric(_rdf["close"], errors="coerce").dropna().reset_index(drop=True)

    period_results: list[dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for period_idx in range(n_periods):
            price_offset = period_idx * step
            print(f"  Period {period_idx + 1}/{n_periods}  (price_offset={price_offset}d)")

            futures = {
                ex.submit(compute_stock_scores, code, forward_days, group, price_offset): code
                for code in codes
            }

            # Detect regime BEFORE scoring so we use the right factor weights
            if use_regime:
                regime_name, exposure, regime_wts = _detect_regime(regime_close, price_offset, weights_table)
            else:
                wt = (weights_table or REGIME_WEIGHTS)["NORMAL"]
                regime_name, exposure, regime_wts = "NORMAL", 1.0, wt

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
                            "code":         code,
                            "composite":    comp,
                            "forward_ret":  r.get("forward_ret"),
                        })
                    except Exception:
                        pass
            except concurrent.futures.TimeoutError:
                print(f"    Warning: period timed out after {per_period_timeout}s, "
                      f"collected {len(period_rows)} stocks so far")

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

            long_basket  = df_period.head(top_n)
            short_basket = df_period.tail(top_n)  # bottom quantile for reference

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
            "n_stocks":      n_stocks,
            "n_periods":     len(period_results),
            "forward_days":  forward_days,
            "step_days":     step,
            "top_pct":       top_pct,
            "top_n":         top_n,
            "txn_cost_pct":  txn_cost_pct,
            "group":         group,
            "use_regime":    use_regime,
            "sector_neutral": sector_neutral,
            "strategy":      strategy_label,
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
    beat_bench  = np.array(alphas) > 0 if alphas else np.array([])
    win_rate    = float(np.mean(beat_bench)) * 100 if len(beat_bench) > 0 else 0.0

    annualized  = (float(np.prod(1 + arr / 100)) ** (periods_per_year / len(arr)) - 1) * 100

    mean_alpha  = float(np.mean(alphas))  if alphas  else None
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
    """Compound a list of period returns into cumulative returns (0-based)."""
    result: list[Optional[float]] = []
    cum = 1.0
    for r in rets:
        if r is None:
            result.append(None)
        else:
            cum *= (1 + r / 100)
            result.append(round((cum - 1) * 100, 3))
    return result


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
        codes          = codes,
        forward_days   = args.fwd,
        n_periods      = args.periods,
        step           = args.step,
        top_pct        = args.top / 100,
        txn_cost_pct   = args.cost,
        group          = args.group,
        max_workers    = args.workers,
        use_regime     = not args.no_regime,
        sector_neutral = not args.no_sector_neutral,
        weights_table  = wt,
    )

    _print_results(result)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\nFull results saved to {args.out}")
