#!/usr/bin/env python3
"""
ETF 择时回测框架 (etf_backtest.py)

针对场内 ETF/LOF 的多因子择时回测。与 backtest.py 的核心区别：
  - 仅使用价格/成交量因子（ETF 无基本面数据）
  - 无行业中性化（ETF 本身已代表行业/主题）
  - 使用 FACTOR_WEIGHTS_ETF（factor_config.py）专项权重
  - 基准：ETF 池全部等权重买入持有

Universe: alert_config.json → etf_watchlist，或 --universe 指定 JSON 文件
Strategy: 每 step 天排名，做多得分最高的 top_pct ETF（等权重）

Usage:
  python scripts/etf_backtest.py                               # 12 期 × 10d 前向
  python scripts/etf_backtest.py --periods 16 --fwd 5         # 16 期，5 日收益
  python scripts/etf_backtest.py --top 30 --out data/etf_backtest_16p.json
  python scripts/etf_backtest.py --universe my_etfs.json --periods 8
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Fast connect timeout so hung APIs don't stall the backtest
import requests as _requests
_orig = _requests.Session.request
def _patched(self, method, url, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = (5, 25)
    return _orig(self, method, url, **kwargs)
_requests.Session.request = _patched

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

import fetcher
from factor_analysis import compute_stock_scores
from factor_config import (
    FACTOR_WEIGHTS_ETF,
    REGIME_MA_SHORT, REGIME_MA_LONG,
    REGIME_EXPOSURE, REGIME_WEIGHTS,
    REGIME_CAUTION_THRESHOLD, REGIME_CRISIS_THRESHOLD,
    REGIME_BULL_THRESHOLD, REGIME_EXTREME_BULL_THRESHOLD,
)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_ROOT, "alert_config.json")


# ---------------------------------------------------------------------------
# Universe loader
# ---------------------------------------------------------------------------

def load_etf_universe(universe_file: Optional[str] = None) -> list[dict]:
    """
    Load ETF list.
    If universe_file is given, load from that JSON file (list of {code, name} dicts).
    Otherwise load from alert_config.json → etf_watchlist.
    Returns list of {code, name} dicts.
    """
    if universe_file:
        with open(universe_file, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            # Support both [{code, name}] and [code_str] formats
            result = []
            for item in data:
                if isinstance(item, dict):
                    result.append({"code": item["code"], "name": item.get("name", item["code"])})
                else:
                    result.append({"code": str(item), "name": str(item)})
            return result
        raise ValueError(f"Universe file must be a JSON list, got {type(data)}")

    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    etf_list = cfg.get("etf_watchlist", [])
    if not etf_list:
        raise ValueError("alert_config.json 中 etf_watchlist 为空，请先填入 ETF 列表")
    return [{"code": e["code"], "name": e.get("name", e["code"])} for e in etf_list]


# ---------------------------------------------------------------------------
# Composite score (ETF weights)
# ---------------------------------------------------------------------------

def _composite_etf_score(factor_scores: dict,
                          weights: Optional[dict] = None) -> Optional[float]:
    """
    Weighted composite score using FACTOR_WEIGHTS_ETF.
    Ignores factors with weight=0 or missing data.
    Returns None if fewer than 3 active factors.
    """
    if weights is None:
        weights = FACTOR_WEIGHTS_ETF
    exclude = {"forward_ret", "code"}
    weighted_sum = 0.0
    weight_total = 0.0
    n_active = 0
    for k, v in factor_scores.items():
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


# ---------------------------------------------------------------------------
# Regime detection (reuses same CSI 300 logic as backtest.py)
# ---------------------------------------------------------------------------

def _detect_regime(regime_close: Optional[pd.Series],
                   price_offset: int) -> tuple[str, float, dict]:
    default = ("NORMAL", REGIME_EXPOSURE["NORMAL"], REGIME_WEIGHTS["NORMAL"])
    if regime_close is None:
        return default

    ma60_lookback = 60
    lookback = 20
    needed = price_offset + max(lookback, ma60_lookback) + 2
    if len(regime_close) < needed:
        return default

    end_px = float(regime_close.iloc[-(price_offset + 1)])
    ma60_start = max(0, len(regime_close) - (price_offset + ma60_lookback + 1))
    ma60_end   = len(regime_close) - price_offset
    ma60 = float(regime_close.iloc[ma60_start:ma60_end].mean())

    if end_px < ma60:
        return "BEAR", REGIME_EXPOSURE["BEAR"], REGIME_WEIGHTS["BEAR"]

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

    return r, REGIME_EXPOSURE[r], REGIME_WEIGHTS[r]


# ---------------------------------------------------------------------------
# Benchmark: equal-weight all ETFs in universe
# ---------------------------------------------------------------------------

def _get_etf_benchmark_returns(
    codes: list[str],
    forward_days: int,
    n_periods: int,
    step: int,
    max_workers: int = 4,
) -> list[Optional[float]]:
    """
    Equal-weight forward return for the full ETF pool at each period.
    Used as the "hold-all" baseline.
    """
    def _fwd_ret(code: str, price_offset: int) -> Optional[float]:
        try:
            needed = max(400, 300 + forward_days + price_offset + 10)
            df = fetcher.get_price_history(code, needed)
            if df is None or len(df) < forward_days + price_offset + 5:
                return None
            close = df["close"]
            total_skip = forward_days + price_offset
            return float(
                (close.iloc[-(price_offset + 1)] - close.iloc[-(total_skip + 1)]) /
                close.iloc[-(total_skip + 1)] * 100
            )
        except Exception:
            return None

    results: list[Optional[float]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for period_idx in range(n_periods):
            price_offset = period_idx * step
            futs = {ex.submit(_fwd_ret, code, price_offset): code for code in codes}
            rets = []
            for fut in as_completed(futs, timeout=120):
                try:
                    v = fut.result(timeout=0)
                    if v is not None:
                        rets.append(v)
                except Exception:
                    pass
            results.append(round(float(np.mean(rets)), 3) if rets else None)
    return results


# ---------------------------------------------------------------------------
# Factor IC computation (cross-sectional Spearman across all periods)
# ---------------------------------------------------------------------------

def _compute_factor_ic(all_period_rows: list[list[dict]]) -> dict:
    """
    Compute mean Spearman IC for each factor across all periods.
    Returns {factor_name: {mean_ic, icir, n_periods}}.
    """
    from scipy.stats import spearmanr

    factor_ic_by_period: dict[str, list[float]] = {}

    for period_rows in all_period_rows:
        if len(period_rows) < 4:
            continue
        forward_rets = [r.get("forward_ret") for r in period_rows]
        # Collect all factor names
        all_factors = set()
        for row in period_rows:
            all_factors.update(k for k in row if k not in ("code", "forward_ret", "composite")
                               and not k.startswith("sell_score_"))

        for fname in all_factors:
            scores = [r.get(fname) for r in period_rows]
            pairs = [(s, f) for s, f in zip(scores, forward_rets)
                     if s is not None and f is not None
                     and not np.isnan(float(s)) and not np.isnan(float(f))]
            if len(pairs) < 4:
                continue
            s_arr = [p[0] for p in pairs]
            f_arr = [p[1] for p in pairs]
            try:
                ic, _ = spearmanr(s_arr, f_arr)
                if not np.isnan(ic):
                    factor_ic_by_period.setdefault(fname, []).append(float(ic))
            except Exception:
                pass

    result = {}
    for fname, ics in factor_ic_by_period.items():
        if not ics:
            continue
        arr = np.array(ics)
        mean_ic = float(np.mean(arr))
        icir = float(mean_ic / np.std(arr, ddof=1)) if len(arr) > 1 and np.std(arr, ddof=1) > 0 else 0.0
        result[fname] = {
            "mean_ic": round(mean_ic, 4),
            "icir":    round(icir, 3),
            "n_periods": len(ics),
        }
    return result


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_etf_backtest(
    etfs: list[dict],
    forward_days: int = 10,
    n_periods: int = 12,
    step: int = 20,
    top_pct: float = 0.30,
    txn_cost_pct: float = 0.05,  # ETFs have lower transaction cost (~0.05% one-way)
    max_workers: int = 4,
    use_regime: bool = True,
) -> dict:
    """
    Run ETF timing backtest.

    At each cross-section (price_offset days ago):
      1. Score all ETFs using price/volume factors (FACTOR_WEIGHTS_ETF)
      2. Apply market regime exposure filter
      3. Go long top-N ETFs by composite score (equal weight)
      4. Compare vs equal-weight hold-all benchmark

    Returns per-period results, aggregate stats, and factor IC table.
    """
    codes = [e["code"] for e in etfs]
    name_map = {e["code"]: e["name"] for e in etfs}
    n_etfs = len(codes)
    top_n  = max(1, int(n_etfs * top_pct))

    print(f"ETF backtest: {n_etfs} ETFs, {n_periods} periods × {step}d step")
    print(f"Forward: {forward_days}d | Top {top_pct*100:.0f}% ({top_n} ETFs) | "
          f"Txn cost: {txn_cost_pct:.2f}% | Regime filter: {'ON' if use_regime else 'OFF'}\n")

    # Load CSI 300 for regime detection
    regime_close: Optional[pd.Series] = None
    if use_regime:
        rdf = fetcher.get_market_regime_data()
        if rdf is not None and "close" in rdf.columns:
            regime_close = pd.to_numeric(rdf["close"], errors="coerce").dropna().reset_index(drop=True)

    # Pre-compute equal-weight benchmark returns for all periods
    print("Computing equal-weight benchmark returns...")
    benchmark_rets = _get_etf_benchmark_returns(codes, forward_days, n_periods, step, max_workers)
    print()

    period_results: list[dict] = []
    all_period_rows: list[list[dict]] = []  # for IC computation

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for period_idx in range(n_periods):
            price_offset = period_idx * step
            print(f"  Period {period_idx + 1}/{n_periods}  (price_offset={price_offset}d)")

            # Detect regime
            if use_regime:
                regime_name, exposure, regime_wts = _detect_regime(regime_close, price_offset)
            else:
                regime_name, exposure, regime_wts = "NORMAL", 1.0, FACTOR_WEIGHTS_ETF

            # Score all ETFs in parallel
            futures = {
                ex.submit(compute_stock_scores, code, forward_days, "A", price_offset): code
                for code in codes
            }

            period_rows: list[dict] = []
            try:
                for future in as_completed(futures, timeout=n_etfs * 90):
                    code = futures[future]
                    try:
                        r = future.result(timeout=0)
                        if r is None:
                            continue
                        # Use ETF-specific weights for composite score
                        comp = _composite_etf_score(r, FACTOR_WEIGHTS_ETF)
                        if comp is None:
                            continue
                        row = {"code": code, "composite": comp,
                               "forward_ret": r.get("forward_ret")}
                        # Store individual factor scores for IC analysis
                        row.update({k: v for k, v in r.items()
                                    if k not in ("forward_ret", "code")})
                        period_rows.append(row)
                    except Exception:
                        pass
            except concurrent.futures.TimeoutError:
                print(f"    Warning: period timed out, got {len(period_rows)} ETFs")

            if len(period_rows) < 3:
                print(f"    Skipped — only {len(period_rows)} ETFs scored\n")
                continue

            df = (pd.DataFrame(period_rows)
                    .dropna(subset=["composite", "forward_ret"])
                    .sort_values("composite", ascending=False)
                    .reset_index(drop=True))

            if len(df) < 3:
                print(f"    Skipped — only {len(df)} valid after dropna\n")
                continue

            all_period_rows.append(period_rows)

            long_basket  = df.head(top_n)
            short_basket = df.tail(top_n)

            basket_ret = float(long_basket["forward_ret"].mean())
            port_ret   = (basket_ret - txn_cost_pct) * exposure
            bench_ret  = benchmark_rets[period_idx]
            alpha      = round(port_ret - bench_ret, 3) if bench_ret is not None else None

            regime_tag = f" [{regime_name} {exposure:.0%}]" if regime_name != "NORMAL" else ""
            status = (f"port={port_ret:+.2f}%  bench={bench_ret:+.2f}%  alpha={alpha:+.2f}%{regime_tag}"
                      if bench_ret is not None else f"port={port_ret:+.2f}%{regime_tag}")
            print(f"    {status}")
            top_names = [name_map.get(c, c) for c in long_basket["code"].tolist()]
            print(f"    Top {top_n}: {', '.join(top_names[:6])}"
                  + ("..." if len(top_names) > 6 else ""))
            print()

            period_results.append({
                "period":         period_idx + 1,
                "price_offset_d": price_offset,
                "n_valid":        len(df),
                "exposure":       round(exposure, 2),
                "regime":         regime_name,
                "portfolio_ret":  round(port_ret, 3),
                "basket_ret":     round(basket_ret - txn_cost_pct, 3),
                "benchmark_ret":  bench_ret,
                "alpha":          alpha,
                "bottom_ret":     round(float(short_basket["forward_ret"].mean()), 3),
                "long_short_spread": round(
                    float(long_basket["forward_ret"].mean()) -
                    float(short_basket["forward_ret"].mean()), 3),
                "top_etfs":       long_basket["code"].tolist(),
                "top_names":      [name_map.get(c, c) for c in long_basket["code"].tolist()],
            })

    if not period_results:
        return {"error": "所有期次均失败 — ETF 数据不足"}

    # Aggregate stats
    port_rets  = [p["portfolio_ret"]    for p in period_results]
    bench_rets = [p["benchmark_ret"]    for p in period_results if p["benchmark_ret"] is not None]
    alphas     = [p["alpha"]            for p in period_results if p["alpha"] is not None]
    ls_spreads = [p["long_short_spread"] for p in period_results]

    stats = _compute_stats(port_rets, bench_rets, alphas, ls_spreads, forward_days)

    cum_port  = _cumulative(port_rets)
    cum_bench = _cumulative([p["benchmark_ret"] for p in period_results])

    # Factor IC across all periods
    print("Computing factor IC...")
    factor_ic = _compute_factor_ic(all_period_rows)
    print(f"  IC computed for {len(factor_ic)} factors\n")

    return {
        "meta": {
            "n_etfs":        n_etfs,
            "n_periods":     len(period_results),
            "forward_days":  forward_days,
            "step_days":     step,
            "top_pct":       top_pct,
            "top_n":         top_n,
            "txn_cost_pct":  txn_cost_pct,
            "use_regime":    use_regime,
            "etf_list":      [{"code": e["code"], "name": e["name"]} for e in etfs],
        },
        "period_results":       period_results,
        "cumulative_portfolio": cum_port,
        "cumulative_benchmark": cum_bench,
        "stats":                stats,
        "factor_ic":            factor_ic,
    }


# ---------------------------------------------------------------------------
# Stats helpers (same as backtest.py)
# ---------------------------------------------------------------------------

def _compute_stats(
    port_rets: list[float],
    bench_rets: list[float],
    alphas: list[float],
    ls_spreads: list[float],
    forward_days: int,
) -> dict:
    if not port_rets:
        return {}
    arr = np.array(port_rets)
    periods_per_year = 252 / forward_days
    mean_ret = float(np.mean(arr))
    std_ret  = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    sharpe   = mean_ret / std_ret * np.sqrt(periods_per_year) if std_ret > 0 else np.nan
    cum      = np.cumprod(1 + arr / 100)
    running_max = np.maximum.accumulate(cum)
    max_dd   = float(((cum - running_max) / running_max * 100).min())
    beat     = np.array(alphas) > 0 if alphas else np.array([])
    win_rate = float(np.mean(beat)) * 100 if len(beat) > 0 else 0.0
    annualized = (float(np.prod(1 + arr / 100)) ** (periods_per_year / len(arr)) - 1) * 100

    if bench_rets and len(bench_rets) == len(port_rets):
        excess = np.array(port_rets) - np.array(bench_rets)
        ir = float(np.mean(excess) / np.std(excess, ddof=1) * np.sqrt(periods_per_year)) \
             if np.std(excess, ddof=1) > 0 else np.nan
    else:
        ir = np.nan

    return {
        "mean_period_ret_pct":        round(mean_ret, 3),
        "annualized_ret_pct":         round(annualized, 2),
        "sharpe_ratio":               round(sharpe, 3) if not np.isnan(sharpe) else None,
        "information_ratio":          round(ir, 3) if not np.isnan(ir) else None,
        "max_drawdown_pct":           round(max_dd, 2),
        "win_rate_pct":               round(win_rate, 1),
        "mean_alpha_pct":             round(float(np.mean(alphas)), 3) if alphas else None,
        "mean_long_short_spread_pct": round(float(np.mean(ls_spreads)), 3) if ls_spreads else None,
        "n_periods":                  len(port_rets),
    }


def _cumulative(rets: list[Optional[float]]) -> list[Optional[float]]:
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
# CLI output
# ---------------------------------------------------------------------------

def _print_results(result: dict) -> None:
    if "error" in result:
        print(f"\nError: {result['error']}")
        return

    meta  = result["meta"]
    stats = result["stats"]

    print("\n" + "=" * 70)
    print("ETF BACKTEST RESULTS")
    print("=" * 70)
    print(f"Universe: {meta['n_etfs']} ETFs  |  "
          f"{meta['n_periods']} periods × {meta['step_days']}d step  |  "
          f"{meta['forward_days']}d forward")
    print(f"Long basket: top {meta['top_pct']*100:.0f}% ({meta['top_n']} ETFs)  |  "
          f"Txn cost: {meta['txn_cost_pct']:.2f}%/period\n")

    print(f"{'Period':>7} {'Port%':>8} {'Bench%':>8} {'Alpha%':>8} {'L/S%':>7} {'#ETF':>5}  Regime")
    print("-" * 68)
    for p in result["period_results"]:
        bench = f"{p['benchmark_ret']:+.2f}" if p["benchmark_ret"] is not None else "  N/A"
        alpha = f"{p['alpha']:+.2f}"         if p["alpha"]         is not None else "  N/A"
        print(f"{p['period']:>7}  {p['portfolio_ret']:>+7.2f}%  {bench:>7}%  "
              f"{alpha:>7}%  {p['long_short_spread']:>+6.2f}%  {p['n_valid']:>4}  "
              f"{p['regime']}({p['exposure']:.0%})")

    cum_p = result["cumulative_portfolio"]
    cum_b = result["cumulative_benchmark"]
    print("\nCumulative returns:")
    for i, (cp, cb) in enumerate(zip(cum_p, cum_b)):
        cp_s = f"{cp:+.1f}%" if cp is not None else "N/A"
        cb_s = f"{cb:+.1f}%" if cb is not None else "N/A"
        print(f"  P{i+1}: port={cp_s}  bench={cb_s}")

    print("\n" + "=" * 50)
    print("AGGREGATE STATS")
    print("=" * 50)
    for k, v in stats.items():
        if v is not None:
            print(f"  {k:<35}  {v}")

    # Top factor ICs
    fic = result.get("factor_ic", {})
    if fic:
        valid = [(n, d) for n, d in fic.items() if d.get("mean_ic") is not None]
        sorted_fic = sorted(valid, key=lambda x: abs(x[1]["mean_ic"]), reverse=True)
        print("\nTop 10 factors by |IC| (ETF-specific):")
        for name, d in sorted_fic[:10]:
            print(f"  {name:<28}  IC={d['mean_ic']:+.4f}  ICIR={d['icir']:+.3f}  n={d['n_periods']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF timing backtest using price/volume factors")
    parser.add_argument("--periods",   type=int,   default=12,   help="Backtest periods (default 12)")
    parser.add_argument("--fwd",       type=int,   default=10,   help="Forward return window in days (default 10)")
    parser.add_argument("--step",      type=int,   default=20,   help="Days between periods (default 20)")
    parser.add_argument("--top",       type=float, default=30.0, help="Long basket size as %% of universe (default 30)")
    parser.add_argument("--cost",      type=float, default=0.05, help="Round-trip transaction cost %% (default 0.05)")
    parser.add_argument("--workers",   type=int,   default=4,    help="Thread pool size (default 4)")
    parser.add_argument("--no-regime", action="store_true",      help="Disable market-regime exposure filter")
    parser.add_argument("--universe",  type=str,   default="",   help="Custom ETF list JSON file (overrides etf_watchlist)")
    parser.add_argument("--out",       type=str,   default="",   help="Save full output to JSON file")
    args = parser.parse_args()

    etfs = load_etf_universe(args.universe if args.universe else None)
    print(f"Loaded {len(etfs)} ETFs from {'custom file' if args.universe else 'alert_config.json'}")

    result = run_etf_backtest(
        etfs=etfs,
        forward_days=args.fwd,
        n_periods=args.periods,
        step=args.step,
        top_pct=args.top / 100,
        txn_cost_pct=args.cost,
        max_workers=args.workers,
        use_regime=not args.no_regime,
    )

    _print_results(result)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\nResults saved → {args.out}")
