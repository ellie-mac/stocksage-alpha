#!/usr/bin/env python3
"""
Factor IC (Information Coefficient) backtesting framework.

Methodology:
  For each stock in the test universe, compute factor scores using price/financial
  data as of T-N days, then compare against actual N-day forward return.
  IC = Spearman rank correlation between factor scores and forward returns.

  IC > 0.05  with ICIR > 0.5 = economically meaningful factor
  IC > 0.03  with ICIR > 0.3 = weak but present signal
  |IC| < 0.02              = noise

Rolling mode (--rolling K):
  Runs K cross-sectional periods evenly spaced --step days apart, then reports
  mean IC and ICIR = mean(IC) / std(IC) across periods.  Higher ICIR means the
  factor is consistently predictive, not just a one-period fluke.

  Note: price-based factors get valid historical estimates; factors that rely on
  financial statements / analyst revisions / social data always use current data
  (look-ahead bias for those factors in rolling mode).

Usage:
  python factor_analysis.py                              # single period, 50 stocks, 20d
  python factor_analysis.py --n 100 --fwd 10
  python factor_analysis.py --group A                    # price factors only (fast)
  python factor_analysis.py --rolling 6 --step 20        # 6 periods × 20d step
  python factor_analysis.py --rolling 12 --step 20 --group A --out rolling.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

import fetcher
from factors import (
    score_value, score_growth, score_momentum, score_quality,
    score_northbound, score_volume_breakout, score_52w_position,
    score_dividend_yield, score_volume_ratio, score_ma_alignment,
    score_low_volatility,
)
from factors_extended import (
    score_reversal, score_accruals, score_asset_growth, score_piotroski,
    score_short_interest, score_rsi_signal, score_macd_signal,
    score_turnover_percentile, score_chip_distribution,
    score_limit_hits, score_price_inertia,
    # Group B
    score_shareholder_change, score_lhb, score_lockup_pressure,
    score_insider, score_institutional_visits, score_industry_momentum,
    score_northbound_actual, score_earnings_revision,
    score_social_heat, score_market_regime, score_concept_momentum,
)


# ---------------------------------------------------------------------------
# Test universe — diversified 80-stock A-share sample
# ---------------------------------------------------------------------------
TEST_UNIVERSE = [
    # Consumer staples
    "600519", "000858", "600276", "600887", "603288", "000568",
    # Finance
    "601318", "601166", "600036", "601398", "000001", "600030",
    # Healthcare
    "000538", "002415", "300760", "600196", "002555", "300015",
    # Technology / semiconductors
    "688981", "002230", "000725", "688036", "300059", "002241",
    # Industry / manufacturing
    "000333", "600031", "601899", "600585", "603816",
    # Energy / utilities
    "600900", "601985", "600028", "601857", "600019", "601088",
    # Real estate
    "000002", "600048", "001979", "600606", "000069",
    # Retail / e-commerce
    "002304", "600690", "601888", "000895",
    # Auto
    "600104", "000625", "601238", "002594",
    # New energy
    "300750", "601012", "002460", "300274", "688599",
    # Telecom / media
    "600050", "000063", "002475",
    # Chemical
    "600309", "000792", "002648",
    # Banks extra
    "600016", "600015", "601328",
]
# De-duplicate and normalise
TEST_UNIVERSE = list(dict.fromkeys(c.zfill(6) for c in TEST_UNIVERSE))[:80]


# ---------------------------------------------------------------------------
# Factor registry: name -> callable(code, price_df, financial_df, ...) -> float
# Each wrapper returns a single float score (or NaN on failure).
# ---------------------------------------------------------------------------

def _safe(fn, *args, **kwargs) -> float:
    try:
        result = fn(*args, **kwargs)
        return float(result.get("score", np.nan)) if isinstance(result, dict) else float(result)
    except Exception:
        return np.nan


def _safe_sell(fn, *args, **kwargs) -> float:
    """Like _safe but returns the sell_score field."""
    try:
        result = fn(*args, **kwargs)
        if isinstance(result, dict):
            return float(result.get("sell_score", np.nan))
        return np.nan
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# Per-stock score computation
# ---------------------------------------------------------------------------

def compute_stock_scores(code: str, forward_days: int, group: str, price_offset: int = 0) -> Optional[dict]:
    """
    Fetch all data for one stock, compute all factor scores and forward return.
    Returns a flat dict {factor_name: score, "forward_ret": float} or None on failure.

    price_offset: shift the price window back by this many additional days.
      - price_df uses rows up to -(forward_days + price_offset) from the end
      - forward return = return from -(forward_days + price_offset + 1) to -(price_offset + 1)
      Used by rolling mode to simulate historical cross-sections.
    """
    try:
        history_needed = max(400, 300 + forward_days + price_offset + 10)
        price_df_full = fetcher.get_price_history(code, history_needed)
        total_skip = forward_days + price_offset
        if price_df_full is None or len(price_df_full) < total_skip + 30:
            return None

        # Simulate "as of (forward_days + price_offset) days ago"
        price_df = price_df_full.iloc[:-total_skip].copy()
        close    = price_df_full["close"]
        forward_ret = float(
            (close.iloc[-(price_offset + 1)] - close.iloc[-(total_skip + 1)]) /
            close.iloc[-(total_skip + 1)] * 100
        )

        # Fetch supporting data (uses cache so repeated calls are free)
        quote        = fetcher.get_realtime_quote(code) or {}
        financial_df = fetcher.get_financial_indicators(code)
        val_history  = fetcher.get_valuation_history(code)
        fund_flow_df = fetcher.get_fund_flow(code, 10)
        margin_df    = fetcher.get_margin_data(code)
        circ_cap     = quote.get("circulating_cap", 0) or 0

        scores: dict[str, float] = {"forward_ret": forward_ret}

        # ── Core 7 ──────────────────────────────────────────────────────
        scores["value"]       = _safe(score_value,
                                       quote.get("pe_ttm", 0), quote.get("pb", 0),
                                       val_history, None, price_df)
        scores["growth"]      = _safe(score_growth, financial_df)
        scores["momentum"]    = _safe(score_momentum, price_df, financial_df)
        scores["quality"]     = _safe(score_quality, financial_df, price_df)
        scores["northbound"]  = _safe(score_northbound, fund_flow_df)
        scores["volume"]      = _safe(score_volume_breakout, price_df)
        scores["position_52w"] = _safe(score_52w_position, price_df)

        # Sell scores for core 7
        scores["sell_score_value"]       = _safe_sell(score_value,
                                                       quote.get("pe_ttm", 0), quote.get("pb", 0),
                                                       val_history, None, price_df)
        scores["sell_score_growth"]      = _safe_sell(score_growth, financial_df)
        scores["sell_score_momentum"]    = _safe_sell(score_momentum, price_df, financial_df)
        scores["sell_score_quality"]     = _safe_sell(score_quality, financial_df, price_df)
        scores["sell_score_northbound"]  = _safe_sell(score_northbound, fund_flow_df)
        scores["sell_score_volume"]      = _safe_sell(score_volume_breakout, price_df)
        scores["sell_score_position_52w"] = _safe_sell(score_52w_position, price_df)

        # ── Ext-A spot/price ─────────────────────────────────────────────
        scores["div_yield"]           = _safe(score_dividend_yield, quote.get("div_yield", 0), financial_df)
        scores["volume_ratio"]        = _safe(score_volume_ratio, quote.get("volume_ratio", 0), quote.get("change_pct", 0))
        scores["ma_alignment"]        = _safe(score_ma_alignment, price_df)
        scores["low_volatility"]      = _safe(score_low_volatility, price_df)
        scores["reversal"]            = _safe(score_reversal, price_df, financial_df)
        scores["accruals"]            = _safe(score_accruals, financial_df)
        scores["asset_growth"]        = _safe(score_asset_growth, financial_df)
        scores["piotroski"]           = _safe(score_piotroski, financial_df, price_df)
        scores["short_interest"]      = _safe(score_short_interest, margin_df, circ_cap, price_df)
        scores["rsi_signal"]          = _safe(score_rsi_signal, price_df)
        scores["macd_signal"]         = _safe(score_macd_signal, price_df)
        scores["turnover_percentile"]  = _safe(score_turnover_percentile, price_df)
        scores["chip_distribution"]    = _safe(score_chip_distribution, price_df, fund_flow_df)
        scores["limit_hits"]           = _safe(score_limit_hits, price_df, financial_df)
        scores["price_inertia"]        = _safe(score_price_inertia, price_df)

        # Sell scores for Ext-A
        scores["sell_score_div_yield"]          = _safe_sell(score_dividend_yield, quote.get("div_yield", 0), financial_df)
        scores["sell_score_volume_ratio"]       = _safe_sell(score_volume_ratio, quote.get("volume_ratio", 0), quote.get("change_pct", 0))
        scores["sell_score_ma_alignment"]       = _safe_sell(score_ma_alignment, price_df)
        scores["sell_score_low_volatility"]     = _safe_sell(score_low_volatility, price_df)
        scores["sell_score_reversal"]           = _safe_sell(score_reversal, price_df, financial_df)
        scores["sell_score_accruals"]           = _safe_sell(score_accruals, financial_df)
        scores["sell_score_asset_growth"]       = _safe_sell(score_asset_growth, financial_df)
        scores["sell_score_piotroski"]          = _safe_sell(score_piotroski, financial_df, price_df)
        scores["sell_score_short_interest"]     = _safe_sell(score_short_interest, margin_df, circ_cap, price_df)
        scores["sell_score_rsi_signal"]         = _safe_sell(score_rsi_signal, price_df)
        scores["sell_score_macd_signal"]        = _safe_sell(score_macd_signal, price_df)
        scores["sell_score_turnover_percentile"] = _safe_sell(score_turnover_percentile, price_df)
        scores["sell_score_chip_distribution"]   = _safe_sell(score_chip_distribution, price_df, fund_flow_df)
        scores["sell_score_limit_hits"]          = _safe_sell(score_limit_hits, price_df, financial_df)
        scores["sell_score_price_inertia"]       = _safe_sell(score_price_inertia, price_df)

        # ── Ext-B (only if group includes B) ─────────────────────────────
        if "B" in group.upper():
            shareholder_df = fetcher.get_shareholder_count(code)
            lhb_df         = fetcher.get_lhb_flow(code)
            lockup_df      = fetcher.get_lockup_pressure(code)
            insider_df     = fetcher.get_insider_transactions(code)
            visits_df      = fetcher.get_institutional_visits(code)
            nb_df          = fetcher.get_northbound_holdings(code)
            revision_df    = fetcher.get_earnings_revision(code)
            market_ret     = fetcher.get_market_return_1m()
            social_dict    = fetcher.get_social_heat(code)
            market_regime_df = fetcher.get_market_regime_data()

            # Re-compute with revision_df now available
            scores["value"]                    = _safe(score_value, quote.get("pe_ttm", 0), quote.get("pb", 0), val_history, None, price_df, revision_df, financial_df, market_ret_1m=market_ret)
            scores["sell_score_value"]         = _safe_sell(score_value, quote.get("pe_ttm", 0), quote.get("pb", 0), val_history, None, price_df, revision_df, financial_df, market_ret_1m=market_ret)

            # Re-compute quality and piotroski with pe_pct/pb_pct from value
            try:
                _val_full = score_value(quote.get("pe_ttm", 0), quote.get("pb", 0), val_history, None, price_df, revision_df, financial_df)
                _pe_pct   = _val_full.get("details", {}).get("pe_percentile")
                _pb_pct   = _val_full.get("details", {}).get("pb_percentile")
            except Exception:
                _pe_pct = _pb_pct = None
            scores["quality"]          = _safe(score_quality, financial_df, price_df, _pe_pct, _pb_pct)
            scores["sell_score_quality"] = _safe_sell(score_quality, financial_df, price_df, _pe_pct, _pb_pct)
            scores["piotroski"]          = _safe(score_piotroski, financial_df, price_df, _pe_pct, _pb_pct, revision_df=revision_df)
            scores["sell_score_piotroski"] = _safe_sell(score_piotroski, financial_df, price_df, _pe_pct, _pb_pct, revision_df=revision_df)

            scores["northbound"]               = _safe(score_northbound, fund_flow_df, price_df, revision_df=revision_df)
            scores["sell_score_northbound"]    = _safe_sell(score_northbound, fund_flow_df, price_df, revision_df=revision_df)
            scores["reversal"]                 = _safe(score_reversal, price_df, financial_df, revision_df)
            scores["sell_score_reversal"]      = _safe_sell(score_reversal, price_df, financial_df, revision_df)
            scores["short_interest"]           = _safe(score_short_interest, margin_df, circ_cap, price_df, revision_df)
            scores["sell_score_short_interest"] = _safe_sell(score_short_interest, margin_df, circ_cap, price_df, revision_df)
            scores["ma_alignment"]             = _safe(score_ma_alignment, price_df, revision_df)
            scores["sell_score_ma_alignment"]  = _safe_sell(score_ma_alignment, price_df, revision_df)
            scores["rsi_signal"]               = _safe(score_rsi_signal, price_df, revision_df=revision_df)
            scores["sell_score_rsi_signal"]    = _safe_sell(score_rsi_signal, price_df, revision_df=revision_df)
            scores["chip_distribution"]        = _safe(score_chip_distribution, price_df, fund_flow_df, social_dict=social_dict, revision_df=revision_df)
            scores["sell_score_chip_distribution"] = _safe_sell(score_chip_distribution, price_df, fund_flow_df, social_dict=social_dict, revision_df=revision_df)

            scores["shareholder_change"]   = _safe(score_shareholder_change, shareholder_df, price_df, revision_df)
            scores["lhb"]                  = _safe(score_lhb, lhb_df, price_df, revision_df=revision_df)
            scores["lockup_pressure"]      = _safe(score_lockup_pressure, lockup_df, circ_cap, price_df, financial_df, social_dict, market_ret_1m=market_ret, revision_df=revision_df)
            scores["insider"]              = _safe(score_insider, insider_df, price_df, revision_df)
            scores["institutional_visits"] = _safe(score_institutional_visits, visits_df, revision_df)
            scores["northbound_actual"]    = _safe(score_northbound_actual, nb_df, price_df, revision_df, None, market_ret, social_dict=social_dict)
            scores["social_heat"]          = _safe(score_social_heat, social_dict, price_df, financial_df, revision_df=revision_df)
            scores["market_regime"]        = _safe(score_market_regime, market_regime_df)

            concept_data = fetcher.get_concept_momentum(code)
            _best_concept_ret = max((c["ret_1m"] for c in concept_data), default=None) if concept_data else None
            scores["earnings_revision"]    = _safe(score_earnings_revision, revision_df, price_df, financial_df, visits_df, best_concept_ret=_best_concept_ret, social_dict=social_dict)
            _regime_score = scores.get("market_regime")
            _regime_float = float(_regime_score) if _regime_score is not None and not np.isnan(_regime_score) else None
            scores["concept_momentum"]     = _safe(score_concept_momentum, concept_data, price_df, _regime_float, financial_df, revision_df=revision_df)
            scores["reversal"]             = _safe(score_reversal, price_df, financial_df, revision_df, best_concept_ret=_best_concept_ret)
            scores["sell_score_reversal"]  = _safe_sell(score_reversal, price_df, financial_df, revision_df, best_concept_ret=_best_concept_ret)
            scores["volume"]               = _safe(score_volume_breakout, price_df, best_concept_ret=_best_concept_ret, revision_df=revision_df)
            scores["sell_score_volume"]    = _safe_sell(score_volume_breakout, price_df, best_concept_ret=_best_concept_ret, revision_df=revision_df)
            scores["macd_signal"]          = _safe(score_macd_signal, price_df, best_concept_ret=_best_concept_ret, revision_df=revision_df)
            scores["sell_score_macd_signal"] = _safe_sell(score_macd_signal, price_df, best_concept_ret=_best_concept_ret, revision_df=revision_df)

            # Re-compute regime- and valuation-dependent factors now that both are available
            scores["low_volatility"]       = _safe(score_low_volatility, price_df, _regime_float)
            scores["sell_score_low_volatility"] = _safe_sell(score_low_volatility, price_df, _regime_float)
            scores["growth"]               = _safe(score_growth, financial_df, _pe_pct, revision_df=revision_df)
            scores["sell_score_growth"]    = _safe_sell(score_growth, financial_df, _pe_pct, revision_df=revision_df)
            scores["momentum"]             = _safe(score_momentum, price_df, financial_df, _regime_float, revision_df=revision_df)
            scores["sell_score_momentum"]  = _safe_sell(score_momentum, price_df, financial_df, _regime_float, revision_df=revision_df)

            # Sell scores for Ext-B
            scores["sell_score_shareholder_change"]   = _safe_sell(score_shareholder_change, shareholder_df, price_df, revision_df)
            scores["sell_score_lhb"]                  = _safe_sell(score_lhb, lhb_df, price_df, revision_df=revision_df)
            scores["sell_score_lockup_pressure"]      = _safe_sell(score_lockup_pressure, lockup_df, circ_cap, price_df, financial_df, social_dict, market_ret_1m=market_ret, revision_df=revision_df)
            scores["sell_score_insider"]              = _safe_sell(score_insider, insider_df, price_df, revision_df)
            scores["sell_score_institutional_visits"] = _safe_sell(score_institutional_visits, visits_df, revision_df)
            scores["sell_score_northbound_actual"]    = _safe_sell(score_northbound_actual, nb_df, price_df, revision_df, None, market_ret, social_dict=social_dict)
            scores["sell_score_earnings_revision"]    = _safe_sell(score_earnings_revision, revision_df, price_df, financial_df, visits_df, social_dict=social_dict)
            scores["sell_score_social_heat"]          = _safe_sell(score_social_heat, social_dict, price_df, financial_df, revision_df=revision_df)
            scores["sell_score_market_regime"]        = _safe_sell(score_market_regime, market_regime_df)
            scores["sell_score_concept_momentum"]     = _safe_sell(score_concept_momentum, concept_data, price_df, _regime_float, financial_df, revision_df=revision_df)

            # Re-compute limit_hits with social_dict now available
            scores["limit_hits"]           = _safe(score_limit_hits, price_df, financial_df, social_dict, revision_df=revision_df)
            scores["sell_score_limit_hits"] = _safe_sell(score_limit_hits, price_df, financial_df, social_dict, revision_df=revision_df)

            # Industry momentum
            try:
                info = fetcher.get_stock_info(code) or {}
                ind  = info.get("industry", "")
                ind_ret = fetcher.get_industry_momentum(ind) if ind else None
                scores["industry_momentum"] = _safe(score_industry_momentum, ind_ret, market_ret, price_df, social_dict=social_dict)
                scores["sell_score_industry_momentum"] = _safe_sell(score_industry_momentum, ind_ret, market_ret, price_df, social_dict=social_dict)
                # Re-compute northbound_actual with full industry context
                scores["northbound_actual"]         = _safe(score_northbound_actual, nb_df, price_df, revision_df, ind_ret, market_ret, social_dict=social_dict)
                scores["sell_score_northbound_actual"] = _safe_sell(score_northbound_actual, nb_df, price_df, revision_df, ind_ret, market_ret, social_dict=social_dict)
                # Re-compute social_heat with industry context now available
                scores["social_heat"]               = _safe(score_social_heat, social_dict, price_df, financial_df, industry_ret_1m=ind_ret, market_ret_1m=market_ret, revision_df=revision_df)
                scores["sell_score_social_heat"]    = _safe_sell(score_social_heat, social_dict, price_df, financial_df, industry_ret_1m=ind_ret, market_ret_1m=market_ret, revision_df=revision_df)
                # Re-compute value with industry context
                scores["value"]                     = _safe(score_value, quote.get("pe_ttm", 0), quote.get("pb", 0), val_history, None, price_df, revision_df, financial_df, industry_ret_1m=ind_ret, market_ret_1m=market_ret)
                scores["sell_score_value"]          = _safe_sell(score_value, quote.get("pe_ttm", 0), quote.get("pb", 0), val_history, None, price_df, revision_df, financial_df, industry_ret_1m=ind_ret, market_ret_1m=market_ret)
                # Re-compute div_yield with industry context
                scores["div_yield"]                 = _safe(score_dividend_yield, quote.get("div_yield", 0), financial_df, _regime_float, price_df, industry_ret_1m=ind_ret, market_ret_1m=market_ret, revision_df=revision_df)
                scores["sell_score_div_yield"]      = _safe_sell(score_dividend_yield, quote.get("div_yield", 0), financial_df, _regime_float, price_df, industry_ret_1m=ind_ret, market_ret_1m=market_ret, revision_df=revision_df)
            except Exception:
                scores["industry_momentum"] = np.nan
                scores["sell_score_industry_momentum"] = np.nan

        return scores

    except Exception as e:
        return None


# ---------------------------------------------------------------------------
# IC computation
# ---------------------------------------------------------------------------

def spearman_ic(factor_scores: pd.Series, forward_returns: pd.Series) -> tuple[float, float]:
    """Returns (IC, p_value). Requires scipy."""
    try:
        from scipy.stats import spearmanr
        mask = factor_scores.notna() & forward_returns.notna()
        if mask.sum() < 10:
            return np.nan, np.nan
        ic, pval = spearmanr(factor_scores[mask], forward_returns[mask])
        return float(ic), float(pval)
    except ImportError:
        # Fallback without scipy
        mask = factor_scores.notna() & forward_returns.notna()
        if mask.sum() < 10:
            return np.nan, np.nan
        ic = float(factor_scores[mask].corr(forward_returns[mask], method="spearman"))
        return ic, np.nan


def ic_summary(ic: float, n: int, pval: float = np.nan) -> dict:
    """Compute t-stat and quality label from a single cross-sectional IC.

    pval: p-value from spearman_ic(); used to append significance marker to quality.
    """
    t_stat = ic * np.sqrt(n - 2) / np.sqrt(1 - ic ** 2) if abs(ic) < 1 else np.nan
    mag = (
        "strong"   if abs(ic) >= 0.08 else
        "moderate" if abs(ic) >= 0.05 else
        "weak"     if abs(ic) >= 0.02 else
        "noise"
    )
    if not np.isnan(pval):
        sig = "p<0.01" if pval < 0.01 else "p<0.05" if pval < 0.05 else "p≥0.05"
        quality = f"{mag} ({sig})"
    else:
        quality = mag
    direction = "positive ✓" if ic > 0 else "inverted ✗"
    return {
        "ic":        round(ic, 4) if not np.isnan(ic) else None,
        "t_stat":    round(t_stat, 2) if not np.isnan(t_stat) else None,
        "p_value":   round(pval, 4) if not np.isnan(pval) else None,
        "n_stocks":  n,
        "quality":   quality,
        "direction": direction,
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_analysis(
    codes: list[str] = TEST_UNIVERSE,
    forward_days: int = 20,
    group: str = "A",
    max_workers: int = 8,
    rolling: int = 1,
    step: int = 20,
) -> dict:
    """Run factor IC analysis across the test universe.

    rolling > 1: delegates to _run_rolling() for multi-period ICIR analysis.
    """
    if rolling > 1:
        return _run_rolling(codes, forward_days, group, max_workers, rolling, step)

    print(f"Running IC analysis: {len(codes)} stocks, {forward_days}d forward, group={group}")
    print("Fetching data concurrently...\n")

    results: list[dict] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(compute_stock_scores, code, forward_days, group): code
                   for code in codes}
        for i, future in enumerate(as_completed(futures), 1):
            code = futures[future]
            try:
                r = future.result(timeout=60)
                if r is not None:
                    r["code"] = code
                    results.append(r)
                else:
                    errors += 1
            except Exception:
                errors += 1
            if i % 10 == 0:
                print(f"  Progress: {i}/{len(codes)} (errors: {errors})")

    print(f"\nData collected: {len(results)} stocks, {errors} failed\n")
    if len(results) < 10:
        return {"error": "Insufficient data for IC analysis", "collected": len(results)}

    df = pd.DataFrame(results)
    forward_ret = df["forward_ret"]

    factor_cols = [c for c in df.columns if c not in ("code", "forward_ret")]

    ic_results: dict[str, dict] = {}
    for factor in sorted(factor_cols):
        scores = df[factor]
        valid  = scores.notna() & forward_ret.notna()
        n      = int(valid.sum())
        if n < 10:
            ic_results[factor] = {"ic": None, "t_stat": None, "n_stocks": n,
                                  "quality": "insufficient data", "direction": "N/A"}
            continue
        ic, pval = spearman_ic(scores, forward_ret)
        ic_results[factor] = ic_summary(ic, n, pval)

    # Sort by abs(IC) descending
    ic_results = dict(sorted(ic_results.items(),
                              key=lambda x: abs(x[1].get("ic") or 0), reverse=True))

    # Weight recommendations based on IC magnitude and direction
    weight_recs = _recommend_weights(ic_results)

    return {
        "meta": {
            "n_stocks":    len(results),
            "forward_days": forward_days,
            "group":       group,
        },
        "ic_table":        ic_results,
        "weight_recommendations": weight_recs,
        "forward_return_stats": {
            "mean_pct":   round(float(forward_ret.mean()), 2),
            "median_pct": round(float(forward_ret.median()), 2),
            "std_pct":    round(float(forward_ret.std()), 2),
        },
    }


def _recommend_weights(stats_table: dict, ic_field: str = "ic") -> dict:
    """
    Suggest FactorWeights multipliers based on IC quality.
    Works for both single-period (ic_field="ic") and rolling (ic_field="mean_ic").

    quality strings may include a p-value suffix (e.g. "strong (p<0.01)"),
    so comparisons use startswith rather than exact equality.
    """
    recs: dict[str, float] = {}
    for factor, stats in stats_table.items():
        ic = stats.get(ic_field)
        if ic is None:
            recs[factor] = 0.1  # unknown — keep minimal
            continue
        quality   = stats.get("quality", "noise")
        direction = stats.get("direction", "")

        if "inverted" in direction:
            recs[factor] = 0.0 if quality.startswith(("strong", "moderate")) else 0.1
        elif quality.startswith("strong"):
            recs[factor] = 2.0
        elif quality.startswith("moderate"):
            recs[factor] = 1.0
        elif quality.startswith("weak"):
            recs[factor] = 0.5
        else:
            recs[factor] = 0.2  # noise — keep but minimal weight

    return recs


# ---------------------------------------------------------------------------
# Rolling IC runner
# ---------------------------------------------------------------------------

def _run_rolling(
    codes: list[str],
    forward_days: int,
    group: str,
    max_workers: int,
    n_periods: int,
    step: int,
) -> dict:
    """
    Run n_periods cross-sectional IC evaluations, each shifted `step` trading days
    further back in time.  Period 0 = most recent (price_offset = 0), period k =
    price_offset k*step.

    Returns aggregated mean_IC and ICIR per factor, plus per-period detail.
    """
    print(f"Rolling IC: {n_periods} periods × {step}d step, {forward_days}d forward, group={group}\n")

    # Collect IC per period
    # period_ics[factor][period_idx] = ic_value
    period_ics: dict[str, list[float]] = {}
    period_meta: list[dict] = []

    # Reuse a single thread pool across all periods to avoid repeated creation overhead
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for period_idx in range(n_periods):
            price_offset = period_idx * step
            print(f"  Period {period_idx + 1}/{n_periods}  (price_offset={price_offset}d back)")

            results: list[dict] = []
            errors = 0
            futures = {
                ex.submit(compute_stock_scores, code, forward_days, group, price_offset): code
                for code in codes
            }
            for future in as_completed(futures):
                try:
                    r = future.result(timeout=60)
                    if r is not None:
                        results.append(r)
                    else:
                        errors += 1
                except Exception:
                    errors += 1

            if len(results) < 10:
                print(f"    Skipped (only {len(results)} stocks succeeded)\n")
                continue

            df = pd.DataFrame(results)
            forward_ret = df["forward_ret"]
            factor_cols = [c for c in df.columns if c not in ("code", "forward_ret")]

            period_ic: dict[str, float] = {}
            for factor in factor_cols:
                scores = df[factor]
                valid  = scores.notna() & forward_ret.notna()
                if valid.sum() < 10:
                    period_ic[factor] = np.nan
                else:
                    ic, _ = spearman_ic(scores, forward_ret)
                    period_ic[factor] = ic

            period_meta.append({
                "period":       period_idx + 1,
                "price_offset": price_offset,
                "n_stocks":     len(results),
                "errors":       errors,
                "mean_fwd_ret": round(float(forward_ret.mean()), 2),
                "ic":           {f: (round(v, 4) if not np.isnan(v) else None)
                                 for f, v in period_ic.items()},
            })

            for factor, ic_val in period_ic.items():
                period_ics.setdefault(factor, []).append(ic_val)

            print(f"    OK: {len(results)} stocks, {errors} failed\n")

    if not period_meta:
        return {"error": "All periods failed"}

    # Aggregate across periods
    agg: dict[str, dict] = {}
    for factor, ic_list in period_ics.items():
        valid_ics = [v for v in ic_list if not np.isnan(v)]
        if len(valid_ics) < 2:
            agg[factor] = {
                "mean_ic":   round(valid_ics[0], 4) if valid_ics else None,
                "icir":      None,
                "n_periods": len(valid_ics),
                "quality":   "insufficient periods",
                "direction": "N/A",
            }
            continue
        mean_ic = float(np.mean(valid_ics))
        std_ic  = float(np.std(valid_ics, ddof=1))
        icir    = mean_ic / std_ic if std_ic > 0 else np.nan
        quality = (
            "strong"   if abs(mean_ic) >= 0.08 and abs(icir) >= 0.5  else
            "moderate" if abs(mean_ic) >= 0.05 and abs(icir) >= 0.3  else
            "weak"     if abs(mean_ic) >= 0.02                        else
            "noise"
        )
        agg[factor] = {
            "mean_ic":   round(mean_ic, 4),
            "icir":      round(icir, 3) if not np.isnan(icir) else None,
            "n_periods": len(valid_ics),
            "quality":   quality,
            "direction": "positive ✓" if mean_ic > 0 else "inverted ✗",
        }

    # Sort by abs(mean_ic) descending
    agg = dict(sorted(agg.items(), key=lambda x: abs(x[1].get("mean_ic") or 0), reverse=True))

    weight_recs = _recommend_weights(agg, ic_field="mean_ic")

    return {
        "meta": {
            "n_stocks":    len(codes),
            "n_periods":   len(period_meta),
            "step_days":   step,
            "forward_days": forward_days,
            "group":       group,
        },
        "ic_table":               agg,
        "weight_recommendations": weight_recs,
        "period_detail":          period_meta,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Factor IC backtesting for A-share factors")
    parser.add_argument("--n",       type=int,   default=50,   help="Max stocks to test (default 50)")
    parser.add_argument("--fwd",     type=int,   default=20,   help="Forward return window in days (default 20)")
    parser.add_argument("--group",   type=str,   default="A",  help="Factor group: A (fast) or AB (all)")
    parser.add_argument("--out",     type=str,   default="",   help="Output JSON file path (optional)")
    parser.add_argument("--rolling", type=int,   default=1,    help="Number of rolling periods (default 1 = single period)")
    parser.add_argument("--step",    type=int,   default=20,   help="Days between rolling periods (default 20)")
    args = parser.parse_args()

    codes = TEST_UNIVERSE[:args.n]
    result = run_analysis(
        codes=codes, forward_days=args.fwd, group=args.group,
        rolling=args.rolling, step=args.step,
    )

    output = json.dumps(result, ensure_ascii=False, indent=2)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"\nResults saved to {args.out}")
    elif "error" in result:
        print(f"\nError: {result['error']}")
    elif args.rolling > 1:
        # ── Rolling mode output ────────────────────────────────────────────
        print("\n" + "="*70)
        print(f"ROLLING IC RESULTS  ({result['meta']['n_periods']} periods × {result['meta']['step_days']}d)")
        print("="*70)
        if "ic_table" in result:
            print(f"\n{'Factor':<28} {'MeanIC':>8} {'ICIR':>7} {'Periods':>8} {'Quality':<14} {'Direction'}")
            print("-" * 80)
            for factor, stats in result["ic_table"].items():
                mic  = f"{stats['mean_ic']:.4f}" if stats["mean_ic"] is not None else "  N/A "
                icir = f"{stats['icir']:.3f}"    if stats["icir"]    is not None else "  N/A"
                print(f"{factor:<28} {mic:>8} {icir:>7} {stats['n_periods']:>8} "
                      f"{stats['quality']:<14} {stats['direction']}")

            print("\n" + "="*70)
            print("WEIGHT RECOMMENDATIONS (based on rolling mean IC + ICIR)")
            print("="*70)
            recs = result.get("weight_recommendations", {})
            for factor, w in sorted(recs.items(), key=lambda x: -x[1]):
                print(f"  {factor:<28}  →  weight = {w:.1f}")
    else:
        # ── Single-period output ───────────────────────────────────────────
        print("\n" + "="*70)
        print("IC ANALYSIS RESULTS")
        print("="*70)
        if "ic_table" in result:
            print(f"\n{'Factor':<28} {'IC':>8} {'t-stat':>8} {'p-val':>7} {'N':>5} {'Quality':<22} {'Direction'}")
            print("-" * 90)
            for factor, stats in result["ic_table"].items():
                ic    = f"{stats['ic']:.4f}"    if stats["ic"]      is not None else "  N/A "
                tstat = f"{stats['t_stat']:.2f}" if stats["t_stat"]  is not None else "  N/A"
                pval  = f"{stats['p_value']:.4f}" if stats.get("p_value") is not None else "  N/A"
                print(f"{factor:<28} {ic:>8} {tstat:>8} {pval:>7} {stats['n_stocks']:>5} "
                      f"{stats['quality']:<22} {stats['direction']}")

            print("\n" + "="*60)
            print("WEIGHT RECOMMENDATIONS")
            print("="*60)
            recs = result.get("weight_recommendations", {})
            for factor, w in sorted(recs.items(), key=lambda x: -x[1]):
                print(f"  {factor:<25}  →  weight = {w:.1f}")
