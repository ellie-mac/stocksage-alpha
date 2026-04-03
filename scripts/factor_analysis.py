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
  python factor_analysis.py                              # single period, 200 stocks, group AB, 20d fwd
  python factor_analysis.py --rolling 6 --step 20 --out factor_ic.json   # recommended full run
  python factor_analysis.py --group A                    # price/financial factors only (fast)
  python factor_analysis.py --universe screener_universe.json --rolling 6 --step 20 --out factor_ic.json
  python factor_analysis.py --rolling 12 --step 20 --n 100 --group A --out rolling_fast.json
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
    score_limit_hits, score_price_inertia, score_divergence,
    score_bollinger_position, score_roe_trend, score_cash_flow_quality, score_main_inflow,
    score_turnover_acceleration, score_momentum_concavity, score_bb_squeeze,
    score_idiosyncratic_vol, score_gross_margin_trend, score_ar_quality, score_size_factor,
    score_amihud_illiquidity, score_medium_term_momentum, score_obv_trend,
    score_market_beta, score_atr_normalized, score_ma60_deviation,
    score_max_return, score_return_skewness, score_upday_ratio,
    score_volume_expansion, score_nearness_to_high,
    score_price_volume_corr, score_trend_linearity, score_gap_frequency,
    score_market_relative_strength, score_price_efficiency, score_intraday_vs_overnight,
    score_hammer_bottom, score_limit_open_rate, score_upper_shadow_reversal,
    score_sector_sympathy, score_overhead_resistance,
    # Group B
    score_shareholder_change, score_lhb, score_lockup_pressure,
    score_insider, score_institutional_visits, score_industry_momentum,
    score_northbound_actual, score_earnings_revision,
    score_social_heat, score_market_regime, score_concept_momentum,
)


# ---------------------------------------------------------------------------
# Test universe — ~200-stock diversified A-share sample
#
# Coverage: large / mid / small cap across 20+ sectors, including blue chips,
# mid-cap growth (ChiNext 300xxx), and STAR market (688xxx).
#
# Statistical motivation for ~200 stocks (vs. the old 50):
#   N=50:  SE(IC) ≈ 0.14  →  t-stat for IC=0.08 ≈ 0.55  (p≈0.29, pure noise)
#   N=200: SE(IC) ≈ 0.07  →  t-stat for IC=0.08 ≈ 1.13  (p≈0.13, detectable)
# Combined with rolling ICIR ≥ 0.5 this gives much more reliable factor verdicts.
# ---------------------------------------------------------------------------
TEST_UNIVERSE = [
    # ── Large-cap consumer / baijiu / food ──────────────────────────────────
    "600519", "000858", "000568", "600809", "603369", "000799",
    "600887", "603288", "002304", "600132", "603345", "600702",
    "600276",
    # ── Banks (large) ───────────────────────────────────────────────────────
    "600036", "601166", "601398", "601939", "600000", "601288",
    "601988", "601328",
    # ── Banks (mid/small) ───────────────────────────────────────────────────
    "000001", "601169", "601229", "601009", "601577", "600015",
    "600016", "002142",
    # ── Insurance ───────────────────────────────────────────────────────────
    "601318", "601601", "601628", "601336",
    # ── Brokers / asset management ──────────────────────────────────────────
    "600030", "600837", "601688", "000776", "600999", "601211",
    "601901",
    # ── Healthcare / pharma ─────────────────────────────────────────────────
    "000538", "600436", "002607", "603259", "600196", "300347",
    "600867", "300759", "688180", "300015", "002555",
    # ── Medical devices ─────────────────────────────────────────────────────
    "300760", "002415",
    # ── Technology / IT / software ──────────────────────────────────────────
    "002230", "300059", "002241", "688111", "600100", "002049",
    "300408", "002179", "603986", "688008",
    # ── Semiconductors / chips ──────────────────────────────────────────────
    "688981", "000725", "688036", "688012", "688041", "688099",
    "688005", "002459",
    # ── Industry / machinery / equipment ────────────────────────────────────
    "000333", "600031", "600585", "603816", "601100", "300124",
    "601766", "600406", "002352", "603882",
    # ── Energy / utilities ──────────────────────────────────────────────────
    "600900", "601985", "600028", "601857", "601088", "601225",
    "600188", "600019",
    # ── New energy / solar / wind ───────────────────────────────────────────
    "601012", "688599", "300274", "601615", "600905", "603659",
    # ── EV / battery ────────────────────────────────────────────────────────
    "300750", "002460", "603799",
    # ── Auto / components ───────────────────────────────────────────────────
    "600104", "000625", "601238", "002594", "600741", "002027",
    # ── Real estate ─────────────────────────────────────────────────────────
    "000002", "600048", "001979", "600606", "000069",
    # ── Consumer electronics / appliances ───────────────────────────────────
    "600690", "000651", "002010",
    # ── Retail / e-commerce / duty-free ─────────────────────────────────────
    "601888", "000895", "002024", "603939",
    # ── Logistics / express ─────────────────────────────────────────────────
    "002352", "600233",
    # ── Telecom / networks ──────────────────────────────────────────────────
    "600050", "000063", "002475",
    # ── Media / education / gaming ──────────────────────────────────────────
    "300213", "002502", "002646",
    # ── Chemical / new materials ────────────────────────────────────────────
    "600309", "000792", "002648", "600346", "002038", "600516",
    # ── Steel / metals ──────────────────────────────────────────────────────
    "000898", "601600", "000708", "600022", "600282",
    # ── Mining / rare earth ─────────────────────────────────────────────────
    "601899", "000983", "002155",
    # ── Agriculture / livestock ─────────────────────────────────────────────
    "002714", "300498", "000876", "002385", "600598", "000998",
    # ── Defence / aerospace ─────────────────────────────────────────────────
    "600893", "000768", "300489", "002013", "600316",
    # ── Mid-cap growth — ChiNext (300xxx) ───────────────────────────────────
    "300033", "300122", "300347", "300661", "300782", "300896",
    "300750", "300760", "300014", "300498", "300124",
    # ── STAR market growth (688xxx) ─────────────────────────────────────────
    "688981", "688036", "688012", "688111", "688180", "688599",
    "688008", "688041", "688099", "688005",
]
# De-duplicate and normalise to 6-digit strings
TEST_UNIVERSE = list(dict.fromkeys(c.zfill(6) for c in TEST_UNIVERSE))


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
# Cache warm-up helper
# ---------------------------------------------------------------------------

def _prefetch_one(code: str, history_days: int) -> None:
    """Pre-warm per-stock caches.  Errors are silently swallowed."""
    for fn, args in [
        (fetcher.get_price_history,        (code, history_days)),
        (fetcher.get_financial_indicators, (code,)),
        (fetcher.get_valuation_history,    (code,)),
        (fetcher.get_fund_flow,            (code, 10)),
        (fetcher.get_margin_data,          (code,)),
    ]:
        try:
            fn(*args)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Per-stock score computation
# ---------------------------------------------------------------------------

def compute_stock_scores(code: str, forward_days: int, group: str, price_offset: int = 0,
                          _shared: Optional[dict] = None) -> Optional[dict]:
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

        # Fetch supporting data (uses cache so repeated calls are free).
        # market_price_df and spot_df are shared across all stocks; if pre-fetched
        # by the caller they are passed in via _shared to avoid per-stock re-fetches.
        _sh             = _shared or {}
        quote           = fetcher.get_realtime_quote(code) or {}
        financial_df    = fetcher.get_financial_indicators(code)
        val_history     = fetcher.get_valuation_history(code)
        fund_flow_df    = fetcher.get_fund_flow(code, 10)
        margin_df       = fetcher.get_margin_data(code)
        market_price_df = _sh.get("market_df") or fetcher.get_market_regime_data()
        circ_cap        = quote.get("circulating_cap", 0) or 0

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
        scores["divergence"]           = _safe(score_divergence, price_df)
        scores["bollinger_position"]   = _safe(score_bollinger_position, price_df)
        scores["roe_trend"]            = _safe(score_roe_trend, financial_df)
        scores["cash_flow_quality"]    = _safe(score_cash_flow_quality, financial_df)
        scores["main_inflow"]          = _safe(score_main_inflow, fund_flow_df)
        scores["turnover_acceleration"] = _safe(score_turnover_acceleration, price_df)
        scores["momentum_concavity"]    = _safe(score_momentum_concavity, price_df)
        scores["bb_squeeze"]            = _safe(score_bb_squeeze, price_df)
        scores["idiosyncratic_vol"]     = _safe(score_idiosyncratic_vol, price_df, market_price_df)
        scores["gross_margin_trend"]    = _safe(score_gross_margin_trend, financial_df)
        scores["ar_quality"]            = _safe(score_ar_quality, financial_df)
        scores["size_factor"]              = _safe(score_size_factor, circ_cap)
        scores["amihud_illiquidity"]       = _safe(score_amihud_illiquidity, price_df)
        scores["medium_term_momentum"]     = _safe(score_medium_term_momentum, price_df)
        scores["obv_trend"]                = _safe(score_obv_trend, price_df)
        scores["market_beta"]              = _safe(score_market_beta, price_df, market_price_df)
        scores["atr_normalized"]           = _safe(score_atr_normalized, price_df)
        scores["ma60_deviation"]           = _safe(score_ma60_deviation, price_df)
        scores["max_return"]               = _safe(score_max_return, price_df)
        scores["return_skewness"]          = _safe(score_return_skewness, price_df)
        scores["upday_ratio"]              = _safe(score_upday_ratio, price_df)
        scores["volume_expansion"]         = _safe(score_volume_expansion, price_df)
        scores["nearness_to_high"]         = _safe(score_nearness_to_high, price_df)
        scores["price_volume_corr"]        = _safe(score_price_volume_corr, price_df)
        scores["trend_linearity"]          = _safe(score_trend_linearity, price_df)
        scores["gap_frequency"]            = _safe(score_gap_frequency, price_df)
        scores["market_relative_strength"] = _safe(score_market_relative_strength, price_df, market_price_df)
        scores["price_efficiency"]         = _safe(score_price_efficiency, price_df)
        scores["intraday_vs_overnight"]    = _safe(score_intraday_vs_overnight, price_df)
        scores["hammer_bottom"]            = _safe(score_hammer_bottom, price_df)
        scores["limit_open_rate"]          = _safe(score_limit_open_rate, price_df)
        scores["upper_shadow_reversal"]    = _safe(score_upper_shadow_reversal, price_df)

        # sector_sympathy: uses full-market spot data + stock's industry
        _spot_df_sym  = _sh.get("spot_df") or fetcher._get_spot_df()
        _info_sym     = fetcher.get_stock_info(code) if "B" not in group.upper() else None
        _industry_sym = (_info_sym or {}).get("industry", "") if _info_sym is not None else ""
        scores["sector_sympathy"]      = _safe(score_sector_sympathy, code, _industry_sym, _spot_df_sym)
        scores["sell_score_sector_sympathy"] = _safe_sell(score_sector_sympathy, code, _industry_sym, _spot_df_sym)

        # overhead_resistance: chip distribution overhead pressure (套牢盘)
        try:
            cyq_df = fetcher.get_cyq(code)
        except Exception:
            cyq_df = None
        scores["overhead_resistance"]           = _safe(score_overhead_resistance, cyq_df, price_df)
        scores["sell_score_overhead_resistance"] = _safe_sell(score_overhead_resistance, cyq_df, price_df)

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
        scores["sell_score_divergence"]          = _safe_sell(score_divergence, price_df)
        scores["sell_score_bollinger_position"]  = _safe_sell(score_bollinger_position, price_df)
        scores["sell_score_roe_trend"]           = _safe_sell(score_roe_trend, financial_df)
        scores["sell_score_cash_flow_quality"]   = _safe_sell(score_cash_flow_quality, financial_df)
        scores["sell_score_main_inflow"]          = _safe_sell(score_main_inflow, fund_flow_df)
        scores["sell_score_turnover_acceleration"] = _safe_sell(score_turnover_acceleration, price_df)
        scores["sell_score_momentum_concavity"]    = _safe_sell(score_momentum_concavity, price_df)
        scores["sell_score_bb_squeeze"]            = _safe_sell(score_bb_squeeze, price_df)
        scores["sell_score_idiosyncratic_vol"]     = _safe_sell(score_idiosyncratic_vol, price_df, market_price_df)
        scores["sell_score_gross_margin_trend"]    = _safe_sell(score_gross_margin_trend, financial_df)
        scores["sell_score_ar_quality"]            = _safe_sell(score_ar_quality, financial_df)
        scores["sell_score_size_factor"]           = _safe_sell(score_size_factor, circ_cap)
        scores["sell_score_amihud_illiquidity"]    = _safe_sell(score_amihud_illiquidity, price_df)
        scores["sell_score_medium_term_momentum"]  = _safe_sell(score_medium_term_momentum, price_df)
        scores["sell_score_obv_trend"]             = _safe_sell(score_obv_trend, price_df)
        scores["sell_score_market_beta"]           = _safe_sell(score_market_beta, price_df, market_price_df)
        scores["sell_score_atr_normalized"]        = _safe_sell(score_atr_normalized, price_df)
        scores["sell_score_ma60_deviation"]        = _safe_sell(score_ma60_deviation, price_df)
        scores["sell_score_max_return"]            = _safe_sell(score_max_return, price_df)
        scores["sell_score_return_skewness"]       = _safe_sell(score_return_skewness, price_df)
        scores["sell_score_upday_ratio"]           = _safe_sell(score_upday_ratio, price_df)
        scores["sell_score_volume_expansion"]      = _safe_sell(score_volume_expansion, price_df)
        scores["sell_score_nearness_to_high"]      = _safe_sell(score_nearness_to_high, price_df)
        scores["sell_score_price_volume_corr"]     = _safe_sell(score_price_volume_corr, price_df)
        scores["sell_score_trend_linearity"]       = _safe_sell(score_trend_linearity, price_df)
        scores["sell_score_gap_frequency"]              = _safe_sell(score_gap_frequency, price_df)
        scores["sell_score_market_relative_strength"]   = _safe_sell(score_market_relative_strength, price_df, market_price_df)
        scores["sell_score_price_efficiency"]           = _safe_sell(score_price_efficiency, price_df)
        scores["sell_score_intraday_vs_overnight"]      = _safe_sell(score_intraday_vs_overnight, price_df)
        scores["sell_score_hammer_bottom"]              = _safe_sell(score_hammer_bottom, price_df)
        scores["sell_score_limit_open_rate"]            = _safe_sell(score_limit_open_rate, price_df)
        scores["sell_score_upper_shadow_reversal"]      = _safe_sell(score_upper_shadow_reversal, price_df)

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
            market_regime_df = market_price_df  # already fetched above (shared)

            # Re-compute value + extract pe/pb percentiles in one call
            try:
                _val_full = score_value(quote.get("pe_ttm", 0), quote.get("pb", 0), val_history, None, price_df, revision_df, financial_df, market_ret_1m=market_ret)
                scores["value"]           = float(_val_full.get("score",      np.nan)) if isinstance(_val_full, dict) else float(_val_full)
                scores["sell_score_value"] = float(_val_full.get("sell_score", np.nan)) if isinstance(_val_full, dict) else np.nan
                _pe_pct = _val_full.get("details", {}).get("pe_percentile") if isinstance(_val_full, dict) else None
                _pb_pct = _val_full.get("details", {}).get("pb_percentile") if isinstance(_val_full, dict) else None
            except Exception:
                scores["value"] = np.nan
                scores["sell_score_value"] = np.nan
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
            # Re-compute Ext-B factors that accept market_regime_score (initial calls above had _regime_float=None)
            scores["shareholder_change"]             = _safe(score_shareholder_change, shareholder_df, price_df, revision_df, None, _regime_float)
            scores["sell_score_shareholder_change"]  = _safe_sell(score_shareholder_change, shareholder_df, price_df, revision_df, None, _regime_float)
            scores["lhb"]                            = _safe(score_lhb, lhb_df, price_df, _regime_float, market_ret_1m=market_ret, revision_df=revision_df)
            scores["sell_score_lhb"]                 = _safe_sell(score_lhb, lhb_df, price_df, _regime_float, market_ret_1m=market_ret, revision_df=revision_df)
            scores["lockup_pressure"]                = _safe(score_lockup_pressure, lockup_df, circ_cap, price_df, financial_df, social_dict, _regime_float, market_ret_1m=market_ret, revision_df=revision_df)
            scores["sell_score_lockup_pressure"]     = _safe_sell(score_lockup_pressure, lockup_df, circ_cap, price_df, financial_df, social_dict, _regime_float, market_ret_1m=market_ret, revision_df=revision_df)
            scores["insider"]                        = _safe(score_insider, insider_df, price_df, revision_df, None, _regime_float)
            scores["sell_score_insider"]             = _safe_sell(score_insider, insider_df, price_df, revision_df, None, _regime_float)
            scores["institutional_visits"]           = _safe(score_institutional_visits, visits_df, revision_df, price_df, _regime_float)
            scores["sell_score_institutional_visits"] = _safe_sell(score_institutional_visits, visits_df, revision_df, price_df, _regime_float)
            scores["earnings_revision"]              = _safe(score_earnings_revision, revision_df, price_df, financial_df, visits_df, _regime_float, best_concept_ret=_best_concept_ret, social_dict=social_dict)
            scores["sell_score_earnings_revision"]   = _safe_sell(score_earnings_revision, revision_df, price_df, financial_df, visits_df, _regime_float, best_concept_ret=_best_concept_ret, social_dict=social_dict)

            # Sell scores for Ext-B (northbound_actual and social_heat get re-computed below with industry context)
            scores["sell_score_northbound_actual"]    = _safe_sell(score_northbound_actual, nb_df, price_df, revision_df, None, market_ret, social_dict=social_dict)
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
                # Re-compute sector_sympathy with confirmed industry name
                scores["sector_sympathy"]            = _safe(score_sector_sympathy, code, ind, _spot_df_sym)
                scores["sell_score_sector_sympathy"] = _safe_sell(score_sector_sympathy, code, ind, _spot_df_sym)
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

    # ── Phase 1: parallel I/O pre-fetch (warm caches before scoring) ────────
    history_needed = max(400, 300 + forward_days + 10)
    prefetch_workers = min(len(codes), max(max_workers, 16))
    print(f"Pre-fetching stock data ({prefetch_workers} workers)...")
    with ThreadPoolExecutor(max_workers=prefetch_workers) as pre_ex:
        pre_futs = [pre_ex.submit(_prefetch_one, c, history_needed) for c in codes]
        for f in as_completed(pre_futs):
            try:
                f.result()
            except Exception:
                pass

    # Shared per-run data (same for all stocks; fetch once after pre-warm)
    _shared = {
        "market_df": fetcher.get_market_regime_data(),
        "spot_df":   fetcher._get_spot_df(),
    }

    # ── Phase 2: parallel scoring (data now in cache) ────────────────────────
    print(f"Scoring factors ({max_workers} workers)...\n")
    results: list[dict] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(compute_stock_scores, code, forward_days, group, 0, _shared): code
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

    # ── Pre-fetch all stock data once (covers max price_offset) ─────────────
    # All rolling periods reuse the same cache entry (_PRICE_FETCH_DAYS covers them all)
    max_offset       = (n_periods - 1) * step
    history_needed   = max(400, 300 + forward_days + max_offset + 10)
    prefetch_workers = min(len(codes), max(max_workers, 16))
    print(f"Pre-fetching stock data for all periods ({prefetch_workers} workers)...")
    with ThreadPoolExecutor(max_workers=prefetch_workers) as pre_ex:
        pre_futs = [pre_ex.submit(_prefetch_one, c, history_needed) for c in codes]
        for f in as_completed(pre_futs):
            try:
                f.result()
            except Exception:
                pass

    # Shared per-run data fetched once and reused across all periods
    _shared = {
        "market_df": fetcher.get_market_regime_data(),
        "spot_df":   fetcher._get_spot_df(),
    }
    print()

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
                ex.submit(compute_stock_scores, code, forward_days, group, price_offset, _shared): code
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
    parser.add_argument("--n",        type=int,   default=200,  help="Max stocks to test (default 200)")
    parser.add_argument("--fwd",      type=int,   default=20,   help="Forward return window in days (default 20)")
    parser.add_argument("--group",    type=str,   default="AB", help="Factor group: A (price-only, fast) or AB (all factors)")
    parser.add_argument("--out",      type=str,   default="",   help="Output JSON file path (optional)")
    parser.add_argument("--rolling",  type=int,   default=1,    help="Number of rolling periods (default 1 = single period)")
    parser.add_argument("--step",     type=int,   default=20,   help="Days between rolling periods (default 20)")
    parser.add_argument("--workers",  type=int,   default=8,    help="Thread pool size (default 8; use 1 to avoid V8 crashes)")
    parser.add_argument("--universe", type=str,   default="",
                        help="Path to a JSON file with a list of stock codes to use instead of built-in TEST_UNIVERSE. "
                             "Format: [\"600519\", \"000858\", ...] or {\"codes\": [...]}")
    args = parser.parse_args()

    # Resolve stock universe
    if args.universe:
        try:
            with open(args.universe, encoding="utf-8") as uf:
                raw = json.load(uf)
            loaded = raw if isinstance(raw, list) else raw.get("codes", [])
            loaded = [str(c).zfill(6) for c in loaded if str(c).strip()]
            if not loaded:
                print(f"[warn] --universe file '{args.universe}' is empty; falling back to built-in universe")
                loaded = TEST_UNIVERSE
            else:
                print(f"Loaded {len(loaded)} stocks from {args.universe}")
        except Exception as e:
            print(f"[warn] Could not load --universe file '{args.universe}': {e}; falling back to built-in universe")
            loaded = TEST_UNIVERSE
    else:
        loaded = TEST_UNIVERSE

    codes = loaded[:args.n]
    result = run_analysis(
        codes=codes, forward_days=args.fwd, group=args.group,
        max_workers=args.workers, rolling=args.rolling, step=args.step,
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
