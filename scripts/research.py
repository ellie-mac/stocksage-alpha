#!/usr/bin/env python3
"""
Stock deep-research script.
Usage : python research.py <stock_code_or_name> [--weights "focus on growth"]
Output: JSON with fundamentals, technicals, and multi-factor scores.
Accepts both Chinese names (贵州茅台) and 6-digit codes (600519).
"""

import sys
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd

from fetcher import (
    normalize_code,
    get_realtime_quote,
    get_stock_info,
    get_price_history,
    get_valuation_history,
    get_financial_indicators,
    get_fund_flow,
    get_margin_data,
    get_shareholder_count,
    get_lhb_flow,
    get_lockup_pressure,
    get_insider_transactions,
    get_institutional_visits,
    get_industry_momentum,
    get_market_return_1m,
    get_northbound_holdings,
    get_earnings_revision,
    get_social_heat,
    get_market_regime_data,
    get_concept_momentum,
    search_stock_by_name,
    _get_spot_df,
)
from factors import (
    FactorWeights,
    DEFAULT_WEIGHTS,
    parse_weights,
    score_value,
    score_growth,
    score_momentum,
    score_quality,
    score_northbound,
    score_volume_breakout,
    score_52w_position,
    score_dividend_yield,
    score_volume_ratio,
    score_ma_alignment,
    score_low_volatility,
    compute_total_score,
    compute_sell_score,
    compute_technical,
)
from factors_extended import (
    score_reversal,
    score_accruals,
    score_asset_growth,
    score_piotroski,
    score_short_interest,
    score_rsi_signal,
    score_macd_signal,
    score_turnover_percentile,
    score_chip_distribution,
    score_shareholder_change,
    score_lhb,
    score_lockup_pressure,
    score_insider,
    score_institutional_visits,
    score_industry_momentum,
    score_northbound_actual,
    score_earnings_revision,
    score_limit_hits,
    score_price_inertia,
    score_social_heat,
    score_market_regime,
    score_concept_momentum,
)


def is_stock_code(s: str) -> bool:
    """Return True if the string looks like a 6-digit A-share code."""
    return bool(re.match(r"^\d{6}$", s.strip()))


def research(raw_input: str, weights: FactorWeights = DEFAULT_WEIGHTS) -> dict:
    raw_input = raw_input.strip()

    # Resolve input to a 6-digit code
    if is_stock_code(raw_input):
        code = normalize_code(raw_input)
    else:
        code = search_stock_by_name(raw_input)
        if not code:
            return {"error": f"Stock not found: '{raw_input}'. Please check the code or name."}

    # Real-time quote (fast path, uses shared spot cache)
    quote = get_realtime_quote(code)
    if not quote or "error" in quote:
        return {"error": f"No data for stock code {code}."}

    # ── Fetch all data concurrently (core + extended in one pool) ──────────
    with ThreadPoolExecutor(max_workers=17) as ex:
        # Core
        f_info      = ex.submit(get_stock_info, code)
        f_price     = ex.submit(get_price_history, code, 365)
        f_valuation = ex.submit(get_valuation_history, code)
        f_financial = ex.submit(get_financial_indicators, code)
        f_fundflow  = ex.submit(get_fund_flow, code, 10)
        f_margin    = ex.submit(get_margin_data, code)
        f_mktret    = ex.submit(get_market_return_1m)
        f_shareh    = ex.submit(get_shareholder_count, code)
        # Extended (best-effort)
        f_lhb      = ex.submit(get_lhb_flow, code)
        f_lockup   = ex.submit(get_lockup_pressure, code)
        f_insider  = ex.submit(get_insider_transactions, code)
        f_visits   = ex.submit(get_institutional_visits, code)
        f_nb       = ex.submit(get_northbound_holdings, code)
        f_revision = ex.submit(get_earnings_revision, code)
        f_social   = ex.submit(get_social_heat, code)
        f_market   = ex.submit(get_market_regime_data)
        f_concept  = ex.submit(get_concept_momentum, code)

    info           = f_info.result() or {}
    price_df       = f_price.result()
    val_history    = f_valuation.result()
    financial_df   = f_financial.result()
    fund_flow_df   = f_fundflow.result()
    margin_df      = f_margin.result()
    market_ret     = f_mktret.result()
    shareholder_df = f_shareh.result()
    lhb_df         = f_lhb.result()
    lockup_df      = f_lockup.result()
    insider_df     = f_insider.result()
    visits_df      = f_visits.result()
    nb_df          = f_nb.result()
    revision_df    = f_revision.result()
    social_dict    = f_social.result()
    market_df      = f_market.result()
    concept_data   = f_concept.result()

    industry = info.get("industry", "Unknown")

    # Industry momentum (needs industry name from info)
    industry_ret = None
    if industry and industry != "Unknown":
        try:
            industry_ret = get_industry_momentum(industry)
        except Exception:
            pass

    # Build intra-industry valuation context if the industry map is cached
    industry_stats = None
    if industry and industry != "Unknown":
        try:
            from industry import build_industry_map, get_industry_pe_stats
            industry_map = build_industry_map()
            if industry_map:
                spot_df = _get_spot_df()
                industry_stats = get_industry_pe_stats(industry, spot_df, industry_map)
        except Exception:
            pass

    circ_cap = quote.get("circulating_cap", 0) or 0

    # ── Factor scoring ──────────────────────────────────────────────────────
    # Compute market regime first so regime_score is available to all subsequent factors
    mkt_factor   = score_market_regime(market_df)
    regime_score = mkt_factor.get("score") if mkt_factor else None

    # Core 4 + 3 extended (original)
    value_factor    = score_value(quote.get("pe_ttm", 0), quote.get("pb", 0),
                                  val_history, industry_stats, price_df, revision_df, financial_df,
                                  regime_score, industry_ret, market_ret)
    _pe_pct = value_factor["details"].get("pe_percentile")
    _pb_pct = value_factor["details"].get("pb_percentile")
    growth_factor   = score_growth(financial_df, _pe_pct, regime_score, industry_ret, market_ret, price_df, revision_df)
    momentum_factor = score_momentum(price_df, financial_df, regime_score, industry_ret, market_ret, revision_df)
    quality_factor  = score_quality(financial_df, price_df, _pe_pct, _pb_pct, regime_score, industry_ret, market_ret)
    nb_factor       = score_northbound(fund_flow_df, price_df, regime_score, industry_ret, market_ret, revision_df)
    best_concept_ret = max((c["ret_1m"] for c in concept_data), default=None) if concept_data else None
    vol_factor      = score_volume_breakout(price_df, regime_score, industry_ret, market_ret, best_concept_ret, revision_df)
    pos_factor      = score_52w_position(price_df, regime_score)

    # Extended A: spot / price-based
    div_factor   = score_dividend_yield(quote.get("div_yield", 0), financial_df, regime_score, price_df, industry_ret, market_ret, revision_df)
    vr_factor    = score_volume_ratio(quote.get("volume_ratio", 0), quote.get("change_pct", 0), price_df, regime_score, revision_df)
    ma_factor    = score_ma_alignment(price_df, revision_df, regime_score, industry_ret, market_ret)
    lv_factor    = score_low_volatility(price_df, regime_score, industry_ret, market_ret)
    rev_factor   = score_reversal(price_df, financial_df, revision_df, regime_score, industry_ret, market_ret, best_concept_ret)
    acc_factor   = score_accruals(financial_df, regime_score, price_df)
    ag_factor    = score_asset_growth(financial_df, regime_score, industry_ret, market_ret)
    pf_factor    = score_piotroski(financial_df, price_df, _pe_pct, _pb_pct, regime_score, industry_ret, market_ret, revision_df)
    si_factor    = score_short_interest(margin_df, circ_cap, price_df, revision_df, regime_score, industry_ret, market_ret)
    rsi_factor   = score_rsi_signal(price_df, regime_score, industry_ret, market_ret, revision_df)
    macd_factor  = score_macd_signal(price_df, regime_score, industry_ret, market_ret, best_concept_ret, revision_df)
    tpct_factor  = score_turnover_percentile(price_df, regime_score)
    chip_factor  = score_chip_distribution(price_df, fund_flow_df, regime_score, industry_ret, market_ret, social_dict, revision_df)

    # Extended B: additional API data
    industry_excess = (industry_ret - market_ret) if (industry_ret is not None and market_ret is not None) else None
    sh_factor    = score_shareholder_change(shareholder_df, price_df, revision_df, industry_excess, regime_score)
    lhb_factor   = score_lhb(lhb_df, price_df, regime_score, industry_ret, market_ret, revision_df)
    lk_factor    = score_lockup_pressure(lockup_df, circ_cap, price_df, financial_df, social_dict, regime_score, industry_ret, market_ret, revision_df)
    ins_factor   = score_insider(insider_df, price_df, revision_df, industry_excess, regime_score)
    vis_factor   = score_institutional_visits(visits_df, revision_df, price_df, regime_score, industry_ret, market_ret)
    nba_factor   = score_northbound_actual(nb_df, price_df, revision_df, industry_ret, market_ret, regime_score, social_dict)
    er_factor    = score_earnings_revision(revision_df, price_df, financial_df, visits_df, regime_score, industry_ret, market_ret, best_concept_ret, social_dict)

    # Extended C: behavioral / market-context factors
    ind_factor   = score_industry_momentum(industry_ret, market_ret, price_df, regime_score, industry_stats, best_concept_ret, social_dict)
    lim_factor    = score_limit_hits(price_df, financial_df, social_dict, best_concept_ret, regime_score, industry_ret, market_ret, revision_df)
    inr_factor    = score_price_inertia(price_df, regime_score, industry_ret, market_ret)
    soc_factor    = score_social_heat(social_dict, price_df, financial_df, best_concept_ret, regime_score, industry_ret, market_ret, revision_df)
    con_factor    = score_concept_momentum(concept_data, price_df, regime_score, financial_df, industry_excess, revision_df)

    extra = {
        "div_yield":           div_factor,
        "volume_ratio":        vr_factor,
        "ma_alignment":        ma_factor,
        "low_volatility":      lv_factor,
        "reversal":            rev_factor,
        "accruals":            acc_factor,
        "asset_growth":        ag_factor,
        "piotroski":           pf_factor,
        "short_interest":      si_factor,
        "rsi_signal":          rsi_factor,
        "macd_signal":         macd_factor,
        "turnover_percentile": tpct_factor,
        "chip_distribution":   chip_factor,
        "shareholder_change":  sh_factor,
        "lhb":                 lhb_factor,
        "lockup_pressure":     lk_factor,
        "insider":             ins_factor,
        "institutional_visits": vis_factor,
        "industry_momentum":   ind_factor,
        "northbound_actual":   nba_factor,
        "earnings_revision":   er_factor,
        "limit_hits":       lim_factor,
        "price_inertia":    inr_factor,
        "social_heat":      soc_factor,
        "market_regime":    mkt_factor,
        "concept_momentum": con_factor,
    }

    total_score = compute_total_score(
        value_factor, growth_factor, momentum_factor, quality_factor,
        nb_factor, vol_factor, pos_factor, weights,
        extra_factors=extra,
        market_regime_score=regime_score,
    )

    total_sell_score = compute_sell_score(
        value_factor, growth_factor, momentum_factor, quality_factor,
        nb_factor, vol_factor, pos_factor, weights,
        extra_factors=extra,
        market_regime_score=regime_score,
    )

    technical = compute_technical(price_df)
    margin_summary = _summarize_margin(margin_df)

    # Historical valuation range (p10 / median / p90)
    valuation_summary = {}
    if val_history is not None and not val_history.empty:
        for col, prefix in [("pe_ttm", "pe"), ("pb", "pb")]:
            if col in val_history.columns:
                series = val_history[col].replace(0, None).dropna()
                series = series[series > 0]
                if len(series) >= 10:
                    valuation_summary[f"{prefix}_3y_low"]    = round(float(series.quantile(0.1)), 1)
                    valuation_summary[f"{prefix}_3y_median"] = round(float(series.median()), 1)
                    valuation_summary[f"{prefix}_3y_high"]   = round(float(series.quantile(0.9)), 1)

    from dataclasses import fields as dc_fields
    return {
        "code": code,
        "name": quote.get("name", ""),
        "basic": {
            "industry":                info.get("industry", "Unknown"),
            "listing_date":            info.get("listing_date", ""),
            "market_cap_billion":      round(quote.get("market_cap", 0) / 1e8, 1),
            "circulating_cap_billion": round(circ_cap / 1e8, 1),
        },
        "price": {
            "current":        quote.get("price"),
            "change_pct":     quote.get("change_pct"),
            "change_amt":     quote.get("change_amt"),
            "open":           quote.get("open"),
            "high":           quote.get("high"),
            "low":            quote.get("low"),
            "prev_close":     quote.get("prev_close"),
            "volume_million":  round((quote.get("volume", 0) or 0) / 1e6, 2),
            "amount_billion":  round((quote.get("amount", 0) or 0) / 1e8, 2),
            "turnover_rate":  quote.get("turnover_rate"),
            "volume_ratio":   quote.get("volume_ratio"),
            "div_yield":      quote.get("div_yield"),
        },
        "valuation": {
            "pe_ttm":          quote.get("pe_ttm"),
            "pb":              quote.get("pb"),
            **valuation_summary,
            "pe_percentile":   value_factor["details"].get("pe_percentile"),
            "pb_percentile":   value_factor["details"].get("pb_percentile"),
            "valuation_basis": value_factor["details"].get("pe_score_source"),
            "industry_stats":  industry_stats,
        },
        "financial":  growth_factor["details"],
        "technical":  technical,
        "margin":     margin_summary,
        "factors": {
            # Core
            "value":          value_factor,
            "growth":         growth_factor,
            "momentum":       momentum_factor,
            "quality":        quality_factor,
            # Extended A (original)
            "northbound":     nb_factor,
            "volume":         vol_factor,
            "position_52w":   pos_factor,
            # Extended A (new)
            "div_yield":      div_factor,
            "volume_ratio":   vr_factor,
            "ma_alignment":   ma_factor,
            "low_volatility": lv_factor,
            "reversal":       rev_factor,
            "accruals":       acc_factor,
            "asset_growth":   ag_factor,
            "piotroski":      pf_factor,
            "short_interest": si_factor,
            "rsi_signal":     rsi_factor,
            "macd_signal":    macd_factor,
            "turnover_percentile": tpct_factor,
            "chip_distribution":   chip_factor,
            # Extended B
            "shareholder_change":   sh_factor,
            "lhb":                  lhb_factor,
            "lockup_pressure":      lk_factor,
            "insider":              ins_factor,
            "institutional_visits": vis_factor,
            "industry_momentum":    ind_factor,
            "northbound_actual":    nba_factor,
            "earnings_revision":    er_factor,
            "limit_hits":       lim_factor,
            "price_inertia":    inr_factor,
            "social_heat":      soc_factor,
            "market_regime":    mkt_factor,
            "concept_momentum": con_factor,
        },
        "weights_used": {f.name: getattr(weights, f.name) for f in dc_fields(weights)},
        "total_score": total_score,
        "score_interpretation": _interpret_score(total_score),
        "total_sell_score": total_sell_score,
        "sell_score_interpretation": _interpret_sell_score(total_sell_score),
    }


def _summarize_margin(margin_df) -> dict:
    """Extract margin balance trend from the raw margin DataFrame."""
    if margin_df is None or margin_df.empty:
        return {"available": False}

    bal_cols = [c for c in margin_df.columns
                if any(k in c for k in ["融资余额", "融资买入额", "margin"])]
    if not bal_cols:
        return {"available": False}

    col = bal_cols[0]
    series = pd.to_numeric(margin_df[col], errors="coerce").dropna()
    if len(series) < 2:
        return {"available": False}

    change_pct = (float((series.iloc[-1] - series.iloc[-5]) / series.iloc[-5] * 100)
                  if len(series) >= 5 else None)
    return {
        "available": True,
        "latest_balance_billion": round(float(series.iloc[-1]) / 1e8, 2),
        "change_5d_pct": round(change_pct, 1) if change_pct is not None else None,
        "trend": ("increasing" if (change_pct or 0) > 2
                  else "decreasing" if (change_pct or 0) < -2 else "flat"),
    }


def _interpret_score(score: float) -> str:
    if score >= 80:
        return "Excellent — strong across all dimensions, high priority watchlist"
    elif score >= 65:
        return "Good — solid overall, worth tracking"
    elif score >= 50:
        return "Average — some strengths but notable weaknesses"
    elif score >= 35:
        return "Weak — underperforms on multiple dimensions, proceed with caution"
    else:
        return "Poor — significant fundamental or valuation concerns"


def _interpret_sell_score(score: float) -> str:
    if score >= 70:
        return "Strong sell — multiple significant bearish signals"
    elif score >= 50:
        return "Moderate sell pressure — consider reducing position"
    elif score >= 35:
        return "Mild caution — monitor closely"
    elif score >= 20:
        return "Low sell pressure — hold unless other factors change"
    else:
        return "No significant sell signal"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Deep research report for an A-share stock")
    parser.add_argument("target", nargs="+", help="Stock code (600519) or Chinese name (贵州茅台)")
    parser.add_argument("--weights", type=str, default="",
                        help='Weight preference string, e.g. "focus on growth" or "重视成长"')
    args = parser.parse_args()

    target_str = " ".join(args.target)
    w = parse_weights(args.weights) if args.weights else DEFAULT_WEIGHTS
    result = research(target_str, weights=w)
    print(json.dumps(result, ensure_ascii=False, indent=2))
