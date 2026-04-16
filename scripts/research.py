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
    # IC-validated factors (2026-04-03 backtest)
    score_idiosyncratic_vol,
    score_gap_frequency,
    score_atr_normalized,
    score_return_skewness,
    score_ma60_deviation,
    score_divergence,
    score_main_inflow,
    score_cash_flow_quality,
    score_amihud_illiquidity,
    score_max_return,
    score_turnover_acceleration,
    score_upday_ratio,
    score_roe_trend,
    score_nearness_to_high,
    score_hammer_bottom,
    score_limit_open_rate,
    score_medium_term_momentum,
    score_price_volume_corr,
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

    # Industry momentum + intra-industry valuation stats — run concurrently
    industry_ret = None
    industry_stats = None
    if industry and industry != "Unknown":
        def _fetch_industry_ret():
            try:
                return get_industry_momentum(industry)
            except Exception:
                return None

        def _fetch_industry_stats():
            try:
                from industry import build_industry_map, get_industry_pe_stats
                industry_map = build_industry_map()
                if industry_map:
                    spot_df = _get_spot_df()
                    return get_industry_pe_stats(industry, spot_df, industry_map)
            except Exception:
                pass
            return None

        with ThreadPoolExecutor(max_workers=2) as ex2:
            f_ir = ex2.submit(_fetch_industry_ret)
            f_is = ex2.submit(_fetch_industry_stats)
        industry_ret   = f_ir.result()
        industry_stats = f_is.result()

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

    # Extended D: IC-validated factors (2026-04-03 backtest)
    idio_factor   = score_idiosyncratic_vol(price_df, market_df)
    gap_factor    = score_gap_frequency(price_df)
    atr_factor    = score_atr_normalized(price_df)
    skew_factor   = score_return_skewness(price_df)
    ma60_factor   = score_ma60_deviation(price_df)
    div_factor2   = score_divergence(price_df)
    inf_factor    = score_main_inflow(fund_flow_df)
    cfq_factor    = score_cash_flow_quality(financial_df)
    amh_factor    = score_amihud_illiquidity(price_df)
    mxr_factor    = score_max_return(price_df)
    tacc_factor   = score_turnover_acceleration(price_df)
    upr_factor    = score_upday_ratio(price_df)
    roe_factor    = score_roe_trend(financial_df)
    nth_factor    = score_nearness_to_high(price_df)
    ham_factor    = score_hammer_bottom(price_df)
    lor_factor    = score_limit_open_rate(price_df)
    mtm_factor    = score_medium_term_momentum(price_df)
    pvc_factor    = score_price_volume_corr(price_df)

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
        "limit_hits":          lim_factor,
        "price_inertia":       inr_factor,
        "social_heat":         soc_factor,
        "market_regime":       mkt_factor,
        "concept_momentum":    con_factor,
        # Extended D (IC-validated 2026-04-03)
        "idiosyncratic_vol":     idio_factor,
        "gap_frequency":         gap_factor,
        "atr_normalized":        atr_factor,
        "return_skewness":       skew_factor,
        "ma60_deviation":        ma60_factor,
        "divergence":            div_factor2,
        "main_inflow":           inf_factor,
        "cash_flow_quality":     cfq_factor,
        "amihud_illiquidity":    amh_factor,
        "max_return":            mxr_factor,
        "turnover_acceleration": tacc_factor,
        "upday_ratio":           upr_factor,
        "roe_trend":             roe_factor,
        "nearness_to_high":      nth_factor,
        "hammer_bottom":         ham_factor,
        "limit_open_rate":       lor_factor,
        "medium_term_momentum":  mtm_factor,
        "price_volume_corr":     pvc_factor,
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
            # Extended D (IC-validated 2026-04-03)
            "idiosyncratic_vol":     idio_factor,
            "gap_frequency":         gap_factor,
            "atr_normalized":        atr_factor,
            "return_skewness":       skew_factor,
            "ma60_deviation":        ma60_factor,
            "divergence":            div_factor2,
            "main_inflow":           inf_factor,
            "cash_flow_quality":     cfq_factor,
            "amihud_illiquidity":    amh_factor,
            "max_return":            mxr_factor,
            "turnover_acceleration": tacc_factor,
            "upday_ratio":           upr_factor,
            "roe_trend":             roe_factor,
            "nearness_to_high":      nth_factor,
            "hammer_bottom":         ham_factor,
            "limit_open_rate":       lor_factor,
            "medium_term_momentum":  mtm_factor,
            "price_volume_corr":     pvc_factor,
        },
        "weights_used": {f.name: getattr(weights, f.name)
                         for f in dc_fields(weights) if getattr(weights, f.name) != 0},
        "total_score": total_score,
        "score_interpretation": _interpret_score(total_score),
        "total_sell_score": total_sell_score,
        "sell_score_interpretation": _interpret_sell_score(total_sell_score),
        "signals_summary": _build_signals_summary({
            "value": value_factor, "growth": growth_factor,
            "momentum": momentum_factor, "quality": quality_factor,
            "northbound": nb_factor, "volume": vol_factor,
            "position_52w": pos_factor,
            **extra,
        }),
    }


def _build_signals_summary(factors: dict) -> dict:
    """
    Quick-scan summary: top bullish and top bearish factor signals.
    A factor is 'bullish' when its buy score >= 70% of its declared max,
    'bearish' when its sell_score >= 70% of max.
    """
    bullish: list[dict] = []
    bearish: list[dict] = []
    n_valid = 0
    for name, f in factors.items():
        if not isinstance(f, dict):
            continue
        score = f.get("score") or 0
        sell  = f.get("sell_score") or 0
        mx    = f.get("max") or 10
        signal = (f.get("details") or {}).get("signal") or f.get("signal", "")
        n_valid += 1
        if score / mx >= 0.70:
            bullish.append({"factor": name, "score": score, "signal": signal})
        if sell / mx >= 0.70:
            bearish.append({"factor": name, "sell_score": sell, "signal": signal})

    bullish.sort(key=lambda x: -x["score"])
    bearish.sort(key=lambda x: -x["sell_score"])
    consensus_pct = round(len(bullish) / n_valid * 100, 0) if n_valid else 0
    return {
        "top_bullish":          bullish[:5],
        "top_bearish":          bearish[:5],
        "bullish_count":        len(bullish),
        "bearish_count":        len(bearish),
        "factor_consensus_pct": consensus_pct,
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


# ─────────────────────────────────────────────────────────────────────────────
# Text report (--text mode, for Discord bot fx command)
# ─────────────────────────────────────────────────────────────────────────────

def _stars(score: float, max_score: float = 100) -> str:
    pct = score / max_score
    filled = round(pct * 5)
    return "★" * filled + "☆" * (5 - filled)


def _score_tag(score: float) -> str:
    if score >= 80: return "极强 🟢"
    if score >= 65: return "偏强 🟢"
    if score >= 50: return "中性 🟡"
    if score >= 35: return "偏弱 🔴"
    return "极弱 🔴"


def _factor_bar(score: float, max_score: float = 10) -> str:
    """Return a compact level label for a single factor score."""
    pct = (score or 0) / max_score if max_score else 0
    if pct >= 0.75: return "强"
    if pct >= 0.50: return "中上"
    if pct >= 0.25: return "中下"
    return "弱"


_FACTOR_ZH_REPORT = {
    "value":               "估值",
    "growth":              "成长",
    "momentum":            "动量",
    "quality":             "质量",
    "northbound":          "北向(流向)",
    "volume":              "放量突破",
    "position_52w":        "52周位置",
    "div_yield":           "股息率",
    "volume_ratio":        "量比",
    "ma_alignment":        "均线排列",
    "low_volatility":      "低波动",
    "reversal":            "反转信号",
    "accruals":            "应计质量",
    "asset_growth":        "资产扩张",
    "piotroski":           "Piotroski",
    "short_interest":      "融券做空",
    "rsi_signal":          "RSI",
    "macd_signal":         "MACD",
    "turnover_percentile": "换手分位",
    "chip_distribution":   "筹码集中",
    "shareholder_change":  "股东变化",
    "lhb":                 "龙虎榜",
    "lockup_pressure":     "解禁压力",
    "insider":             "内部交易",
    "institutional_visits":"机构调研",
    "industry_momentum":   "行业动量",
    "northbound_actual":   "北向持仓",
    "earnings_revision":   "业绩预期",
    "limit_hits":          "涨停强度",
    "price_inertia":       "价格惯性",
    "social_heat":         "社交热度",
    "market_regime":       "市场状态",
    "concept_momentum":    "概念动量",
    "idiosyncratic_vol":   "特质波动",
    "gap_frequency":       "跳空频率",
    "atr_normalized":      "ATR波动",
    "return_skewness":     "收益偏度",
    "ma60_deviation":      "MA60偏离",
    "divergence":          "背离信号",
    "main_inflow":         "主力流入",
    "cash_flow_quality":   "现金流质量",
    "amihud_illiquidity":  "流动性",
    "max_return":          "最大涨幅",
    "turnover_acceleration":"换手加速",
    "upday_ratio":         "上涨天比",
    "roe_trend":           "ROE趋势",
    "nearness_to_high":    "接近高点",
    "hammer_bottom":       "锤形底",
    "limit_open_rate":     "开板率",
    "medium_term_momentum":"中期动量",
    "price_volume_corr":   "量价相关",
}


def format_text_report(result: dict) -> str:
    """Generate a concise Chinese text report from research() result dict."""
    if "error" in result:
        return f"❌ {result['error']}"

    name      = result.get("name", "")
    code      = result.get("code", "")
    basic     = result.get("basic", {})
    price     = result.get("price", {})
    val       = result.get("valuation", {})
    factors   = result.get("factors", {})
    total     = result.get("total_score", 0) or 0
    sell      = result.get("total_sell_score", 0) or 0
    signals   = result.get("signals_summary", {})
    financial = result.get("financial", {})

    industry  = basic.get("industry", "")
    mktcap    = basic.get("market_cap_billion", 0)
    cur_price = price.get("current", 0) or 0
    chg_pct   = price.get("change_pct", 0) or 0
    chg_sign  = "+" if chg_pct >= 0 else ""
    pe        = val.get("pe_ttm")
    pb        = val.get("pb")
    pe_pct    = val.get("pe_percentile")

    lines = []

    # ── Header ────────────────────────────────────────────────────────────────
    cap_str = f"{mktcap:.0f}亿" if mktcap else "N/A"
    lines.append(f"【{name} {code}】{industry} | 市值{cap_str} | {cur_price:.2f}元 ({chg_sign}{chg_pct:.1f}%)")
    lines.append("")

    # ── Overall score ─────────────────────────────────────────────────────────
    stars = _stars(total)
    tag   = _score_tag(total)
    lines.append(f"▌ 综合买入分: {total:.1f}/100  {stars}  {tag}")
    if sell >= 50:
        lines.append(f"▌ 卖出压力:   {sell:.1f}/100  ⚠️ 有较强卖出信号")
    elif sell >= 35:
        lines.append(f"▌ 卖出压力:   {sell:.1f}/100  注意")
    lines.append("")

    # ── Fundamentals ──────────────────────────────────────────────────────────
    lines.append("基本面")
    # Valuation
    pe_str = f"PE {pe:.1f}x" if pe else "PE N/A"
    pb_str = f"PB {pb:.2f}x" if pb else ""
    if pe_pct is not None:
        if pe_pct >= 75:
            pe_comment = f"，历史高位({pe_pct:.0f}%分位) ⚠️"
        elif pe_pct <= 25:
            pe_comment = f"，历史低位({pe_pct:.0f}%分位) ✅"
        else:
            pe_comment = f"，历史{pe_pct:.0f}%分位"
    else:
        pe_comment = ""
    val_score = (factors.get("value") or {}).get("score", 0) or 0
    lines.append(f"  估值  {pe_str}  {pb_str}{pe_comment}  [{_factor_bar(val_score)}]")

    # Growth
    rev_g = financial.get("revenue_growth_yoy")
    np_g  = financial.get("net_profit_growth_yoy")
    roe   = financial.get("roe")
    gr_score = (factors.get("growth") or {}).get("score", 0) or 0
    g_parts = []
    if rev_g is not None: g_parts.append(f"营收增速{rev_g:+.1f}%")
    if np_g  is not None: g_parts.append(f"净利增速{np_g:+.1f}%")
    g_str = "  ".join(g_parts) if g_parts else "数据不足"
    lines.append(f"  成长  {g_str}  [{_factor_bar(gr_score)}]")

    # Quality
    q_score = (factors.get("quality") or {}).get("score", 0) or 0
    roe_str = f"ROE {roe:.1f}%" if roe else "ROE N/A"
    pf_score = (factors.get("piotroski") or {}).get("score", 0) or 0
    pf_val = (factors.get("piotroski") or {}).get("details", {}).get("f_score")
    pf_str = f"  Piotroski {pf_val}/9" if pf_val is not None else ""
    lines.append(f"  质量  {roe_str}{pf_str}  [{_factor_bar(q_score)}]")
    lines.append("")

    # ── Technicals ────────────────────────────────────────────────────────────
    lines.append("技术面")
    tech = result.get("technical", {})

    mom_score = (factors.get("momentum") or {}).get("score", 0) or 0
    mom_detail = (factors.get("momentum") or {}).get("details", {})
    ret_20 = mom_detail.get("ret_20d")
    mom_str = f"近20日{ret_20:+.1f}%" if ret_20 is not None else ""
    lines.append(f"  动量      {mom_str}  [{_factor_bar(mom_score)}]")

    ma_score  = (factors.get("ma_alignment") or {}).get("score", 0) or 0
    ma_signal = (factors.get("ma_alignment") or {}).get("details", {}).get("signal", "")
    lines.append(f"  均线排列  {ma_signal}  [{_factor_bar(ma_score)}]")

    rsi_score  = (factors.get("rsi_signal") or {}).get("score", 0) or 0
    rsi_detail = (factors.get("rsi_signal") or {}).get("details", {})
    rsi_val    = rsi_detail.get("rsi14")
    rsi_str    = f"RSI {rsi_val:.0f}" if rsi_val is not None else "RSI N/A"
    if rsi_val:
        if rsi_val >= 75: rsi_str += " ⚠️超买"
        elif rsi_val <= 30: rsi_str += " 超卖✅"
    lines.append(f"  RSI       {rsi_str}  [{_factor_bar(rsi_score)}]")

    macd_score  = (factors.get("macd_signal") or {}).get("score", 0) or 0
    macd_signal = (factors.get("macd_signal") or {}).get("details", {}).get("signal", "")
    lines.append(f"  MACD      {macd_signal}  [{_factor_bar(macd_score)}]")

    mtm_score = (factors.get("medium_term_momentum") or {}).get("score", 0) or 0
    lines.append(f"  中期动量  [{_factor_bar(mtm_score)}]")
    lines.append("")

    # ── Fund flow ─────────────────────────────────────────────────────────────
    lines.append("资金面")
    inf_score  = (factors.get("main_inflow") or {}).get("score", 0) or 0
    inf_detail = (factors.get("main_inflow") or {}).get("details", {})
    inf_val    = inf_detail.get("net_inflow_5d_m")
    inf_str    = f"主力近5日净{'流入' if (inf_val or 0) > 0 else '流出'} {abs(inf_val or 0):.1f}百万" if inf_val is not None else "主力流向 N/A"
    lines.append(f"  {inf_str}  [{_factor_bar(inf_score)}]")

    nba_score  = (factors.get("northbound_actual") or {}).get("score", 0) or 0
    nba_signal = (factors.get("northbound_actual") or {}).get("details", {}).get("signal", "")
    lines.append(f"  北向持仓  {nba_signal}  [{_factor_bar(nba_score)}]")

    margin = result.get("margin", {})
    if margin.get("available"):
        m_trend = {"increasing": "融资余额上升 ✅", "decreasing": "融资余额下降", "flat": "融资余额平稳"}.get(
            margin.get("trend", ""), "")
        m_chg = margin.get("change_5d_pct")
        m_chg_str = f" ({m_chg:+.1f}%/5d)" if m_chg is not None else ""
        lines.append(f"  {m_trend}{m_chg_str}")
    lines.append("")

    # ── Top bullish signals ───────────────────────────────────────────────────
    bullish = signals.get("top_bullish", [])
    if bullish:
        lines.append(f"强势因子 ({signals.get('bullish_count', 0)}个触发)")
        for b in bullish[:4]:
            fname = _FACTOR_ZH_REPORT.get(b["factor"], b["factor"])
            sig   = b.get("signal", "")
            sig_str = f"  {sig}" if sig else ""
            lines.append(f"  ✅ {fname}{sig_str}")
        lines.append("")

    # ── Risk warnings ─────────────────────────────────────────────────────────
    bearish = signals.get("top_bearish", [])
    if bearish:
        lines.append(f"风险因子 ({signals.get('bearish_count', 0)}个触发)")
        for b in bearish[:3]:
            fname = _FACTOR_ZH_REPORT.get(b["factor"], b["factor"])
            sig   = b.get("signal", "")
            sig_str = f"  {sig}" if sig else ""
            lines.append(f"  ⚠️ {fname}{sig_str}")
        lines.append("")

    # ── Action suggestion ─────────────────────────────────────────────────────
    lines.append("操作建议")
    bullish_n = signals.get("bullish_count", 0)
    bearish_n = signals.get("bearish_count", 0)
    if total >= 70 and sell < 40:
        action = "信号偏多，可考虑关注/轻仓介入，注意止损"
    elif total >= 70 and sell >= 40:
        action = "买入分高但有卖出信号，谨慎操作，建议等待企稳"
    elif total >= 55 and sell < 35:
        action = "信号中性偏强，可观望等待更明确信号"
    elif sell >= 55:
        action = "卖出信号较强，持仓者建议减仓或止损"
    elif total < 40:
        action = "信号偏弱，不建议追入，等待改善"
    else:
        action = "信号混合，建议观望"
    lines.append(f"  → {action}")

    return "\n".join(lines)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Deep research report for an A-share stock")
    parser.add_argument("target", nargs="+", help="Stock code (600519) or Chinese name (贵州茅台)")
    parser.add_argument("--weights", type=str, default="",
                        help='Weight preference string, e.g. "focus on growth" or "重视成长"')
    parser.add_argument("--text", action="store_true",
                        help="Output human-readable Chinese text report instead of JSON")
    args = parser.parse_args()

    target_str = " ".join(args.target)
    w = parse_weights(args.weights) if args.weights else DEFAULT_WEIGHTS
    result = research(target_str, weights=w)
    if args.text:
        print(format_text_report(result))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
