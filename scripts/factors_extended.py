"""
Extended factor scoring functions (17 additional dimensions).

Group A — computed from data already fetched in research.py:
  reversal, accruals, asset_growth, piotroski,
  short_interest, rsi_signal, macd_signal, turnover_percentile,
  chip_distribution

Group B — require additional per-stock API calls:
  shareholder_change, lhb, lockup_pressure, insider,
  institutional_visits, industry_momentum, northbound_actual, earnings_revision

All functions return: {"score": float, "max": int, "details": dict}
Missing-data neutral is factor_max * 0.4 (matches existing convention).
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract(df: pd.DataFrame, candidates: list[str]) -> Optional[float]:
    """Return the most recent non-null value for the first matching column."""
    for col in candidates:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if not vals.empty:
                return float(vals.iloc[0])
    return None


def _extract_two(df: pd.DataFrame, candidates: list[str]) -> tuple[Optional[float], Optional[float]]:
    """Return (most_recent, one_period_back) for the first matching column."""
    for col in candidates:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(vals) >= 2:
                return float(vals.iloc[0]), float(vals.iloc[1])
            elif len(vals) == 1:
                return float(vals.iloc[0]), None
    return None, None


def _neutral(max_pts: int) -> dict:
    """Neutral result: buy_score = 40% of max, sell_score = 20% of max (no data = no strong sell)."""
    neutral_buy = round(max_pts * 0.4, 1)
    neutral_sell = round(max_pts * 0.2, 1)
    return {"score": neutral_buy, "sell_score": neutral_sell, "max": max_pts,
            "details": {"signal": "no data, neutral", "sell_score": neutral_sell}}


def _get_price_position(price_df) -> Optional[float]:
    """Return 52-week price position (0.0–1.0) or None if unavailable."""
    if price_df is None or len(price_df) < 20 or "close" not in price_df.columns:
        return None
    window = price_df["close"].tail(252)
    high_52w = float(window.max())
    low_52w  = float(window.min())
    current  = float(window.iloc[-1])
    if high_52w <= low_52w:
        return None
    return (current - low_52w) / (high_52w - low_52w)


# ===========================================================================
# GROUP A — From already-fetched data
# ===========================================================================

def score_reversal(
    price_df: Optional[pd.DataFrame],
    financial_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Short-term reversal factor (max 10).
    Contrarian: recent extreme losers are mean-reversion candidates.
    Strong in A-share markets due to retail overreaction.
      ret_1m <= -15%  -> 10 pts
      ret_1m in [-15, 0%] -> linear 5-10 pts
      ret_1m in [0, +10%] -> linear 3-5 pts
      ret_1m >= +10%  ->  1 pt  (overbought, reversal risk)

    Context cross with 52w price position:
      ret_1m <= -10% + position < 0.3  -> true retail panic at lows, buy +2
      ret_1m <= -10% + position > 0.7  -> trend reversal beginning (not a dip), buy -4, sell +3
      ret_1m >= +10% + position < 0.3  -> possible bottom breakout, sell -2

    Volume cross (量价交叉):
      ret_1m <= -10% + volume drying up (v5/v10 < 0.80) -> selling exhausted, classic bottom formation, buy +2
      ret_1m <= -10% + volume still elevated (v5/v10 > 1.20) -> panic ongoing, not the bottom yet, buy -2

    Fundamental quality cross: falling knife vs genuine oversold
      ret_1m <= -10% + ROE >= 12% + debt <= 60% -> solid fundamentals, market overreaction -> buy +1.5
      ret_1m <= -10% + ROE < 5% or debt > 70%   -> weak fundamentals, structural decline -> buy -2.5, sell +2

    Earnings revision cross: real reversal vs. dead-cat bounce (requires revision_df)
      Oversold (ret_1m <= -10%) + net upgrades >= 2   -> fundamentals improving, genuine reversal -> buy +2
      Oversold                  + net downgrades <= -2 -> fundamentals still deteriorating         -> buy -1.5, sell +1.5
      Overbought (ret >= +10%)  + net upgrades >= 2   -> rally justified, soften overbought sell  -> sell -1.5
      Overbought                + net downgrades <= -2 -> rally unjustified, amplify sell         -> sell +1.5
    """
    if price_df is None or len(price_df) < 22 or "close" not in price_df.columns:
        return _neutral(10)

    close = price_df["close"]
    current = float(close.iloc[-1])
    past = float(close.iloc[-21]) if len(close) >= 21 else None
    if past is None or past <= 0:
        return _neutral(10)

    ret_1m = (current - past) / past * 100

    if ret_1m <= -15:
        score = 10.0
    elif ret_1m < 0:
        score = 5.0 + (-ret_1m / 15) * 5.0
    elif ret_1m < 10:
        score = 5.0 - (ret_1m / 10) * 2.0
    else:
        score = 1.0

    signal = ("strong oversold" if ret_1m <= -15 else
              "oversold" if ret_1m < 0 else
              "neutral" if ret_1m < 10 else "overbought")

    # --- Sell score: overbought reversal risk ---
    if ret_1m >= 20:
        sell_score = 9.0
    elif ret_1m >= 10:
        # linear: 10% -> 5pts, 20% -> 9pts
        sell_score = 5.0 + (ret_1m - 10) / 10 * 4.0
    elif ret_1m >= 5:
        sell_score = 2.0
    else:
        sell_score = 0.0

    # --- Context cross: 52w price position ---
    position = _get_price_position(price_df)
    if position is not None:
        if ret_1m <= -10:
            if position < 0.3:
                # True bottom panic: stock beaten down + retail capitulating → strong buy
                score = min(10.0, score + 2.0)
                signal = "strong oversold (low position — true panic, reversal likely)"
            elif position > 0.7:
                # High position drop: this is a trend reversal START, not a dip to buy
                score = max(0.0, score - 4.0)
                sell_score = min(10.0, sell_score + 3.0)
                signal = "high position decline — trend start, NOT a reversal opportunity"
        elif ret_1m >= 10:
            if position < 0.3:
                # Low position rally: possible base breakout — soften overbought sell
                sell_score = max(0.0, sell_score - 2.0)
                signal = "low position rally — possible bottom breakout"

    # --- Volume cross: is selling pressure drying up? ---
    vol_ratio = None
    try:
        if "volume" in price_df.columns and len(price_df) >= 15:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 15:
                v5  = float(vol.tail(5).mean())
                v10 = float(vol.tail(15).head(10).mean())
                if v10 > 0:
                    vol_ratio = v5 / v10
    except Exception:
        pass

    if vol_ratio is not None and ret_1m <= -10:
        if vol_ratio < 0.80:
            # Volume drying up on the way down = sellers running out = classic bottom formation
            score = min(10.0, score + 2.0)
            signal = signal + " + volume drying up (bottom formation)"
        elif vol_ratio > 1.20:
            # Volume still high during decline = panic still ongoing = premature to buy
            score = max(0.0, score - 2.0)
            signal = signal + " + volume elevated (panic ongoing, not the bottom yet)"

    # --- Fundamental quality cross: is this a genuine oversold or a falling knife? ---
    if financial_df is not None and ret_1m <= -10:
        roe = _extract(financial_df, ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"])
        debt = _extract(financial_df, ["资产负债率(%)", "负债率(%)"])
        fundamentally_strong = (roe is not None and roe >= 12
                                 and (debt is None or debt <= 60))
        fundamentally_weak   = (roe is not None and roe < 5) or (debt is not None and debt > 70)

        if fundamentally_strong:
            # Good business oversold: market overreacted, fundamentals intact
            score = min(10.0, score + 1.5)
            signal = signal + " + solid fundamentals (genuine oversold)"
        elif fundamentally_weak:
            # Weak business declining: may continue falling (falling knife)
            score = max(0.0, score - 2.5)
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " + weak fundamentals (falling knife risk)"

    # --- Magnitude cross: extreme declines elevate mean-reversion probability ---
    if ret_1m <= -25:
        # Extreme capitulation: 25%+ 1m decline is statistically rare and creates peak oversold
        score      = min(10.0, score + 1.5)
        sell_score = max(0.0, sell_score - 2.0)
        signal     = signal + " (extreme capitulation: >25% 1m — mean-reversion probability peaks)"
    elif ret_1m <= -20:
        # Deep decline: even without -25% threshold, strongly oversold
        score      = min(10.0, score + 0.5)
        sell_score = max(0.0, sell_score - 1.0)
        signal     = signal + " (deep decline: >20% 1m — elevated reversal probability)"

    # --- Earnings revision cross: real reversal vs dead-cat bounce ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if ret_1m <= -10:
                if net_rev >= 2:
                    # Fundamentals improving while price oversold: genuine reversal signal
                    score = min(10.0, score + 2.0)
                    signal = signal + f" + analyst upgrades (net {net_rev:+d}) — genuine reversal"
                elif net_rev <= -2:
                    # Still being cut while price down: dead-cat bounce risk
                    score = max(0.0, score - 1.5)
                    sell_score = min(10.0, sell_score + 1.5)
                    signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dead-cat risk"
            elif ret_1m >= 10:
                if net_rev >= 2:
                    # Overbought but fundamentals justify it: soften sell pressure
                    sell_score = max(0.0, sell_score - 1.5)
                    signal = signal + f" + analyst upgrades (net {net_rev:+d}) — rally justified"
                elif net_rev <= -2:
                    # Overbought and analysts cutting: rally unjustified, amplify sell
                    sell_score = min(10.0, sell_score + 1.5)
                    signal = signal + f" + analyst downgrades (net {net_rev:+d}) — rally unjustified"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "return_1m_pct": round(ret_1m, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "vol_ratio_5d_10d": round(vol_ratio, 2) if vol_ratio is not None else None,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_accruals(financial_df: Optional[pd.DataFrame]) -> dict:
    """
    Earnings quality via accruals (max 10).
    accruals_ratio = (net_income - operating_cashflow) / total_assets (%).
    Negative accruals = cash-backed earnings = quality signal.
      ratio <= -5%  -> 10 pts
      ratio == 0    ->  5 pts
      ratio >= +10% ->  0 pts

    Cross with profit growth rate (both from financial_df):
      High accruals (>= 5%) + high profit growth (>= 20%) -> inflated growth story -> sell +2
      Low accruals (<= -5%) + high profit growth (>= 20%) -> genuine cash-backed growth -> buy +2
      High accruals (>= 5%) + profit growth < 0           -> bad quality AND shrinking -> sell +1
    """
    if financial_df is None or financial_df.empty:
        return _neutral(10)

    net_income = _extract(financial_df, ["净利润(元)", "归母净利润(元)", "净利润"])
    op_cf = _extract(financial_df, [
        "经营活动现金流量净额(元)", "经营活动产生的现金流量净额",
        "经营现金流净额", "经营活动净现金流量",
    ])
    total_assets = _extract(financial_df, ["总资产(元)", "资产总计(元)", "资产总额"])

    if net_income is None or op_cf is None:
        # Try ratio-based fallback
        ratio_raw = _extract(financial_df, [
            "经营活动净现金流/营业收入", "现金流量比率",
        ])
        if ratio_raw is None:
            return _neutral(10)
        # Interpret as % of revenue; assume neutral if can't compute vs assets
        accruals_pct = -ratio_raw  # high CF/revenue -> low accruals
    else:
        accruals = net_income - op_cf
        denom = total_assets if (total_assets and total_assets > 0) else abs(net_income) * 10
        accruals_pct = accruals / denom * 100 if denom else 0.0

    if accruals_pct <= -5:
        score = 10.0
    elif accruals_pct <= 0:
        score = 5.0 + (-accruals_pct / 5) * 5.0
    elif accruals_pct <= 10:
        score = 5.0 * (1 - accruals_pct / 10)
    else:
        score = 0.0

    signal = ("cash-rich" if accruals_pct <= -5 else
              "good quality" if accruals_pct <= 0 else
              "accrual-heavy" if accruals_pct <= 10 else "low quality")

    # --- Sell score: low earnings quality (accrual-heavy) ---
    if accruals_pct >= 10:
        sell_score = 9.0
    elif accruals_pct >= 0:
        # linear: 0% -> 2pts, 10% -> 9pts
        sell_score = 2.0 + (accruals_pct / 10) * 7.0
    else:
        sell_score = 0.0

    # --- Cross with profit growth: quality of growth ---
    profit_growth = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    profit_growth = float(vals.iloc[0])
                break

    if profit_growth is not None:
        if accruals_pct >= 5 and profit_growth >= 20:
            # High growth but not backed by cash: inflated earnings narrative
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " + high growth uninflated (earnings quality mismatch)"
        elif accruals_pct <= -5 and profit_growth >= 20:
            # High growth AND cash-backed: genuine quality growth
            score = min(10.0, score + 2.0)
            signal = signal + " + high cash-backed growth (genuine quality)"
        elif accruals_pct >= 5 and profit_growth < 0:
            # Declining profit + high accruals: fundamentals deteriorating fast
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " + declining profit (double quality warning)"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "accruals_pct": round(accruals_pct, 2),
            "profit_growth_pct": round(profit_growth, 1) if profit_growth is not None else None,
            "net_income": round(net_income / 1e8, 2) if net_income else None,
            "op_cashflow": round(op_cf / 1e8, 2) if op_cf else None,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_asset_growth(financial_df: Optional[pd.DataFrame]) -> dict:
    """
    Asset over-expansion penalty (max 10).
    Excessive total-asset growth signals overinvestment (destroys future returns).
      growth <= 5%   -> 10 pts (disciplined)
      growth 5-20%   -> linear 10-5 pts
      growth 20-50%  -> linear 5-2 pts
      growth >= 50%  ->  0 pts

    Quality cross: ROE level validates whether expansion is value-accretive
      Aggressive growth (>= 20%) + ROE >= 15% -> capital deployed productively, reduce sell (-2)
      Aggressive growth (>= 20%) + ROE < 5%   -> empire building without returns, amplify sell (+2)
    """
    if financial_df is None or financial_df.empty:
        return _neutral(10)

    # Try direct growth rate column first
    growth = _extract(financial_df, ["总资产增长率(%)", "资产增长率(%)", "资产总计增长率(%)"])

    if growth is None:
        # Compute from two consecutive periods
        cur, prev = _extract_two(financial_df, ["总资产(元)", "资产总计(元)"])
        if cur is not None and prev is not None and prev > 0:
            growth = (cur - prev) / prev * 100
        else:
            return _neutral(10)

    if growth <= 5:
        score = 10.0
    elif growth <= 20:
        score = 10.0 - (growth - 5) / 15 * 5.0
    elif growth <= 50:
        score = 5.0 - (growth - 20) / 30 * 3.0
    else:
        score = 0.0

    signal = ("disciplined" if growth <= 5 else
              "moderate" if growth <= 20 else
              "aggressive" if growth <= 50 else "over-expansion")

    # --- Sell score: over-expansion risk ---
    # In A-shares high growth is rewarded, but extreme over-expansion is risky
    if growth >= 50:
        sell_score = 8.0
    elif growth >= 30:
        sell_score = 5.0
    else:
        sell_score = 0.0

    # --- Quality cross: ROE level as validation of expansion quality ---
    roe = _extract(financial_df, ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"])
    if roe is not None and growth >= 20:
        if roe >= 15:
            # Expanding aggressively but generating excellent returns: productive deployment
            sell_score = max(0.0, sell_score - 2.0)
            signal = signal + " (productive — high ROE, expansion validated)"
        elif roe < 5:
            # Expanding aggressively but barely earning returns: empire building
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " (wasteful — low ROE, empire building)"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "asset_growth_pct": round(growth, 1),
            "roe_pct": round(roe, 1) if roe is not None else None,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_piotroski(
    financial_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    pe_pct: Optional[float] = None,
    pb_pct: Optional[float] = None,
) -> dict:
    """
    Piotroski F-score (max 9).
    9 binary signals: 1 if condition met, 0 otherwise.
    Profitability (4): ROA>0, CFO>0, ΔROA>0, CFO>NI
    Leverage (3): ΔDebt<0, ΔCurrentRatio>0, no share dilution
    Efficiency (2): ΔGrossMargin>0, ΔAssetTurnover>0

    Context cross with 52w price position:
      F-score >= 7 + low position (< 0.3)  -> quality-at-value setup, buy +2
      F-score <= 2 + high position (> 0.7) -> weak fundamentals at high price, sell +2

    Valuation cross (requires pe_pct / pb_pct from score_value):
      F-score >= 7 + cheap valuation (pe_pct <= 30 or pb_pct <= 30) -> buy +1.5
        (improving financial health at bargain price — data-driven GARP)
      F-score <= 3 + high valuation (pe_pct >= 80 or pb_pct >= 80) -> sell +1.5
        (deteriorating financials at premium price — priced-to-perfection with cracks showing)
    """
    if financial_df is None or financial_df.empty:
        return {"score": 4.0, "sell_score": 2.0, "max": 9, "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    signals: dict[str, int] = {}

    # --- Profitability ---
    roa_cur, roa_prev = _extract_two(financial_df, ["总资产净利率(%)", "资产净利率(ROA)(%)", "净资产收益率(%)"])
    cfo_cur, _ = _extract_two(financial_df, [
        "经营活动现金流量净额(元)", "经营活动产生的现金流量净额"])
    ni_cur, _ = _extract_two(financial_df, ["净利润(元)", "归母净利润(元)"])

    signals["roa_positive"]    = 1 if (roa_cur is not None and roa_cur > 0) else 0
    signals["cfo_positive"]    = 1 if (cfo_cur is not None and cfo_cur > 0) else 0
    signals["roa_improving"]   = 1 if (roa_cur is not None and roa_prev is not None
                                        and roa_cur > roa_prev) else 0
    if cfo_cur is not None and ni_cur is not None and ni_cur != 0:
        signals["accruals_ok"] = 1 if cfo_cur > ni_cur else 0
    else:
        signals["accruals_ok"] = 0

    # --- Leverage/Liquidity ---
    debt_cur, debt_prev = _extract_two(financial_df, ["资产负债率(%)", "负债率(%)"])
    cr_cur, cr_prev = _extract_two(financial_df, ["流动比率", "流动比率(倍)"])
    # Share dilution proxy: ROE with same ROA + lower equity = dilution
    # Use a simple heuristic: check if revenue_growth >> profit_growth (dilution signal)
    rev_g = _extract(financial_df, ["营业收入增长率(%)", "营收增长率"])
    prof_g = _extract(financial_df, ["净利润增长率(%)", "净利润同比增长率(%)"])

    signals["debt_decreasing"]     = 1 if (debt_cur is not None and debt_prev is not None
                                            and debt_cur < debt_prev) else 0
    signals["liquidity_improving"] = 1 if (cr_cur is not None and cr_prev is not None
                                            and cr_cur > cr_prev) else 0
    signals["no_dilution"]         = 1 if (rev_g is not None and prof_g is not None
                                            and prof_g >= rev_g - 5) else 0

    # --- Efficiency ---
    gm_cur, gm_prev = _extract_two(financial_df, ["销售毛利率(%)", "毛利率(%)"])
    at_cur, at_prev = _extract_two(financial_df, ["总资产周转率(次)", "资产周转率(次)"])

    signals["gross_margin_up"] = 1 if (gm_cur is not None and gm_prev is not None
                                        and gm_cur > gm_prev) else 0
    signals["asset_turnover_up"] = 1 if (at_cur is not None and at_prev is not None
                                          and at_cur > at_prev) else 0

    f_score = sum(signals.values())
    profitability = sum(signals[k] for k in ["roa_positive", "cfo_positive",
                                              "roa_improving", "accruals_ok"])
    leverage      = sum(signals[k] for k in ["debt_decreasing", "liquidity_improving",
                                              "no_dilution"])
    efficiency    = sum(signals[k] for k in ["gross_margin_up", "asset_turnover_up"])

    # --- Sell score: low F-score ---
    if f_score <= 2:
        sell_score = 8.0
    elif f_score <= 4:
        sell_score = 4.0
    else:
        sell_score = 0.0

    score = float(f_score)
    fscore_signal = ("strong" if f_score >= 7 else
                     "good" if f_score >= 5 else
                     "neutral" if f_score >= 3 else "weak")

    # --- Context cross: price position × F-score ---
    position = _get_price_position(price_df)
    if position is not None:
        if f_score >= 7 and position < 0.3:
            # Strong fundamentals + beaten-down price = quality-at-value setup
            score = min(9.0, score + 2.0)
            fscore_signal = "strong fundamentals at low price (quality-at-value)"
        elif f_score <= 2 and position > 0.7:
            # Weak fundamentals near highs = priced to perfection with no substance
            sell_score = min(9.0, sell_score + 2.0)
            fscore_signal = "weak fundamentals at high price (value trap risk)"

    # --- Valuation cross: F-score × PE/PB percentile ---
    if pe_pct is not None or pb_pct is not None:
        cheap_pf = ((pe_pct is not None and pe_pct <= 30)
                    or (pb_pct is not None and pb_pct <= 30))
        exp_pf   = ((pe_pct is not None and pe_pct >= 80)
                    or (pb_pct is not None and pb_pct >= 80))
        if f_score >= 7 and cheap_pf:
            score = min(9.0, score + 1.5)
            fscore_signal = fscore_signal + " + cheap valuation (improving financials at bargain price)"
        elif f_score <= 3 and exp_pf:
            sell_score = min(9.0, sell_score + 1.5)
            fscore_signal = fscore_signal + " + high valuation (deteriorating financials at premium)"

    return {
        "score": round(min(9.0, score), 1),
        "sell_score": round(sell_score, 1),
        "max": 9,
        "details": {
            "f_score": f_score,
            "profitability": profitability,
            "leverage_liquidity": leverage,
            "efficiency": efficiency,
            "signals": signals,
            "position_52w": round(position, 3) if position is not None else None,
            "signal": fscore_signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_short_interest(
    margin_df: Optional[pd.DataFrame],
    circulating_cap: float = 0,
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Short interest ratio: 融券余额 / 流通市值 (max 10).
    Low short interest is positive; high = bearish sentiment.
      ratio <= 0.5%  -> 10 pts
      ratio 0.5-3%   -> linear 10-3 pts
      ratio >= 5%    ->  0 pts

    Context cross with 52w price position:
      High short (>= 3%) + low position (< 0.3) -> short squeeze potential -> buy boost (+3)
      High short (>= 3%) + high position (> 0.7) -> shorts likely right -> sell boost (+2)

    Earnings revision cross: short squeeze catalyst (requires revision_df)
      High short (>= 3%) + net analyst upgrades >= 2  -> squeeze catalyst, buy +2
      High short (>= 3%) + net analyst downgrades <= -2 -> shorts confirmed by analysts, sell +2
    """
    if margin_df is None or margin_df.empty or circulating_cap <= 0:
        return _neutral(10)

    short_cols = [c for c in margin_df.columns
                  if any(k in c for k in ["融券余量金额", "融券余额", "融券余量"])]
    if not short_cols:
        return _neutral(10)

    series = pd.to_numeric(margin_df[short_cols[0]], errors="coerce").dropna()
    if series.empty:
        return _neutral(10)

    short_balance = float(series.iloc[-1])
    ratio = short_balance / circulating_cap * 100  # in %

    if ratio <= 0.5:
        score = 10.0
    elif ratio <= 3.0:
        score = 10.0 - (ratio - 0.5) / 2.5 * 7.0
    elif ratio <= 5.0:
        score = 3.0 - (ratio - 3.0) / 2.0 * 3.0
    else:
        score = 0.0

    signal = ("minimal short" if ratio <= 0.5 else
              "moderate short" if ratio <= 3.0 else "heavily shorted")

    # --- Sell score: high short interest ---
    if ratio >= 5:
        sell_score = 9.0
    elif ratio >= 3:
        # linear: 3% -> 5pts, 5% -> 9pts
        sell_score = 5.0 + (ratio - 3) / 2 * 4.0
    elif ratio >= 0.5:
        # linear: 0.5% -> 0pts, 3% -> 5pts
        sell_score = (ratio - 0.5) / 2.5 * 5.0
    else:
        sell_score = 0.0

    # --- Context cross: price position × short interest ---
    position = _get_price_position(price_df)
    if position is not None and ratio >= 3.0:
        if position < 0.3:
            # Low position + heavy short = squeeze setup (shorts trapped, stock beaten down)
            score = min(10.0, score + 3.0)
            signal = "squeeze potential (low position + heavy short)"
        elif position > 0.7:
            # High position + heavy short = shorts likely right at the top
            sell_score = min(10.0, sell_score + 2.0)
            signal = "bearish confirmation (high position + heavy short)"

    # --- Earnings revision cross: short squeeze catalyst ---
    net_revisions = None
    if revision_df is not None and not revision_df.empty and ratio >= 3.0:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级变动", "方向", "上调", "下调", "rating"])]
        if rating_cols:
            try:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up   = int(col_str.str.contains("上调|upgrade|buy|strong").sum())
                down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_revisions = up - down
                if net_revisions >= 2:
                    # Analysts upgrading a heavily shorted stock: classic squeeze setup
                    score = min(10.0, score + 2.0)
                    signal = signal + " + analyst upgrades (squeeze catalyst)"
                elif net_revisions <= -2:
                    # Both shorts and analysts are bearish: strong conviction sell
                    sell_score = min(10.0, sell_score + 2.0)
                    signal = signal + " + analyst downgrades (shorts confirmed)"
            except Exception:
                pass

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "short_balance_billion": round(short_balance / 1e8, 2),
            "ratio_pct": round(ratio, 3),
            "position_52w": round(position, 3) if position is not None else None,
            "net_revisions": net_revisions,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_rsi_signal(price_df: Optional[pd.DataFrame]) -> dict:
    """
    RSI zone scoring (max 10).
    Rewards oversold recovery setups; penalises extreme overbought.
      RSI <= 30  -> 10 pts (oversold)
      RSI 30-50  -> linear 10-5 pts (recovering)
      RSI 50-70  -> linear 5-3 pts (healthy trend)
      RSI >= 70  ->  1 pt  (overbought risk)

    Context cross with MA5/MA20 trend:
      RSI >= 70 + MA bullish (ma5 > ma20) -> confirmed uptrend, soften sell (-2)
      RSI >= 70 + MA bearish (ma5 < ma20) -> dead-cat bounce, amplify sell (+2)
      RSI <= 30 + MA bearish              -> falling knife, reduce buy (-3)
      RSI <= 30 + MA bullish              -> dip in uptrend, keep high buy (no change)
    """
    if price_df is None or len(price_df) < 20 or "close" not in price_df.columns:
        return _neutral(10)

    try:
        import ta
        rsi = float(ta.momentum.RSIIndicator(price_df["close"], window=14).rsi().iloc[-1])
    except Exception:
        return _neutral(10)

    if np.isnan(rsi):
        return _neutral(10)

    if rsi <= 30:
        score = 10.0
    elif rsi <= 50:
        score = 10.0 - (rsi - 30) / 20 * 5.0
    elif rsi <= 70:
        score = 5.0 - (rsi - 50) / 20 * 2.0
    else:
        score = 1.0

    signal = ("oversold" if rsi <= 30 else
              "recovering" if rsi <= 50 else
              "healthy" if rsi <= 70 else "overbought")

    # --- Sell score: overbought RSI ---
    if rsi >= 80:
        sell_score = 9.0
    elif rsi >= 70:
        # linear: 70 -> 5pts, 80 -> 9pts
        sell_score = 5.0 + (rsi - 70) / 10 * 4.0
    elif rsi >= 60:
        sell_score = 2.0
    else:
        sell_score = 0.0

    # --- Context cross: MA trend direction ---
    ma_bull = None
    try:
        ma5  = float(price_df["close"].tail(5).mean())
        ma20 = float(price_df["close"].tail(20).mean())
        ma_bull = ma5 > ma20
    except Exception:
        pass

    if ma_bull is not None:
        if rsi >= 70:
            if ma_bull:
                # Overbought in confirmed uptrend — may continue, soften sell pressure
                sell_score = max(0.0, sell_score - 2.0)
                signal = "overbought (uptrend confirmed — reduced sell pressure)"
            else:
                # Overbought but MA bearish — dead-cat bounce, amplify sell
                sell_score = min(10.0, sell_score + 2.0)
                signal = "overbought (bearish MA — likely dead-cat bounce)"
        elif rsi <= 30:
            if not ma_bull:
                # Oversold but MA still bearish — falling knife, reduce buy confidence
                score = max(1.0, score - 3.0)
                signal = "oversold (bearish MA — may continue falling)"
            # else: RSI oversold + MA bullish = healthy dip, no adjustment needed

    # --- Volume cross: does volume confirm RSI extreme? ---
    vol_ratio = None
    try:
        if "volume" in price_df.columns and len(price_df) >= 15:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 15:
                v5  = float(vol.tail(5).mean())
                v10 = float(vol.tail(15).head(10).mean())
                if v10 > 0:
                    vol_ratio = v5 / v10
    except Exception:
        pass

    if vol_ratio is not None:
        if rsi <= 30 and vol_ratio < 0.80:
            # Oversold + volume drying up = selling exhausted, high conviction reversal
            score = min(10.0, score + 2.0)
            signal = signal + " + volume drying (selling exhausted)"
        elif rsi >= 70 and vol_ratio < 0.80:
            # Overbought + volume shrinking = buyers leaving, confirms overbought sell
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " + volume drying (buyers leaving)"
        elif rsi >= 70 and vol_ratio > 1.30:
            # Overbought + expanding volume = strong momentum, might continue longer
            sell_score = max(0.0, sell_score - 1.0)
            signal = signal + " + volume expanding (momentum still strong)"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "rsi": round(rsi, 1),
            "ma_bull": ma_bull,
            "vol_ratio_5d_10d": round(vol_ratio, 2) if vol_ratio is not None else None,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_macd_signal(price_df: Optional[pd.DataFrame]) -> dict:
    """
    MACD histogram momentum scoring (max 10).
    Rewards strengthening positive momentum; penalises deteriorating.
      histogram > 0 and increasing  -> 8-10 pts
      histogram > 0 and decreasing  ->  5-7 pts
      histogram < 0 and increasing  ->  3-5 pts (recovery)
      histogram < 0 and decreasing  ->  0-2 pts

    Context cross with 52w price position:
      Bullish + low position  -> base breakout signal, buy +2
      Bullish + high position -> late-stage rally, sell +2
      Bearish + high position -> confirmed top, sell +2
      Bearish + low position  -> bottom testing, sell -2
    """
    if price_df is None or len(price_df) < 35 or "close" not in price_df.columns:
        return _neutral(10)

    try:
        import ta
        ind = ta.trend.MACD(price_df["close"])
        hist = ind.macd_diff()
        curr = float(hist.iloc[-1])
        prev = float(hist.iloc[-2])
    except Exception:
        return _neutral(10)

    if np.isnan(curr) or np.isnan(prev):
        return _neutral(10)

    improving = curr > prev

    if curr > 0 and improving:
        # Magnitude bonus: larger positive histogram = stronger signal
        mag = min(1.0, abs(curr) / (abs(prev) + 1e-9))
        score = 8.0 + mag * 2.0
        signal = "bullish strengthening"
    elif curr > 0 and not improving:
        score = 6.0
        signal = "bullish weakening"
    elif curr <= 0 and improving:
        score = 4.0
        signal = "bearish recovery"
    else:
        mag = min(1.0, abs(curr) / (abs(prev) + 1e-9))
        score = max(0.0, 2.0 - mag * 2.0)
        signal = "bearish deepening"

    # --- Sell score: bearish MACD momentum ---
    if curr <= 0 and not improving:
        # bearish deepening
        mag = min(1.0, abs(curr) / (abs(prev) + 1e-9))
        sell_score = 7.0 + mag * 2.0  # 7-9pts
    elif curr > 0 and not improving:
        # bullish weakening (falling fast)
        decline = abs(curr - prev) / (abs(prev) + 1e-9)
        sell_score = 3.0 + min(1.0, decline) * 2.0  # 3-5pts
    else:
        sell_score = 0.0

    sell_score = round(max(0.0, min(9.0, sell_score)), 1)

    # --- Context cross: 52w price position ---
    position = _get_price_position(price_df)
    if position is not None:
        if curr > 0 and improving:
            if position < 0.3:
                # Bullish MACD at low position = base breakout, strong accumulation signal
                score = min(10.0, score + 2.0)
                signal = "bullish strengthening (low position — base breakout)"
            elif position > 0.7:
                # Bullish MACD at high position = late-stage rally, caution
                sell_score = min(10.0, sell_score + 2.0)
                signal = "bullish strengthening (high position — late stage, caution)"
        elif curr <= 0 and not improving:
            if position > 0.7:
                # Bearish MACD at high position = confirmed distribution top
                sell_score = min(10.0, sell_score + 2.0)
                signal = "bearish deepening (high position — confirmed top)"
            elif position < 0.3:
                # Bearish MACD at low position = bottom testing, reversal may be close
                sell_score = max(0.0, sell_score - 2.0)
                signal = "bearish deepening (low position — bottom testing)"

    # --- Volume confirmation cross: is the MACD signal backed by real participation? ---
    vol_ratio = None
    try:
        if "volume" in price_df.columns and len(price_df) >= 60:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 60:
                v20 = float(vol.tail(20).mean())
                v60 = float(vol.tail(60).mean())
                if v60 > 0:
                    vol_ratio = v20 / v60
    except Exception:
        pass

    if vol_ratio is not None:
        if curr > 0 and improving and vol_ratio > 1.3:
            # Bullish MACD + expanding volume: signal confirmed by market participation
            score = min(10.0, score + 1.5)
            signal = signal + f" + volume expanding ×{vol_ratio:.1f} (confirmed)"
        elif curr > 0 and improving and vol_ratio < 0.8:
            # Bullish MACD + shrinking volume: likely a weak or false breakout
            score = max(0.0, score - 1.0)
            signal = signal + f" + volume contracting ×{vol_ratio:.1f} (questionable)"
        elif curr <= 0 and not improving and vol_ratio > 1.3:
            # Bearish MACD + expanding volume: breakdown confirmed, sellers piling in
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + f" + volume expanding ×{vol_ratio:.1f} (breakdown confirmed)"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "macd_diff": round(curr, 4),
            "macd_diff_prev": round(prev, 4),
            "trend": "improving" if improving else "deteriorating",
            "position_52w": round(position, 3) if position is not None else None,
            "vol_ratio_20_60": round(vol_ratio, 2) if vol_ratio is not None else None,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_turnover_percentile(price_df: Optional[pd.DataFrame]) -> dict:
    """
    Turnover rate vs 90-day rolling average (max 10).
    Rewards moderate elevated turnover (accumulation zone).
      ratio 1.5–3.0x -> 8-10 pts (sweet spot)
      ratio 1.0–1.5x ->  5-8 pts
      ratio < 0.8x   ->  2 pts (cold)
      ratio >= 4.0x  ->  5 pts (climax caution)
    """
    if price_df is None or len(price_df) < 10 or "turnover" not in price_df.columns:
        return _neutral(10)

    turnover = pd.to_numeric(price_df["turnover"], errors="coerce").dropna()
    if len(turnover) < 10:
        return _neutral(10)

    current_5d = float(turnover.tail(5).mean())
    avg_90d = float(turnover.tail(90).mean()) if len(turnover) >= 20 else float(turnover.mean())

    if avg_90d <= 0:
        return _neutral(10)

    ratio = current_5d / avg_90d

    # Get today's price direction
    last = price_df.iloc[-1]
    today_chg = float(last.get("change_pct", 0) or 0) if "change_pct" in price_df.columns else 0.0

    # --- Buy score: cross turnover ratio with price direction ---
    if ratio >= 4.0:
        if today_chg <= -2.0:
            score = 3.0   # high turnover + big drop = distribution, not a buy
            signal = "climax selloff (distribution)"
        else:
            score = 5.0   # keep climax caution for neutral/up days
            signal = "climax volume (caution)"
    elif ratio >= 3.0:
        if today_chg >= 1.0:
            score = 10.0
            signal = "strong accumulation confirmed"
        elif today_chg <= -2.0:
            score = 4.0
            signal = "high turnover selloff (caution)"
        else:
            score = 8.0
            signal = "strong accumulation"
    elif ratio >= 1.5:
        if today_chg >= 0.5:
            score = 8.0 + (ratio - 1.5) / 1.5 * 2.0
            score = min(10.0, score)
            signal = "active (price up)"
        elif today_chg <= -2.0:
            score = 4.0
            signal = "active volume but declining"
        else:
            score = 8.0 + (ratio - 1.5) / 1.5 * 2.0
            score = min(10.0, score)
            signal = "active"
    elif ratio >= 1.0:
        score = 5.0 + (ratio - 1.0) / 0.5 * 3.0
        signal = "slightly above average"
    elif ratio >= 0.8:
        if today_chg <= -0.5:
            score = 5.5   # 缩量下跌 — mild positive
            signal = "low volume decline (selling exhausted)"
        else:
            score = 5.0
            signal = "normal"
    else:
        if today_chg <= -0.5:
            score = 4.5   # very low volume decline — possible bottom
            signal = "very low volume decline (possible bottoming)"
        elif today_chg >= 0.5:
            score = 1.5   # 缩量上涨 — weak
            signal = "low volume rally (weak)"
        else:
            score = ratio / 0.8 * 2.0
            signal = "cold"

    # --- Sell score: cross turnover ratio with price direction ---
    if ratio >= 3.0 and today_chg <= -2.0:
        sell_score = 9.0   # high turnover + big drop = distribution
    elif ratio >= 1.5 and today_chg <= -2.0:
        sell_score = 6.0
    elif ratio < 0.8 and today_chg >= 1.0:
        sell_score = 5.0   # 缩量上涨
    elif ratio >= 4.0:
        sell_score = 7.0   # climax even without price context
    elif ratio >= 3.0:
        sell_score = 4.0
    else:
        sell_score = 0.0

    # --- 1m return cross: quiet accumulation vs active distribution ---
    ret_1m = None
    if len(price_df) >= 20 and "close" in price_df.columns:
        closes = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(closes) >= 20:
            ret_1m = float((closes.iloc[-1] - closes.iloc[-20]) / closes.iloc[-20] * 100)

    if ret_1m is not None:
        if ratio < 0.7 and 3.0 <= ret_1m <= 15.0:
            # Low volume + gradual price rise: retail exhaustion done, slow accumulation underway
            score = min(10.0, score + 1.5)
            signal = signal + " (quiet accumulation: price rising on shrinking volume)"
        elif ratio >= 2.0 and ret_1m <= -5.0:
            # High volume + sustained 1m decline: active distribution by holders
            sell_score = min(9.0, sell_score + 1.5)
            signal = signal + " (active distribution: sustained decline with elevated volume)"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "current_turnover_5d": round(current_5d, 2),
            "avg_90d_turnover": round(avg_90d, 2),
            "ratio": round(ratio, 2),
            "ret_1m": round(ret_1m, 1) if ret_1m is not None else None,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_chip_distribution(
    price_df: Optional[pd.DataFrame],
    fund_flow_df: Optional[pd.DataFrame],
) -> dict:
    """
    筹码分布 (Chip Distribution) cross-interaction factor.

    Returns TWO independent scores (both 0–10, higher = stronger signal):
      buy_score  — how strong the buy signal is
      sell_score — how strong the sell signal is

    The main `score` field = buy_score (used by compute_total_score as usual).
    sell_score is exposed in details for display in research reports.

    Key insight (A-share specific):
      - Bottom + retail panic sell (small-order outflow >> large-order outflow)
        = smart money holding while retail capitulates → strong BUY
      - Bottom + institutional exit (large-order outflow >> small-order outflow)
        = smart money leaving even at lows → strong SELL
      - Top + institutional distribution (large-order outflow >> small-order)
        = smart money exiting at high price → strong SELL
      - Top + retail exit + large-order buying
        = ambiguous (momentum or 对倒 wash trade) → neutral both sides
    """
    # ── Step 1: 52w price position ───────────────────────────────────────
    position: Optional[float] = None
    if price_df is not None and len(price_df) >= 20 and "close" in price_df.columns:
        window = price_df["close"].tail(252)
        hi = float(window.max())
        lo = float(window.min())
        cur = float(window.iloc[-1])
        if hi > lo:
            position = (cur - lo) / (hi - lo)   # 0 to 1

    # ── Step 2: 5-day large-order and small-order net flows ──────────────
    large_net_5d: Optional[float] = None
    small_net_5d: Optional[float] = None

    if fund_flow_df is not None and not fund_flow_df.empty:
        large_cols = [c for c in fund_flow_df.columns
                      if any(k in c for k in ["主力净流入", "大单净流入", "超大单净流入"])]
        small_cols = [c for c in fund_flow_df.columns
                      if any(k in c for k in ["小单净流入", "散单净流入", "小单净额"])]

        if large_cols:
            large_series = pd.to_numeric(fund_flow_df[large_cols[0]], errors="coerce").dropna()
            if not large_series.empty:
                large_net_5d = float(large_series.tail(5).sum())

        if small_cols:
            small_series = pd.to_numeric(fund_flow_df[small_cols[0]], errors="coerce").dropna()
            if not small_series.empty:
                small_net_5d = float(small_series.tail(5).sum())

    # ── Step 3: insufficient data → neutral ──────────────────────────────
    if position is None or large_net_5d is None:
        neutral_score = round(10 * 0.4, 1)
        return {
            "score": neutral_score, "sell_score": neutral_score, "max": 10,
            "details": {
                "buy_score": neutral_score, "sell_score": neutral_score,
                "scenario": "no data, neutral",
            },
        }

    # ── Step 4: classify order flow ──────────────────────────────────────
    epsilon = 1e-9
    large_sell = max(0.0, -large_net_5d)
    small_sell = max(0.0, -(small_net_5d if small_net_5d is not None else 0.0))
    total_sell = large_sell + small_sell

    # retail_sell_frac: fraction of total selling that comes from small orders (0–1)
    retail_sell_frac = small_sell / (total_sell + epsilon)

    buy_score: float = 0.0
    sell_score: float = 0.0
    scenario: str

    if position < 0.3:
        # ── Near 52w LOW ──────────────────────────────────────────────────
        if total_sell < epsilon:
            # Both sides buying at the bottom → accumulation
            buy_score  = 8.0
            sell_score = 0.0
            scenario   = "bottom accumulation (strong buy)"
        elif retail_sell_frac >= 0.6:
            # Retail panic capitulation, institutions relatively calm → buy
            strength   = min(1.0, (retail_sell_frac - 0.6) / 0.4)
            buy_score  = 7.0 + strength * 3.0
            sell_score = 0.0
            scenario   = "retail panic at bottom (buy signal)"
        elif retail_sell_frac <= 0.35:
            # Institutions exiting even at lows → dangerous, strong sell
            strength   = min(1.0, (0.35 - retail_sell_frac) / 0.35)
            buy_score  = 1.0
            sell_score = 7.0 + strength * 3.0
            scenario   = "institutional exit at bottom (strong sell)"
        else:
            # Mixed — neither signal is clear
            buy_score  = 4.0
            sell_score = 3.0
            scenario   = "mixed selling at bottom (neutral)"

    elif position > 0.7:
        # ── Near 52w HIGH ─────────────────────────────────────────────────
        if total_sell < epsilon:
            # Both sides still buying near the top — momentum
            buy_score  = 5.0
            sell_score = 2.0
            scenario   = "top momentum (mild caution)"
        elif retail_sell_frac <= 0.35:
            # Institutions selling heavily at the top → distribution warning
            strength   = min(1.0, (0.35 - retail_sell_frac) / 0.35)
            buy_score  = 2.0
            sell_score = 7.0 + strength * 3.0
            scenario   = "institutional distribution at top (sell signal)"
        elif retail_sell_frac >= 0.65:
            # Retail taking profit, large orders net buying — ambiguous (对倒 risk)
            buy_score  = 5.0
            sell_score = 4.0
            scenario   = "retail exit at top, large-order buying (ambiguous)"
        else:
            # Mixed
            buy_score  = 4.0
            sell_score = 4.0
            scenario   = "mixed selling at top (neutral)"

    else:
        # ── Mid-range — signal ambiguous ─────────────────────────────────
        buy_score  = 5.0
        sell_score = 2.0
        scenario   = "mid-range (neutral)"

    buy_score  = round(max(0.0, min(10.0, buy_score)),  1)
    sell_score = round(max(0.0, min(10.0, sell_score)), 1)

    return {
        "score": buy_score,   # main score used by compute_total_score
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "buy_score":  buy_score,
            "sell_score": sell_score,
            "position_52w_pct":      round(position * 100, 1),
            "large_net_5d_million":  round(large_net_5d / 1e6, 1),
            "small_net_5d_million":  round(small_net_5d / 1e6, 1) if small_net_5d is not None else None,
            "retail_sell_frac":      round(retail_sell_frac, 3),
            "scenario":              scenario,
        },
    }


# ===========================================================================
# GROUP B — From additional per-stock API calls
# ===========================================================================

def score_shareholder_change(
    shareholder_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Shareholder count quarterly change (max 15).
    Decreasing count = share concentration = bullish signal (A-share specific).
      change <= -10%  -> 15 pts
      change 0 to -10% -> linear 8-15 pts
      change 0 to +10% -> linear 3-8 pts
      change >= +20%  ->  0 pts

    Context cross with 52w price position:
      Concentration (change <= -5%) + low position (< 0.3)  -> smart money accumulating at lows -> buy +3
      Dispersion (change >= +10%) + high position (> 0.7)   -> top distribution confirmed -> sell +2
      Dispersion (change >= +10%) + low position (< 0.3)    -> retail bottom-fishing (less bearish) -> sell -2

    Earnings revision cross (dual institutional confirmation):
      Concentration (change <= -5%) + net analyst upgrades >= 2
        -> two independent signals (chip concentration + sell-side upgrade) pointing the same way -> buy +2
      Dispersion (change >= +10%) + net analyst downgrades <= -2
        -> smart money exiting + analysts cutting simultaneously -> sell +2 (dual exit signal)
    """
    if shareholder_df is None or shareholder_df.empty:
        return _neutral(15)

    holder_cols = [c for c in shareholder_df.columns
                   if any(k in c for k in ["股东人数", "股东总人数", "持股人数"])]
    if not holder_cols:
        return _neutral(15)

    series = pd.to_numeric(shareholder_df[holder_cols[0]], errors="coerce").dropna()
    if len(series) < 2:
        return _neutral(15)

    current = float(series.iloc[0])
    prev    = float(series.iloc[1])
    if prev <= 0:
        return _neutral(15)

    change_pct = (current - prev) / prev * 100

    if change_pct <= -10:
        score = 15.0
    elif change_pct <= 0:
        score = 8.0 + (-change_pct / 10) * 7.0
    elif change_pct <= 10:
        score = 8.0 - (change_pct / 10) * 5.0
    elif change_pct <= 20:
        score = 3.0 - (change_pct - 10) / 10 * 3.0
    else:
        score = 0.0

    signal = ("strong concentration" if change_pct <= -10 else
              "concentrating" if change_pct <= 0 else
              "dispersing" if change_pct <= 10 else "heavy distribution")

    # --- Sell score: shareholder count increasing (dispersion/distribution) ---
    if change_pct >= 20:
        sell_score = 12.0
    elif change_pct >= 10:
        # linear: 10% -> 7pts, 20% -> 12pts
        sell_score = 7.0 + (change_pct - 10) / 10 * 5.0
    elif change_pct >= 0:
        # linear: 0% -> 2pts, 10% -> 7pts
        sell_score = 2.0 + (change_pct / 10) * 5.0
    else:
        sell_score = 0.0

    sell_score = round(min(15.0, sell_score), 1)

    # --- Context cross: price position × shareholder change direction ---
    position = _get_price_position(price_df)
    if position is not None:
        if change_pct <= -5 and position < 0.3:
            # Concentration at low price: smart money picking up shares while stock is beaten down
            score = min(15.0, score + 3.0)
            signal = "strong concentration at low price (smart money accumulating)"
        elif change_pct >= 10 and position > 0.7:
            # Dispersion at high price: classic top — retail buying as institutions exit
            sell_score = min(15.0, sell_score + 2.0)
            signal = "dispersion at high price (top distribution confirmed)"
        elif change_pct >= 10 and position < 0.3:
            # Dispersion at low price: retail bottom-fishing (many new buyers entering)
            # Less bearish — could be driven by new long-term investors, not distribution
            sell_score = max(0.0, sell_score - 2.0)
            signal = "dispersion at low price (bottom-fishing, less bearish)"

    # --- Earnings revision cross: dual institutional confirmation ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if change_pct <= -5 and net_rev >= 2:
                # Chip concentration + analyst upgrades: two independent institutions pointing the same way
                score = min(15.0, score + 2.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — dual confirmation"
            elif change_pct >= 10 and net_rev <= -2:
                # Dispersion + analyst cuts: smart money exits + sell-side consensus deteriorates
                sell_score = min(15.0, sell_score + 2.0)
                signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dual exit signal"

    sell_score = round(sell_score, 1)

    return {
        "score": round(score, 1),
        "sell_score": sell_score,
        "max": 15,
        "details": {
            "current_holders": int(current),
            "prev_holders": int(prev),
            "change_pct": round(change_pct, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "signal": signal,
            "sell_score": sell_score,
        },
    }


def score_lhb(
    lhb_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Dragon-Tiger list net institutional buy 龙虎榜 (max 10).
    Net institutional buy (大单净买入) on LHB triggers = informed accumulation.
      net_buy >= 5000万  -> 10 pts
      net_buy 1000-5000万 -> linear 7-10 pts
      net_buy ±1000万     ->  5 pts (neutral)
      net_buy <= -5000万  ->  0 pts

    52-week position cross: LHB flow meaning changes completely with price level
      Net buy >= 1000万 + position < 0.3  -> institutional bottom discovery, buy +2
      Net sell <= -1000万 + position > 0.7 -> confirmed distribution at highs, sell +2
      Net buy >= 1000万 + position > 0.7  -> buying at highs = possible markup/exit setup, sell +1
    """
    if lhb_df is None or lhb_df.empty:
        return _neutral(10)

    buy_cols  = [c for c in lhb_df.columns if any(k in c for k in ["买入", "净买入", "净额"])]
    sell_cols = [c for c in lhb_df.columns if any(k in c for k in ["卖出"])]

    try:
        if buy_cols:
            net_buy = float(pd.to_numeric(lhb_df[buy_cols[0]], errors="coerce").sum())
            if sell_cols:
                net_buy -= float(pd.to_numeric(lhb_df[sell_cols[0]], errors="coerce").sum())
        else:
            return _neutral(10)
    except Exception:
        return _neutral(10)

    net_m = net_buy / 1e6  # in millions CNY

    if net_m >= 5000:
        score = 10.0
    elif net_m >= 1000:
        score = 7.0 + (net_m - 1000) / 4000 * 3.0
    elif net_m >= -1000:
        score = 5.0 + net_m / 1000 * 2.0
    elif net_m >= -5000:
        score = 5.0 + (net_m + 1000) / 4000 * 5.0
    else:
        score = 0.0

    signal = "strong buy" if net_m >= 1000 else ("neutral" if abs(net_m) < 1000 else "selling")

    # --- Sell score: strong institutional selling on Dragon-Tiger list ---
    if net_m <= -5000:
        sell_score = 9.0
    elif net_m <= -1000:
        # linear: -1000 -> 5pts, -5000 -> 9pts
        sell_score = 5.0 + (-net_m - 1000) / 4000 * 4.0
    elif net_m <= 0:
        sell_score = 2.0
    else:
        sell_score = 0.0

    sell_score = round(min(10.0, sell_score), 1)

    # --- 52w position cross: context determines meaning of LHB flow ---
    position = _get_price_position(price_df)
    if position is not None:
        if net_m >= 1000 and position < 0.3:
            # Institutions buying on LHB while stock is near 52w lows: genuine bottom discovery
            score = min(10.0, score + 2.0)
            sell_score = max(0.0, sell_score - 1.0)
            signal = "institutional bottom discovery (buy at 52w low)"
        elif net_m <= -1000 and position > 0.7:
            # Institutions selling on LHB while stock is near 52w highs: confirmed distribution
            sell_score = min(10.0, sell_score + 2.0)
            signal = "institutional distribution at highs (confirmed exit)"
        elif net_m >= 1000 and position > 0.7:
            # Buying at highs via LHB: could be late-stage markup or cover for distribution
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " at high price (possible markup/exit setup)"

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "net_buy_million": round(net_m, 1),
            "appearances": len(lhb_df),
            "position_52w": round(position, 2) if position is not None else None,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_lockup_pressure(
    lockup_df: Optional[pd.DataFrame],
    circulating_cap: float = 0,
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Upcoming lock-up expiry supply pressure (max 10, inverse signal).
    Large upcoming unlock → supply overhang → lower score.
      ratio <= 1%    -> 10 pts
      ratio 1-5%     -> linear 10-5 pts
      ratio 5-20%    -> linear 5-2 pts
      ratio >= 20%   ->  0 pts

    Context cross with 52w price position:
      Large unlock (>= 5%) + low position (< 0.3)  -> insiders underwater, unlikely to sell -> reduce sell (-3)
      Large unlock (>= 5%) + high position (> 0.7) -> insiders sitting on profits, likely to sell -> amplify sell (+2)

    Earnings growth cross: can buyers absorb the unlock supply? (requires financial_df)
      Large unlock (>= 5%) + profit growth >= 20% -> growing business attracts buyers, sell -2
      Large unlock (>= 5%) + profit growth < 0    -> declining earnings, no buyers to absorb supply, sell +2
    """
    if lockup_df is None or lockup_df.empty or circulating_cap <= 0:
        # No lockup data: neutral buy score, minimal sell score
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    # Look for unlock amount column
    amount_cols = [c for c in lockup_df.columns
                   if any(k in c for k in ["解禁数量", "解禁金额", "解禁市值", "解禁股数"])]
    if not amount_cols:
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    try:
        amounts = pd.to_numeric(lockup_df[amount_cols[0]], errors="coerce").dropna()
        if amounts.empty:
            return {"score": 2.0, "sell_score": 2.0, "max": 10,
                    "details": {"signal": "no data, neutral", "sell_score": 2.0}}

        # Sum unlocks within next 90 days if date column available
        date_cols = [c for c in lockup_df.columns if any(k in c for k in ["日期", "解禁日"])]
        if date_cols:
            lockup_df = lockup_df.copy()
            lockup_df["_date"] = pd.to_datetime(lockup_df[date_cols[0]], errors="coerce")
            today = pd.Timestamp.now()
            near_term = lockup_df[
                (lockup_df["_date"] >= today) &
                (lockup_df["_date"] <= today + pd.Timedelta(days=90))
            ]
            unlock_val = float(pd.to_numeric(near_term[amount_cols[0]], errors="coerce").sum())
        else:
            unlock_val = float(amounts.iloc[0])
    except Exception:
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    # If unlock value is in share count (not CNY), we can't compute ratio without price
    # Use simple ratio if within same unit as circulating_cap (CNY)
    ratio = unlock_val / circulating_cap * 100

    # buy_score: 2 for no/minimal upcoming lockup (neutral, not a real buy signal)
    if ratio <= 1:
        buy_score = 2.0
    elif ratio <= 5:
        buy_score = 2.0 - (ratio - 1) / 4 * 1.0   # slight negative
    else:
        buy_score = 0.0

    # sell_score: pressure-based — this IS the sell signal for this factor
    if ratio >= 20:
        sell_score = 9.0
    elif ratio >= 5:
        # linear: 5% -> 5pts, 20% -> 9pts
        sell_score = 5.0 + (ratio - 5) / 15 * 4.0
    elif ratio >= 1:
        # linear: 1% -> 1pt, 5% -> 5pts
        sell_score = 1.0 + (ratio - 1) / 4 * 4.0
    else:
        sell_score = 0.0

    sell_score = round(min(10.0, sell_score), 1)
    signal = ("clean" if ratio <= 1 else
              "moderate overhang" if ratio <= 5 else "heavy overhang")

    # --- Context cross: price position × unlock pressure ---
    position = _get_price_position(price_df)
    if position is not None and ratio >= 5:
        if position < 0.3:
            # Low price position: insiders are likely underwater → less incentive to sell
            sell_score = max(0.0, sell_score - 3.0)
            signal = f"{signal} (mitigated — low price, insiders likely underwater)"
        elif position > 0.7:
            # High price position: insiders sitting on large profits → strong incentive to sell
            sell_score = min(10.0, sell_score + 2.0)
            signal = f"{signal} (amplified — high price, insiders motivated to sell)"

    sell_score = round(sell_score, 1)

    # --- Earnings growth cross: does the business have enough buyers to absorb supply? ---
    if financial_df is not None and ratio >= 5:
        profit_growth = _extract(financial_df, [
            "净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"])
        if profit_growth is not None:
            if profit_growth >= 20:
                # Growing fast: fundamental buyers entering, absorbing unlock supply
                sell_score = max(0.0, sell_score - 2.0)
                signal = signal + " (growth attracts buyers — supply absorbed)"
            elif profit_growth < 0:
                # Declining earnings: no fundamental buyers; unlock supply hits a weak bid
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " (declining earnings — no buyers, amplified pressure)"

    sell_score = round(sell_score, 1)
    return {
        "score": round(max(0.0, buy_score), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "unlock_amount_billion": round(unlock_val / 1e8, 2),
            "ratio_pct": round(ratio, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "signal": signal,
            "sell_score": sell_score,
        },
    }


def score_insider(
    insider_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Major shareholder net buy/sell in past 6 months (max 10).
    Net buying = positive alignment; selling = negative signal.
      Net positive   -> 5-10 pts (proportional to buy/sell ratio)
      No activity    ->  5 pts (neutral)
      Net negative   -> 0-4 pts

    Context cross with 52w price position:
      Net buy + low position (< 0.3)   -> highest conviction buy (insiders invest at lows) -> buy +2
      Net sell + low position (< 0.3)  -> RED FLAG (selling underwater = structural problem) -> sell +3
      Net sell + high position (> 0.7) -> rational profit-taking confirmed -> sell +2
    """
    if insider_df is None or insider_df.empty:
        return _neutral(10)

    buy_cols  = [c for c in insider_df.columns if any(k in c for k in ["增持", "买入数量", "增持数量"])]
    sell_cols = [c for c in insider_df.columns if any(k in c for k in ["减持", "卖出数量", "减持数量"])]

    try:
        buy_total  = float(pd.to_numeric(insider_df[buy_cols[0]], errors="coerce").sum()) if buy_cols else 0.0
        sell_total = float(pd.to_numeric(insider_df[sell_cols[0]], errors="coerce").sum()) if sell_cols else 0.0
    except Exception:
        return _neutral(10)

    net = buy_total - sell_total
    total = buy_total + sell_total

    buy_events  = len(insider_df[insider_df[buy_cols[0]].notna()]) if buy_cols else 0
    sell_events = len(insider_df[insider_df[sell_cols[0]].notna()]) if sell_cols else 0

    if total == 0:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"net_shares": 0, "buy_events": 0, "sell_events": 0, "signal": "no activity", "sell_score": 0.0}}

    # Score based on net buy ratio
    net_ratio = net / total  # -1 to +1
    score = 5.0 + net_ratio * 5.0

    signal = ("strong buy" if net_ratio > 0.5 else
              "net buy" if net_ratio > 0 else
              "net sell" if net_ratio > -0.5 else "strong sell")

    # --- Sell score: insider net selling ---
    if net_ratio < -0.5:
        # strong sell: net_ratio from -0.5 to -1.0 -> 3 to 8pts
        sell_score = 3.0 + (-net_ratio - 0.5) / 0.5 * 5.0  # 3-8pts
    elif net_ratio <= 0:
        # net sell: 0 to -0.5 -> 0 to 3pts
        sell_score = (-net_ratio) / 0.5 * 3.0
    else:
        sell_score = 0.0

    sell_score = round(min(8.0, sell_score), 1)

    # --- Context cross: price position × insider transaction direction ---
    position = _get_price_position(price_df)
    if position is not None:
        if net_ratio > 0.3 and position < 0.3:
            # Insider buying at depressed prices: maximum conviction — they're putting money in at lows
            score = min(10.0, score + 2.0)
            signal = signal + " (at low price — highest conviction)"
        elif net_ratio < -0.3 and position < 0.3:
            # Insider selling even when underwater: RED FLAG — they see structural issues ahead
            sell_score = min(8.0, sell_score + 3.0)
            signal = signal + " (at low price — RED FLAG: selling underwater)"
        elif net_ratio < -0.3 and position > 0.7:
            # Insider selling near 52w high: rational profit-taking → amplify sell signal
            sell_score = min(8.0, sell_score + 2.0)
            signal = signal + " (at high price — profit-taking confirmed)"

    sell_score = round(sell_score, 1)

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "net_shares_million": round(net / 1e6, 1),
            "buy_events": buy_events,
            "sell_events": sell_events,
            "position_52w": round(position, 3) if position is not None else None,
            "signal": signal,
            "sell_score": sell_score,
        },
    }


def score_institutional_visits(
    visits_df: Optional[pd.DataFrame],
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Institutional research visit frequency in past 90 days (max 10).
    More visits = analyst/fund attention = rising conviction.
      visits >= 10  -> 10 pts
      visits  5-10  -> linear 7-10 pts
      visits  1-5   -> linear 3-7 pts
      visits == 0   ->  2 pts

    Earnings revision cross: early information signal (requires revision_df)
      Visits >= 5 (past 90d) + net revisions == 0 -> buy +1
        (institutions surveying ahead of consensus — the "pre-upgrade accumulation" pattern)
      Visits >= 5 + net upgrades >= 2             -> buy +1
        (visits AND upgrades together = institutional consensus crystallising)
    """
    if visits_df is None or visits_df.empty:
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"visit_count_90d": 0, "signal": "no visits recorded", "sell_score": 2.0}}

    # Filter to past 90 days if date column available
    date_cols = [c for c in visits_df.columns if any(k in c for k in ["日期", "调研日期", "接待日期"])]
    count = len(visits_df)
    if date_cols:
        try:
            visits_df = visits_df.copy()
            visits_df["_date"] = pd.to_datetime(visits_df[date_cols[0]], errors="coerce")
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
            count = int((visits_df["_date"] >= cutoff).sum())
        except Exception:
            pass

    if count >= 10:
        score = 10.0
    elif count >= 5:
        score = 7.0 + (count - 5) / 5 * 3.0
    elif count >= 1:
        score = 3.0 + (count - 1) / 4 * 4.0
    else:
        score = 2.0

    signal = ("high attention" if count >= 10 else
              "moderate" if count >= 5 else
              "low" if count >= 1 else "none")

    # --- Sell score: declining visits (not a strong sell signal) ---
    # 0 visits in 90 days = mild, analysts losing interest
    sell_score = 2.0 if count == 0 else 0.0

    # --- Earnings revision cross: early positioning vs. confirmed consensus ---
    if revision_df is not None and not revision_df.empty and count >= 5:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if net_rev == 0:
                # Many visits but no public upgrade yet: institutions building positions quietly
                score = min(10.0, score + 1.0)
                signal = signal + " (pre-upgrade: institutions active, no consensus yet)"
            elif net_rev >= 2:
                # Both visits and upgrades: consensus is crystallising
                score = min(10.0, score + 1.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — institutional consensus forming"

    return {
        "score": round(score, 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {"visit_count_90d": count, "signal": signal, "sell_score": sell_score},
    }


def score_industry_momentum(
    industry_ret_1m: Optional[float],
    market_ret_1m: Optional[float],
    price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Industry 1-month excess return vs broad market (max 10).
    Rewards stocks in industries with positive relative momentum.
      excess >= +5%   -> 10 pts
      excess  0-+5%   -> linear 5-10 pts
      excess -5-0%    -> linear 2-5 pts
      excess <= -5%   ->  0 pts

    Context cross with individual stock 52w position:
      Industry outperforming (excess >= 2%) + stock low position (< 0.3)
        -> sector hot but this stock hasn't moved yet = late-mover opportunity -> buy +2
      Industry underperforming (excess <= -2%) + stock high position (> 0.7)
        -> sector falling AND stock at highs = double negative -> sell +2
      Industry outperforming + stock high position (> 0.7)
        -> stock already rode the sector wave, late entry risk -> sell +1
      Industry underperforming + stock low position (< 0.3)
        -> sector falling but stock already beaten down, most damage done -> sell -1
    """
    if industry_ret_1m is None:
        return _neutral(10)

    market = market_ret_1m if market_ret_1m is not None else 0.0
    excess = industry_ret_1m - market

    if excess >= 5:
        score = 10.0
    elif excess >= 0:
        score = 5.0 + excess / 5 * 5.0
    elif excess >= -5:
        score = 2.0 + (excess + 5) / 5 * 3.0
    else:
        score = 0.0

    signal = ("outperforming" if excess >= 2 else
              "in-line" if excess >= -2 else "underperforming")

    # --- Sell score: industry underperforming market ---
    if excess <= -5:
        sell_score = 9.0
    elif excess <= 0:
        # linear: 0% -> 3pts, -5% -> 9pts
        sell_score = 3.0 + (-excess / 5) * 6.0
    else:
        sell_score = 0.0

    sell_score = round(min(9.0, sell_score), 1)

    # --- Context cross: industry momentum × individual stock price position ---
    position = _get_price_position(price_df)
    if position is not None:
        if excess >= 2 and position < 0.3:
            # Hot sector, stock hasn't moved: late-mover setup
            score = min(10.0, score + 2.0)
            signal = "outperforming sector + stock lagging (late-mover opportunity)"
        elif excess <= -2 and position > 0.7:
            # Weak sector, stock still high: catch-down risk
            sell_score = min(9.0, sell_score + 2.0)
            signal = "underperforming sector + stock at highs (catch-down risk)"
        elif excess >= 2 and position > 0.7:
            # Stock already moved with sector: late entry
            sell_score = min(9.0, sell_score + 1.0)
            signal = "outperforming sector + stock at highs (late entry risk)"
        elif excess <= -2 and position < 0.3:
            # Sector falling but stock already beaten down: most damage done
            sell_score = max(0.0, sell_score - 1.0)
            signal = "underperforming sector + stock already low (damage absorbed)"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "industry_ret_1m_pct": round(industry_ret_1m, 2),
            "market_ret_1m_pct": round(market, 2),
            "excess_pct": round(excess, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_northbound_actual(
    northbound_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Real 沪深港通 per-stock holding change (max 10).
    Distinct from score_northbound (which uses per-stock order flow).
    Uses actual share-count change over last 5 data points.
      change >= +5%   -> 10 pts
      change  0-+5%   -> linear 5-10 pts
      change -2-0%    -> linear 3-5 pts
      change <= -5%   ->  0 pts

    Context cross with 52w price position:
      NB reducing (< -2%) + low position (< 0.3)  -> likely passive redemption/ETF rebalancing -> reduce sell (-2.5)
      NB reducing (< -2%) + high position (> 0.7) -> active profit-taking exit -> amplify sell (+2)

    Momentum direction cross:
      NB buying (>= +2%) + 1m return <= -10%  -> smart money buying the dip, high conviction -> buy +2
      NB reducing (< -2%) + 1m return <= -10% -> foreign capital exiting declining stock -> sell +1.5

    Earnings revision cross: dual institutional confirmation (requires revision_df)
      NB increasing (>= +2%) + net upgrades >= 2   -> foreign + domestic institutions aligned -> buy +2
      NB reducing (<= -2%) + net downgrades <= -2  -> both institutional groups exiting       -> sell +2

    Industry momentum cross: contra-sector NB flow carries far more information
      NB buying (>= +2%) + industry excess <= -2%  -> single-stock pick against weak sector, buy +2
      NB buying (>= +2%) + industry excess >= +5%  -> trend-following, weaker signal, buy -1
      NB reducing (<= -2%) + industry excess >= +5% -> exiting a hot sector, sell +1.5
    """
    if northbound_df is None or northbound_df.empty:
        return _neutral(10)

    hold_cols = [c for c in northbound_df.columns
                 if any(k in c for k in ["持股数量", "持仓量", "持股比例", "持有股数"])]
    if not hold_cols:
        return _neutral(10)

    series = pd.to_numeric(northbound_df[hold_cols[0]], errors="coerce").dropna()
    if len(series) < 2:
        return _neutral(10)

    current = float(series.iloc[-1])
    past    = float(series.iloc[max(0, len(series) - 5)])

    if past <= 0:
        return _neutral(10)

    change_pct = (current - past) / past * 100

    if change_pct >= 5:
        score = 10.0
    elif change_pct >= 0:
        score = 5.0 + change_pct / 5 * 5.0
    elif change_pct >= -2:
        score = 3.0 + (change_pct + 2) / 2 * 2.0
    elif change_pct >= -5:
        score = 3.0 * (change_pct + 5) / 3
    else:
        score = 0.0

    signal = ("strong inflow" if change_pct >= 3 else
              "inflow" if change_pct >= 0 else
              "slight outflow" if change_pct >= -2 else "outflow")

    # --- Sell score: NB holdings declining ---
    if change_pct <= -5:
        sell_score = 9.0
    elif change_pct <= -2:
        # linear: -2% -> 5pts, -5% -> 9pts
        sell_score = 5.0 + (-change_pct - 2) / 3 * 4.0
    elif change_pct <= 0:
        sell_score = 2.0
    else:
        sell_score = 0.0

    sell_score = round(min(9.0, sell_score), 1)

    # --- Context cross: price position × NB flow direction ---
    position = _get_price_position(price_df)
    if position is not None and change_pct < -2:
        if position < 0.3:
            # NB reducing at low price = likely passive redemption (ETF weight rebalancing)
            # Not a genuine conviction exit → weaken sell signal
            sell_score = max(0.0, sell_score - 2.5)
            signal = f"{signal} (at low price — passive redemption likely)"
        elif position > 0.7:
            # NB reducing at high price = active profit-taking → stronger sell signal
            sell_score = min(10.0, sell_score + 2.0)
            signal = f"{signal} (at high price — active exit, stronger sell)"

    sell_score = round(sell_score, 1)

    # --- Momentum direction cross: is NB flow contrarian or confirmatory? ---
    ret_1m = None
    try:
        if price_df is not None and len(price_df) >= 21 and "close" in price_df.columns:
            close = price_df["close"]
            cur_p  = float(close.iloc[-1])
            past_p = float(close.iloc[-21])
            if past_p > 0:
                ret_1m = (cur_p - past_p) / past_p * 100
    except Exception:
        pass

    if ret_1m is not None:
        if change_pct >= 2 and ret_1m <= -10:
            # Foreign money buying into a falling stock: high-conviction contrarian bottom signal
            score = min(10.0, score + 2.0)
            signal = signal + " (buying the dip — smart money contrarian)"
        elif change_pct <= -2 and ret_1m <= -10:
            # Foreign money also exiting a falling stock: fundamental sell conviction
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (selling into decline — fundamental exit)"

    # --- Earnings revision cross: dual institutional confirmation ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if change_pct >= 2 and net_rev >= 2:
                # Foreign money buying + domestic analysts upgrading: highest-conviction buy
                score = min(10.0, score + 2.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — NB × analyst consensus"
            elif change_pct <= -2 and net_rev <= -2:
                # Foreign money exiting + analysts cutting: dual institutional exit
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dual institutional exit"

    # --- Industry momentum cross: contra-sector NB flow = highest conviction ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if change_pct >= 2:
            if excess <= -2:
                # NB buying while sector is underperforming market: stock-specific high-conviction pick
                score = min(10.0, score + 2.0)
                signal = signal + f" (contra-sector: NB buying while industry excess {excess:+.1f}%)"
            elif excess >= 5:
                # NB buying into sector momentum: trend-following, information value lower
                score = max(0.0, score - 1.0)
                signal = signal + f" (sector-following: NB buying with hot sector {excess:+.1f}%)"
        elif change_pct <= -2 and excess >= 5:
            # NB reducing while sector is hot: foreign capital exiting a popular trade
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + f" (smart exit: NB reducing in hot sector {excess:+.1f}%)"

    sell_score = round(sell_score, 1)
    return {
        "score": round(max(0.0, score), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "latest_holding": round(current / 1e6, 1),
            "change_pct": round(change_pct, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "ret_1m_pct": round(ret_1m, 2) if ret_1m is not None else None,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "signal": signal,
            "sell_score": sell_score,
        },
    }


def score_earnings_revision(
    revision_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Analyst EPS forecast revision direction (max 10).
    Upward revisions → subsequent outperformance (strong academic evidence).
      net_up >= 3    -> 10 pts
      net_up  1-3    -> linear 7-10 pts
      net_up == 0    ->  5 pts (no coverage / neutral)
      net_down       -> linear 0-4 pts

    Context cross with 52w price position:
      Upgrades (net >= 2) + low position (< 0.3)   -> analysts discovered an underpriced stock -> buy +2
      Downgrades (net <= -2) + high position (> 0.7) -> analysts finally cutting an expensive stock -> sell +2
      Upgrades (net >= 1) + high position (> 0.7)   -> price-chasing upgrades, stock already moved -> sell +1

    Trailing growth cross (requires financial_df): are upgrades grounded in real results?
      Upgrades (net >= 2) + trailing profit growth >= 20% -> buy +1.5
        (analyst optimism validated by actual results — highest-conviction upgrade)
      Upgrades (net >= 2) + trailing profit growth < 0%   -> sell +1.5
        (analysts upgrading despite declining earnings — likely relationship-driven, not signal)
    """
    if revision_df is None or revision_df.empty:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"up": 0, "down": 0, "net": 0, "signal": "no coverage", "sell_score": 0.0}}

    # Look for rating change direction columns
    rating_cols = [c for c in revision_df.columns
                   if any(k in c for k in ["评级变动", "方向", "上调", "下调", "rating"])]
    up_down_col = rating_cols[0] if rating_cols else None

    try:
        if up_down_col:
            col_str = revision_df[up_down_col].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
        else:
            # Fallback: treat all rows as coverage with unknown direction
            return {"score": 5.0, "sell_score": 0.0, "max": 10,
                    "details": {"up": 0, "down": 0, "net": 0, "signal": "no direction data", "sell_score": 0.0}}
    except Exception:
        return _neutral(10)

    net = up - down

    if net >= 3:
        score = 10.0
    elif net >= 1:
        score = 7.0 + (net - 1) / 2 * 3.0
    elif net == 0:
        score = 5.0
    elif net >= -2:
        score = 5.0 + net / 2 * 5.0
    else:
        score = 0.0

    signal = ("strong upgrade" if net >= 3 else
              "upgraded" if net > 0 else
              "neutral" if net == 0 else
              "downgraded" if net >= -2 else "strong downgrade")

    # --- Sell score: analyst downgrades ---
    if net <= -3:
        sell_score = 9.0
    elif net <= -1:
        # linear: -1 -> 5pts, -3 -> 9pts
        sell_score = 5.0 + (-net - 1) / 2 * 4.0
    elif net < 0:
        sell_score = 2.0
    else:
        sell_score = 0.0

    sell_score = round(min(9.0, sell_score), 1)

    # --- Context cross: revision direction × 52w price position ---
    position = _get_price_position(price_df)
    if position is not None:
        if net >= 2 and position < 0.3:
            # Analysts upgrading a beaten-down stock: contra-consensus discovery → high conviction
            score = min(10.0, score + 2.0)
            signal = "analyst upgrades at low price (contra-consensus discovery)"
        elif net <= -2 and position > 0.7:
            # Analysts cutting targets on a high-priced stock: confirmed top
            sell_score = min(9.0, sell_score + 2.0)
            signal = "analyst downgrades at high price (confirmed top)"
        elif net >= 1 and position > 0.7:
            # Upgrades after the stock already ran: price-chasing, late to the party
            sell_score = min(9.0, sell_score + 1.0)
            signal = "analyst upgrades at high price (may be price-chasing)"

    # --- Trailing growth cross: are upgrades grounded in actual results? ---
    trailing_growth = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    trailing_growth = float(vals.iloc[0])
                break
        if trailing_growth is not None and net >= 2:
            if trailing_growth >= 20:
                # Forward upgrades validated by actual growth: highest-conviction upgrade
                score = min(10.0, score + 1.5)
                signal = signal + f" + trailing growth {trailing_growth:.1f}% (upgrade grounded in results)"
            elif trailing_growth < 0:
                # Analysts upgrading while actual earnings are declining: hollow signal
                sell_score = min(9.0, sell_score + 1.5)
                signal = signal + f" + trailing decline {trailing_growth:.1f}% (upgrade not backed by results)"

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": round(min(9.0, sell_score), 1),
        "max": 10,
        "details": {
            "up_revisions":    up,
            "down_revisions":  down,
            "net_revisions":   net,
            "trailing_growth": round(trailing_growth, 1) if trailing_growth is not None else None,
            "position_52w":    round(position, 3) if position is not None else None,
            "signal":          signal,
            "sell_score":      round(min(9.0, sell_score), 1),
        },
    }


# ===========================================================================
# GROUP C — New behavioral / market-context factors
# ===========================================================================

def score_limit_hits(
    price_df: Optional[pd.DataFrame],
    financial_df: Optional[pd.DataFrame] = None,
    social_dict: Optional[dict] = None,
) -> dict:
    """
    Limit-up / limit-down frequency in last 20 trading days (max 10).
    A-share limit = ±10% (±9.9% threshold used to be safe).

    Crossed with 52w position:
      Net limit-ups + low position  -> momentum breakout from base (strong buy)
      Net limit-ups + high position -> overheated at top (sell warning)
      Net limit-downs + low position -> panic capitulation (potential reversal)
      Net limit-downs + high position -> distribution selling (strong sell)

    Fundamental quality cross: hot money vs genuine momentum (requires financial_df)
      Net limit-ups (>= 2) + ROE >= 12% -> earnings-backed acceleration, buy +1.5
      Net limit-ups (>= 2) + ROE < 5%   -> pure hot money/speculation, sell +2

    Social heat cross: A-share pump detection (requires social_dict)
      Net limit-ups (>= 2) + extreme heat (rank_pct <= 5%)  -> 炒作顶部三件套, sell +2
      Net limit-ups (>= 2) + low heat (rank_pct > 50%)      -> institutional-driven, buy +1.5
      Net limit-downs (>= 2) + extreme heat at lows         -> retail panic = contrarian bottom, sell -1
    """
    if price_df is None or len(price_df) < 5 or "change_pct" not in price_df.columns:
        return _neutral(10)

    try:
        chg = pd.to_numeric(price_df["change_pct"], errors="coerce").dropna().tail(20)
        up_count   = int((chg >= 9.9).sum())
        down_count = int((chg <= -9.9).sum())
        net = up_count - down_count
    except Exception:
        return _neutral(10)

    # Base score from net limit-up count
    if net >= 3:
        score = 9.0
        signal = "frequent limit-ups (strong momentum)"
    elif net >= 1:
        score = 7.0
        signal = "net limit-ups"
    elif net == 0 and up_count == 0 and down_count == 0:
        score = 5.0  # no limit events = neutral
        signal = "no limit events"
    elif net == 0:
        score = 4.0
        signal = "balanced limit events"
    elif net == -1:
        score = 3.0
        signal = "slight net limit-downs"
    else:
        score = 1.0
        signal = "frequent limit-downs"

    # Base sell score
    if net <= -3:
        sell_score = 9.0
    elif net <= -1:
        sell_score = 6.0
    elif net == 0 and down_count >= 2:
        sell_score = 4.0
    else:
        sell_score = 0.0

    # Context cross with 52w position
    position = _get_price_position(price_df)
    if position is not None:
        if net >= 2:
            if position < 0.3:
                # Limit-up breakout from base = genuine momentum
                score = min(10.0, score + 1.0)
                signal = "limit-up breakout from base (strong buy)"
            elif position > 0.7:
                # Frequent limit-ups at top = overheated retail frenzy
                sell_score = min(10.0, sell_score + 3.0)
                signal = "overheated at high position (limit-up frenzy)"
        elif net <= -2:
            if position < 0.3:
                # Limit-downs at bottom = panic selling = potential reversal
                score = min(10.0, score + 3.0)  # contrarian: panic = buy
                sell_score = max(0.0, sell_score - 2.0)
                signal = "panic selling at lows (potential reversal)"
            elif position > 0.7:
                # Limit-downs from high = institutional distribution
                sell_score = min(10.0, sell_score + 2.0)
                signal = "distribution selling from highs (strong sell)"

    # --- Fundamental quality cross: genuine momentum vs hot money ---
    if financial_df is not None and net >= 2:
        roe = _extract(financial_df, ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"])
        if roe is not None:
            if roe >= 12:
                # Limit-ups backed by solid earnings: sustainable acceleration
                score = min(10.0, score + 1.5)
                signal = signal + " + strong ROE (genuine momentum)"
            elif roe < 5:
                # Limit-ups with near-zero returns: purely speculative hot money
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " + weak ROE (hot money, no fundamentals)"

    # --- Social heat cross: A-share pump detection ---
    if social_dict is not None and "rank_pct" in social_dict:
        rank_pct = float(social_dict["rank_pct"])
        if net >= 2:
            if rank_pct <= 5:
                # Consecutive limit-ups + trending on social media: the classic A-share pump trio
                # (游资 + retail FOMO + hot-search = top signal)
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " + extreme social heat (pump pattern — 炒作顶部)"
            elif rank_pct > 50:
                # Limit-ups with no social buzz: institutions quietly driving price, more sustainable
                score = min(10.0, score + 1.5)
                signal = signal + " + low social heat (institutional-driven, sustainable)"
        elif net <= -2 and rank_pct <= 10 and position is not None and position < 0.3:
            # Panic limit-downs at lows with extreme social heat: retail capitulation = bottom
            sell_score = max(0.0, sell_score - 1.0)
            signal = signal + " + extreme heat at lows (retail panic = contrarian bottom)"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "limit_up_count_20d":   up_count,
            "limit_down_count_20d": down_count,
            "net_limit_up":         net,
            "position_52w":         round(position, 3) if position is not None else None,
            "signal":               signal,
            "sell_score":           round(min(10.0, sell_score), 1),
        },
    }


def score_price_inertia(price_df: Optional[pd.DataFrame]) -> dict:
    """
    Short-term price inertia: consecutive up/down day streak (max 10).
    Crossed with volume trend to confirm continuation vs exhaustion.

      Consecutive up days (3+) + volume expanding -> strong continuation (buy)
      Consecutive up days (3+) + volume contracting -> unsustainable (sell warning)
      Consecutive down days (3+) + volume expanding -> accelerating sell (sell)
      Consecutive down days (3+) + volume contracting -> exhaustion (potential reversal)
    """
    if price_df is None or len(price_df) < 5 or "close" not in price_df.columns:
        return _neutral(10)

    try:
        chg = price_df["close"].pct_change().tail(11)
        values = chg.dropna().values

        consec_up = consec_down = 0
        for c in reversed(values):
            if c > 0.001:
                if consec_down > 0:
                    break
                consec_up += 1
            elif c < -0.001:
                if consec_up > 0:
                    break
                consec_down += 1
            else:
                break
    except Exception:
        return _neutral(10)

    # Volume trend: recent 5d vs prior 10d
    vol_expanding = None
    try:
        if "volume" in price_df.columns and len(price_df) >= 15:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 15:
                v5  = float(vol.tail(5).mean())
                v10 = float(vol.tail(15).head(10).mean())
                if v10 > 0:
                    vr = v5 / v10
                    vol_expanding = vr > 1.15
    except Exception:
        pass

    # Score based on consecutive days
    if consec_up >= 4:
        score = 8.0; signal = "strong up streak (%dd)" % consec_up
    elif consec_up >= 3:
        score = 7.0; signal = "up streak (%dd)" % consec_up
    elif consec_up >= 2:
        score = 6.0; signal = "2-day up"
    elif consec_down >= 4:
        score = 1.0; signal = "strong down streak (%dd)" % consec_down
    elif consec_down >= 3:
        score = 2.0; signal = "down streak (%dd)" % consec_down
    elif consec_down >= 2:
        score = 3.0; signal = "2-day down"
    else:
        score = 5.0; signal = "mixed / flat"

    sell_score = 0.0
    if consec_down >= 4:
        sell_score = 7.0
    elif consec_down >= 3:
        sell_score = 5.0
    elif consec_down >= 2:
        sell_score = 3.0

    # Volume cross
    if vol_expanding is not None:
        if consec_up >= 3 and vol_expanding:
            score = min(10.0, score + 2.0)
            signal = signal + " + volume expanding (confirmed)"
        elif consec_up >= 3 and not vol_expanding:
            sell_score = min(10.0, sell_score + 3.0)
            signal = signal + " + volume contracting (unsustainable)"
        elif consec_down >= 3 and vol_expanding:
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " + volume expanding (accelerating down)"
        elif consec_down >= 3 and not vol_expanding:
            score = min(10.0, score + 2.0)  # exhaustion bounce potential
            sell_score = max(0.0, sell_score - 2.0)
            signal = signal + " + volume contracting (selling exhausted)"

    # --- Annualized volatility cross: momentum quality differs by vol regime ---
    ann_vol = None
    try:
        if len(price_df) >= 20 and "close" in price_df.columns:
            daily_ret = price_df["close"].tail(60).pct_change().dropna()
            if len(daily_ret) >= 10:
                ann_vol = float(daily_ret.std() * np.sqrt(252) * 100)
    except Exception:
        pass

    if ann_vol is not None:
        if consec_up >= 3 and ann_vol <= 25:
            # Low-vol upstreak: institutional-driven, smooth and persistent
            score = min(10.0, score + 2.0)
            signal = signal + f" + low vol {ann_vol:.0f}% (institutional momentum, persistent)"
        elif consec_up >= 3 and ann_vol > 50:
            # High-vol upstreak: choppy, retail-driven, mean-reversion risk
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + f" + high vol {ann_vol:.0f}% (volatile, mean-reversion risk)"
        elif consec_down >= 3 and ann_vol <= 25:
            # Low-vol downstreak: quiet structural selling, no panic = stubborn sellers
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + f" + low vol {ann_vol:.0f}% (quiet structural decline)"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "consecutive_up_days":   consec_up,
            "consecutive_down_days": consec_down,
            "vol_expanding":         vol_expanding,
            "annualized_vol_pct":    round(ann_vol, 1) if ann_vol is not None else None,
            "signal":                signal,
            "sell_score":            round(min(10.0, sell_score), 1),
        },
    }


def score_social_heat(
    social_dict: Optional[dict],
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    East Money hot stock ranking as a sentiment proxy (max 10).
    CONTRARIAN: very high ranking (extreme retail attention) = sell signal.
    Moderate attention = positive signal (institutional interest growing).

      rank_pct top 1%   -> sell 8 (extreme retail frenzy)
      rank_pct top 5%   -> sell 5, buy 4
      rank_pct top 20%  -> buy 7 (healthy attention)
      rank_pct > 50%    -> buy 3 (low attention)
      no data           -> neutral

    Context cross with 52w price position:
      Extreme heat (top 5%) + high position (> 0.7) -> hype at the peak, amplify contrarian sell (+2)
      Extreme heat (top 5%) + low position (< 0.3)  -> retail FOMO on a beaten-down stock;
        A-share short squeezes from lows are real → soften contrarian sell (-2)

    Fundamental quality cross (requires financial_df): genuine discovery vs speculative frenzy
      High heat (top 5%) + ROE >= 15% -> institutional discovery of quality, soften contrarian sell (-1.5), buy +1
      High heat (top 5%) + ROE <  5%  -> speculative retail frenzy on weak business, amplify sell (+2)
      Moderate heat (top 20%) + ROE >= 15% -> quality company gaining deserved attention, buy +1
    """
    if social_dict is None or "rank_pct" not in social_dict:
        return _neutral(10)

    rank_pct = float(social_dict["rank_pct"])  # lower = more popular (top ranked)

    # Buy: moderate attention is good, extreme attention is bad
    if rank_pct <= 1:
        # Top 1%: extreme retail heat = contrarian sell
        score = 2.0
        sell_score = 8.0
        signal = "extreme retail heat (contrarian sell)"
    elif rank_pct <= 5:
        score = 4.0
        sell_score = 5.0
        signal = "very high attention"
    elif rank_pct <= 20:
        score = 7.0
        sell_score = 1.0
        signal = "healthy attention"
    elif rank_pct <= 50:
        score = 5.0
        sell_score = 0.0
        signal = "moderate attention"
    else:
        score = 3.0
        sell_score = 0.0
        signal = "low attention"

    # --- Context cross: retail heat × 52w price position ---
    position = _get_price_position(price_df)
    if position is not None and rank_pct <= 5:
        # Only applies when retail heat is high (top 5%)
        if position > 0.7:
            # Extreme attention at price highs: classic "last buyer" scenario — amplify contrarian
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " (at highs — peak frenzy, strong contrarian sell)"
        elif position < 0.3:
            # Extreme attention on a beaten-down stock: retail FOMO from lows
            # A-share low-position squeezes are real; soften the contrarian sell
            sell_score = max(0.0, sell_score - 2.0)
            signal = signal + " (at lows — retail FOMO from low base, soften contrarian)"

    # --- Fundamental quality cross: genuine interest vs speculative frenzy ---
    roe = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe = float(vals.iloc[0])
                break

    if roe is not None:
        if rank_pct <= 5:
            # High heat: quality makes the difference between discovery and frenzy
            if roe >= 15:
                # Institutions accumulating a quality company: contrarian signal overstated
                sell_score = max(0.0, sell_score - 1.5)
                score = min(10.0, score + 1.0)
                signal = signal + f" + high ROE {roe:.0f}% (institutional discovery, not frenzy)"
            elif roe < 5:
                # Pure speculative retail pile-in on a weak business: classic pump pattern
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + f" + low ROE {roe:.0f}% (speculative frenzy, amplify contrarian)"
        elif rank_pct <= 20 and roe >= 15:
            # Moderate heat + quality: deserved attention growing, genuine buy signal
            score = min(10.0, score + 1.0)
            signal = signal + f" + high ROE {roe:.0f}% (quality company gaining attention)"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "rank":      social_dict.get("rank"),
            "rank_pct":  round(rank_pct, 1),
            "position_52w": round(position, 3) if position is not None else None,
            "roe_pct":   round(roe, 1) if roe is not None else None,
            "signal":    signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_concept_momentum(
    concept_data: Optional[list],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    financial_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Concept/theme board momentum score (max 10).

    Uses the stock's top-5 concept boards by 1-month return magnitude.
    The best concept (highest return) drives the buy signal; the worst
    (most negative return) drives the sell signal.

    Cross-rule: compare the stock's own 1m return against its hottest concept.
      - Stock lags concept by ≥15%  → buy +2 (catch-up candidate)
      - Stock leads concept by ≥20% → sell +2 (dragon-head fade risk)

    Market regime cross: concept rallies behave differently across market environments.
      Hot concept (best_ret ≥ +10%) + bear market (regime ≤ 3) → buy -2, sell +1.5
        (熊市题材炒作持续性极差，快进快出的游资行为为主)
      Hot concept + bull market (regime ≥ 7) → buy +1
        (牛市板块共振具有延续性，跟进性价比更高)

    ROE quality cross: distinguishes fundamentals-backed thematic rally from pure speculation.
      Hot concept (best_ret ≥ +8%) + ROE >= 15% → buy +1.5 (quality company in hot sector)
      Hot concept + ROE < 5%                    → sell +2  (speculative play, no earnings support)
    """
    if not concept_data:
        return _neutral(10)

    best  = max(concept_data, key=lambda x: x["ret_1m"])
    worst = min(concept_data, key=lambda x: x["ret_1m"])
    best_ret  = best["ret_1m"]
    worst_ret = worst["ret_1m"]

    # Buy signal: driven by best concept performance
    if best_ret >= 15:
        score = 9.0
        signal = f"hot concept 【{best['name']}】 +{best_ret:.1f}%"
    elif best_ret >= 8:
        score = 7.0
        signal = f"strong concept 【{best['name']}】 +{best_ret:.1f}%"
    elif best_ret >= 3:
        score = 5.5
        signal = f"rising concept 【{best['name']}】 +{best_ret:.1f}%"
    elif best_ret >= 0:
        score = 4.0
        signal = f"flat concept 【{best['name']}】 {best_ret:.1f}%"
    else:
        score = 2.0
        signal = f"all concepts falling, best: 【{best['name']}】 {best_ret:.1f}%"

    # Sell signal: driven by worst concept performance
    if worst_ret <= -15:
        sell_score = 8.0
        sell_signal = f"concept collapse 【{worst['name']}】 {worst_ret:.1f}%"
    elif worst_ret <= -8:
        sell_score = 5.0
        sell_signal = f"concept weakness 【{worst['name']}】 {worst_ret:.1f}%"
    elif worst_ret <= -3:
        sell_score = 3.0
        sell_signal = f"concept softening 【{worst['name']}】 {worst_ret:.1f}%"
    else:
        sell_score = 1.0
        sell_signal = "no significant concept sell pressure"

    # Cross-rule: stock return vs. best concept return
    stock_ret_1m = None
    if price_df is not None and "close" in price_df.columns:
        close = price_df["close"].dropna()
        if len(close) >= 20:
            stock_ret_1m = float((close.iloc[-1] / close.iloc[-20] - 1) * 100)

    if stock_ret_1m is not None and best_ret >= 8:
        lag = best_ret - stock_ret_1m
        lead = stock_ret_1m - best_ret
        if lag >= 15:
            # Stock massively lags its hot concept — catch-up opportunity
            score = min(10.0, score + 2.0)
            signal = signal + f" (stock lags concept by {lag:.1f}% — catch-up potential)"
        elif lead >= 20:
            # Stock already dramatically outran its concept — dragon-head fade
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + f" (stock leads concept by {lead:.1f}% — dragon-head fade risk)"

    # --- Market regime cross: concept rally sustainability ---
    if market_regime_score is not None and best_ret >= 10:
        if market_regime_score <= 3:
            # Bear market concept pump: game played by short-term traders, 3-5 day window max
            score      = max(0.0, score - 2.0)
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — concept pump likely short-lived)"
        elif market_regime_score >= 7:
            # Bull market concept rally: institutional participation, more follow-through
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — concept rally more sustainable)"

    # --- ROE quality cross: is this a real thematic rally or pure speculation? ---
    roe_concept = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe_concept = float(vals.iloc[0])
                break

    if roe_concept is not None and best_ret >= 8:
        if roe_concept >= 15:
            # Hot sector + quality business: theme rally backed by real earnings power
            score = min(10.0, score + 1.5)
            signal = signal + f" + ROE {roe_concept:.0f}% (fundamentals-backed theme — sustainable)"
        elif roe_concept < 5:
            # Hot sector + near-zero earnings: pure speculative play with no fundamental anchor
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + f" + ROE {roe_concept:.0f}% (speculative theme — no earnings support)"

    return {
        "score":      round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "best_concept":       best["name"],
            "best_ret_1m":        round(best_ret, 2),
            "worst_concept":      worst["name"],
            "worst_ret_1m":       round(worst_ret, 2),
            "concepts_count":     len(concept_data),
            "stock_ret_1m":       round(stock_ret_1m, 2) if stock_ret_1m is not None else None,
            "market_regime_score": market_regime_score,
            "roe_pct":            round(roe_concept, 1) if roe_concept is not None else None,
            "signal":             signal,
            "sell_signal":        sell_signal,
            "sell_score":         round(sell_score, 1),
        },
    }


def score_market_regime(market_df: Optional[pd.DataFrame]) -> dict:
    """
    CSI 300 market regime score (max 10).
    Measures whether the broad market (沪深300) is in bull or bear mode.
    Uses MA5/MA20/MA60 alignment of the index.

      MA5 > MA20 > MA60  -> bull market (buy 9, sell 0)
      MA5 > MA20          -> recovering (buy 7, sell 1)
      price < MA20        -> caution (buy 4, sell 5)
      price < MA60        -> bear market (buy 1, sell 9)

    This factor captures systematic risk: even the best stocks struggle in a
    prolonged bear market.
    """
    if market_df is None or len(market_df) < 60 or "close" not in market_df.columns:
        return _neutral(10)

    try:
        close = market_df["close"].dropna()
        if len(close) < 60:
            return _neutral(10)
        current = float(close.iloc[-1])
        ma5  = float(close.tail(5).mean())
        ma20 = float(close.tail(20).mean())
        ma60 = float(close.tail(60).mean())
    except Exception:
        return _neutral(10)

    if ma5 > ma20 > ma60:
        score = 9.0
        sell_score = 0.0
        signal = "bull market (MA5>MA20>MA60)"
    elif ma5 > ma20 and ma20 > ma60 * 0.97:
        score = 8.0
        sell_score = 1.0
        signal = "bull market (recovering)"
    elif current > ma20:
        score = 6.0
        sell_score = 2.0
        signal = "above MA20 (neutral-positive)"
    elif current > ma60:
        score = 4.0
        sell_score = 5.0
        signal = "below MA20, above MA60 (caution)"
    elif current > ma60 * 0.95:
        score = 2.0
        sell_score = 7.0
        signal = "near MA60 support (bear risk)"
    else:
        score = 1.0
        sell_score = 9.0
        signal = "bear market (price below MA60)"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "index_close": round(current, 2),
            "ma5":         round(ma5, 2),
            "ma20":        round(ma20, 2),
            "ma60":        round(ma60, 2),
            "signal":      signal,
            "sell_score":  round(sell_score, 1),
        },
    }
