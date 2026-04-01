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
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    best_concept_ret: Optional[float] = None,
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

    Earnings revision cross: real reversal vs dead-cat bounce (requires revision_df)
      Oversold (ret_1m <= -10%) + net upgrades >= 2   -> fundamentals improving, genuine reversal -> buy +2
      Oversold                  + net downgrades <= -2 -> fundamentals still deteriorating         -> buy -1.5, sell +1.5
      Overbought (ret >= +10%)  + net upgrades >= 2   -> rally justified, soften overbought sell  -> sell -1.5
      Overbought                + net downgrades <= -2 -> rally unjustified, amplify sell         -> sell +1.5

    Market regime cross: reversal signal reliability (requires market_regime_score)
      Oversold + bear market (regime <= 3) -> falling knife risk, buy -1.5, sell +1
      Oversold + bull market (regime >= 7) -> pullback more buyable, buy +1

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      Oversold + industry outperforming (excess >= +3%) -> sector strength backstops reversal, buy +2
      Oversold + industry underperforming (excess <= -3%) -> sector still falling, soften buy, buy -1.5
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

    # --- Market regime cross: reversal reliability differs in bull vs bear ---
    if market_regime_score is not None and ret_1m <= -10:
        if market_regime_score <= 3:
            # Bear market: falling stocks tend to keep falling — avoid catching falling knives
            score = max(0.0, score - 1.5)
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bear market — reversal less reliable, falling knife risk)"
        elif market_regime_score >= 7:
            # Bull market: pullbacks are buyable; oversold bounces are sharper and more reliable
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — pullback more buyable)"

    # --- Industry excess cross: sector momentum changes reversal conviction ---
    if industry_ret_1m is not None and market_ret_1m is not None and ret_1m <= -10:
        excess = industry_ret_1m - market_ret_1m
        if excess >= 3:
            # Oversold stock in a strong sector: sector bid provides floor, reversal more likely
            score = min(10.0, score + 2.0)
            signal = signal + f" (industry outperforming {excess:+.1f}% — sector backstops reversal)"
        elif excess <= -3:
            # Oversold stock + weak sector: headwind from sector, don't rush to buy
            score = max(0.0, score - 1.5)
            signal = signal + f" (industry weak {excess:+.1f}% — sector drag, soften reversal)"

    # --- Concept cross: hot concept provides catalyst for oversold reversal ---
    if best_concept_ret is not None and ret_1m <= -10:
        if best_concept_ret >= 8:
            # Oversold stock + hot concept board: sector rotation can ignite a bounce
            score = min(10.0, score + 2.0)
            signal = signal + f" (hot concept {best_concept_ret:+.1f}% — rotation catalyst boosts reversal)"
        elif best_concept_ret <= 0:
            # Oversold stock + no concept heat: no catalyst to drive recovery
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" (cold concept {best_concept_ret:+.1f}% — no theme catalyst)"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "return_1m_pct": round(ret_1m, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "vol_ratio_5d_10d": round(vol_ratio, 2) if vol_ratio is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_accruals(
    financial_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    price_df: Optional[pd.DataFrame] = None,
) -> dict:
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

    Market regime cross (requires market_regime_score):
      Low accruals (<= -5%) + bear market (regime <= 3) -> 风险偏好下降，资金流向现金流扎实的公司 -> buy +1
      High accruals (>= 5%) + bear market               -> 盈利质量差在熊市资金出逃时首先被抛弃 -> sell +1

    52w position cross (requires price_df):
      Low accruals (<= -5%) + low position (< 0.3) -> buy +1.5 (现金流优质+低位=价值洼地被低估)
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

    # --- Market regime cross: earnings quality carries defensive premium in bear markets ---
    if market_regime_score is not None:
        if accruals_pct <= -5 and market_regime_score <= 3:
            # Bear market: risk-off flight to quality — cash-backed earnings attract defensive capital
            score = min(10.0, score + 1.0)
            signal = signal + " (bear market — 现金盈利防御溢价)"
        elif accruals_pct >= 5 and market_regime_score <= 3:
            # Bear market: poor earnings quality exposed first as capital flees
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bear market — 低质量盈利在熊市首先被抛弃)"

    # --- 52w position cross: quality-at-value is the optimal fundamental setup ---
    position_signal = None
    if price_df is not None and accruals_pct <= -5:
        pos = _get_price_position(price_df)
        if pos is not None and pos < 0.3:
            # Cash-backed quality earnings at a low price: value investors' ideal setup
            score = min(10.0, score + 1.5)
            position_signal = f"低应计+低位({pos:.2f}) — 现金流优质+价值洼地，被市场低估"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "accruals_pct": round(accruals_pct, 2),
            "profit_growth_pct": round(profit_growth, 1) if profit_growth is not None else None,
            "net_income": round(net_income / 1e8, 2) if net_income else None,
            "op_cashflow": round(op_cf / 1e8, 2) if op_cf else None,
            "market_regime_score": market_regime_score,
            "position_signal": position_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_asset_growth(
    financial_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
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

    Market regime cross (requires market_regime_score):
      Aggressive expansion (>= 20%) + bear market (regime <= 3)
        -> 熊市扩张=融资成本上升+需求萎缩，双重压力 -> sell +1.5
      Disciplined growth (<= 5%) + bear market
        -> 熊市保守扩张=管理层稳健，防御性加分 -> buy +0.5

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      Disciplined growth (<= 5%) + hot industry (excess >= +3%) -> buy +1 (保守扩张+行业顺风=最优质的成长模式)
      Aggressive growth (>= 20%) + weak industry (excess <= -3%) -> sell +1.5 (逆行业大肆扩张=管理层判断失误)
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

    # --- Market regime cross: expansion risk is regime-dependent ---
    if market_regime_score is not None:
        if growth >= 20 and market_regime_score <= 3:
            # Aggressive expansion in bear market: financing costs rise, demand contracts simultaneously
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市扩张融资成本上升+需求萎缩)"
        elif growth <= 5 and market_regime_score <= 3:
            # Disciplined conservative growth in bear market: management is prudent, mildly defensive
            score = min(10.0, score + 0.5)
            signal = signal + " (bear market — 保守扩张体现管理层稳健)"

    # --- Industry excess return cross: expansion quality validated by industry environment ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if growth <= 5 and excess >= 3.0:
            # Disciplined expansion + hot sector: the best-quality growth profile
            score = min(10.0, score + 1.0)
            industry_signal = f"保守扩张+行业顺风(超额{excess:.1f}%) — 最优质成长模式"
        elif growth >= 20 and excess <= -3.0:
            # Aggressive expansion against falling sector: management misjudged the cycle
            sell_score = min(10.0, sell_score + 1.5)
            industry_signal = f"激进扩张+行业弱(超额{excess:.1f}%) — 逆行业大肆扩张，判断失误"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "asset_growth_pct": round(growth, 1),
            "roe_pct": round(roe, 1) if roe is not None else None,
            "market_regime_score": market_regime_score,
            "industry_signal": industry_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_piotroski(
    financial_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    pe_pct: Optional[float] = None,
    pb_pct: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Market regime cross (requires market_regime_score):
      F-score >= 7 + bear market (regime <= 3) -> 防御性基本面溢价，机构避险首选 -> buy +1.5
      F-score <= 3 + bear market               -> 弱基本面在熊市压力下更快暴露 -> sell +1

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      F-score >= 7 + industry outperforming (excess >= +3%) -> buy +1.5 (基本面强+行业顺风=双重加持)
      F-score <= 2 + industry weak (excess <= -3%) -> sell +1.5 (基本面差+行业逆风=双杀，最确定的卖出)
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

    # --- Market regime cross: fundamental quality premium is regime-dependent ---
    if market_regime_score is not None:
        if f_score >= 7 and market_regime_score <= 3:
            # Bear market: institutional capital flees to quality; high F-score becomes defensive premium
            score = min(9.0, score + 1.5)
            fscore_signal = fscore_signal + " (bear market — 防御性基本面溢价，机构避险首选)"
        elif f_score <= 3 and market_regime_score <= 3:
            # Bear market exposes weak fundamentals faster (financing tighter, margins squeezed)
            sell_score = min(9.0, sell_score + 1.0)
            fscore_signal = fscore_signal + " (bear market — 弱基本面在熊市更快暴露)"

    # --- Industry excess return cross: sector tailwind/headwind amplifies fundamental signal ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if f_score >= 7 and excess >= 3.0:
            # Strong fundamentals + strong sector: dual confirmation, highest conviction buy
            score = min(9.0, score + 1.5)
            industry_signal = f"F-score强+行业强(超额{excess:.1f}%) — 基本面+行业双重加持"
        elif f_score <= 2 and excess <= -3.0:
            # Weak fundamentals + weak sector: double negative, highest conviction sell
            sell_score = min(9.0, sell_score + 1.5)
            industry_signal = f"F-score弱+行业弱(超额{excess:.1f}%) — 基本面+行业双杀"

    # --- Earnings revision cross: forward-looking analyst view on financial health ---
    revision_signal_pf = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_pf   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_pf = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_pf  = up_pf - down_pf
                if f_score >= 7 and net_pf >= 2:
                    # Strong historical financials + analyst upgrades: quality confirmed forward-looking
                    score = min(9.0, score + 1.5)
                    revision_signal_pf = f"F-score强+分析师上调({net_pf:+d}家) — 历史财务健康+未来向好，三重基本面确认"
                elif f_score <= 3 and net_pf <= -2:
                    # Weak financials + analyst downgrades: deterioration confirmed by two independent sources
                    sell_score = min(9.0, sell_score + 1.5)
                    revision_signal_pf = f"F-score弱+分析师下调({net_pf:+d}家) — 财务恶化+分析师确认，双重利空"
        except Exception:
            pass

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
            "market_regime_score": market_regime_score,
            "industry_signal": industry_signal,
            "revision_signal": revision_signal_pf,
            "signal": fscore_signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_short_interest(
    margin_df: Optional[pd.DataFrame],
    circulating_cap: float = 0,
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
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

    Market regime cross (requires market_regime_score):
      High short (>= 3%) + bull market (regime >= 7) -> squeeze risk elevated in rising market, buy +1.5
      High short (>= 3%) + bear market (regime <= 3) -> shorts likely right in downtrend, sell +1

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      High short (>= 3%) + industry weak (excess <= -3%) -> sell +1 (空头被行业下行趋势确认)
      High short (>= 3%) + industry strong (excess >= +3%) -> buy +1 (逆势做空=可能的逼空，行业没有配合空头)
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

    # --- Market regime cross: short squeeze risk is regime-dependent ---
    if market_regime_score is not None and ratio >= 3.0:
        if market_regime_score >= 7:
            # Bull market + heavy short: rising tide can trigger a squeeze, stocks become more buyable
            score = min(10.0, score + 1.5)
            signal = signal + " (bull market — squeeze risk elevated, 逼空风险高)"
        elif market_regime_score <= 3:
            # Bear market + heavy short: shorts are directionally correct, conviction sell
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bear market — shorts likely right, 熊市空头有优势)"

    # --- Industry excess return cross: sector environment confirms or undermines short thesis ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None and ratio >= 3.0:
        excess = industry_ret_1m - market_ret_1m
        if excess <= -3.0:
            # Sector falling + heavy short: shorts are aligned with sector direction
            sell_score = min(10.0, sell_score + 1.0)
            industry_signal = f"高融券+行业弱(超额{excess:.1f}%) — 空头被行业趋势确认"
        elif excess >= 3.0:
            # Sector strong + heavy short: shorts are fighting the sector, squeeze risk elevated
            score = min(10.0, score + 1.0)
            industry_signal = f"高融券+行业强(超额{excess:.1f}%) — 逆势做空，逼空风险高"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "short_balance_billion": round(short_balance / 1e8, 2),
            "ratio_pct": round(ratio, 3),
            "position_52w": round(position, 3) if position is not None else None,
            "net_revisions": net_revisions,
            "market_regime_score": market_regime_score,
            "industry_signal": industry_signal,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_rsi_signal(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
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

    Market regime cross (requires market_regime_score):
      RSI <= 30 + bull market (regime >= 7) -> 牛市超卖是绝佳买点，反弹可信 -> buy +2
      RSI <= 30 + bear market (regime <= 3) -> 熊市超卖可以继续跌，勿接飞刀 -> buy -2, sell +1
      RSI >= 70 + bull market              -> 强趋势中RSI可长期高位运行，减弱卖出 -> sell -1.5
      RSI >= 70 + bear market              -> 熊市中的反弹更脆弱，死猫弹 -> sell +1.5

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      RSI <= 30 + industry outperforming (excess >= +3%) -> sector bid lifts oversold, buy +1.5
      RSI <= 30 + industry weak (excess <= -3%)          -> sector drags recovery, soften buy -1
      RSI >= 70 + industry hot (excess >= +5%)           -> overbought in hot sector, slight sell +0.5
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

    # --- Market regime cross: RSI signal reliability is fundamentally different in bull vs bear ---
    if market_regime_score is not None:
        if rsi <= 30:
            if market_regime_score >= 7:
                # Bull market oversold: pullback fully absorbed, high-probability reversal entry
                score = min(10.0, score + 2.0)
                signal = signal + " (bull market — 超卖是绝佳买点)"
            elif market_regime_score <= 3:
                # Bear market oversold: can keep falling for weeks; classic falling-knife trap
                score = max(0.0, score - 2.0)
                sell_score = min(10.0, sell_score + 1.0)
                signal = signal + " (bear market — 熊市超卖可继续跌，勿接飞刀)"
        elif rsi >= 70:
            if market_regime_score >= 7:
                # Bull market overbought: RSI can stay elevated for extended periods in strong trends
                sell_score = max(0.0, sell_score - 1.5)
                signal = signal + " (bull market — 强趋势中RSI可长期高位运行)"
            elif market_regime_score <= 3:
                # Bear market overbought: bounces are short-lived and typically sell opportunities
                sell_score = min(10.0, sell_score + 1.5)
                signal = signal + " (bear market — 熊市超买更脆弱，死猫弹)"

    # --- Industry excess cross: sector momentum changes RSI reversal conviction ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if rsi <= 30:
            if excess >= 3:
                # Oversold stock + strong sector: sector provides support for the bounce
                score = min(10.0, score + 1.5)
                signal = signal + f" (industry outperforming {excess:+.1f}% — 行业强撑超卖反弹)"
            elif excess <= -3:
                # Oversold stock + weak sector: no sector floor, RSI reversal less reliable
                score = max(0.0, score - 1.0)
                signal = signal + f" (industry weak {excess:+.1f}% — 行业弱，超卖反弹打折)"
        elif rsi >= 70 and excess >= 5:
            # Overbought + very hot sector: momentum may continue slightly longer
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" (industry very hot {excess:+.1f}% — 行业热，超买略强)"

    # --- Earnings revision cross: fundamental confirmation of RSI extremes ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if rsi <= 30:
                if net_rev >= 2:
                    # Oversold RSI + analyst upgrades: technical + fundamental double bottom
                    score = min(10.0, score + 2.0)
                    signal = signal + f" + upgrades (net {net_rev:+d}) — 技术超卖+基本面底部双确认"
                elif net_rev <= -2:
                    # Oversold RSI but analysts still cutting: true falling knife
                    score = max(0.0, score - 1.5)
                    sell_score = min(10.0, sell_score + 1.0)
                    signal = signal + f" + downgrades (net {net_rev:+d}) — 超卖但基本面恶化=真飞刀"
            elif rsi >= 70:
                if net_rev <= -2:
                    # Overbought RSI + analysts cutting: technical + fundamental double top
                    sell_score = min(10.0, sell_score + 1.5)
                    signal = signal + f" + downgrades (net {net_rev:+d}) — 超买+基本面恶化双顶确认"
                elif net_rev >= 2:
                    # Overbought RSI but analysts upgrading: fundamentals support rally
                    sell_score = max(0.0, sell_score - 1.0)
                    signal = signal + f" + upgrades (net {net_rev:+d}) — 超买但基本面支撑，可能持续"

    # --- 52w price position cross: RSI extremes at price extremes are highest-conviction signals ---
    position_rsi = _get_price_position(price_df)
    if position_rsi is not None:
        if rsi <= 30:
            if position_rsi < 0.3:
                # Oversold RSI at 52w low: genuine panic bottom, maximum buy conviction
                score = min(10.0, score + 2.0)
                signal = signal + f" (52w low {position_rsi:.2f} — 真正的恐慌底部，超卖最可信)"
            elif position_rsi > 0.7:
                # Oversold RSI but fell from highs: structural decline, not a simple mean-reversion
                score = max(0.0, score - 1.5)
                sell_score = min(10.0, sell_score + 1.0)
                signal = signal + f" (52w high {position_rsi:.2f} — 从高位跌下来的超卖，结构性下行)"
        elif rsi >= 70:
            if position_rsi > 0.7:
                # Overbought RSI at 52w high: confirmed distribution top
                sell_score = min(10.0, sell_score + 1.5)
                signal = signal + f" (52w high {position_rsi:.2f} — 技术顶+价格顶双重确认)"
            elif position_rsi < 0.3:
                # Overbought RSI at 52w low: short-covering bounce, not a real trend, soften sell
                sell_score = max(0.0, sell_score - 1.0)
                signal = signal + f" (52w low {position_rsi:.2f} — 低位反弹超买，非真正顶部)"

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "rsi": round(rsi, 1),
            "ma_bull": ma_bull,
            "vol_ratio_5d_10d": round(vol_ratio, 2) if vol_ratio is not None else None,
            "position_52w": round(position_rsi, 3) if position_rsi is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_macd_signal(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    best_concept_ret: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
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

    Market regime cross (requires market_regime_score):
      Bullish MACD + bull market (regime >= 7) -> 趋势延续概率高 -> buy +1
      Bullish MACD + bear market (regime <= 3) -> 熊市假突破概率高 -> buy -1, sell +1
      Bearish MACD + bear market               -> 系统性下行确认 -> sell +1

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      Bullish MACD + industry outperforming (excess >= +3%) -> sector tailwind confirms signal, buy +1
      Bullish MACD + industry weak (excess <= -3%)          -> fighting sector headwind, soften buy -0.5
      Bearish MACD + industry weak                          -> double weakness confirmed, sell +0.5

    Concept cross (requires best_concept_ret):
      Bullish MACD + hot concept (>= +8%) -> 概念资金推动MACD改善，有持续性 -> buy +1.5
      Bearish MACD + hot concept (>= +8%) -> 题材热度可能带来反转，软化卖出 -> sell -1
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

    # --- Market regime cross: MACD reliability differs sharply in bull vs bear ---
    if market_regime_score is not None:
        bullish_macd = curr > 0 and improving
        bearish_macd = curr <= 0 and not improving
        if bullish_macd:
            if market_regime_score >= 7:
                # Bull market: MACD golden cross has high follow-through, trend continuation likely
                score = min(10.0, score + 1.0)
                signal = signal + " (bull market — 趋势延续概率高)"
            elif market_regime_score <= 3:
                # Bear market: MACD bullish crosses frequently fail as dead-cat bounces
                score = max(0.0, score - 1.0)
                sell_score = min(10.0, sell_score + 1.0)
                signal = signal + " (bear market — 熊市假突破风险高)"
        elif bearish_macd and market_regime_score <= 3:
            # Bearish MACD confirmed by broad bear market: systemic decline, amplify sell
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bear market — 系统性下行确认)"

    # --- Industry excess cross: sector momentum shapes MACD signal reliability ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        bullish_macd = curr > 0 and improving
        bearish_macd = curr <= 0 and not improving
        if bullish_macd:
            if excess >= 3:
                # Bullish MACD + outperforming sector: sector momentum confirms the break
                score = min(10.0, score + 1.0)
                signal = signal + f" (industry outperforming {excess:+.1f}% — 行业顺风增强MACD信号)"
            elif excess <= -3:
                # Bullish MACD but sector falling: isolated stock move, less reliable
                score = max(0.0, score - 0.5)
                signal = signal + f" (industry weak {excess:+.1f}% — 行业逆风，MACD金叉打折)"
        elif bearish_macd and excess <= -3:
            # Bearish MACD + weak sector: double confirmation of downside
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" (industry weak {excess:+.1f}% — 行业弱叠加MACD死叉)"

    # --- Concept cross: theme heat as catalyst for MACD signal ---
    if best_concept_ret is not None:
        bullish_macd = curr > 0 and improving
        bearish_macd = curr <= 0 and not improving
        if bullish_macd and best_concept_ret >= 8.0:
            # Bullish MACD + hot concept: concept money is likely driving the improvement, high persistence
            score = min(10.0, score + 1.5)
            signal = signal + f" (hot concept {best_concept_ret:+.1f}% — 概念资金推动，持续性强)"
        elif bearish_macd and best_concept_ret >= 8.0:
            # Bearish MACD but hot concept in play: theme rotation may provide a catalyst to reverse
            sell_score = max(0.0, sell_score - 1.0)
            signal = signal + f" (hot concept {best_concept_ret:+.1f}% — 题材热度可能带来反转，软化卖出)"

    # --- Earnings revision cross: fundamental confirmation of technical MACD signal ---
    revision_signal_macd = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_m   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_m = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_m  = up_m - down_m
                bullish_macd_r = curr > 0 and improving
                bearish_macd_r = curr <= 0 and not improving
                if bullish_macd_r and net_m >= 2:
                    # Bullish MACD + analyst upgrades: technical + fundamental double confirmation
                    score = min(10.0, score + 1.5)
                    revision_signal_macd = f"MACD金叉+分析师上调({net_m:+d}家) — 技术基本面双重确认，信号可靠性高"
                elif bearish_macd_r and net_m <= -2:
                    # Bearish MACD + analyst downgrades: double negative confirmation
                    sell_score = min(10.0, sell_score + 1.0)
                    revision_signal_macd = f"MACD死叉+分析师下调({net_m:+d}家) — 双重利空确认"
        except Exception:
            pass

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
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "best_concept_ret": round(best_concept_ret, 1) if best_concept_ret is not None else None,
            "revision_signal": revision_signal_macd,
            "signal": signal,
            "sell_score": round(min(10.0, sell_score), 1),
        },
    }


def score_turnover_percentile(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
) -> dict:
    """
    Turnover rate vs 90-day rolling average (max 10).
    Rewards moderate elevated turnover (accumulation zone).
      ratio 1.5–3.0x -> 8-10 pts (sweet spot)
      ratio 1.0–1.5x ->  5-8 pts
      ratio < 0.8x   ->  2 pts (cold)
      ratio >= 4.0x  ->  5 pts (climax caution)

    Market regime cross (requires market_regime_score):
      Bull market (regime >= 7) + high turnover + price up -> broad participation confirmed -> buy +1
      Bear market (regime <= 3) + high turnover + price down -> panic / distribution amplified -> sell +1.5

    52w position cross (uses price_df):
      High turnover (>= 1.5x) + low position (< 0.3) + price up -> buy +1.5 (低位放量=底部确认承接)
      High turnover (>= 1.5x) + high position (> 0.7) + price down -> sell +1.5 (高位放量下跌=顶部分发确认)
      Low turnover (< 0.8x) + high position (> 0.7) + price up -> sell +1 (高位缩量上涨=上涨乏力)
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

    # --- Market regime cross: high turnover meaning depends on market environment ---
    if market_regime_score is not None:
        if market_regime_score >= 7 and ratio >= 1.5 and today_chg >= 0.5:
            # Bull market + high turnover + price up: broad participation, trend continuation
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — broad participation confirmed)"
        elif market_regime_score <= 3 and ratio >= 1.5:
            # Bear market + elevated turnover: more likely distribution than accumulation
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (bear market — high turnover likely distribution)"

    # --- 52w position cross: price level determines what high/low turnover means ---
    position_signal = None
    if len(price_df) >= 20 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(252)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo:
                pos = (cur - lo) / (hi - lo)
                if ratio >= 1.5 and pos < 0.3 and today_chg >= 0.5:
                    # High volume + low price + rising: institutional bottom accumulation confirmed
                    score = min(10.0, score + 1.5)
                    position_signal = f"高换手+低位({pos:.2f})+上涨 — 底部放量承接，买入确认"
                elif ratio >= 1.5 and pos > 0.7 and today_chg <= -1.0:
                    # High volume + high price + falling: top distribution confirmed
                    sell_score = min(9.0, sell_score + 1.5)
                    position_signal = f"高换手+高位({pos:.2f})+下跌 — 顶部分发，卖出确认"
                elif ratio < 0.8 and pos > 0.7 and today_chg >= 0.5:
                    # Low volume + high price + rising: unsustainable, no one is buying
                    sell_score = min(9.0, sell_score + 1.0)
                    position_signal = f"低换手+高位({pos:.2f})+上涨 — 高位缩量，上涨乏力"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "current_turnover_5d": round(current_5d, 2),
            "avg_90d_turnover": round(avg_90d, 2),
            "ratio": round(ratio, 2),
            "ret_1m": round(ret_1m, 1) if ret_1m is not None else None,
            "market_regime_score": market_regime_score,
            "position_signal": position_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_chip_distribution(
    price_df: Optional[pd.DataFrame],
    fund_flow_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    social_dict: Optional[dict] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      Bottom panic (buy_score >= 7) + strong industry (excess >= +3%) -> buy +1.5 (底部恐慌+行业顺风=最强反弹信号)
      Top distribution (sell_score >= 7) + weak industry (excess <= -3%) -> sell +1.5 (高位分发+行业逆风=最强卖出信号)
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

    # --- Market regime cross: chip signal reliability differs in bull vs bear ---
    if market_regime_score is not None:
        if position < 0.3 and buy_score >= 7:
            # Bottom accumulation signal
            if market_regime_score >= 7:
                # Bull market: retail panic at lows is a reliable reversal setup
                buy_score = min(10.0, buy_score + 1.5)
                scenario = scenario + " (bull market — 底部恐慌反转信号可信度高)"
            elif market_regime_score <= 3:
                # Bear market: even retail panic doesn't guarantee a bottom; falling knives abundant
                buy_score = max(0.0, buy_score - 1.5)
                scenario = scenario + " (bear market — 熊市底部信号可靠性下降)"
        elif position > 0.7 and sell_score >= 7:
            # Top distribution signal
            if market_regime_score <= 3:
                # Bear market distribution at highs: amplified by macro environment
                sell_score = min(10.0, sell_score + 1.5)
                scenario = scenario + " (bear market — 高位分发在熊市中信号放大)"

    buy_score  = round(max(0.0, min(10.0, buy_score)),  1)
    sell_score = round(max(0.0, min(10.0, sell_score)), 1)

    # --- Industry excess return cross: sector tailwind amplifies chip signal ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if position < 0.3 and buy_score >= 7 and excess >= 3.0:
            # Bottom panic buy + strong industry: double confirmation of reversal
            buy_score = min(10.0, buy_score + 1.5)
            industry_signal = f"底部恐慌+行业强(超额{excess:.1f}%) — 最强反弹信号"
        elif position > 0.7 and sell_score >= 7 and excess <= -3.0:
            # Top distribution + weak industry: double confirmation of exit
            sell_score = min(10.0, sell_score + 1.5)
            industry_signal = f"高位分发+行业弱(超额{excess:.1f}%) — 最强卖出信号"

    buy_score  = round(max(0.0, min(10.0, buy_score)),  1)
    sell_score = round(max(0.0, min(10.0, sell_score)), 1)

    # --- Social heat cross: sentiment confirms chip signal extremes ---
    social_signal = None
    if social_dict is not None:
        rank_pct = social_dict.get("rank_pct")
        if rank_pct is not None:
            if buy_score >= 7 and rank_pct <= 0.20:
                # Bottom panic chips + high social attention: retail capitulation = bottom trio signal
                buy_score = min(10.0, buy_score + 1.5)
                social_signal = f"底部恐慌+社交热度高(rank_pct={rank_pct:.0%}) — 散户割肉底部三件套"
            elif sell_score >= 7 and rank_pct <= 0.20:
                # Top distribution + high buzz: institutions selling into FOMO retail
                sell_score = min(10.0, sell_score + 1.5)
                social_signal = f"高位分发+社交热度高(rank_pct={rank_pct:.0%}) — 机构借FOMO出货"

    # --- Earnings revision cross: fundamental catalyst confirms chip structure ---
    revision_signal_cd = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_cd   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_cd = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_cd  = up_cd - down_cd
                if buy_score >= 7 and net_cd >= 2:
                    # Bottom chip structure + analyst upgrades: fundamental catalyst confirms technical bottom
                    buy_score = min(10.0, buy_score + 1.5)
                    revision_signal_cd = f"底部筹码+分析师上调({net_cd:+d}家) — 基本面催化技术底部，双重确认"
                elif sell_score >= 7 and net_cd <= -1:
                    # Top distribution + analyst downgrade: institutional exit confirmed by analysts
                    sell_score = min(10.0, sell_score + 1.0)
                    revision_signal_cd = f"顶部派发+分析师下调({net_cd:+d}家) — 机构减仓同步确认"
        except Exception:
            pass

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
            "market_regime_score":   market_regime_score,
            "industry_signal":       industry_signal,
            "social_signal":         social_signal,
            "revision_signal":       revision_signal_cd,
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
    industry_excess: Optional[float] = None,
    market_regime_score: Optional[float] = None,
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

    Industry momentum cross (requires industry_excess):
      Concentration (change <= -5%) + industry outperforming (excess >= +3%) -> buy +1.5 (轮动窗口加速建仓)
      Concentration + industry underperforming (excess <= -3%)               -> buy -1 (可能是套牢盘集中)
      Dispersion (change >= +10%) + industry underperforming (excess <= -3%) -> sell +1 (行业下行加速出逃)

    Market regime cross (requires market_regime_score):
      Concentration (change <= -5%) + bear market (regime <= 3) -> smart money bottom-fishing in bear, buy +2
      Concentration + bull market (regime >= 7)                 -> normal accumulation, slightly less informative, buy -0.5
      Dispersion (change >= +10%) + bear market                 -> retail fleeing a falling market, sell +1.5
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

    # --- Industry momentum cross: sector context changes chip signal interpretation ---
    if industry_excess is not None:
        if change_pct <= -5 and industry_excess >= 3:
            # Chip concentration while sector is rising: institutions accelerating accumulation in rotation window
            score = min(15.0, score + 1.5)
            signal = signal + f" + industry outperforming ({industry_excess:+.1f}%) — 轮动窗口加速建仓"
        elif change_pct <= -5 and industry_excess <= -3:
            # Concentration in a falling sector: could be trapped longs, not conviction buying
            score = max(0.0, score - 1.0)
            signal = signal + f" + industry weak ({industry_excess:+.1f}%) — 可能是套牢盘集中，打折"
        elif change_pct >= 10 and industry_excess <= -3:
            # Dispersion while sector falls: holders fleeing a weak sector
            sell_score = min(15.0, sell_score + 1.0)
            signal = signal + f" + industry weak ({industry_excess:+.1f}%) — 行业下行加速出逃"

    # --- Market regime cross: concentration signal reliability is highest in bear markets ---
    if market_regime_score is not None:
        if change_pct <= -5:
            if market_regime_score <= 3:
                # Bear market concentration: informed buyers picking up shares against the trend — highest conviction
                score = min(15.0, score + 2.0)
                signal = signal + " (bear market — 熊市集中是高置信度逆势建仓)"
            elif market_regime_score >= 7:
                # Bull market concentration: normal in rising markets, lower informational edge
                score = max(0.0, score - 0.5)
                signal = signal + " (bull market — 牛市集中信号平凡化，略打折)"
        elif change_pct >= 10 and market_regime_score <= 3:
            # Dispersion in a bear market: holders fleeing a weak environment — amplify sell
            sell_score = min(15.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市分散加速出逃)"

    sell_score = round(sell_score, 1)

    return {
        "score": round(score, 1),
        "sell_score": sell_score,
        "max": 15,
        "details": {
            "current_holders":  int(current),
            "prev_holders":     int(prev),
            "change_pct":       round(change_pct, 2),
            "position_52w":     round(position, 3) if position is not None else None,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "market_regime_score": market_regime_score,
            "signal":           signal,
            "sell_score":       sell_score,
        },
    }


def score_lhb(
    lhb_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Market regime cross (requires market_regime_score):
      Net buy >= 1000万 + bull market (regime >= 7) -> buy +1.5 (牛市龙虎买入=趋势持续性强)
      Net buy >= 1000万 + bear market (regime <= 3) -> buy -1 (熊市中即使龙虎买入也可能是接刀)
      Net sell <= -1000万 + bear market             -> sell +1 (熊市龙虎卖出=加速出逃)

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      Net buy >= 1000万 + weak industry (excess <= -3%) -> buy +1.5 (弱行业中逆势买入=最高置信度)
      Net sell <= -1000万 + hot industry (excess >= +3%) -> sell +1.5 (行业热但龙虎卖出=内部人趁好出货)
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

    # --- Market regime cross: LHB signal persistence differs in bull vs bear ---
    regime_signal = None
    if market_regime_score is not None:
        if net_m >= 1000 and market_regime_score >= 7:
            # Bull market: LHB buys tend to persist — momentum and institutional follow-through
            score = min(10.0, score + 1.5)
            regime_signal = f"龙虎买入+牛市({market_regime_score:.1f}) — 趋势持续性强"
        elif net_m >= 1000 and market_regime_score <= 3:
            # Bear market: even LHB buys can be catching falling knives
            score = max(0.0, score - 1.0)
            regime_signal = f"龙虎买入+熊市({market_regime_score:.1f}) — 熊市接刀风险"
        elif net_m <= -1000 and market_regime_score <= 3:
            # Bear market exit: accelerating flight
            sell_score = min(10.0, sell_score + 1.0)
            regime_signal = f"龙虎卖出+熊市({market_regime_score:.1f}) — 加速出逃"

    # --- Industry excess return cross: sector context confirms LHB intent ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if net_m >= 1000 and excess <= -3.0:
            # Buying against a falling sector: highest conviction counter-trend bet
            score = min(10.0, score + 1.5)
            industry_signal = f"龙虎买入+弱行业(超额{excess:.1f}%) — 逆行业买入，最高置信度"
        elif net_m <= -1000 and excess >= 3.0:
            # Selling while sector is hot: insiders dumping into sector strength
            sell_score = min(10.0, sell_score + 1.5)
            industry_signal = f"龙虎卖出+热行业(超额{excess:.1f}%) — 趁行业热出货，信息优势明显"

    # --- Earnings revision cross: dual institutional signal confirmation ---
    revision_signal_lhb = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_lhb   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_lhb = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_lhb  = up_lhb - down_lhb
                if net_m >= 1000 and net_lhb >= 2:
                    # LHB net buy + analyst upgrades: trading action + research consensus aligned
                    score = min(10.0, score + 1.5)
                    revision_signal_lhb = f"龙虎净买+分析师上调({net_lhb:+d}家) — 交易行为+研究判断双重确认，高置信度"
                elif net_m <= -1000 and net_lhb <= -2:
                    # LHB net sell + analyst downgrades: institutional exit confirmed by analysts
                    sell_score = min(10.0, sell_score + 1.5)
                    revision_signal_lhb = f"龙虎净卖+分析师下调({net_lhb:+d}家) — 机构抛售被研究同步确认"
        except Exception:
            pass

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "net_buy_million": round(net_m, 1),
            "appearances": len(lhb_df),
            "position_52w": round(position, 2) if position is not None else None,
            "market_regime_score": market_regime_score,
            "regime_signal": regime_signal,
            "industry_signal": industry_signal,
            "revision_signal": revision_signal_lhb,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_lockup_pressure(
    lockup_df: Optional[pd.DataFrame],
    circulating_cap: float = 0,
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    social_dict: Optional[dict] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Social heat cross: unlock into retail frenzy = A-share "lockup dump" pattern (requires social_dict)
      Large unlock (>= 5%) + social heat top 10%  -> PE holders dump into retail FOMO = amplified sell +2
      Large unlock (>= 5%) + social heat > 50%    -> no retail bid to absorb, insiders can't easily exit = sell -1

    Volume distribution cross (requires price_df):
      Large unlock (>= 5%) + price up > 5% in 1m + volume contracting (v10/v30 < 0.75)
        -> classic pre-unlock distribution: insiders drip-selling into strength -> sell +2

    Market regime cross (requires market_regime_score):
      Large unlock (>= 5%) + bear market (regime <= 3) -> no buyers to absorb, amplify sell +1.5
      Large unlock (>= 5%) + bull market (regime >= 7) -> rising tide provides buyer depth, reduce sell -1
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

    # --- Social heat cross: unlocking into retail frenzy = A-share lockup dump pattern ---
    if social_dict is not None and ratio >= 5:
        rank_pct = social_dict.get("rank_pct")
        if rank_pct is not None:
            rank_pct_f = float(rank_pct)
            if rank_pct_f <= 10:
                # Extreme retail attention: PE/founder holders see the perfect exit window
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " + extreme social heat (unlock into retail FOMO — amplified)"
            elif rank_pct_f > 50:
                # Low retail interest: insiders have no easy buyer pool to exit into
                sell_score = max(0.0, sell_score - 1.0)
                signal = signal + " + low social heat (no retail bid — exit harder, mitigated)"

    # --- Volume distribution cross: pre-unlock distribution pattern ---
    if price_df is not None and ratio >= 5 and "volume" in price_df.columns and len(price_df) >= 40:
        try:
            closes = pd.to_numeric(price_df["close"], errors="coerce").dropna()
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(closes) >= 21 and len(vol) >= 40:
                ret_1m_lk = float((closes.iloc[-1] - closes.iloc[-21]) / closes.iloc[-21] * 100)
                v10 = float(vol.tail(10).mean())
                v30 = float(vol.tail(40).head(30).mean())
                if v30 > 0 and ret_1m_lk > 5 and v10 / v30 < 0.75:
                    # Price has risen while volume contracted + big unlock coming
                    # = classic pre-unlock distribution: insiders drip-selling into strength
                    sell_score = min(10.0, sell_score + 2.0)
                    signal = signal + f" + price up {ret_1m_lk:.0f}% on contracting volume (pre-unlock distribution)"
        except Exception:
            pass

    # --- Market regime cross: unlock absorption capacity is regime-dependent ---
    if market_regime_score is not None and ratio >= 5:
        if market_regime_score <= 3:
            # Bear market: incremental sellers from unlock have no buyers → supply overhang worsens
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市无人接盘，解禁压力放大)"
        elif market_regime_score >= 7:
            # Bull market: rising prices draw in buyers who can absorb unlock supply
            sell_score = max(0.0, sell_score - 1.0)
            signal = signal + " (bull market — 牛市有买盘消化解禁压力)"

    # --- Industry excess cross: sector direction shifts unlock holder motivation ---
    industry_signal_lk = None
    if industry_ret_1m is not None and market_ret_1m is not None and ratio >= 5:
        excess_lk = industry_ret_1m - market_ret_1m
        if excess_lk <= -3.0:
            # Weak sector: holders see deteriorating fundamentals, rush to exit during unlock window
            sell_score = min(10.0, sell_score + 1.5)
            industry_signal_lk = f"行业弱(超额{excess_lk:+.1f}%) — 股东借解禁窗口加速撤退"

    # --- Earnings revision cross: analyst view on company health during unlock window ---
    revision_signal_lk = None
    if revision_df is not None and not revision_df.empty and ratio >= 5:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_lk   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_lk = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_lk  = up_lk - down_lk
                if net_lk <= -2:
                    # Large unlock + analyst downgrades: supply shock + demand collapse double kill
                    sell_score = min(10.0, sell_score + 1.5)
                    revision_signal_lk = f"大解禁+分析师下调({net_lk:+d}家) — 供给冲击+需求萎缩双杀"
                elif net_lk >= 2:
                    # Large unlock + analyst upgrades: company in good shape, unlock may not trigger sell-off
                    sell_score = max(0.0, sell_score - 1.0)
                    revision_signal_lk = f"大解禁+分析师上调({net_lk:+d}家) — 基本面向好，解禁冲击有限"
        except Exception:
            pass

    sell_score = round(sell_score, 1)
    return {
        "score": round(max(0.0, buy_score), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "unlock_amount_billion": round(unlock_val / 1e8, 2),
            "ratio_pct": round(ratio, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "market_regime_score": market_regime_score,
            "industry_signal": industry_signal_lk,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "revision_signal": revision_signal_lk,
            "signal": signal,
            "sell_score": sell_score,
        },
    }


def score_insider(
    insider_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    industry_excess: Optional[float] = None,
    market_regime_score: Optional[float] = None,
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

    Earnings revision cross: insider vs. analyst information signal
      Net buy + net upgrades >= 2  -> insider AND analyst both bullish = dual conviction signal -> buy +2
      Net sell + net downgrades <= -2 -> insider AND analyst both bearish = dual exit signal -> sell +2
      Net buy + net downgrades <= -2 -> management buying despite analyst cuts = insider conviction overrides -> sell -1

    Industry momentum cross (requires industry_excess):
      Net buy + industry underperforming (excess <= -3%) -> buy +2 (逆势增持，熊途最高置信度)
      Net sell + industry outperforming (excess >= +3%)  -> sell +2 (趁行业好出货，信息优势明显)
      Net sell + industry also falling (excess <= -3%)   -> sell +1.5 (随行业下行出逃确认)

    Market regime cross (requires market_regime_score):
      Net buy (ratio > 0.3) + bear market (regime <= 3) -> buy +2 (熊市逆势增持=最高置信度的内部人信号)
      Net sell (ratio < -0.3) + bull market (regime >= 7) -> sell -1 (牛市减持可能只是正常套现，降低惩罚)
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

    # --- Earnings revision cross: insider intent vs. analyst consensus ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up_r   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down_r = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up_r - down_r
            if net_ratio > 0.3 and net_rev >= 2:
                # Insider buying + analyst upgrades: both information sources bullish
                score = min(10.0, score + 2.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — dual conviction"
            elif net_ratio < -0.3 and net_rev <= -2:
                # Insider selling + analyst downgrades: dual institutional exit
                sell_score = min(8.0, sell_score + 2.0)
                signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dual exit signal"
            elif net_ratio > 0.3 and net_rev <= -2:
                # Insiders buying but analysts cutting: management has conviction analysts don't
                sell_score = max(0.0, sell_score - 1.0)
                signal = signal + f" (buying despite analyst cuts — management conviction overrides)"

    # --- Industry momentum cross: sector context amplifies insider signal ---
    if industry_excess is not None:
        if net_ratio > 0.3 and industry_excess <= -3:
            # Insider buying while sector is falling: maximum conviction (against the tide)
            score = min(10.0, score + 2.0)
            signal = signal + f" (逆势增持 — industry {industry_excess:+.1f}%, 熊途最高置信度)"
        elif net_ratio < -0.3 and industry_excess >= 3:
            # Insider selling while sector is hot: information advantage, dumping into sector rally
            sell_score = min(8.0, sell_score + 2.0)
            signal = signal + f" (趁好出货 — industry {industry_excess:+.1f}%, 信息优势明显)"
        elif net_ratio < -0.3 and industry_excess <= -3:
            # Insider also selling in a weak sector: confirms structural deterioration
            sell_score = min(8.0, sell_score + 1.5)
            signal = signal + f" (随行业下行出逃 — industry {industry_excess:+.1f}%)"

    # --- Market regime cross: insider buy/sell conviction varies with market environment ---
    regime_signal = None
    if market_regime_score is not None:
        if net_ratio > 0.3 and market_regime_score <= 3:
            # Insider buying in a bear market: putting capital in against the tide = highest conviction
            score = min(10.0, score + 2.0)
            regime_signal = f"逆熊增持(regime={market_regime_score:.1f}) — 最高置信度的内部人信号"
        elif net_ratio < -0.3 and market_regime_score >= 7:
            # Insider selling in a bull market: likely routine profit-taking, less alarming
            sell_score = max(0.0, sell_score - 1.0)
            regime_signal = f"牛市减持(regime={market_regime_score:.1f}) — 可能是正常套现，降低惩罚"

    sell_score = round(sell_score, 1)

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "net_shares_million": round(net / 1e6, 1),
            "buy_events":         buy_events,
            "sell_events":        sell_events,
            "position_52w":       round(position, 3) if position is not None else None,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "market_regime_score": market_regime_score,
            "regime_signal":       regime_signal,
            "signal":             signal,
            "sell_score":         sell_score,
        },
    }


def score_institutional_visits(
    visits_df: Optional[pd.DataFrame],
    revision_df: Optional[pd.DataFrame] = None,
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
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

    52w price position cross (requires price_df):
      High visits (>= 5) + low position (< 0.3) -> 机构在低位调研=抄底发现被低估标的 -> buy +2
      High visits (>= 5) + high position (> 0.7) -> 机构高位调研可能是卖前尽调 -> sell +1

    Market regime cross (requires market_regime_score):
      High visits (>= 5) + bear market (regime <= 3) -> buy +1.5 (熊市主动调研=内部人发现被低估的逆向信号)

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      High visits (>= 5) + weak industry (excess <= -3%) -> buy +1 (弱行业中仍在调研=对个股alpha有信心)
      High visits (>= 5) + hot industry (excess >= +3%)  -> buy -0.5 (热行业调研可能是被动跟随而非主动发现)
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

    # --- 52w price position cross: visit intent changes completely with price level ---
    position_iv = _get_price_position(price_df)
    if position_iv is not None and count >= 5:
        if position_iv < 0.3:
            # Institutions visiting a beaten-down stock: bottom-fishing, genuine discovery
            score = min(10.0, score + 2.0)
            signal = signal + " (at low price — 低位调研=抄底发现低估标的)"
        elif position_iv > 0.7:
            # Institutions visiting a high-priced stock: possibly due-diligence before exit
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (at high price — 高位调研可能是卖前尽调)"

    # --- Market regime cross: bear market visits signal genuine contrarian discovery ---
    regime_signal = None
    if market_regime_score is not None and count >= 5:
        if market_regime_score <= 3:
            # Institutions actively visiting in bear market: going against the grain
            # = internal discovery of undervalued stock, highly contrarian signal
            score = min(10.0, score + 1.5)
            regime_signal = f"熊市主动调研({count}次) — 逆向发现被低估标的，内部人信号"

    # --- Industry excess return cross: context determines if visit is discovery or momentum-chasing ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None and count >= 5:
        excess = industry_ret_1m - market_ret_1m
        if excess <= -3.0:
            # Visiting in a weak sector: analysts going against the grain, high discovery value
            score = min(10.0, score + 1.0)
            industry_signal = f"弱行业调研(超额{excess:.1f}%) — 逆行业个股alpha，高发现价值"
        elif excess >= 3.0:
            # Hot sector: institutions may be following the crowd rather than discovering value
            score = max(0.0, score - 0.5)
            industry_signal = f"热行业调研(超额{excess:.1f}%) — 可能是被动跟随热点，发现价值较低"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "visit_count_90d": count,
            "position_52w": round(position_iv, 3) if position_iv is not None else None,
            "market_regime_score": market_regime_score,
            "regime_signal": regime_signal,
            "industry_signal": industry_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_industry_momentum(
    industry_ret_1m: Optional[float],
    market_ret_1m: Optional[float],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_stats: Optional[dict] = None,
    best_concept_ret: Optional[float] = None,
    social_dict: Optional[dict] = None,
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

    Market regime cross (requires market_regime_score):
      Hot sector (excess >= 3%) + bear market (regime <= 3) -> sector momentum unreliable -> buy -1.5, sell +1
      Hot sector + bull market (regime >= 7) -> sector rotation has follow-through -> buy +1

    Industry valuation cross (requires industry_stats):
      Cheap industry (median PE <= 20) + positive momentum -> early rotation setup -> buy +1.5
      Expensive industry (median PE >= 40) + outperforming -> late-stage stretched rally -> sell +1

    Concept momentum cross (requires best_concept_ret):
      Industry outperforming (excess >= +3%) + hot concept (>= +8%) -> buy +1.5 (行业+概念双重催化=最强的散户共振信号)
      Industry underperforming + hot concept (>= +8%) -> sell -0.5 (热概念可能即将轮动到该行业，略减弱卖出)
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

    # --- Market regime cross: sector momentum reliability in bull vs bear ---
    if market_regime_score is not None:
        if excess >= 3 and market_regime_score <= 3:
            # Hot sector in bear market: sector pumps are short-lived (游资 dominates)
            score = max(0.0, score - 1.5)
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (bear market — sector momentum unreliable)"
        elif excess >= 3 and market_regime_score >= 7:
            # Hot sector in bull market: sector rotation has institutional follow-through
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — sector momentum more reliable)"

    # --- Industry valuation cross: early rotation vs late-stage rally ---
    if industry_stats is not None and excess >= 2:
        pe_vals = industry_stats.get("pe")
        if pe_vals and len(pe_vals) >= 5:
            try:
                median_pe = float(pd.Series(pe_vals).median())
                if 0 < median_pe <= 20:
                    # Cheap sector starting to move: classic early rotation setup
                    score = min(10.0, score + 1.5)
                    signal = signal + f" (cheap sector PE~{median_pe:.0f}x — early rotation)"
                elif median_pe >= 40:
                    # Expensive sector still rallying: late-stage, stretched valuation
                    sell_score = min(9.0, sell_score + 1.0)
                    signal = signal + f" (expensive sector PE~{median_pe:.0f}x — late-stage rally)"
            except Exception:
                pass

    # --- Concept momentum cross: concept board as sector amplifier or rotation signal ---
    concept_signal = None
    if best_concept_ret is not None:
        if excess >= 3.0 and best_concept_ret >= 8.0:
            # Hot sector + hot concept: retail capital is converging on dual catalysts
            score = min(10.0, score + 1.5)
            concept_signal = f"行业强+热概念(+{best_concept_ret:.1f}%) — 双重催化，散户资金共振"
        elif excess <= -2.0 and best_concept_ret >= 8.0:
            # Weak sector but hot concept board: rotation may be imminent, soften sell signal
            sell_score = max(0.0, sell_score - 0.5)
            concept_signal = f"行业弱+热概念(+{best_concept_ret:.1f}%) — 概念可能轮动至此行业"

    # --- Social heat cross: retail lag vs institutional sector rotation ---
    social_signal_im = None
    if social_dict is not None and excess >= 3.0:
        rank_pct_im = social_dict.get("rank_pct")
        if rank_pct_im is not None:
            rank_pct_im = float(rank_pct_im)
            if rank_pct_im > 50:
                # Strong sector + low social heat: institutional rotation in progress, retail hasn't noticed
                score = min(10.0, score + 1.5)
                social_signal_im = f"行业强+社交低热(rank={rank_pct_im:.0f}%) — 机构已在推板块散户未感知，轮动早期"
            elif rank_pct_im <= 5:
                # Strong sector + extreme social heat: retail FOMO at sector peak, rotation likely ending
                sell_score = min(10.0, sell_score + 1.5)
                social_signal_im = f"行业强+社交极热(rank={rank_pct_im:.0f}%) — 散户FOMO接盘，板块轮动尾声"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "industry_ret_1m_pct": round(industry_ret_1m, 2),
            "market_ret_1m_pct": round(market, 2),
            "excess_pct": round(excess, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "market_regime_score": market_regime_score,
            "best_concept_ret": round(best_concept_ret, 2) if best_concept_ret is not None else None,
            "concept_signal": concept_signal,
            "social_signal": social_signal_im,
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
    market_regime_score: Optional[float] = None,
    social_dict: Optional[dict] = None,
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

    Market regime cross (requires market_regime_score):
      NB increasing (>= +2%) + bull market (regime >= 7) -> foreign capital riding bull, amplify buy +1
      NB reducing (<= -2%) + bear market (regime <= 3)   -> systematic foreign exit in downturn, sell +1.5
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

    # --- Market regime cross: NB flow conviction is amplified by market direction ---
    if market_regime_score is not None:
        if change_pct >= 2 and market_regime_score >= 7:
            # NB adding in a bull market: trend-aligned capital with real follow-through
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — 北向增仓顺势而为，信号增强)"
        elif change_pct <= -2 and market_regime_score <= 3:
            # NB reducing in a bear market: systematic risk-off exit, structural sell pressure
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 北向熊市减仓，系统性出逃)"

    # --- Social heat cross: A-share divergence between foreign and retail money ---
    social_signal_nb = None
    if social_dict is not None:
        rank_pct_nb = social_dict.get("rank_pct")
        if rank_pct_nb is not None:
            rank_pct_nb = float(rank_pct_nb)
            if change_pct <= -2 and rank_pct_nb <= 20:
                # NB reducing + high social heat: foreign money exits while domestic retail holds
                sell_score = min(10.0, sell_score + 2.0)
                social_signal_nb = f"北向减仓+社交高热(rank={rank_pct_nb:.0f}%) — A股散户接盘陷阱，外资出货"
            elif change_pct >= 2 and rank_pct_nb > 50:
                # NB increasing + low social heat: foreign money quietly accumulating before retail catches on
                score = min(10.0, score + 1.5)
                social_signal_nb = f"北向增仓+社交低热(rank={rank_pct_nb:.0f}%) — 外资悄悄买入散户未感知，早期机会"

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
            "market_regime_score": market_regime_score,
            "social_signal": social_signal_nb,
            "signal": signal,
            "sell_score": sell_score,
        },
    }


def score_earnings_revision(
    revision_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    visits_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    best_concept_ret: Optional[float] = None,
    social_dict: Optional[dict] = None,
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

    Institutional visits cross: sell-side × buy-side dual confirmation (requires visits_df)
      Upgrades (net >= 2) + visit_count >= 5  -> buy-side AND sell-side both bullish -> buy +1.5
      Downgrades (net <= -2) + visit_count == 0 -> no buy-side interest + sell-side cutting -> sell +1.5
      Upgrades (net >= 2) + visit_count == 0   -> analysts upgrading but buy-side absent -> sell +1
        (possible relationship/IR-driven upgrade without real institutional conviction)

    Market regime cross (requires market_regime_score):
      Upgrades (net >= 2) + bull market (regime >= 7) -> buy +1 (牛市双击：EPS↑ × 估值扩张)
      Upgrades (net >= 2) + bear market (regime <= 3) -> buy -1 (上修也对抗不了整体去估值)
      Downgrades (net <= -2) + bear market            -> sell +1 (熊市下修雪上加霜)

    Industry background cross (requires industry_ret_1m, market_ret_1m):
      Upgrades (net >= 2) + industry underperforming (excess <= -2%) -> buy +2 (异类上修，区分度最高)
      Upgrades (net >= 2) + industry outperforming (excess >= +5%)   -> buy -1 (随波逐流，打折处理)
      Downgrades (net <= -2) + industry weak (excess <= -3%)         -> sell +1 (行业顺风下调确认)
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

    # --- Institutional visits cross: sell-side × buy-side dual confirmation ---
    visit_count = None
    if visits_df is not None and not visits_df.empty:
        date_cols_v = [c for c in visits_df.columns if any(k in c for k in ["日期", "调研日期", "接待日期"])]
        visit_count = len(visits_df)
        if date_cols_v:
            try:
                _vc = visits_df.copy()
                _vc["_date"] = pd.to_datetime(_vc[date_cols_v[0]], errors="coerce")
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
                visit_count = int((_vc["_date"] >= cutoff).sum())
            except Exception:
                pass
        if net >= 2 and visit_count >= 5:
            # Sell-side upgrading + buy-side actively visiting: both institutional groups bullish
            score = min(10.0, score + 1.5)
            signal = signal + f" + {visit_count} institutional visits (sell-side + buy-side consensus)"
        elif net <= -2 and visit_count == 0:
            # Analysts cutting + zero buy-side interest: stock abandoned by all institutional players
            sell_score = min(9.0, sell_score + 1.5)
            signal = signal + " (downgrades + no institutional visits — fully abandoned)"
        elif net >= 2 and visit_count == 0:
            # Analysts upgrading but buy-side not visiting: possible IR-driven or relationship upgrade
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (upgrades without institutional visits — conviction questionable)"

    # --- Market regime cross: revision reliability differs in bull vs bear ---
    if market_regime_score is not None:
        if net >= 2 and market_regime_score >= 7:
            # Bull market: upgrades trigger multiple expansion on top of EPS growth (双击效应)
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — 牛市双击效应放大)"
        elif net >= 2 and market_regime_score <= 3:
            # Bear market: upgrades fighting against systematic de-rating
            score = max(0.0, score - 1.0)
            signal = signal + " (bear market — 上修对抗不了整体去估值)"
        elif net <= -2 and market_regime_score <= 3:
            # Downgrades in bear market: macro headwind amplifies fundamental deterioration
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (bear market — 熊市下修雪上加霜)"

    # --- Industry background cross: is this upgrade an anomaly or just sector tailwind? ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        ind_excess = industry_ret_1m - market_ret_1m
        if net >= 2 and ind_excess <= -2:
            # Individual upgrade while sector is weak: analyst independently discovered value
            score = min(10.0, score + 2.0)
            signal = signal + f" (异类上修 — sector excess {ind_excess:+.1f}%, 区分度最高)"
        elif net >= 2 and ind_excess >= 5:
            # Individual upgrade when sector is already hot: likely just riding sector tailwind
            score = max(0.0, score - 1.0)
            signal = signal + f" (随波逐流 — sector excess {ind_excess:+.1f}%, 打折处理)"
        elif net <= -2 and ind_excess <= -3:
            # Downgrades in a weak sector: sector headwind confirms the cut
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + f" (行业顺风下调 — sector excess {ind_excess:+.1f}%)"

    # --- Concept cross: fundamental revision + theme catalyst = A-share dual catalyst ---
    if best_concept_ret is not None:
        if net >= 2 and best_concept_ret >= 8.0:
            # Analyst upgrades + hot concept board: fundamental and theme catalysts converge
            score = min(10.0, score + 2.0)
            signal = signal + f" + hot concept {best_concept_ret:+.1f}% — 基本面+题材双重催化，A股最强买入信号"
        elif net <= -2 and best_concept_ret >= 8.0:
            # Analyst cuts + hot concept: theme is hiding fundamental deterioration
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + f" + hot concept {best_concept_ret:+.1f}% — 题材掩盖业绩恶化，散户被热度迷惑"

    # --- Social heat cross: A-share early-discovery vs peak-consensus signal ---
    social_signal_er = None
    if social_dict is not None and net >= 2:
        rank_pct_er = social_dict.get("rank_pct")
        if rank_pct_er is not None:
            rank_pct_er = float(rank_pct_er)
            if rank_pct_er > 50:
                # Analyst upgrades + low social heat: institutional discovery before retail notices
                score = min(10.0, score + 2.0)
                social_signal_er = f"分析师上调+社交低热(rank={rank_pct_er:.0f}%) — 机构悄悄发现散户未感知，A股最佳早期买点"
            elif rank_pct_er <= 5:
                # Analyst upgrades + extreme social heat: too much consensus, likely at peak
                sell_score = min(9.0, sell_score + 1.0)
                social_signal_er = f"分析师上调+社交极热(rank={rank_pct_er:.0f}%) — 过度共识可能是顶部，反向警示"

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": round(min(9.0, sell_score), 1),
        "max": 10,
        "details": {
            "up_revisions":      up,
            "down_revisions":    down,
            "net_revisions":     net,
            "trailing_growth":   round(trailing_growth, 1) if trailing_growth is not None else None,
            "visit_count_90d":   visit_count,
            "position_52w":      round(position, 3) if position is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 2) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "social_signal":     social_signal_er,
            "signal":            signal,
            "sell_score":        round(min(9.0, sell_score), 1),
        },
    }


# ===========================================================================
# GROUP C — New behavioral / market-context factors
# ===========================================================================

def score_limit_hits(
    price_df: Optional[pd.DataFrame],
    financial_df: Optional[pd.DataFrame] = None,
    social_dict: Optional[dict] = None,
    best_concept_ret: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Concept momentum cross (requires best_concept_ret):
      Net limit-ups (>= 2) + hot concept (>= +8%)  -> 板块联动连板，持续性强 -> buy +1.5
      Net limit-ups (>= 2) + concept cold (< 0%)   -> 孤立炒作，无板块支撑 -> sell +0.5

    Market regime cross (requires market_regime_score):
      Net limit-ups (>= 1) + bull market (regime >= 7) -> 牛市连板延续性强 -> buy +1.5
      Net limit-ups (>= 1) + bear market (regime <= 3) -> 熊市连板难以持续 -> buy -1, sell +1

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      Net limit-ups (>= 1) + industry outperforming (excess >= +3%) -> buy +1 (行业顺风中连板，有逻辑支撑，可持续性强)
      Net limit-ups (>= 1) + industry weak (excess <= -3%) -> sell +0.5 (独立于行业的纯题材炒作，回撤风险高)
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

    # --- Concept momentum cross: 连板 with or without sector support ---
    if best_concept_ret is not None and net >= 2:
        if best_concept_ret >= 8:
            # 连板 + hot concept board: 板块联动, strong persistence (full sector rotation)
            score = min(10.0, score + 1.5)
            signal = signal + f" + concept +{best_concept_ret:.1f}% (板块联动连板 — 持续性强)"
        elif best_concept_ret < 0:
            # 连板 but concept board falling: isolated speculation, quick fade
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" + concept {best_concept_ret:.1f}% (孤立炒作 — 无板块支撑)"

    # --- Market regime cross: limit-up persistence is regime-dependent ---
    if market_regime_score is not None and net >= 1:
        if market_regime_score >= 7:
            # Bull market: consecutive limit-ups have follow-through as institutional momentum persists
            score = min(10.0, score + 1.5)
            signal = signal + " (bull market — 牛市连板延续性强)"
        elif market_regime_score <= 3:
            # Bear market: limit-up rallies are short-lived, frequently reverse
            score = max(0.0, score - 1.0)
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bear market — 熊市连板难持续，警惕反转)"

    # --- Industry excess return cross: sector context determines 连板 sustainability ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None and net >= 1:
        excess = industry_ret_1m - market_ret_1m
        if excess >= 3.0:
            # Limit-ups with sector tailwind: fundamental logic supports continuation
            score = min(10.0, score + 1.0)
            industry_signal = f"连板+行业强(超额{excess:.1f}%) — 行业逻辑支撑，可持续性强"
        elif excess <= -3.0:
            # Limit-ups against a weak sector: pure speculative play, high reversal risk
            sell_score = min(10.0, sell_score + 0.5)
            industry_signal = f"连板+行业弱(超额{excess:.1f}%) — 纯题材炒作，脱离行业，回撤风险高"

    # --- Earnings revision cross: news-driven events need fundamental validation ---
    revision_signal_lh = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_lh   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_lh = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_lh  = up_lh - down_lh
                if net >= 2 and net_lh >= 2:
                    # Multiple limit-ups + analyst upgrades: fundamental news driving the event
                    score = min(10.0, score + 2.0)
                    revision_signal_lh = f"连板+分析师上调({net_lh:+d}家) — 基本面新闻驱动，非游资炒作，可持续性强"
                elif net <= -2 and net_lh <= -2:
                    # Multiple limit-downs + analyst downgrades: don't catch the knife
                    sell_score = min(10.0, sell_score + 2.0)
                    revision_signal_lh = f"连跌停+分析师下调({net_lh:+d}家) — 业绩暴雷确认，不要接刀"
        except Exception:
            pass

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "limit_up_count_20d":   up_count,
            "limit_down_count_20d": down_count,
            "net_limit_up":         net,
            "position_52w":         round(position, 3) if position is not None else None,
            "best_concept_ret":     round(best_concept_ret, 2) if best_concept_ret is not None else None,
            "market_regime_score":  market_regime_score,
            "industry_signal":      industry_signal,
            "revision_signal":      revision_signal_lh,
            "signal":               signal,
            "sell_score":           round(min(10.0, sell_score), 1),
        },
    }


def score_price_inertia(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Short-term price inertia: consecutive up/down day streak (max 10).
    Crossed with volume trend to confirm continuation vs exhaustion.

      Consecutive up days (3+) + volume expanding -> strong continuation (buy)
      Consecutive up days (3+) + volume contracting -> unsustainable (sell warning)
      Consecutive down days (3+) + volume expanding -> accelerating sell (sell)
      Consecutive down days (3+) + volume contracting -> exhaustion (potential reversal)

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      Consecutive up (3+) + industry outperforming (excess >= +3%) -> sector tailwind, buy +1
      Consecutive up (3+) + industry weak (excess <= -3%) -> fighting the sector tide, sell +0.5
      Consecutive down (3+) + industry weak -> sector drags further, sell +0.5
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

    # --- Market regime cross: streak continuation probability is regime-dependent ---
    if market_regime_score is not None:
        if consec_up >= 3:
            if market_regime_score >= 7:
                # Bull market: uptrend continuation more reliable, institutional follow-through
                score = min(10.0, score + 1.5)
                signal = signal + " (bull market — 上升趋势持续性更强)"
            elif market_regime_score <= 3:
                # Bear market: multi-day rallies are typically dead-cat bounces
                sell_score = min(10.0, sell_score + 1.5)
                signal = signal + " (bear market — 熊市连涨大概率是死猫弹)"
        elif consec_down >= 3:
            if market_regime_score >= 7:
                # Downtrend in bull market: oversold bounce likely, reduce sell urgency
                sell_score = max(0.0, sell_score - 1.0)
                signal = signal + " (bull market — 超跌反弹机会，减弱卖出)"
            elif market_regime_score <= 3:
                # Bear market downtrend: structural decline, amplify sell
                sell_score = min(10.0, sell_score + 1.0)
                signal = signal + " (bear market — 趋势性下跌确认)"

    # --- Industry excess cross: streak reliability depends on sector direction ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if consec_up >= 3:
            if excess >= 3:
                # Sector is hot — upstreak has tailwind, likely to continue
                score = min(10.0, score + 1.0)
                signal = signal + f" (industry outperforming {excess:+.1f}% — 行业顺风，惯性更强)"
            elif excess <= -3:
                # Sector is weak — upstreak is against the tide, fade risk elevated
                sell_score = min(10.0, sell_score + 0.5)
                signal = signal + f" (industry weak {excess:+.1f}% — 逆行业连涨，注意回落)"
        elif consec_down >= 3 and excess <= -3:
            # Sector also falling: double headwind, amplify sell
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" (industry weak {excess:+.1f}% — 行业下行加剧连跌)"

    # --- 52w position cross: streak meaning changes completely at price extremes ---
    position_signal = None
    if len(price_df) >= 20 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(252)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo:
                pos = (cur - lo) / (hi - lo)
                if consec_up >= 3 and pos < 0.3:
                    # Up streak just starting from near 52w low: maximum upside remaining
                    score = min(10.0, score + 1.0)
                    position_signal = f"连涨+低位({pos:.2f}) — 动量刚启动，空间最大的买点"
                elif consec_up >= 3 and pos > 0.8:
                    # Up streak near 52w high: likely late-stage, distribution risk grows
                    sell_score = min(10.0, sell_score + 0.5)
                    position_signal = f"连涨+高位({pos:.2f}) — 接近历史高点，注意回撤"
                elif consec_down >= 3 and pos > 0.7:
                    # Down streak from 52w high: structural decline confirmed, not a dip
                    sell_score = min(10.0, sell_score + 1.5)
                    position_signal = f"连跌+高位({pos:.2f}) — 高位开始崩，趋势性下行确认"
        except Exception:
            pass

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "consecutive_up_days":   consec_up,
            "consecutive_down_days": consec_down,
            "vol_expanding":         vol_expanding,
            "annualized_vol_pct":    round(ann_vol, 1) if ann_vol is not None else None,
            "market_regime_score":   market_regime_score,
            "industry_excess_pct":   round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "position_signal":       position_signal,
            "signal":                signal,
            "sell_score":            round(min(10.0, sell_score), 1),
        },
    }


def score_social_heat(
    social_dict: Optional[dict],
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    best_concept_ret: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Volume cross (requires price_df): real money vs pure social noise
      High heat (top 20%) + volume surge (v5/v20 >= 2x) -> retail FOMO confirmed, amplify contrarian sell +1.5
      High heat (top 20%) + volume low (v5/v20 < 0.7x)  -> buzz fading quickly, sell +1
      Low heat (> 50%) + volume breakout (>= 2x)        -> institutional buying quietly, buy +1.5

    Concept momentum alignment cross (requires best_concept_ret):
      High heat (top 20%) + hot concept (>= +8%) -> 双重FOMO信号，炒作顶部 -> sell +1.5
      High heat (top 20%) + concept cold (< 0%)  -> 热度没有板块支撑，快速消退 -> sell +1
      Low heat (> 50%) + hot concept (>= +8%)    -> 机构在推概念，散户未感知，早期机会 -> buy +1.5

    Market regime cross (requires market_regime_score):
      Extreme heat (top 5%) + bear market (regime <= 3) -> 熊市炒作无持续性，更强卖出 -> sell +1.5
      Extreme heat (top 5%) + bull market (regime >= 7) -> 牛市热度有基础，略减弱卖出 -> sell -0.5
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

    # --- Volume cross: distinguish real money interest from pure social noise ---
    if price_df is not None and "volume" in price_df.columns and len(price_df) >= 25:
        try:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 25:
                v5  = float(vol.tail(5).mean())
                v20 = float(vol.tail(25).head(20).mean())
                if v20 > 0:
                    heat_vol_ratio = v5 / v20
                    if rank_pct <= 20 and heat_vol_ratio >= 2.0:
                        # Trending socially + volume surge: retail FOMO is confirmed, amplify contrarian sell
                        sell_score = min(10.0, sell_score + 1.5)
                        signal = signal + f" + volume surge ×{heat_vol_ratio:.1f} (retail FOMO confirmed — amplify contrarian)"
                    elif rank_pct <= 20 and heat_vol_ratio < 0.7:
                        # Social buzz but volume drying up: attention without trading = fading quickly
                        sell_score = min(10.0, sell_score + 1.0)
                        signal = signal + f" + volume low ×{heat_vol_ratio:.1f} (social buzz without trading — fading)"
                    elif rank_pct > 50 and heat_vol_ratio >= 2.0:
                        # Low social heat but volume surging: institutional buying quietly without retail noise
                        score = min(10.0, score + 1.5)
                        signal = signal + f" + volume breakout ×{heat_vol_ratio:.1f} (institutional buying quietly)"
        except Exception:
            pass

    # --- Concept momentum alignment cross ---
    if best_concept_ret is not None:
        if rank_pct <= 20 and best_concept_ret >= 8:
            # Social heat + hot concept: double FOMO signal, classic A-share pump top
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + f" + hot concept +{best_concept_ret:.1f}% (双重FOMO — 炒作顶部风险)"
        elif rank_pct <= 20 and best_concept_ret < 0:
            # Social heat but concept is falling: buzz without sector backing, will fade quickly
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + f" + concept {best_concept_ret:.1f}% (热度无板块支撑 — 快速消退)"
        elif rank_pct > 50 and best_concept_ret >= 8:
            # Low social heat + hot concept: institutional rotation without retail noise = early
            score = min(10.0, score + 1.5)
            signal = signal + f" + hot concept +{best_concept_ret:.1f}% (低热度+板块热 — 机构推动早期)"

    # --- Market regime cross: social heat contrarian signal is stronger in bear markets ---
    if market_regime_score is not None and rank_pct <= 5:
        if market_regime_score <= 3:
            # Bear market extreme heat: speculative pop has no macro tailwind, fades faster
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市热炒无持续性，更强反向信号)"
        elif market_regime_score >= 7:
            # Bull market extreme heat: rising tide softens the contrarian sell
            sell_score = max(0.0, sell_score - 0.5)
            signal = signal + " (bull market — 牛市热度有基础，略减弱卖出)"

    # --- Industry excess cross: hot stock in hot sector vs isolated retail hype ---
    industry_signal_s = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess_s = industry_ret_1m - market_ret_1m
        if rank_pct <= 20:
            if excess_s >= 3.0:
                # High social heat + strong sector: institutional and retail converging — bubble risk higher
                sell_score = min(10.0, sell_score + 1.0)
                industry_signal_s = f"社交热+行业强(超额{excess_s:+.1f}%) — 机构散户共振炒作，泡沫风险更高"
            elif excess_s <= -3.0:
                # High social heat + weak sector: isolated retail hype with no sector support, fades faster
                sell_score = min(10.0, sell_score + 1.5)
                industry_signal_s = f"社交热+行业弱(超额{excess_s:+.1f}%) — 散户孤立炒作无行业支撑，热度更快消退"
        elif rank_pct > 50 and excess_s >= 3.0:
            # Low heat + hot sector: stock being overlooked while sector rallies — catch-up potential
            score = min(10.0, score + 1.0)
            industry_signal_s = f"低热度+行业强(超额{excess_s:+.1f}%) — 行业热但此股被忽视，补涨机会"

    # --- Earnings revision cross: analyst view vs retail sentiment divergence ---
    revision_signal_sh = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_sh   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_sh = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_sh  = up_sh - down_sh
                if rank_pct <= 20 and net_sh <= -1:
                    # High social heat + analyst downgrade: retail pump while institutions exit
                    sell_score = min(10.0, sell_score + 2.0)
                    revision_signal_sh = f"社交热+分析师下调({net_sh:+d}家) — 散户炒作机构撤退，最强卖出信号"
                elif rank_pct <= 20 and net_sh >= 2:
                    # High social heat + analyst upgrade: rare dual confirmation
                    sell_score = max(0.0, sell_score - 1.0)
                    revision_signal_sh = f"社交热+分析师上调({net_sh:+d}家) — 基本面情绪共振，热度有业绩支撑"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "rank":            social_dict.get("rank"),
            "rank_pct":        round(rank_pct, 1),
            "position_52w":    round(position, 3) if position is not None else None,
            "roe_pct":         round(roe, 1) if roe is not None else None,
            "best_concept_ret": round(best_concept_ret, 2) if best_concept_ret is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "industry_signal":  industry_signal_s,
            "revision_signal":  revision_signal_sh,
            "signal":           signal,
            "sell_score":       round(sell_score, 1),
        },
    }


def score_concept_momentum(
    concept_data: Optional[list],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    financial_df: Optional[pd.DataFrame] = None,
    industry_excess: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
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

    Volume confirmation cross (requires price_df):
      Hot concept (best_ret >= 8%) + volume breakout (vol_5d/vol_20d >= 1.5) → buy +1.5 (real participation)
      Hot concept + volume contraction (vol_5d/vol_20d < 0.7) → buy -1, sell +1 (hype without money)

    Industry × concept dual momentum cross (requires industry_excess):
      Hot concept + industry outperforming (excess >= 3%) → buy +1.5 (dual catalyst — sector + concept)
      Hot concept + industry underperforming (excess <= -3%) → sell +0.5 (isolated play, less reliable)

    52w price position cross (requires price_df):
      Hot concept (best_ret >= +8%) + low position (< 0.3) -> concept rally + low base = buy +1.5
      Hot concept (best_ret >= +8%) + high position (> 0.7) -> overextended concept rally, sell +1
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
        if lag >= 15:
            # Stock massively lags its hot concept — catch-up opportunity
            score = min(10.0, score + 2.0)
            signal = signal + f" (stock lags concept by {lag:.1f}% — catch-up potential)"
        elif stock_ret_1m - best_ret >= 20:
            # Stock has massively outrun its concept board — dragon-head fade risk
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + f" (stock leads concept by {stock_ret_1m - best_ret:.1f}% — dragon-head fade risk)"

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

    # --- Volume confirmation cross: is the concept rally backed by real trading activity? ---
    if price_df is not None and "volume" in price_df.columns and len(price_df) >= 25 and best_ret >= 8:
        try:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 25:
                v5  = float(vol.tail(5).mean())
                v20 = float(vol.tail(25).head(20).mean())
                if v20 > 0:
                    concept_vol_ratio = v5 / v20
                    if concept_vol_ratio >= 1.5:
                        # Hot concept + volume expansion: real money participating
                        score = min(10.0, score + 1.5)
                        signal = signal + f" + volume ×{concept_vol_ratio:.1f} (real participation confirmed)"
                    elif concept_vol_ratio < 0.7:
                        # Hot concept but volume shrinking: social/news hype without trading follow-through
                        score = max(0.0, score - 1.0)
                        sell_score = min(10.0, sell_score + 1.0)
                        signal = signal + f" + volume ×{concept_vol_ratio:.1f} (hype without real money)"
        except Exception:
            pass

    # --- Industry × concept dual momentum cross ---
    if industry_excess is not None and best_ret >= 8:
        if industry_excess >= 3:
            # Both concept board AND industry sector are hot: dual catalyst, mutual amplification
            score = min(10.0, score + 1.5)
            signal = signal + f" + industry also outperforming ({industry_excess:+.1f}%) — dual momentum"
        elif industry_excess <= -3:
            # Hot concept but the underlying industry is weak: isolated play without sector support
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" + industry underperforming ({industry_excess:+.1f}%) — isolated concept play"

    # --- 52w price position cross: concept rally at a low base = best setup ---
    position_cm = _get_price_position(price_df)
    if position_cm is not None and best_ret >= 8:
        if position_cm < 0.3:
            # Hot concept + stock at 52w low: concept rally has a fresh base to run
            score = min(10.0, score + 1.5)
            signal = signal + f" (low position {position_cm:.2f} — 热概念+低位，上涨空间大)"
        elif position_cm > 0.7:
            # Hot concept but stock already at highs: late-entry risk, overextended
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + f" (high position {position_cm:.2f} — 热概念+高位，注意回调风险)"

    # --- Earnings revision cross: fundamental support separates real from speculative theme plays ---
    revision_signal_cm = None
    if revision_df is not None and not revision_df.empty and best_ret >= 8:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_cm   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_cm = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_cm  = up_cm - down_cm
                if net_cm >= 2:
                    # Hot concept + analyst upgrades: fundamental backing, not pure speculation
                    score = min(10.0, score + 1.5)
                    revision_signal_cm = f"热概念+分析师上调({net_cm:+d}家) — 有业绩支撑的概念行情，可持续性强"
                elif net_cm <= -2:
                    # Hot concept + analyst downgrades: pure theme bubble, no earnings support
                    sell_score = min(10.0, sell_score + 2.0)
                    revision_signal_cm = f"热概念+分析师下调({net_cm:+d}家) — 纯主题泡沫无业绩支撑，下调确认见顶"
        except Exception:
            pass

    return {
        "score":      round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "best_concept":        best["name"],
            "best_ret_1m":         round(best_ret, 2),
            "worst_concept":       worst["name"],
            "worst_ret_1m":        round(worst_ret, 2),
            "concepts_count":      len(concept_data),
            "stock_ret_1m":        round(stock_ret_1m, 2) if stock_ret_1m is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "position_52w":        round(position_cm, 3) if position_cm is not None else None,
            "roe_pct":             round(roe_concept, 1) if roe_concept is not None else None,
            "revision_signal":     revision_signal_cm,
            "signal":              signal,
            "sell_signal":         sell_signal,
            "sell_score":          round(sell_score, 1),
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


# ===========================================================================
# DIVERGENCE — Multi-indicator confluence (底背离 / 顶背离)
# ===========================================================================

def _compute_macd_hist(close: pd.Series, fast: int = 12,
                       slow: int = 26, signal: int = 9) -> pd.Series:
    """Return MACD histogram (DIF - DEA)."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif      = ema_fast - ema_slow
    dea      = dif.ewm(span=signal, adjust=False).mean()
    return dif - dea


def _compute_rsi(close: pd.Series, window: int = 14) -> pd.Series:
    """Return RSI series."""
    delta    = close.diff()
    gain     = delta.clip(lower=0).ewm(span=window, adjust=False).mean()
    loss     = (-delta.clip(upper=0)).ewm(span=window, adjust=False).mean()
    rs       = gain / (loss + 1e-10)
    return 100 - 100 / (1 + rs)


def _compute_kdj_k(high: pd.Series, low: pd.Series,
                   close: pd.Series, n: int = 9, m: int = 3) -> pd.Series:
    """Return KDJ K-line."""
    lo = low.rolling(n).min()
    hi = high.rolling(n).max()
    rsv = (close - lo) / (hi - lo + 1e-10) * 100
    return rsv.ewm(com=m - 1, adjust=False).mean()


def _divergence_signal(price: pd.Series, indicator: pd.Series,
                       pivot_window: int = 5, lookback: int = 80,
                       min_sep: int = 10) -> float:
    """
    Detect divergence between price and indicator over the last `lookback` bars.

    Returns:
      +1.0  strong bullish divergence  (price lower-low, indicator higher-low)
      +0.5  weak   bullish divergence  (small magnitude)
       0.0  no divergence
      -0.5  weak   bearish divergence
      -1.0  strong bearish divergence  (price higher-high, indicator lower-high)
    """
    # Align and trim to lookback window
    combined = pd.concat([price.rename("p"), indicator.rename("i")], axis=1).dropna()
    if len(combined) < lookback // 2:
        return 0.0
    combined = combined.tail(lookback)
    p = combined["p"].values
    ind = combined["i"].values
    n = len(p)

    # ── Find local minima (troughs) for bullish divergence ─────────────────
    troughs: list[int] = []
    for i in range(pivot_window, n - pivot_window):
        if (all(p[i] <= p[i - j] for j in range(1, pivot_window + 1)) and
                all(p[i] <= p[i + j] for j in range(1, pivot_window + 1))):
            if not troughs or i - troughs[-1] >= min_sep:
                troughs.append(i)

    if len(troughs) >= 2:
        t1, t2 = troughs[-2], troughs[-1]
        price_ll  = p[t2]   < p[t1]   * 0.998   # price made lower low
        ind_hl    = ind[t2] > ind[t1] * 1.002    # indicator made higher low
        if price_ll and ind_hl:
            price_drop = (p[t1] - p[t2]) / (abs(p[t1]) + 1e-10)
            ind_rise   = (ind[t2] - ind[t1]) / (abs(ind[t1]) + 1e-10)
            magnitude  = price_drop + ind_rise
            return +1.0 if magnitude > 0.04 else +0.5

    # ── Find local maxima (peaks) for bearish divergence ───────────────────
    peaks: list[int] = []
    for i in range(pivot_window, n - pivot_window):
        if (all(p[i] >= p[i - j] for j in range(1, pivot_window + 1)) and
                all(p[i] >= p[i + j] for j in range(1, pivot_window + 1))):
            if not peaks or i - peaks[-1] >= min_sep:
                peaks.append(i)

    if len(peaks) >= 2:
        k1, k2 = peaks[-2], peaks[-1]
        price_hh  = p[k2]   > p[k1]   * 1.002   # price made higher high
        ind_lh    = ind[k2] < ind[k1] * 0.998    # indicator made lower high
        if price_hh and ind_lh:
            price_rise = (p[k2] - p[k1]) / (abs(p[k1]) + 1e-10)
            ind_drop   = (ind[k1] - ind[k2]) / (abs(ind[k1]) + 1e-10)
            magnitude  = price_rise + ind_drop
            return -1.0 if magnitude > 0.04 else -0.5

    return 0.0


def score_divergence(price_df: Optional[pd.DataFrame]) -> dict:
    """
    Multi-indicator divergence confluence score (max 10).

    Checks four independent divergence signals:
      1. MACD histogram 日线背离  (12,26,9)  — weight 1.5×  (most followed in A-shares)
      2. RSI 背离               (14-period) — weight 1.0×
      3. KDJ K-line 背离        (9,3,3)    — weight 1.0×  (popular in Chinese retail)
      4. 量价背离               (volume vs price direction) — weight 1.0×

    Composite scoring:
      Bottom divergence (底背离): positive contribution → bullish → high score
      Top    divergence (顶背离): negative contribution → bearish → low score

      Weighted sum → normalised to [0, 10] with neutral = 5.0.

    Multiple signals aligning (共振) amplifies score:
      All 4 signals  bullish → ~10
      3   signals    bullish → ~8–9
      2   signals    bullish → ~7
      1   signal     bullish → ~6
      No divergence          →  5
      1   signal     bearish → ~4
      2   signals    bearish → ~3
      3   signals    bearish → ~1–2
      All 4 signals  bearish → ~0
    """
    MAX = 10
    if price_df is None or len(price_df) < 40:
        return _neutral(MAX)

    required = {"close", "high", "low", "volume"}
    if not required.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        close  = pd.to_numeric(price_df["close"],  errors="coerce").dropna()
        high   = pd.to_numeric(price_df["high"],   errors="coerce").dropna()
        low    = pd.to_numeric(price_df["low"],    errors="coerce").dropna()
        volume = pd.to_numeric(price_df["volume"], errors="coerce").dropna()

        if len(close) < 40:
            return _neutral(MAX)

        # ── 1. MACD histogram divergence ──────────────────────────────────
        macd_hist  = _compute_macd_hist(close)
        sig_macd   = _divergence_signal(close, macd_hist)

        # ── 2. RSI divergence ─────────────────────────────────────────────
        rsi        = _compute_rsi(close)
        sig_rsi    = _divergence_signal(close, rsi)

        # ── 3. KDJ K-line divergence ──────────────────────────────────────
        kdj_k      = _compute_kdj_k(high, low, close)
        sig_kdj    = _divergence_signal(close, kdj_k)

        # ── 4. 量价背离 (volume-price divergence) ─────────────────────────
        # Bullish vol-price: price at lower low, but volume contracting (seller exhaustion)
        # Bearish vol-price: price at higher high, but volume shrinking (buyer exhaustion)
        vol_ma5    = volume.rolling(5).mean()
        vol_ma20   = volume.rolling(20).mean()
        vol_ratio  = (vol_ma5 / (vol_ma20 + 1e-10)).reindex(close.index)
        sig_vol    = _divergence_signal(close, -vol_ratio)  # invert: low vol at trough = bullish

        # ── Composite weighted score ──────────────────────────────────────
        W_MACD, W_RSI, W_KDJ, W_VOL = 1.5, 1.0, 1.0, 1.0
        total_weight = W_MACD + W_RSI + W_KDJ + W_VOL  # 4.5

        weighted_sum = (sig_macd * W_MACD + sig_rsi * W_RSI +
                        sig_kdj  * W_KDJ  + sig_vol * W_VOL)
        # weighted_sum range: [-4.5, +4.5]
        # Map to [0, 10]: score = 5 + weighted_sum / 4.5 * 5
        score = 5.0 + (weighted_sum / total_weight) * 5.0
        score = float(np.clip(score, 0.0, 10.0))

        # sell_score: high when bearish divergence is strong
        sell_score = float(np.clip(5.0 - (weighted_sum / total_weight) * 5.0, 0.0, 10.0))

        # ── Signal label ──────────────────────────────────────────────────
        n_bull = sum(1 for s in [sig_macd, sig_rsi, sig_kdj, sig_vol] if s > 0)
        n_bear = sum(1 for s in [sig_macd, sig_rsi, sig_kdj, sig_vol] if s < 0)
        if n_bull >= 3:
            label = f"strong bullish divergence ({n_bull}/4 signals)"
        elif n_bull == 2:
            label = f"moderate bullish divergence ({n_bull}/4 signals)"
        elif n_bull == 1:
            label = "weak bullish divergence (1/4 signals)"
        elif n_bear >= 3:
            label = f"strong bearish divergence ({n_bear}/4 signals)"
        elif n_bear == 2:
            label = f"moderate bearish divergence ({n_bear}/4 signals)"
        elif n_bear == 1:
            label = "weak bearish divergence (1/4 signals)"
        else:
            label = "no divergence"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":           label,
                "macd_divergence":  sig_macd,
                "rsi_divergence":   sig_rsi,
                "kdj_divergence":   sig_kdj,
                "vol_divergence":   sig_vol,
                "n_bullish":        n_bull,
                "n_bearish":        n_bear,
                "weighted_sum":     round(weighted_sum, 3),
                "sell_score":       round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ===========================================================================
# NEW GROUP A FACTORS — added 2026-03
# ===========================================================================

# ---------------------------------------------------------------------------
# score_bollinger_position — 布林带位置 (Bollinger Band Position)
# ---------------------------------------------------------------------------

def score_bollinger_position(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """布林带位置因子 — Bollinger Band position as a contrarian mean-reversion signal.

    Logic:
      BB position = (close - lower_band) / (upper_band - lower_band)
        0.0 = at lower band (oversold)   → bullish
        0.5 = at midline (neutral)
        1.0 = at upper band (overbought) → bearish

    Parameters: 20-period MA with 2× std bands (standard BB).

    Scoring:
      position ≤ 0.10  (near lower band, deeply oversold)  → score 9–10
      position ≈ 0.25                                       → score 7
      position = 0.50  (midline, neutral)                  → score 5
      position ≈ 0.75                                       → score 3
      position ≥ 0.90  (near upper band, overbought)       → score 0–1
    """
    MAX = 10
    if price_df is None or len(price_df) < 25:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 25:
            return _neutral(MAX)

        # Compute BB on full series, take latest value
        ma20    = close.rolling(20).mean()
        std20   = close.rolling(20).std(ddof=1)
        upper   = ma20 + 2.0 * std20
        lower   = ma20 - 2.0 * std20

        latest_close = float(close.iloc[-1])
        latest_upper = float(upper.iloc[-1])
        latest_lower = float(lower.iloc[-1])
        band_width   = latest_upper - latest_lower

        if band_width < 1e-8:
            return _neutral(MAX)

        position = (latest_close - latest_lower) / band_width
        position = float(np.clip(position, 0.0, 1.0))

        # Contrarian: low position = oversold = high score
        score      = float(np.clip((1.0 - position) * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(position * 10.0, 0.0, 10.0))

        if position <= 0.15:
            signal = f"near lower band (oversold) pos={position:.2f}"
        elif position >= 0.85:
            signal = f"near upper band (overbought) pos={position:.2f}"
        else:
            signal = f"mid-range pos={position:.2f}"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":      signal,
                "bb_position": round(position, 3),
                "close":       round(latest_close, 2),
                "upper_band":  round(latest_upper, 2),
                "lower_band":  round(latest_lower, 2),
                "sell_score":  round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_roe_trend — ROE趋势 (ROE Trend / Improvement)
# ---------------------------------------------------------------------------

def score_roe_trend(
    financial_df: Optional[pd.DataFrame],
) -> dict:
    """ROE趋势因子 — direction of ROE change signals profitability improvement.

    Logic:
      Compares most recent ROE with prior period ROE.
      Improving ROE (margin expansion, efficiency gains) → bullish.
      Deteriorating ROE (earnings quality erosion) → bearish.

    The change is normalised against the prior ROE level so small changes on
    high-ROE firms and large changes on low-ROE firms are properly weighted.

    Scoring (change defined as roe_cur - roe_prev in percentage points):
      Δ ≥ +5 pp   → score 9–10  (strong improvement)
      Δ  +2~+5 pp → score 7–8   (moderate improvement)
      Δ  0~+2 pp  → score 5–6   (mild improvement)
      Δ  -2~0 pp  → score 4–5   (mild deterioration)
      Δ  -5~-2 pp → score 2–3   (moderate deterioration)
      Δ ≤ -5 pp   → score 0–1   (sharp deterioration)
    """
    MAX = 10
    if financial_df is None or financial_df.empty:
        return _neutral(MAX)

    try:
        roe_cur, roe_prev = _extract_two(
            financial_df,
            ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"],
        )

        if roe_cur is None or roe_prev is None:
            # Only one period: score on absolute level only
            if roe_cur is not None:
                score = float(np.clip(5.0 + roe_cur / 6.0, 0.0, 10.0))
                sell_score = float(np.clip(10.0 - score, 0.0, 10.0))
                return {
                    "score":      round(score, 1),
                    "sell_score": round(sell_score, 1),
                    "max":        MAX,
                    "details": {
                        "signal":    f"single period ROE={roe_cur:.1f}%",
                        "roe_cur":   round(roe_cur, 2),
                        "roe_prev":  None,
                        "roe_delta": None,
                        "sell_score": round(sell_score, 1),
                    },
                }
            return _neutral(MAX)

        delta = roe_cur - roe_prev   # percentage points change

        # Score: 5 = no change, higher = improving, lower = deteriorating
        # Scale: ±5 pp maps to ±2.5 score points; clamp to [0,10]
        score      = float(np.clip(5.0 + delta * 0.5, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - delta * 0.5, 0.0, 10.0))

        if delta >= 5:
            signal = f"ROE sharply improving +{delta:.1f}pp ({roe_prev:.1f}%→{roe_cur:.1f}%)"
        elif delta >= 2:
            signal = f"ROE improving +{delta:.1f}pp ({roe_prev:.1f}%→{roe_cur:.1f}%)"
        elif delta >= 0:
            signal = f"ROE stable/mild improvement +{delta:.1f}pp ({roe_cur:.1f}%)"
        elif delta >= -2:
            signal = f"ROE mild decline {delta:.1f}pp ({roe_cur:.1f}%)"
        elif delta >= -5:
            signal = f"ROE declining {delta:.1f}pp ({roe_prev:.1f}%→{roe_cur:.1f}%)"
        else:
            signal = f"ROE sharply declining {delta:.1f}pp ({roe_prev:.1f}%→{roe_cur:.1f}%)"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "roe_cur":   round(roe_cur, 2),
                "roe_prev":  round(roe_prev, 2),
                "roe_delta": round(delta, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_cash_flow_quality — 现金流质量 (Cash Flow Quality)
# ---------------------------------------------------------------------------

def score_cash_flow_quality(
    financial_df: Optional[pd.DataFrame],
) -> dict:
    """现金流质量因子 — operating cash flow backing of reported earnings.

    Logic:
      ratio = operating_CF / net_income (as a percentage)
      A high ratio means earnings are well-supported by actual cash receipts.
      A low or negative ratio is a red flag: profits may be paper-based
      (accruals, channel stuffing, aggressive revenue recognition).

    Uses akshare `经营现金净流量与净利润的比率(%)` column directly when available.
    Falls back to computing from raw CF and net income fields.

    Scoring:
      ratio ≥ 150%  → score 10  (exceptional cash backing)
      ratio 100–150 → score 8
      ratio  80–100 → score 6
      ratio  50–80  → score 4
      ratio  0–50   → score 2
      ratio ≤ 0%    → score 0   (earnings not backed by cash — bearish)
    """
    MAX = 10
    if financial_df is None or financial_df.empty:
        return _neutral(MAX)

    try:
        # 1. Try direct ratio column
        ratio = _extract(
            financial_df,
            ["经营现金净流量与净利润的比率(%)", "经营活动现金流/净利润(%)",
             "CFO/净利润(%)", "经营现金流与净利润比率(%)"],
        )

        # 2. Fallback: compute from raw components
        if ratio is None:
            cf = _extract(financial_df, [
                "经营活动现金流量净额(元)", "经营活动产生的现金流量净额",
                "经营活动现金流净额", "经营现金流量净额",
            ])
            ni = _extract(financial_df, [
                "净利润(元)", "归母净利润(元)", "净利润",
            ])
            if cf is not None and ni is not None and abs(ni) > 1e-6:
                ratio = cf / ni * 100.0
            else:
                return _neutral(MAX)

        if ratio is None:
            return _neutral(MAX)

        # Score: linearly map ratio → [0, 10]
        # Breakpoints: 0% → 0, 100% → 6.67, 150% → 10
        if ratio >= 150:
            score = 10.0
        elif ratio >= 0:
            score = ratio / 150.0 * 10.0
        else:
            # Negative ratio: earnings exceed cash by magnitude
            score = float(np.clip(ratio / 50.0 + 0.0, -10.0, 0.0))
        score      = float(np.clip(score, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if ratio >= 150:
            signal = f"excellent cash backing: CF/NI={ratio:.0f}%"
        elif ratio >= 100:
            signal = f"good cash backing: CF/NI={ratio:.0f}%"
        elif ratio >= 50:
            signal = f"moderate cash backing: CF/NI={ratio:.0f}%"
        elif ratio >= 0:
            signal = f"weak cash backing: CF/NI={ratio:.0f}%"
        else:
            signal = f"negative cash ratio (earnings not backed by cash): CF/NI={ratio:.0f}%"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "cf_ni_ratio": round(ratio, 1),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_main_inflow — 大单净流入 (Main/Institutional Capital Net Inflow)
# ---------------------------------------------------------------------------

def score_main_inflow(
    fund_flow_df: Optional[pd.DataFrame],
) -> dict:
    """大单净流入因子 — institutional/large-order net buying pressure.

    Logic:
      Uses `主力净流入-净占比` (main capital net inflow as % of total turnover),
      averaged over the most recent 5 trading days. Positive = net institutional
      buying; negative = net selling.

      Unlike chip_distribution which uses net 元 amount, this factor uses the
      percentage metric which normalises across stocks with different market caps.

    Scoring:
      net_pct ≥ +5%  → score 10  (strong institutional accumulation)
      net_pct  +2~+5 → score 8
      net_pct  0~+2  → score 6
      net_pct  -2~0  → score 4
      net_pct  -5~-2 → score 2
      net_pct ≤ -5%  → score 0   (heavy institutional distribution)
    """
    MAX = 10
    if fund_flow_df is None or fund_flow_df.empty:
        return _neutral(MAX)

    try:
        # Look for percentage column first, then fall back to raw 净额 normalised
        pct_cols = [c for c in fund_flow_df.columns
                    if "主力净流入" in c and "净占比" in c]
        amt_cols = [c for c in fund_flow_df.columns
                    if any(k in c for k in ["主力净流入-净额", "大单净流入-净额", "超大单净流入-净额"])]

        net_pct: Optional[float] = None

        if pct_cols:
            series = pd.to_numeric(fund_flow_df[pct_cols[0]], errors="coerce").dropna()
            if not series.empty:
                net_pct = float(series.tail(5).mean())

        elif amt_cols:
            # Use raw amount — normalise to a pseudo-percentage via 5d average sign
            series = pd.to_numeric(fund_flow_df[amt_cols[0]], errors="coerce").dropna()
            if not series.empty:
                amt_5d = float(series.tail(5).sum())
                # Scale: ≥ +1e8 yuan → +5%, ≤ -1e8 → -5%
                net_pct = float(np.clip(amt_5d / 2e7, -10.0, 10.0))

        if net_pct is None:
            return _neutral(MAX)

        # Map net_pct to score: neutral at 0%, +/- 5% maps to ±5 score points above/below 5
        score      = float(np.clip(5.0 + net_pct, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - net_pct, 0.0, 10.0))

        if net_pct >= 5:
            signal = f"strong institutional accumulation: net={net_pct:.1f}%"
        elif net_pct >= 2:
            signal = f"moderate institutional buying: net={net_pct:.1f}%"
        elif net_pct >= 0:
            signal = f"mild net inflow: net={net_pct:.1f}%"
        elif net_pct >= -2:
            signal = f"mild net outflow: net={net_pct:.1f}%"
        elif net_pct >= -5:
            signal = f"moderate institutional distribution: net={net_pct:.1f}%"
        else:
            signal = f"heavy institutional distribution: net={net_pct:.1f}%"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":   signal,
                "net_pct_5d_avg": round(net_pct, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ===========================================================================
# BATCH-2 FACTORS — added 2026-04-01
# ===========================================================================

# ---------------------------------------------------------------------------
# score_turnover_acceleration — 换手率加速度 (Turnover Rate Trend)
# ---------------------------------------------------------------------------

def score_turnover_acceleration(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """换手率加速度 — whether market attention/accumulation is accelerating.

    Logic:
      acceleration = avg_turnover(last 5d) / avg_turnover(last 20d)
      A rising ratio means participation is increasing relative to the recent
      baseline, which in A-shares often signals institutional accumulation or
      fresh retail interest.

      Combined with 5-day price direction for confirmation:
        High acceleration + price rising  → strong bullish (放量上涨)
        High acceleration + price falling → strong bearish (放量下跌 / distribution)
        Low acceleration  (turnover drying up) → slightly bearish

    Distinct from score_volume_breakout (absolute volume vs 20d MA) and
    score_turnover_percentile (level vs 90d history):
    this factor captures the *rate of change* in turnover rate (换手率 %).
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    turn_col = next((c for c in ["turnover", "turnover_rate"] if c in price_df.columns), None)
    if turn_col is None:
        return _neutral(MAX)

    try:
        turnover = pd.to_numeric(price_df[turn_col], errors="coerce").dropna()
        close    = pd.to_numeric(price_df["close"],   errors="coerce").dropna()
        if len(turnover) < 22 or len(close) < 6:
            return _neutral(MAX)

        avg_5d  = float(turnover.tail(5).mean())
        avg_20d = float(turnover.tail(20).mean())
        if avg_20d < 1e-10:
            return _neutral(MAX)

        accel = avg_5d / avg_20d           # >1 = accelerating, <1 = decelerating

        # 5-day price direction: +1 rising, -1 falling
        ret_5d = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0.0
        price_dir = 1 if ret_5d > 0.5 else (-1 if ret_5d < -0.5 else 0)

        # Base score from acceleration (neutral=5 at accel=1)
        base = float(np.clip(5.0 + (accel - 1.0) * 4.0, 1.0, 9.0))

        # Direction modifier: amplify signal if acceleration + direction agree
        if price_dir == 1 and accel >= 1.3:
            score = float(np.clip(base + 1.5, 0.0, 10.0))
            signal = f"放量上涨 accel={accel:.2f}x ret5d={ret_5d:+.1f}%"
        elif price_dir == -1 and accel >= 1.3:
            score = float(np.clip(base - 2.0, 0.0, 10.0))
            signal = f"放量下跌(分发) accel={accel:.2f}x ret5d={ret_5d:+.1f}%"
        elif accel < 0.7:
            score = float(np.clip(base - 0.5, 0.0, 10.0))
            signal = f"缩量 accel={accel:.2f}x"
        else:
            score = base
            signal = f"换手正常 accel={accel:.2f}x"

        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":       signal,
                "accel_5d_20d": round(accel, 3),
                "ret_5d_pct":   round(ret_5d, 2),
                "sell_score":   round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_momentum_concavity — 动量加速度 (Momentum Concavity / Acceleration)
# ---------------------------------------------------------------------------

def score_momentum_concavity(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """动量加速度 — whether price momentum is speeding up or slowing down.

    Logic:
      recent_mom  = 10-day return (last  1–10 trading days)
      prior_mom   = 10-day return (last 11–20 trading days)
      concavity   = recent_mom - prior_mom  (percentage points)

      Positive concavity = momentum accelerating   → bullish
      Negative concavity = momentum decelerating   → approaching reversal

    Complements price_inertia (which measures overall 20d direction):
    this factor detects *change in velocity*, catching early trend exhaustion
    or fresh momentum ignition earlier than raw price_inertia.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 22:
            return _neutral(MAX)

        p_now   = float(close.iloc[-1])
        p_10d   = float(close.iloc[-11])   # 10 trading days ago
        p_20d   = float(close.iloc[-21])   # 20 trading days ago

        if p_10d <= 0 or p_20d <= 0:
            return _neutral(MAX)

        recent_mom = (p_now / p_10d - 1) * 100
        prior_mom  = (p_10d / p_20d - 1) * 100
        concavity  = recent_mom - prior_mom      # pp change in velocity

        # Score: neutral=5, ±5pp maps to ±2 score; ±10pp maps to ±4 score
        score      = float(np.clip(5.0 + concavity * 0.35, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - concavity * 0.35, 0.0, 10.0))

        if concavity >= 5:
            signal = f"动量强加速 conc={concavity:+.1f}pp (近10d {recent_mom:+.1f}% vs 前10d {prior_mom:+.1f}%)"
        elif concavity >= 2:
            signal = f"动量加速 conc={concavity:+.1f}pp"
        elif concavity >= -2:
            signal = f"动量平稳 conc={concavity:+.1f}pp"
        elif concavity >= -5:
            signal = f"动量减速 conc={concavity:+.1f}pp — 趋势衰减"
        else:
            signal = f"动量急减速 conc={concavity:+.1f}pp — 可能反转"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":         signal,
                "concavity_pp":   round(concavity, 2),
                "recent_10d_pct": round(recent_mom, 2),
                "prior_10d_pct":  round(prior_mom, 2),
                "sell_score":     round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_bb_squeeze — BOLL带宽收缩 (Bollinger Band Squeeze)
# ---------------------------------------------------------------------------

def score_bb_squeeze(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """BOLL带宽收缩因子 — volatility compression signals an impending breakout.

    Logic:
      BB bandwidth = (upper - lower) / MA20  (normalised band width)
      A low bandwidth relative to recent history = volatility squeeze.
      After a squeeze, the subsequent expansion (breakout) tends to persist.

      Signal is directional: combine squeeze with price position relative to
      the midline (MA20) to determine if the coming breakout is bullish or bearish.

    Two-component score:
      1. Squeeze intensity: current_bw / avg_bw_60d  (low ratio = tight squeeze)
      2. Direction: close vs MA20 (above = bullish, below = bearish)

    Scoring:
      Tight squeeze (ratio ≤ 0.60) + close > MA20  → score 8–9  (bullish coil)
      Tight squeeze (ratio ≤ 0.60) + close < MA20  → score 1–2  (bearish coil)
      Moderate squeeze (0.60–0.80) + above MA20    → score 6–7
      Moderate squeeze (0.60–0.80) + below MA20    → score 3–4
      Wide band (ratio ≥ 1.0) → neutral 5 (already in breakout mode)
    """
    MAX = 10
    if price_df is None or len(price_df) < 30:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 30:
            return _neutral(MAX)

        ma20  = close.rolling(20).mean()
        std20 = close.rolling(20).std(ddof=1)
        upper = ma20 + 2.0 * std20
        lower = ma20 - 2.0 * std20

        # Normalised bandwidth time series
        bw = ((upper - lower) / ma20.replace(0, np.nan)).dropna()
        if len(bw) < 10:
            return _neutral(MAX)

        current_bw = float(bw.iloc[-1])
        avg_bw_60  = float(bw.tail(60).mean()) if len(bw) >= 60 else float(bw.mean())
        if avg_bw_60 < 1e-10:
            return _neutral(MAX)

        squeeze_ratio = current_bw / avg_bw_60   # <1 = tighter than average

        latest_close = float(close.iloc[-1])
        latest_ma20  = float(ma20.iloc[-1])
        above_ma = latest_close > latest_ma20

        # Score based on squeeze intensity × direction
        if squeeze_ratio <= 0.60:
            base_bullish, base_bearish = 8.5, 1.5
        elif squeeze_ratio <= 0.80:
            base_bullish, base_bearish = 7.0, 3.0
        elif squeeze_ratio <= 1.00:
            base_bullish, base_bearish = 6.0, 4.0
        else:
            # Wide band — already post-squeeze, follow trend mildly
            base_bullish, base_bearish = 5.5, 4.5

        score      = float(np.clip(base_bullish if above_ma else base_bearish, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        direction_str = "价格在MA20上方" if above_ma else "价格在MA20下方"
        if squeeze_ratio <= 0.60:
            squeeze_str = f"紧缩(ratio={squeeze_ratio:.2f})"
        elif squeeze_ratio <= 0.80:
            squeeze_str = f"收窄(ratio={squeeze_ratio:.2f})"
        else:
            squeeze_str = f"扩张(ratio={squeeze_ratio:.2f})"

        signal = f"{squeeze_str}, {direction_str}"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":        signal,
                "squeeze_ratio": round(squeeze_ratio, 3),
                "current_bw":    round(current_bw, 4),
                "avg_bw_60d":    round(avg_bw_60, 4),
                "above_ma20":    above_ma,
                "sell_score":    round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_idiosyncratic_vol — 特质波动率 (Idiosyncratic Volatility)
# ---------------------------------------------------------------------------

def score_idiosyncratic_vol(
    price_df: Optional[pd.DataFrame],
    market_price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """特质波动率因子 — idiosyncratic (residual) volatility after removing market beta.

    Logic:
      Regresses daily stock returns against CSI 300 daily returns over 60 days.
      Idiosyncratic vol = annualised std(residuals).

      A-share lottery-stock effect: high idiosyncratic vol stocks are
      systematically overpriced (retail speculation premium) and subsequently
      underperform. Low idiosyncratic vol stocks are underappreciated.

      Score is *inverted*: low idio vol → high score (prefer boring, low-residual stocks).

    Fallback: if market_price_df not provided, uses total volatility as proxy
    (correlates well with idio_vol since market beta is relatively stable).
    """
    MAX = 10
    if price_df is None or len(price_df) < 30:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close      = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        stock_ret  = close.pct_change().dropna()
        N          = min(60, len(stock_ret))
        if N < 20:
            return _neutral(MAX)
        stock_ret_arr = stock_ret.tail(N).values

        if market_price_df is not None and "close" in market_price_df.columns:
            mkt_close = pd.to_numeric(market_price_df["close"], errors="coerce").dropna()
            mkt_ret   = mkt_close.pct_change().dropna()
            M         = min(N, len(mkt_ret))
            if M >= 20:
                s = stock_ret_arr[-M:]
                m = mkt_ret.tail(M).values
                var_m = float(np.var(m, ddof=1))
                if var_m > 1e-12:
                    beta  = float(np.cov(s, m, ddof=1)[0, 1] / var_m)
                    resid = s - beta * m
                    idio_vol = float(np.std(resid, ddof=1)) * np.sqrt(252)
                else:
                    idio_vol = float(np.std(stock_ret_arr, ddof=1)) * np.sqrt(252)
            else:
                idio_vol = float(np.std(stock_ret_arr, ddof=1)) * np.sqrt(252)
        else:
            idio_vol = float(np.std(stock_ret_arr, ddof=1)) * np.sqrt(252)

        # Score inverted: low idio vol = high score
        # A-share range: ~0.20 (large cap) to ~0.65 (small speculative)
        score      = float(np.clip((0.65 - idio_vol) / 0.50 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if idio_vol <= 0.20:
            signal = f"低特质波动率={idio_vol:.2f} — 稳健，被低估"
        elif idio_vol <= 0.35:
            signal = f"中等特质波动率={idio_vol:.2f}"
        elif idio_vol <= 0.50:
            signal = f"较高特质波动率={idio_vol:.2f} — 彩票效应风险"
        else:
            signal = f"高特质波动率={idio_vol:.2f} — 投机溢价，预期跑输"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":       signal,
                "idio_vol_ann": round(idio_vol, 4),
                "n_days_used":  N,
                "sell_score":   round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_gross_margin_trend — 毛利率趋势 (Gross Margin Trend)
# ---------------------------------------------------------------------------

def score_gross_margin_trend(
    financial_df: Optional[pd.DataFrame],
) -> dict:
    """毛利率趋势因子 — direction of gross margin change as competitive moat signal.

    Logic:
      Expanding gross margin = pricing power improving or cost structure
      improving → precedes ROE improvement → bullish.
      Contracting gross margin = competitive pressure or cost inflation → bearish.

    Complements roe_trend (which is net) and cash_flow_quality (which is CF-based):
    gross margin is earlier-stage, reflecting revenue/cost dynamics before
    financing and tax effects.

    Scoring (delta = gm_cur - gm_prev in pp):
      Δ ≥ +3pp  → score 9–10  (material improvement)
      Δ +1~+3pp → score 7–8
      Δ  0~+1pp → score 5–6
      Δ -1~0pp  → score 4–5
      Δ -3~-1pp → score 2–3
      Δ ≤ -3pp  → score 0–1
    """
    MAX = 10
    if financial_df is None or financial_df.empty:
        return _neutral(MAX)

    try:
        gm_cur, gm_prev = _extract_two(
            financial_df,
            ["销售毛利率(%)", "毛利率(%)", "综合毛利率(%)"],
        )

        if gm_cur is None or gm_prev is None:
            if gm_cur is not None:
                score      = float(np.clip(gm_cur / 5.0, 0.0, 10.0))
                sell_score = float(np.clip(10.0 - score, 0.0, 10.0))
                return {
                    "score":      round(score, 1),
                    "sell_score": round(sell_score, 1),
                    "max":        MAX,
                    "details": {
                        "signal": f"单期毛利率={gm_cur:.1f}%",
                        "gm_cur": round(gm_cur, 2), "gm_prev": None,
                        "gm_delta": None, "sell_score": round(sell_score, 1),
                    },
                }
            return _neutral(MAX)

        delta = gm_cur - gm_prev

        score      = float(np.clip(5.0 + delta * 0.7, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - delta * 0.7, 0.0, 10.0))

        if delta >= 3:
            signal = f"毛利率明显改善 +{delta:.1f}pp ({gm_prev:.1f}%→{gm_cur:.1f}%)"
        elif delta >= 1:
            signal = f"毛利率改善 +{delta:.1f}pp"
        elif delta >= -1:
            signal = f"毛利率稳定 {delta:+.1f}pp ({gm_cur:.1f}%)"
        elif delta >= -3:
            signal = f"毛利率收窄 {delta:.1f}pp"
        else:
            signal = f"毛利率明显恶化 {delta:.1f}pp ({gm_prev:.1f}%→{gm_cur:.1f}%)"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":   signal,
                "gm_cur":   round(gm_cur, 2),
                "gm_prev":  round(gm_prev, 2),
                "gm_delta": round(delta, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_ar_quality — 应收账款质量 (Accounts Receivable Quality)
# ---------------------------------------------------------------------------

def score_ar_quality(
    financial_df: Optional[pd.DataFrame],
) -> dict:
    """应收账款质量因子 — AR growing faster than revenue signals earnings inflation.

    Logic:
      ar_spread = AR_growth% - Revenue_growth%
      High positive spread → AR inflating relative to revenue → revenue quality risk → bearish
      Negative spread      → AR shrinking vs revenue → healthy earnings → bullish

    Complements cash_flow_quality: catches earlier-stage receivables inflation
    before it shows up in the cash flow statement.

    Scoring (spread in pp):
      spread ≤ -20pp  → score 10  (AR shrinking: healthy)
      spread -20~0pp  → score 7–9
      spread 0~+20pp  → score 4–6 (mild build-up)
      spread +20~+50pp→ score 2–3 (concerning)
      spread ≥ +50pp  → score 0–1 (serious risk)
    """
    MAX = 10
    if financial_df is None or financial_df.empty:
        return _neutral(MAX)

    try:
        ar_cur, ar_prev = _extract_two(
            financial_df,
            ["应收账款(元)", "应收账款净额(元)", "应收票据及应收账款(元)",
             "应收账款净额", "应收票据及应收账款"],
        )
        rev_cur, rev_prev = _extract_two(
            financial_df,
            ["营业总收入(元)", "营业收入(元)", "总营收(元)", "营业总收入"],
        )

        if (ar_cur is None or ar_prev is None or
                rev_cur is None or rev_prev is None):
            return _neutral(MAX)
        if abs(ar_prev) < 1 or abs(rev_prev) < 1:
            return _neutral(MAX)

        ar_growth  = (ar_cur  - ar_prev)  / abs(ar_prev)  * 100
        rev_growth = (rev_cur - rev_prev) / abs(rev_prev) * 100
        spread     = ar_growth - rev_growth

        # Inverted: lower spread = better quality = higher score
        score      = float(np.clip(6.0 - spread * 0.1, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if spread <= -20:
            signal = f"应收账款质量优(AR收缩): spread={spread:+.0f}pp"
        elif spread <= 0:
            signal = f"应收账款健康: spread={spread:+.0f}pp"
        elif spread <= 20:
            signal = f"应收账款略增: spread={spread:+.0f}pp — 温和风险"
        elif spread <= 50:
            signal = f"应收账款增速远超营收: spread={spread:+.0f}pp — 关注"
        else:
            signal = f"应收账款质量差: spread={spread:+.0f}pp — 收入粉饰风险"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":         signal,
                "ar_growth_pct":  round(ar_growth, 1),
                "rev_growth_pct": round(rev_growth, 1),
                "ar_spread_pp":   round(spread, 1),
                "sell_score":     round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_size_factor — 小市值效应 (Size Factor / Small-Cap Premium)
# ---------------------------------------------------------------------------

def score_size_factor(
    circ_cap: float = 0,
) -> dict:
    """小市值效应 — A-share small-cap premium factor.

    Logic:
      Smaller stocks in A-shares systematically outperform over long horizons.
      Driven by: limited analyst coverage, institutional neglect, higher
      liquidity premium, and speculative rotation cycles.

      Score is a monotonically decreasing function of log(circulating_cap).
      circ_cap is in yuan (元), as returned by akshare realtime quote.

    Scoring (log-linear):
      circ_cap < 3e9  (30亿)  → score 9–10
      circ_cap ≈ 1e10 (100亿) → score 7
      circ_cap ≈ 5e10 (500亿) → score 5
      circ_cap ≈ 2e11 (2000亿)→ score 3
      circ_cap > 1e12 (1万亿) → score 0–1
    """
    MAX = 10
    if circ_cap <= 0:
        return _neutral(MAX)

    try:
        log_size   = np.log10(max(circ_cap, 1e7) / 3e8)
        score      = float(np.clip(10.0 - 2.0 * log_size, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        cap_bn = circ_cap / 1e8   # 亿元 for display
        if cap_bn < 30:
            signal = f"小市值 {cap_bn:.0f}亿 — A股小盘溢价"
        elif cap_bn < 100:
            signal = f"中小市值 {cap_bn:.0f}亿"
        elif cap_bn < 500:
            signal = f"中市值 {cap_bn:.0f}亿"
        elif cap_bn < 2000:
            signal = f"大市值 {cap_bn:.0f}亿 — 覆盖充分，超额收益受限"
        else:
            signal = f"超大市值 {cap_bn:.0f}亿 — 指数化，难以超越"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":      signal,
                "circ_cap_bn": round(cap_bn, 1),
                "sell_score":  round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ===========================================================================
# BATCH-3 FACTORS — added 2026-04-01
# ===========================================================================

# ---------------------------------------------------------------------------
# score_amihud_illiquidity — Amihud非流动性 (Illiquidity Premium)
# ---------------------------------------------------------------------------

def score_amihud_illiquidity(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """Amihud非流动性因子 — illiquidity premium.

    Amihud (2002) illiquidity ratio:
        IL = mean( |r_t| / Amount_t )   over last 60 trading days

    where |r_t| = absolute daily return, Amount_t = daily turnover in yuan.
    High IL → illiquid → earns liquidity risk premium → bullish.

    Economically distinct from low_volatility (return smoothness) and
    size_factor (market cap): isolates actual market-impact cost.

    Scoring (log-linear, typical A-share range 1e-12 to 1e-7):
      IL ~ 1e-12  (mega-liquid, e.g. Moutai)   → score 0–2
      IL ~ 1e-10  (mid-cap average)             → score 5
      IL ~ 1e-8   (small/thin)                  → score 8–10
    """
    MAX = 10
    if price_df is None or len(price_df) < 10:
        return _neutral(MAX)

    if not {"close", "amount"}.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        close  = pd.to_numeric(price_df["close"],  errors="coerce")
        amount = pd.to_numeric(price_df["amount"], errors="coerce")
        ret    = close.pct_change().abs()
        il_ser = (ret / amount.replace(0, np.nan)).dropna()

        N = min(60, len(il_ser))
        if N < 5:
            return _neutral(MAX)

        il_mean = float(il_ser.tail(N).mean())
        if il_mean <= 0:
            return _neutral(MAX)

        # log10(IL * 1e10): range [-2, +4] → score [0, 10]
        log_il = np.log10(il_mean * 1e10)
        score  = float(np.clip((log_il + 2.0) / 6.0 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if il_mean >= 1e-8:
            signal = f"高非流动性 IL={il_mean:.2e} — 流动性溢价显著"
        elif il_mean >= 1e-9:
            signal = f"中等非流动性 IL={il_mean:.2e}"
        elif il_mean >= 1e-10:
            signal = f"流动性一般 IL={il_mean:.2e}"
        else:
            signal = f"高流动性 IL={il_mean:.2e} — 溢价已定价"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "amihud_il": float(f"{il_mean:.4e}"),
                "n_days":    N,
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_medium_term_momentum — 中期动量 (Medium-Term Momentum, 60d skip 20d)
# ---------------------------------------------------------------------------

def score_medium_term_momentum(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """中期动量因子 — 40-day return ending 20 days ago (skip recent month).

    Return window: T-61d → T-21d.
    Skipping last 20 days avoids short-term reversal noise.
    Captures the prior medium-term trend that price_inertia (20d) and
    momentum_concavity (10d/10d) do not cover.

    Scoring:
      +20% → score ~8    0% → 5    -20% → score ~2
    """
    MAX = 10
    if price_df is None or len(price_df) < 65:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 65:
            return _neutral(MAX)

        p_end   = float(close.iloc[-21])   # 20 trading days ago
        p_start = float(close.iloc[-61])   # 60 trading days ago
        if p_start <= 0:
            return _neutral(MAX)

        mom_40d = (p_end / p_start - 1) * 100

        score      = float(np.clip(5.0 + mom_40d * 0.15, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - mom_40d * 0.15, 0.0, 10.0))

        if mom_40d >= 15:
            signal = f"中期强势 mom40d={mom_40d:+.1f}%"
        elif mom_40d >= 5:
            signal = f"中期上涨 mom40d={mom_40d:+.1f}%"
        elif mom_40d >= -5:
            signal = f"中期盘整 mom40d={mom_40d:+.1f}%"
        elif mom_40d >= -15:
            signal = f"中期弱势 mom40d={mom_40d:+.1f}%"
        else:
            signal = f"中期明显下跌 mom40d={mom_40d:+.1f}%"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":      signal,
                "mom_40d_pct": round(mom_40d, 2),
                "sell_score":  round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_obv_trend — OBV趋势 (On Balance Volume Trend Slope)
# ---------------------------------------------------------------------------

def score_obv_trend(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """OBV趋势因子 — On Balance Volume slope as directional accumulation signal.

    OBV[t] = OBV[t-1] + vol if close rises, - vol if close falls.
    Linear regression slope of OBV over last 20 days, normalised by avg volume.

    Positive slope = net accumulation = bullish.
    Distinct from volume_breakout (spike) and main_inflow (order-size based):
    captures *directional* volume persistence without large-order classification.

    Scoring:
      slope_norm ≥ +0.15 → score 8–10  (strong accumulation)
      slope_norm ~ 0     → score 5
      slope_norm ≤ -0.15 → score 0–2   (strong distribution)
    """
    MAX = 10
    if price_df is None or len(price_df) < 25:
        return _neutral(MAX)

    if not {"close", "volume"}.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        close  = pd.to_numeric(price_df["close"],  errors="coerce").ffill()
        volume = pd.to_numeric(price_df["volume"], errors="coerce").fillna(0)

        if len(close) < 25:
            return _neutral(MAX)

        direction = np.sign(close.diff().fillna(0))
        obv       = (direction * volume).cumsum()

        N         = 20
        obv_slice = obv.tail(N).values
        vol_avg   = float(volume.tail(N).mean())
        if vol_avg < 1:
            return _neutral(MAX)

        x       = np.arange(N, dtype=float)
        x_c     = x - x.mean()
        slope   = float(np.dot(x_c, obv_slice) / np.dot(x_c, x_c))
        slope_norm = slope / vol_avg

        score      = float(np.clip(5.0 + slope_norm * 33.0, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - slope_norm * 33.0, 0.0, 10.0))

        if slope_norm >= 0.15:
            signal = f"OBV强势积累 slope={slope_norm:+.3f}"
        elif slope_norm >= 0.03:
            signal = f"OBV温和积累 slope={slope_norm:+.3f}"
        elif slope_norm >= -0.03:
            signal = f"OBV中性 slope={slope_norm:+.3f}"
        elif slope_norm >= -0.15:
            signal = f"OBV温和分发 slope={slope_norm:+.3f}"
        else:
            signal = f"OBV强势分发 slope={slope_norm:+.3f}"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":      signal,
                "slope_norm":  round(slope_norm, 4),
                "vol_avg_20d": round(vol_avg, 0),
                "sell_score":  round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ===========================================================================
# BATCH-4 FACTORS — added 2026-04-01
# ===========================================================================

# ---------------------------------------------------------------------------
# score_market_beta — 市场Beta (Market Sensitivity / Defensive Beta)
# ---------------------------------------------------------------------------

def score_market_beta(
    price_df: Optional[pd.DataFrame],
    market_price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """市场Beta因子 — systematic risk exposure (defensive low-beta premium).

    Beta = cov(stock_ret, market_ret) / var(market_ret) over 60 days.

    Low beta stocks in A-shares outperform in NORMAL/CAUTION regimes because:
      1. Institutional preference for predictable earnings in risk-off
      2. Less crowded by retail speculation
      3. Smaller drawdowns attract more sticky capital

    Score is *inverted*: low beta → high score (prefer defensive names).
    In BULL regime, factor_config.py can neutralise or invert this weight.

    Scoring:
      beta ≤ 0.3  → score 9–10  (ultra-defensive)
      beta ~ 0.7  → score 6
      beta = 1.0  → score 4     (market-neutral)
      beta ~ 1.3  → score 2
      beta ≥ 1.8  → score 0     (high-beta speculative)
    """
    MAX = 10
    if price_df is None or len(price_df) < 30:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close     = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        stock_ret = close.pct_change().dropna()
        N         = min(60, len(stock_ret))
        if N < 20:
            return _neutral(MAX)
        s = stock_ret.tail(N).values

        if market_price_df is not None and "close" in market_price_df.columns:
            mkt_close = pd.to_numeric(market_price_df["close"], errors="coerce").dropna()
            mkt_ret   = mkt_close.pct_change().dropna()
            M         = min(N, len(mkt_ret))
            if M >= 20:
                sv = s[-M:]
                mv = mkt_ret.tail(M).values
                var_m = float(np.var(mv, ddof=1))
                if var_m > 1e-12:
                    beta = float(np.cov(sv, mv, ddof=1)[0, 1] / var_m)
                else:
                    beta = 1.0
            else:
                beta = 1.0
        else:
            # Fallback: estimate beta from total vol ratio (beta ≈ sigma_s / sigma_m)
            # Without market data, use normalised vol as proxy (beta ~ 1 for average stock)
            total_vol = float(np.std(s, ddof=1)) * np.sqrt(252)
            beta = total_vol / 0.20   # 0.20 = typical A-share index annualised vol

        beta = float(np.clip(beta, -0.5, 3.0))

        # Score inversely: beta=0 → 10, beta=1 → ~5.5, beta=2 → ~1
        score      = float(np.clip(10.0 - beta * 4.5, 0.0, 10.0))
        sell_score = float(np.clip(beta * 4.5, 0.0, 10.0))

        if beta <= 0.4:
            signal = f"低Beta={beta:.2f} — 防御型，系统性风险极低"
        elif beta <= 0.7:
            signal = f"偏低Beta={beta:.2f} — 弱周期，回撤相对小"
        elif beta <= 1.1:
            signal = f"中性Beta={beta:.2f} — 随市波动"
        elif beta <= 1.5:
            signal = f"偏高Beta={beta:.2f} — 弹性标的，波动放大"
        else:
            signal = f"高Beta={beta:.2f} — 高弹性投机，风险敞口大"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "beta":      round(beta, 3),
                "n_days":    N,
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_atr_normalized — 归一化ATR (Normalized Average True Range)
# ---------------------------------------------------------------------------

def score_atr_normalized(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """归一化ATR因子 — range-based volatility normalized by price.

    ATR = mean(True Range, 14d)
    True Range = max(High - Low, |High - prev_Close|, |Low - prev_Close|)
    Normalized ATR = ATR / Close

    Unlike close-to-close volatility (used by idiosyncratic_vol and low_volatility),
    ATR incorporates intraday range and gap risk. High ATR/price stocks have:
      - Greater intraday manipulation risk in A-shares
      - Higher bid-ask spread (implicit transaction cost)
      - More speculative retail participation

    Score *inverted*: low ATR/price → high score (prefer calm, stable stocks).

    Scoring:
      ATR/price ≤ 0.015  → score 9–10  (very stable)
      ATR/price ~ 0.025  → score 6
      ATR/price ~ 0.035  → score 3
      ATR/price ≥ 0.050  → score 0     (highly volatile/manipulated)
    """
    MAX = 10
    if price_df is None or len(price_df) < 16:
        return _neutral(MAX)

    required = {"close", "high", "low"}
    if not required.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").ffill()
        high  = pd.to_numeric(price_df["high"],  errors="coerce").ffill()
        low   = pd.to_numeric(price_df["low"],   errors="coerce").ffill()

        if len(close) < 16:
            return _neutral(MAX)

        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)

        atr14 = float(tr.tail(14).mean())
        latest_close = float(close.iloc[-1])
        if latest_close <= 0:
            return _neutral(MAX)

        atr_norm = atr14 / latest_close

        # Invert: low ATR/price = high score
        # score = clip((0.05 - atr_norm) / 0.035 * 10, 0, 10)
        score      = float(np.clip((0.05 - atr_norm) / 0.035 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if atr_norm <= 0.015:
            signal = f"低ATR={atr_norm:.3f} — 盘中稳定，操纵风险低"
        elif atr_norm <= 0.025:
            signal = f"中低ATR={atr_norm:.3f}"
        elif atr_norm <= 0.035:
            signal = f"中等ATR={atr_norm:.3f} — 正常波动区间"
        elif atr_norm <= 0.050:
            signal = f"偏高ATR={atr_norm:.3f} — 振幅较大"
        else:
            signal = f"高ATR={atr_norm:.3f} — 盘中大幅震荡，操纵/炒作风险高"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":   signal,
                "atr_norm": round(atr_norm, 4),
                "atr14":    round(atr14, 3),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# score_ma60_deviation — 长期均线偏离度 (MA60 Deviation / Mean-Reversion Signal)
# ---------------------------------------------------------------------------

def score_ma60_deviation(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """长期均线偏离度因子 — distance from 60-day MA as mean-reversion signal.

    Deviation = (close - MA60) / MA60

    In A-shares, stocks that are significantly above their 60-day MA
    (overbought) tend to revert. Stocks near or below the 60-day MA
    (potential oversold) tend to bounce, especially in NORMAL regimes.

    Consistent with our finding that medium_term_momentum is negative IC:
    A-share mean-reversion dominates at the 1-3 month horizon.

    Score is *contrarian*: high positive deviation → low score (overbought/revert);
    price near/below MA60 → high score (mean-reversion setup).

    Scoring:
      deviation ≤ -0.10  (far below MA60, oversold)   → score 8–9
      deviation -0.05~0  (just below MA60)             → score 6–7
      deviation 0~+0.05  (just above MA60)             → score 5
      deviation +0.10    (10% above MA60, extended)    → score 3
      deviation ≥ +0.20  (far above MA60, overbought)  → score 0–1
    """
    MAX = 10
    if price_df is None or len(price_df) < 65:
        return _neutral(MAX)

    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 65:
            return _neutral(MAX)

        ma60         = float(close.tail(60).mean())
        latest_close = float(close.iloc[-1])
        if ma60 <= 0:
            return _neutral(MAX)

        deviation = (latest_close - ma60) / ma60

        # Contrarian score: score = 5 - deviation * 20, clipped [0, 10]
        # deviation = -0.25 → score 10, deviation = 0 → 5, deviation = +0.25 → 0
        score      = float(np.clip(5.0 - deviation * 20.0, 0.0, 10.0))
        sell_score = float(np.clip(5.0 + deviation * 20.0, 0.0, 10.0))


        pct = deviation * 100
        if deviation <= -0.10:
            signal = f"大幅低于MA60 {pct:+.1f}% — 均值回归机会"
        elif deviation <= -0.03:
            signal = f"略低于MA60 {pct:+.1f}% — 支撑区"
        elif deviation <= +0.05:
            signal = f"贴近MA60 {pct:+.1f}% — 中性"
        elif deviation <= +0.15:
            signal = f"高于MA60 {pct:+.1f}% — 短线偏贵"
        else:
            signal = f"大幅高于MA60 {pct:+.1f}% — 均值回归风险高"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "deviation":  round(deviation, 4),
                "close":      round(latest_close, 2),
                "ma60":       round(ma60, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# Batch 5 — Distribution & momentum-quality factors (2026-04-01)
# ---------------------------------------------------------------------------

def score_max_return(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """MAX effect — maximum single-day return over past 20 days.

    Bali, Cakici & Whitelaw (2011): stocks with extreme positive daily returns
    are overpriced by lottery-seeking investors and subsequently underperform.
    A-share lottery effect is especially strong given high retail participation.

    Score is *inverted*: high MAX → low score (lottery stock, expect reversion).

    Scoring (inverted):
      MAX ≤ 1%   (no extreme moves, stable)   → score 8–10
      MAX 1–3%   (modest peak, normal range)  → score 6–7
      MAX 3–5%   (noticeable spike)           → score 4–5
      MAX 5–8%   (one big gap-up / limit hit) → score 2–3
      MAX ≥ 10%  (limit-up / extreme spike)   → score 0–1
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 22:
            return _neutral(MAX)

        rets_20 = close.pct_change().dropna().tail(20)
        if len(rets_20) < 10:
            return _neutral(MAX)

        max_ret = float(rets_20.max()) * 100  # in percent

        # Inverted score: score = 10 - max_ret * 1.2, clipped [0, 10]
        # max_ret = 0% → 10, max_ret = 5% → ~4, max_ret = 9% → ~0
        score      = float(np.clip(10.0 - max_ret * 1.2, 0.0, 10.0))
        sell_score = float(np.clip(max_ret * 1.2, 0.0, 10.0))

        if max_ret <= 1.0:
            signal = "stable — no extreme moves (low lottery risk)"
        elif max_ret <= 3.0:
            signal = f"modest peak {max_ret:.1f}% — normal range"
        elif max_ret <= 5.0:
            signal = f"noticeable spike {max_ret:.1f}% — mild lottery risk"
        elif max_ret <= 8.0:
            signal = f"large spike {max_ret:.1f}% — elevated lottery overpricing"
        else:
            signal = f"extreme spike {max_ret:.1f}% — strong lottery effect, expect underperformance"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "max_ret_pct": round(max_ret, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_return_skewness(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """Return skewness — 60-day distribution shape as lottery-stock proxy.

    Positive skewness = asymmetric right-tail (lottery-like returns).
    Academic evidence: positive-skew stocks are overpriced by investors who
    prefer right-tail exposure; they subsequently underperform (Harvey & Siddique 2000).
    Related to MAX effect but captures overall distribution shape, not just peak.

    Score is *inverted*: high positive skewness → low score.

    Scoring (inverted):
      skew ≤ -0.5  (left-skewed, no lottery appeal)  → score 8–9
      skew -0.5~0  (slightly left / symmetric)        → score 6–7
      skew 0~+0.5  (slightly positive)               → score 5
      skew +0.5~+1 (moderately lottery-like)         → score 3–4
      skew ≥ +1.5  (strongly lottery-like)           → score 0–1
    """
    MAX = 10
    if price_df is None or len(price_df) < 65:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 25:
            return _neutral(MAX)

        rets = close.pct_change().dropna().tail(60)
        if len(rets) < 20:
            return _neutral(MAX)

        skew = float(rets.skew())

        # Inverted score: score = 5 - skew * 2.5, clipped [0, 10]
        # skew = -2 → 10, skew = 0 → 5, skew = +2 → 0
        score      = float(np.clip(5.0 - skew * 2.5, 0.0, 10.0))
        sell_score = float(np.clip(5.0 + skew * 2.5, 0.0, 10.0))

        if skew <= -0.5:
            signal = f"left-skewed ({skew:.2f}) — no lottery appeal, stable distribution"
        elif skew <= 0.0:
            signal = f"slightly left/symmetric ({skew:.2f}) — low lottery risk"
        elif skew <= 0.5:
            signal = f"slightly positive ({skew:.2f}) — mild lottery characteristics"
        elif skew <= 1.5:
            signal = f"positive skew ({skew:.2f}) — lottery-like, overpricing risk"
        else:
            signal = f"high positive skew ({skew:.2f}) — strong lottery premium, expect underperformance"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "skewness":  round(skew, 3),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_upday_ratio(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """Up-day ratio — fraction of positive-return days over past 20 days.

    Measures momentum *consistency* rather than magnitude. A stock rising 8%
    over 20 days but with only 6 up-days is less stable than one with 14 up-days.
    High up-day ratio = persistent buying pressure; low ratio = churn / noise.

    Complementary to price_inertia (which captures magnitude).
    IC direction expected positive: consistent uptrends continue in short-horizon.

    Scoring:
      ratio ≥ 0.70  (≥14/20 days up)         → score 8–9
      ratio 0.55–0.70 (moderate consistency) → score 6–7
      ratio 0.45–0.55 (balanced/noisy)       → score 5
      ratio 0.30–0.45 (more down than up)    → score 3–4
      ratio ≤ 0.30   (persistent selling)    → score 0–2
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 22:
            return _neutral(MAX)

        rets_20 = close.pct_change().dropna().tail(20)
        if len(rets_20) < 10:
            return _neutral(MAX)

        ratio = float((rets_20 > 0).sum()) / len(rets_20)

        # score = (ratio - 0.5) * 20 + 5, clipped [0, 10]
        # ratio = 0.0 → -5 → 0, ratio = 0.5 → 5, ratio = 1.0 → 15 → 10
        score      = float(np.clip((ratio - 0.5) * 20.0 + 5.0, 0.0, 10.0))
        sell_score = float(np.clip((0.5 - ratio) * 20.0 + 5.0, 0.0, 10.0))

        pct = ratio * 100
        if ratio >= 0.70:
            signal = f"highly consistent ({pct:.0f}% up-days) — persistent buying pressure"
        elif ratio >= 0.55:
            signal = f"moderate consistency ({pct:.0f}% up-days)"
        elif ratio >= 0.45:
            signal = f"balanced ({pct:.0f}% up-days) — no directional bias"
        elif ratio >= 0.30:
            signal = f"more down than up ({pct:.0f}% up-days) — selling pressure"
        else:
            signal = f"persistent selling ({pct:.0f}% up-days) — strong downtrend"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":      signal,
                "upday_ratio": round(ratio, 3),
                "up_days":     int((rets_20 > 0).sum()),
                "total_days":  len(rets_20),
                "sell_score":  round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# Batch 6 — Momentum-quality & breakout factors (2026-04-01)
# ---------------------------------------------------------------------------

def score_volume_expansion(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """成交量扩张因子 — volume trend as accumulation/distribution signal.

    Ratio of recent 10-day average volume to 60-day average volume.
    Rising volume alongside price = institutional accumulation (bullish).
    Shrinking volume = distribution or loss of interest.

    Unlike `volume` (absolute turnover level), this captures directional change.
    Especially powerful in bull markets combined with price momentum.
    """
    MAX = 10
    if price_df is None or len(price_df) < 65:
        return _neutral(MAX)

    vol_col = None
    for c in ["volume", "成交量", "vol", "turnover", "换手率", "turnover_rate"]:
        if c in price_df.columns:
            vol_col = c
            break
    if vol_col is None:
        return _neutral(MAX)

    try:
        vol = pd.to_numeric(price_df[vol_col], errors="coerce").dropna()
        if len(vol) < 65:
            return _neutral(MAX)

        avg_10 = float(vol.tail(10).mean())
        avg_60 = float(vol.tail(60).mean())
        if avg_60 <= 0:
            return _neutral(MAX)

        ratio = avg_10 / avg_60

        # score = (ratio - 1.0) * 6.67 + 5, clipped [0, 10]
        # ratio=0.25→0, ratio=1.0→5, ratio=1.75→10
        score      = float(np.clip((ratio - 1.0) * 6.67 + 5.0, 0.0, 10.0))
        sell_score = float(np.clip((1.0 - ratio) * 6.67 + 5.0, 0.0, 10.0))

        if ratio >= 1.8:
            signal = f"volume surging ({ratio:.2f}×) — strong accumulation"
        elif ratio >= 1.3:
            signal = f"volume expanding ({ratio:.2f}×) — buying interest growing"
        elif ratio >= 0.8:
            signal = f"volume flat ({ratio:.2f}×) — neutral"
        elif ratio >= 0.5:
            signal = f"volume contracting ({ratio:.2f}×) — interest fading"
        else:
            signal = f"volume drying up ({ratio:.2f}×) — distribution"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "vol_ratio": round(ratio, 3),
                "avg_10d":   round(avg_10, 0),
                "avg_60d":   round(avg_60, 0),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_nearness_to_high(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """近期高点接近度 — proximity to 20-day high as breakout momentum signal.

    Ratio of current close to highest close in past 20 trading days.
    Near-high = strong short-term momentum, less overhead resistance.
    In A-shares, retail FOMO and index flows push breakout stocks further.

    Distinct from position_52w (excluded as noise) — 20-day horizon is
    tighter and captures recent momentum structure, not value positioning.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 22:
            return _neutral(MAX)

        current = float(close.iloc[-1])
        high_20 = float(close.tail(20).max())
        if high_20 <= 0:
            return _neutral(MAX)

        ratio = current / high_20

        # score = (ratio - 0.75) / 0.25 * 10, clipped [0, 10]
        # ratio=0.75→0, ratio=1.0→10
        score      = float(np.clip((ratio - 0.75) / 0.25 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip((1.0 - ratio) / 0.25 * 10.0, 0.0, 10.0))

        pct_below = (1 - ratio) * 100
        if ratio >= 0.98:
            signal = f"at 20d high ({pct_below:.1f}% below) — breakout zone"
        elif ratio >= 0.95:
            signal = f"near 20d high ({pct_below:.1f}% below) — strong momentum"
        elif ratio >= 0.90:
            signal = f"moderate pullback ({pct_below:.1f}% below 20d high)"
        elif ratio >= 0.80:
            signal = f"significant pullback ({pct_below:.1f}% below 20d high)"
        else:
            signal = f"far from high ({pct_below:.1f}% below) — weak momentum"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":         signal,
                "ratio_to_high":  round(ratio, 4),
                "current_close":  round(current, 2),
                "high_20d":       round(high_20, 2),
                "pct_below_high": round(pct_below, 2),
                "sell_score":     round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ---------------------------------------------------------------------------
# Batch 7 — Price-volume interaction & trend quality factors (2026-04-01)
# ---------------------------------------------------------------------------

def score_price_volume_corr(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """价量关系质量 — Spearman correlation between |return| and volume over 20d.

    Measures whether price moves are confirmed by volume.
    High positive correlation = volume-backed moves (institutional accumulation,
    momentum quality). Low/negative = price moves on thin volume (unreliable).

    Distinct from `volume` (absolute level), `volume_expansion` (trend),
    and `obv_trend` (directional). This measures the *consistency* of the
    volume-price relationship.

    In A-shares, moves confirmed by volume tend to continue; low-volume
    breakouts/breakdowns typically reverse within days.

    Score: high positive correlation → high score.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    vol_col = None
    for c in ["volume", "成交量", "vol", "turnover", "换手率"]:
        if c in price_df.columns:
            vol_col = c
            break
    if vol_col is None:
        return _neutral(MAX)

    try:
        from scipy.stats import spearmanr as _spearmanr
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        vol   = pd.to_numeric(price_df[vol_col], errors="coerce")

        # Align
        df = pd.DataFrame({"close": close, "vol": vol}).dropna().tail(21)
        if len(df) < 10:
            return _neutral(MAX)

        abs_ret = df["close"].pct_change().abs().dropna()
        v       = df["vol"].iloc[1:].reset_index(drop=True)
        abs_ret = abs_ret.reset_index(drop=True)

        if len(abs_ret) < 8:
            return _neutral(MAX)

        corr, _ = _spearmanr(abs_ret, v)
        if np.isnan(corr):
            return _neutral(MAX)

        # score = corr * 5 + 5, clipped [0, 10]
        # corr=-1→0, corr=0→5, corr=+1→10
        score      = float(np.clip(corr * 5.0 + 5.0, 0.0, 10.0))
        sell_score = float(np.clip(-corr * 5.0 + 5.0, 0.0, 10.0))

        if corr >= 0.4:
            signal = f"strong volume-price confirmation (r={corr:.2f}) — moves are reliable"
        elif corr >= 0.1:
            signal = f"moderate confirmation (r={corr:.2f})"
        elif corr >= -0.1:
            signal = f"no relationship (r={corr:.2f}) — volume uninformative"
        else:
            signal = f"volume-price divergence (r={corr:.2f}) — moves unreliable, reversal risk"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "pv_corr":   round(float(corr), 3),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_trend_linearity(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """趋势线性度 — R² × direction of OLS fit on close over 20 days.

    Measures how orderly the price trend is. A steady, linear uptrend scores
    high; a volatile or sideways stock scores low; a linear downtrend scores
    negative (inverted).

    Distinct from price_inertia (magnitude of return) and momentum_concavity
    (acceleration). This captures *consistency* of the trend — institutional
    accumulation typically produces clean linear trends; retail chasing
    produces jagged, volatile price action.

    Score: high R² with upward slope → high score; high R² downward → low score.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna().tail(20)
        if len(close) < 10:
            return _neutral(MAX)

        x = np.arange(len(close), dtype=float)
        y = close.values.astype(float)

        # OLS
        x_mean, y_mean = x.mean(), y.mean()
        slope = float(np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean) ** 2))
        y_hat = slope * x + (y_mean - slope * x_mean)
        ss_res = float(np.sum((y - y_hat) ** 2))
        ss_tot = float(np.sum((y - y_mean) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        direction = 1.0 if slope >= 0 else -1.0
        # signed_r2 in [-1, +1]: +1 = perfect uptrend, -1 = perfect downtrend
        signed_r2 = float(r2 * direction)

        # score = signed_r2 * 5 + 5, clipped [0, 10]
        score      = float(np.clip(signed_r2 * 5.0 + 5.0, 0.0, 10.0))
        sell_score = float(np.clip(-signed_r2 * 5.0 + 5.0, 0.0, 10.0))

        if signed_r2 >= 0.6:
            signal = f"clean uptrend (R²={r2:.2f}, slope+) — institutional-quality trend"
        elif signed_r2 >= 0.2:
            signal = f"moderate uptrend (R²={r2:.2f})"
        elif signed_r2 >= -0.2:
            signal = f"sideways/noisy (R²={r2:.2f}) — no clear trend"
        elif signed_r2 >= -0.6:
            signal = f"moderate downtrend (R²={r2:.2f}, slope-)"
        else:
            signal = f"clean downtrend (R²={r2:.2f}, slope-) — persistent selling"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "r2":        round(r2, 3),
                "slope":     round(float(slope), 4),
                "signed_r2": round(signed_r2, 3),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_gap_frequency(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """跳空频率 — fraction of significant overnight gaps in past 20 days (inverted).

    Measures how often the stock gaps significantly at open vs prior close.
    A significant gap is defined as |open - prev_close| / prev_close > 0.5%.

    High gap frequency = news-driven, unpredictable, high tail risk.
    Low gap frequency = steady, predictable price action (institutional flow).

    Distinct from ATR (which includes intraday range) — gaps capture
    *overnight* risk specifically. Stocks that frequently gap are harder
    to hold and tend to underperform on risk-adjusted basis.

    Score is *inverted*: high gap frequency → low score.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    open_col = None
    for c in ["open", "开盘", "open_price"]:
        if c in price_df.columns:
            open_col = c
            break
    if open_col is None or "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        opn   = pd.to_numeric(price_df[open_col], errors="coerce")

        df = pd.DataFrame({"close": close, "open": opn}).dropna().tail(21)
        if len(df) < 10:
            return _neutral(MAX)

        prev_close = df["close"].shift(1).dropna()
        curr_open  = df["open"].iloc[1:].reset_index(drop=True)
        prev_close = prev_close.reset_index(drop=True)

        gap_ratio = ((curr_open - prev_close) / prev_close).abs()
        gap_freq  = float((gap_ratio > 0.005).mean())  # >0.5% = significant gap

        # Inverted score: score = (1 - gap_freq) * 10, clipped [0, 10]
        # gap_freq=0→10, gap_freq=0.5→5, gap_freq=1.0→0
        score      = float(np.clip((1.0 - gap_freq) * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(gap_freq * 10.0, 0.0, 10.0))

        pct = gap_freq * 100
        if gap_freq <= 0.1:
            signal = f"very low gap frequency ({pct:.0f}%) — stable, predictable"
        elif gap_freq <= 0.25:
            signal = f"low gap frequency ({pct:.0f}%) — mostly steady"
        elif gap_freq <= 0.5:
            signal = f"moderate gaps ({pct:.0f}%) — some news sensitivity"
        elif gap_freq <= 0.7:
            signal = f"high gap frequency ({pct:.0f}%) — news-driven, hard to hold"
        else:
            signal = f"very high gap frequency ({pct:.0f}%) — extreme tail risk"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "gap_freq":  round(gap_freq, 3),
                "gap_pct":   round(pct, 1),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


# ============================================================================
# BATCH 8 — Three new technical factors
# ============================================================================

def score_market_relative_strength(
    price_df: Optional[pd.DataFrame],
    market_price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """相对强弱因子 — stock 20d return minus CSI300 20d return.

    Stocks that outperform the market on a rolling 20d basis tend to
    continue outperforming (relative momentum). Distinct from absolute
    momentum (price_inertia) — controls for market-wide moves.

    Score: linear map excess_return in [-15%, +15%] -> [0, 10].
    Center at 0% excess = score 5.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 22:
            return _neutral(MAX)

        stock_ret = float(close.iloc[-1] / close.iloc[-21] - 1) * 100  # pct

        mkt_ret = 0.0
        if market_price_df is not None and "close" in market_price_df.columns:
            mkt_close = pd.to_numeric(market_price_df["close"], errors="coerce").dropna()
            if len(mkt_close) >= 21:
                mkt_ret = float(mkt_close.iloc[-1] / mkt_close.iloc[-21] - 1) * 100

        excess = stock_ret - mkt_ret

        # Map excess in [-15%, +15%] -> [0, 10]; center 0% -> 5
        score      = float(np.clip((excess + 15.0) / 30.0 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if excess >= 10:
            signal = f"strong market leader (+{excess:.1f}%)"
        elif excess >= 3:
            signal = f"market outperformer (+{excess:.1f}%)"
        elif excess >= -3:
            signal = f"in line with market ({excess:+.1f}%)"
        elif excess >= -10:
            signal = f"market underperformer ({excess:.1f}%)"
        else:
            signal = f"sharp underperformer ({excess:.1f}%) — potential laggard"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "stock_ret":  round(stock_ret, 2),
                "mkt_ret":    round(mkt_ret, 2),
                "excess_ret": round(excess, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_price_efficiency(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """价格效率因子 (Kaufman效率比率) — directional efficiency of price movement.

    Kaufman Efficiency Ratio (ER) = |net_price_change| / sum(|daily_changes|)
    over a rolling 20-day window.

    ER = 1.0: price moved perfectly directionally (straight line).
    ER -> 0:  price is fully random / choppy (path cancels out).

    High ER: clean trending move (institutional accumulation).
    Low ER: noisy / whipsawing (retail-dominated or indecisive).

    Score: ER in [0, 1] -> [0, 10].
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna().tail(21)
        if len(close) < 10:
            return _neutral(MAX)

        daily_changes = close.diff().dropna().abs()
        net_change    = abs(float(close.iloc[-1] - close.iloc[0]))
        total_path    = float(daily_changes.sum())

        if total_path < 1e-8:
            return _neutral(MAX)

        er = net_change / total_path  # Kaufman ER in [0, 1]

        score      = float(np.clip(er * 10.0, 0.0, 10.0))
        sell_score = float(np.clip((1.0 - er) * 10.0, 0.0, 10.0))

        if er >= 0.7:
            signal = f"very efficient trend (ER={er:.2f}) — clean directional move"
        elif er >= 0.5:
            signal = f"efficient (ER={er:.2f}) — mostly directional"
        elif er >= 0.3:
            signal = f"moderate efficiency (ER={er:.2f}) — some noise"
        elif er >= 0.15:
            signal = f"low efficiency (ER={er:.2f}) — choppy/sideways"
        else:
            signal = f"very low efficiency (ER={er:.2f}) — random / indecisive"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "er":         round(er, 3),
                "net_change": round(net_change, 3),
                "total_path": round(total_path, 3),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_intraday_vs_overnight(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """日内vs隔夜收益分拆因子 — institutional (intraday) vs retail (overnight) signal.

    Decomposes total return into:
      - Intraday:   (close - open) / open       -> institutional activity proxy
      - Overnight:  (open - prev_close) / prev_close -> retail/news reaction proxy

    Net signal = avg_intraday - avg_overnight over 20 days.

    Positive net: institutions buying intraday while retail gaps fade -> bullish.
    Negative net: retail gaps up then institutions distribute -> bearish distribution.

    Score: net in [-1.5%, +1.5%] -> [0, 10]; center 0% -> 5.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    open_col = None
    for c in ["open", "\u5f00\u76d8", "open_price"]:
        if c in price_df.columns:
            open_col = c
            break
    if open_col is None or "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce")
        opn   = pd.to_numeric(price_df[open_col], errors="coerce")

        df = pd.DataFrame({"close": close, "open": opn}).dropna().tail(21)
        if len(df) < 10:
            return _neutral(MAX)

        prev_close = df["close"].shift(1).dropna()
        curr_open  = df["open"].iloc[1:].reset_index(drop=True)
        curr_close = df["close"].iloc[1:].reset_index(drop=True)
        prev_close = prev_close.reset_index(drop=True)

        intraday_ret  = (curr_close - curr_open) / curr_open.replace(0, np.nan)
        overnight_ret = (curr_open - prev_close) / prev_close.replace(0, np.nan)

        avg_intraday  = float(intraday_ret.dropna().mean()) * 100   # pct
        avg_overnight = float(overnight_ret.dropna().mean()) * 100  # pct
        net           = avg_intraday - avg_overnight  # pct

        # Map net in [-1.5%, +1.5%] -> [0, 10]
        score      = float(np.clip((net + 1.5) / 3.0 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if net >= 0.5:
            signal = f"institutional accumulation (net={net:+.2f}%): intraday buying > overnight gap"
        elif net >= 0.1:
            signal = f"mild institutional bias (net={net:+.2f}%)"
        elif net >= -0.1:
            signal = f"balanced intraday/overnight (net={net:+.2f}%) — neutral"
        elif net >= -0.5:
            signal = f"mild distribution signal (net={net:+.2f}%)"
        else:
            signal = f"distribution pattern (net={net:+.2f}%): retail gaps, institutions sell"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":        signal,
                "avg_intraday":  round(avg_intraday, 3),
                "avg_overnight": round(avg_overnight, 3),
                "net":           round(net, 3),
                "sell_score":    round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)
