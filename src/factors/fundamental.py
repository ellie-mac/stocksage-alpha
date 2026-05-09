from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from ._utils import _extract, _extract_two, _neutral, _get_price_position


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

    net_income   = _extract(financial_df, [
        "净利润(元)", "归母净利润(元)", "扣除非经常性损益后的净利润(元)", "净利润",
    ])
    op_cf        = _extract(financial_df, [
        "经营活动现金流量净额(元)", "经营活动产生的现金流量净额",
        "经营现金流净额", "经营活动净现金流量",
    ])
    total_assets = _extract(financial_df, ["总资产(元)", "资产总计(元)", "资产总额"])

    _cf_ratio = _extract(financial_df, [
        "经营现金净流量与净利润的比率(%)", "经营活动净现金流/营业收入",
        "现金流量比率(%)", "现金流量比率",
    ])
    if op_cf is None and net_income is not None and _cf_ratio is not None:
        op_cf = net_income * _cf_ratio / 100.0

    if net_income is None or op_cf is None:
        if _cf_ratio is None:
            return _neutral(10)
        accruals_pct = -(_cf_ratio - 100.0) / 4.0  # ratio=120 -> -5, ratio=80 -> +5
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
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


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

