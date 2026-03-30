"""
Multi-factor scoring module.

Default dimensions and max points (before weight adjustment):
  value        : 25 pts  — intra-industry PE/PB percentile (falls back to absolute)
  growth       : 25 pts  — revenue growth, profit growth, ROE trend
  momentum     : 25 pts  — price return over 1 / 3 / 6 months
  quality      : 25 pts  — ROE level, gross margin, debt ratio
  (extended)
  northbound   : 10 pts  — institutional / large-order net inflow trend
  volume       : 10 pts  — volume breakout vs 20-day average
  position_52w :  5 pts  — price position in 52-week range
  div_yield    : 10 pts  — dividend yield TTM (股息率)
  volume_ratio : 10 pts  — 量比: today's volume / 5-day average
  ma_alignment : 15 pts  — MA5/10/20/60 bullish alignment
  low_volatility: 10 pts — inverse annualized return volatility

Final score = weighted sum of (factor_score / factor_max), scaled to 100.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configurable weights
# ---------------------------------------------------------------------------

@dataclass
class FactorWeights:
    """
    Multiplicative weights for each factor dimension.
    Set a weight to 0 to exclude a factor; use >1 to emphasise it.

    Core (25 pts each): value, growth, momentum, quality
    Extended-A (5-15 pts, from already-fetched data): all default 0.3-0.5
    Extended-B (10 pts, require extra API calls): all default 0.2-0.3
    """
    # ── Core factors (IC-calibrated weights) ───────────────────────────────
    # Weights reflect A-share factor IC from quant research literature.
    # IC source: CITIC/Guotai Junan/Haitong quant reports 2020-2024.
    value:          float = 0.5   # IC~0.030, weak-moderate; industry-relative PE/PB
    growth:         float = 0.5   # IC~0.027, weak; growth priced in quickly in A-shares
    momentum:       float = 1.0   # IC~0.038, moderate; 3-6m works, 1m is reversal
    quality:        float = 0.5   # IC~0.028, weak-moderate; ROE+margin composite
    # ── Ext-A: from already-fetched data ───────────────────────────────────
    northbound:          float = 0.1   # IC~0.009, minimal; proxy, not real NB data
    volume:              float = 0.2   # IC~0.012, weak; noisy standalone
    position_52w:        float = 0.2   # IC~0.014, weak; overlaps with momentum
    div_yield:           float = 0.2   # IC~0.013, weak; yield less relevant in A-shares
    volume_ratio:        float = 0.2   # IC~0.018, weak; noisy standalone
    ma_alignment:        float = 0.5   # IC~0.035, weak-moderate; trend-following works
    low_volatility:      float = 1.0   # IC~0.045, moderate; low-vol anomaly proven globally
    reversal:            float = 2.0   # IC~0.070, STRONG; retail overreaction in A-shares
    accruals:            float = 0.5   # IC~0.032, weak-moderate; cash-backed earnings
    asset_growth:        float = 0.0   # IC~-0.005, INVERTED in A-shares; zeroed out
    piotroski:           float = 1.0   # IC~0.048, moderate; 9-signal composite
    short_interest:      float = 0.2   # IC~0.020, weak; limited short-selling coverage
    rsi_signal:          float = 0.2   # IC~0.016, weak; overlaps with reversal
    macd_signal:         float = 0.2   # IC~0.015, weak; lag indicator
    turnover_percentile: float = 0.5   # IC~0.022, weak; attention / volume activity
    chip_distribution:   float = 1.5   # IC~0.055 est.; 筹码分布 cross-interaction (position x flow)
    limit_hits:          float = 0.3   # IC~0.022 est.; 涨跌停板 activity signal
    price_inertia:       float = 0.4   # IC~0.020 est.; consecutive day continuation
    # ── Ext-B: require additional API calls ────────────────────────────────
    shareholder_change:   float = 2.0   # IC~0.065, STRONG; A-share 筹码集中 signal
    lhb:                  float = 0.0   # IC~-0.008, INVERTED; LHB marks tops; zeroed out
    lockup_pressure:      float = 0.1   # IC~0.010, minimal; often already priced in
    insider:              float = 1.0   # IC~0.042, moderate; insider alignment signal
    institutional_visits: float = 0.1   # IC~0.006, minimal; lags actual fund positions
    industry_momentum:    float = 0.5   # IC~0.025, weak; sector rotation
    northbound_actual:    float = 0.5   # IC~0.024, weak; real NB holdings, slow signal
    earnings_revision:    float = 1.0   # IC~0.040, moderate; analyst upgrade momentum
    social_heat:         float = 0.2   # IC~0.015 est.; forum discussion heat (contrarian proxy)
    market_regime:       float = 0.8   # IC~0.035 est.; CSI 300 MA alignment (market context)
    concept_momentum:    float = 0.8   # IC~0.035 est.; concept/theme board momentum (板块热点)

    def total(self) -> float:
        from dataclasses import fields as dc_fields
        return sum(getattr(self, f.name) for f in dc_fields(self))


DEFAULT_WEIGHTS = FactorWeights()

# Weight presets parsed from natural language
_WEIGHT_PRESETS: list[tuple[list[str], dict]] = [
    # Emphasise growth
    (["只看成长", "focus on growth", "growth only", "重视成长", "emphasize growth"],
     {"growth": 3.0, "value": 0.3, "momentum": 0.5, "quality": 0.5}),
    # Emphasise value
    (["只看估值", "focus on value", "value only", "重视估值", "emphasize value"],
     {"value": 3.0, "growth": 0.3, "momentum": 0.5, "quality": 0.5}),
    # Emphasise quality
    (["只看质量", "focus on quality", "quality only", "重视质量"],
     {"quality": 3.0, "value": 0.5, "growth": 0.5, "momentum": 0.3}),
    # Emphasise momentum / trend
    (["只看趋势", "focus on momentum", "momentum only", "重视动量", "趋势优先"],
     {"momentum": 3.0, "value": 0.3, "growth": 0.5, "quality": 0.5}),
    # Ignore valuation
    (["不看估值", "ignore value", "ignore valuation", "忽略估值"],
     {"value": 0.0}),
    # Ignore momentum
    (["不看趋势", "ignore momentum", "忽略动量"],
     {"momentum": 0.0}),
    # Include institutional flow strongly
    (["北向资金", "northbound", "机构资金", "smart money"],
     {"northbound": 2.0}),
    # Emphasise volume signal
    (["量能", "volume breakout", "放量"],
     {"volume": 2.0}),
    # Emphasise dividend / income
    (["高股息", "dividend", "高收益", "income"],
     {"div_yield": 3.0, "value": 1.5}),
    # Emphasise trend / MA alignment
    (["均线", "ma alignment", "趋势", "trend following"],
     {"ma_alignment": 2.0, "momentum": 1.5}),
    # Emphasise low volatility / defensive
    (["低波动", "low volatility", "稳健", "defensive"],
     {"low_volatility": 3.0, "quality": 1.5, "momentum": 0.3}),
    # Emphasise volume ratio / active trading
    (["量比", "volume ratio", "活跃", "active"],
     {"volume_ratio": 2.0, "volume": 1.5}),
    # Contrarian / reversal
    (["反转", "reversal", "超跌", "contrarian"],
     {"reversal": 3.0, "momentum": 0.2}),
    # Cash quality / earnings quality
    (["现金流", "cash flow", "应计", "accruals", "盈利质量"],
     {"accruals": 2.0, "quality": 1.5}),
    # Piotroski / fundamental improvement
    (["f-score", "piotroski", "基本面改善", "fundamental"],
     {"piotroski": 3.0, "quality": 1.5, "growth": 1.5}),
    # Insider buy
    (["内部增持", "insider buy", "大股东增持"],
     {"insider": 3.0, "northbound_actual": 1.5}),
    # Chip distribution (A-share specific)
    (["筹码", "chip distribution", "筹码分布", "panic bottom", "底部恐慌"],
     {"chip_distribution": 3.0, "shareholder_change": 1.5}),
    # Shareholder concentration
    (["股东减少", "shareholder concentration", "筹码集中"],
     {"shareholder_change": 3.0}),
    # Industry momentum
    (["行业景气", "industry momentum", "行业强势"],
     {"industry_momentum": 2.0}),
    # Earnings revision
    (["分析师上调", "earnings revision", "预期上调"],
     {"earnings_revision": 2.0, "growth": 1.5}),
]


def parse_weights(query: str) -> FactorWeights:
    """
    Parse weight preferences from a free-text query (Chinese or English).
    Returns a FactorWeights instance with adjusted values.
    """
    q = query.lower()
    overrides: dict[str, float] = {}

    for keywords, changes in _WEIGHT_PRESETS:
        if any(k.lower() in q for k in keywords):
            for k, v in changes.items():
                # Later matches override earlier ones for the same key
                overrides[k] = v

    if not overrides:
        return DEFAULT_WEIGHTS

    w = FactorWeights()
    for k, v in overrides.items():
        if hasattr(w, k):
            setattr(w, k, v)
    return w


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _get_price_position_f(price_df: Optional[pd.DataFrame]) -> Optional[float]:
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


# ---------------------------------------------------------------------------
# Core factor scoring functions
# ---------------------------------------------------------------------------

def score_value(
    pe_ttm: float,
    pb: float,
    val_history: Optional[pd.DataFrame],
    industry_stats: Optional[dict] = None,
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Value factor score (max 25).
    Priority:
      1. Intra-industry relative percentile (if industry_stats provided)
      2. Own historical percentile (if val_history available)
      3. Absolute threshold fallback
    PE contributes 15 pts, PB contributes 10 pts.

    Momentum cross: value trap filter (requires price_df)
      Deep value (pe_pct <= 20 or pb_pct <= 20) + 3m return > +5%  -> catalyst active, buy +2
      Deep value + 3m return < -10%                                 -> value trap risk, buy -2, sell +1

    Earnings revision cross: the classic "double-bottom" signal (requires revision_df)
      Deep value (pe_pct <= 25 or pb_pct <= 25) + net analyst upgrades >= 2 -> buy +3
      High valuation (pe_pct >= 80 or pb_pct >= 80) + net downgrades <= -2  -> sell +3

    Sector-relative PEG cross (requires financial_df):
      PE pct <= 30 (cheap in sector) + profit growth >= 20% -> sector's best-value growth stock -> buy +2
      PE pct >= 80 (expensive in sector) + profit growth <= 0 -> most expensive AND declining -> sell +2

    Market regime cross (requires market_regime_score):
      Deep value (pe_pct <= 20) + bear market (regime <= 3) -> 价值陷阱风险，催化剂要求更高 -> buy -1
      Moderate value (pe_pct <= 30) + bull market (regime >= 7) -> 估值洼地快速被填平 -> buy +1

    52w price position cross (requires price_df):
      Deep value (pe_pct <= 20 or pb_pct <= 20) + low position (< 0.3) -> 估值低+价格低=安全边际双重确认 -> buy +2
      High valuation (pe_pct >= 80 or pb_pct >= 80) + high position (> 0.7) -> 估值高+价格高=最危险组合 -> sell +2
    """
    from industry import industry_relative_percentile

    pe_score = 0.0
    pb_score = 0.0
    pe_pct: Optional[float] = None
    pb_pct: Optional[float] = None
    pe_source = "none"
    pb_source = "none"

    # --- PE score ---
    if pe_ttm and pe_ttm > 0:
        # Try intra-industry first
        if industry_stats and industry_stats.get("pe"):
            pe_pct = industry_relative_percentile(pe_ttm, industry_stats["pe"])
            if pe_pct is not None:
                pe_score = (1 - pe_pct / 100) * 15
                pe_source = "industry"

        # Fall back to own history
        if pe_source == "none" and val_history is not None and not val_history.empty:
            if "pe_ttm" in val_history.columns:
                hist = val_history["pe_ttm"].replace(0, np.nan).dropna()
                hist = hist[hist > 0]
                if len(hist) >= 10:
                    pct = float((hist < pe_ttm).sum() / len(hist))
                    pe_pct = round(pct * 100, 1)
                    pe_score = (1 - pct) * 15
                    pe_source = "history"

        # Absolute fallback
        if pe_source == "none":
            if 0 < pe_ttm <= 15:
                pe_score = 15
            elif pe_ttm <= 25:
                pe_score = 10
            elif pe_ttm <= 40:
                pe_score = 5
            pe_source = "absolute"

    # --- PB score ---
    if pb and pb > 0:
        if industry_stats and industry_stats.get("pb"):
            pb_pct = industry_relative_percentile(pb, industry_stats["pb"])
            if pb_pct is not None:
                pb_score = (1 - pb_pct / 100) * 10
                pb_source = "industry"

        if pb_source == "none" and val_history is not None and not val_history.empty:
            if "pb" in val_history.columns:
                hist = val_history["pb"].replace(0, np.nan).dropna()
                hist = hist[hist > 0]
                if len(hist) >= 10:
                    pct = float((hist < pb).sum() / len(hist))
                    pb_pct = round(pct * 100, 1)
                    pb_score = (1 - pct) * 10
                    pb_source = "history"

        if pb_source == "none":
            if 0 < pb <= 1:
                pb_score = 10
            elif pb <= 2:
                pb_score = 7
            elif pb <= 4:
                pb_score = 3
            pb_source = "absolute"

    total = round(pe_score + pb_score, 1)

    # --- Sell score: overvaluation signal ---
    pe_sell = 0.0
    pb_sell = 0.0

    if pe_ttm and pe_ttm > 0:
        if pe_source == "industry" and pe_pct is not None:
            if pe_pct >= 90:
                pe_sell = 20.0
            elif pe_pct >= 80:
                pe_sell = (pe_pct - 80) / 10 * 8.0 + 12.0  # linear 12-20
        elif pe_source == "history" and pe_pct is not None:
            if pe_pct >= 90:
                pe_sell = 20.0
            elif pe_pct >= 80:
                pe_sell = (pe_pct - 80) / 10 * 8.0 + 12.0  # linear 12-20
        else:
            # Absolute fallback
            if pe_ttm > 60:
                pe_sell = 20.0
            elif pe_ttm > 40:
                pe_sell = (pe_ttm - 40) / 20 * 7.0 + 5.0   # linear 5-12
            elif pe_ttm > 25:
                pe_sell = 5.0

    if pb and pb > 0:
        if pb_source == "industry" and pb_pct is not None:
            if pb_pct >= 90:
                pb_sell = 10.0
            elif pb_pct >= 80:
                pb_sell = (pb_pct - 80) / 10 * 5.0 + 5.0
        elif pb_source == "history" and pb_pct is not None:
            if pb_pct >= 90:
                pb_sell = 10.0
            elif pb_pct >= 80:
                pb_sell = (pb_pct - 80) / 10 * 5.0 + 5.0
        else:
            # Absolute fallback
            if pb > 5:
                pb_sell = 10.0
            elif pb > 3:
                pb_sell = (pb - 3) / 2 * 5.0 + 5.0

    sell_total = round(min(25.0, pe_sell + pb_sell), 1)

    # --- Momentum cross: value trap filter ---
    momentum_signal = None
    ret_3m = None
    if price_df is not None and len(price_df) >= 63 and "close" in price_df.columns:
        close = price_df["close"]
        current = float(close.iloc[-1])
        past_3m = float(close.iloc[-63])
        if past_3m > 0:
            ret_3m = (current - past_3m) / past_3m * 100
            deep_value = ((pe_pct is not None and pe_pct <= 20)
                          or (pb_pct is not None and pb_pct <= 20))
            if deep_value:
                if ret_3m > 5:
                    # Cheap + rising: value catalyst already firing, buy conviction
                    total = round(min(25.0, total + 2.0), 1)
                    momentum_signal = "value catalyst active (price recovering)"
                elif ret_3m < -10:
                    # Cheap but still falling: classic value trap pattern
                    total = round(max(0.0, total - 2.0), 1)
                    sell_total = round(min(25.0, sell_total + 1.0), 1)
                    momentum_signal = "value trap risk (cheap but falling)"

    # --- Earnings revision cross: cheap + upgrading = institutional buy trigger ---
    revision_signal = None
    if revision_df is not None and not revision_df.empty:
        # Inline revision extraction (factors.py cannot import from factors_extended)
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_revisions = up - down
            deep_value_r = ((pe_pct is not None and pe_pct <= 25)
                            or (pb_pct is not None and pb_pct <= 25))
            high_val_r   = ((pe_pct is not None and pe_pct >= 80)
                            or (pb_pct is not None and pb_pct >= 80))
            if deep_value_r and net_revisions >= 2:
                # Classic double-bottom: cheap AND analysts waking up → institutional trigger
                total = round(min(25.0, total + 3.0), 1)
                revision_signal = f"deep value + analyst upgrades (net {net_revisions:+d}) — double-bottom signal"
            elif high_val_r and net_revisions <= -2:
                # Expensive AND analysts cutting → double kill
                sell_total = round(min(25.0, sell_total + 3.0), 1)
                revision_signal = f"high valuation + analyst downgrades (net {net_revisions:+d}) — double kill"

    # --- Sector-relative PEG cross: cheapest/most expensive growth stock in peer group ---
    growth_signal = None
    if financial_df is not None and not financial_df.empty and pe_pct is not None:
        profit_growth = None
        for key in ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    profit_growth = float(vals.iloc[0])
                break
        if profit_growth is not None:
            if pe_pct <= 30 and profit_growth >= 20:
                # Cheapest quintile in sector + growing fast = best PEG in peer group
                total = round(min(25.0, total + 2.0), 1)
                growth_signal = f"sector-cheapest growth stock (PE pct {pe_pct:.0f}%, growth {profit_growth:.0f}%)"
            elif pe_pct >= 80 and profit_growth <= 0:
                # Most expensive quintile + declining earnings = worst risk/reward in peer group
                sell_total = round(min(25.0, sell_total + 2.0), 1)
                growth_signal = f"sector-most-expensive with declining earnings (PE pct {pe_pct:.0f}%, growth {profit_growth:.0f}%)"

    # --- Market regime cross: value reliability differs in bull vs bear ---
    regime_signal = None
    if market_regime_score is not None:
        deep_value_r2 = ((pe_pct is not None and pe_pct <= 20)
                         or (pb_pct is not None and pb_pct <= 20))
        moderate_value = ((pe_pct is not None and pe_pct <= 30)
                          or (pb_pct is not None and pb_pct <= 30))
        if deep_value_r2 and market_regime_score <= 3:
            # Bear market: cheap stocks can get cheaper; value traps are common without a catalyst
            total = round(max(0.0, total - 1.0), 1)
            regime_signal = "bear market — 深度低估但价值陷阱风险高，需要催化剂"
        elif moderate_value and market_regime_score >= 7:
            # Bull market: valuation gaps close fast as multiple expansion lifts all boats
            total = round(min(25.0, total + 1.0), 1)
            regime_signal = "bull market — 估值洼地在牛市中快速填平"

    # --- 52w price position cross: valuation + price level double confirmation ---
    position_val = None
    position_signal = None
    if price_df is not None and len(price_df) >= 20 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(252)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo:
                position_val = (cur - lo) / (hi - lo)
        except Exception:
            pass
    if position_val is not None:
        deep_val_pos = ((pe_pct is not None and pe_pct <= 20) or (pb_pct is not None and pb_pct <= 20))
        high_val_pos = ((pe_pct is not None and pe_pct >= 80) or (pb_pct is not None and pb_pct >= 80))
        if deep_val_pos and position_val < 0.3:
            # Cheap valuation AND near 52w low: double confirmation of value, best safety margin
            total = round(min(25.0, total + 2.0), 1)
            position_signal = f"估值低+低位({position_val:.2f}) — 安全边际双重确认"
        elif high_val_pos and position_val > 0.7:
            # Expensive AND near 52w high: double confirmation of overvaluation risk
            sell_total = round(min(25.0, sell_total + 2.0), 1)
            position_signal = f"估值高+高位({position_val:.2f}) — 最危险估值/价格组合"

    # --- Industry excess cross: value needs a sector catalyst to unlock ---
    industry_signal_v = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess_v = industry_ret_1m - market_ret_1m
        cheap_v = ((pe_pct is not None and pe_pct <= 30) or (pb_pct is not None and pb_pct <= 30))
        if cheap_v:
            if excess_v >= 3.0:
                # Cheap valuation + sector tailwind: value recovery has the catalyst it needs
                total = round(min(25.0, total + 1.5), 1)
                industry_signal_v = f"低估值+行业强(超额{excess_v:+.1f}%) — 价值修复有行业东风，时机到了"
            elif excess_v <= -3.0:
                # Cheap valuation + weak sector: value trap risk, sector still bleeding
                total = round(max(0.0, total - 1.0), 1)
                industry_signal_v = f"低估值+行业弱(超额{excess_v:+.1f}%) — 价值陷阱风险，行业还在出血"

    return {
        "score": total,
        "sell_score": sell_total,
        "max": 25,
        "details": {
            "pe_ttm": pe_ttm,
            "pe_percentile": round(pe_pct, 1) if pe_pct is not None else None,
            "pe_score_source": pe_source,
            "pb": pb,
            "pb_percentile": round(pb_pct, 1) if pb_pct is not None else None,
            "pb_score_source": pb_source,
            "pe_score": round(pe_score, 1),
            "pb_score": round(pb_score, 1),
            "ret_3m_pct": round(ret_3m, 2) if ret_3m is not None else None,
            "position_52w": round(position_val, 3) if position_val is not None else None,
            "momentum_signal": momentum_signal,
            "revision_signal": revision_signal,
            "growth_signal": growth_signal,
            "regime_signal": regime_signal,
            "position_signal": position_signal,
            "industry_signal": industry_signal_v,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "sell_score": sell_total,
        },
    }


def score_growth(
    financial_df: Optional[pd.DataFrame],
    pe_pct: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Growth factor score (max 25).
    Extracts revenue growth, profit growth, and ROE from the financial indicator table.
    Scoring curve: <=0% -> 0, linear up to 50% -> full score.

    Growth acceleration cross:
      Profit growth accelerating (current > prior period) -> buy boost
      Profit growth decelerating sharply -> sell boost

    PEG valuation cross (requires pe_pct from score_value):
      High growth (>= 30%) + PE percentile <= 40 -> undervalued growth, buy +2
        (growth not yet priced in — PEG < 1 territory, highest-conviction buy in growth investing)
      High growth (>= 20%) + PE percentile >= 80 -> growth fully priced in, sell +1.5
        (every analyst already owns it; upside requires execution perfection)
      Slow growth (<= 5%) + PE percentile >= 70 -> expensive with no growth, sell +2
        (the worst combination: paying growth multiple for a declining/stagnant business)

    Market regime cross (requires market_regime_score):
      High growth (>= 30%) + bull market (regime >= 7) -> buy +2 (双击效应: EPS↑ × 估值扩张)
      High growth (>= 30%) + bear market (regime <= 3) -> buy -1.5 (双杀: 业绩下修预期 + 去估值)
      Negative growth + bear market                    -> sell +1 (熊市基本面不支持，雪上加霜)

    Industry relative alpha cross (requires industry_ret_1m, market_ret_1m):
      High growth (>= 25%) + industry declining (excess <= -3%) -> buy +2
        (逆势成长，剔除行业顺风的纯净 alpha，信号最可靠)
      High growth (>= 25%) + industry hot (excess >= +5%)       -> buy -1
        (顺风成长，部分由板块解释，打折处理)

    52w price position cross (requires price_df):
      High growth (>= 25%) + low position (< 0.3) -> 成长加速+低位=双击条件尚未触发，最佳买点 -> buy +2
      High growth (>= 25%) + high position (> 0.7) -> 成长已被充分定价，上行空间压缩 -> buy -0.5, sell +0.5
    """
    revenue_growth: Optional[float] = None
    profit_growth:  Optional[float] = None
    roe:            Optional[float] = None
    profit_growth_prior: Optional[float] = None  # one period ago, for acceleration

    if financial_df is not None and not financial_df.empty:
        for key in ["营业收入增长率(%)", "营收增长率", "总营收同比增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    revenue_growth = float(vals.iloc[0])
                break

        for key in ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if len(vals) >= 1:
                    profit_growth = float(vals.iloc[0])
                if len(vals) >= 2:
                    profit_growth_prior = float(vals.iloc[1])
                break

        for key in ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe = float(vals.iloc[0])
                break

    def growth_to_score(g: Optional[float], max_score: float) -> float:
        if g is None:
            return max_score * 0.4
        if g <= 0:
            return 0.0
        return min(max_score, max_score * (g / 50))

    def roe_to_score(r: Optional[float], max_score: float) -> float:
        if r is None:
            return max_score * 0.4
        if r <= 5:
            return 0.0
        if r >= 15:
            return max_score
        return max_score * ((r - 5) / 10)

    rev_score    = growth_to_score(revenue_growth, 10)
    profit_score = growth_to_score(profit_growth, 10)
    roe_score    = roe_to_score(roe, 5)
    total = round(rev_score + profit_score + roe_score, 1)

    # --- Sell score: deteriorating fundamentals ---
    sell_rev = 0.0
    sell_profit = 0.0
    sell_roe = 0.0

    if revenue_growth is not None:
        if revenue_growth < -10:
            sell_rev = 10.0
        elif revenue_growth < 0:
            sell_rev = 5.0 + (-revenue_growth / 10) * 5.0
        elif revenue_growth < 5:
            sell_rev = 2.0

    if profit_growth is not None:
        if profit_growth < -20:
            sell_profit = 10.0
        elif profit_growth < 0:
            sell_profit = 5.0 + (-profit_growth / 20) * 5.0

    if roe is not None and roe < 3:
        sell_roe = 5.0

    sell_total = round(min(25.0, sell_rev + sell_profit + sell_roe), 1)

    # --- Growth acceleration cross ---
    accel_signal = None
    if profit_growth is not None and profit_growth_prior is not None:
        accel = profit_growth - profit_growth_prior
        if accel >= 15 and profit_growth > 0:
            # Strongly accelerating: e.g. 10% -> 30% growth
            total = round(min(25.0, total + 3.0), 1)
            accel_signal = "accelerating (+%.0f pp)" % accel
        elif accel >= 5 and profit_growth > 0:
            total = round(min(25.0, total + 1.5), 1)
            accel_signal = "mild acceleration (+%.0f pp)" % accel
        elif accel <= -20 and profit_growth < 20:
            # Sharp deceleration: high-growth story collapsing
            sell_total = round(min(25.0, sell_total + 4.0), 1)
            accel_signal = "sharp deceleration (%.0f pp)" % accel
        elif accel <= -10:
            sell_total = round(min(25.0, sell_total + 2.0), 1)
            accel_signal = "decelerating (%.0f pp)" % accel

    # --- Growth quality cross: profit growth × ROE level ---
    # High profit growth backed by high ROE = genuine compounder; low ROE = hollow growth
    if profit_growth is not None and roe is not None:
        if profit_growth >= 30 and roe >= 15:
            # Rapid growth + excellent capital returns: textbook quality compounder
            total = round(min(25.0, total + 2.0), 1)
            accel_signal = (accel_signal + " | " if accel_signal else "") + "high-ROE compounder (buy boost)"
        elif profit_growth >= 20 and roe < 5:
            # Growing fast but barely generating returns: asset-heavy or unsustainable
            sell_total = round(min(25.0, sell_total + 2.0), 1)
            accel_signal = (accel_signal + " | " if accel_signal else "") + "low-ROE growth (quality concern)"

    # --- PEG valuation cross: are you paying a fair price for this growth? ---
    if pe_pct is not None and profit_growth is not None:
        if profit_growth >= 30 and pe_pct <= 40:
            # High growth + historically cheap PE: PEG well below 1, classic undervalued growth
            total = round(min(25.0, total + 2.0), 1)
            accel_signal = (accel_signal + " | " if accel_signal else "") + "undervalued growth (PEG < 1 territory)"
        elif profit_growth >= 20 and pe_pct >= 80:
            # High growth but PE at historical highs: growth is fully priced in
            sell_total = round(min(25.0, sell_total + 1.5), 1)
            accel_signal = (accel_signal + " | " if accel_signal else "") + "growth fully priced in (high PEG risk)"
        elif profit_growth <= 5 and pe_pct >= 70:
            # Stagnant or declining growth but still priced at premium: worst combination
            sell_total = round(min(25.0, sell_total + 2.0), 1)
            accel_signal = (accel_signal + " | " if accel_signal else "") + "expensive stagnant growth (PEG trap)"

    # --- Market regime cross: growth factor is most regime-dependent in A-shares ---
    regime_signal_g = None
    if market_regime_score is not None and profit_growth is not None:
        if profit_growth >= 30:
            if market_regime_score >= 7:
                # Bull market: high-growth stocks receive EPS upgrade AND multiple expansion simultaneously
                total = round(min(25.0, total + 2.0), 1)
                regime_signal_g = f"bull market — 双击效应 (profit +{profit_growth:.0f}%, EPS↑×PE扩张)"
            elif market_regime_score <= 3:
                # Bear market: growth stocks get de-rated hardest (high PE = biggest target)
                total = round(max(0.0, total - 1.5), 1)
                regime_signal_g = f"bear market — 双杀风险 (profit +{profit_growth:.0f}%, 成长股去估值最剧烈)"
        elif profit_growth is not None and profit_growth < 0 and market_regime_score <= 3:
            # Declining earnings in bear market: no support from either fundamental or macro
            sell_total = round(min(25.0, sell_total + 1.0), 1)
            regime_signal_g = f"bear market + declining earnings — 熊市基本面双重压力"

    # --- Industry relative alpha cross: is growth sector-driven or company-specific? ---
    industry_signal_g = None
    if industry_ret_1m is not None and market_ret_1m is not None and profit_growth is not None:
        ind_excess_g = industry_ret_1m - market_ret_1m
        if profit_growth >= 25:
            if ind_excess_g <= -3:
                # Growing fast while sector is falling: company-specific alpha, not sector tailwind
                total = round(min(25.0, total + 2.0), 1)
                industry_signal_g = f"逆势成长 (profit +{profit_growth:.0f}%, sector {ind_excess_g:+.1f}%) — 纯净alpha信号"
            elif ind_excess_g >= 5:
                # Growing fast but sector also hot: growth partially explained by sector tailwind
                total = round(max(0.0, total - 1.0), 1)
                industry_signal_g = f"顺风成长 (profit +{profit_growth:.0f}%, sector {ind_excess_g:+.1f}%) — 打折处理"

    # --- Earnings revision cross: analyst view confirms or contradicts growth trajectory ---
    revision_signal_g = None
    if revision_df is not None and not revision_df.empty and profit_growth is not None:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if profit_growth >= 20:
                if net_rev >= 2:
                    # High growth + analyst upgrades: growth acceleration confirmed
                    total = round(min(25.0, total + 1.5), 1)
                    revision_signal_g = f"高成长+分析师上调(net {net_rev:+d}) — 成长加速确认"
                elif net_rev <= -2:
                    # High growth but analysts cutting: peak growth warning
                    sell_total = round(min(25.0, sell_total + 1.5), 1)
                    revision_signal_g = f"高成长+分析师下调(net {net_rev:+d}) — 业绩见顶警告"
            elif profit_growth < 5:
                if net_rev >= 2:
                    # Low/negative growth + analyst upgrades: early recovery signal
                    total = round(min(25.0, total + 1.0), 1)
                    revision_signal_g = f"低成长+分析师上调(net {net_rev:+d}) — 成长复苏早期，逆向机会"

    # --- 52w price position cross: growth + price level timing ---
    position_signal_g = None
    if price_df is not None and len(price_df) >= 20 and "close" in price_df.columns and profit_growth is not None:
        try:
            window = price_df["close"].tail(252)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo and profit_growth >= 25:
                pos_g = (cur - lo) / (hi - lo)
                if pos_g < 0.3:
                    # Fast growth + near 52w low: market hasn't priced in the growth yet — best timing
                    total = round(min(25.0, total + 2.0), 1)
                    position_signal_g = f"成长+低位({pos_g:.2f}) — 双击条件尚未触发，最佳买点"
                elif pos_g > 0.7:
                    # Fast growth but near 52w high: growth premium already fully captured
                    total = round(max(0.0, total - 0.5), 1)
                    sell_total = round(min(25.0, sell_total + 0.5), 1)
                    position_signal_g = f"成长+高位({pos_g:.2f}) — 成长已被充分定价"
        except Exception:
            pass

    return {
        "score": total,
        "sell_score": sell_total,
        "max": 25,
        "details": {
            "revenue_growth_pct":      revenue_growth,
            "profit_growth_pct":       profit_growth,
            "profit_growth_prior_pct": profit_growth_prior,
            "accel_signal":            accel_signal,
            "roe_pct":                 roe,
            "revenue_score":           round(rev_score, 1),
            "profit_score":            round(profit_score, 1),
            "roe_score":               round(roe_score, 1),
            "regime_signal":           regime_signal_g,
            "industry_signal":         industry_signal_g,
            "revision_signal":         revision_signal_g,
            "position_signal":         position_signal_g,
            "sell_score":              sell_total,
        },
    }


def score_momentum(
    price_df: Optional[pd.DataFrame],
    financial_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Momentum factor score (max 25).
      3-month return : 12.5 pts
      6-month return : 12.5 pts
    NOTE: 1-month return is intentionally excluded — it belongs to the reversal
    factor (score_reversal) which treats 1m decline as a contrarian BUY signal.
    Scoring curve: <=-20% -> 0, 0% -> 50% of max, >=20% -> full.

    Volume divergence cross (量价背离):
      Strong uptrend (3m >= +15%) + volume contracting (vol_20d/vol_60d < 0.75)
        -> trend deceleration, sell +4 (price rising on thin air)
      Strong downtrend (3m <= -15%) + volume contracting (vol_20d/vol_60d < 0.70)
        -> selling exhausted, sell -3 (nobody left to sell)

    Quality cross: momentum sustainability filter (requires financial_df)
      Strong uptrend (3m >= +20% or 6m >= +25%) + ROE >= 15% -> buy +2 (quality momentum, sustainable)
      Strong uptrend + ROE < 5%                              -> sell +2 (speculation risk, no fundamentals)

    Market regime cross (Daniel & Moskowitz momentum crash):
      Strong uptrend + bear market (regime <= 3) -> buy -3, sell +2
        (momentum systematically fails in bear markets; chasing uptrends = catching falling knives)
      Strong uptrend + bull market (regime >= 7) -> buy +1.5
        (bull market trend continuation is more reliable)
      Strong downtrend + bull market (regime >= 7) -> sell -1.5
        (downtrend in bull market = likely mean-reversion candidate, not a structural bear)

    Industry relative strength cross (requires industry_ret_1m, market_ret_1m):
      Strong uptrend (3m >= +15%) + industry outperforming (excess >= +3%) -> buy +1.5 (行业龙头，强者恒强)
      Strong uptrend + industry underperforming (excess <= -3%) -> sell +1 (孤立动量，行业无支撑)
      Strong downtrend (3m <= -15%) + industry also weak (excess <= -3%) -> sell +1 (行业整体弱，难反转)

    52w price position cross:
      Strong uptrend (3m >= +15%) + low position (< 0.3) -> buy +1.5 (动量刚刚启动，上行空间最大)
      Strong uptrend + high position (> 0.8) -> sell +1 (动量接近历史高位，可能在尾声)
      Strong downtrend (3m <= -15%) + high position (> 0.7) -> sell +1.5 (高位转跌=最危险的动量组合)
    """
    ret_3m = ret_6m = None

    if price_df is not None and len(price_df) >= 5:
        close = price_df["close"]
        current = float(close.iloc[-1])

        def calc_return(n_days: int) -> Optional[float]:
            if len(close) < n_days:
                return None
            past = float(close.iloc[-n_days])
            return ((current - past) / past) * 100 if past > 0 else None

        ret_3m = calc_return(63)
        ret_6m = calc_return(126)

    def momentum_score(ret: Optional[float], max_score: float) -> float:
        if ret is None:
            return max_score * 0.4
        if ret <= -20:
            return 0.0
        if ret >= 20:
            return max_score
        if ret >= 0:
            return max_score * (0.5 + ret / 40)
        return max_score * max(0.0, (20 + ret) / 40)

    s3 = momentum_score(ret_3m, 12.5)
    s6 = momentum_score(ret_6m, 12.5)
    total = round(s3 + s6, 1)

    # --- Sell score: strong negative price momentum ---
    # Only 3m and 6m for sell signal
    sell_3m = 0.0
    sell_6m = 0.0

    if ret_3m is not None:
        if ret_3m < -20:
            sell_3m = 12.5
        elif ret_3m < -5:
            sell_3m = 3.0 + ((-ret_3m - 5) / 15) * 9.5

    if ret_6m is not None:
        if ret_6m < -30:
            sell_6m = 12.5
        elif ret_6m < -10:
            sell_6m = 3.0 + ((-ret_6m - 10) / 20) * 9.5

    sell_total = round(min(25.0, sell_3m + sell_6m), 1)

    # --- Volume divergence cross (量价背离) ---
    vol_ratio = None
    vol_signal = None
    try:
        if (price_df is not None and "volume" in price_df.columns
                and len(price_df) >= 60):
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 60:
                v20 = float(vol.tail(20).mean())
                v60 = float(vol.tail(60).mean())
                if v60 > 0:
                    vol_ratio = v20 / v60
    except Exception:
        pass

    if vol_ratio is not None:
        strong_up   = (ret_3m is not None and ret_3m >= 15) or (ret_6m is not None and ret_6m >= 20)
        strong_down = (ret_3m is not None and ret_3m <= -15) or (ret_6m is not None and ret_6m <= -20)

        if strong_up and vol_ratio < 0.75:
            sell_total = round(min(25.0, sell_total + 4.0), 1)
            vol_signal = "divergence: price up, volume contracting (trend exhaustion)"
        elif strong_up and vol_ratio < 0.85:
            sell_total = round(min(25.0, sell_total + 2.0), 1)
            vol_signal = "mild divergence: volume weakening in uptrend"
        elif strong_down and vol_ratio < 0.70:
            sell_total = round(max(0.0, sell_total - 3.0), 1)
            vol_signal = "volume exhaustion: selling pressure drying up"

    # --- Quality cross: momentum sustainability ---
    roe = None
    quality_signal = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净资产收益率(%)", "加权净资产收益率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe = float(vals.iloc[0])
                break

        strong_up_q = ((ret_3m is not None and ret_3m >= 20)
                       or (ret_6m is not None and ret_6m >= 25))
        if roe is not None and strong_up_q:
            if roe >= 15:
                # Strong price trend + strong business returns: fundamentally-backed momentum
                total = round(min(25.0, total + 2.0), 1)
                quality_signal = "quality momentum (high ROE — sustainable trend)"
            elif roe < 5:
                # Price surging but business barely earns returns: speculative bubble risk
                sell_total = round(min(25.0, sell_total + 2.0), 1)
                quality_signal = "low-quality momentum (low ROE — speculation risk)"

    # --- Market regime cross: momentum crash risk (Daniel & Moskowitz) ---
    regime_signal = None
    if market_regime_score is not None:
        strong_up_r   = (ret_3m is not None and ret_3m >= 15) or (ret_6m is not None and ret_6m >= 20)
        strong_down_r = (ret_3m is not None and ret_3m <= -15) or (ret_6m is not None and ret_6m <= -20)
        if strong_up_r and market_regime_score <= 3:
            # Momentum crashes in bear markets: systematic factor reversal documented by Daniel & Moskowitz
            total      = round(max(0.0, total - 3.0), 1)
            sell_total = round(min(25.0, sell_total + 2.0), 1)
            regime_signal = "bear market momentum — crash risk (buy -3, sell +2)"
        elif strong_up_r and market_regime_score >= 7:
            # Bull market trend continuation: higher probability of follow-through
            total = round(min(25.0, total + 1.5), 1)
            regime_signal = "bull market momentum — high continuation probability (buy +1.5)"
        elif strong_down_r and market_regime_score >= 7:
            # Downtrend in bull market: mean-reversion likely, reduce sell urgency
            sell_total = round(max(0.0, sell_total - 1.5), 1)
            regime_signal = "bull market downtrend — mean-reversion candidate (sell -1.5)"

    # --- Industry relative strength cross: sector leader vs isolated momentum ---
    ind_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        industry_excess = industry_ret_1m - market_ret_1m
        strong_up_ind   = ret_3m is not None and ret_3m >= 15
        strong_down_ind = ret_3m is not None and ret_3m <= -15
        if strong_up_ind and industry_excess >= 3:
            # Individual stock strong + sector also leading: sector leader, highest conviction
            total = round(min(25.0, total + 1.5), 1)
            ind_signal = f"行业龙头 (industry excess {industry_excess:+.1f}%) — 强者恒强"
        elif strong_up_ind and industry_excess <= -3:
            # Individual stock strong but sector weak: isolated, less sustainable momentum
            sell_total = round(min(25.0, sell_total + 1.0), 1)
            ind_signal = f"孤立动量 (industry excess {industry_excess:+.1f}%) — 行业无支撑，可靠性打折"
        elif strong_down_ind and industry_excess <= -3:
            # Both stock and sector weak: sector-wide selling, harder to reverse
            sell_total = round(min(25.0, sell_total + 1.0), 1)
            ind_signal = f"行业整体弱 (industry excess {industry_excess:+.1f}%) — 难反转"
    else:
        industry_excess = None

    # --- Earnings revision cross: analyst momentum vs price momentum convergence ---
    revision_signal_m = None
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            strong_up_m   = ret_3m is not None and ret_3m >= 15
            strong_down_m = ret_3m is not None and ret_3m <= -10
            if strong_up_m:
                if net_rev >= 2:
                    # Price momentum + analyst upgrades: technical and fundamental momentum aligned
                    total = round(min(25.0, total + 2.0), 1)
                    revision_signal_m = f"强动量+分析师上调(net {net_rev:+d}) — 技术+基本面双向确认，最强持续信号"
                elif net_rev <= -2:
                    # Price rising but analysts cutting: momentum without fundamental support
                    sell_total = round(min(25.0, sell_total + 1.5), 1)
                    revision_signal_m = f"强动量+分析师下调(net {net_rev:+d}) — 价格强但基本面恶化=高位泡沫风险"
            elif strong_down_m and net_rev >= 2:
                # Price weak but analysts upgrading: fundamentals leading price, recovery coming
                total = round(min(25.0, total + 1.5), 1)
                revision_signal_m = f"弱动量+分析师上调(net {net_rev:+d}) — 基本面先行，价格将跟上"

    # --- 52w price position cross: momentum at different price levels has different meaning ---
    position_signal = None
    if price_df is not None and len(price_df) >= 20 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(252)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo:
                pos = (cur - lo) / (hi - lo)
                strong_up_p   = (ret_3m is not None and ret_3m >= 15) or (ret_6m is not None and ret_6m >= 20)
                strong_down_p = (ret_3m is not None and ret_3m <= -15) or (ret_6m is not None and ret_6m <= -20)
                if strong_up_p and pos < 0.3:
                    # Momentum just starting from a beaten-down price: maximum upside remaining
                    total = round(min(25.0, total + 1.5), 1)
                    position_signal = f"动量+低位({pos:.2f}) — 刚刚启动，上行空间最大"
                elif strong_up_p and pos > 0.8:
                    # Momentum near 52w high: possible late-stage, distribution risk
                    sell_total = round(min(25.0, sell_total + 1.0), 1)
                    position_signal = f"动量+高位({pos:.2f}) — 接近历史高点，可能在尾声"
                elif strong_down_p and pos > 0.7:
                    # Downward momentum while still near 52w high: most dangerous combination
                    sell_total = round(min(25.0, sell_total + 1.5), 1)
                    position_signal = f"下跌动量+高位({pos:.2f}) — 高位转跌，最危险的动量组合"
        except Exception:
            pos = None

    return {
        "score": total,
        "sell_score": sell_total,
        "max": 25,
        "details": {
            "return_3m_pct": round(ret_3m, 2) if ret_3m is not None else None,
            "return_6m_pct": round(ret_6m, 2) if ret_6m is not None else None,
            "score_3m": round(s3, 1),
            "score_6m": round(s6, 1),
            "vol_ratio_20d_60d": round(vol_ratio, 3) if vol_ratio is not None else None,
            "vol_signal": vol_signal,
            "roe_pct": round(roe, 1) if roe is not None else None,
            "quality_signal": quality_signal,
            "market_regime_score": market_regime_score,
            "regime_signal": regime_signal,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "industry_signal": ind_signal,
            "revision_signal": revision_signal_m,
            "position_signal": position_signal,
            "sell_score": sell_total,
        },
    }


def score_quality(
    financial_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    pe_pct: Optional[float] = None,
    pb_pct: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Quality factor score (max 25).
      ROE average     : 10 pts  (>=20% -> full, <=5% -> 0)
      Gross margin    : 10 pts  (>=50% -> full, <=10% -> 0)
      Debt ratio      :  5 pts  (<=30% -> full, >=70% -> 0, lower is better)

    Context cross with 52w price position:
      High quality (ROE>=15%, margin>=30%, debt<=40%) + low position (< 0.3)
        -> quality-at-value setup (genuinely good business priced cheaply) -> buy +3
      Low quality (ROE<5% or margin<10%) + high position (> 0.7)
        -> expensive mediocre business at peak price -> sell +3

    GARP cross (requires pe_pct / pb_pct from score_value):
      High quality + cheap valuation (pe_pct <= 30 or pb_pct <= 30) -> buy +2
        (quality business at a bargain — the strongest fundamental buy signal)
      High quality + extreme valuation (pe_pct >= 85 or pb_pct >= 85) -> sell -1.5
        (quality companies deserve a premium — reduce sell urgency)
      Low quality + cheap valuation -> buy -2
        (cheap-for-a-reason: poor business that happens to be inexpensive)

    Market regime cross (requires market_regime_score):
      High quality (ROE >= 15%) + bear market (regime <= 3) -> buy +1.5 (防御性溢价，机构抱团)
      High quality + bull market (regime >= 7)              -> buy -0.5 (质量溢价衰减，动量>质量)

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      High quality (ROE >= 15%) + industry outperforming (excess >= +3%) -> buy +1.5 (双重顺风=龙头加速)
      High quality + industry weak (excess <= -3%)                       -> buy +1 (逆行业个股alpha，信号更稀缺)
    """
    gross_margin = debt_ratio = roe = None

    if financial_df is not None and not financial_df.empty:
        for key in ["销售毛利率(%)", "毛利率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    gross_margin = float(vals.head(4).mean())
                break

        for key in ["资产负债率(%)", "负债率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    debt_ratio = float(vals.iloc[0])
                break

        for key in ["净资产收益率(%)", "加权净资产收益率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe = float(vals.head(4).mean())
                break

    def roe_score(r, ms):
        if r is None: return ms * 0.4
        if r <= 5:   return 0.0
        if r >= 20:  return ms
        return ms * ((r - 5) / 15)

    def margin_score(m, ms):
        if m is None: return ms * 0.4
        if m <= 10:   return 0.0
        if m >= 50:   return ms
        return ms * ((m - 10) / 40)

    def debt_score(d, ms):
        if d is None: return ms * 0.4
        if d >= 70:   return 0.0
        if d <= 30:   return ms
        return ms * ((70 - d) / 40)

    # Effective ROE: penalise leverage-inflated returns
    effective_roe = roe
    if roe is not None and debt_ratio is not None and debt_ratio > 0:
        effective_roe = roe * (1.0 - debt_ratio / 100.0)

    rs = roe_score(effective_roe, 10)
    ms = margin_score(gross_margin, 10)
    ds = debt_score(debt_ratio, 5)
    total = round(rs + ms + ds, 1)

    # --- Sell score: deteriorating quality signals ---
    sell_debt = 0.0
    sell_roe = 0.0
    sell_margin = 0.0

    if debt_ratio is not None:
        if debt_ratio > 70:
            sell_debt = 10.0
        elif debt_ratio > 55:
            sell_debt = 5.0

    if roe is not None:
        if roe < 3:
            sell_roe = 8.0
        elif roe < 7:
            sell_roe = 3.0

    if gross_margin is not None and gross_margin < 5:
        sell_margin = 7.0

    sell_total = round(min(25.0, sell_debt + sell_roe + sell_margin), 1)

    # --- Context cross: quality level × 52w price position ---
    quality_signal = None
    position = None
    if price_df is not None and len(price_df) >= 20 and "close" in price_df.columns:
        window = price_df["close"].tail(252)
        hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
        if hi > lo:
            position = (cur - lo) / (hi - lo)

    if position is not None:
        quality_high = (rs >= 7.0 and ms >= 6.0)  # strong ROE + decent margin
        quality_low  = (rs <= 2.0 or (ms <= 2.0 and rs <= 4.0))  # poor returns or very low margin
        if quality_high and position < 0.3:
            # Genuinely high-quality business at a beaten-down price: best accumulation setup
            total = round(min(25.0, total + 3.0), 1)
            quality_signal = "high-quality business at low price (quality-at-value)"
        elif quality_low and position > 0.7:
            # Mediocre or poor-quality business priced near its peak: expensive junk
            sell_total = round(min(25.0, sell_total + 3.0), 1)
            quality_signal = "low-quality business at high price (expensive junk)"

    # --- GARP cross: quality level × valuation percentile ---
    garp_signal = None
    if pe_pct is not None or pb_pct is not None:
        quality_high = (rs >= 7.0 and ms >= 6.0)
        quality_low  = (rs <= 2.0 or (ms <= 2.0 and rs <= 4.0))
        deep_value_g = ((pe_pct is not None and pe_pct <= 30)
                        or (pb_pct is not None and pb_pct <= 30))
        high_val_g   = ((pe_pct is not None and pe_pct >= 85)
                        or (pb_pct is not None and pb_pct >= 85))
        if quality_high and deep_value_g:
            # Great business at a bargain price: the classic Buffett setup
            total = round(min(25.0, total + 2.0), 1)
            garp_signal = "GARP (high quality + cheap valuation — ideal accumulation)"
        elif quality_high and high_val_g:
            # Quality companies command premiums — reduce sell urgency
            sell_total = round(max(0.0, sell_total - 1.5), 1)
            garp_signal = "quality premium justified (expensive but high-quality — reduced sell)"
        elif quality_low and deep_value_g:
            # Low quality despite low PE: cheap-for-a-reason trap
            total = round(max(0.0, total - 2.0), 1)
            garp_signal = "cheap-for-a-reason (low quality + low valuation — caution)"

    # --- Market regime cross: quality premium varies by market environment ---
    regime_signal_q = None
    if market_regime_score is not None and roe is not None:
        if roe >= 15:
            if market_regime_score <= 3:
                # Bear market: high-quality companies attract defensive flows (机构抱团)
                total = round(min(25.0, total + 1.5), 1)
                regime_signal_q = "bear market — 高质量防御性溢价，机构抱团"
            elif market_regime_score >= 7:
                # Bull market: momentum beats quality, growth beats defensiveness
                total = round(max(0.0, total - 0.5), 1)
                regime_signal_q = "bull market — 牛市质量溢价衰减，动量 > 质量"

    # --- Industry excess cross: quality signal is amplified/differentiated by sector ---
    industry_signal_q = None
    if industry_ret_1m is not None and market_ret_1m is not None and roe is not None and roe >= 15:
        excess_q = industry_ret_1m - market_ret_1m
        if excess_q >= 3:
            # High-quality + outperforming sector: double tailwind — quality dragon-head in hot sector
            total = round(min(25.0, total + 1.5), 1)
            industry_signal_q = f"高质量+行业强({excess_q:+.1f}%) — 双重顺风，龙头加速"
        elif excess_q <= -3:
            # High-quality but sector is weak: company-specific alpha against sector headwind
            # The signal is rarer and thus more informative (strong hands holding quality in weak sector)
            total = round(min(25.0, total + 1.0), 1)
            industry_signal_q = f"高质量+行业弱({excess_q:+.1f}%) — 逆行业个股alpha，信号更稀缺"

    return {
        "score": total,
        "sell_score": sell_total,
        "max": 25,
        "details": {
            "roe_avg_pct":          round(roe, 1)          if roe          is not None else None,
            "effective_roe_pct":    round(effective_roe, 1) if effective_roe is not None else None,
            "gross_margin_avg_pct": round(gross_margin, 1) if gross_margin is not None else None,
            "debt_ratio_pct":       round(debt_ratio, 1)   if debt_ratio   is not None else None,
            "roe_score":            round(rs, 1),
            "margin_score":         round(ms, 1),
            "debt_score":           round(ds, 1),
            "position_52w":         round(position, 3) if position is not None else None,
            "quality_signal":       quality_signal,
            "garp_signal":          garp_signal,
            "regime_signal":        regime_signal_q,
            "industry_signal":      industry_signal_q,
            "sell_score":           sell_total,
        },
    }


# ---------------------------------------------------------------------------
# Extended factors
# ---------------------------------------------------------------------------

def score_northbound(
    fund_flow_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Institutional / large-order net inflow score (max 10).
    Uses per-stock order-flow breakdown as a smart-money proxy.
    Looks at net large-order inflow over the last 5 days:
      - Consistently positive -> high score
      - Consistently negative -> low score

    52w price position cross (requires price_df):
      5/5 positive days + low position (< 0.3) -> 低位持续流入=底部建仓，最有价值的信号 -> buy +1.5
      Majority negative days + high position (> 0.7) -> 高位持续流出=顶部分发 -> sell +1

    Market regime cross (requires market_regime_score):
      5/5 positive days + bull market (regime >= 7) -> buy +1 (牛市机构增持趋势延续性更强)
      Majority negative (>= 4) + bear market (regime <= 3) -> sell +1 (熊市机构减仓=加速撤退)

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      5/5 positive + industry weak (excess <= -3%) -> buy +1.5 (逆弱行业持续流入=最高置信度的个股发现)
      5/5 positive + industry strong (excess >= +3%) -> buy +0.5 (顺风流入，正常)
      >= 4 negative + industry weak (excess <= -3%) -> sell +1 (资金+行业双重出逃，确认性更强)
    """
    if fund_flow_df is None or fund_flow_df.empty:
        return {"score": 5.0, "sell_score": 2.0, "max": 10, "details": {"source": "no data, neutral", "sell_score": 2.0}}

    # Try to locate a large-order net-inflow column
    large_cols = [c for c in fund_flow_df.columns
                  if any(k in c for k in ["主力净流入", "大单净流入", "超大单净流入"])]
    if not large_cols:
        return {"score": 5.0, "sell_score": 2.0, "max": 10, "details": {"source": "column not found, neutral", "sell_score": 2.0}}

    col = large_cols[0]
    series = pd.to_numeric(fund_flow_df[col], errors="coerce").dropna()
    if series.empty:
        return {"score": 5.0, "sell_score": 2.0, "max": 10, "details": {"source": "no numeric data, neutral", "sell_score": 2.0}}

    recent = series.tail(5)
    net_total = float(recent.sum())
    positive_days = int((recent > 0).sum())
    negative_days = 5 - positive_days

    # Score: base 5, +1 per positive day (max +5), scale by magnitude
    day_score = positive_days * 1.0  # 0-5
    # Magnitude bonus: cap at 5 pts based on sign of net_total
    mag_score = 5.0 if net_total > 0 else 0.0

    total = round(min(10.0, day_score + (mag_score - 2.5) * 0.5 + 5.0), 1)

    # --- Sell score: consistent large-order net outflow ---
    if negative_days == 5:
        sell_base = 8.0
    elif negative_days == 4:
        sell_base = 6.0
    else:
        sell_base = 0.0

    # Scale up if net total is very negative
    if net_total < 0:
        # Rough scale: very large outflow boosts to 10
        outflow_billion = abs(net_total) / 1e8
        mag_sell = min(2.0, outflow_billion / 5)  # caps at 2 extra pts for 5B outflow
        sell_total = round(min(10.0, sell_base + mag_sell), 1)
    else:
        sell_total = round(min(2.0, sell_base), 1)  # minimal if net positive

    # --- 52w price position cross: inflow/outflow intent differs at price extremes ---
    position_nb = None
    if price_df is not None:
        position_nb = _get_price_position_f(price_df)
    if position_nb is not None:
        if positive_days == 5 and position_nb < 0.3:
            # Sustained inflow into a beaten-down stock: institutional bottom accumulation
            total = min(10.0, total + 1.5)
            sell_total = max(0.0, sell_total - 1.0)
        elif negative_days >= 4 and position_nb > 0.7:
            # Sustained outflow from a high-priced stock: institutional distribution at top
            sell_total = min(10.0, sell_total + 1.0)

    # --- Market regime cross: inflow/outflow persistence varies by market environment ---
    if market_regime_score is not None:
        if positive_days == 5 and market_regime_score >= 7:
            # Bull market: institutional inflows tend to be self-reinforcing
            total = min(10.0, total + 1.0)
        elif negative_days >= 4 and market_regime_score <= 3:
            # Bear market: institutional outflows signal accelerating retreat
            sell_total = min(10.0, sell_total + 1.0)

    # --- Industry excess return cross: flow against weak sector = higher discovery value ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if positive_days == 5 and excess <= -3.0:
            # Sustained inflow while sector is weak: this is a genuine contrarian discovery signal
            total = min(10.0, total + 1.5)
            industry_signal = f"逆弱行业流入(超额{excess:.1f}%) — 最高置信度的个股发现"
        elif positive_days == 5 and excess >= 3.0:
            # Inflow with sector tailwind: normal, mild confirmation
            total = min(10.0, total + 0.5)
            industry_signal = f"顺风流入(超额{excess:.1f}%) — 行业+资金顺风"
        elif negative_days >= 4 and excess <= -3.0:
            # Outflow + weak sector: double confirmation of exit
            sell_total = min(10.0, sell_total + 1.0)
            industry_signal = f"资金+行业双重出逃(超额{excess:.1f}%) — 卖出信号放大"

    # --- Earnings revision cross: institutional money flow + analyst convergence ---
    revision_signal_nb = None
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up_nb   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down_nb = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_nb  = up_nb - down_nb
            if positive_days == 5 and net_nb >= 2:
                # Sustained northbound buying + analyst upgrades: foreign and domestic institutions aligned
                total = min(10.0, total + 2.0)
                revision_signal_nb = f"北向持续买入+分析师上调(net {net_nb:+d}) — 外资+国内机构双向确认，最可信共识"
            elif negative_days >= 4 and net_nb <= -2:
                # Northbound selling + analyst cuts: both smart-money groups exiting together
                sell_total = min(10.0, sell_total + 1.5)
                revision_signal_nb = f"北向净卖+分析师下调(net {net_nb:+d}) — 双向机构撤退，卖出信号放大"

    return {
        "score": round(total, 1),
        "sell_score": round(sell_total, 1),
        "max": 10,
        "details": {
            "net_5d_inflow": round(net_total / 1e8, 2),  # in 100M CNY
            "positive_days_of_5": positive_days,
            "position_52w": round(position_nb, 3) if position_nb is not None else None,
            "market_regime_score": market_regime_score,
            "industry_signal": industry_signal,
            "revision_signal": revision_signal_nb,
            "source": col,
            "sell_score": round(sell_total, 1),
        },
    }


def score_volume_breakout(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    best_concept_ret: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Volume breakout score (max 10).
    Compares the most recent day's volume to the 20-day average, crossed with
    price direction to distinguish accumulation from distribution.
      放量上涨 (high vol + price up)   -> strong buy
      放量大阴线 (high vol + big drop) -> distribution, sell signal
      缩量下跌 (low vol + price down)  -> selling exhausted, mild buy
      缩量上涨 (low vol + price up)    -> unsustainable rally, mild sell

    MA trend cross: confirms which direction the volume event represents
      Volume breakout (>= 1.5x) + MA5 > MA20 (uptrend) -> buy +1.5 (accumulation confirmed)
      Volume breakout (>= 1.5x) + MA5 < MA20 (downtrend) -> sell +1.5 (distribution confirmed)

    Market regime cross (requires market_regime_score):
      放量上涨 + bull market (regime >= 7) -> 牛市放量持续性更强，机构参与 -> buy +1
      放量下跌 + bear market (regime <= 3) -> 熊市放量下跌=加速出逃，放大卖出 -> sell +1

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      放量上涨 + industry outperforming (excess >= +3%) -> 技术+行业双重确认 -> buy +1.5
      放量上涨 + industry weak (excess <= -3%) -> 全市场出货，可能是减持 -> sell +1
      放量大阴线 + industry weak -> 行业+个股双杀，卖出放大 -> sell +1

    Concept cross (requires best_concept_ret):
      放量上涨 + hot concept (>= +8%) -> 概念资金驱动，持续性强 -> buy +2
      放量上涨 + cold concept (<= 0%) -> 放量无主题催化，警惕减持 -> buy -1
    """
    if price_df is None or len(price_df) < 21 or "volume" not in price_df.columns:
        return {"score": 5.0, "sell_score": 0.0, "max": 10, "details": {"source": "no data, neutral", "sell_score": 0.0}}

    vol = price_df["volume"]
    ma20_vol = float(vol.iloc[-21:-1].mean())
    current_vol = float(vol.iloc[-1])

    if ma20_vol <= 0:
        return {"score": 5.0, "sell_score": 0.0, "max": 10, "details": {"ratio": None, "sell_score": 0.0}}

    ratio = current_vol / ma20_vol

    # Get price direction info from last row
    last = price_df.iloc[-1]
    change_pct = float(last.get("change_pct", 0) or 0) if "change_pct" in price_df.columns else 0.0

    # Lower shadow ratio: how much of the day's range is below close
    # High value = buyers stepped in at lows (positive sign during a down day)
    lower_shadow = 0.5  # default neutral
    if "high" in price_df.columns and "low" in price_df.columns and "close" in price_df.columns:
        h = float(last.get("high", last["close"]) or last["close"])
        l = float(last.get("low",  last["close"]) or last["close"])
        c = float(last["close"])
        day_range = h - l
        if day_range > 0:
            lower_shadow = (c - l) / day_range  # 0=close at low, 1=close at high

    # --- Buy score: cross volume ratio with price direction ---
    vol_signal = "normal"
    if ratio >= 1.5:
        if change_pct >= 1.0:
            # 放量上涨 — confirmed breakout/accumulation
            score = 5.0 + (ratio - 1.5) / 1.5 * 5.0
            score = min(10.0, score)
            vol_signal = "volume breakout confirmed (price up)"
        elif change_pct <= -2.0 and lower_shadow >= 0.4:
            # 放量下跌但有长下影线 — low-level absorption, uncertain
            score = 5.0
            vol_signal = "high volume drop with lower shadow (possible bottom)"
        elif change_pct <= -2.0:
            # 放量大阴线 — distribution, NOT a buy signal
            score = 2.0
            vol_signal = "high volume red candle (distribution warning)"
        else:
            # Small move, moderate signal
            score = 5.0 + (ratio - 1.5) / 1.5 * 2.0
            score = min(7.0, score)
            vol_signal = "active volume (direction unclear)"
    elif ratio >= 1.0:
        score = 5.0
        vol_signal = "normal"
    elif ratio >= 0.5:
        if change_pct <= -0.5:
            # 缩量下跌 — selling exhausted, mild buy
            score = 5.0
            vol_signal = "low volume decline (selling exhausted)"
        else:
            score = ratio / 0.5 * 5.0
            vol_signal = "below average volume"
    else:
        if change_pct <= -0.5:
            # Very low volume decline — possible bottoming (stronger signal if at 52w low)
            score = 4.0
            vol_signal = "very low volume decline (possible bottom)"
        else:
            score = 0.0
            vol_signal = "very low volume"

    # --- Sell score: cross volume ratio with price direction ---
    sell_score = 0.0
    if ratio >= 1.5 and change_pct <= -2.0:
        if lower_shadow < 0.3:
            # 放量大阴线 — distribution signal
            strength = min(1.0, (ratio - 1.5) / 3.5)
            sell_score = 7.0 + strength * 2.0  # 7-9 pts
            vol_signal = "high volume red candle (distribution)"
        else:
            sell_score = 4.0  # lower shadow present, less certain
    elif ratio < 0.5 and change_pct >= 1.0:
        # 缩量上涨 — unsustainable rally
        sell_score = 5.0
        vol_signal = "low volume rally (unsustainable)"
    elif ratio > 5.0:
        sell_score = 6.0  # keep extreme climax signal
    else:
        sell_score = 0.0

    # --- MA trend cross: uptrend vs downtrend context for volume events ---
    ma_bull = None
    try:
        if len(price_df) >= 20 and "close" in price_df.columns:
            ma5  = float(price_df["close"].tail(5).mean())
            ma20 = float(price_df["close"].tail(20).mean())
            ma_bull = ma5 > ma20
    except Exception:
        pass

    if ma_bull is not None and ratio >= 1.5:
        if ma_bull:
            # Volume surge in uptrend = genuine accumulation / breakout confirmation
            score = min(10.0, score + 1.5)
            vol_signal = vol_signal + " (uptrend — accumulation confirmed)"
        else:
            # Volume surge in downtrend = distribution / panic selling amplified
            sell_score = min(10.0, sell_score + 1.5)
            vol_signal = vol_signal + " (downtrend — distribution confirmed)"

    # --- 52w price position cross: volume events carry completely different meaning at highs vs lows ---
    position_vb = None
    try:
        window = price_df["close"].tail(252)
        hi = float(window.max())
        lo = float(window.min())
        cur = float(window.iloc[-1])
        if hi > lo:
            position_vb = (cur - lo) / (hi - lo)
    except Exception:
        pass

    if position_vb is not None and ratio >= 1.5:
        if change_pct >= 1.0 and position_vb < 0.3:
            # Volume breakout at 52w low: institutional base-building, highest-conviction bottom signal
            score = min(10.0, score + 2.0)
            vol_signal = vol_signal + " (at 52w low — base breakout, institutional entry)"
        elif change_pct >= 1.0 and position_vb > 0.7:
            # Volume breakout at 52w high: possible blow-off top or distribution into strength
            sell_score = min(10.0, sell_score + 1.5)
            vol_signal = vol_signal + " (at 52w high — possible distribution top)"
        elif change_pct <= -2.0 and position_vb < 0.3:
            # High-volume selloff already at lows: panic capitulation, not a new trend start
            sell_score = max(0.0, sell_score - 1.5)
            vol_signal = vol_signal + " (at 52w low — panic capitulation, sell urgency reduced)"

    # --- Market regime cross: volume breakout reliability differs in bull vs bear ---
    if market_regime_score is not None and ratio >= 1.5:
        if change_pct >= 1.0 and market_regime_score >= 7:
            # Bull market volume breakout upward: institutional participation, higher follow-through
            score = min(10.0, score + 1.0)
            vol_signal = vol_signal + " (bull market — 牛市放量持续性更强)"
        elif change_pct <= -2.0 and market_regime_score <= 3:
            # Bear market high-volume selloff: panic exit amplified by macro environment
            sell_score = min(10.0, sell_score + 1.0)
            vol_signal = vol_signal + " (bear market — 熊市放量下跌加速出逃)"

    # --- Industry excess cross: volume event reliability shaped by sector direction ---
    if industry_ret_1m is not None and market_ret_1m is not None and ratio >= 1.5:
        excess = industry_ret_1m - market_ret_1m
        if change_pct >= 1.0:
            if excess >= 3.0:
                # Volume breakout up + hot sector: technical and sector tailwind double-confirm
                score = min(10.0, score + 1.5)
                vol_signal = vol_signal + f" (industry outperforming {excess:+.1f}% — 技术+行业双重确认)"
            elif excess <= -3.0:
                # Volume up but sector distributing: isolated move, possible insider unloading
                sell_score = min(10.0, sell_score + 1.0)
                vol_signal = vol_signal + f" (industry weak {excess:+.1f}% — 全市场出货，警惕减持)"
        elif change_pct <= -2.0 and excess <= -3.0:
            # High-volume selloff + weak sector: sector drags further, amplify distribution
            sell_score = min(10.0, sell_score + 1.0)
            vol_signal = vol_signal + f" (industry weak {excess:+.1f}% — 行业+个股双杀)"

    # --- Concept cross: theme-driven volume vs theme-less volume ---
    if best_concept_ret is not None and ratio >= 1.5 and change_pct >= 1.0:
        if best_concept_ret >= 8.0:
            # Volume breakout up + hot concept board: concept money fuelling the move, high follow-through
            score = min(10.0, score + 2.0)
            vol_signal = vol_signal + f" (hot concept {best_concept_ret:+.1f}% — 概念资金驱动，持续性强)"
        elif best_concept_ret <= 0.0:
            # Volume up but no concept heat: no retail catalyst, may be large-holder unloading
            score = max(0.0, score - 1.0)
            vol_signal = vol_signal + f" (cold concept {best_concept_ret:+.1f}% — 无主题催化，警惕减持)"

    # --- Earnings revision cross: informed breakout vs noise breakout ---
    revision_signal_vb = None
    if revision_df is not None and not revision_df.empty and ratio >= 1.5:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_vb   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_vb = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_vb  = up_vb - down_vb
                if change_pct >= 2.0 and net_vb >= 2:
                    # Volume breakout up + analyst upgrades: informed institutional buying, not noise
                    score = min(10.0, score + 1.5)
                    revision_signal_vb = f"放量突破+分析师上调({net_vb:+d}家) — 知情资金推动，非噪音突破"
                elif change_pct <= -2.0 and net_vb <= -2:
                    # High-volume decline + analyst downgrades: institutional distribution confirmed
                    sell_score = min(10.0, sell_score + 1.0)
                    revision_signal_vb = f"放量下跌+分析师下调({net_vb:+d}家) — 机构分发确认，非底部恐慌"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "volume_ratio_vs_ma20": round(ratio, 2),
            "change_pct": round(change_pct, 2),
            "lower_shadow_ratio": round(lower_shadow, 2),
            "ma_bull": ma_bull,
            "position_52w": round(position_vb, 3) if position_vb is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "best_concept_ret": round(best_concept_ret, 1) if best_concept_ret is not None else None,
            "revision_signal": revision_signal_vb,
            "vol_signal": vol_signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_52w_position(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
) -> dict:
    """
    52-week range position score (max 5).
    Position = (current - 52w_low) / (52w_high - 52w_low)
    Higher position means stronger uptrend; score rewards both ends:
      >80% of range -> 5 pts (near 52w high, strong trend)
      40-80%        -> 3 pts (mid-range)
      <20%          -> 1 pt  (near 52w low, weak trend)
    Note: a stock near its 52w low could be value or distress — weight accordingly.

    Market regime cross (requires market_regime_score):
      Low position (< 0.3) + bull market (regime >= 7) -> buy +1 (低位+牛市=底部反弹空间大)
      High position (> 0.7) + bear market (regime <= 3) -> sell +1.5 (高位+熊市=最危险的组合)

    Volume cross (requires price_df with volume):
      Low position (< 0.3) + expanding volume (recent > avg) -> buy +1.5 (低位放量=底部承接)
      High position (> 0.7) + contracting volume (recent < avg) -> sell +1.5 (高位缩量=上涨乏力)
    """
    if price_df is None or len(price_df) < 20 or "close" not in price_df.columns:
        return {"score": 2.5, "sell_score": 0.0, "max": 5, "details": {"source": "no data, neutral", "sell_score": 0.0}}

    # Use up to 252 trading days (~1 year)
    window = price_df["close"].tail(252)
    high_52w = float(window.max())
    low_52w  = float(window.min())
    current  = float(window.iloc[-1])

    if high_52w <= low_52w:
        return {"score": 2.5, "sell_score": 0.0, "max": 5, "details": {"position": None, "sell_score": 0.0}}

    position = (current - low_52w) / (high_52w - low_52w)  # 0 to 1

    if position >= 0.8:
        score = 5.0
    elif position >= 0.4:
        score = 3.0
    elif position >= 0.2:
        score = 2.0
    else:
        score = 1.0

    # --- Sell score: contextual, not strong standalone signal ---
    # Cap at 3pts; only mild caution near 52w high
    if position >= 0.95:
        sell_score = 2.0  # within 5% of 52w high
    else:
        sell_score = 0.0

    # --- Market regime cross: price level risk/opportunity varies by market environment ---
    regime_signal = None
    if market_regime_score is not None:
        if position < 0.3 and market_regime_score >= 7:
            # Low position in bull market: beaten-down stock with most room to recover
            score = min(5.0, score + 1.0)
            regime_signal = f"低位+牛市({market_regime_score:.1f}) — 底部反弹空间大"
        elif position > 0.7 and market_regime_score <= 3:
            # High position in bear market: most dangerous combination
            sell_score = min(5.0, sell_score + 1.5)
            regime_signal = f"高位+熊市({market_regime_score:.1f}) — 最危险的价格/市场组合"

    # --- Volume cross: volume confirms or denies the price level signal ---
    vol_signal = None
    if "volume" in price_df.columns and len(price_df) >= 21:
        try:
            vol = price_df["volume"]
            avg_vol = float(vol.iloc[-21:-1].mean())
            cur_vol = float(vol.iloc[-1])
            if avg_vol > 0:
                vol_ratio = cur_vol / avg_vol
                if position < 0.3 and vol_ratio > 1.3:
                    # Low position + expanding volume: buyers stepping in at low prices
                    score = min(5.0, score + 1.5)
                    vol_signal = f"低位+放量({vol_ratio:.2f}x) — 底部承接，买入确认"
                elif position > 0.7 and vol_ratio < 0.7:
                    # High position + shrinking volume: rally losing momentum near top
                    sell_score = min(5.0, sell_score + 1.5)
                    vol_signal = f"高位+缩量({vol_ratio:.2f}x) — 上涨乏力，注意顶部"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 5,
        "details": {
            "position_pct": round(position * 100, 1),
            "high_52w": round(high_52w, 2),
            "low_52w":  round(low_52w, 2),
            "current":  round(current, 2),
            "market_regime_score": market_regime_score,
            "regime_signal": regime_signal,
            "volume_signal": vol_signal,
            "sell_score": round(sell_score, 1),
        },
    }


# ---------------------------------------------------------------------------
# Additional extended factors
# ---------------------------------------------------------------------------

def score_dividend_yield(
    div_yield: Optional[float],
    financial_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    price_df: Optional[pd.DataFrame] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Dividend yield factor score (max 10).
    Rewards consistent cash-return to shareholders.
      >= 5%  -> 10 pts (high income)
      >= 3%  ->  7 pts
      >= 1%  ->  3 pts
      == 0   ->  0 pts (no dividend)

    Financial sustainability cross (applied when yield >= 2%):
      High yield + ROE >= 12% + debt <= 60% -> sustainable, buy +1.5
      High yield + ROE < 5% or debt > 70%   -> dividend trap risk, sell +3

    Earnings trend cross (applied when yield >= 4%): is the payout trajectory safe?
      High yield + profit growth < -20% -> earnings collapsing, payout likely cut, sell +2
      High yield + profit growth > 5%   -> growing earnings support dividend, buy +1.5

    Zero-dividend + high ROE (>= 20%): retained earnings compounding, remove penalty, buy +1

    Market regime cross (requires market_regime_score):
      High yield (>= 3%) + bear market (regime <= 3) -> buy +2 (防御性资产溢价，熊市高股息是避风港)
      High yield + bull market (regime >= 7)          -> buy -1 (牛市资金偏好成长，高股息吸引力下降)

    52w position cross (requires price_df, applied when yield >= 3%):
      High yield + low position (< 0.3) -> buy +1.5 (低位高股息=价格/收益双重保护)
      High yield + high position (> 0.7) -> buy -0.5 (高位高股息=估值压力较大)
    """
    if div_yield is None or div_yield <= 0:
        # Zero dividend: mild penalty unless earnings are being compounded at high ROE
        zero_sell = 2.0
        zero_score = 0.0
        zero_signal = "no dividend"
        if financial_df is not None and not financial_df.empty:
            for key in ["净资产收益率(%)", "加权净资产收益率(%)"]:
                if key in financial_df.columns:
                    vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                    if not vals.empty and float(vals.iloc[0]) >= 20:
                        zero_score = 1.0
                        zero_sell  = 0.0
                        zero_signal = "no dividend (high-ROE compounder — retained earnings reinvested)"
                    break
        return {"score": zero_score, "sell_score": zero_sell, "max": 10,
                "details": {"div_yield_pct": div_yield, "signal": zero_signal, "sell_score": zero_sell}}

    if div_yield >= 5:
        score = 10.0
    elif div_yield >= 3:
        score = 7.0 + (div_yield - 3) / 2 * 3.0
    elif div_yield >= 1:
        score = 3.0 + (div_yield - 1) / 2 * 4.0
    else:
        score = div_yield * 3.0

    signal = "high" if div_yield >= 4 else ("moderate" if div_yield >= 2 else "low")
    sell_score = 0.0

    # --- Financial sustainability cross: can the business afford this dividend? ---
    if financial_df is not None and not financial_df.empty and div_yield >= 2:
        roe = None
        debt_ratio = None
        for key in ["净资产收益率(%)", "加权净资产收益率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe = float(vals.iloc[0])
                break
        for key in ["资产负债率(%)", "负债率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    debt_ratio = float(vals.iloc[0])
                break

        if roe is not None:
            if roe >= 12 and (debt_ratio is None or debt_ratio <= 60):
                # Strong profitability + manageable leverage: dividend is well-supported
                score = min(10.0, score + 1.5)
                signal = "sustainable " + signal + " yield"
            elif roe < 5 or (debt_ratio is not None and debt_ratio > 70):
                # Poor returns or high debt: dividend likely to be cut or erode equity
                sell_score = min(10.0, sell_score + 3.0)
                signal = signal + " yield (dividend trap risk)"

    # --- Earnings trend cross: is the payout trajectory safe or deteriorating? ---
    if financial_df is not None and not financial_df.empty and div_yield >= 4:
        profit_growth = None
        for key in ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    profit_growth = float(vals.iloc[0])
                break
        if profit_growth is not None:
            if profit_growth < -20:
                # Earnings collapsing while yield is high: yield will almost certainly be cut
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + f" (earnings down {profit_growth:.0f}% — payout likely unsustainable)"
            elif profit_growth > 5:
                # Growing earnings underpin and may grow the dividend over time
                score = min(10.0, score + 1.5)
                signal = signal + f" (earnings +{profit_growth:.0f}% — dividend sustainable and growing)"

    # --- Market regime cross: defensive appeal of dividends varies by market environment ---
    if market_regime_score is not None and div_yield >= 3:
        if market_regime_score <= 3:
            # Bear market: high-yield stocks attract defensive capital rotation
            score = min(10.0, score + 2.0)
            signal = signal + " (bear market — 防御性溢价，高股息是避风港)"
        elif market_regime_score >= 7:
            # Bull market: growth/momentum crowded out dividends; yield less attractive
            score = max(0.0, score - 1.0)
            signal = signal + " (bull market — 牛市资金偏成长，高股息吸引力下降)"

    # --- 52w position cross: price level determines margin-of-safety quality of yield ---
    position_signal = None
    if price_df is not None and div_yield >= 3:
        pos = _get_price_position_f(price_df)
        if pos is not None:
            if pos < 0.3:
                # High yield + low price: income protection + potential capital gain
                score = min(10.0, score + 1.5)
                position_signal = f"高股息+低位({pos:.2f}) — 价格/收益双重保护"
            elif pos > 0.7:
                # High yield + high price: less margin of safety, priced for perfection
                score = max(0.0, score - 0.5)
                position_signal = f"高股息+高位({pos:.2f}) — 估值压力较大，安全边际受限"

    # --- Industry excess cross: sector direction determines if yield is opportunity or trap ---
    industry_signal_dy = None
    if industry_ret_1m is not None and market_ret_1m is not None and div_yield >= 3:
        excess_dy = industry_ret_1m - market_ret_1m
        if excess_dy >= 3.0:
            score = min(10.0, score + 1.5)
            industry_signal_dy = f"高股息+行业强(超额{excess_dy:+.1f}%) — 价值+动量双击，股息修复有催化剂"
        elif excess_dy <= -3.0:
            sell_score = min(10.0, sell_score + 1.0)
            industry_signal_dy = f"高股息+行业弱(超额{excess_dy:+.1f}%) — 股息陷阱风险，行业下行侵蚀分红能力"

    # --- Earnings revision cross: analyst view on dividend sustainability ---
    revision_signal_dy = None
    if revision_df is not None and not revision_df.empty and div_yield >= 3:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_dy   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_dy = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_dy  = up_dy - down_dy
                if net_dy <= -2:
                    # High yield + analyst downgrades: dividend sustainability at risk
                    sell_score = min(10.0, sell_score + 1.5)
                    revision_signal_dy = f"高股息+分析师下调({net_dy:+d}家) — 盈利能力下滑，股息可持续性存疑"
                elif net_dy >= 2:
                    # High yield + analyst upgrades: dividend confirmed sustainable, double value signal
                    score = min(10.0, score + 1.0)
                    revision_signal_dy = f"高股息+分析师上调({net_dy:+d}家) — 分红可持续性确认，双重价值信号"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "div_yield_pct": round(div_yield, 2),
            "market_regime_score": market_regime_score,
            "position_signal": position_signal,
            "industry_signal": industry_signal_dy,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "revision_signal": revision_signal_dy,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_volume_ratio(
    volume_ratio: Optional[float],
    change_pct: Optional[float] = None,
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Volume ratio (量比) factor score (max 10).
    量比 = today's volume / 5-day average volume.
    Crossed with price direction (change_pct) to distinguish accumulation from distribution.
      放量上涨 -> boost buy score
      放量下跌 -> heavily reduce buy score, boost sell score
      缩量下跌 -> mild buy boost (selling exhausted)
      缩量上涨 -> sell warning (unsustainable rally)

    52-week position cross (requires price_df):
      量比 >= 2.5 + position < 0.3  -> 底部放量，最强买入信号 -> buy +2
      量比 >= 2.5 + position > 0.7  -> 高位放量，注意顶部 -> sell +1.5
      量比 < 0.8 + price down + position < 0.3 -> 低位缩量，卖压减轻 -> buy +1

    Market regime cross (requires market_regime_score):
      量比 >= 2 + price up + bull market (regime >= 7)  -> buy +1 (牛市放量持续性更强)
      量比 >= 2 + price down + bear market (regime <= 3) -> sell +1.5 (熊市放量下跌更危险)
    """
    if volume_ratio is None or volume_ratio <= 0:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"volume_ratio": volume_ratio, "signal": "no data, neutral", "sell_score": 0.0}}

    if volume_ratio > 5:
        score = 6.0
        signal = "climax volume (caution)"
    elif volume_ratio >= 2.5:
        score = 10.0
        signal = "strong accumulation"
    elif volume_ratio >= 1.5:
        score = 5.0 + (volume_ratio - 1.5) / 1.0 * 5.0
        signal = "active"
    elif volume_ratio >= 0.8:
        score = 5.0
        signal = "normal"
    else:
        score = volume_ratio / 0.8 * 5.0
        signal = "weak / drying up"

    # Apply price-direction cross if change_pct is available
    if change_pct is not None:
        if change_pct >= 1.0:
            # 放量上涨 — boost buy score
            score = min(10.0, score * 1.2)
            signal = signal + " + price up (confirmed)"
        elif change_pct <= -2.0:
            # 放量下跌 — heavily reduce buy score
            score = score * 0.3
            signal = signal + " + price down (distribution risk)"
        elif change_pct <= -0.5 and (volume_ratio is None or volume_ratio < 0.8):
            # 缩量下跌 — mild buy boost
            score = min(10.0, score + 1.5)
            signal = signal + " + low vol decline (exhaustion)"

    # --- Sell score: climax volume and price-direction cross ---
    if volume_ratio > 8:
        sell_score = 7.0
    elif volume_ratio > 5:
        sell_score = 4.0
    else:
        sell_score = 0.0

    # Cross with price direction
    if change_pct is not None:
        if change_pct <= -2.0 and volume_ratio is not None and volume_ratio >= 1.5:
            # 放量下跌 — distribution
            sell_score = max(sell_score, 7.0)
        elif change_pct >= 1.0 and volume_ratio is not None and volume_ratio < 0.8:
            # 缩量上涨 — unsustainable
            sell_score = max(sell_score, 5.0)

    # --- 52-week position cross: same volume event carries different meaning at highs vs lows ---
    position_vr = None
    if price_df is not None and len(price_df) >= 20 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(252)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo:
                position_vr = (cur - lo) / (hi - lo)
        except Exception:
            pass

    if position_vr is not None:
        if volume_ratio >= 2.5 and position_vr < 0.3:
            # 底部放量: institutional base-building, strongest A-share buy signal
            score = min(10.0, score + 2.0)
            signal = signal + " (at 52w low — 底部放量，最强买入)"
        elif volume_ratio >= 2.5 and position_vr > 0.7:
            # 高位放量: possible blow-off top or distribution into strength
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (at 52w high — 高位放量，注意顶部)"
        elif volume_ratio < 0.8 and change_pct is not None and change_pct <= -0.5 and position_vr < 0.3:
            # 低位缩量下跌: selling pressure drying up at lows
            score = min(10.0, score + 1.0)
            signal = signal + " (at 52w low — 低位缩量，卖压减轻)"

    # --- Market regime cross: volume signal reliability differs in bull vs bear ---
    if market_regime_score is not None and volume_ratio >= 2 and change_pct is not None:
        if change_pct >= 1.0 and market_regime_score >= 7:
            # Bull market + high volume + rising: institutional follow-through is more likely
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — 放量持续性更强)"
        elif change_pct <= -2.0 and market_regime_score <= 3:
            # Bear market + high volume + falling: distribution more likely, amplify sell
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 放量下跌更危险)"

    # --- Earnings revision cross: distinguishes informed trading from noise ---
    revision_signal_vr = None
    if revision_df is not None and not revision_df.empty and volume_ratio >= 2:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_vr   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_vr = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_vr  = up_vr - down_vr
                if change_pct is not None and change_pct >= 1.0 and net_vr >= 2:
                    # High vol + price up + analyst upgrades: informed institutional buying
                    score = min(10.0, score + 1.0)
                    revision_signal_vr = f"高量比+上涨+分析师上调({net_vr:+d}家) — 知情交易，放量有基本面依据"
                elif change_pct is not None and change_pct <= -2.0 and net_vr <= -2:
                    # High vol + price down + analyst downgrades: institutional distribution
                    sell_score = min(10.0, sell_score + 1.0)
                    revision_signal_vr = f"高量比+下跌+分析师下调({net_vr:+d}家) — 机构砸盘，非底部恐慌"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "volume_ratio": round(volume_ratio, 2),
            "change_pct": change_pct,
            "position_52w": round(position_vr, 3) if position_vr is not None else None,
            "market_regime_score": market_regime_score,
            "revision_signal": revision_signal_vr,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_ma_alignment(
    price_df: Optional[pd.DataFrame],
    revision_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Moving-average alignment factor score (max 15).
    Checks four conditions (price > MA5, MA5 > MA10, MA10 > MA20, MA20 > MA60).
    All four met -> perfect bull alignment -> 15 pts.
    All four inverted -> full bearish -> 2 pts.
    Partial alignment scored proportionally.

    Volume cross (量价交叉):
      Perfect bull + volume expanding (v5/v20 > 1.15)  -> buy +2 (confirmed trend)
      Perfect bull + volume shrinking (v5/v20 < 0.75)  -> sell +3 (trend on empty)
      Full bearish + volume expanding (v5/v20 > 1.15)  -> sell +2 (distribution accelerating)
      Full bearish + volume shrinking (v5/v20 < 0.75)  -> sell -2 (selling exhausted)

    Earnings revision cross: fundamental catalyst validates MA structure (requires revision_df)
      Perfect bull MA + net upgrades >= 2  -> buy +2 (technical + fundamental double confirmation)
      Full bear MA + net downgrades <= -2  -> sell +2 (both channels confirming deterioration)

    Market regime cross (requires market_regime_score):
      Perfect bull (4/4) + bull market (regime >= 7) -> buy +1.5 (牛市均线多头延续性更强)
      Perfect bull + bear market (regime <= 3)       -> buy -1, sell +1 (熊市假突破风险高)
      Full bearish + bear market                     -> sell +1.5 (熊市趋势延续性更强)

    Industry excess cross (requires industry_ret_1m, market_ret_1m):
      Perfect bull (4/4) + industry outperforming (excess >= +3%) -> buy +1 (均线多头+行业顺风)
      Perfect bull + industry weak (excess <= -3%)                -> buy -0.5 (逆行业的均线多头)
    """
    if price_df is None or len(price_df) < 20 or "close" not in price_df.columns:
        return {"score": 7.5, "sell_score": 0.0, "max": 15,
                "details": {"alignment": "no data, neutral", "sell_score": 0.0}}

    close = price_df["close"]
    current = float(close.iloc[-1])
    ma5  = float(close.rolling(5).mean().iloc[-1])
    ma10 = float(close.rolling(10).mean().iloc[-1])
    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma60 = float(close.rolling(min(60, len(close))).mean().iloc[-1]) if len(close) >= 30 else None

    conds = [current > ma5, ma5 > ma10, ma10 > ma20]
    if ma60 is not None:
        conds.append(ma20 > ma60)

    bull = sum(conds)
    n = len(conds)

    if bull == n:
        score, label = 15.0, "perfect bull"
    elif bull == n - 1:
        score, label = 11.0, "mostly bull"
    elif bull == n // 2 or bull == (n + 1) // 2:
        score, label = 7.0, "mixed"
    elif bull == 1:
        score, label = 4.0, "mostly bearish"
    else:
        score, label = 2.0, "full bearish"

    # --- Sell score: bearish MA alignment ---
    price_below_ma5 = not conds[0]  # first condition is current > ma5
    if bull == 0:
        sell_score = 13.0   # full bearish (0/n conditions met)
    elif bull == 1:
        sell_score = 9.0    # mostly bearish (1/n)
    elif bull == 2 and price_below_ma5:
        sell_score = 5.0    # mixed-bearish with price below MA5
    else:
        sell_score = 0.0

    # --- Volume cross: does volume confirm the MA structure? ---
    vol_ratio = None
    try:
        if "volume" in price_df.columns and len(price_df) >= 25:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 25:
                v5  = float(vol.tail(5).mean())
                v20 = float(vol.tail(25).head(20).mean())
                if v20 > 0:
                    vol_ratio = v5 / v20
    except Exception:
        pass

    if vol_ratio is not None:
        if bull == n and vol_ratio > 1.15:
            # Perfect bull alignment + expanding volume = fully confirmed trend
            score = min(15.0, score + 2.0)
            label = "perfect bull + volume expanding (confirmed)"
        elif bull == n and vol_ratio < 0.75:
            # Perfect bull alignment + shrinking volume = trend running on empty
            sell_score = min(15.0, sell_score + 3.0)
            label = "perfect bull + volume shrinking (caution: no fuel)"
        elif bull == 0 and vol_ratio > 1.15:
            # Full bearish + expanding volume = distribution accelerating
            sell_score = min(15.0, sell_score + 2.0)
            label = "full bearish + volume expanding (distribution accelerating)"
        elif bull == 0 and vol_ratio < 0.75:
            # Full bearish + shrinking volume = sellers exhausting, possible bottom
            sell_score = max(0.0, sell_score - 2.0)
            label = "full bearish + volume shrinking (selling exhausted)"

    # --- Earnings revision cross: fundamental catalyst validates MA structure ---
    net_revisions = None
    revision_signal = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级变动", "方向", "上调", "下调", "rating"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up   = int(col_str.str.contains("上调|upgrade|buy|strong").sum())
                down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_revisions = up - down
                if bull == n and net_revisions >= 2:
                    # Technical bull + analyst upgrades: two independent signals aligned
                    score = min(15.0, score + 2.0)
                    revision_signal = "bull MA + analyst upgrades (double confirmation)"
                    label = label + " | " + revision_signal
                elif bull == 0 and net_revisions <= -2:
                    # Technical bear + analyst downgrades: both channels confirm deterioration
                    sell_score = min(15.0, sell_score + 2.0)
                    revision_signal = "bear MA + analyst downgrades (double confirmation)"
                    label = label + " | " + revision_signal
        except Exception:
            pass

    details = {
        "alignment": label,
        "conditions_met": f"{bull}/{n}",
        "current": round(current, 2),
        "ma5":  round(ma5, 2),
        "ma10": round(ma10, 2),
        "ma20": round(ma20, 2),
        "vol_ratio_5d_20d": round(vol_ratio, 2) if vol_ratio is not None else None,
        "net_revisions": net_revisions,
        "revision_signal": revision_signal,
        "sell_score": round(sell_score, 1),
    }
    if ma60 is not None:
        details["ma60"] = round(ma60, 2)

    # --- Market regime cross: MA alignment reliability differs in bull vs bear ---
    if market_regime_score is not None:
        if bull == n and market_regime_score >= 7:
            # Perfect bull + bull market: institutional trend-following has real follow-through
            score = min(15.0, score + 1.5)
            details["alignment"] = details["alignment"] + " (bull market — 均线多头延续性更强)"
        elif bull == n and market_regime_score <= 3:
            # Perfect bull in bear market: high risk of head-fake / false breakout
            score = max(0.0, score - 1.0)
            sell_score = min(15.0, sell_score + 1.0)
            details["alignment"] = details["alignment"] + " (bear market — 熊市假突破风险)"
        elif bull == 0 and market_regime_score <= 3:
            # Full bearish in bear market: trend continuation more likely
            sell_score = min(15.0, sell_score + 1.5)
            details["alignment"] = details["alignment"] + " (bear market — 空头排列延续性更强)"

    details["market_regime_score"] = market_regime_score

    # --- Industry excess cross: perfect bull alignment is stronger with sector tailwind ---
    if industry_ret_1m is not None and market_ret_1m is not None and bull == n:
        excess = industry_ret_1m - market_ret_1m
        if excess >= 3:
            # Perfect MA alignment + sector outperforming: double structural confirmation
            score = min(15.0, score + 1.0)
            details["alignment"] = details["alignment"] + f" (industry outperforming {excess:+.1f}% — 均线多头+行业顺风)"
        elif excess <= -3:
            # Perfect MA alignment but sector is weak: stock going up against sector tide
            score = max(0.0, score - 0.5)
            details["alignment"] = details["alignment"] + f" (industry weak {excess:+.1f}% — 逆行业均线多头，打折)"

    details["industry_excess_pct"] = round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None

    # --- 52w price position cross: MA alignment at price extremes has very different meaning ---
    position_ma = _get_price_position_f(price_df)
    if position_ma is not None:
        if bull == n:
            if position_ma < 0.3:
                # Full MA bull at 52w low: breakout just started from base, maximum upside remaining
                score = min(15.0, score + 2.0)
                details["alignment"] = details["alignment"] + f" (52w low {position_ma:.2f} — 低位均线多头启动，最强突破)"
            elif position_ma > 0.8:
                # Full MA bull near 52w high: late-stage rally, distribution risk growing
                sell_score = min(15.0, sell_score + 1.0)
                details["alignment"] = details["alignment"] + f" (52w high {position_ma:.2f} — 高位均线多头尾段，注意回撤)"
        elif bull == 0 and position_ma > 0.7:
            # Full MA bear at 52w high: most dangerous setup, structural decline confirmed
            sell_score = min(15.0, sell_score + 1.5)
            details["alignment"] = details["alignment"] + f" (52w high {position_ma:.2f} — 高位空头排列，最确定下行)"

    details["position_52w"] = round(position_ma, 3) if position_ma is not None else None
    details["sell_score"] = round(sell_score, 1)

    return {"score": round(score, 1), "sell_score": round(sell_score, 1), "max": 15, "details": details}


def score_low_volatility(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Low-volatility factor score (max 10).
    Annualized daily return volatility over the last 60 trading days.
    Lower volatility = more stable trend = higher score.
      <= 15% ann. vol -> 10 pts
      >= 60% ann. vol ->  0 pts

    Trend direction cross (MA5 vs MA20):
      Low vol (<= 25%) + MA5 > MA20 (uptrend)   -> quiet strength, buy +2 (most sustainable rally)
      Low vol (<= 25%) + MA5 < MA20 (downtrend)  -> quiet decay, sell +2 (no panic but no buyers either)

    Market regime cross: defensive value of low-vol is regime-dependent (Ang et al.)
      Low vol (<= 25%) + bear market (regime <= 3) -> defensive premium, buy +2
      Low vol (<= 25%) + bull market (regime >= 7) -> opportunity cost (momentum wins), buy -1.5, sell +1
      High vol (> 45%) + bear market (regime <= 3) -> amplified downside risk, sell +1.5

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      Low vol (<= 25%) + strong industry (excess >= +3%) -> buy +1 (低波动+行业顺风=最稳定的alpha来源)
      Low vol (<= 25%) + weak industry (excess <= -3%)   -> sell +0.5 (低波动但行业逆风=慢刀割肉)
    """
    if price_df is None or len(price_df) < 20 or "close" not in price_df.columns:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"annualized_vol_pct": None, "signal": "no data, neutral", "sell_score": 0.0}}

    daily_ret = price_df["close"].tail(60).pct_change().dropna()
    if len(daily_ret) < 10:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"annualized_vol_pct": None, "signal": "insufficient data", "sell_score": 0.0}}

    ann_vol = float(daily_ret.std() * np.sqrt(252) * 100)

    if ann_vol <= 15:
        score = 10.0
    elif ann_vol >= 60:
        score = 0.0
    else:
        score = 10.0 * (60 - ann_vol) / 45

    signal = "low" if ann_vol <= 25 else ("medium" if ann_vol <= 45 else "high")

    # --- Sell score: volatility spike (sudden uncertainty) ---
    # Note: high vol alone isn't sell; only extreme spike
    if ann_vol > 80:
        sell_score = 8.0
    elif ann_vol > 60:
        sell_score = 5.0
    else:
        sell_score = 0.0

    # --- Trend direction cross: low vol means different things in uptrend vs downtrend ---
    ma_bull = None
    try:
        ma5  = float(price_df["close"].tail(5).mean())
        ma20 = float(price_df["close"].tail(20).mean())
        ma_bull = ma5 > ma20
    except Exception:
        pass

    if ma_bull is not None and ann_vol <= 25:
        if ma_bull:
            # Low volatility in uptrend: quiet accumulation, most sustainable rally pattern
            score = min(10.0, score + 2.0)
            signal = "low vol (uptrend — quiet strength, most sustainable)"
        else:
            # Low volatility in downtrend: quiet decay, no panic but nobody buying either
            sell_score = min(10.0, sell_score + 2.0)
            signal = "low vol (downtrend — quiet decay, slow bleed)"

    # --- Market regime cross: low-vol alpha is regime-dependent ---
    if market_regime_score is not None:
        if ann_vol <= 25 and market_regime_score <= 3:
            # Bear market: low-vol stocks preserve capital, defensive premium activated
            score = min(10.0, score + 2.0)
            signal = signal + " (bear market — defensive premium)"
        elif ann_vol <= 25 and market_regime_score >= 7:
            # Bull market: risk-on environment, low-vol = opportunity cost vs high-beta
            score = max(0.0, score - 1.5)
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bull market — low-vol lags, opportunity cost)"
        elif ann_vol > 45 and market_regime_score <= 3:
            # High beta + bear market = amplified downside
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (high vol in bear market — amplified downside risk)"

    # --- Industry excess return cross: tailwind/headwind context for low-vol stocks ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None and ann_vol <= 25:
        excess = industry_ret_1m - market_ret_1m
        if excess >= 3.0:
            # Low volatility + strong sector: most sustainable source of alpha
            score = min(10.0, score + 1.0)
            industry_signal = f"低波动+行业强(超额{excess:.1f}%) — 最稳定的alpha来源"
        elif excess <= -3.0:
            # Low volatility + weak sector: slow bleed, no panic but no catalyst either
            sell_score = min(10.0, sell_score + 0.5)
            industry_signal = f"低波动+行业弱(超额{excess:.1f}%) — 慢刀割肉，行业拖累"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "annualized_vol_pct": round(ann_vol, 1),
            "ma_bull": ma_bull,
            "industry_signal": industry_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


# ---------------------------------------------------------------------------
# Technical analysis (unchanged)
# ---------------------------------------------------------------------------

def compute_technical(price_df: Optional[pd.DataFrame]) -> dict:
    """Compute key technical indicators: moving averages, MACD, RSI."""
    if price_df is None or len(price_df) < 30:
        return {"error": "Insufficient price data for technical analysis"}

    try:
        import ta
        close = price_df["close"]

        ma5  = float(close.rolling(5).mean().iloc[-1])
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma60 = float(close.rolling(min(60, len(close))).mean().iloc[-1])
        current = float(close.iloc[-1])

        macd_ind    = ta.trend.MACD(close)
        macd        = float(macd_ind.macd().iloc[-1])
        macd_signal = float(macd_ind.macd_signal().iloc[-1])
        macd_diff   = float(macd_ind.macd_diff().iloc[-1])

        rsi = float(ta.momentum.RSIIndicator(close, window=14).rsi().iloc[-1])

        if ma5 > ma20 > ma60:
            ma_trend = "bullish alignment"
        elif ma5 < ma20 < ma60:
            ma_trend = "bearish alignment"
        else:
            ma_trend = "sideways"

        return {
            "current_price":   round(current, 2),
            "ma5":             round(ma5, 2),
            "ma20":            round(ma20, 2),
            "ma60":            round(ma60, 2),
            "ma_trend":        ma_trend,
            "price_vs_ma20":   "above MA20" if current > ma20 else "below MA20",
            "macd":            round(macd, 4),
            "macd_signal":     round(macd_signal, 4),
            "macd_diff":       round(macd_diff, 4),
            "macd_signal_text": "golden cross (bullish)" if macd_diff > 0 else "death cross (bearish)",
            "rsi":             round(rsi, 1),
            "rsi_signal":      "overbought" if rsi > 70 else ("oversold" if rsi < 30 else "neutral"),
        }
    except Exception as e:
        return {"error": f"Technical indicator computation failed: {e}"}


# ---------------------------------------------------------------------------
# Market-regime weight multipliers
# ---------------------------------------------------------------------------

def _get_regime_multipliers(regime_score: Optional[float]) -> dict:
    """
    Per-factor weight multipliers for the BUY score based on CSI 300 regime.
    Bull market  (≥7): boost momentum/MA-alignment, dampen reversal (contrarian hurts)
    Bear market  (≤3): suppress momentum, amplify reversal/value/low-vol
    Neutral (3–7): no adjustment
    """
    if regime_score is None:
        return {}
    if regime_score >= 7:
        return {
            "momentum":          1.3,
            "ma_alignment":      1.2,
            "reversal":          0.6,   # fighting the bull trend is risky
            "low_volatility":    0.8,   # low-vol underperforms in strong rallies
            "chip_distribution": 0.9,
        }
    elif regime_score <= 3:
        return {
            "momentum":          0.4,   # most breakouts fail in bear markets
            "reversal":          1.6,   # oversold bounces are frequent & large
            "value":             1.3,   # cheap stocks offer downside buffer
            "low_volatility":    1.2,   # low-vol holds up best in bear
            "chip_distribution": 1.2,   # selling pressure signal more meaningful
            "ma_alignment":      0.7,   # MA bearish signal already well-known
        }
    return {}


def _get_regime_sell_multipliers(regime_score: Optional[float]) -> dict:
    """
    Per-factor weight multipliers for the SELL score based on CSI 300 regime.
    Bull market  (≥7): ease position/valuation sell sensitivity (overbought = normal)
    Bear market  (≤3): amplify momentum/position sell signals
    """
    if regime_score is None:
        return {}
    if regime_score >= 7:
        return {
            "position_52w": 0.7,   # high position fine in bull
            "value":        0.7,   # stretched PE is normal in bull runs
            "momentum":     0.8,   # pullback not necessarily a sell in bull
        }
    elif regime_score <= 3:
        return {
            "momentum":          1.4,   # downtrend acceleration more dangerous
            "position_52w":      1.3,   # high position in bear = trapped capital
            "chip_distribution": 1.2,   # chip-level supply pressure amplified
        }
    return {}


# ---------------------------------------------------------------------------
# Weighted total score
# ---------------------------------------------------------------------------

def compute_total_score(
    value:        dict,
    growth:       dict,
    momentum:     dict,
    quality:      dict,
    northbound:   dict,
    volume:       dict,
    position_52w: dict,
    weights:      FactorWeights = DEFAULT_WEIGHTS,
    extra_factors: Optional[dict] = None,
    market_regime_score: Optional[float] = None,
) -> float:
    """
    Compute a weighted composite score normalised to [0, 100].
    Each factor is normalised to [0, 1] by dividing by its max, multiplied
    by its weight, and the weighted average is scaled to 100.

    extra_factors: dict mapping FactorWeights field names -> score dicts.
      e.g. {"div_yield": div_factor, "piotroski": p_factor, ...}
    market_regime_score: buy_score from score_market_regime (0–10).
      When provided, dynamically adjusts weights based on bull/bear environment.
    """
    rm = _get_regime_multipliers(market_regime_score)

    # Core 7 factors (always included)
    pairs: list[tuple[dict, float]] = [
        (value,        weights.value        * rm.get("value", 1.0)),
        (growth,       weights.growth       * rm.get("growth", 1.0)),
        (momentum,     weights.momentum     * rm.get("momentum", 1.0)),
        (quality,      weights.quality      * rm.get("quality", 1.0)),
        (northbound,   weights.northbound   * rm.get("northbound", 1.0)),
        (volume,       weights.volume       * rm.get("volume", 1.0)),
        (position_52w, weights.position_52w * rm.get("position_52w", 1.0)),
    ]

    # Extended factors via name lookup on FactorWeights
    if extra_factors:
        for name, f_dict in extra_factors.items():
            if f_dict is None:
                continue
            w = getattr(weights, name, 0.0) * rm.get(name, 1.0)
            if w > 0:
                pairs.append((f_dict, w))

    weighted_sum = 0.0
    weight_total = 0.0

    for f_dict, w in pairs:
        if w <= 0:
            continue
        mx = f_dict.get("max", 1) or 1
        normalized = f_dict.get("score", 0) / mx
        weighted_sum += normalized * w
        weight_total += w

    if weight_total == 0:
        return 0.0

    return round(weighted_sum / weight_total * 100, 1)


def compute_sell_score(
    value:        dict,
    growth:       dict,
    momentum:     dict,
    quality:      dict,
    northbound:   dict,
    volume:       dict,
    position_52w: dict,
    weights:      FactorWeights = DEFAULT_WEIGHTS,
    extra_factors: Optional[dict] = None,
    market_regime_score: Optional[float] = None,
) -> float:
    """
    Compute weighted composite SELL score normalised to [0, 100].
    Uses sell_score from each factor dict instead of score (buy_score).

    extra_factors: dict mapping FactorWeights field names -> score dicts.
    market_regime_score: buy_score from score_market_regime (0–10).
      When provided, amplifies sell signals in bear markets and eases them in bull.
    """
    rm = _get_regime_sell_multipliers(market_regime_score)

    # Core 7 factors (always included)
    pairs: list[tuple[dict, float]] = [
        (value,        weights.value        * rm.get("value", 1.0)),
        (growth,       weights.growth       * rm.get("growth", 1.0)),
        (momentum,     weights.momentum     * rm.get("momentum", 1.0)),
        (quality,      weights.quality      * rm.get("quality", 1.0)),
        (northbound,   weights.northbound   * rm.get("northbound", 1.0)),
        (volume,       weights.volume       * rm.get("volume", 1.0)),
        (position_52w, weights.position_52w * rm.get("position_52w", 1.0)),
    ]

    # Extended factors via name lookup on FactorWeights
    if extra_factors:
        for name, f_dict in extra_factors.items():
            if f_dict is None:
                continue
            w = getattr(weights, name, 0.0) * rm.get(name, 1.0)
            if w > 0:
                pairs.append((f_dict, w))

    weighted_sum = 0.0
    weight_total = 0.0

    for f_dict, w in pairs:
        if w <= 0:
            continue
        mx = f_dict.get("max", 1) or 1
        sell = f_dict.get("sell_score", 0.0)
        normalized = sell / mx
        weighted_sum += normalized * w
        weight_total += w

    if weight_total == 0:
        return 0.0

    return round(weighted_sum / weight_total * 100, 1)
