from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from .._utils import _extract, _neutral, _get_price_position


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
        window = price_df["close"].tail(260)
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


def score_amihud_illiquidity(
    price_df: Optional[pd.DataFrame],
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


def score_overhead_resistance(
    cyq_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Measure overhead resistance (套牢盘压力) from East Money chip distribution data
    (ak.stock_cyq_em).

    Returns {"score": float, "sell_score": float, "max": 10, "details": dict}.
    Neutral score is 4.0 (overhead pressure is slightly bearish by default).

    Logic:
      1. If cyq_df is None/empty → neutral {"score": 5.0, "sell_score": 5.0}.
      2. Get current price from price_df (last close); fall back to inferring from cyq_df.
      3. overhead_ratio = fraction of chips ABOVE current price (套牢盘比例).
      4. Score inversely with overhead_ratio:
           0–10%  → score=8.0  (little overhead, clean chart)
           10–30% → linear 8.0→5.0
           30–60% → linear 5.0→2.0
           >60%   → score=1.0  (heavily trapped)
      5. sell_score = 10.0 - score
      6. Context cross: if overhead_ratio > 50% AND price is in lower 30% of 52w range
         → subtract 1.5 from sell_score (bottoming pattern; trapped chips at historical
           highs while price is at historical low = potential reversal).
    """
    MAX = 10
    NEUTRAL_SCORE = 5.0
    NEUTRAL_SELL  = 5.0

    # ── 1. Guard: no data ───────────────────────────────────────────────────
    if cyq_df is None or cyq_df.empty:
        return {
            "score": NEUTRAL_SCORE,
            "sell_score": NEUTRAL_SELL,
            "max": MAX,
            "details": {"signal": "no cyq data, neutral"},
        }

    try:
        # ── 2. Get current price ────────────────────────────────────────────
        current_price: Optional[float] = None

        if price_df is not None and not price_df.empty and "close" in price_df.columns:
            closes = pd.to_numeric(price_df["close"], errors="coerce").dropna()
            if not closes.empty:
                current_price = float(closes.iloc[-1])

        # Fallback: try to infer current price from cyq_df itself.
        # stock_cyq_em may return columns: price, percent (chip density per price level).
        # Use the price with the highest chip density near the recent traded area, or
        # just take the 获利比例-weighted average cost as a proxy.
        if current_price is None:
            # Try "平均成本" column
            for col in ("平均成本", "average_cost"):
                if col in cyq_df.columns:
                    vals = pd.to_numeric(cyq_df[col], errors="coerce").dropna()
                    if not vals.empty:
                        current_price = float(vals.iloc[0])
                        break

        if current_price is None or current_price <= 0:
            return {
                "score": NEUTRAL_SCORE,
                "sell_score": NEUTRAL_SELL,
                "max": MAX,
                "details": {"signal": "cannot determine current price, neutral"},
            }

        # ── 3. Calculate overhead_ratio ─────────────────────────────────────
        # stock_cyq_em typically returns columns: price, percent
        # (chip density at each price level; percent sums to ~100).
        overhead_ratio: Optional[float] = None

        if "price" in cyq_df.columns and "percent" in cyq_df.columns:
            prices   = pd.to_numeric(cyq_df["price"],   errors="coerce")
            percents = pd.to_numeric(cyq_df["percent"], errors="coerce")
            valid    = prices.notna() & percents.notna()
            if valid.sum() > 0:
                total_pct    = float(percents[valid].sum())
                overhead_pct = float(percents[valid & (prices > current_price)].sum())
                if total_pct > 0:
                    overhead_ratio = overhead_pct / total_pct

        # Fallback: use 获利比例 column — fraction of chips currently at a profit.
        # If 获利比例 = X%, then (100 - X)% of chips are above current price (trapped).
        if overhead_ratio is None:
            for col in ("获利比例", "profit_ratio"):
                if col in cyq_df.columns:
                    vals = pd.to_numeric(cyq_df[col], errors="coerce").dropna()
                    if not vals.empty:
                        profit_ratio   = float(vals.iloc[0]) / 100.0  # convert % → fraction
                        overhead_ratio = max(0.0, min(1.0, 1.0 - profit_ratio))
                        break

        if overhead_ratio is None:
            return {
                "score": NEUTRAL_SCORE,
                "sell_score": NEUTRAL_SELL,
                "max": MAX,
                "details": {"signal": "cannot compute overhead_ratio, neutral"},
            }

        overhead_ratio = max(0.0, min(1.0, float(overhead_ratio)))

        # ── 4. Map overhead_ratio → buy score ──────────────────────────────
        if overhead_ratio <= 0.10:
            score = 8.0
        elif overhead_ratio <= 0.30:
            # linear 8.0 → 5.0 over [0.10, 0.30]
            t     = (overhead_ratio - 0.10) / 0.20
            score = 8.0 - t * 3.0
        elif overhead_ratio <= 0.60:
            # linear 5.0 → 2.0 over [0.30, 0.60]
            t     = (overhead_ratio - 0.30) / 0.30
            score = 5.0 - t * 3.0
        else:
            score = 1.0

        score = round(max(0.0, min(10.0, score)), 2)

        # ── 5. sell_score = mirror ──────────────────────────────────────────
        sell_score = round(10.0 - score, 2)

        # ── 6. Context cross: bottoming pattern ────────────────────────────
        # High overhead (>50%) + price in lower 30% of 52w range
        # → trapped chips are at historical highs, current low = potential reversal.
        # Reduce sell pressure slightly.
        price_pos_52w = _get_price_position(price_df)
        bottoming     = False
        if overhead_ratio > 0.50 and price_pos_52w is not None and price_pos_52w < 0.30:
            sell_score = round(max(0.0, sell_score - 1.5), 2)
            bottoming  = True

        # Build signal label
        overhead_pct_display = round(overhead_ratio * 100, 1)
        if overhead_ratio <= 0.10:
            signal = f"clean chart: only {overhead_pct_display}% overhead resistance"
        elif overhead_ratio <= 0.30:
            signal = f"moderate overhead: {overhead_pct_display}% chips trapped above"
        elif overhead_ratio <= 0.60:
            signal = f"heavy overhead: {overhead_pct_display}% chips trapped — breakout difficult"
        else:
            signal = f"extreme overhead: {overhead_pct_display}% chips trapped — avoid"
        if bottoming:
            signal += " | bottoming context: price at 52w low, sell_score reduced"

        return {
            "score":      score,
            "sell_score": sell_score,
            "max":        MAX,
            "details": {
                "signal":           signal,
                "overhead_ratio":   overhead_pct_display,
                "current_price":    round(current_price, 2),
                "price_pos_52w":    round(price_pos_52w, 3) if price_pos_52w is not None else None,
                "bottoming":        bottoming,
                "sell_score":       sell_score,
            },
        }

    except Exception:
        return {
            "score":      NEUTRAL_SCORE,
            "sell_score": NEUTRAL_SELL,
            "max":        MAX,
            "details":    {"signal": "error, neutral"},
        }
