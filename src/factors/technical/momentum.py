from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from .._utils import _neutral


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
            window = price_df["close"].tail(260)
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
