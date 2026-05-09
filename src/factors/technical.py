from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from ._utils import _extract, _extract_two, _neutral, _get_price_position


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




def score_rsi_signal(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    14-period RSI mean-reversion / momentum signal (max 10).

    Oversold zones (RSI < 30) are buy setups; overbought (RSI > 70) are sell setups.
    RSI direction (rising vs falling) provides confirmation.
    All inputs are derived from price_df — no external data source required.
    """
    MAX = 10
    if price_df is None or len(price_df) < 18 or "close" not in price_df.columns:
        return _neutral(MAX)

    close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
    if len(close) < 18:
        return _neutral(MAX)

    def _rsi(series: pd.Series, period: int = 14) -> float:
        delta = series.diff()
        avg_gain = float(delta.clip(lower=0).tail(period).mean())
        avg_loss = float((-delta.clip(upper=0)).tail(period).mean())
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    rsi_curr = _rsi(close)
    rsi_prev = _rsi(close.iloc[:-3]) if len(close) >= 21 else None
    rsi_rising = (rsi_curr > rsi_prev) if rsi_prev is not None else None

    # ── Buy score ──
    if rsi_curr < 20:
        score, signal = 9.5, "extreme oversold"
    elif rsi_curr < 30:
        score, signal = 8.0, "oversold"
    elif rsi_curr < 40:
        score, signal = 7.0, "mild oversold"
    elif rsi_curr < 50:
        score, signal = 6.0, "below midline"
    elif rsi_curr < 55:
        score, signal = 5.0, "neutral"
    elif rsi_curr < 65:
        score, signal = 4.0, "mild overbought"
    elif rsi_curr < 75:
        score, signal = 3.0, "overbought"
    else:
        score, signal = 1.5, "extreme overbought"

    if rsi_rising is not None:
        if rsi_curr < 50 and rsi_rising:
            score = min(MAX, score + 1.0)
            signal += " + RSI rising (bullish)"
        elif rsi_curr > 70 and not rsi_rising:
            score = max(0.0, score - 0.5)
            signal += " + RSI falling (fading)"

    if market_regime_score is not None:
        if rsi_curr < 30 and float(market_regime_score) >= 7:
            score = min(MAX, score + 0.5)
            signal += " + bull regime dip"
        elif rsi_curr > 70 and float(market_regime_score) <= 3:
            score = max(0.0, score - 1.0)
            signal += " + bear regime risky"

    # ── Sell score ──
    if rsi_curr > 80:
        sell_score, sell_signal = 8.0, "extreme overbought sell"
    elif rsi_curr > 70:
        sell_score, sell_signal = 5.5, "overbought sell zone"
    elif rsi_curr > 65:
        sell_score, sell_signal = 3.0, "mild overbought"
    else:
        sell_score, sell_signal = max(0.0, (rsi_curr - 50.0) / 15.0), "neutral sell"

    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in str(c) for k in ["评级", "rating", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                upgrades = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                if rsi_curr < 35 and upgrades >= 2:
                    score = min(MAX, score + 0.5)
                    signal += f" + analyst upgrades({upgrades})"
        except Exception:
            pass

    return {
        "score":      round(min(MAX, max(0.0, score)), 1),
        "sell_score": round(min(MAX, max(0.0, sell_score)), 1),
        "max": MAX,
        "details": {
            "rsi": round(rsi_curr, 1),
            "rsi_rising": rsi_rising,
            "signal": signal,
            "sell_signal": sell_signal,
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
    Standard 12/26/9 MACD trend signal (max 10).

    Scoring combines:
    - Zero-line position (MACD>0 = bullish trend)
    - Histogram direction (expanding = accelerating momentum)
    - Signal-line cross (golden/death cross)
    - Zero-line cross bonus
    All inputs derived from price_df close prices — no external data required.
    """
    MAX = 10
    if price_df is None or len(price_df) < 35 or "close" not in price_df.columns:
        return _neutral(MAX)

    close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
    if len(close) < 35:
        return _neutral(MAX)

    ema12     = close.ewm(span=12, adjust=False).mean()
    ema26     = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    sig_line  = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - sig_line

    macd_c = float(macd_line.iloc[-1]); macd_p = float(macd_line.iloc[-2])
    sig_c  = float(sig_line.iloc[-1]);  sig_p  = float(sig_line.iloc[-2])
    hist_c = float(histogram.iloc[-1]); hist_p = float(histogram.iloc[-2])

    score = 5.0
    sell_score = 0.0
    signals: list = []

    # 1. Zero-line position
    if macd_c > 0:
        score += 1.5
        signals.append("MACD>0")
    else:
        score -= 1.0
        sell_score += 1.0
        signals.append("MACD<0")

    # 2. Histogram direction
    if hist_c > 0 and hist_c > hist_p:
        score += 2.0
        signals.append("hist↑ bullish")
    elif hist_c > 0 and hist_c < hist_p:
        score += 0.5
        sell_score += 1.5
        signals.append("hist↓ fading")
    elif hist_c < 0 and hist_c > hist_p:
        score -= 0.5
        signals.append("hist recovering")
    else:
        score -= 1.5
        sell_score += 1.5
        signals.append("hist↓ bearish")

    # 3. Signal-line cross
    golden_cross = (macd_p < sig_p) and (macd_c >= sig_c)
    death_cross  = (macd_p > sig_p) and (macd_c <= sig_c)
    if golden_cross:
        score += 2.0
        signals.append("golden cross!")
    elif death_cross:
        score -= 2.0
        sell_score += 2.5
        signals.append("death cross!")
    elif macd_c > sig_c:
        score += 0.5
        signals.append("MACD>signal")
    else:
        score -= 0.5
        sell_score += 0.5

    # 4. Zero-line cross bonus
    if macd_p < 0 <= macd_c:
        score += 1.5
        signals.append("zero-line bull cross!")
    elif macd_p > 0 >= macd_c:
        score -= 1.5
        sell_score += 1.5
        signals.append("zero-line bear cross!")

    # Regime cross
    if market_regime_score is not None:
        regime = float(market_regime_score)
        if golden_cross and regime >= 7:
            score = min(MAX, score + 0.5)
        elif death_cross and regime <= 3:
            sell_score = min(MAX, sell_score + 0.5)

    # Concept momentum sustains MACD trends in A-share hot sectors
    if best_concept_ret is not None and best_concept_ret > 5 and hist_c > 0:
        score = min(MAX, score + 0.5)
        signals.append(f"hot sector({best_concept_ret:.1f}%)")

    return {
        "score":      round(min(MAX, max(0.0, score)), 1),
        "sell_score": round(min(MAX, max(0.0, sell_score)), 1),
        "max": MAX,
        "details": {
            "macd":         round(macd_c, 4),
            "signal_line":  round(sig_c, 4),
            "histogram":    round(hist_c, 4),
            "golden_cross": golden_cross,
            "death_cross":  death_cross,
            "signals":      " | ".join(signals),
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
    if len(price_df) >= 252 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(260)
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


# ===========================================================================
# GROUP B — From additional per-stock API calls
# ===========================================================================



def score_limit_open_rate(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """涨停开板率因子 — limit-up break (开板) rate over recent trading days.

    Detects when a stock touched the limit-up price intraday but failed to
    close there, i.e., the limit was "broken" (开板).  High open-board rate =
    heavy supply at the limit price = distribution, bearish.

    Detection (per day, using OHLC):
      prev_limit = prev_close × 1.099  (≈ +10% limit, with tolerance)
      touched    = high  ≥ prev_limit  (price reached limit intraday)
      broke      = touched AND close < prev_limit  (couldn't hold limit by close)

    Lookback windows:
      - 20-day window (medium-term signal)
      - 5-day recency window (recent sell pressure amplifier)

    Score (MAX = 10):
      touched == 0  → neutral (no limit events, factor not applicable)
      break_rate == 0  → 8 (all limits held, solid momentum)
      break_rate 0–30% → 6–8 (mostly held)
      break_rate 30–60% → 3–6 (mixed / weakening)
      break_rate > 60%  → 0–3 (heavy distribution)

    Sell score: mirrors break_rate; amplified if recent (5d) break rate is high.
    """
    MAX = 10
    if price_df is None or len(price_df) < 6:
        return _neutral(MAX)

    required_cols = {"close", "open", "high", "low"}
    if not required_cols.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        df = price_df.copy()
        for col in ("close", "open", "high", "low"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close", "high"]).tail(25).reset_index(drop=True)
        if len(df) < 5:
            return _neutral(MAX)

        prev_close = df["close"].shift(1)
        limit_price = prev_close * 1.099  # ≈ +10% limit threshold

        touched = (df["high"] >= limit_price) & (prev_close.notna())
        broke   = touched & (df["close"] < limit_price)

        # 20-day window
        window20 = slice(max(0, len(df) - 20), len(df))
        n_touched20 = int(touched.iloc[window20].sum())
        n_broke20   = int(broke.iloc[window20].sum())

        # 5-day recency window
        window5 = slice(max(0, len(df) - 5), len(df))
        n_touched5 = int(touched.iloc[window5].sum())
        n_broke5   = int(broke.iloc[window5].sum())

        if n_touched20 == 0:
            # No limit-up events — factor not applicable, return neutral
            return _neutral(MAX)

        break_rate20 = n_broke20 / n_touched20  # 0.0–1.0
        break_rate5  = (n_broke5 / n_touched5) if n_touched5 > 0 else break_rate20

        # Score: low break rate = good (momentum intact); high = bad (distribution)
        score = float(np.clip((1.0 - break_rate20) * 10.0, 0.0, 10.0))

        # Sell score: break rate + recency amplifier
        base_sell = float(np.clip(break_rate20 * 10.0, 0.0, 10.0))
        recency_boost = 0.0
        if n_touched5 > 0 and break_rate5 > break_rate20 + 0.2:
            # Recent break rate significantly worse than 20d average
            recency_boost = min(2.0, (break_rate5 - break_rate20) * 5.0)
        sell_score = float(np.clip(base_sell + recency_boost, 0.0, 10.0))

        # Signal text
        if break_rate20 == 0.0:
            signal = f"all {n_touched20} limit(s) held — solid momentum, no distribution"
        elif break_rate20 < 0.3:
            signal = (
                f"low break rate {break_rate20:.0%} ({n_broke20}/{n_touched20}) "
                f"— mostly holding, minor supply"
            )
        elif break_rate20 < 0.6:
            signal = (
                f"moderate break rate {break_rate20:.0%} ({n_broke20}/{n_touched20}) "
                f"— supply pressure building"
            )
        else:
            signal = (
                f"high break rate {break_rate20:.0%} ({n_broke20}/{n_touched20}) "
                f"— distribution at limit, bearish"
            )

        if recency_boost > 0:
            signal += f" | recent 5d rate {break_rate5:.0%} — worsening"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":         signal,
                "n_touched_20d":  n_touched20,
                "n_broke_20d":    n_broke20,
                "break_rate_20d": round(break_rate20, 3),
                "n_touched_5d":   n_touched5,
                "n_broke_5d":     n_broke5,
                "break_rate_5d":  round(break_rate5, 3),
                "recency_boost":  round(recency_boost, 2),
                "sell_score":     round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)




def score_upper_shadow_reversal(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """上涨中长上影线因子 — bearish reversal signal in uptrend.

    Detects shooting-star / gravestone-doji candles that appear during a
    recent uptrend.  The pattern signals distribution: price tested higher
    intraday but sellers pushed it back down — supply is heavy at these levels.

    Upper-shadow candle criteria (per candle):
      - upper_shadow  ≥ 50% of total range   (明显上引线)
      - upper_shadow  ≥ 2× real body          (引线远超实体)
      - lower_shadow  ≤ 20% of total range    (无明显下引线, 排除十字星)
      - real body     ≥ 3% of total range     (非无量空心)
      - total_range   > 0.3% of close price   (排除无波动日)

    Uptrend context (required to generate a signal):
      - 10-day return > +3%  OR  close > MA10

    Strength bonuses (each +1 quality point, up to 4):
      - Bear body: close < open  (阴线上影 更危险)
      - Very long shadow: upper_shadow ≥ 3× body
      - Recent (within last 3 days)
      - Strong prior uptrend: 10d return > +8%  (上涨越猛, 反转越危险)

    Score (MAX = 10):
      No pattern in 10d OR no uptrend context  → neutral (4.0)
      Pattern present, quality 0–4             → sell_score 5–9
      Multiple patterns in 10d                 → sell_score +1 (capped 10)

    This is primarily a SELL signal.  Buy score is inverted (absence of
    upper shadows in uptrend = clean trend = slight buy signal).
    """
    MAX = 10
    if price_df is None or len(price_df) < 15:
        return _neutral(MAX)

    required = {"close", "open", "high", "low"}
    if not required.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        df = price_df.copy().tail(25)
        for col in ("close", "open", "high", "low"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close", "open", "high", "low"]).reset_index(drop=True)
        if len(df) < 10:
            return _neutral(MAX)

        close_arr = df["close"].values

        # ── Uptrend check (using last 10 candles before the window) ────
        if len(close_arr) >= 10:
            ret_10d = (close_arr[-1] - close_arr[-10]) / max(close_arr[-10], 1e-8) * 100
        else:
            ret_10d = 0.0
        ma10 = float(pd.Series(close_arr).rolling(10).mean().iloc[-1]) if len(close_arr) >= 10 else close_arr[-1]
        in_uptrend = ret_10d > 3.0 or close_arr[-1] > ma10

        # ── Scan last 10 candles for upper-shadow pattern ──────────────
        window = df.tail(10).reset_index(drop=True)
        shadow_days: list[dict] = []

        for i in range(len(window)):
            row  = window.iloc[i]
            o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])

            body  = abs(c - o)
            upper = h - max(o, c)
            lower = min(o, c) - l
            rng   = h - l

            if rng < 1e-8 or rng < 0.003 * c:
                continue

            body_r  = body  / rng
            upper_r = upper / rng
            lower_r = lower / rng

            if (
                upper_r >= 0.50
                and upper >= 2.0 * max(body, 1e-8)
                and lower_r <= 0.20
                and body_r >= 0.03
            ):
                quality = 0
                notes   = []

                if c < o:
                    quality += 1
                    notes.append("阴线上影")
                if upper >= 3.0 * max(body, 1e-8):
                    quality += 1
                    notes.append(f"超长上影({upper_r:.0%})")
                if i >= len(window) - 3:
                    quality += 1
                    notes.append("近期出现")
                if ret_10d > 8.0:
                    quality += 1
                    notes.append(f"强势上涨后({ret_10d:+.1f}%)")

                shadow_days.append({
                    "day_index": i,
                    "quality":   quality,
                    "upper_r":   upper_r,
                    "notes":     notes,
                })

        # ── Scoring ────────────────────────────────────────────────────
        if not shadow_days or not in_uptrend:
            # No pattern or not in uptrend → no sell signal
            # Clean uptrend without upper shadows is slightly bullish
            if in_uptrend and not shadow_days:
                score      = 6.0  # clean uptrend, no distribution candles
                sell_score = 0.0
                signal     = f"clean uptrend ({ret_10d:+.1f}% 10d) — no upper-shadow distribution"
            else:
                score      = float(MAX) * 0.4   # neutral
                sell_score = float(MAX) * 0.2
                signal     = "no upper-shadow pattern in uptrend — neutral"
            return {
                "score":      round(score, 1),
                "sell_score": round(sell_score, 1),
                "max":        MAX,
                "details": {
                    "signal":        signal,
                    "in_uptrend":    in_uptrend,
                    "ret_10d":       round(ret_10d, 2),
                    "shadow_count":  0,
                    "sell_score":    round(sell_score, 1),
                },
            }

        best    = max(shadow_days, key=lambda x: x["quality"])
        quality = min(best["quality"], 4)

        sell_score = float(np.clip(5.0 + quality, 0.0, 9.0))
        if len(shadow_days) >= 2:
            sell_score = min(sell_score + 1.0, float(MAX))

        # Buy score inversely reflects distribution risk
        score = float(np.clip(float(MAX) - sell_score, 0.0, float(MAX)))

        notes_str  = " + ".join(best["notes"]) if best["notes"] else "basic upper shadow"
        multi_note = f" ({len(shadow_days)} patterns in 10d)" if len(shadow_days) >= 2 else ""
        signal = (
            f"上涨中长上影线{multi_note}: {notes_str}, "
            f"shadow={best['upper_r']:.0%}, trend={ret_10d:+.1f}%"
        )

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":        signal,
                "in_uptrend":    in_uptrend,
                "ret_10d":       round(ret_10d, 2),
                "shadow_count":  len(shadow_days),
                "best_quality":  quality,
                "best_upper_r":  round(best["upper_r"], 3),
                "best_notes":    best["notes"],
                "sell_score":    round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)




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



def score_amihud_illiquidity(
    price_df: Optional[pd.DataFrame],
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


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
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


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




def score_hammer_bottom(
    price_df: Optional[pd.DataFrame],
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


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


# ===========================================================================
# score_sector_sympathy — added 2026-04-02
# ===========================================================================



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
