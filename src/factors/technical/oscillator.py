from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from .._utils import _neutral
from ._helpers import _compute_macd_hist, _compute_rsi, _compute_kdj_k, _divergence_signal


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
