from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from .._utils import _neutral


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
