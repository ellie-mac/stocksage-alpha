from __future__ import annotations
import numpy as np
import pandas as pd


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
