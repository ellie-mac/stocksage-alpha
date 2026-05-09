from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd


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
    """Return 52-week price position (0.0–1.0) or None if unavailable.

    Requires at least 252 trading days of history; returns None for newer stocks
    so callers receive a genuine "no data" rather than a spurious partial-window value.
    """
    if price_df is None or len(price_df) < 20 or "close" not in price_df.columns:
        return None
    window = price_df["close"].tail(260)
    if len(window) < 260:   # not enough history for a true 52-week metric (+8 suspension buffer)
        return None
    high_52w = float(window.max())
    low_52w  = float(window.min())
    current  = float(window.iloc[-1])
    if high_52w <= low_52w:
        return None
    return (current - low_52w) / (high_52w - low_52w)


# ===========================================================================
# GROUP A — From already-fetched data
# ===========================================================================

