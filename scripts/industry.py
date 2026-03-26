"""
Industry classification and intra-industry relative valuation.

Building the full code->industry map requires ~90 API calls (one per industry board).
The map is cached for 7 days since industry membership rarely changes.
"""

import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import cache

INDUSTRY_MAP_KEY = "industry_map"
INDUSTRY_MAP_TTL = 7 * 86400  # 7 days


def build_industry_map() -> dict[str, str]:
    """
    Returns {code: industry_name} for all A-share stocks.
    Iterates over all industry boards from East Money and collects constituents.
    """
    cached = cache.get(INDUSTRY_MAP_KEY, INDUSTRY_MAP_TTL)
    if cached:
        return cached

    import akshare as ak

    try:
        boards = ak.stock_board_industry_name_em()
    except Exception:
        return {}

    industry_map: dict[str, str] = {}
    name_col = "板块名称" if "板块名称" in boards.columns else boards.columns[0]

    for industry_name in boards[name_col]:
        try:
            cons = ak.stock_board_industry_cons_em(symbol=industry_name)
            if cons is None or cons.empty:
                continue
            code_col = "代码" if "代码" in cons.columns else cons.columns[0]
            for code in cons[code_col]:
                industry_map[str(code).zfill(6)] = industry_name
        except Exception:
            continue

    if industry_map:
        cache.set(INDUSTRY_MAP_KEY, industry_map)
    return industry_map


def get_industry_pe_stats(
    industry: str,
    spot_df: pd.DataFrame,
    industry_map: dict[str, str],
) -> dict:
    """
    Compute PE/PB distribution statistics for stocks in the same industry,
    using the already-fetched full-market spot DataFrame.
    Returns {p10, p25, p50, p75, p90, count} or {} if insufficient peers.
    """
    df = spot_df.copy()
    df["_code"] = df["代码"].astype(str).str.zfill(6)
    df["_industry"] = df["_code"].map(industry_map)
    df["_pe"] = pd.to_numeric(df.get("市盈率-动态"), errors="coerce")
    df["_pb"] = pd.to_numeric(df.get("市净率"), errors="coerce")

    peers = df[df["_industry"] == industry]

    def stats(series: pd.Series) -> dict:
        s = series.dropna()
        s = s[s > 0]
        if len(s) < 5:
            return {}
        return {
            "p10": float(s.quantile(0.10)),
            "p25": float(s.quantile(0.25)),
            "p50": float(s.median()),
            "p75": float(s.quantile(0.75)),
            "p90": float(s.quantile(0.90)),
            "count": len(s),
        }

    return {
        "pe": stats(peers["_pe"]),
        "pb": stats(peers["_pb"]),
        "industry": industry,
    }


def industry_relative_percentile(value: float, stats: dict) -> float | None:
    """
    Return the in-industry percentile (0–100) of the given value.
    Returns None if stats are unavailable.
    Uses linear interpolation between the stored quantile breakpoints.
    """
    if not stats or value <= 0:
        return None

    breakpoints = [
        (0.10, stats.get("p10")),
        (0.25, stats.get("p25")),
        (0.50, stats.get("p50")),
        (0.75, stats.get("p75")),
        (0.90, stats.get("p90")),
    ]
    breakpoints = [(q, v) for q, v in breakpoints if v is not None]
    if not breakpoints:
        return None

    # Below lowest breakpoint
    if value <= breakpoints[0][1]:
        return breakpoints[0][0] * 100

    # Above highest breakpoint
    if value >= breakpoints[-1][1]:
        return breakpoints[-1][0] * 100

    # Interpolate between adjacent breakpoints
    for i in range(len(breakpoints) - 1):
        q_lo, v_lo = breakpoints[i]
        q_hi, v_hi = breakpoints[i + 1]
        if v_lo <= value <= v_hi:
            frac = (value - v_lo) / (v_hi - v_lo) if v_hi != v_lo else 0.5
            return (q_lo + frac * (q_hi - q_lo)) * 100

    return 50.0
