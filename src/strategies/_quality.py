"""统一的流动性 / 量能 / 价格质量指标。

供各 scanner 在 fetch 完 K 线后调用，输出口径一致的 amt_5d_yi 和 vol_ratio
字段，避免每条策略各自重新发明轮子（单位不同、口径不同、阈值不同）。

典型用法：
    from strategies._quality import compute_metrics, passes_quality

    df = fetcher.get_price_history(code, days=65)
    m  = compute_metrics(df)
    if not passes_quality(m):
        return None
    pick = {..., "amt_5d_yi": m["amt_5d_yi"], "vol_ratio": m["vol_ratio"]}
"""
from __future__ import annotations

from typing import Optional

import pandas as pd


# Default gate thresholds — strategies can override per-策略
DEFAULT_MIN_AMT_YI    = 0.5   # 5 日均成交额 ≥ 0.5 亿（排除死水股）
DEFAULT_MIN_VOL_RATIO = 0.5   # 5 日量 / 60 日量 ≥ 0.5（排除越来越冷）


def compute_metrics(df: Optional[pd.DataFrame]) -> dict:
    """从价格历史 DataFrame 算质量指标，口径全 A 股统一。

    要求列：close, volume；可选：high, low（用于一字板判定）。
    单位约定：volume = 手（×100 = 股）；close = 元。

    Returns dict with:
      - amt_5d_yi:      5 日均成交额（亿）
      - vol_ratio:      5 日均量 / 60 日均量（数据不够时给 1.0 neutral）
      - is_limit_today: 当日相对前日 |chg_pct| ≥ 9.5%（涨跌停）
      - is_yi_zi:       当日 high == low（一字板）

    数据不全时返回 {} —— 调用方将这视为"质量不达标，剔除"。
    """
    if df is None or len(df) < 5 or "close" not in df.columns or "volume" not in df.columns:
        return {}
    closes = df["close"].values
    vols   = df["volume"].values
    close  = float(closes[-1])
    if close <= 0:
        return {}

    avg_vol_5d = float(vols[-5:].mean())
    amt_5d_yi  = avg_vol_5d * close * 100 / 1e8   # 手→股 ×100，元→亿 /1e8

    if len(vols) >= 20:
        n60 = min(60, len(vols))
        avg_vol_60d = float(vols[-n60:].mean())
        vol_ratio = avg_vol_5d / avg_vol_60d if avg_vol_60d > 0 else 0.0
    else:
        vol_ratio = 1.0   # 数据不够给 neutral，不卡 < 20 天上市的新股

    is_limit_today = False
    if len(closes) >= 2:
        prev = float(closes[-2])
        if prev > 0 and abs(close - prev) / prev * 100 >= 9.5:
            is_limit_today = True

    is_yi_zi = False
    if "high" in df.columns and "low" in df.columns:
        if float(df["high"].iloc[-1]) == float(df["low"].iloc[-1]):
            is_yi_zi = True

    return {
        "amt_5d_yi":      round(amt_5d_yi, 2),
        "vol_ratio":      round(vol_ratio, 2),
        "is_limit_today": is_limit_today,
        "is_yi_zi":       is_yi_zi,
    }


def passes_liquidity(metrics: dict,
                     min_amt_yi: float = DEFAULT_MIN_AMT_YI,
                     min_vol_ratio: float = DEFAULT_MIN_VOL_RATIO) -> bool:
    """流动性 + 量能门槛单测。"""
    if not metrics:
        return False
    return (metrics.get("amt_5d_yi", 0) >= min_amt_yi and
            metrics.get("vol_ratio", 0) >= min_vol_ratio)


def passes_quality(metrics: dict,
                   reject_limits: bool = True,
                   min_amt_yi: float = DEFAULT_MIN_AMT_YI,
                   min_vol_ratio: float = DEFAULT_MIN_VOL_RATIO) -> bool:
    """流动性 + 量能 + 当日涨跌停/一字板综合判断。"""
    if not passes_liquidity(metrics, min_amt_yi, min_vol_ratio):
        return False
    if reject_limits and (metrics.get("is_limit_today") or metrics.get("is_yi_zi")):
        return False
    return True


def pick_fields(metrics: dict) -> dict:
    """只取要 inline 进 pick 的展示字段（amt/vol_ratio）。"""
    return {
        "amt_5d_yi": metrics.get("amt_5d_yi", 0.0),
        "vol_ratio": metrics.get("vol_ratio", 0.0),
    }
