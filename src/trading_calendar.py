"""
trading_calendar.py — A 股交易日历工具

提供交易日判断、交易时段检测、下次开盘倒计时等功能。
从 Sina（YYYY-MM-DD）和 Tushare（YYYYMMDD）两个源获取日历。

其他模块请直接 import 本模块，也可继续 `from common import is_trading_day`
（common.py 保留了 re-export）。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import cache as _cache

# Trading session windows
_MORNING_OPEN    = (9, 25)
_MORNING_CLOSE   = (11, 35)
_AFTERNOON_OPEN  = (12, 55)
_AFTERNOON_CLOSE = (15, 5)


def _load_trade_dates() -> set[str]:
    """Load A-share trading dates from Sina. Returns set of 'YYYY-MM-DD'. Cached 24 h."""
    cached = _cache.get("trade_dates_sina", _cache.TTL_VALUATION)
    if cached is not None:
        return set(cached)
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        dates = df["trade_date"].astype(str).tolist()
        _cache.set("trade_dates_sina", dates)
        return set(dates)
    except Exception as e:
        print(f"  [WARN] trading calendar fetch failed: {e}")
        return set()


def get_trade_dates(year: str | None = None) -> set[str]:
    """Return A-share trading dates (YYYYMMDD) from Tushare trade_cal. Cached 24 h per year.

    Falls back to empty set on any error (missing token, network, etc.).
    """
    if year is None:
        year = datetime.now().strftime("%Y")
    cached = _cache.get(f"trade_calendar_{year}", 24 * 3600)
    if cached:
        return set(cached)
    try:
        import json as _json
        from pathlib import Path as _Path
        import tushare as ts
        _root = _Path(__file__).resolve().parent.parent
        cfg = _json.loads((_root / "alert_config.json").read_text(encoding="utf-8"))
        token = cfg.get("tushare", {}).get("token", "")
        if not token:
            return set()
        ts.set_token(token)
        pro = ts.pro_api()
        df = pro.trade_cal(exchange="SSE", start_date=f"{year}0101", end_date=f"{year}1231")
        if df is None or df.empty:
            return set()
        open_dates = df[df["is_open"] == 1]["cal_date"].tolist()
        _cache.set(f"trade_calendar_{year}", open_dates)
        return set(open_dates)
    except Exception:
        return set()


def is_trading_day(dt: Optional[datetime] = None) -> bool:
    """True if `dt` (default: now) is a scheduled A-share trading day."""
    if dt is None:
        dt = datetime.now()
    if dt.weekday() >= 5:
        return False
    dates = _load_trade_dates()
    if not dates:
        return True
    return dt.strftime("%Y-%m-%d") in dates


def is_trading_hours(dt: Optional[datetime] = None) -> bool:
    """True if `dt` (default: now) falls within an A-share trading window."""
    if dt is None:
        dt = datetime.now()
    if not is_trading_day(dt):
        return False
    hm = (dt.hour, dt.minute)
    return (_MORNING_OPEN <= hm <= _MORNING_CLOSE or
            _AFTERNOON_OPEN <= hm <= _AFTERNOON_CLOSE)


def nth_trading_day_before(n: int, ref: datetime | None = None) -> str | None:
    """往前数第 n 个交易日，返回 'YYYY-MM-DD'；日历为空或不足则返回 None。"""
    if ref is None:
        ref = datetime.now()
    ref_str = ref.strftime("%Y-%m-%d")
    dates = sorted(d for d in _load_trade_dates() if d < ref_str)
    if len(dates) < n:
        return None
    return dates[-n]


def next_session_seconds() -> int:
    """Seconds until the next trading session opens."""
    now = datetime.now()
    today_open = now.replace(hour=_MORNING_OPEN[0], minute=_MORNING_OPEN[1],
                              second=0, microsecond=0)
    if now < today_open and is_trading_day(now):
        return max(0, int((today_open - now).total_seconds()))
    aftn_open = now.replace(hour=_AFTERNOON_OPEN[0], minute=_AFTERNOON_OPEN[1],
                             second=0, microsecond=0)
    if now < aftn_open and is_trading_day(now):
        return max(0, int((aftn_open - now).total_seconds()))
    candidate = now + timedelta(days=1)
    for _ in range(10):
        candidate = candidate.replace(hour=_MORNING_OPEN[0],
                                      minute=_MORNING_OPEN[1],
                                      second=0, microsecond=0)
        if is_trading_day(candidate):
            return max(0, int((candidate - now).total_seconds()))
        candidate += timedelta(days=1)
    return 16 * 3600
