#!/usr/bin/env python3
"""
Shared utilities for StockSage monitor scripts.

Centralises:
  - A-share trading calendar (holiday-aware)
  - Trading hours helpers
  - WeChat push (PushPlus preferred, Server酱 fallback)
  - ETF / T+0 identification
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))

import cache as _cache

# ── Trading session windows ────────────────────────────────────────────────────
_MORNING_OPEN    = (9, 25)
_MORNING_CLOSE   = (11, 35)
_AFTERNOON_OPEN  = (12, 55)
_AFTERNOON_CLOSE = (15, 5)


# ── Trading calendar ───────────────────────────────────────────────────────────

def _load_trade_dates() -> set[str]:
    """
    Load A-share trading dates from Sina.  Cached for 24 h.
    Returns a set of 'YYYY-MM-DD' strings.
    On failure returns an empty set (callers should degrade gracefully).
    """
    cached = _cache.get("trade_dates_sina", _cache.TTL_VALUATION)  # 24 h
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


def is_trading_day(dt: Optional[datetime] = None) -> bool:
    """True if `dt` (default: now) is a scheduled A-share trading day."""
    if dt is None:
        dt = datetime.now()
    if dt.weekday() >= 5:          # weekend
        return False
    dates = _load_trade_dates()
    if not dates:                  # calendar unavailable → trust weekday check
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


def next_session_seconds() -> int:
    """Seconds until the next trading session opens."""
    now = datetime.now()
    # Try today's morning open
    today_open = now.replace(hour=_MORNING_OPEN[0], minute=_MORNING_OPEN[1],
                              second=0, microsecond=0)
    if now < today_open and is_trading_day(now):
        return max(0, int((today_open - now).total_seconds()))
    # Try today's afternoon open
    aftn_open = now.replace(hour=_AFTERNOON_OPEN[0], minute=_AFTERNOON_OPEN[1],
                             second=0, microsecond=0)
    if now < aftn_open and is_trading_day(now):
        return max(0, int((aftn_open - now).total_seconds()))
    # Find next trading day morning open
    candidate = now + timedelta(days=1)
    for _ in range(10):           # guard against long holiday gaps
        candidate = candidate.replace(hour=_MORNING_OPEN[0],
                                      minute=_MORNING_OPEN[1],
                                      second=0, microsecond=0)
        if is_trading_day(candidate):
            return max(0, int((candidate - now).total_seconds()))
        candidate += timedelta(days=1)
    # Fallback: 16 hours
    return 16 * 3600


# ── WeChat push ────────────────────────────────────────────────────────────────

_pushplus_token: str = ""   # set once at startup via configure_pushplus()


def configure_pushplus(token: str) -> None:
    """Call once at startup with the PushPlus token from config.json."""
    global _pushplus_token
    _pushplus_token = token.strip() if token else ""


def _send_pushplus(title: str, desp: str, token: str, retries: int = 3) -> None:
    import urllib.request, json as _json, time as _time
    payload = _json.dumps({
        "token":    token,
        "title":    title[:100],
        "content":  desp,
        "template": "markdown",
    }).encode("utf-8")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                "https://www.pushplus.plus/send",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                resp = _json.loads(r.read().decode("utf-8"))
            if resp.get("code") == 200:
                print(f"[OK] 微信推送成功: {title}")
                return
            print(f"[WARN] PushPlus: code={resp.get('code')} msg={resp.get('msg')}（第{attempt}次）")
            if attempt < retries:
                _time.sleep(3)
        except Exception as e:
            print(f"[WARN] PushPlus 推送失败（第{attempt}次）: {e}")
            if attempt < retries:
                _time.sleep(3)


def send_wechat(title: str, desp: str, sendkey: str, dry_run: bool = False) -> None:
    if dry_run:
        print(f"[DRY-RUN] 微信推送: {title}")
        print(f"[DRY-RUN] 内容预览:\n{desp[:300]}{'...' if len(desp) > 300 else ''}")
        return
    if _pushplus_token:
        _send_pushplus(title, desp, _pushplus_token)
    elif sendkey:
        from serverchan_sdk import sc_send
        resp = sc_send(sendkey, title, desp)
        if resp.get("code") == 0:
            print(f"[OK] 微信推送成功: {title}")
        else:
            print(f"[WARN] 微信推送: code={resp.get('code')} msg={resp.get('message')}")
    else:
        print(f"[WARN] 未配置推送渠道（pushplus.token / serverchan.sendkey），跳过: {title}")


# ── Spot market (cached) ──────────────────────────────────────────────────────

def get_spot_em(retries: int = 3):
    """
    Fetch full A-share spot market data (stock_zh_a_spot_em) with caching.

    TTL: 90s during trading hours, 4h after close (prices are final).
    Multiple scripts can share one API call per session.
    Returns a pandas DataFrame with all columns from stock_zh_a_spot_em,
    or an empty DataFrame on failure.
    """
    import pandas as pd
    now = datetime.now()
    hm  = now.hour * 60 + now.minute
    in_trading = (
        (9 * 60 + 25 <= hm <= 11 * 60 + 35) or
        (12 * 60 + 55 <= hm <= 15 * 60 + 5)
    )
    ttl = 90 if in_trading else 4 * 3600   # 90s live; 4h after close

    cached = _cache.get_df("spot_em", ttl)
    if cached is not None:
        return cached
    import akshare as ak
    for attempt in range(1, retries + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            _cache.set("spot_em", df)
            return df
        except Exception as e:
            print(f"[spot_em] 获取失败（第{attempt}次）: {e}")
            if attempt < retries:
                time.sleep(3)
    return pd.DataFrame()


# ── ETF / T+0 identification ───────────────────────────────────────────────────

def is_etf(code: str, name: str = "", is_t0_override: Optional[bool] = None) -> bool:
    """
    True for exchange-listed ETFs eligible for T+0 secondary-market trading.

    Priority:
      1. Explicit `is_t0` flag in the holding dict (set by user — most reliable).
      2. Unambiguous ETF code ranges (510xxx-518xxx, 588xxx, 159xxx).
      3. All other ranges → conservatively T+1 unless explicitly flagged.
    """
    if is_t0_override is not None:
        return is_t0_override
    c = str(code).zfill(6)
    return c.startswith("51") or c.startswith("159") or c.startswith("588")


def is_t1_locked(holding: dict) -> bool:
    """True if holding was bought today and is subject to T+1 restriction."""
    flag = holding.get("is_t0")
    if is_etf(holding.get("code", ""), holding.get("name", ""), flag):
        return False
    bought_date = holding.get("bought_date")
    if not bought_date:
        return False
    return bought_date == datetime.now().strftime("%Y-%m-%d")
