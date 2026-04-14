"""
Local file cache module.
- Real-time quotes: 30s TTL
- Price history:    1h TTL during trading hours; extended until next open otherwise
- Valuation data:  until next trading session open (or 24h fallback)
- Financial data:  14d TTL (quarterly report frequency)
"""

import json
import os
import time
from datetime import datetime
from typing import Any, Optional
import pandas as pd

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".cache")


def _cache_path(key: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe_key = key.replace("/", "_").replace("\\", "_")
    return os.path.join(CACHE_DIR, f"{safe_key}.json")


def get(key: str, ttl_seconds: int) -> Optional[Any]:
    path = _cache_path(key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            entry = json.load(f)
        if time.time() - entry["ts"] > ttl_seconds:
            return None
        return entry["data"]
    except Exception:
        # Corrupted or unreadable file — delete it so subsequent calls re-fetch cleanly
        try:
            os.remove(path)
        except OSError:
            pass
        return None


def set(key: str, data: Any) -> None:
    path = _cache_path(key)
    try:
        if isinstance(data, pd.DataFrame):
            payload = {"__type": "dataframe", "records": data.to_json(orient="records", date_format="iso", force_ascii=False)}
        else:
            payload = data
        with open(path, "w", encoding="utf-8") as f:
            # allow_nan=False ensures NaN is rejected at write time rather than
            # producing invalid JSON literal `NaN` that non-Python parsers reject.
            json.dump({"ts": time.time(), "data": payload}, f,
                      ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError):
        # NaN / non-serializable value — skip caching silently
        pass
    except Exception:
        pass


def get_df(key: str, ttl_seconds: int) -> Optional[pd.DataFrame]:
    raw = get(key, ttl_seconds)
    if raw is None:
        return None
    if isinstance(raw, dict) and raw.get("__type") == "dataframe":
        try:
            return pd.read_json(raw["records"], orient="records")
        except Exception:
            return None
    return None


def set_df(key: str, df: pd.DataFrame) -> None:
    set(key, df)


# TTL constants (seconds)
TTL_REALTIME   = 30
TTL_PRICE_HISTORY = 3600       # during trading hours
TTL_VALUATION  = 86400         # 24h fallback
TTL_FINANCIAL  = 1209600       # 14 days (quarterly reports don't change faster)


def _secs_to_next_open() -> int:
    """Return seconds until the next A-share market open (9:30).

    If today is Mon-Fri and current time is before 9:30, returns seconds
    until today's open.  Otherwise advances to the next weekday.
    Weekend days are skipped.  Does not account for public holidays —
    occasional early re-fetches on holiday open-days are acceptable.
    """
    now = datetime.now()
    hour_min = now.hour * 60 + now.minute
    open_min = 9 * 60 + 30

    # How many calendar days until next weekday open?
    days_ahead = 0
    candidate = now
    while True:
        # Advance by one day if we're past today's open (or it's weekend)
        if days_ahead > 0 or hour_min >= open_min or candidate.weekday() >= 5:
            days_ahead += 1
            from datetime import timedelta
            candidate = now + timedelta(days=days_ahead)
        # Found a weekday before open
        if candidate.weekday() < 5:
            if days_ahead == 0:
                # same day, before open
                return (open_min - hour_min) * 60
            else:
                # future weekday: remaining seconds today + full days + seconds to open
                secs_today = (24 * 60 - hour_min) * 60
                secs_full_days = (days_ahead - 1) * 86400
                secs_to_open = open_min * 60
                return secs_today + secs_full_days + secs_to_open
        days_ahead += 1


def smart_price_ttl() -> int:
    """Return price history TTL: 1h during trading hours, extended otherwise.

    Outside trading hours the day's bars are finalised, so we cache until
    just after the next market open — avoiding repeated fetches overnight
    and during pre-market backtest runs.
    """
    now = datetime.now()
    hour_min = now.hour * 60 + now.minute
    # Morning session 9:25–11:35, afternoon session 12:55–15:05
    in_trading = (
        (9 * 60 + 25 <= hour_min <= 11 * 60 + 35) or
        (12 * 60 + 55 <= hour_min <= 15 * 60 + 5)
    )
    if in_trading:
        return TTL_PRICE_HISTORY  # 1 hour
    # Outside trading: cache until 10 min after next open
    return _secs_to_next_open() + 600


def smart_valuation_ttl() -> int:
    """Return valuation TTL: extended after market close, shorter before open."""
    return max(smart_price_ttl(), TTL_VALUATION)


def purge_expired(max_age_seconds: int = TTL_FINANCIAL) -> int:
    """
    Delete cache files whose timestamp is older than `max_age_seconds`.
    Defaults to 7 days (the longest TTL), so no still-valid data is ever removed.
    Returns the number of files deleted.
    """
    if not os.path.isdir(CACHE_DIR):
        return 0
    deleted = 0
    now = time.time()
    for fname in os.listdir(CACHE_DIR):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(CACHE_DIR, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                entry = json.load(f)
            if now - entry.get("ts", 0) > max_age_seconds:
                os.remove(path)
                deleted += 1
        except Exception:
            # Corrupted file — remove it too
            try:
                os.remove(path)
                deleted += 1
            except OSError:
                pass
    return deleted
