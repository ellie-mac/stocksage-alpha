"""
Local file cache module.
- Real-time quotes: 30s TTL
- Price history:    1h TTL during trading hours; extended until next open otherwise
- Valuation data:  until next trading session open (or 24h fallback)
- Financial data:  14d TTL (quarterly report frequency)

Cache files are organized into subdirectories by data type:
  .cache/chip/        chip distribution, 6m_high
  .cache/price/       price history, forward close
  .cache/concept/     concept return data
  .cache/market/      market regime, market returns, valuation
  .cache/financial/   financial data, batch_financials
  .cache/indicators/  index constituents, industry maps
  .cache/fundflow/    fund flow data
  .cache/shareholders/ gdhs shareholder data
  .cache/meta/        trade calendar, stock info, suspension
  .cache/misc/        everything else
"""

import json
import os
import time
from datetime import datetime
from typing import Any, Optional
import pandas as pd

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

# prefix → subdirectory mapping (longest-prefix wins)
_SUBDIR_MAP = {
    "chip_data":        "chip",
    "chip_strategy":    "chip",
    "cyq":              "chip",
    "6m_high":          "chip",
    "price":            "price",
    "fwd_close":        "price",
    "concept_ret":      "concept",
    "concept_reverse":  "concept",
    "market_ret":       "market",
    "market_regime":    "market",
    "market_valuation": "market",
    "financial":        "financial",
    "valuation":        "financial",
    "batch_financials": "financial",
    "index_cons":       "indicators",
    "industry_map":     "indicators",
    "sw_industry":      "indicators",
    "trade_calendar":   "meta",
    "trade_dates":      "meta",
    "stock_info":       "meta",
    "suspension":       "meta",
    "fundflow":         "fundflow",
    "gdhs2":            "shareholders",
    "visits":           "misc",
}

# Pre-sorted by descending prefix length so longest-prefix wins on first match
_SUBDIR_LIST = sorted(_SUBDIR_MAP.items(), key=lambda x: -len(x[0]))


def _subdir_for(key: str) -> str:
    """Return the subdirectory name for a given cache key."""
    for prefix, subdir in _SUBDIR_LIST:
        if key.startswith(prefix):
            return subdir
    return "misc"


def _cache_path(key: str) -> str:
    subdir = os.path.join(CACHE_DIR, _subdir_for(key))
    os.makedirs(subdir, exist_ok=True)
    safe_key = key.replace("/", "_").replace("\\", "_")
    return os.path.join(subdir, f"{safe_key}.json")


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
    except Exception as e:
        print(f"[cache] warn: corrupt cache '{key}': {e}")
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
    except Exception as e:
        print(f"[cache] warn: failed to write '{key}': {e}")


def get_df(key: str, ttl_seconds: int) -> Optional[pd.DataFrame]:
    raw = get(key, ttl_seconds)
    if raw is None:
        return None
    if isinstance(raw, dict) and raw.get("__type") == "dataframe":
        try:
            import io
            return pd.read_json(io.StringIO(raw["records"]), orient="records")
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

    # Weekends: no market activity; keep Friday's cache valid until Monday open
    if now.weekday() >= 5:
        return _secs_to_next_open() + 600

    # Monday – Friday intraday logic
    # Morning session 9:25–11:35, afternoon session 12:55–15:05
    in_trading = (
        (9 * 60 + 25 <= hour_min <= 11 * 60 + 35) or
        (12 * 60 + 55 <= hour_min <= 15 * 60 + 5)
    )
    if in_trading:
        return TTL_PRICE_HISTORY  # 1 hour
    # After close (15:10+): TTL = seconds since close + buffer
    # → any cache built before close is instantly stale; post-close caches stay valid
    CLOSE_HM = 15 * 60 + 10
    if hour_min >= CLOSE_HM:
        close_dt = now.replace(hour=15, minute=10, second=0, microsecond=0)
        return int((now - close_dt).total_seconds()) + 60
    # Pre-market / lunch break: cache until 10 min after next open
    return _secs_to_next_open() + 600


def smart_valuation_ttl() -> int:
    """Return valuation TTL: always 24h — PE/PB doesn't change intraday."""
    return TTL_VALUATION


def purge_expired(max_age_seconds: int = TTL_FINANCIAL) -> int:
    """
    Delete cache files whose timestamp is older than `max_age_seconds`.
    Defaults to 14 days. Returns the number of files deleted.
    """
    if not os.path.isdir(CACHE_DIR):
        return 0
    deleted = 0
    now = time.time()
    for root, _, files in os.walk(CACHE_DIR):
        for fname in files:
            if not fname.endswith(".json"):
                continue
            path = os.path.join(root, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    entry = json.load(f)
                if now - entry.get("ts", 0) > max_age_seconds:
                    os.remove(path)
                    deleted += 1
            except Exception:
                try:
                    os.remove(path)
                    deleted += 1
                except OSError:
                    pass
    return deleted


def migrate_flat_cache() -> int:
    """Move legacy flat .cache/*.json files into their subdirectories."""
    if not os.path.isdir(CACHE_DIR):
        return 0
    moved = 0
    for fname in os.listdir(CACHE_DIR):
        if not fname.endswith(".json"):
            continue
        src = os.path.join(CACHE_DIR, fname)
        if not os.path.isfile(src):
            continue
        key = fname[:-5]  # strip .json
        dest = _cache_path(key)
        if src == dest:
            continue
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            os.rename(src, dest)
            moved += 1
        except OSError:
            pass
    return moved
