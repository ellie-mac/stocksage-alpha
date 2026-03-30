"""
Local file cache module.
- Real-time quotes: 30s TTL
- Price history:    1h TTL
- Valuation data:  24h TTL
- Financial data:  7d TTL (quarterly report frequency)
"""

import json
import os
import time
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
TTL_REALTIME = 30
TTL_PRICE_HISTORY = 3600
TTL_VALUATION = 86400
TTL_FINANCIAL = 604800
