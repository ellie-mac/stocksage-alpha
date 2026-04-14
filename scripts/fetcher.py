"""
Data fetching module — wraps akshare with error handling and caching.
"""

import socket
import threading

socket.setdefaulttimeout(40)  # 40s cap on all socket ops (akshare HTTP + BaoStock TCP)

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import cache

_spot_lock = threading.Lock()
_spot_em_failed = False       # True after a timeout; reset after _SPOT_RETRY_SEC
_spot_em_failed_at: float = 0.0
_SPOT_RETRY_SEC = 300         # retry after 5 min (avoids permanent lock-out on transient failure)
_hist_em_failed = False  # once set True, skip stock_zh_a_hist and go straight to fallback
_hist_ts_failed = False  # once set True, skip Tushare daily this session
_hist_tdx_failed = False  # once set True, skip mootdx for price history this session
_rt_tdx_failed   = False  # once set True, skip mootdx for realtime quotes this session

_ts_pro = None           # Tushare Pro API handle; initialised lazily from alert_config token


def _get_tushare_pro():
    """Return a cached tushare Pro API handle, or None if token not configured / import fails."""
    global _ts_pro
    if _ts_pro is not None:
        return _ts_pro
    try:
        import tushare as ts
        import json, os
        cfg_path = os.path.join(os.path.dirname(__file__), "..", "alert_config.json")
        with open(cfg_path, encoding="utf-8") as _f:
            token = json.load(_f).get("tushare", {}).get("token", "")
        if not token:
            return None
        ts.set_token(token)
        _ts_pro = ts.pro_api()
        return _ts_pro
    except Exception:
        return None


# Module-level caches for full-market LHB and shareholder snapshots.
# Both are refreshed lazily; the helpers below are the only writers.
_lhb_cache: dict = {"df": None, "ts": 0.0}   # {"df": DataFrame, "ts": float}
_lhb_cache_lock = threading.Lock()
_shareholder_cache: dict = {}                  # date_str -> {"df": DataFrame, "ts": float}
_shareholder_cache_lock = threading.Lock()
_hot_rank_cache: dict = {"df": None, "ts": 0.0}  # full market hot-rank table; refreshed every 2h
_hot_rank_cache_lock = threading.Lock()

# stock_zh_a_daily and stock_fund_flow_individual both use py_mini_racer's V8 engine.
# Concurrent initialisation of V8 from multiple threads causes a fatal crash:
#   "Check failed: !IsConfigurablePoolInitialized()"
# Only the *first* call needs to be serialised (V8 init); once initialised, concurrent
# calls are safe.  Use an Event to signal that V8 is ready after the first call.
_v8_lock = threading.Lock()
_v8_initialised = threading.Event()

_bs_module = None        # baostock module, None if unavailable
_bs_lock = threading.Lock()


def _get_baostock():
    """Return the logged-in baostock module, logging in once per process."""
    global _bs_module
    if _bs_module is not None:
        return _bs_module
    with _bs_lock:
        if _bs_module is not None:
            return _bs_module
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != "0":
                return None
            # Best-effort: set a 30s recv timeout on the underlying socket so
            # rs.next() raises socket.timeout rather than blocking forever when
            # the server connection becomes stale (e.g. after hours of running).
            try:
                # baostock stores its socket as BaoStockSdk.__socket (name-mangled).
                # The module exposes the singleton's private attrs at module level.
                for _attr in ("_BaoStockSdk__socket", "_BaoStockSdk__client",
                              "_socket", "_client"):
                    _sock_candidate = getattr(bs, _attr, None)
                    if _sock_candidate is not None and hasattr(_sock_candidate, "settimeout"):
                        _sock_candidate.settimeout(30.0)
                        break
            except Exception:
                pass  # socket timeout is a best-effort optimisation; proceed without it
            import atexit
            atexit.register(bs.logout)
            _bs_module = bs
            return bs
        except Exception:
            return None


def _reset_baostock() -> None:
    """Force a fresh BaoStock login on the next _get_baostock() call.
    Called when a query times out, indicating a stale connection."""
    global _bs_module
    with _bs_lock:
        try:
            if _bs_module is not None:
                _bs_module.logout()
        except Exception:
            pass
        _bs_module = None


def _call_with_timeout(fn, timeout: float = 20.0, *args, **kwargs):
    """
    Run fn(*args, **kwargs) in a daemon thread and return its result.
    Returns None if the call doesn't finish within `timeout` seconds.
    Needed for akshare functions that use DrissionPage / JavaScript scrapers,
    which are unaffected by socket or requests timeouts.
    """
    result: list = [None]
    exc: list = [None]

    def _run():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            exc[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return None  # timed out — leave daemon thread running
    if exc[0] is not None:
        raise exc[0]
    return result[0]


def normalize_code(code: str) -> str:
    """Normalize stock code to a 6-digit string, stripping exchange prefixes."""
    code = code.strip().upper()
    for prefix in ("SH", "SZ", "SHE", "SSE", "BJ"):
        if code.startswith(prefix):
            code = code[len(prefix):]
    return code.zfill(6)


def _market_from_code(code: str) -> str:
    """Infer exchange from code prefix: 6xx -> sh, 8xx/43xxxx -> bj, others -> sz."""
    if code.startswith("6"):
        return "sh"
    if code.startswith("8") or code.startswith("43"):
        return "bj"
    return "sz"


def _try_numeric(val) -> Optional[float]:
    """Coerce val to float; return None on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _get_spot_df() -> pd.DataFrame:
    """
    Fetch full A-share real-time quote DataFrame.
    Cached for TTL_REALTIME seconds to avoid redundant full-market pulls
    (e.g. when both get_realtime_quote and search_stock_by_name are called).
    Uses a double-checked lock so concurrent threads don't each trigger
    a separate akshare call on a simultaneous cache miss.

    _spot_em_failed: once stock_zh_a_spot_em times out once, we mark it failed
    and skip all subsequent calls.  This prevents concurrent daemon threads from
    both initialising py_mini_racer's V8 engine simultaneously, which causes a
    fatal "Check failed: !IsConfigurablePoolInitialized()" crash (exit code 3).
    """
    global _spot_em_failed, _spot_em_failed_at
    import time as _time
    if _spot_em_failed:
        if _time.time() - _spot_em_failed_at < _SPOT_RETRY_SEC:
            return pd.DataFrame()
        # Cool-down elapsed — allow one retry
        _spot_em_failed = False
    cached = cache.get("spot_all", cache.TTL_REALTIME)
    if cached is not None:
        return pd.DataFrame(cached)
    with _spot_lock:
        # Re-check inside the lock; another thread may have populated it
        if _spot_em_failed:
            return pd.DataFrame()
        cached = cache.get("spot_all", cache.TTL_REALTIME)
        if cached is not None:
            return pd.DataFrame(cached)
        df = _call_with_timeout(ak.stock_zh_a_spot_em, 30)   # 30s; Sina fallback kicks in after
        if df is None or df.empty:
            _spot_em_failed = True
            _spot_em_failed_at = _time.time()
            return pd.DataFrame()
        cache.set("spot_all", df.to_dict("records"))
        return df


def _get_lhb_df() -> pd.DataFrame:
    """
    Fetch full-market LHB (龙虎榜) detail for the past 90 days.
    Cached in-process for 2 hours (TTL = 7200 s).  Thread-safe via _lhb_cache_lock.
    Returns an empty DataFrame on failure.
    """
    import time as _time
    TTL = 7200  # 2 hours
    with _lhb_cache_lock:
        cached_df = _lhb_cache.get("df")
        cached_ts = _lhb_cache.get("ts", 0.0)
        if cached_df is not None and (_time.time() - cached_ts) < TTL:
            return cached_df
        try:
            end_dt   = datetime.now()
            start_dt = end_dt - timedelta(days=90)
            end_str   = end_dt.strftime("%Y%m%d")
            start_str = start_dt.strftime("%Y%m%d")
            df = _call_with_timeout(ak.stock_lhb_detail_em, 30,
                                    start_date=start_str, end_date=end_str)
            if df is None or df.empty:
                _lhb_cache["df"] = pd.DataFrame()
                _lhb_cache["ts"] = _time.time()
                return pd.DataFrame()
            df.columns = [c.strip() for c in df.columns]
            df = df.reset_index(drop=True)
            _lhb_cache["df"] = df
            _lhb_cache["ts"] = _time.time()
            return df
        except Exception:
            _lhb_cache["df"] = pd.DataFrame()
            _lhb_cache["ts"] = _time.time()
            return pd.DataFrame()


def _get_shareholder_snapshot(date_str: str) -> pd.DataFrame:
    """
    Fetch full-market quarterly shareholder count from CNINFO for a given quarter-end date.
    date_str example: '20251231', '20250930'.
    Cached in-process for 7 days (TTL = 604800 s).  Thread-safe via _shareholder_cache_lock.
    Returns an empty DataFrame on failure.
    """
    import time as _time
    TTL = 604800  # 7 days
    with _shareholder_cache_lock:
        entry = _shareholder_cache.get(date_str)
        if entry is not None and (_time.time() - entry.get("ts", 0.0)) < TTL:
            return entry["df"]
        try:
            df = _call_with_timeout(ak.stock_hold_num_cninfo, 30, date=date_str)
            if df is None or df.empty:
                _shareholder_cache[date_str] = {"df": pd.DataFrame(), "ts": _time.time()}
                return pd.DataFrame()
            df.columns = [c.strip() for c in df.columns]
            df = df.reset_index(drop=True)
            _shareholder_cache[date_str] = {"df": df, "ts": _time.time()}
            return df
        except Exception:
            _shareholder_cache[date_str] = {"df": pd.DataFrame(), "ts": _time.time()}
            return pd.DataFrame()


_sina_cache: dict = {}          # code -> dict
_sina_cache_ts: float = 0.0
_sina_cache_lock = threading.Lock()
_SINA_TTL = 25                  # seconds; aligns with ~30s loop interval


def _parse_sina_entry(code: str, text_block: str) -> Optional[dict]:
    """Parse one var hq_str_xxx="..." line from Sina response."""
    inner = text_block.split('"')[1] if '"' in text_block else ""
    if not inner or len(inner) < 5:
        return None
    parts = inner.split(",")
    if len(parts) < 9:
        return None
    try:
        prev_close = float(parts[2] or 0)
        price      = float(parts[3] or 0)
        change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
        return {
            "code":       code,
            "name":       parts[0],
            "price":      price,
            "change_pct": round(change_pct, 2),
            "change_amt": round(price - prev_close, 3),
            "open":       float(parts[1] or 0),
            "prev_close": prev_close,
            "high":       float(parts[4] or 0),
            "low":        float(parts[5] or 0),
            "volume":     float(parts[8] or 0),
            "amount":     float(parts[9] or 0) if len(parts) > 9 else 0.0,
        }
    except (ValueError, IndexError):
        return None


def _warm_sina_cache(codes: list) -> None:
    """Batch-fetch quotes for all codes in one Sina request and populate _sina_cache."""
    import urllib.request, time as _t
    global _sina_cache, _sina_cache_ts
    keys = [f"{_market_from_code(c)}{c}" for c in codes if _market_from_code(c) != "bj"]
    if not keys:
        return
    url = f"https://hq.sinajs.cn/list={','.join(keys)}"
    try:
        req = urllib.request.Request(url, headers={"Referer": "https://finance.sina.com.cn"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            text = resp.read().decode("gbk", errors="replace")
        new_cache: dict = {}
        for line in text.strip().splitlines():
            # line: var hq_str_sh600036="..."
            if "hq_str_" not in line:
                continue
            raw_key = line.split("hq_str_")[1].split("=")[0].strip()
            code = raw_key[2:]   # strip market prefix
            entry = _parse_sina_entry(code, line)
            if entry:
                new_cache[code] = entry
        with _sina_cache_lock:
            _sina_cache = new_cache
            _sina_cache_ts = _t.time()
    except Exception:
        # Mark as attempted so _get_realtime_quote_sina doesn't spin on cache misses
        with _sina_cache_lock:
            _sina_cache_ts = _t.time()


def _get_realtime_quote_tencent(code: str) -> Optional[dict]:
    """
    Tencent Finance real-time quote via qt.gtimg.cn.
    Field layout (split by '~'):
      [1]=name [2]=code [3]=price [4]=prev_close [5]=open [6]=volume(手)
      [31]=change_amt [32]=change_pct(%) [33]=high [34]=low
      [36]=volume(手) [37]=amount(万元) [38]=turnover_rate [39]=pe_ttm
    """
    import urllib.request as _ur
    market = _market_from_code(code)
    if market == "bj":
        return None
    key = f"{market}{code}"
    url = f"https://qt.gtimg.cn/q={key}"
    try:
        req = _ur.Request(url, headers={"Referer": "https://finance.qq.com"})
        with _ur.urlopen(req, timeout=5) as resp:
            text = resp.read().decode("gbk", errors="replace")
        if '"' not in text:
            return None
        inner = text.split('"')[1]
        parts = inner.split("~")
        if len(parts) < 38 or not parts[3]:
            return None
        price      = float(parts[3] or 0)
        prev_close = float(parts[4] or 0)
        change_amt = float(parts[31] or 0) if len(parts) > 31 else price - prev_close
        change_pct = float(parts[32] or 0) if len(parts) > 32 else 0.0
        return {
            "code":         code,
            "name":         parts[1],
            "price":        price,
            "change_pct":   round(change_pct, 2),
            "change_amt":   round(change_amt, 3),
            "open":         float(parts[5] or 0),
            "prev_close":   prev_close,
            "high":         float(parts[33] or 0) if len(parts) > 33 else 0.0,
            "low":          float(parts[34] or 0) if len(parts) > 34 else 0.0,
            "volume":       float(parts[36] or 0) if len(parts) > 36 else float(parts[6] or 0),
            "amount":       float(parts[37] or 0) * 10000 if len(parts) > 37 else 0.0,
            "turnover_rate": float(parts[38] or 0) if len(parts) > 38 else 0.0,
            "pe_ttm":       float(parts[39] or 0) if len(parts) > 39 else 0.0,
        }
    except Exception:
        return None


def _get_realtime_quote_sina(code: str) -> Optional[dict]:
    """
    Fallback: return quote from module-level Sina cache (populated by _warm_sina_cache).
    Falls back to a single-stock request if cache is stale or code is missing.
    """
    import urllib.request, time as _t
    with _sina_cache_lock:
        if _t.time() - _sina_cache_ts < _SINA_TTL and code in _sina_cache:
            return _sina_cache[code]
    # Cache miss or stale — single-stock fetch
    market = _market_from_code(code)
    if market == "bj":
        return None
    key = f"{market}{code}"
    url = f"https://hq.sinajs.cn/list={key}"
    try:
        req = urllib.request.Request(url, headers={"Referer": "https://finance.sina.com.cn"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            text = resp.read().decode("gbk", errors="replace")
        for line in text.strip().splitlines():
            entry = _parse_sina_entry(code, line)
            if entry:
                return entry
        return None
    except Exception:
        return None


def _get_realtime_quote_tdx(code: str) -> Optional[dict]:
    """
    mootdx/通达信 real-time quote via binary TCP.
    Current price is always unadjusted (== actual market price), so no
    adjustment needed for realtime — this is the correct value to display
    and compare against cost_price.
    """
    global _rt_tdx_failed
    if _rt_tdx_failed:
        return None
    try:
        from mootdx.quotes import Quotes as _MootdxQuotes
        _tdx = _MootdxQuotes.factory(market='std')
        mkt = 1 if _market_from_code(code) == "sh" else 0
        df = _tdx.quotes(security_list=[(mkt, code)])
        if df is None or df.empty:
            return None
        r = df.iloc[0]
        price      = float(r.get("price",      0) or 0)
        prev_close = float(r.get("last_close",  0) or r.get("pre_close", 0) or 0)
        open_p     = float(r.get("open",        0) or 0)
        high       = float(r.get("high",        0) or 0)
        low        = float(r.get("low",         0) or 0)
        vol        = float(r.get("vol",         r.get("volume", 0)) or 0)
        amount     = float(r.get("amount",      0) or 0)
        if price <= 0:
            return None
        change_amt = round(price - prev_close, 4) if prev_close else 0.0
        change_pct = round(change_amt / prev_close * 100, 2) if prev_close else 0.0
        return {
            "code":       code,
            "name":       str(r.get("name", "")),
            "price":      price,
            "change_pct": change_pct,
            "change_amt": change_amt,
            "open":       open_p,
            "prev_close": prev_close,
            "high":       high,
            "low":        low,
            "volume":     vol,
            "amount":     amount,
        }
    except ImportError:
        _rt_tdx_failed = True
        return None
    except Exception:
        return None


def get_realtime_quote(code: str) -> Optional[dict]:
    """Return real-time quote fields for a single stock code."""
    try:
        df = _get_spot_df()
        if not df.empty:
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                return {
                    "code": code,
                    "name": str(r.get("名称", "")),
                    "price": float(r.get("最新价", 0) or 0),
                    "change_pct": float(r.get("涨跌幅", 0) or 0),
                    "change_amt": float(r.get("涨跌额", 0) or 0),
                    "volume": float(r.get("成交量", 0) or 0),
                    "amount": float(r.get("成交额", 0) or 0),
                    "market_cap": float(r.get("总市值", 0) or 0),
                    "circulating_cap": float(r.get("流通市值", 0) or 0),
                    "pe_ttm": float(r.get("市盈率-动态", 0) or 0),
                    "pb": float(r.get("市净率", 0) or 0),
                    "turnover_rate": float(r.get("换手率", 0) or 0),
                    "amplitude":     float(r.get("振幅", 0) or 0),
                    "high":          float(r.get("最高", 0) or 0),
                    "low":           float(r.get("最低", 0) or 0),
                    "open":          float(r.get("今开", 0) or 0),
                    "prev_close":    float(r.get("昨收", 0) or 0),
                    "volume_ratio":  float(r.get("量比", 0) or 0),
                    "div_yield":     float(r.get("股息率-TTM", 0) or 0),
                    "return_5d":     float(r.get("5日涨跌幅", 0) or 0),
                    "return_10d":    float(r.get("10日涨跌幅", 0) or 0),
                    "return_20d":    float(r.get("20日涨跌幅", 0) or 0),
                }
        # East Money full-market fetch failed — fall back to Sina, then Tencent, then mootdx
        result = _get_realtime_quote_sina(code)
        if result:
            return result
        result = _get_realtime_quote_tencent(code)
        if result:
            return result
        return _get_realtime_quote_tdx(code)
    except Exception as e:
        return {"error": f"Failed to fetch quote: {e}"}


def get_stock_info(code: str) -> Optional[dict]:
    """Fetch stock meta: industry, listing date, share counts.  Cached for 7 days."""
    cache_key = f"stock_info_{code}"
    cached = cache.get(cache_key, cache.TTL_FINANCIAL)
    if cached is not None:
        return cached
    try:
        df = ak.stock_individual_info_em(symbol=code)
        info = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        result = {
            "industry": str(info.get("行业", "Unknown")),
            "listing_date": str(info.get("上市时间", "")),
            "total_shares": str(info.get("总股本", "")),
            "circulating_shares": str(info.get("流通股", "")),
        }
        cache.set(cache_key, result)
        return result
    except Exception:
        return {}


_PRICE_FETCH_DAYS = 550  # Always fetch this many days so all rolling periods share one cache entry

def get_price_history(code: str, days: int = 365) -> Optional[pd.DataFrame]:
    """Fetch daily OHLCV history (qfq adjusted). Cached for 1 hour.

    Always fetches _PRICE_FETCH_DAYS (550d) so that rolling-period IC tests
    with different `days` values share a single cache entry per stock.

    Source priority:
      1. East Money  (stock_zh_a_hist)   — best adjusted (qfq) data
      2. Tushare Pro (daily, adj=qfq)    — basic tier, reliable; data available 15:00-16:00
      3. BaoStock                         — free, no V8; single-socket, Windows-fragile
      4. 163/Netease (stock_zh_a_daily)  — V8-based
      5. mootdx/通达信                   — binary TCP; unadjusted (last resort)
    """
    global _hist_em_failed, _hist_ts_failed, _hist_tdx_failed
    fetch_days = max(days, _PRICE_FETCH_DAYS)
    cache_key = f"price_{code}_{fetch_days}"
    cached = cache.get_df(cache_key, cache.smart_price_ttl())
    if cached is not None:
        # Caller may ask for fewer rows — slice to requested window
        if len(cached) > days:
            return cached.tail(days).reset_index(drop=True)
        return cached

    # ── Source 1: East Money ─────────────────────────────────────────────
    if not _hist_em_failed:
        try:
            end = datetime.now()
            start = end - timedelta(days=days)
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                adjust="qfq",
            )
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns={
                "日期": "date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "成交额": "amount", "振幅": "amplitude",
                "涨跌幅": "change_pct", "涨跌额": "change_amt", "换手率": "turnover",
            })
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            cache.set_df(cache_key, df)
            return df
        except Exception:
            _hist_em_failed = True  # fail fast for all subsequent calls this session

    # ── Source 2: Tushare Pro daily (adj=qfq) ────────────────────────────
    # Basic tier (120pts), 500 calls/min, data available ~15:00-16:00 each day.
    if not _hist_ts_failed:
        try:
            pro = _get_tushare_pro()
            if pro is None:
                _hist_ts_failed = True
            else:
                ts_code = f"{code}.SH" if _market_from_code(code) == "sh" else f"{code}.SZ"
                end_ts = datetime.now()
                start_ts = end_ts - timedelta(days=fetch_days + 10)
                df = pro.daily(
                    ts_code=ts_code,
                    adj="qfq",
                    start_date=start_ts.strftime("%Y%m%d"),
                    end_date=end_ts.strftime("%Y%m%d"),
                    fields="trade_date,open,high,low,close,vol,amount,pct_chg,change",
                )
                if df is not None and not df.empty:
                    df = df.rename(columns={
                        "trade_date": "date", "vol": "volume",
                        "pct_chg": "change_pct", "change": "change_amt",
                    })
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date").reset_index(drop=True)
                    for col in ["open", "high", "low", "close", "volume", "change_pct", "change_amt"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                    if not df.empty:
                        cache.set_df(cache_key, df)
                        if len(df) > days:
                            return df.tail(days).reset_index(drop=True)
                        return df
        except Exception:
            _hist_ts_failed = True

    # ── Source 3: BaoStock (free, no V8, reliable) ───────────────────────
    # BaoStock's Python client is NOT thread-safe: all concurrent callers share
    # one TCP socket.  Without _bs_lock here, concurrent rs.next() calls in
    # different threads consume each other's response bytes, leaving some threads
    # blocked forever in socket.recv() — which deadlocks the executor across
    # backtest periods.  Hold _bs_lock for the entire send+receive cycle.
    #
    # After long runs the TCP connection can become stale (server-side timeout).
    # We wrap the query in _call_with_timeout so rs.next() cannot block forever:
    # on timeout _reset_baostock() forces a fresh login for the next caller.
    try:
        bs = _get_baostock()
        if bs is not None:
            prefix = _market_from_code(code)
            bs_code = f"{prefix}.{code}"
            end = datetime.now()
            start = end - timedelta(days=fetch_days)

            _bs_rows: list = []

            def _do_bs_query():
                # Use acquire(timeout) instead of `with _bs_lock` so that a stalled
                # daemon thread (left behind by a previous _call_with_timeout expiry)
                # cannot hold the lock indefinitely and block all subsequent callers.
                # 65 s > the 60 s _call_with_timeout budget, so the outer wrapper will
                # have already given up and called _reset_baostock() before this path
                # returns — but at least every thread eventually exits rather than
                # accumulating into a permanent deadlock.
                if not _bs_lock.acquire(timeout=65.0):
                    return [], []  # previous stalled thread still owns the lock; skip
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,turn,pctChg",
                        start_date=start.strftime("%Y-%m-%d"),
                        end_date=end.strftime("%Y-%m-%d"),
                        frequency="d",
                        adjustflag="2",  # qfq
                    )
                    result = []
                    while rs.error_code == "0" and rs.next():
                        result.append(rs.get_row_data())
                    return result, rs.fields
                except OSError:
                    # WinError 10038 / broken socket — reset so next caller gets
                    # a fresh login instead of reusing the dead connection.
                    _reset_baostock()
                    return [], []
                finally:
                    _bs_lock.release()

            _bs_result = _call_with_timeout(_do_bs_query, timeout=60.0)
            if _bs_result is None:
                # Timed out — connection likely stale; reset for next caller
                _reset_baostock()
            else:
                rows, _bs_fields = _bs_result
                if rows:
                    df = pd.DataFrame(rows, columns=_bs_fields)
                    df = df.rename(columns={"turn": "turnover", "pctChg": "change_pct"})
                    df["date"] = pd.to_datetime(df["date"])
                    for col in ["open", "high", "low", "close", "volume", "turnover", "change_pct"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                    df["change_amt"] = df["close"].diff()
                    df = df.sort_values("date").reset_index(drop=True)
                    if not df.empty:
                        cache.set_df(cache_key, df)
                        if len(df) > days:
                            return df.tail(days).reset_index(drop=True)
                        return df
    except Exception:
        pass

    # ── Source 4: 163/Netease via stock_zh_a_daily (V8 — last resort) ───
    # Note: 163/Netease does not carry BJ exchange stocks; they will 404 silently.
    # BJ stocks (prefix="bj") should have been served by BaoStock above.
    try:
        prefix = _market_from_code(code)
        if not _v8_initialised.is_set():
            with _v8_lock:
                df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
                _v8_initialised.set()
        else:
            df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df = df[df["date"] >= cutoff].reset_index(drop=True)
        if df.empty:
            return None
        if "change_pct" not in df.columns:
            df["change_pct"] = df["close"].pct_change() * 100
        if "change_amt" not in df.columns:
            df["change_amt"] = df["close"].diff()
        if "turnover" in df.columns:
            df["turnover"] = df["turnover"] * 100
        cache.set_df(cache_key, df)
        return df
    except Exception:
        pass

    # ── Source 5: mootdx / 通达信 (unadjusted — last resort) ─────────────
    # WARNING: returns unadjusted prices. Price-based factors (momentum, MA, etc.)
    # may be slightly off for stocks with recent corporate actions.
    if not _hist_tdx_failed:
        try:
            from mootdx.quotes import Quotes as _MootdxQuotes  # lazy import
            _tdx = _MootdxQuotes.factory(market='std')
            df = _tdx.bars(symbol=code, frequency=9, offset=fetch_days + 50)
            if df is not None and not df.empty:
                df = df.reset_index(drop=True)
                if "vol" in df.columns and "volume" in df.columns:
                    df = df.drop(columns=["vol"])
                elif "vol" in df.columns:
                    df = df.rename(columns={"vol": "volume"})
                df["date"] = pd.to_datetime(df["datetime"]).dt.normalize()
                for col in ["open", "high", "low", "close", "volume"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.sort_values("date").reset_index(drop=True)
                df["change_pct"] = df["close"].pct_change() * 100
                df["change_amt"] = df["close"].diff()
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=fetch_days + 10)
                df = df[df["date"] >= cutoff].reset_index(drop=True)
                if not df.empty:
                    cache.set_df(cache_key, df)
                    if len(df) > days:
                        return df.tail(days).reset_index(drop=True)
                    return df
        except ImportError:
            _hist_tdx_failed = True
        except Exception:
            _hist_tdx_failed = True

    return None


def get_valuation_history(code: str) -> Optional[pd.DataFrame]:
    """Fetch historical PE/PB valuation data. Cached until next market open.

    Source priority:
      1. BaoStock  (peTTM, pbMRQ, psTTM — free, permanent)
    """
    cache_key = f"valuation_{code}"
    cached = cache.get_df(cache_key, cache.smart_valuation_ttl())
    if cached is not None:
        return cached

    # ── Source 1: BaoStock ───────────────────────────────────────────────
    try:
        bs = _get_baostock()
        if bs is not None:
            prefix = _market_from_code(code)
            bs_code = f"{prefix}.{code}"
            end_v = datetime.now()
            start_v = end_v - timedelta(days=_PRICE_FETCH_DAYS + 10)

            _val_rows: list = []

            def _do_val_query():
                if not _bs_lock.acquire(timeout=65.0):
                    return [], []
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                        start_date=start_v.strftime("%Y-%m-%d"),
                        end_date=end_v.strftime("%Y-%m-%d"),
                        frequency="d",
                        adjustflag="3",  # no adjustment needed for valuation
                    )
                    result = []
                    while rs.error_code == "0" and rs.next():
                        result.append(rs.get_row_data())
                    return result, rs.fields
                except OSError:
                    _reset_baostock()
                    return [], []
                finally:
                    _bs_lock.release()

            _val_result = _call_with_timeout(_do_val_query, timeout=60.0)
            if _val_result is None:
                _reset_baostock()
            else:
                rows, _fields = _val_result
                if rows:
                    df = pd.DataFrame(rows, columns=_fields)
                    df = df.rename(columns={
                        "peTTM": "pe_ttm", "pbMRQ": "pb",
                        "psTTM": "ps_ttm", "pcfNcfTTM": "pcf_ttm",
                    })
                    df["date"] = pd.to_datetime(df["date"])
                    for col in ["pe_ttm", "pb", "ps_ttm", "pcf_ttm"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.sort_values("date").reset_index(drop=True)
                    if not df.empty:
                        cache.set_df(cache_key, df)
                        return df
    except Exception:
        pass

    # ── Stale cache fallback (all sources down) ───────────────────────────
    # PE/PB changes little day-to-day; using data up to 5 days old is fine
    # for factor scoring and avoids hard failures during BaoStock outages.
    stale = cache.get_df(cache_key, 5 * 86400)
    if stale is not None:
        return stale

    return None


def get_financial_indicators(code: str) -> Optional[pd.DataFrame]:
    """Fetch financial indicators (ROE, margins, growth rates). Cached for 14 days.

    Source priority:
      1. akshare EM  (stock_financial_analysis_indicator)
    """
    cache_key = f"financial_{code}"
    cached = cache.get_df(cache_key, cache.TTL_FINANCIAL)
    if cached is not None:
        return cached

    # ── Source 1: akshare East Money ─────────────────────────────────────
    try:
        df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2020")
        if df is not None and not df.empty:
            df = df.reset_index(drop=True)
            cache.set_df(cache_key, df)
            return df
    except Exception:
        pass

    # ── Stale cache fallback (all sources down) ───────────────────────────
    # Financial reports change quarterly; 30-day-old data is still valid for scoring.
    stale = cache.get_df(cache_key, 30 * 86400)
    if stale is not None:
        return stale

    return None


def get_fund_flow(code: str, days: int = 10) -> Optional[pd.DataFrame]:
    """
    Fetch per-stock order-flow breakdown (large / medium / small orders).
    Used as an institutional money-flow proxy.
    Cached for 5 minutes — fund flow changes rapidly intraday.
    """
    cache_key = f"fundflow_{code}_{days}"
    cached = cache.get_df(cache_key, cache.TTL_PRICE_HISTORY // 12)
    if cached is not None:
        return cached
    try:
        market = _market_from_code(code)
        if not _v8_initialised.is_set():
            with _v8_lock:
                df = ak.stock_individual_fund_flow(stock=code, market=market)
                _v8_initialised.set()
        else:
            df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.tail(days).reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_margin_data(code: str) -> Optional[pd.DataFrame]:
    """
    Fetch margin trading balance history for a stock.
    Supports both SSE (6xx) and SZSE codes.
    Cached for 24 hours.
    """
    cache_key = f"margin_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        if code.startswith("6"):
            df = ak.stock_margin_detail_sse(symbol=code)
        else:
            df = ak.stock_margin_detail_szse(symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.sort_values(df.columns[0]).tail(30).reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_cyq(code: str) -> Optional[pd.DataFrame]:
    """
    Fetch chip distribution data (筹码分布) for a stock via East Money.
    Uses ak.stock_cyq_em(symbol=code).
    Cached for 4 hours (14400 seconds) — chip distribution is slow-moving intraday.
    Returns the DataFrame or None on any error.
    """
    cache_key = f"cyq_{code}"
    cached = cache.get_df(cache_key, 14400)
    if cached is not None:
        return cached
    try:
        df = ak.stock_cyq_em(symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Group B: Additional per-stock data (optional; add latency)
# ---------------------------------------------------------------------------

def get_shareholder_count(code: str) -> Optional[pd.DataFrame]:
    """
    Fetch quarterly shareholder count history for a single stock.

    Uses ak.stock_hold_num_cninfo (full-market, quarter-end snapshot) instead of the
    deprecated per-stock ak.stock_zh_a_gdhs (V8-based, slow, unreliable).

    Tries the last 4 quarter-end dates in descending order; returns the first hit.
    Returns a DataFrame with standardised columns:
        date, total_holders, holder_change_pct
    so that score_shareholder_change can consume it unchanged.
    Cached per-stock for 7 days (TTL_FINANCIAL).
    """
    cache_key = f"gdhs2_{code}"
    cached = cache.get_df(cache_key, cache.TTL_FINANCIAL)
    if cached is not None:
        return cached

    # Build last-4 quarter-end dates relative to today
    today = datetime.now()
    quarter_ends = []
    for year_offset in range(2):
        yr = today.year - year_offset
        for month, day in [(12, 31), (9, 30), (6, 30), (3, 31)]:
            qdate = datetime(yr, month, day)
            if qdate <= today:
                quarter_ends.append(qdate.strftime("%Y%m%d"))
    quarter_ends = quarter_ends[:4]

    rows = []
    for date_str in quarter_ends:
        snap = _get_shareholder_snapshot(date_str)
        if snap.empty:
            continue
        # Column name may be '证券代码' with or without leading zeros
        code_col = None
        for c in snap.columns:
            if "代码" in c:
                code_col = c
                break
        if code_col is None:
            continue
        row = snap[snap[code_col].astype(str).str.zfill(6) == code.zfill(6)]
        if row.empty:
            continue
        r = row.iloc[0]
        rows.append({
            "date": date_str,
            "total_holders":     _try_numeric(r.get("本期股东人数")),
            "holder_change_pct": _try_numeric(r.get("股东人数增幅")),
        })

    if not rows:
        return None

    result = pd.DataFrame(rows).sort_values("date", ascending=False).reset_index(drop=True)
    cache.set_df(cache_key, result)
    return result


def get_lhb_flow(code: str, days: int = 90) -> Optional[pd.DataFrame]:
    """
    Fetch Dragon-Tiger list (龙虎榜) net flows for the past N days.

    Fetches the full-market LHB DataFrame once (cached 2 h via _get_lhb_df()),
    then filters by stock code.  This avoids the broken per-stock API signature
    `ak.stock_lhb_detail_em(symbol=code)`.

    Returns rows sorted by date (ascending) or None if no entries found.
    """
    full_df = _get_lhb_df()
    if full_df.empty:
        return None
    # Identify the code column
    code_col = None
    for c in full_df.columns:
        if c in ("代码", "股票代码"):
            code_col = c
            break
    if code_col is None:
        return None
    stock_df = full_df[full_df[code_col].astype(str).str.zfill(6) == code.zfill(6)].copy()
    if stock_df.empty:
        return None
    # Sort by date column if present
    date_col = None
    for c in stock_df.columns:
        if "日" in c or "date" in c.lower():
            date_col = c
            break
    if date_col is not None:
        stock_df = stock_df.sort_values(date_col, ascending=True)
    return stock_df.reset_index(drop=True)


def get_lockup_pressure(code: str) -> Optional[pd.DataFrame]:
    """Fetch upcoming lock-up expiry (解禁) schedule. Cached for 24h."""
    cache_key = f"lockup_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        df = _call_with_timeout(ak.stock_restricted_release_detail_em, 20, symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_insider_transactions(code: str) -> Optional[pd.DataFrame]:
    """Fetch major shareholder buy/sell transactions (增减持). Cached for 24h."""
    cache_key = f"insider_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        df = _call_with_timeout(ak.stock_share_hold_change_em, 20, symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_institutional_visits(code: str) -> Optional[pd.DataFrame]:
    """Fetch institutional research visit records (机构调研). Cached for 24h."""
    cache_key = f"visits_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        df = _call_with_timeout(ak.stock_irm_cninfo, 20, symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_industry_momentum(industry_name: str) -> Optional[float]:
    """
    Fetch 1-month return for an industry board.
    Returns the percentage return as a float, or None on failure.
    Cached for 1 hour.
    """
    cache_key = f"industry_ret_{industry_name}"
    cached = cache.get(cache_key, cache.TTL_PRICE_HISTORY)
    if cached is not None:
        return float(cached)
    try:
        end   = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=35)).strftime("%Y%m%d")
        df = ak.stock_board_industry_hist_em(
            symbol=industry_name, period="daily",
            start_date=start, end_date=end, adjust=""
        )
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        close_col = next((c for c in df.columns if "收盘" in c), None)
        if close_col is None:
            return None
        close = pd.to_numeric(df[close_col], errors="coerce").dropna()
        if len(close) < 2:
            return None
        ret = float((close.iloc[-1] / close.iloc[0] - 1) * 100)
        cache.set(cache_key, ret)
        return ret
    except Exception:
        return None


def get_market_return_1m() -> Optional[float]:
    """Fetch 1-month return of the broad A-share market (CSI 300 proxy). Cached 1h."""
    cache_key = "market_ret_1m"
    cached = cache.get(cache_key, cache.TTL_PRICE_HISTORY)
    if cached is not None:
        return float(cached)
    try:
        # Use 000300 (CSI 300) as market proxy; fetch full history and take last 25 trading days
        df = ak.stock_zh_index_daily_em(symbol="sh000300")
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        close_col = next((c for c in df.columns if "close" in c.lower() or "收盘" in c), None)
        if close_col is None:
            return None
        close = pd.to_numeric(df[close_col], errors="coerce").dropna().tail(25)
        if len(close) < 2:
            return None
        ret = float((close.iloc[-1] / close.iloc[0] - 1) * 100)
        cache.set(cache_key, ret)
        return ret
    except Exception:
        return None


def get_northbound_holdings(code: str) -> Optional[pd.DataFrame]:
    """Fetch per-stock 沪深港通 northbound holding history. Cached for 24h."""
    cache_key = f"nb_hold_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        market = "沪股通" if code.startswith("6") else "深股通"
        df = _call_with_timeout(ak.stock_hsgt_hold_stock_em, 20, market=market, stock=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.sort_values(df.columns[0]).tail(20).reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_earnings_revision(code: str) -> Optional[pd.DataFrame]:
    """Fetch analyst EPS forecast revision history. Cached for 24h."""
    cache_key = f"revision_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        df = _call_with_timeout(ak.stock_analyst_forecast_em, 20, symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def _get_hot_rank_df() -> Optional[pd.DataFrame]:
    """Fetch the full East Money hot-rank table once and cache it for 2 hours.

    Called by get_social_heat() for every stock; centralising the fetch here
    means the full table is downloaded only once per 2h window regardless of
    how many stocks are scored.  Thread-safe via _hot_rank_cache_lock.
    """
    TTL = 7200  # 2 hours
    with _hot_rank_cache_lock:
        cached_df = _hot_rank_cache.get("df")
        cached_ts = _hot_rank_cache.get("ts", 0.0)
        if cached_df is not None and (_time.time() - cached_ts) < TTL:
            return cached_df
        try:
            df = _call_with_timeout(ak.stock_hot_rank_em, 20)
            if df is None or df.empty:
                _hot_rank_cache["df"] = pd.DataFrame()
                _hot_rank_cache["ts"] = _time.time()
                return None
            df.columns = [c.strip() for c in df.columns]
            _hot_rank_cache["df"] = df
            _hot_rank_cache["ts"] = _time.time()
            return df
        except Exception:
            _hot_rank_cache["df"] = pd.DataFrame()
            _hot_rank_cache["ts"] = _time.time()
            return None


def get_social_heat(code: str) -> Optional[dict]:
    """
    Fetch East Money stock discussion heat.
    Returns dict with rank and recent post metrics, or None.
    Cached per-stock for 2 hours.  The underlying full-market table is fetched
    only once per 2h via _get_hot_rank_df() (shared across all stock calls).
    """
    cache_key = f"social_heat_{code}"
    cached = cache.get(cache_key, cache.TTL_PRICE_HISTORY * 2)
    if cached is not None:
        return cached
    try:
        df = _get_hot_rank_df()
        if df is not None and not df.empty:
            code_col = next((c for c in df.columns if "代码" in c or "code" in c.lower()), None)
            rank_col = next((c for c in df.columns if "排名" in c or "rank" in c.lower()), None)
            if code_col and rank_col:
                row = df[df[code_col].astype(str).str.contains(code)]
                if not row.empty:
                    rank = int(row[rank_col].iloc[0])
                    result = {"rank": rank, "total": len(df), "rank_pct": round(rank / len(df) * 100, 1)}
                    cache.set(cache_key, result)
                    return result
    except Exception:
        pass
    return None


def get_market_regime_data() -> Optional[pd.DataFrame]:
    """
    Fetch CSI 300 (沪深300) recent price history for market regime detection.
    Returns last 300 trading days. Cached for 2 hours.
    """
    cache_key = "market_regime_data"
    cached = cache.get_df(cache_key, cache.TTL_PRICE_HISTORY * 2)
    if cached is not None:
        return cached
    for fetch in [
        lambda: ak.stock_zh_index_daily_em(symbol="sh000300"),
        lambda: ak.stock_zh_index_daily(symbol="sh000300"),  # 163/Netease fallback
    ]:
        try:
            df = fetch()
            if df is None or df.empty:
                continue
            df.columns = [c.strip() for c in df.columns]
            close_col = next((c for c in df.columns if "close" in c.lower() or "收盘" in c), None)
            if close_col is None:
                continue
            df = df.rename(columns={close_col: "close"})
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["close"]).tail(300).reset_index(drop=True)
            cache.set_df(cache_key, df)
            return df
        except Exception:
            continue
    return None


def _get_concept_1m_ret(concept_name: str) -> Optional[float]:
    """Fetch 1-month return for a concept board. Cached 1h."""
    cache_key = f"concept_ret_{concept_name}"
    cached = cache.get(cache_key, cache.TTL_PRICE_HISTORY)
    if cached is not None:
        return float(cached)
    try:
        end   = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=35)).strftime("%Y%m%d")
        df = ak.stock_board_concept_hist_em(
            symbol=concept_name, period="daily",
            start_date=start, end_date=end, adjust=""
        )
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        close_col = next((c for c in df.columns if "收盘" in c), None)
        if close_col is None:
            return None
        close = pd.to_numeric(df[close_col], errors="coerce").dropna()
        if len(close) < 2:
            return None
        ret = float((close.iloc[-1] / close.iloc[0] - 1) * 100)
        cache.set(cache_key, ret)
        return ret
    except Exception:
        return None


def _build_concept_reverse_map() -> dict:
    """
    Build {stock_code: [concept_name, ...]} reverse lookup map.
    Iterates all concept boards in parallel (16 workers) and indexes their constituents.
    Cached for 6 hours — expensive first call (~30s), free on subsequent calls.
    """
    cache_key = "concept_reverse_map"
    cached = cache.get(cache_key, 6 * 3600)
    if cached is not None:
        return cached
    try:
        from concurrent.futures import ThreadPoolExecutor
        concept_df = ak.stock_board_concept_name_em()
        if concept_df is None or concept_df.empty:
            return {}
        concept_df.columns = [c.strip() for c in concept_df.columns]
        name_col = next(
            (c for c in concept_df.columns if "名称" in c),
            concept_df.columns[0],
        )
        concept_names = [str(n) for n in concept_df[name_col].dropna().tolist()]

        reverse_map: dict = {}

        def _fetch_cons(cname: str):
            try:
                df = ak.stock_board_concept_cons_em(symbol=cname)
                if df is None or df.empty:
                    return cname, []
                df.columns = [c.strip() for c in df.columns]
                code_col = next(
                    (c for c in df.columns if "代码" in c or c.lower() == "code"),
                    None,
                )
                if code_col is None:
                    return cname, []
                codes = [str(c).zfill(6) for c in df[code_col].dropna().tolist()]
                return cname, codes
            except Exception:
                return cname, []

        with ThreadPoolExecutor(max_workers=16) as ex:
            for cname, codes in ex.map(_fetch_cons, concept_names):
                for code in codes:
                    reverse_map.setdefault(code, []).append(cname)

        cache.set(cache_key, reverse_map)
        return reverse_map
    except Exception:
        return {}


def get_concept_momentum(code: str) -> Optional[list]:
    """
    Fetch top-5 concept board 1-month returns for a given stock code.
    Returns a list of {"name": str, "ret_1m": float} sorted by |ret_1m| desc,
    so both hot-concept and collapsing-concept signals surface at the top.
    Returns None if the stock has no mapped concepts or data is unavailable.
    First call triggers _build_concept_reverse_map (cached 6h; ~30s cold start).
    """
    reverse_map = _build_concept_reverse_map()
    concept_names = reverse_map.get(code, [])
    if not concept_names:
        return None

    results = []
    for cname in concept_names[:40]:   # cap per-stock concept scan
        ret = _get_concept_1m_ret(cname)
        if ret is not None:
            results.append({"name": cname, "ret_1m": round(ret, 2)})

    if not results:
        return None

    results.sort(key=lambda x: abs(x["ret_1m"]), reverse=True)
    return results[:5]


def get_market_valuation() -> Optional[pd.DataFrame]:
    """Fetch index-level PE context. Returns daily history with columns:
    date, market_pe (TTM), and optionally market_cap / market_dv.

    Source priority:
      1. AKShare 乐咕乐股  (stock_market_pe_lg, 上证A股, 1600+ days)
      2. AKShare 乐咕乐股  (stock_index_pe_lg, 沪深300, 5100+ days — different endpoint)
      3. CSI 官网          (stock_zh_index_value_csindex, 000300, last ~20 trading days)

    Cached until next market open.
    """
    cache_key = "market_valuation"
    cached = cache.get_df(cache_key, cache.smart_valuation_ttl())
    if cached is not None:
        return cached

    # ── Source 1: 乐咕乐股 上证A股 broad-market PE ─────────────────────────
    try:
        df = ak.stock_market_pe_lg(symbol="上证A股")
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns={"日期": "date", "总市值": "market_cap", "市盈率": "market_pe"})
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            cache.set_df(cache_key, df)
            return df
    except Exception:
        pass

    # ── Source 2: 乐咕乐股 沪深300 index PE (different endpoint/server) ────
    try:
        df = ak.stock_index_pe_lg(symbol="沪深300")
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            pe_col = next(
                (c for c in df.columns if "滚动市盈率" in c
                 and "中位" not in c and "等权" not in c),
                next((c for c in df.columns if "市盈率" in c), None),
            )
            df = df.rename(columns={"日期": "date"})
            if pe_col:
                df = df.rename(columns={pe_col: "market_pe"})
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            cache.set_df(cache_key, df)
            return df
    except Exception:
        pass

    # ── Source 3: CSI 官网 (中证指数, 沪深300, last ~20 trading days) ──────
    try:
        df = ak.stock_zh_index_value_csindex(symbol="000300")
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns={"日期": "date", "市盈率2": "market_pe", "股息率2": "market_dv"})
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            cache.set_df(cache_key, df)
            return df
    except Exception:
        pass

    return None


def get_sw_industry_pe() -> Optional[pd.DataFrame]:
    """Fetch current PE/PB snapshot for all 申万一级 (Shenwan level-1) industries.

    Returns DataFrame with columns: 行业代码, 行业名称, 成份个数, 静态市盈率,
    TTM(滚动)市盈率, 市净率, 静态股息率.
    Cached for 24h (refreshed once per trading day).
    """
    cache_key = "sw_industry_pe"
    cached = cache.get_df(cache_key, cache.smart_valuation_ttl())
    if cached is not None:
        return cached
    try:
        df = ak.sw_index_first_info()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            cache.set_df(cache_key, df)
            return df
    except Exception:
        pass
    return None


def get_sw_industry_map() -> dict:
    """Build and cache a {stock_code: sw1_industry_name} reverse-lookup map.

    Iterates all 申万一级行业 (31 industries) using index_component_sw()
    and inverts the member lists.  Cached for 7 days — industry classification
    rarely changes.
    Returns {} on failure (callers should degrade gracefully).
    """
    cache_key = "sw_industry_map"
    cached = cache.get(cache_key, cache.TTL_FINANCIAL)
    if cached is not None:
        return cached
    try:
        sw1 = ak.sw_index_first_info()
        if sw1 is None or sw1.empty:
            return {}
        sw1.columns = [c.strip() for c in sw1.columns]
        result: dict = {}
        for _, row in sw1.iterrows():
            # 行业代码 is like '801010.SI' — strip suffix for index_component_sw
            raw_code = str(row.get("行业代码", ""))
            ind_name = str(row.get("行业名称", ""))
            numeric = raw_code.replace(".SI", "")
            if not numeric:
                continue
            try:
                members = ak.index_component_sw(symbol=numeric)
                if members is None or members.empty:
                    continue
                members.columns = [c.strip() for c in members.columns]
                code_col = next(
                    (c for c in members.columns if "代码" in c or c.lower() == "code"),
                    None,
                )
                if code_col is None:
                    continue
                for sc in members[code_col].dropna():
                    result[str(sc).zfill(6)] = ind_name
            except Exception:
                continue
        if result:
            cache.set(cache_key, result)
        return result
    except Exception:
        return {}


def get_index_constituents(index_code: str) -> list:
    """Fetch constituent stock codes for a CSI index (e.g. '000300', '000905', '000852').

    Uses akshare index_stock_cons_csindex.  Returns a list of 6-digit code strings.
    Cached for 24h — constituent changes happen quarterly but daily cache is fine.
    Returns [] on failure.
    """
    cache_key = f"index_cons_{index_code}"
    cached = cache.get(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        df = ak.index_stock_cons_csindex(symbol=index_code)
        if df is None or df.empty:
            return []
        df.columns = [c.strip() for c in df.columns]
        code_col = next(
            (c for c in df.columns if "成分券代码" in c),
            next((c for c in df.columns if c == "代码"), None),
        )
        if code_col is None:
            return []
        codes = [str(c).zfill(6) for c in df[code_col].dropna().tolist()]
        cache.set(cache_key, codes)
        return codes
    except Exception:
        return []


def search_stock_by_name(name: str) -> Optional[str]:
    """Fuzzy-search a stock by Chinese name; return its 6-digit code."""
    try:
        df = _get_spot_df()
        matched = df[df["名称"].str.contains(name, na=False)]
        if matched.empty:
            return None
        return str(matched.iloc[0]["代码"])
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Trading calendar + suspension
# ---------------------------------------------------------------------------

_CALENDAR_START_YEARS = 3   # fetch this many years back + 1 year ahead


def get_trade_calendar() -> list:
    """Return sorted list of A-share trading date strings ('YYYY-MM-DD').

    Covers 3 years back to 1 year ahead — enough for any rolling window.

    Source priority:
      1. BaoStock  query_trade_dates
      2. AKShare   tool_trade_date_hist_sina (all historical trading days; trimmed to window)

    Cached for 30 days (holiday schedule rarely changes mid-year).
    """
    cache_key = "trade_calendar"
    cached = cache.get(cache_key, 30 * 86400)
    if cached is not None:
        return cached

    start = (datetime.now() - timedelta(days=_CALENDAR_START_YEARS * 365)).strftime("%Y-%m-%d")
    end   = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    # ── Source 1: BaoStock ────────────────────────────────────────────────
    try:
        bs = _get_baostock()
        if bs is not None:
            _cal_rows: list = []

            def _do_cal_query():
                if not _bs_lock.acquire(timeout=65.0):
                    return []
                try:
                    rs = bs.query_trade_dates(start_date=start, end_date=end)
                    result = []
                    while rs.error_code == "0" and rs.next():
                        row = rs.get_row_data()
                        # row = [calendar_date, is_trading_day]
                        if len(row) >= 2 and row[1] == "1":
                            result.append(row[0])  # 'YYYY-MM-DD'
                    return result
                except OSError:
                    _reset_baostock()
                    return []
                finally:
                    _bs_lock.release()

            result = _call_with_timeout(_do_cal_query, timeout=60.0)
            if result is None:
                _reset_baostock()
            elif result:
                dates = sorted(result)
                cache.set(cache_key, dates)
                return dates
    except Exception:
        pass

    # ── Source 2: AKShare Sina ────────────────────────────────────────────
    try:
        df = ak.tool_trade_date_hist_sina()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            col = df.columns[0]
            dates = sorted(
                str(d)[:10] for d in df[col].dropna()
                if start <= str(d)[:10] <= end
            )
            if dates:
                cache.set(cache_key, dates)
                return dates
    except Exception:
        pass

    return []


def is_trading_day(date=None) -> bool:
    """Return True if *date* is an A-share trading day.

    *date* can be a datetime, date, or 'YYYY-MM-DD' string.
    Defaults to today if omitted.
    Falls back to weekday check (Mon-Fri) when calendar is unavailable.
    """
    if date is None:
        date = datetime.now()
    if hasattr(date, "strftime"):
        date_str = date.strftime("%Y-%m-%d")
    else:
        date_str = str(date)[:10]

    cal = get_trade_calendar()
    if cal:
        return date_str in cal
    # Fallback: treat weekdays as trading days
    from datetime import date as _date
    d = _date.fromisoformat(date_str)
    return d.weekday() < 5


def get_suspension_list(trade_date: str = None) -> pd.DataFrame:
    """Return stocks suspended or resuming on *trade_date* ('YYYYMMDD').

    Defaults to today.  Returned DataFrame columns:
      code          — 6-digit stock code
      trade_date    — 'YYYYMMDD'
      suspend_type  — 'S' (停牌) | 'R' (复牌)

    Source: Tushare suspend_d (120pts, confirmed working).
    Cached for 24h per date.
    """
    if trade_date is None:
        trade_date = datetime.now().strftime("%Y%m%d")

    cache_key = f"suspension_{trade_date}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached

    try:
        pro = _get_tushare_pro()
        if pro is not None:
            df = pro.suspend_d(
                trade_date=trade_date,
                fields="ts_code,trade_date,suspend_type",
            )
            if df is not None and not df.empty:
                # Convert ts_code '000001.SZ' → '000001'
                df["code"] = df["ts_code"].str[:6]
                df = df[["code", "trade_date", "suspend_type"]].reset_index(drop=True)
                cache.set_df(cache_key, df)
                return df
    except Exception:
        pass

    return pd.DataFrame(columns=["code", "trade_date", "suspend_type"])


def get_suspended_codes(trade_date: str = None) -> set:
    """Convenience wrapper: return set of codes currently suspended (type='S')."""
    df = get_suspension_list(trade_date)
    if df.empty:
        return set()
    return set(df.loc[df["suspend_type"] == "S", "code"].tolist())
