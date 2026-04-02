"""
Data fetching module — wraps akshare with error handling and caching.
"""

import threading

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import cache

_spot_lock = threading.Lock()
_spot_em_failed = False  # once set True, skip all subsequent stock_zh_a_spot_em calls


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
    """Infer exchange from code prefix: 6xx -> sh, others -> sz."""
    return "sh" if code.startswith("6") else "sz"


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
    global _spot_em_failed
    if _spot_em_failed:
        return pd.DataFrame()
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
        df = _call_with_timeout(ak.stock_zh_a_spot_em, 20)
        if df is None or df.empty:
            _spot_em_failed = True  # don't retry; prevents concurrent V8 re-init crash
            return pd.DataFrame()
        cache.set("spot_all", df.to_dict("records"))
        return df


def get_realtime_quote(code: str) -> Optional[dict]:
    """Return real-time quote fields for a single stock code."""
    try:
        df = _get_spot_df()
        row = df[df["代码"] == code]
        if row.empty:
            return None
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
            # Extended fields (present in East Money spot data)
            "volume_ratio":  float(r.get("量比", 0) or 0),
            "div_yield":     float(r.get("股息率-TTM", 0) or 0),
            "return_5d":     float(r.get("5日涨跌幅", 0) or 0),
            "return_10d":    float(r.get("10日涨跌幅", 0) or 0),
            "return_20d":    float(r.get("20日涨跌幅", 0) or 0),
        }
    except Exception as e:
        return {"error": f"Failed to fetch quote: {e}"}


def get_stock_info(code: str) -> Optional[dict]:
    """Fetch stock meta: industry, listing date, share counts."""
    try:
        df = ak.stock_individual_info_em(symbol=code)
        info = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        return {
            "industry": str(info.get("行业", "Unknown")),
            "listing_date": str(info.get("上市时间", "")),
            "total_shares": str(info.get("总股本", "")),
            "circulating_shares": str(info.get("流通股", "")),
        }
    except Exception:
        return {}


_PRICE_FETCH_DAYS = 550  # Always fetch this many days so all rolling periods share one cache entry

def get_price_history(code: str, days: int = 365) -> Optional[pd.DataFrame]:
    """Fetch daily OHLCV history (qfq adjusted). Cached for 1 hour.

    Always fetches _PRICE_FETCH_DAYS (550d) so that rolling-period IC tests
    with different `days` values (400, 420, 440…) all share a single cache
    entry per stock, eliminating redundant API calls.

    Primary source: East Money (stock_zh_a_hist).
    Fallback: stock_zh_a_daily (163/Netease source) when primary is unavailable.
    """
    fetch_days = max(days, _PRICE_FETCH_DAYS)
    cache_key = f"price_{code}_{fetch_days}"
    cached = cache.get_df(cache_key, cache.TTL_PRICE_HISTORY)
    if cached is not None:
        # Caller may ask for fewer rows — slice to requested window
        if len(cached) > days:
            return cached.tail(days).reset_index(drop=True)
        return cached
    try:
        end = datetime.now()
        start = end - timedelta(days=fetch_days)
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
        if len(df) > days:
            return df.tail(days).reset_index(drop=True)
        return df
    except Exception:
        pass

    # Fallback: 163/Netease source via stock_zh_a_daily
    try:
        prefix = "sh" if code.startswith("6") else "sz"
        df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        # Trim to requested window
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df = df[df["date"] >= cutoff].reset_index(drop=True)
        if df.empty:
            return None
        # Compute change_pct and change_amt from close if not present
        if "change_pct" not in df.columns:
            df["change_pct"] = df["close"].pct_change() * 100
        if "change_amt" not in df.columns:
            df["change_amt"] = df["close"].diff()
        # turnover from stock_zh_a_daily is a decimal ratio; convert to percentage
        if "turnover" in df.columns:
            df["turnover"] = df["turnover"] * 100
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_valuation_history(code: str) -> Optional[pd.DataFrame]:
    """Fetch historical PE/PB valuation data. Cached for 24 hours."""
    cache_key = f"valuation_{code}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        df = ak.stock_a_lg_indicator(symbol=code)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={
            "trade_date": "date", "pe": "pe", "pe_ttm": "pe_ttm",
            "pb": "pb", "ps": "ps", "ps_ttm": "ps_ttm",
            "dv_ratio": "div_yield", "dv_ttm": "div_yield_ttm",
            "total_mv": "market_cap",
        })
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_financial_indicators(code: str) -> Optional[pd.DataFrame]:
    """Fetch financial indicators (ROE, margins, growth rates). Cached for 7 days."""
    cache_key = f"financial_{code}"
    cached = cache.get_df(cache_key, cache.TTL_FINANCIAL)
    if cached is not None:
        return cached
    try:
        df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2020")
        if df is None or df.empty:
            return None
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
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


# ---------------------------------------------------------------------------
# Group B: Additional per-stock data (optional; add latency)
# ---------------------------------------------------------------------------

def get_shareholder_count(code: str) -> Optional[pd.DataFrame]:
    """Fetch quarterly shareholder count history. Cached for 7 days."""
    cache_key = f"gdhs_{code}"
    cached = cache.get_df(cache_key, cache.TTL_FINANCIAL)
    if cached is not None:
        return cached
    try:
        df = _call_with_timeout(ak.stock_zh_a_gdhs, 20, symbol=code)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


def get_lhb_flow(code: str, days: int = 90) -> Optional[pd.DataFrame]:
    """Fetch Dragon-Tiger list (龙虎榜) net flows for the past N days. Cached for 24h."""
    cache_key = f"lhb_{code}_{days}"
    cached = cache.get_df(cache_key, cache.TTL_VALUATION)
    if cached is not None:
        return cached
    try:
        end   = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        df = _call_with_timeout(ak.stock_lhb_detail_em, 20, symbol=code, start_date=start, end_date=end)
        if df is None or df.empty:
            return None
        df.columns = [c.strip() for c in df.columns]
        df = df.reset_index(drop=True)
        cache.set_df(cache_key, df)
        return df
    except Exception:
        return None


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


def get_social_heat(code: str) -> Optional[dict]:
    """
    Fetch East Money stock discussion heat.
    Returns dict with rank and recent post metrics, or None.
    Cached for 2 hours.
    """
    cache_key = f"social_heat_{code}"
    cached = cache.get(cache_key, cache.TTL_PRICE_HISTORY * 2)
    if cached is not None:
        return cached
    try:
        # Try East Money hot rank
        df = _call_with_timeout(ak.stock_hot_rank_em, 20)
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            # Find code column
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
