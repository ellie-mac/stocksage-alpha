#!/usr/bin/env python3
"""
筹码策略扫描：收盘价 > 95% 筹码成本（获利盘比例 >= min_win%）

数据源：Tushare Pro
  - cyq_perf(trade_date)   → 全市场筹码分布（winner_rate / cost_95pct）
  - daily(trade_date)      → 收盘价 / 涨跌幅 / 成交额

股票名称 / 行业：独立持久文件 data/stock_names.json（自动更新，超 7 天则刷新）
  优先 Tushare stock_basic（含行业），受限时 akshare spot 兜底（仅名称）

每日 chip 数据缓存：scripts/.cache/chip_data_YYYYMMDD.json
  当天第二次运行直接读缓存，不重复拉 Tushare

用法：
    python -X utf8 scripts/chip_strategy.py               # 自动取最近交易日
    python -X utf8 scripts/chip_strategy.py --date 20260416
    python -X utf8 scripts/chip_strategy.py --min-win 90  # 放宽至 90%
    python -X utf8 scripts/chip_strategy.py --dry-run     # 仅打印，不推送
    python -X utf8 scripts/chip_strategy.py --refresh-names  # 强制刷新名称缓存
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
DATA    = ROOT / "data"
sys.path.insert(0, str(SCRIPTS))

import cache as _cache
from common import send_wechat, configure_pushplus

_NAMES_FILE = DATA / "stock_names.json"   # persistent: {ts_code: {name, industry}}
_NAMES_TTL  = 7 * 24 * 3600               # refresh weekly


# ---------------------------------------------------------------------------
# Tushare init
# ---------------------------------------------------------------------------

def _get_pro():
    try:
        import tushare as ts
        cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        token = cfg.get("tushare", {}).get("token", "")
        if not token:
            raise RuntimeError("alert_config.json 未配置 tushare.token")
        ts.set_token(token)
        return ts.pro_api()
    except Exception as e:
        print(f"[ERROR] Tushare 初始化失败: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Latest trade date
# ---------------------------------------------------------------------------

def _latest_trade_date() -> str:
    now = datetime.now()
    d   = now.date()
    if now.hour < 15 or (now.hour == 15 and now.minute < 30) or d.weekday() >= 5:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Stock names — persistent JSON file, refreshed weekly
# ---------------------------------------------------------------------------

def _names_stale(force: bool = False) -> bool:
    if force or not _NAMES_FILE.exists():
        return True
    age = time.time() - _NAMES_FILE.stat().st_mtime
    return age > _NAMES_TTL


def load_names(force: bool = False) -> dict[str, dict]:
    """
    Return {ts_code: {name, industry}}.
    Loads from data/stock_names.json; refreshes if stale.
    """
    if not _names_stale(force):
        try:
            data = json.loads(_NAMES_FILE.read_text(encoding="utf-8"))
            if data:
                print(f"[names] 读取缓存 {len(data)} 条")
                return data
        except Exception:
            pass

    print("[names] 刷新股票名称 / 行业...")
    pro = _get_pro()
    names: dict[str, dict] = {}

    # 1. Try bak_basic (backup daily basic — not rate-limited like stock_basic)
    #    Returns ts_code, name, industry for all listed stocks on a given date.
    try:
        from datetime import date as _date
        latest = _date.today().strftime("%Y%m%d")
        df = pro.bak_basic(trade_date=latest, fields="ts_code,name,industry")
        if df is None or df.empty:
            # Try yesterday if today has no data yet
            yesterday = (_date.today() - timedelta(days=1)).strftime("%Y%m%d")
            df = pro.bak_basic(trade_date=yesterday, fields="ts_code,name,industry")
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                names[row["ts_code"]] = {
                    "name":     str(row.get("name", "") or ""),
                    "industry": str(row.get("industry", "") or ""),
                }
            print(f"[names] bak_basic: {len(names)} 条")
    except Exception as e:
        print(f"[names] bak_basic 失败: {e}，尝试 stock_basic...")

    # 2. Fallback to stock_basic (rate-limited to 1/hour, but has richer data)
    if len(names) < 100:
        try:
            df = pro.stock_basic(exchange="", list_status="L",
                                 fields="ts_code,name,industry")
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    names[row["ts_code"]] = {
                        "name":     str(row.get("name", "") or ""),
                        "industry": str(row.get("industry", "") or ""),
                    }
                print(f"[names] stock_basic: {len(names)} 条")
        except Exception as e2:
            print(f"[names] stock_basic 也失败: {e2}")

    if names:
        DATA.mkdir(parents=True, exist_ok=True)
        _NAMES_FILE.write_text(json.dumps(names, ensure_ascii=False, indent=None),
                               encoding="utf-8")
        print(f"[names] 已写入 {_NAMES_FILE}")
    else:
        print("[names] 无法获取名称数据，将显示空名称")

    return names


# ---------------------------------------------------------------------------
# Daily chip data — fetch + cache
# ---------------------------------------------------------------------------

_CHIP_TTL = 23 * 3600


def _chip_cache_key(trade_date: str) -> str:
    return f"chip_data_{trade_date}"


# ---------------------------------------------------------------------------
# 6-month high — fetch once, cache alongside chip data
# ---------------------------------------------------------------------------

def fetch_6m_high(ts_codes: list[str], trade_date: str, pro) -> dict[str, float]:
    """
    Return {ts_code: approximate_max_close_over_past_6_months}.

    Uses bi-weekly date sampling (13 all-market daily calls) rather than one
    call per stock, staying well under Tushare's 50-calls/min limit.
    Results are cached with the same 23-hour TTL as chip data.
    """
    from datetime import datetime, timedelta

    cache_key = f"6m_high_{trade_date}"
    cached: dict[str, float] = _cache.get(cache_key, _CHIP_TTL) or {}

    ts_set = set(ts_codes)
    missing = [ts for ts in ts_set if ts not in cached]
    if not missing:
        print(f"[6m_high cache] 命中 {len(ts_codes)} 条")
        return {ts: cached[ts] for ts in ts_codes if ts in cached}

    # Build ~13 bi-weekly sample dates spanning the past 6 months.
    # Slide backwards off weekends so we always try a weekday.
    td = datetime.strptime(trade_date, "%Y%m%d")
    sample_dates: list[str] = []
    for weeks_back in range(2, 27, 2):          # 2, 4, 6 … 26 weeks
        d = td - timedelta(weeks=weeks_back)
        while d.weekday() >= 5:                 # skip Sat/Sun
            d -= timedelta(days=1)
        sample_dates.append(d.strftime("%Y%m%d"))

    max_closes: dict[str, float] = {ts: cached.get(ts, 0.0) for ts in ts_set}

    print(f"[6m_high] 采样 {len(sample_dates)} 个日期，更新 {len(missing)} 只股票最高价...")
    for date in sample_dates:
        try:
            df = pro.daily(trade_date=date, fields="ts_code,close")
            if df is None or df.empty:
                continue
            sub = df[df["ts_code"].isin(ts_set) & df["close"].notna()]
            for row in sub.itertuples(index=False):
                if row.close > max_closes.get(row.ts_code, 0.0):
                    max_closes[row.ts_code] = float(row.close)
        except Exception as e:
            print(f"  [warn] date={date}: {e}")
        time.sleep(0.3)   # ~3 calls/sec ≪ 50/min limit

    result = {ts: v for ts, v in max_closes.items() if v > 0}
    cached.update(result)
    _cache.set(cache_key, cached)
    print(f"[6m_high] 完成，覆盖 {len(result)} 只，已缓存")
    return {ts: result[ts] for ts in ts_codes if ts in result}


def _load_chip_cache(trade_date: str) -> pd.DataFrame | None:
    raw = _cache.get(_chip_cache_key(trade_date), _CHIP_TTL)
    if raw is None:
        return None
    try:
        if isinstance(raw, dict) and raw.get("__type") == "dataframe":
            import io
            df = pd.read_json(io.StringIO(raw["records"]), orient="records")
        else:
            df = pd.DataFrame(raw)
        # Re-derive code from ts_code to restore leading zeros lost during JSON round-trip
        if "ts_code" in df.columns:
            df["code"] = df["ts_code"].str.split(".").str[0]
        return df
    except Exception:
        return None


def fetch_chip_data(trade_date: str, pro) -> pd.DataFrame:
    """
    Pull cyq_perf + daily for trade_date and cache.
    Returns DataFrame with columns:
        ts_code, trade_date, cost_5pct, cost_50pct, cost_85pct, cost_95pct,
        weight_avg, winner_rate, close, pct_chg, amount
    """
    cached = _load_chip_cache(trade_date)
    if cached is not None:
        print(f"[chip cache] 命中 chip_data_{trade_date}，共 {len(cached)} 条")
        return cached

    print(f"[fetch] trade_date={trade_date}")

    print("  拉取 cyq_perf ...")
    t0 = time.time()
    cyq = pro.cyq_perf(
        trade_date=trade_date,
        fields="ts_code,trade_date,cost_5pct,cost_50pct,cost_85pct,cost_95pct,weight_avg,winner_rate",
    )
    print(f"  cyq_perf: {len(cyq) if cyq is not None else 0} 条  ({time.time()-t0:.1f}s)")

    if cyq is None or cyq.empty:
        print(f"[WARN] cyq_perf 无数据 (trade_date={trade_date})，可能是非交易日或数据延迟")
        return pd.DataFrame()

    print("  拉取 daily (close/pct_chg/amount) ...")
    t0 = time.time()
    daily = pro.daily(trade_date=trade_date, fields="ts_code,close,pct_chg,vol,amount")
    print(f"  daily: {len(daily) if daily is not None else 0} 条  ({time.time()-t0:.1f}s)")

    df = cyq.copy()
    if daily is not None and not daily.empty:
        df = df.merge(daily[["ts_code", "close", "pct_chg", "amount"]],
                      on="ts_code", how="left")
    else:
        df["close"] = float("nan")
        df["pct_chg"] = float("nan")
        df["amount"]  = float("nan")

    for col in ("winner_rate", "cost_95pct", "close", "pct_chg"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 6-digit code for display
    df["code"] = df["ts_code"].str.split(".").str[0]

    _cache.set(_chip_cache_key(trade_date), df)
    print(f"[chip cache] 已写入 chip_data_{trade_date}")
    return df


# ---------------------------------------------------------------------------
# Technical indicators (BOLL / MACD) via historical price data
# ---------------------------------------------------------------------------

def _ema(series: list[float], period: int) -> list[float]:
    k = 2 / (period + 1)
    result = [series[0]]
    for v in series[1:]:
        result.append(result[-1] * (1 - k) + v * k)
    return result


def _compute_indicators(ts_code: str) -> dict | None:
    """
    Compute BOLL middle band and MACD histogram (current + previous bar).
    Uses fetcher.get_price_history (BaoStock/AKShare, cached, no Tushare quota).
    Returns {'boll_mid', 'macd_hist', 'macd_hist_prev'} or None on failure.
    """
    try:
        import fetcher as _fetcher
        hist = _fetcher.get_price_history(ts_code.split(".")[0], days=60)
        if hist is None or len(hist) < 28:
            return None
        closes = hist["close"].dropna().tolist()
        if len(closes) < 28:
            return None

        # BOLL middle = 20-day SMA
        boll_mid = sum(closes[-20:]) / 20

        # MACD(12, 26, 9) — need last two histogram values to detect convergence
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        macd_line = [a - b for a, b in zip(ema12[25:], ema26[25:])]
        signal    = _ema(macd_line, 9)
        hist_cur  = macd_line[-1] - signal[-1]
        hist_prev = macd_line[-2] - signal[-2]

        return {"boll_mid": boll_mid, "macd_hist": hist_cur, "macd_hist_prev": hist_prev}
    except Exception:
        return None


def add_indicators(df: pd.DataFrame, max_workers: int = 8) -> pd.DataFrame:
    """
    Fetch BOLL/MACD for every stock in df (after chip filter, typically <300 stocks).
    Adds columns: boll_mid, macd_hist.  Runs in parallel for speed.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    codes = df["ts_code"].tolist()
    results: dict[str, dict] = {}
    print(f"[indicators] 计算 {len(codes)} 只股票的 BOLL/MACD ...", flush=True)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut = {ex.submit(_compute_indicators, c): c for c in codes}
        for f in as_completed(fut):
            code = fut[f]
            val  = f.result()
            if val:
                results[code] = val
    df = df.copy()
    df["boll_mid"]  = df["ts_code"].map(lambda c: results.get(c, {}).get("boll_mid"))
    df["macd_hist"] = df["ts_code"].map(lambda c: results.get(c, {}).get("macd_hist"))
    print(f"[indicators] 完成，命中 {len(results)}/{len(codes)} 只")
    return df


# ---------------------------------------------------------------------------
# Screening
# ---------------------------------------------------------------------------

def screen(
    df: pd.DataFrame,
    min_win: float = 90.0,
    max_win: float | None = None,
    max_today_pct: float | None = 5.0,
    max_6m_ratio: float | None = 0.9,
    six_month_high: dict[str, float] | None = None,
    max_price: float | None = None,
    exclude_kcb: bool = False,
    boll_near_mid: bool = False,
    macd_converging: bool = False,
) -> pd.DataFrame:
    """
    boll_near_mid    : True → 收盘价在 BOLL中轨 ±8% 范围内（不过于偏离中轨）
    macd_converging  : True → MACD柱绝对值在缩小（当前柱 < 上一根，往零靠近）
    df 需要预先调用 add_indicators(df) 才能使用这两个参数。
    """
    """
    Filter stocks from chip data.

    Parameters
    ----------
    min_win : float
        Minimum winner_rate.  Default 90%.
    max_today_pct : float | None
        Cap on today's pct_chg.  Excludes limit-up / hot-momentum stocks.
        None = disabled.
    max_6m_ratio : float | None
        Maximum (close / 6m_max_close).  Stocks where current price is within
        this ratio of the 6-month high are considered at a high position and
        excluded.  E.g. 0.9 = exclude stocks within 10% of their 6m high.
        None = disabled.  Requires six_month_high dict.
    six_month_high : dict | None
        {ts_code: max_close_6m} — provided by fetch_6m_high().
    max_price : float | None
        Exclude stocks with close > max_price.  None = disabled.
    exclude_kcb : bool
        If True, exclude STAR Market stocks (ts_code starting with "688").
    """
    if df.empty:
        return df

    mask = df["winner_rate"].notna() & (df["winner_rate"] >= min_win)
    if max_win is not None:
        mask &= df["winner_rate"] < max_win
    result = df[mask].copy()

    # Exclude ST/退
    if "name" in result.columns:
        result = result[~result["name"].str.contains(r"ST|退", na=False)]

    # Exclude 科创板 (688xxx.SH)
    if exclude_kcb:
        kcb = result["ts_code"].str.startswith("688")
        n_kcb = kcb.sum()
        result = result[~kcb]
        if n_kcb:
            print(f"[screen] 剔除 {n_kcb} 只科创板股票")

    # Cross-check: close should be >= cost_95pct (allow 1% rounding slack)
    inconsistent = (
        result["close"].notna() &
        result["cost_95pct"].notna() &
        (result["close"] < result["cost_95pct"] * 0.99)
    )
    if inconsistent.sum() > 0:
        print(f"[warn] 剔除 {inconsistent.sum()} 条 close < cost_95pct（数据不一致）")
        result = result[~inconsistent]

    before = len(result)

    # Filter 1: today's gain — exclude limit-up / continuous-rally stocks
    if max_today_pct is not None:
        hot = result["pct_chg"].notna() & (result["pct_chg"] > max_today_pct)
        n_hot = hot.sum()
        result = result[~hot]
        if n_hot:
            print(f"[screen] pct_chg>{max_today_pct:.1f}% 剔除 {n_hot} 只（今日涨幅过大）")

    # Filter 2: 6-month position — exclude stocks near their 6-month high
    if max_6m_ratio is not None and six_month_high:
        result["_6m_high"] = result["ts_code"].map(six_month_high)
        result["_6m_ratio"] = result["close"] / result["_6m_high"]
        near_high = (
            result["_6m_ratio"].notna() &
            (result["_6m_ratio"] >= max_6m_ratio)
        )
        n_high = near_high.sum()
        result = result[~near_high]
        result = result.drop(columns=["_6m_high", "_6m_ratio"], errors="ignore")
        if n_high:
            print(f"[screen] close/6m_high>={max_6m_ratio:.2f} 剔除 {n_high} 只（当前价处于半年高位）")

    # Filter 3: price cap
    if max_price is not None:
        expensive = result["close"].notna() & (result["close"] > max_price)
        n_exp = expensive.sum()
        result = result[~expensive]
        if n_exp:
            print(f"[screen] close>{max_price:.0f} 剔除 {n_exp} 只（股价偏高）")

    # Filter 4: BOLL中轨附近（需 add_indicators 预先调用）
    if boll_near_mid and "boll_mid" in result.columns:
        valid = result["boll_mid"].notna() & result["close"].notna()
        ratio = (result["close"] - result["boll_mid"]).abs() / result["boll_mid"]
        far = valid & (ratio > 0.08)   # 偏离中轨超过 8% 则剔除
        n_far = far.sum()
        result = result[~far]
        if n_far:
            print(f"[screen] 偏离BOLL中轨>8% 剔除 {n_far} 只")

    # Filter 5: MACD绿柱收敛（需 add_indicators 预先调用）
    # 红柱（hist>=0）无条件保留；绿柱（hist<0）只保留在缩小（往零靠近）的
    if macd_converging and "macd_hist" in result.columns and "macd_hist_prev" in result.columns:
        h     = result["macd_hist"]
        h_pre = result["macd_hist_prev"]
        valid = h.notna() & h_pre.notna()
        green_expanding = valid & (h < 0) & (h.abs() >= h_pre.abs())
        n_exp = green_expanding.sum()
        result = result[~green_expanding]
        if n_exp:
            print(f"[screen] 绿柱扩张 剔除 {n_exp} 只")

    after = len(result)
    print(f"[screen] 过滤后: {before} → {after} 只")

    return result.sort_values("winner_rate", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Format push message
# ---------------------------------------------------------------------------

def format_message(
    result: pd.DataFrame,
    trade_date: str,
    min_win: float,
    max_win: float | None = None,
    max_today_pct: float | None = None,
    max_6m_ratio: float | None = None,
    max_price: float | None = None,
    exclude_kcb: bool = False,
) -> tuple[str, str]:
    n = len(result)
    date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    win_range = f"{min_win:.0f}-{max_win:.0f}%" if max_win else f"≥{min_win:.0f}%"
    title = f"筹码策略 {date_fmt} | 获利盘{win_range}  共{n}只"

    if n == 0:
        return title, f"**{date_fmt}** 无符合条件股票（获利盘 {win_range}）"

    filter_parts = [f"获利盘 **{win_range}**"]
    if max_6m_ratio is not None:
        pct = int((1 - max_6m_ratio) * 100)
        filter_parts.append(f"距半年高点 ≥ **{pct}%**（排除高位）")
    if max_today_pct is not None:
        filter_parts.append(f"今日涨幅 ≤ **{max_today_pct:.0f}%**（排除连续急涨）")
    if max_price is not None:
        filter_parts.append(f"股价 ≤ **{max_price:.0f}**（排除高价股）")
    if exclude_kcb:
        filter_parts.append("**排除科创板**")

    lines = [
        f"## 筹码策略 {date_fmt}",
        "> " + "  |  ".join(filter_parts),
        f"> 共 **{n}** 只",
        "",
        "| 代码 | 名称 | 行业 | 收盘 | 涨跌% | 获利盘% | 95%成本 | 均成本 |",
        "|------|------|------|-----:|------:|--------:|--------:|-------:|",
    ]

    for _, row in result.iterrows():
        code    = row.get("code", "")
        name    = str(row.get("name", "") or "")[:8]
        ind     = str(row.get("industry", "") or "")[:6]
        close   = row.get("close",       float("nan"))
        pct_chg = row.get("pct_chg",     float("nan"))
        win     = row.get("winner_rate",  float("nan"))
        c95     = row.get("cost_95pct",   float("nan"))
        wavg    = row.get("weight_avg",   float("nan"))

        close_s = f"{close:.2f}"       if pd.notna(close)  else "-"
        pct_s   = f"{pct_chg:+.2f}%"  if pd.notna(pct_chg) else "-"
        win_s   = f"{win:.1f}%"        if pd.notna(win)    else "-"
        c95_s   = f"{c95:.2f}"         if pd.notna(c95)    else "-"
        wavg_s  = f"{wavg:.2f}"        if pd.notna(wavg)   else "-"

        lines.append(
            f"| {code} | {name} | {ind} | {close_s} | {pct_s} | {win_s} | {c95_s} | {wavg_s} |"
        )

    lines += ["", f"_数据: Tushare Pro · {datetime.now():%Y-%m-%d %H:%M}_"]
    return title, "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",          type=str,   default="")
    parser.add_argument("--min-win",       type=float, default=90.0,
                        help="获利盘最低比例%%，默认90")
    parser.add_argument("--max-win",       type=float, default=0.0,
                        help="获利盘上限%%（用于分层推送），0=不限")
    parser.add_argument("--max-today-pct", type=float, default=5.0,
                        help="今日涨幅上限%%，默认5.0；0=关闭")
    parser.add_argument("--max-6m-ratio",  type=float, default=0.9,
                        help="最大(收盘/半年最高)比值，默认0.9（距高点≥10%%）；0=关闭")
    parser.add_argument("--max-price",     type=float, default=0.0,
                        help="股价上限，默认0=不限；50=剔除50元以上高价股")
    parser.add_argument("--no-kcb",        action="store_true",
                        help="剔除科创板（688开头）")
    parser.add_argument("--boll-near",     action="store_true",
                        help="只保留收盘价在BOLL中轨±8%%范围内的股票")
    parser.add_argument("--macd-conv",     action="store_true",
                        help="只保留MACD柱正在收敛（绝对值缩小，往零靠近）的股票")
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--refresh-names", action="store_true", help="强制刷新名称缓存")
    args = parser.parse_args()

    max_today_pct = args.max_today_pct if args.max_today_pct > 0 else None
    max_6m_ratio  = args.max_6m_ratio  if args.max_6m_ratio  > 0 else None
    max_win       = args.max_win       if args.max_win       > 0 else None
    max_price     = args.max_price     if args.max_price     > 0 else None
    exclude_kcb     = args.no_kcb
    boll_near_mid   = args.boll_near
    macd_converging = args.macd_conv

    trade_date = args.date or _latest_trade_date()
    win_range = f"{args.min_win:.0f}-{max_win:.0f}%" if max_win else f"≥{args.min_win:.0f}%"
    print(f"[chip] trade_date={trade_date}  win={win_range}  "
          f"max_today_pct={max_today_pct}  max_6m_ratio={max_6m_ratio}  "
          f"max_price={max_price}  exclude_kcb={exclude_kcb}")

    cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    pro    = _get_pro()
    df     = fetch_chip_data(trade_date, pro)

    if df.empty:
        print("[chip] 无数据，退出")
        return

    # Load names (refreshes from Tushare/akshare if stale)
    names  = load_names(force=args.refresh_names)

    # Merge names into df
    if names:
        df["name"]     = df["ts_code"].map(lambda c: names.get(c, {}).get("name", ""))
        df["industry"] = df["ts_code"].map(lambda c: names.get(c, {}).get("industry", ""))
    else:
        df["name"]     = ""
        df["industry"] = ""

    # Step 1: cheap filters (no extra API calls)
    result = screen(df, args.min_win, max_win=max_win, max_today_pct=max_today_pct,
                    max_6m_ratio=None, six_month_high=None,
                    max_price=max_price, exclude_kcb=exclude_kcb)

    # Step 2: 6-month high filter (fetches per-stock history for survivors only)
    six_month_high: dict[str, float] = {}
    if max_6m_ratio is not None and not result.empty:
        six_month_high = fetch_6m_high(result["ts_code"].tolist(), trade_date, pro)
        result = screen(df, args.min_win, max_win=max_win, max_today_pct=max_today_pct,
                        max_6m_ratio=max_6m_ratio, six_month_high=six_month_high,
                        max_price=max_price, exclude_kcb=exclude_kcb)

    # Step 3: BOLL / MACD filter (fetches 60d history for survivors only)
    if (boll_near_mid or macd_converging) and not result.empty:
        result = add_indicators(result)
        result = screen(result, args.min_win, max_win=max_win, max_today_pct=None,
                        max_6m_ratio=None, six_month_high=None,
                        max_price=None, exclude_kcb=False,
                        boll_near_mid=boll_near_mid, macd_converging=macd_converging)

    print(f"[chip] 最终结果: {len(result)} 只")

    if not result.empty:
        cols = ["code", "name", "industry", "close", "pct_chg", "winner_rate", "cost_95pct"]
        print(result[cols].head(20).to_string(index=False))

    title, body = format_message(result, trade_date, args.min_win, max_win=max_win,
                                 max_today_pct=max_today_pct, max_6m_ratio=max_6m_ratio,
                                 max_price=max_price, exclude_kcb=exclude_kcb)
    send_wechat(title, body, sendkey, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
