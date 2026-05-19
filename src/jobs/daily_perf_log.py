#!/usr/bin/env python3
"""
每日统一胜率记录 — 收盘后一条微信
运行时间：16:00（市场收盘后）

包含策略：
  主策略    — latest_picks.json（前日 18:30 扫盘）
  小票策略  — latest_picks.json smallcap（同上）
  筹码策略  — CAH∩CAD∩CADM 三者共有 C0-C3（前日 20:30 扫描）
  金叉共振  — golden_cross_YYYYMMDD.json G0-G2（前日 19:30 扫描）
  热榜策略  — hot_scan_YYYYMMDD.json picks（前日 19:00 扫描）
  ETF策略   — etf_picks_YYYYMMDD.json（etf_strategy 有买入信号时写入）
  监控强买  — wl_strong_buy_log.json（watchlist 信号触发）

用法：
    python -X utf8 src/jobs/daily_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data"

MAIN_PICKS_PATH = DATA_DIR / "latest_picks.json"
SIG_PATH        = DATA_DIR / "signals_log.json"

MAIN_PERF_PATH  = DATA_DIR / "main_daily_perf.json"
SC_PERF_PATH    = DATA_DIR / "sc_daily_perf.json"
CHIP_PERF_PATH  = DATA_DIR / "chip_daily_perf.json"
GC_PERF_PATH    = DATA_DIR / "gc_daily_perf.json"
HOT_PERF_PATH   = DATA_DIR / "hot_daily_perf.json"
ETF_PERF_PATH   = DATA_DIR / "etf_daily_perf.json"
WL_MON_PERF_PATH = DATA_DIR / "wl_monitor_perf.json"
WL_MON_LOG_PATH  = DATA_DIR / "wl_strong_buy_log.json"

CHIP_TIERS = ["C0", "C1", "C2", "C3"]
GC_TIERS   = ["G0", "G1", "G2"]
HOT_TIERS  = ["H0", "H1"]


# ── 行情 ──────────────────────────────────────────────────────────────────────

def _fetch_market_data(
    codes: list[str],
) -> tuple[dict[str, float], dict[str, float], dict[str, dict]]:
    """
    Returns (close_pct, open_pct, raw_prices):
      close_pct   {code: pct}   涨跌幅 vs 昨收
      open_pct    {code: pct}   (今收 - 今开) / 今开 * 100
      raw_prices  {code: {"pc": prev_close, "o": open, "c": close}}
    """
    import sys
    import pandas as pd
    sys.path.insert(0, str(ROOT / "src"))
    from common import get_spot_em
    df = get_spot_em()
    if not df.empty:
        df["_code"] = df["代码"].astype(str).str.zfill(6)
        df = df[df["_code"].isin(codes)].copy()
        df["_pct"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
        for _col in ("今开", "open"):
            if _col in df.columns:
                df["_open"] = pd.to_numeric(df[_col], errors="coerce")
                break
        else:
            df["_open"] = float("nan")
        for _col in ("最新价", "现价", "close"):
            if _col in df.columns:
                df["_close"] = pd.to_numeric(df[_col], errors="coerce")
                break
        else:
            df["_close"] = float("nan")
        for _col in ("昨收", "pre_close"):
            if _col in df.columns:
                df["_pc"] = pd.to_numeric(df[_col], errors="coerce")
                break
        else:
            df["_pc"] = float("nan")
        mask = df["_open"].notna() & (df["_open"] > 0) & df["_close"].notna()
        df.loc[mask, "_open_pct"] = (
            (df.loc[mask, "_close"] - df.loc[mask, "_open"]) / df.loc[mask, "_open"] * 100
        ).round(2)
        close_pct = dict(zip(df["_code"], df["_pct"].dropna()))
        open_pct  = dict(zip(df.loc[mask, "_code"], df.loc[mask, "_open_pct"]))
        raw_prices: dict[str, dict] = {}
        for _, row in df.iterrows():
            code = str(row["_code"])
            pc = float(row["_pc"])   if pd.notna(row.get("_pc"))    and float(row["_pc"])    > 0 else None
            o  = float(row["_open"]) if pd.notna(row.get("_open"))  and float(row["_open"])  > 0 else None
            c  = float(row["_close"]) if pd.notna(row.get("_close")) and float(row["_close"]) > 0 else None
            if o and c:
                raw_prices[code] = {"pc": pc, "o": o, "c": c}
        if close_pct or open_pct:
            return close_pct, open_pct, raw_prices

    # fallback：tushare daily
    print("[daily_perf] spot_em 不可用，改用 tushare daily fallback", flush=True)
    try:
        import tushare as _ts
        from datetime import date as _date, timedelta as _td
        from common import load_alert_config
        _token = load_alert_config().get("tushare", {}).get("token", "")
        _ts.set_token(_token)
        _pro = _ts.pro_api()
        for _delta in range(3):
            _d = (_date.today() - _td(days=_delta)).strftime("%Y%m%d")
            _df = _pro.daily(trade_date=_d, fields="ts_code,pct_chg,open,close,pre_close")
            if _df is not None and not _df.empty:
                _df["_code"] = _df["ts_code"].str.split(".").str[0]
                _df = _df[_df["_code"].isin(codes)]
                close_pct = dict(zip(_df["_code"], _df["pct_chg"].astype(float)))
                import pandas as _pd
                open_pct: dict[str, float] = {}
                raw_prices = {}
                for _, row in _df.iterrows():
                    code = str(row["_code"])
                    op = _pd.to_numeric(row.get("open"),      errors="coerce")
                    cl = _pd.to_numeric(row.get("close"),     errors="coerce")
                    pc = _pd.to_numeric(row.get("pre_close"), errors="coerce")
                    if _pd.notna(op) and float(op) > 0 and _pd.notna(cl):
                        open_pct[code] = round((float(cl) - float(op)) / float(op) * 100, 2)
                        raw_prices[code] = {
                            "pc": float(pc) if _pd.notna(pc) and float(pc) > 0 else None,
                            "o":  float(op),
                            "c":  float(cl),
                        }
                if close_pct:
                    print(f"[daily_perf] tushare daily {_d} 拿到 {len(close_pct)} 只", flush=True)
                    return close_pct, open_pct, raw_prices
    except Exception as e:
        print(f"[daily_perf] tushare daily 失败: {e}", flush=True)
    return {}, {}, {}


# ── 统计工具 ──────────────────────────────────────────────────────────────────

_SIG_SHORT = {
    "MACD金叉": "MACD", "KDJ金叉": "KDJ", "RSI金叉": "RSI",
    "MA5/10金叉": "MA5/10", "MA10/20金叉": "MA10/20",
    "量能金叉": "量", "OBV金叉": "OBV", "布林中轨金叉": "布林",
}


def _stats(items: list[dict], prices: dict[str, float],
           open_prices: dict[str, float] | None = None,
           raw_prices: dict[str, dict] | None = None) -> dict:
    results = []
    for p in items:
        code = p["code"]
        if code not in prices:
            continue
        r = {"code": code, "name": p.get("name", code),
             "pct": prices[code], "signals": p.get("signals", [])}
        if open_prices and code in open_prices:
            r["open_pct"] = open_prices[code]
        if raw_prices and code in raw_prices:
            r["prices"] = raw_prices[code]
        for xk in ("winner_rate", "spread_pct", "breakdown"):
            if xk in p:
                r[xk] = p[xk]
        results.append(r)
    if not results:
        return {"n": 0, "win_rate": None, "avg_ret": None,
                "open_win_rate": None, "open_avg_ret": None, "top5": [], "results": []}
    vals = [r["pct"] for r in results]
    win_rate = round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1)
    avg_ret  = round(sum(vals) / len(vals), 2)
    top5     = sorted(results, key=lambda r: r["pct"], reverse=True)[:5]
    open_vals = [r["open_pct"] for r in results if "open_pct" in r]
    if open_vals:
        open_win_rate = round(sum(1 for v in open_vals if v > 0) / len(open_vals) * 100, 1)
        open_avg_ret  = round(sum(open_vals) / len(open_vals), 2)
    else:
        open_win_rate = None
        open_avg_ret  = None
    return {"n": len(results), "win_rate": win_rate, "avg_ret": avg_ret,
            "open_win_rate": open_win_rate, "open_avg_ret": open_avg_ret,
            "top5": top5, "results": results}


def _emoji(win_rate: float | None) -> str:
    if win_rate is None: return "⚪"
    if win_rate >= 60:   return "🟢"
    if win_rate >= 40:   return "🟡"
    return "🔴"

def _emoji_s(s: dict) -> str:
    """Emoji driven by open win rate when available, else close win rate."""
    return _emoji(s.get("open_win_rate") if s.get("open_win_rate") is not None else s.get("win_rate"))

def _wr(s: dict) -> str:
    return f"{s['win_rate']}%" if s["win_rate"] is not None else "-"

def _ar(s: dict) -> str:
    return f"{s['avg_ret']:+.2f}%" if s["avg_ret"] is not None else "-"

def _owr(s: dict) -> str:
    """Primary shown win rate: open-to-close if available, else close-to-close."""
    v = s.get("open_win_rate") if s.get("open_win_rate") is not None else s.get("win_rate")
    return f"{v}%" if v is not None else "-"

def _oar(s: dict) -> str:
    """Primary shown avg return: open-to-close if available, else close-to-close."""
    v = s.get("open_avg_ret") if s.get("open_avg_ret") is not None else s.get("avg_ret")
    return f"{v:+.2f}%" if v is not None else "-"

def _stock_line(r: dict) -> str:
    """Per-stock formatted line: open_pct as primary %, then price block."""
    pct_s = f"{r['open_pct']:+.2f}%" if "open_pct" in r else f"{r['pct']:+.2f}%"
    px = r.get("prices")
    if px:
        pc_s = f"{px['pc']:.2f}" if px.get("pc") else "?"
        price_s = f"  `昨{pc_s} 开{px['o']:.2f} 收{px['c']:.2f}`"
    else:
        price_s = ""
    return f"  {r['name']} {pct_s}{price_s}"


# ── 各策略数据加载 ─────────────────────────────────────────────────────────────

_EXCH_SUFFIX = {"sz": ".SZ", "sh": ".SH", "bj": ".BJ"}


def _norm_code(code: str) -> str:
    """'sz002183' / 'sh600158' / '000001.SZ' → '000001'."""
    s = str(code)
    if len(s) > 2 and s[:2].lower() in _EXCH_SUFFIX:
        return s[2:]
    return s.split(".")[0]


def _ts_code(code: str) -> str:
    """'sz002183' → '002183.SZ'; plain code → '' (suffix unknown)."""
    s = str(code)
    if len(s) > 2 and s[:2].lower() in _EXCH_SUFFIX:
        return s[2:] + _EXCH_SUFFIX[s[:2].lower()]
    return ""


_STOCK_NAMES: dict = {}


def _stock_names() -> dict:
    global _STOCK_NAMES
    if not _STOCK_NAMES:
        p = DATA_DIR / "stock_names.json"
        if p.exists():
            _STOCK_NAMES = json.loads(p.read_text(encoding="utf-8"))
    return _STOCK_NAMES


def _load_main(today: str) -> tuple[list[dict], list[dict]]:
    """返回 (main_picks, sc_picks)，分别是主策略和小票策略的选股列表。"""
    from datetime import datetime, timedelta
    cutoff = (datetime.strptime(today, "%Y%m%d") - timedelta(days=3)).strftime("%Y%m%d")
    if MAIN_PICKS_PATH.exists():
        raw = json.loads(MAIN_PICKS_PATH.read_text(encoding="utf-8"))
        ts  = raw.get("timestamp", "")
        ts_date = ts[:10].replace("-", "")
        if cutoff <= ts_date <= today:
            def _p(p): return {"code": str(p.get("code", "")).split(".")[0], "name": p.get("name", "")}
            m0 = [_p(p) for p in raw.get("results",  []) if p.get("code")]
            sc = [_p(p) for p in raw.get("smallcap", []) if p.get("code")]
            if m0 or sc:
                return m0, sc
    if SIG_PATH.exists():
        entries = json.loads(SIG_PATH.read_text(encoding="utf-8"))
        for entry in reversed(entries):
            entry_date = entry.get("date", "")
            if cutoff <= entry_date <= today:
                buys = entry.get("buy_signals", [])
                if buys:
                    return [{"code": str(p.get("code", "")).split(".")[0],
                             "name": p.get("name", "")} for p in buys], []
    return [], []


def _find_prev(glob_pat: str, today: str, days: int = 3) -> dict | None:
    from datetime import datetime as _dt, timedelta
    cutoff = (_dt.strptime(today, "%Y%m%d") - timedelta(days=days)).strftime("%Y%m%d")
    candidates = sorted(
        (p for p in DATA_DIR.glob(glob_pat) if cutoff <= p.stem[-8:] < today),
        key=lambda p: p.stem[-8:], reverse=True,
    )
    if not candidates:
        return None
    try:
        return json.loads(candidates[0].read_text(encoding="utf-8"))
    except Exception:
        return None


_CHIP_LEGACY = {"T1": "C0", "T2": "C1", "T3": "C2", "T4": "C3"}


def _chip_tier_picks(tiers_dict: dict, ct: str) -> list:
    """Read tier ct from tiers_dict; fall back to legacy T-key for old files."""
    picks = tiers_dict.get(ct)
    if picks is None:
        legacy = next((lk for lk, ck in _CHIP_LEGACY.items() if ck == ct), None)
        picks = tiers_dict.get(legacy, []) if legacy else []
    return picks


def _load_chip(today: str) -> dict[str, list[dict]]:
    """取可用筹码源的交集（cah/cad/cadm），有几个用几个；全无则返回空。"""
    empty = {t: [] for t in CHIP_TIERS}
    raw = {
        "cad":  _find_prev("chip_cad_????????.json",  today),
        "cadm": _find_prev("chip_cadm_????????.json", today),
        "cah":  _find_prev("chip_cah_????????.json",  today),
    }
    available = {k: v for k, v in raw.items() if v is not None}
    if not available:
        return empty
    base_key = next(k for k in ("cad", "cadm", "cah") if k in available)
    base = available[base_key]
    filter_sets = [
        {p["code"] for ct in CHIP_TIERS for p in _chip_tier_picks(src.get("tiers", {}), ct)}
        for k, src in available.items() if k != base_key
    ]
    return {
        ct: [p for p in _chip_tier_picks(base.get("tiers", {}), ct)
             if all(p["code"] in fs for fs in filter_sets)]
        for ct in CHIP_TIERS
    }


def _load_gc(today: str) -> dict[str, list[dict]]:
    """G0-G2：找前日带日期的扫描文件，按档返回"""
    gc = _find_prev("golden_cross_????????.json", today, days=7)
    if not gc:
        return {t: [] for t in GC_TIERS}
    tiers = gc.get("tiers", {})
    names = _stock_names()
    def _norm(p):
        orig = str(p.get("code", ""))
        normed = _norm_code(orig)
        name = p.get("name", "")
        if not name or name == orig:
            ts = _ts_code(orig)
            name = names.get(ts, {}).get("name", "") or normed
        return {**p, "code": normed, "name": name}
    return {t: [_norm(p) for p in tiers.get(t, [])] for t in GC_TIERS}


def _load_wl_monitor(today: str) -> list[dict]:
    """读取今日 watchlist_monitor 强买信号（去重，同一只取最高分那条）。"""
    if not WL_MON_LOG_PATH.exists():
        return []
    try:
        records = json.loads(WL_MON_LOG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    today_recs = [r for r in records if r.get("date") == today]
    best: dict[str, dict] = {}
    for r in today_recs:
        code = str(r.get("code", "")).zfill(6)
        if code not in best or r.get("buy_score", 0) > best[code].get("buy_score", 0):
            best[code] = {**r, "code": code}
    return list(best.values())


def _load_etf(today: str) -> list[dict]:
    raw = _find_prev("etf_picks_????????.json", today, days=3)
    if not raw:
        return []
    return [{"code": str(p["code"]), "name": p.get("name", p["code"])}
            for p in raw.get("picks", []) if p.get("code")]


def _load_hot(today: str) -> dict[str, list[dict]]:
    """热榜策略：H0=热度top5%，H1=全部picks"""
    raw = _find_prev("hot_scan_????????.json", today, days=3)
    if not raw:
        return {t: [] for t in HOT_TIERS}
    picks = [{"code": str(p["code"]).zfill(6), "name": p.get("name", p["code"]),
               "rank_pct": p.get("rank_pct", 100),
               "breakdown": p.get("breakdown", [])}
              for p in raw.get("picks", []) if p.get("code")]
    return {"H0": picks[:5], "H1": picks}


# ── 推送格式 ──────────────────────────────────────────────────────────────────

def _fmt_section(header: str, rows: list[str]) -> str:
    """用行尾两空格强制 markdown 换行，sections 之间用 \\n\\n 隔开。"""
    return "  \n".join([header] + rows)


# ── 持久化 ────────────────────────────────────────────────────────────────────

def _append(path: Path, record: dict, today: str, force: bool) -> bool:
    existing: list[dict] = []
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
    if any(r["date"] == today for r in existing):
        if force:
            existing = [r for r in existing if r["date"] != today]
        else:
            return False
    existing.append(record)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return True


# ── 主程序 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force:
        hm = now.hour * 60 + now.minute
        if hm < 15 * 60 + 55:
            print(f"[daily_perf] 当前 {now:%H:%M}，需 15:55 后运行，跳过")
            return

    today    = now.strftime("%Y%m%d")
    date_fmt = f"{today[4:6]}/{today[6:]}"

    # ── 加载各策略选股 ────────────────────────────────────────────────────────
    main_picks, sc_picks = _load_main(today)
    chip_by_tier  = _load_chip(today)
    gc_by_tier    = _load_gc(today)
    hot_by_tier   = _load_hot(today)
    wl_mon_picks  = _load_wl_monitor(today)
    etf_picks     = _load_etf(today)

    chip_flat = [p for picks in chip_by_tier.values() for p in picks]
    gc_flat   = [p for picks in gc_by_tier.values()   for p in picks]
    hot_flat  = hot_by_tier["H1"]

    print(f"[daily_perf] 主策略 {len(main_picks)}只 / 小票 {len(sc_picks)}只 / 筹码 {len(chip_flat)}只 / 金叉 {len(gc_flat)}只 / 热榜H0:{len(hot_by_tier['H0'])}只/H1:{len(hot_flat)}只 / 监控强买:{len(wl_mon_picks)}只 / ETF:{len(etf_picks)}只")

    if not main_picks and not sc_picks and not chip_flat and not gc_flat and not hot_flat and not wl_mon_picks and not etf_picks:
        print("[daily_perf] 所有策略均无数据，退出")
        return

    # ── 一次性拉取所有行情 ────────────────────────────────────────────────────
    all_codes = list({p["code"] for p in main_picks + sc_picks + chip_flat + gc_flat + hot_flat + wl_mon_picks + etf_picks})
    print(f"[daily_perf] 获取 {len(all_codes)} 只行情 ...")
    prices, open_prices, raw_prices = _fetch_market_data(all_codes)
    print(f"[daily_perf] 获取到 {len(prices)} 只（今开 {len(open_prices)} 只）")
    if not prices:
        print("[daily_perf] spot_em 返回空，跳过写入历史")
        return

    # ── 统计 ──────────────────────────────────────────────────────────────────
    ms  = _stats(main_picks, prices, open_prices, raw_prices)
    scs = _stats(sc_picks,   prices, open_prices, raw_prices)
    chip_tier_stats = {t: _stats(chip_by_tier[t], prices, open_prices, raw_prices) for t in CHIP_TIERS}
    gc_tier_stats   = {t: _stats(gc_by_tier[t],   prices, open_prices, raw_prices) for t in GC_TIERS}
    hot_tier_stats  = {t: _stats(hot_by_tier[t],  prices, open_prices, raw_prices) for t in HOT_TIERS}
    wl_mon_stats    = _stats(wl_mon_picks, prices, open_prices, raw_prices)
    etf_stats       = _stats(etf_picks,   prices, open_prices, raw_prices)
    cs = _stats(chip_flat, prices, open_prices, raw_prices)
    gs = _stats(gc_flat,   prices, open_prices, raw_prices)
    hs = hot_tier_stats["H1"]

    def _sort_key(r): return r.get("open_pct", r["pct"])

    for lbl, s in [("主策略", ms), ("小票", scs), ("筹码", cs), ("金叉", gs), ("热榜H1", hs), ("监控强买", wl_mon_stats), ("ETF", etf_stats)]:
        if s["n"] > 0:
            print(f"  [{lbl}] {s['n']}只  今开胜率{_owr(s)}  均{_oar(s)}")

    # ── 构建推送 ──────────────────────────────────────────────────────────────
    sections: list[str] = []

    # 主策略
    if ms["results"]:
        rows = [f"{_emoji_s(ms)} **主策略 {ms['n']}只  胜率{_owr(ms)}  均{_oar(ms)}**"]
        for r in sorted(ms["results"], key=_sort_key, reverse=True):
            rows.append(_stock_line(r))
        sections.append("  \n".join(rows))

    # 小票策略
    if scs["results"]:
        rows = [f"{_emoji_s(scs)} **小票策略 {scs['n']}只  胜率{_owr(scs)}  均{_oar(scs)}**"]
        for r in sorted(scs["results"], key=_sort_key, reverse=True):
            rows.append(_stock_line(r))
        sections.append("  \n".join(rows))

    # 筹码策略：各档胜率 + 前五（带筹码峰）
    if cs["results"]:
        rows = [f"{_emoji_s(cs)} **筹码策略 {cs['n']}只  胜率{_owr(cs)}  均{_oar(cs)}**"]
        for t in CHIP_TIERS:
            s = chip_tier_stats[t]
            if s["n"] == 0:
                continue
            rows.append(f"**{t}** {s['n']}只  胜率{_owr(s)}  均{_oar(s)}")
            for r in s["top5"]:
                wr = r.get("winner_rate")
                sp = r.get("spread_pct")
                try:
                    sp_f = float(sp) if sp is not None else float("nan")
                    sp_s = f"{sp_f:.1f}%{'(集中)' if sp_f < 15 else ('(分散)' if sp_f > 30 else '')}" if not math.isnan(sp_f) else ""
                except Exception:
                    sp_s = ""
                try:
                    wr_s = f"{wr:.0f}%" if wr is not None and not math.isnan(float(wr)) else ""
                except Exception:
                    wr_s = ""
                chip_parts = [x for x in [f"获利{wr_s}" if wr_s else "", f"散度{sp_s}" if sp_s else ""] if x]
                chip_tag = f"<br>`{'·'.join(chip_parts)}`" if chip_parts else ""
                rows.append(_stock_line(r) + chip_tag)
        sections.append("  \n".join(rows))

    # 金叉共振：各档胜率 + 前五（带信号缩写）
    if gs["results"]:
        rows = [f"{_emoji_s(gs)} **金叉共振 {gs['n']}只  胜率{_owr(gs)}  均{_oar(gs)}**"]
        for t in GC_TIERS:
            s = gc_tier_stats[t]
            if s["n"] == 0:
                continue
            rows.append(f"**{t}** {s['n']}只  胜率{_owr(s)}  均{_oar(s)}")
            for r in s["top5"]:
                sig_s = "·".join(_SIG_SHORT.get(sg, sg) for sg in r.get("signals", []))
                sig_tag = f"<br>`{sig_s}`" if sig_s else ""
                rows.append(_stock_line(r) + sig_tag)
        sections.append("  \n".join(rows))

    # 热榜策略：H0（top5%）+ H1（全部）
    sh0 = hot_tier_stats["H0"]
    sh1 = hot_tier_stats["H1"]
    if sh0["results"] or sh1["results"]:
        rows = [f"{_emoji_s(sh1)} **热榜策略 {sh1['n']}只  胜率{_owr(sh1)}  均{_oar(sh1)}**"]
        if sh0["n"] > 0:
            rows.append(f"**H0** {sh0['n']}只  胜率{_owr(sh0)}  均{_oar(sh0)}")
            for r in sh0["top5"]:
                bd = r.get("breakdown", [])
                bd_tag = f"<br>`{'·'.join(bd)}`" if bd else ""
                rows.append(_stock_line(r) + bd_tag)
        if sh1["n"] > 0:
            rows.append(f"**H1** {sh1['n']}只  胜率{_owr(sh1)}  均{_oar(sh1)}")
            for r in sh1["top5"]:
                bd = r.get("breakdown", [])
                bd_tag = f"<br>`{'·'.join(bd)}`" if bd else ""
                rows.append(_stock_line(r) + bd_tag)
        sections.append("  \n".join(rows))

    # ETF 策略
    if etf_stats["results"]:
        rows = [f"{_emoji_s(etf_stats)} **ETF策略 {etf_stats['n']}只  胜率{_owr(etf_stats)}  均{_oar(etf_stats)}**"]
        for r in sorted(etf_stats["results"], key=_sort_key, reverse=True):
            rows.append(_stock_line(r))
        sections.append("  \n".join(rows))

    # 自选池监控强买
    if wl_mon_stats["results"]:
        src_label = {"main_scan": "主策略", "gc_scan": "金叉", "hot_scan": "热榜", "manual": "手动"}
        rows = [f"{_emoji_s(wl_mon_stats)} **监控强买 {wl_mon_stats['n']}只  胜率{_owr(wl_mon_stats)}  均{_oar(wl_mon_stats)}**"]
        for r in sorted(wl_mon_stats["results"], key=_sort_key, reverse=True):
            sig = next((p.get("source", "") for p in wl_mon_picks if p["code"] == r["code"]), "")
            tag = f" [{src_label.get(sig, sig)}]" if sig else ""
            rows.append(_stock_line(r) + tag)
        sections.append("  \n".join(rows))

    sections.append("⚠️ 仅供参考，不构成投资建议")
    push_body = "\n\n".join(sections)
    print(f"\n{push_body}\n")

    if args.dry_run:
        print("[daily_perf] dry-run，不写入不推送")
        return

    # ── 各自保存历史 ──────────────────────────────────────────────────────────
    ts = now.isoformat(timespec="seconds")
    def _tier_rec(s):
        rec = {"n": s["n"], "win_rate": s["win_rate"], "avg_ret": s["avg_ret"],
               "picks": [{"code": r["code"], "name": r["name"], "pct": r["pct"],
                          **({"open_pct": r["open_pct"]} if "open_pct" in r else {}),
                          **({"prices": r["prices"]}     if "prices"   in r else {})}
                         for r in s["results"]]}
        if s.get("open_win_rate") is not None:
            rec["open_win_rate"] = s["open_win_rate"]
            rec["open_avg_ret"]  = s["open_avg_ret"]
        return rec
    _append(MAIN_PERF_PATH,
            {"date": today, "logged": ts, **_tier_rec(ms)},
            today, args.force)
    _append(SC_PERF_PATH,
            {"date": today, "logged": ts, **_tier_rec(scs)},
            today, args.force)
    _append(CHIP_PERF_PATH,
            {"date": today, "logged": ts,
             "total": _tier_rec(cs),
             **{t: _tier_rec(chip_tier_stats[t]) for t in CHIP_TIERS}},
            today, args.force)
    _append(GC_PERF_PATH,
            {"date": today, "logged": ts,
             "total": _tier_rec(gs),
             **{t: _tier_rec(gc_tier_stats[t]) for t in GC_TIERS}},
            today, args.force)
    _append(HOT_PERF_PATH,
            {"date": today, "logged": ts,
             "H0": _tier_rec(hot_tier_stats["H0"]),
             "H1": _tier_rec(hot_tier_stats["H1"])},
            today, args.force)
    _append(WL_MON_PERF_PATH,
            {"date": today, "logged": ts, **_tier_rec(wl_mon_stats)},
            today, args.force)
    _append(ETF_PERF_PATH,
            {"date": today, "logged": ts, **_tier_rec(etf_stats)},
            today, args.force)
    print("[daily_perf] 历史记录已写入")

    # ── 推送 ──────────────────────────────────────────────────────────────────
    try:
        import sys
        sys.path.insert(0, str(ROOT / "src"))
        from common import push_wechat
        def _tv(s):
            v = s.get("open_win_rate") if s.get("open_win_rate") is not None else s.get("win_rate")
            return v
        parts = []
        if _tv(ms)        is not None: parts.append(f"主{_tv(ms)}%")
        if _tv(cs)        is not None: parts.append(f"筹{_tv(cs)}%")
        if _tv(gs)        is not None: parts.append(f"叉{_tv(gs)}%")
        if _tv(hs)        is not None: parts.append(f"热{_tv(hs)}%")
        if _tv(wl_mon_stats) is not None: parts.append(f"监{_tv(wl_mon_stats)}%")
        if _tv(etf_stats) is not None: parts.append(f"ETF{_tv(etf_stats)}%")
        title = f"今开胜率 {date_fmt} | {' / '.join(parts)}"
        push_wechat(title, push_body)
        print("[daily_perf] 微信推送成功")
    except Exception as e:
        print(f"[daily_perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
