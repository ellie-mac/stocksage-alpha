#!/usr/bin/env python3
"""
每日统一胜率记录 — 收盘后一条微信
运行时间：16:00（市场收盘后）

包含四个策略：
  主策略    — latest_picks.json（前日 18:30 扫盘）
  筹码策略  — CAH∩CAD∩CADM 三者共有 C0-C3（前日 20:30 扫描）
  金叉共振  — golden_cross_YYYYMMDD.json G0-G2（前日 19:30 扫描）
  热榜策略  — hot_scan_YYYYMMDD.json picks（前日 19:00 扫描）

用法：
    python -X utf8 scripts/daily_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

MAIN_PICKS_PATH = DATA_DIR / "latest_picks.json"
SIG_PATH        = DATA_DIR / "signals_log.json"

MAIN_PERF_PATH  = DATA_DIR / "main_daily_perf.json"
SC_PERF_PATH    = DATA_DIR / "sc_daily_perf.json"
CHIP_PERF_PATH  = DATA_DIR / "chip_daily_perf.json"
GC_PERF_PATH    = DATA_DIR / "gc_daily_perf.json"
HOT_PERF_PATH   = DATA_DIR / "hot_daily_perf.json"

CHIP_TIERS = ["C0", "C1", "C2", "C3"]
GC_TIERS      = ["G0", "G1", "G2"]
HOT_TIERS     = ["H0", "H1"]


# ── 行情 ──────────────────────────────────────────────────────────────────────

def _fetch_prices(codes: list[str]) -> dict[str, float]:
    import sys, pandas as pd
    sys.path.insert(0, str(ROOT / "scripts"))
    from common import get_spot_em
    df = get_spot_em()
    if not df.empty:
        df["_code"] = df["代码"].astype(str).str.zfill(6)
        df = df[df["_code"].isin(codes)].copy()
        df["_pct"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
        result = dict(zip(df["_code"], df.dropna(subset=["_pct"])["_pct"]))
        if result:
            return result

    # fallback：tushare daily 一次拉全市场今日涨跌幅
    print("[daily_perf] spot_em 不可用，改用 tushare daily fallback", flush=True)
    try:
        import json as _json, tushare as _ts
        from datetime import date as _date, timedelta as _td
        _cfg = _json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        _token = _cfg.get("tushare", {}).get("token", "")
        _ts.set_token(_token)
        _pro = _ts.pro_api()
        for _delta in range(3):
            _d = (_date.today() - _td(days=_delta)).strftime("%Y%m%d")
            _df = _pro.daily(trade_date=_d, fields="ts_code,pct_chg")
            if _df is not None and not _df.empty:
                _df["_code"] = _df["ts_code"].str.split(".").str[0]
                _df = _df[_df["_code"].isin(codes)]
                result = dict(zip(_df["_code"], _df["pct_chg"].astype(float)))
                if result:
                    print(f"[daily_perf] tushare daily {_d} 拿到 {len(result)} 只", flush=True)
                    return result
    except Exception as e:
        print(f"[daily_perf] tushare daily 失败: {e}", flush=True)
    return {}


# ── 统计工具 ──────────────────────────────────────────────────────────────────

_SIG_SHORT = {
    "MACD金叉": "MACD", "KDJ金叉": "KDJ", "RSI金叉": "RSI",
    "MA5/10金叉": "MA5/10", "MA10/20金叉": "MA10/20",
    "量能金叉": "量", "OBV金叉": "OBV", "布林中轨金叉": "布林",
}


def _stats(items: list[dict], prices: dict[str, float]) -> dict:
    results = [{"code": p["code"], "name": p.get("name", p["code"]),
                "pct": prices[p["code"]], "signals": p.get("signals", [])}
               for p in items if p["code"] in prices]
    if not results:
        return {"n": 0, "win_rate": None, "avg_ret": None, "top5": [], "results": []}
    vals = [r["pct"] for r in results]
    win_rate = round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1)
    avg_ret  = round(sum(vals) / len(vals), 2)
    top5 = sorted(results, key=lambda r: r["pct"], reverse=True)[:5]
    return {"n": len(results), "win_rate": win_rate, "avg_ret": avg_ret,
            "top5": top5, "results": results}


def _emoji(win_rate: float | None) -> str:
    if win_rate is None: return "⚪"
    if win_rate >= 60:   return "🟢"
    if win_rate >= 40:   return "🟡"
    return "🔴"


def _wr(s: dict) -> str:
    return f"{s['win_rate']}%" if s["win_rate"] is not None else "-"

def _ar(s: dict) -> str:
    return f"{s['avg_ret']:+.2f}%" if s["avg_ret"] is not None else "-"


# ── 各策略数据加载 ─────────────────────────────────────────────────────────────

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
        (p for p in DATA_DIR.glob(glob_pat) if cutoff <= p.stem[-8:] <= today),
        key=lambda p: p.stem[-8:], reverse=True,
    )
    if not candidates:
        return None
    try:
        return json.loads(candidates[0].read_text(encoding="utf-8"))
    except Exception:
        return None


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
        {p["code"] for ct in CHIP_TIERS for p in src.get("tiers", {}).get(ct, [])}
        for k, src in available.items() if k != base_key
    ]
    return {
        ct: [p for p in base.get("tiers", {}).get(ct, [])
             if all(p["code"] in fs for fs in filter_sets)]
        for ct in CHIP_TIERS
    }


def _load_gc(today: str) -> dict[str, list[dict]]:
    """G0-G2：找前日带日期的扫描文件，按档返回"""
    gc = _find_prev("golden_cross_????????.json", today, days=7)
    if not gc:
        return {t: [] for t in GC_TIERS}
    tiers = gc.get("tiers", {})
    return {t: tiers.get(t, []) for t in GC_TIERS}


def _load_hot(today: str) -> dict[str, list[dict]]:
    """热榜策略：H0=热度top5%，H1=全部picks"""
    raw = _find_prev("hot_scan_????????.json", today, days=3)
    if not raw:
        return {t: [] for t in HOT_TIERS}
    picks = [{"code": str(p["code"]).zfill(6), "name": p.get("name", p["code"]),
               "rank_pct": p.get("rank_pct", 100)}
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
    path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
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

    chip_flat = [p for picks in chip_by_tier.values() for p in picks]
    gc_flat   = [p for picks in gc_by_tier.values()   for p in picks]
    hot_flat  = hot_by_tier["H1"]

    print(f"[daily_perf] 主策略 {len(main_picks)}只 / 小票 {len(sc_picks)}只 / 筹码 {len(chip_flat)}只 / 金叉 {len(gc_flat)}只 / 热榜H0:{len(hot_by_tier['H0'])}只/H1:{len(hot_flat)}只")

    if not main_picks and not sc_picks and not chip_flat and not gc_flat and not hot_flat:
        print("[daily_perf] 所有策略均无数据，退出")
        return

    # ── 一次性拉取所有行情 ────────────────────────────────────────────────────
    all_codes = list({p["code"] for p in main_picks + sc_picks + chip_flat + gc_flat + hot_flat})
    print(f"[daily_perf] 获取 {len(all_codes)} 只行情 ...")
    prices = _fetch_prices(all_codes)
    print(f"[daily_perf] 获取到 {len(prices)} 只")
    if not prices:
        print("[daily_perf] spot_em 返回空，跳过写入历史")
        return

    # ── 统计 ──────────────────────────────────────────────────────────────────
    ms  = _stats(main_picks, prices)
    scs = _stats(sc_picks,   prices)
    chip_tier_stats = {t: _stats(chip_by_tier[t], prices) for t in CHIP_TIERS}
    gc_tier_stats   = {t: _stats(gc_by_tier[t],   prices) for t in GC_TIERS}
    hot_tier_stats  = {t: _stats(hot_by_tier[t],  prices) for t in HOT_TIERS}
    cs = _stats(chip_flat, prices)
    gs = _stats(gc_flat,   prices)
    hs = hot_tier_stats["H1"]

    for label, s in [("主策略", ms), ("小票", scs), ("筹码", cs), ("金叉", gs), ("热榜H1", hs)]:
        if s["n"] > 0:
            print(f"  [{label}] {s['n']}只  胜率{_wr(s)}  均涨{_ar(s)}")

    # ── 构建推送 ──────────────────────────────────────────────────────────────
    sections: list[str] = []

    # 主策略
    if ms["results"]:
        rows = [f"{_emoji(ms['win_rate'])} **主策略 {ms['n']}只  胜率{_wr(ms)}  均{_ar(ms)}**"]
        for r in sorted(ms["results"], key=lambda r: r["pct"], reverse=True):
            rows.append(f"  {r['code']} {r['name']} {r['pct']:+.2f}%")
        sections.append("  \n".join(rows))

    # 小票策略
    if scs["results"]:
        rows = [f"{_emoji(scs['win_rate'])} **小票策略 {scs['n']}只  胜率{_wr(scs)}  均{_ar(scs)}**"]
        for r in sorted(scs["results"], key=lambda r: r["pct"], reverse=True):
            rows.append(f"  {r['code']} {r['name']} {r['pct']:+.2f}%")
        sections.append("  \n".join(rows))

    # 筹码策略：各档胜率 + 前五
    if cs["results"]:
        rows = [f"{_emoji(cs['win_rate'])} **筹码策略 {cs['n']}只  胜率{_wr(cs)}  均{_ar(cs)}**"]
        for t in CHIP_TIERS:
            s = chip_tier_stats[t]
            if s["n"] == 0:
                continue
            rows.append(f"**{t}** {s['n']}只  胜率{_wr(s)}  均{_ar(s)}")
            for r in s["top5"]:
                rows.append(f"  {r['name']} {r['pct']:+.2f}%")
        sections.append("  \n".join(rows))

    # 金叉共振：各档胜率 + 前五（带信号缩写）
    if gs["results"]:
        rows = [f"{_emoji(gs['win_rate'])} **金叉共振 {gs['n']}只  胜率{_wr(gs)}  均{_ar(gs)}**"]
        for t in GC_TIERS:
            s = gc_tier_stats[t]
            if s["n"] == 0:
                continue
            rows.append(f"**{t}** {s['n']}只  胜率{_wr(s)}  均{_ar(s)}")
            for r in s["top5"]:
                sig_s = "·".join(_SIG_SHORT.get(sg, sg) for sg in r.get("signals", []))
                sig_tag = f"<br>`{sig_s}`" if sig_s else ""
                rows.append(f"  {r['name']} {r['pct']:+.2f}%{sig_tag}")
        sections.append("  \n".join(rows))

    # 热榜策略：H0（top5%）+ H1（全部）
    sh0 = hot_tier_stats["H0"]
    sh1 = hot_tier_stats["H1"]
    if sh0["results"] or sh1["results"]:
        rows = [f"{_emoji(sh1['win_rate'])} **热榜策略 {sh1['n']}只  胜率{_wr(sh1)}  均{_ar(sh1)}**"]
        if sh0["n"] > 0:
            rows.append(f"**H0** {sh0['n']}只  胜率{_wr(sh0)}  均{_ar(sh0)}")
            for r in sh0["top5"]:
                rows.append(f"  {r['name']} {r['pct']:+.2f}%")
        if sh1["n"] > 0:
            rows.append(f"**H1** {sh1['n']}只  胜率{_wr(sh1)}  均{_ar(sh1)}")
            for r in sh1["top5"]:
                rows.append(f"  {r['name']} {r['pct']:+.2f}%")
        sections.append("  \n".join(rows))

    sections.append("⚠️ 仅供参考，不构成投资建议")
    push_body = "\n\n".join(sections)
    print(f"\n{push_body}\n")

    if args.dry_run:
        print("[daily_perf] dry-run，不写入不推送")
        return

    # ── 各自保存历史 ──────────────────────────────────────────────────────────
    ts = now.isoformat(timespec="seconds")
    def _tier_rec(s): return {"n": s["n"], "win_rate": s["win_rate"], "avg_ret": s["avg_ret"],
                               "picks": [{"code": r["code"], "name": r["name"], "pct": r["pct"]} for r in s["results"]]}
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
    print("[daily_perf] 历史记录已写入")

    # ── 推送 ──────────────────────────────────────────────────────────────────
    try:
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from common import push_wechat
        parts = []
        if ms["win_rate"] is not None: parts.append(f"主{ms['win_rate']}%")
        if cs["win_rate"] is not None: parts.append(f"筹{cs['win_rate']}%")
        if gs["win_rate"] is not None: parts.append(f"叉{gs['win_rate']}%")
        if hs["win_rate"] is not None: parts.append(f"热{hs['win_rate']}%")
        title = f"收盘胜率 {date_fmt} | {' / '.join(parts)}"
        push_wechat(title, push_body)
        print("[daily_perf] 微信推送成功")
    except Exception as e:
        print(f"[daily_perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
