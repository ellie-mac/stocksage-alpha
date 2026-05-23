#!/usr/bin/env python3
"""通用策略回测框架——对所有有 dated json 归档的策略跑 T+N 胜率分析。

输入：
  - strategy name (escalator/gc/sideways/hot/chip)
  - 日期范围（默认所有可用）
  - hold periods (T+N，默认 1/5/10)

输出：
  - CSV: data/backtest/<strategy>_picks.csv  每一行一个 pick + ret_t1/ret_t5/ret_t10
  - CSV: data/backtest/summary.csv  各策略汇总 stats
  - 终端打印简洁 summary 表

冷却去重：同一只 code 在 cooldown_days 个交易日内多次入选只算首次
（避免持续触发的票被反复计数，forward window 不重叠）。

跟 src/backtest/ 其他文件的关系：
  - main.py / run_all.py / etf.py 是 portfolio-style backtest（基于因子打分
    合成历史 portfolio，看 alpha/Sharpe/drawdown）— 学术风格
  - strategy_replay.py（本文件）是 scan-output replay backtest（读策略实际
    选出来的票，看它们后续真实表现）— 更直观
  两种方法互补：portfolio 看策略**逻辑**的潜力上限，replay 看实际**落地**的胜率

用法：
  python -X utf8 src/backtest/strategy_replay.py                  # 全部
  python -X utf8 src/backtest/strategy_replay.py --strategy escalator gc
  python -X utf8 src/backtest/strategy_replay.py --start 20260501 --end 20260520
  python -X utf8 src/backtest/strategy_replay.py --horizons 1 5 10 20
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
OUT_DIR = DATA / "backtest"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── pick extractors ──────────────────────────────────────────────────────────
# 每个 extractor 接受一个 dated json 的 dict，返回 [{code, name, tier?, close}, ...]

def _extract_escalator(d: dict) -> list[dict]:
    out = []
    for tier_name, tier_picks in (d.get("tiers") or {}).items():
        for p in tier_picks:
            if not p.get("code"):
                continue
            out.append({
                "code": str(p["code"])[-6:],
                "name": p.get("name", ""),
                "tier": tier_name,
                "close": p.get("close"),
                "industry": p.get("industry", ""),
                "matched_tiers": ",".join(p.get("matched_tiers") or []),
            })
    return out


def _extract_gc(d: dict) -> list[dict]:
    out = []
    for tier_name, tier_picks in (d.get("tiers") or {}).items():
        for p in tier_picks:
            if not p.get("code"):
                continue
            code = str(p["code"])
            code6 = code[-6:] if len(code) > 6 else code
            out.append({
                "code": code6,
                "name": p.get("name", ""),
                "tier": tier_name,
                "close": p.get("close"),
                "industry": p.get("industry", ""),
                "matched_tiers": "",
            })
    return out


def _extract_sideways(d: dict) -> list[dict]:
    out = []
    for p in d.get("all_picks", []):
        if not p.get("code"):
            continue
        out.append({
            "code": str(p["code"])[-6:],
            "name": p.get("name", ""),
            "tier": p.get("tier", ""),
            "close": p.get("close"),
            "industry": p.get("industry", ""),
            "matched_tiers": "",
        })
    return out


def _extract_hot(d: dict) -> list[dict]:
    out = []
    for p in d.get("picks", []):
        if not p.get("code"):
            continue
        out.append({
            "code": str(p["code"])[-6:],
            "name": p.get("name", ""),
            "tier": "H1" if p.get("rank_pct", 100) > 5 else "H0",
            "close": p.get("close"),
            "industry": p.get("industry", ""),
            "matched_tiers": "",
        })
    return out


def _extract_chip(d: dict) -> list[dict]:
    """chip_scan_<date>.json 的 all_picks，每只票按 tier (C0-C3) 分组。"""
    out = []
    for p in d.get("all_picks", []):
        if not p.get("code"):
            continue
        out.append({
            "code": str(p["code"])[-6:],
            "name": p.get("name", ""),
            "tier": p.get("tier", ""),
            "close": p.get("close"),
            "industry": p.get("industry", ""),
            "matched_tiers": "",
        })
    return out


def _extract_institution(d: dict) -> list[dict]:
    """institution_scan_<date>.json: hits 数组，每项 {stock_code, stock_name, fund_count, buyers}.
    用 fund_count 当 tier（H = ≥5 funds, M = 3-4, L = 1-2）。
    """
    out = []
    for h in d.get("hits", []):
        code = h.get("stock_code")
        if not code:
            continue
        fc = h.get("fund_count", 0) or 0
        if fc >= 5:
            tier = "INS_H"
        elif fc >= 3:
            tier = "INS_M"
        else:
            tier = "INS_L"
        out.append({
            "code": str(code)[-6:],
            "name": h.get("stock_name", ""),
            "tier": tier,
            "close": None,
            "industry": "",
            "matched_tiers": "",
        })
    return out


def _extract_picks_list(d: dict) -> list[dict]:
    """通用 picks 抽取器：main/sc/etf/marketcap 都是 {"picks": [{"code","name",...}]} 格式。

    'close' 字段：main/sc 写 'price' (买入分时收盘价)，marketcap 写 'price'，etf 没有
    close。统一映射到 close = price / None。
    marketcap 还透传 mv_rank / marketcap_yi 供 bucket 分析。
    """
    out = []
    for p in d.get("picks", []):
        if not p.get("code"):
            continue
        close = p.get("close") if p.get("close") is not None else p.get("price")
        out.append({
            "code": str(p["code"])[-6:],
            "name": p.get("name", ""),
            "tier": p.get("tier", ""),
            "close": close,
            "industry": p.get("industry", ""),
            "matched_tiers": "",
            "mv_rank": p.get("mv_rank"),
            "mv_yi": p.get("marketcap_yi"),
        })
    return out


# ── strategy registry ────────────────────────────────────────────────────────

STRATEGIES: dict[str, dict] = {
    "escalator":  {"label": "扶梯",   "glob": "escalator_????????.json",     "extract": _extract_escalator},
    "gc":         {"label": "金叉",   "glob": "golden_cross_????????.json",  "extract": _extract_gc},
    "sideways":   {"label": "横盘",   "glob": "sideways_????????.json",      "extract": _extract_sideways},
    "hot":        {"label": "热榜",   "glob": "hot_scan_????????.json",      "extract": _extract_hot},
    "chip":       {"label": "筹码",   "glob": "chip_scan_????????.json",     "extract": _extract_chip},
    "main":       {"label": "主策略", "glob": "main_picks_????????.json",    "extract": _extract_picks_list},
    "small":      {"label": "小盘",   "glob": "sc_picks_????????.json",      "extract": _extract_picks_list},
    "etf":        {"label": "ETF",    "glob": "etf_picks_????????.json",     "extract": _extract_picks_list},
    "marketcap":  {"label": "市值",   "glob": "marketcap_????????.json",     "extract": _extract_picks_list},
    "institution": {"label": "机构",  "glob": "institution_scan_????????.json", "extract": _extract_institution},
}

# main/sc/etf 因为依赖 score_one_buy 因子链（每个因子用当下数据），
# 没法历史 as-of 回填。它们的 dated 归档是每天 live 跑时新写的，
# 样本会随时间累积；目前样本少属于正常。
# marketcap 用 close × 隐含股本重建市值实现了 --backfill，可以填历史。
TODO_STRATEGIES: dict = {}


# ── core ─────────────────────────────────────────────────────────────────────

def load_picks(strategy: str, start: str = "", end: str = "") -> list[dict]:
    """读所有 dated 文件，扁平为 [{date, strategy, code, name, tier, close, ...}, ...]"""
    if strategy not in STRATEGIES:
        if strategy in TODO_STRATEGIES:
            print(f"[backtest] {strategy}: {TODO_STRATEGIES[strategy]}", flush=True)
        else:
            print(f"[backtest] 未知策略: {strategy}", flush=True)
        return []
    cfg = STRATEGIES[strategy]
    out: list[dict] = []
    for path in sorted(DATA.glob(cfg["glob"])):
        date_str = path.stem.split("_")[-1]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        if start and date_str < start:
            continue
        if end and date_str > end:
            continue
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[backtest] read failed {path.name}: {e}", flush=True)
            continue
        for p in cfg["extract"](d):
            out.append({"date": date_str, "strategy": strategy, **p})
    return out


def _fetch_forward(code: str, pick_date: str, horizons: list[int],
                   entry_close: Optional[float]) -> dict[int, Optional[float]]:
    """T+n forward return % vs entry_close。pick_date YYYYMMDD；fetcher 拉 60 天。

    entry_close 缺失时（etf_picks 不存 close）从 history 取 pick_date 当日 close 兜底。
    """
    import fetcher as _f
    import pandas as _pd
    try:
        df = _f.get_price_history(code, days=60)
    except Exception:
        return {n: None for n in horizons}
    if df is None or df.empty or "date" not in df.columns:
        return {n: None for n in horizons}
    pick_ts = _pd.to_datetime(pick_date, format="%Y%m%d")

    if entry_close is None or entry_close <= 0:
        entry_df = df[df["date"] <= pick_ts]
        if entry_df.empty:
            return {n: None for n in horizons}
        try:
            entry_close = float(entry_df["close"].iloc[-1])
        except Exception:
            return {n: None for n in horizons}
        if entry_close <= 0:
            return {n: None for n in horizons}

    df = df[df["date"] > pick_ts].sort_values("date")
    out: dict[int, Optional[float]] = {}
    for n in horizons:
        if len(df) < n:
            out[n] = None
        else:
            forward_close = float(df["close"].iloc[n - 1])
            out[n] = round((forward_close / entry_close - 1) * 100, 2)
    return out


def attach_returns(picks: list[dict], horizons: list[int],
                   workers: int = 12) -> list[dict]:
    """并发拉每只 pick 的 T+n forward returns。"""
    if not picks:
        return []
    out = [dict(p) for p in picks]   # shallow copy 防破坏 caller
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_fetch_forward, p["code"], p["date"], horizons, p.get("close")): i
            for i, p in enumerate(out)
        }
        done = 0
        for fut in as_completed(futs):
            i = futs[fut]
            rets = fut.result()
            for n in horizons:
                out[i][f"ret_t{n}"] = rets.get(n)
            done += 1
            if done % 200 == 0:
                print(f"[backtest] forward returns: {done}/{len(out)}", flush=True)
    return out


def _apply_cooldown(picks: list[dict], cooldown_days: int) -> list[dict]:
    """同一 code 在 cooldown_days 个交易日内多次入选只保留首次。
    用 picks 内 unique date 排序当 trading-day calendar；cooldown 是该 calendar 上 index 差。"""
    if not picks or cooldown_days <= 0:
        return picks
    all_dates = sorted({p["date"] for p in picks})
    idx_of = {d: i for i, d in enumerate(all_dates)}
    last_idx: dict[str, int] = {}
    kept: list[dict] = []
    for p in sorted(picks, key=lambda x: x["date"]):
        i = idx_of[p["date"]]
        prev = last_idx.get(p["code"])
        if prev is None or (i - prev) >= cooldown_days:
            kept.append(p)
            last_idx[p["code"]] = i
    return kept


def summarize(picks_with_ret: list[dict], horizons: list[int]) -> dict:
    """计算每个 horizon 的 win_rate / avg_ret / N（已 cooldown 去重）。"""
    summary: dict = {"n_total": len(picks_with_ret), "n_dates": len({p["date"] for p in picks_with_ret})}
    for n in horizons:
        deduped = _apply_cooldown([p for p in picks_with_ret if p.get(f"ret_t{n}") is not None], n)
        rets = [p[f"ret_t{n}"] for p in deduped]
        if rets:
            summary[f"t{n}"] = {
                "n": len(rets),
                "win_rate": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
                "avg_ret": round(mean(rets), 2),
                "median_ret": round(sorted(rets)[len(rets) // 2], 2),
            }
        else:
            summary[f"t{n}"] = {"n": 0, "win_rate": None, "avg_ret": None, "median_ret": None}
    return summary


# ── output ───────────────────────────────────────────────────────────────────

def _write_picks_csv(picks_with_ret: list[dict], strategy: str, horizons: list[int]) -> Path:
    out = OUT_DIR / f"{strategy}_picks.csv"
    cols = ["date", "strategy", "code", "name", "tier", "matched_tiers", "industry", "close",
            "mv_rank", "mv_yi"] + [f"ret_t{n}" for n in horizons]
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for p in picks_with_ret:
            w.writerow(p)
    return out


def _write_summary_csv(per_strategy: dict[str, dict], horizons: list[int]) -> Path:
    out = OUT_DIR / "summary.csv"
    cols = ["strategy", "label", "n_dates", "n_total"]
    for n in horizons:
        cols += [f"t{n}_n", f"t{n}_win_rate%", f"t{n}_avg_ret%", f"t{n}_median_ret%"]
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for strategy, s in per_strategy.items():
            row = [strategy, STRATEGIES.get(strategy, {}).get("label", ""), s["n_dates"], s["n_total"]]
            for n in horizons:
                tn = s.get(f"t{n}", {})
                row += [tn.get("n", 0), tn.get("win_rate"), tn.get("avg_ret"), tn.get("median_ret")]
            w.writerow(row)
    return out


def _print_summary(per_strategy: dict[str, dict], horizons: list[int]) -> None:
    print()
    print(f"{'策略':<8} {'天数':>5} {'picks':>7}", end="")
    for n in horizons:
        print(f"  {'T+'+str(n)+'_win':>9} {'T+'+str(n)+'_avg':>9} {'T+'+str(n)+'_n':>7}", end="")
    print()
    print("-" * (28 + 27 * len(horizons)))
    for strategy, s in per_strategy.items():
        label = STRATEGIES.get(strategy, {}).get("label", strategy)
        print(f"{label:<6} {s['n_dates']:>5} {s['n_total']:>7}", end="")
        for n in horizons:
            tn = s.get(f"t{n}", {})
            wr = tn.get("win_rate")
            ar = tn.get("avg_ret")
            nn = tn.get("n", 0)
            wr_s = f"{wr:.1f}%" if wr is not None else "-"
            ar_s = f"{ar:+.2f}%" if ar is not None else "-"
            print(f"  {wr_s:>9} {ar_s:>9} {nn:>7}", end="")
        print()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", nargs="+",
                    help=f"策略名，默认全部。可选: {list(STRATEGIES.keys())}")
    ap.add_argument("--start", default="", help="YYYYMMDD 起始日")
    ap.add_argument("--end",   default="", help="YYYYMMDD 结束日")
    ap.add_argument("--horizons", nargs="+", type=int, default=[1, 5, 10],
                    help="forward return 周期，默认 1 5 10")
    args = ap.parse_args()

    strategies = args.strategy or list(STRATEGIES.keys())
    print(f"[backtest] strategies={strategies}, start={args.start or 'auto'}, end={args.end or 'auto'}, horizons={args.horizons}")

    per_strategy: dict[str, dict] = {}
    for s in strategies:
        print(f"\n=== {s} ===")
        picks = load_picks(s, args.start, args.end)
        if not picks:
            print(f"[backtest] {s}: 无 picks")
            per_strategy[s] = {"n_total": 0, "n_dates": 0, **{f"t{n}": {"n": 0} for n in args.horizons}}
            continue
        print(f"[backtest] {s}: {len(picks)} picks across {len({p['date'] for p in picks})} dates")
        enriched = attach_returns(picks, args.horizons)
        summary = summarize(enriched, args.horizons)
        per_strategy[s] = summary
        path = _write_picks_csv(enriched, s, args.horizons)
        print(f"[backtest] {s}: wrote {len(enriched)} rows → {path.relative_to(ROOT)}")

    summary_path = _write_summary_csv(per_strategy, args.horizons)
    print(f"\n[backtest] summary → {summary_path.relative_to(ROOT)}")
    _print_summary(per_strategy, args.horizons)

    if TODO_STRATEGIES:
        print(f"\n[backtest] 待接入策略（需先加 --date 回填）:")
        for k, v in TODO_STRATEGIES.items():
            print(f"  - {k}: {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
