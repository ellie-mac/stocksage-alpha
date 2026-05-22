#!/usr/bin/env python3
"""共振 alpha 分析 — 同一天被多个策略命中的票，胜率是否显著更高？

读 strategy_replay 写出的各策略 picks.csv，按 (date, code) 聚合，统计：
  - 该 (date, code) 被几个策略命中
  - 该 pick 在 T+1/3/5/10/20 的 forward return（取任一策略的，应该一致）
  - 按 n_strategies 分桶看 win_rate / avg_ret

用法：
  python -X utf8 src/backtest/resonance.py
  python -X utf8 src/backtest/resonance.py --horizons 1 3 5 10
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS_DIR = ROOT / "data" / "backtest"

# 排除 sideways（归档只有 3 天，没法做有意义聚合）
STRATEGIES = ["escalator", "gc", "hot", "chip", "marketcap", "etf"]


def _parse_float(s: str) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_all() -> dict[tuple[str, str], dict]:
    """(date, code) → {strategies: set, ret_t1, ret_t3, ..., mv_rank, mv_yi, regime_score}"""
    agg: dict[tuple[str, str], dict] = {}
    for s in STRATEGIES:
        p = PICKS_DIR / f"{s}_picks.csv"
        if not p.exists():
            continue
        with open(p, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                key = (r["date"], r["code"])
                entry = agg.setdefault(key, {"strategies": set(), "name": r.get("name", "")})
                entry["strategies"].add(s)
                for h in (1, 3, 5, 10, 20):
                    v = _parse_float(r.get(f"ret_t{h}", ""))
                    if v is not None:
                        entry[f"ret_t{h}"] = v
                # mv_rank / mv_yi 只在 marketcap 策略写入（其他策略 CSV 列空）
                if s == "marketcap":
                    rk = r.get("mv_rank", "")
                    if rk:
                        try:
                            entry["mv_rank"] = int(rk)
                        except ValueError:
                            pass
                    my = _parse_float(r.get("mv_yi", ""))
                    if my is not None:
                        entry["mv_yi"] = my
                # regime_score: 同一日所有策略都有同样值；regime_attach.py 写入
                rs = _parse_float(r.get("regime_score", ""))
                if rs is not None and "regime_score" not in entry:
                    entry["regime_score"] = rs
    return agg


def _regime_bucket(score: Optional[float]) -> str:
    if score is None:
        return "unknown"
    if score >= 7:
        return "bull (7-9)"
    if score >= 5:
        return "neutral+ (5-6)"
    if score >= 3:
        return "caution (3-4)"
    return "bear (1-2)"


def _rank_bucket(rk: int) -> str:
    if rk <= 20:
        return "1-20"
    if rk <= 50:
        return "21-50"
    if rk <= 100:
        return "51-100"
    return "101-200"


def _bucket(n: int) -> str:
    if n == 1:
        return "1"
    if n == 2:
        return "2"
    return "3+"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizons", nargs="+", type=int, default=[1, 3, 5, 10, 20])
    args = ap.parse_args()

    agg = load_all()
    print(f"[resonance] total unique (date, code) pairs: {len(agg)}")

    # 桶 → list of entries
    buckets: dict[str, list[dict]] = defaultdict(list)
    for key, entry in agg.items():
        b = _bucket(len(entry["strategies"]))
        buckets[b].append(entry)

    print(f"\n各桶样本量:")
    for b in ["1", "2", "3+"]:
        print(f"  命中 {b} 个策略: {len(buckets[b])} unique picks")

    # 每个桶 × 每个 horizon 的 win_rate / avg_ret
    print(f"\n{'桶':<6}{'n_unique':>10}", end="")
    for h in args.horizons:
        print(f"  {'T+'+str(h)+'_win':>10}{'T+'+str(h)+'_avg':>10}{'T+'+str(h)+'_n':>8}", end="")
    print()
    print("-" * (16 + 28 * len(args.horizons)))

    for b in ["1", "2", "3+"]:
        entries = buckets[b]
        print(f"{b:<6}{len(entries):>10}", end="")
        for h in args.horizons:
            rets = [e[f"ret_t{h}"] for e in entries if f"ret_t{h}" in e]
            if rets:
                wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                avg = mean(rets)
                print(f"  {wr:>9.1f}% {avg:>+9.2f}% {len(rets):>8}", end="")
            else:
                print(f"  {'-':>10}{'-':>10}{0:>8}", end="")
        print()

    # 最常见的 2-策略组合 Top 10
    print(f"\n两策略共振 Top 10 组合（按 unique pick 数）:")
    pair_counts: dict[tuple, int] = defaultdict(int)
    pair_returns: dict[tuple, list] = defaultdict(list)
    for entry in buckets["2"]:
        pair = tuple(sorted(entry["strategies"]))
        pair_counts[pair] += 1
        for h in args.horizons:
            v = entry.get(f"ret_t{h}")
            if v is not None:
                pair_returns[(pair, h)].append(v)

    top_pairs = sorted(pair_counts.items(), key=lambda x: -x[1])[:10]
    print(f"  {'组合':<24}{'n':>6}", end="")
    for h in args.horizons:
        print(f"  {'T+'+str(h)+'_win':>10}{'T+'+str(h)+'_avg':>10}", end="")
    print()
    for pair, n in top_pairs:
        label = "+".join(pair)
        print(f"  {label:<24}{n:>6}", end="")
        for h in args.horizons:
            rets = pair_returns.get((pair, h), [])
            if rets:
                wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                avg = mean(rets)
                print(f"  {wr:>9.1f}% {avg:>+9.2f}%", end="")
            else:
                print(f"  {'-':>10}{'-':>10}", end="")
        print()

    # 按 regime 切桶 — 单策略 / 共振一起拆，看 alpha 是不是 regime-conditional
    print(f"\n按 regime 切桶（同一桶内不分共振，整体 alpha）：")
    print(f"  {'regime':<18}{'n':>6}", end="")
    for h in args.horizons:
        print(f"  {'T+'+str(h)+'_win':>10}{'T+'+str(h)+'_avg':>10}{'T+'+str(h)+'_n':>8}", end="")
    print()
    regime_buckets: dict[str, list[dict]] = defaultdict(list)
    for entry in agg.values():
        regime_buckets[_regime_bucket(entry.get("regime_score"))].append(entry)
    for b in ["bull (7-9)", "neutral+ (5-6)", "caution (3-4)", "bear (1-2)", "unknown"]:
        entries = regime_buckets.get(b, [])
        if not entries:
            continue
        print(f"  {b:<18}{len(entries):>6}", end="")
        for h in args.horizons:
            rets = [e[f"ret_t{h}"] for e in entries if f"ret_t{h}" in e]
            if rets:
                wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                avg = mean(rets)
                print(f"  {wr:>9.1f}% {avg:>+9.2f}% {len(rets):>8}", end="")
            else:
                print(f"  {'-':>10}{'-':>10}{0:>8}", end="")
        print()

    # 按 (regime, 策略) 二维切：看每个策略在不同 regime 的表现差异
    print(f"\n按 (策略, regime) 二维切（仅单策略 unique pick，不含共振）：")
    print(f"  {'策略':<12}{'regime':<18}{'n':>6}", end="")
    for h in args.horizons:
        print(f"  {'T+'+str(h)+'_win':>10}{'T+'+str(h)+'_avg':>10}", end="")
    print()
    for s in STRATEGIES:
        s_entries = [e for e in agg.values() if e["strategies"] == {s}]
        if not s_entries:
            continue
        for b in ["bull (7-9)", "neutral+ (5-6)", "caution (3-4)", "bear (1-2)"]:
            bucket_entries = [e for e in s_entries if _regime_bucket(e.get("regime_score")) == b]
            if not bucket_entries:
                continue
            print(f"  {s:<12}{b:<18}{len(bucket_entries):>6}", end="")
            for h in args.horizons:
                rets = [e[f"ret_t{h}"] for e in bucket_entries if f"ret_t{h}" in e]
                if rets:
                    wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                    avg = mean(rets)
                    print(f"  {wr:>9.1f}% {avg:>+9.2f}%", end="")
                else:
                    print(f"  {'-':>10}{'-':>10}", end="")
            print()

    # chip ∩ marketcap by mv_rank bucket（核心实验）
    chip_mc_entries = [e for e in agg.values()
                       if {"chip", "marketcap"}.issubset(e["strategies"]) and "mv_rank" in e]
    if chip_mc_entries:
        print(f"\nchip ∩ marketcap 按 mv_rank 拆档 (共 {len(chip_mc_entries)} unique picks)：")
        rank_buckets: dict[str, list[dict]] = defaultdict(list)
        for e in chip_mc_entries:
            rank_buckets[_rank_bucket(e["mv_rank"])].append(e)
        print(f"  {'rank 桶':<10}{'n':>5}", end="")
        for h in args.horizons:
            print(f"  {'T+'+str(h)+'_win':>10}{'T+'+str(h)+'_avg':>10}{'T+'+str(h)+'_n':>8}", end="")
        print()
        for b in ["1-20", "21-50", "51-100", "101-200"]:
            entries = rank_buckets.get(b, [])
            print(f"  {b:<10}{len(entries):>5}", end="")
            for h in args.horizons:
                rets = [e[f"ret_t{h}"] for e in entries if f"ret_t{h}" in e]
                if rets:
                    wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                    avg = mean(rets)
                    print(f"  {wr:>9.1f}% {avg:>+9.2f}% {len(rets):>8}", end="")
                else:
                    print(f"  {'-':>10}{'-':>10}{0:>8}", end="")
            print()

    # 3+ 共振具体票（如果有）
    triplets = [e for e in buckets["3+"]]
    if triplets:
        print(f"\n3+ 共振 picks 全部（共 {len(triplets)} 个）：")
        print(f"  {'date':<10}{'code':<8}{'name':<12}{'strategies':<28}", end="")
        for h in args.horizons:
            print(f"  {'T+'+str(h):>7}", end="")
        print()
        # sort by date desc
        triplets_sorted = sorted(
            [(d, c, e) for (d, c), e in agg.items() if len(e["strategies"]) >= 3],
            key=lambda x: (-int(x[0]), x[1]),
        )
        for d, c, e in triplets_sorted[:30]:
            strats = "+".join(sorted(e["strategies"]))
            print(f"  {d:<10}{c:<8}{e.get('name','')[:10]:<12}{strats:<28}", end="")
            for h in args.horizons:
                v = e.get(f"ret_t{h}")
                s = f"{v:+.2f}%" if v is not None else "-"
                print(f"  {s:>7}", end="")
            print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
