"""穷举各种共振组合，找 alpha 最强 + 实战量合理的组合。

对每个组合计算：
  - n (unique pick) / dates / 平均每日只数
  - T+1/T+3/T+5/T+10 win rate / avg ret
重点是带 mv_rank ≤ 50 过滤和 chip tier (C0/C1/C2) 过滤的细分组合。
"""
from __future__ import annotations

import csv
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS_DIR = ROOT / "data" / "backtest"

STRATEGIES = ["escalator", "gc", "hot", "chip", "marketcap"]


def _f(s):
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_all() -> dict[tuple[str, str], dict]:
    agg: dict[tuple[str, str], dict] = {}
    for s in STRATEGIES:
        p = PICKS_DIR / f"{s}_picks.csv"
        if not p.exists():
            continue
        for r in csv.DictReader(open(p, encoding="utf-8-sig")):
            key = (r["date"], r["code"])
            entry = agg.setdefault(key, {"strategies": set(), "tier": {}, "mv_rank": None})
            entry["strategies"].add(s)
            entry["tier"][s] = r.get("tier", "")
            for h in (1, 3, 5, 10, 20):
                v = _f(r.get(f"ret_t{h}"))
                if v is not None:
                    entry[f"ret_t{h}"] = v
            if s == "marketcap":
                rk = r.get("mv_rank", "")
                if rk:
                    try:
                        entry["mv_rank"] = int(rk)
                    except ValueError:
                        pass
            rs = _f(r.get("regime_score"))
            if rs is not None and "regime_score" not in entry:
                entry["regime_score"] = rs
    return agg


def stats(entries: list[dict], horizons=(1, 3, 5, 10)) -> dict:
    out = {"n": len(entries), "dates": len({(e.get("_date",), ) for e in entries})}
    # 不写 dates，直接看 n
    for h in horizons:
        rets = [e[f"ret_t{h}"] for e in entries if f"ret_t{h}" in e]
        if rets:
            wr = sum(1 for r in rets if r > 0) / len(rets) * 100
            avg = mean(rets)
            out[f"t{h}_n"] = len(rets)
            out[f"t{h}_wr"] = round(wr, 1)
            out[f"t{h}_avg"] = round(avg, 2)
        else:
            out[f"t{h}_n"] = 0
            out[f"t{h}_wr"] = None
            out[f"t{h}_avg"] = None
    return out


def per_day_count(entries: list[dict], agg_dates: set) -> float:
    """平均每日 picks（按 entries 横跨的 unique date 数）"""
    if not entries:
        return 0.0
    dates = {e.get("_date_key") for e in entries}
    return len(entries) / max(1, len(dates))


def fmt(s: dict, n_days: int) -> str:
    parts = []
    for h in (1, 3, 5, 10):
        wr = s.get(f"t{h}_wr")
        avg = s.get(f"t{h}_avg")
        if wr is not None:
            parts.append(f"T+{h} {wr:.1f}% / {avg:+.2f}% (n={s.get(f't{h}_n', 0)})")
        else:
            parts.append(f"T+{h} -")
    per_day = s["n"] / max(1, n_days)
    return f"  n={s['n']} (~{per_day:.1f}/天) | " + " | ".join(parts)


def main() -> None:
    agg = load_all()
    # 给每个 entry 标 date
    for (d, c), e in agg.items():
        e["_date_key"] = d
    all_dates = sorted({d for (d, c) in agg.keys()})
    n_days = len(all_dates)
    print(f"总样本: {len(agg)} unique picks, 跨 {n_days} 个日期\n")

    # 定义过滤器
    def has_all(strats):
        return lambda e: strats.issubset(e["strategies"])

    def mv_le(rank):
        return lambda e: e.get("mv_rank") is not None and e["mv_rank"] <= rank

    def chip_tier_in(tiers):
        return lambda e: e.get("tier", {}).get("chip") in tiers

    def regime_in(buckets):
        def f(e):
            s = e.get("regime_score")
            if s is None:
                return False
            if "bull" in buckets and s >= 7:
                return True
            if "neutral" in buckets and 5 <= s < 7:
                return True
            if "caution" in buckets and 3 <= s < 5:
                return True
            if "bear" in buckets and s < 3:
                return True
            return False
        return f

    def apply(filters):
        return [e for e in agg.values() if all(f(e) for f in filters)]

    # 2-way 共振 with mv_rank filter
    print("=" * 80)
    print("2-way 共振 (chip/gc/escalator/hot × marketcap_TOP50/TOP100/无限)")
    print("=" * 80)
    other_strats = ["chip", "gc", "escalator", "hot"]
    for s in other_strats:
        for rk_label, rk_filter in [("all", lambda e: True),
                                       ("TOP50", mv_le(50)),
                                       ("TOP20", mv_le(20))]:
            entries = apply([has_all({s, "marketcap"}), rk_filter])
            if not entries:
                continue
            print(f"\n{s}+market[{rk_label}]:")
            print(fmt(stats(entries), n_days))

    # 3-way 共振
    print("\n" + "=" * 80)
    print("3-way 共振 (穷举 chip/gc/escalator/marketcap 组合)")
    print("=" * 80)
    for combo in combinations(["chip", "gc", "escalator", "marketcap"], 3):
        for rk_label, rk_filter in [("all", lambda e: True), ("TOP50", mv_le(50))]:
            if "marketcap" not in combo and rk_label == "TOP50":
                continue  # 无 marketcap 不需要 rank 过滤
            entries = apply([has_all(set(combo)), rk_filter])
            if not entries:
                continue
            label = "+".join(combo) + (f"[mv≤{rk_label.replace('TOP','')}]" if rk_label != "all" else "")
            print(f"\n{label}:")
            print(fmt(stats(entries), n_days))

    # 4-way
    print("\n" + "=" * 80)
    print("4-way 共振 (chip+gc+escalator+marketcap)")
    print("=" * 80)
    entries = apply([has_all({"chip", "gc", "escalator", "marketcap"})])
    if entries:
        print("chip+gc+escalator+marketcap:")
        print(fmt(stats(entries), n_days))

    # chip + marketcap_TOP50 + chip tier
    print("\n" + "=" * 80)
    print("chip ∩ marketcap_TOP50 按 chip tier 拆")
    print("=" * 80)
    for tier in ("C0", "C1", "C2"):
        entries = apply([has_all({"chip", "marketcap"}), mv_le(50),
                          chip_tier_in({tier})])
        if entries:
            print(f"\nchip[{tier}] ∩ market[TOP50]:")
            print(fmt(stats(entries), n_days))

    # regime × top 组合
    print("\n" + "=" * 80)
    print("最强组合按 regime 拆")
    print("=" * 80)
    target_combos = [
        ("chip+marketcap[TOP50]", {"chip", "marketcap"}, mv_le(50)),
        ("chip+gc",                {"chip", "gc"}, lambda e: True),
        ("chip+gc+marketcap[TOP50]", {"chip", "gc", "marketcap"}, mv_le(50)),
        ("gc+marketcap[TOP50]",     {"gc", "marketcap"}, mv_le(50)),
    ]
    for label, strats, rkf in target_combos:
        print(f"\n{label}:")
        for rb in ("bull", "caution", "bear"):
            entries = apply([has_all(strats), rkf, regime_in({rb})])
            if not entries:
                continue
            print(f"  [{rb}]")
            print(fmt(stats(entries), n_days))


if __name__ == "__main__":
    main()
