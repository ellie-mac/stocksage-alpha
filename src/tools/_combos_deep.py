"""深度组合分析 — 按各策略的 tier × marketcap rank × regime 穷举。

只输出 n ≥ 10 且 alpha 有意义的组合（按 T+10 win 降序排）。
"""
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS_DIR = ROOT / "data" / "backtest"

STRATEGIES = ["chip", "gc", "escalator", "sideways", "marketcap", "hot", "institution"]


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


def stats(entries: list[dict]) -> dict | None:
    if not entries:
        return None
    out = {"n": len(entries)}
    for h in (1, 3, 5, 10):
        rets = [e[f"ret_t{h}"] for e in entries if f"ret_t{h}" in e]
        out[f"t{h}_n"] = len(rets)
        if rets:
            out[f"t{h}_wr"] = sum(1 for r in rets if r > 0) / len(rets) * 100
            out[f"t{h}_avg"] = mean(rets)
        else:
            out[f"t{h}_wr"] = None
            out[f"t{h}_avg"] = None
    return out


def mv_bucket(rk):
    if rk is None:
        return None
    if rk <= 20: return "mv1-20"
    if rk <= 50: return "mv21-50"
    if rk <= 100: return "mv51-100"
    return "mv101-200"


def main() -> None:
    agg = load_all()
    all_dates = sorted({d for (d, c) in agg.keys()})
    n_days = len(all_dates)
    print(f"总样本: {len(agg)} unique picks, 跨 {n_days} 日\n")

    results: list[tuple[str, dict, float]] = []   # (label, stats, picks/day)

    def add(label: str, entries: list[dict]):
        s = stats(entries)
        if s is None or s["n"] < 10:
            return
        # 至少 T+5 有 5+ samples
        if (s.get("t5_n") or 0) < 5:
            return
        results.append((label, s, s["n"] / max(1, n_days)))

    # === 单策略 by tier ===
    for strat in ["chip", "gc", "escalator", "sideways", "hot"]:
        for tier in sorted({e["tier"].get(strat, "") for e in agg.values() if strat in e["strategies"]}):
            if not tier:
                continue
            entries = [e for e in agg.values()
                       if e["strategies"] == {strat} and e["tier"].get(strat) == tier]
            add(f"{strat}[{tier}] 单策略", entries)

    # marketcap by rank bucket
    for bucket_lbl in ["mv1-20", "mv21-50", "mv51-100", "mv101-200"]:
        entries = [e for e in agg.values()
                   if e["strategies"] == {"marketcap"} and mv_bucket(e.get("mv_rank")) == bucket_lbl]
        add(f"marketcap[{bucket_lbl}] 单", entries)

    # === 2-way tier × tier ===
    pairs = [
        ("chip", "gc"), ("chip", "escalator"), ("chip", "sideways"),
        ("gc", "escalator"), ("gc", "sideways"),
        ("escalator", "sideways"),
    ]
    for a, b in pairs:
        a_tiers = sorted({e["tier"].get(a, "") for e in agg.values() if a in e["strategies"]} - {""})
        b_tiers = sorted({e["tier"].get(b, "") for e in agg.values() if b in e["strategies"]} - {""})
        for ta in a_tiers:
            for tb in b_tiers:
                entries = [e for e in agg.values()
                           if {a, b}.issubset(e["strategies"])
                           and e["tier"].get(a) == ta and e["tier"].get(b) == tb]
                add(f"{a}[{ta}]+{b}[{tb}]", entries)

    # === tier × marketcap rank ===
    for strat in ["chip", "gc", "escalator", "sideways"]:
        s_tiers = sorted({e["tier"].get(strat, "") for e in agg.values() if strat in e["strategies"]} - {""})
        for ts in s_tiers:
            for bucket_lbl in ["mv1-20", "mv21-50", "mv51-100", "mv101-200"]:
                entries = [e for e in agg.values()
                           if {strat, "marketcap"}.issubset(e["strategies"])
                           and e["tier"].get(strat) == ts
                           and mv_bucket(e.get("mv_rank")) == bucket_lbl]
                add(f"{strat}[{ts}]+market[{bucket_lbl}]", entries)

    # === 3-way ===
    triples = [
        ({"chip", "gc", "marketcap"}, "chip+gc+market"),
        ({"chip", "escalator", "marketcap"}, "chip+escalator+market"),
        ({"chip", "gc", "escalator"}, "chip+gc+escalator"),
        ({"gc", "escalator", "marketcap"}, "gc+escalator+market"),
    ]
    for strats, label in triples:
        for bucket_lbl in ["mv1-20", "mv21-50", "mv51-100", "mv1-50"]:
            def rank_in(rk):
                if bucket_lbl == "mv1-20": return rk and rk <= 20
                if bucket_lbl == "mv21-50": return rk and 21 <= rk <= 50
                if bucket_lbl == "mv51-100": return rk and 51 <= rk <= 100
                if bucket_lbl == "mv1-50": return rk and rk <= 50
                return True
            entries = [e for e in agg.values()
                       if strats.issubset(e["strategies"])
                       and (not "marketcap" in strats or rank_in(e.get("mv_rank")))]
            add(f"{label}[{bucket_lbl}]", entries)

    # Sort: T+10 win desc, then T+5 win desc
    def sort_key(item):
        _, s, _ = item
        t10 = s.get("t10_wr") or -1
        t5 = s.get("t5_wr") or -1
        return (-t10, -t5)
    results.sort(key=sort_key)

    print(f"=== Top 40 组合（按 T+10 win 降序，n ≥ 10）===\n")
    print(f"{'组合':<38}{'n':>5}{'/天':>6}  "
          f"{'T+1':>7}{'avg':>7}  {'T+5':>7}{'avg':>7}  {'T+10':>7}{'avg':>7}")
    print("-" * 110)
    for label, s, per_day in results[:40]:
        def cell(prefix):
            wr = s.get(f"{prefix}_wr")
            avg = s.get(f"{prefix}_avg")
            n = s.get(f"{prefix}_n")
            if wr is None:
                return f"{'-':>7}{'-':>7}"
            return f"{wr:>6.1f}%{avg:>+6.2f}%"
        print(f"{label:<38}{s['n']:>5}{per_day:>5.1f}/d  "
              f"{cell('t1')}  {cell('t5')}  {cell('t10')}")


if __name__ == "__main__":
    main()
