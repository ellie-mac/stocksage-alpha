"""drill into escalator+marketcap picks — what mv_rank are they at?"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS_DIR = ROOT / "data" / "backtest"

agg = {}
for s in ["chip", "escalator", "gc", "hot", "marketcap", "etf"]:
    p = PICKS_DIR / f"{s}_picks.csv"
    if not p.exists():
        continue
    for r in csv.DictReader(open(p, encoding="utf-8-sig")):
        k = (r["date"], r["code"])
        e = agg.setdefault(k, {"strategies": set(), "name": r.get("name", "")})
        e["strategies"].add(s)
        if s == "marketcap":
            if r.get("mv_rank"):
                e["mv_rank"] = int(r["mv_rank"])
            if r.get("mv_yi"):
                e["mv_yi"] = float(r["mv_yi"])
        for h in (1, 3, 5, 10, 20):
            v = r.get(f"ret_t{h}", "")
            if v:
                e[f"ret_t{h}"] = float(v)

# strict escalator∩marketcap, no other strategy
em_pure = [(k, v) for k, v in agg.items() if v["strategies"] == {"escalator", "marketcap"}]
# any pair containing both (even if also hit by other strategies)
em_any = [(k, v) for k, v in agg.items() if {"escalator", "marketcap"}.issubset(v["strategies"])]
print(f"escalator∩marketcap pure: {len(em_pure)}")
print(f"escalator∩marketcap (with possibly more): {len(em_any)}")

print()
print(f"{'date':<10}{'code':<8}{'name':<12}{'strats':<28}{'rank':>5}{'mv_yi':>9}  {'T+1':>6}{'T+5':>7}{'T+10':>7}")
for k, v in sorted(em_any, key=lambda x: (x[0][0], x[1].get('mv_rank') or 999)):
    d, c = k
    strats = "+".join(sorted(v["strategies"]))
    rk = v.get("mv_rank", "-")
    my = v.get("mv_yi", 0)
    t1 = v.get("ret_t1")
    t5 = v.get("ret_t5")
    t10 = v.get("ret_t10")
    t1s = f"{t1:+.2f}%" if t1 is not None else "-"
    t5s = f"{t5:+.2f}%" if t5 is not None else "-"
    t10s = f"{t10:+.2f}%" if t10 is not None else "-"
    print(f"{d:<10}{c:<8}{v.get('name','')[:10]:<12}{strats:<28}{rk:>5}{my:>8.1f}亿  {t1s:>6}{t5s:>7}{t10s:>7}")
