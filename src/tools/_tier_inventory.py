"""每个策略 picks CSV 里的 tier 分布"""
import csv
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS_DIR = ROOT / "data" / "backtest"

for s in ["chip", "gc", "escalator", "sideways", "marketcap", "hot"]:
    p = PICKS_DIR / f"{s}_picks.csv"
    if not p.exists():
        print(f"{s}: NO FILE")
        continue
    rows = list(csv.DictReader(open(p, encoding="utf-8-sig")))
    if not rows:
        print(f"{s}: empty")
        continue
    tiers = Counter(r.get("tier", "") for r in rows)
    print(f"{s} ({len(rows)} total): " + " ".join(f"{t or '∅'}={n}" for t, n in sorted(tiers.items())))
    if s == "marketcap":
        # mv_rank histogram
        ranks = [int(r["mv_rank"]) for r in rows if r.get("mv_rank")]
        if ranks:
            bins = Counter()
            for r in ranks:
                if r <= 20: bins["1-20"] += 1
                elif r <= 50: bins["21-50"] += 1
                elif r <= 100: bins["51-100"] += 1
                else: bins["101-200"] += 1
            print(f"  mv_rank: " + " ".join(f"{k}={v}" for k, v in sorted(bins.items())))
