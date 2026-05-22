"""Split-half robustness check on marketcap_picks.csv T+5 vs T+10."""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
rows = list(csv.DictReader(open(ROOT / "data/backtest/marketcap_picks.csv", encoding="utf-8-sig")))


def stats(rs, key):
    vals = [float(r[key]) for r in rs if r.get(key)]
    if not vals:
        return None
    n = len(vals)
    wr = sum(1 for v in vals if v > 0) / n * 100
    avg = sum(vals) / n
    return n, wr, avg


dates = sorted({r["date"] for r in rows})
mid = dates[len(dates) // 2]
print(f"split at: {mid} (dates total {len(dates)})")
print(f"{'segment':<14}{'n_t5':>6}{'wr_t5':>9}{'avg_t5':>9}{'n_t10':>7}{'wr_t10':>9}{'avg_t10':>9}")

for label, group in [
    ("first half",  [r for r in rows if r["date"] < mid]),
    ("second half", [r for r in rows if r["date"] >= mid]),
]:
    t5 = stats(group, "ret_t5")
    t10 = stats(group, "ret_t10")
    print(f"{label:<14}{t5[0]:>6}{t5[1]:>8.1f}%{t5[2]:>+8.2f}%{t10[0]:>7}{t10[1]:>8.1f}%{t10[2]:>+8.2f}%")
