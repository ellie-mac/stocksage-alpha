"""drill into escalator[E0] picks — 看 T+10 +21% 是不是 outlier 拉的"""
import csv
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS = ROOT / "data" / "backtest" / "escalator_picks.csv"

rows = [r for r in csv.DictReader(open(PICKS, encoding="utf-8-sig")) if r.get("tier") == "E0"]
print(f"E0 total picks: {len(rows)}")

def parse(s):
    if s == "" or s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None

# T+10 distribution
t10 = [parse(r.get("ret_t10")) for r in rows]
t10 = [v for v in t10 if v is not None]
print(f"\nT+10 valid samples: {len(t10)}")
if t10:
    print(f"  mean: {statistics.mean(t10):+.2f}%")
    print(f"  median: {statistics.median(t10):+.2f}%")
    print(f"  min: {min(t10):+.2f}%   max: {max(t10):+.2f}%")
    print(f"  stdev: {statistics.stdev(t10) if len(t10)>1 else 0:.2f}%")
    win = sum(1 for v in t10 if v > 0)
    print(f"  win rate: {win}/{len(t10)} = {win/len(t10)*100:.1f}%")
    # outlier sensitivity: remove top 3 and bottom 3
    sorted_t10 = sorted(t10)
    print(f"\n  极端 5%：top 3 = {sorted_t10[-3:]}, bottom 3 = {sorted_t10[:3]}")
    if len(t10) > 6:
        trimmed = sorted_t10[3:-3]
        print(f"  剔除极端 6 个后 mean: {statistics.mean(trimmed):+.2f}% (n={len(trimmed)})")

# 列出 top winners and top losers
print(f"\n=== Top 10 winners by T+10 ===")
sorted_rows = sorted([r for r in rows if parse(r.get("ret_t10")) is not None],
                     key=lambda r: -parse(r.get("ret_t10")))
print(f"  {'date':<10}{'code':<8}{'name':<12}{'T+1':>7}{'T+5':>7}{'T+10':>7}")
for r in sorted_rows[:10]:
    t1 = parse(r.get("ret_t1")) or 0
    t5 = parse(r.get("ret_t5")) or 0
    t10 = parse(r.get("ret_t10"))
    print(f"  {r['date']:<10}{r['code']:<8}{r.get('name','')[:10]:<12}{t1:>+6.2f}%{t5:>+6.2f}%{t10:>+6.2f}%")

print(f"\n=== Bottom 10 losers by T+10 ===")
for r in sorted_rows[-10:]:
    t1 = parse(r.get("ret_t1")) or 0
    t5 = parse(r.get("ret_t5")) or 0
    t10 = parse(r.get("ret_t10"))
    print(f"  {r['date']:<10}{r['code']:<8}{r.get('name','')[:10]:<12}{t1:>+6.2f}%{t5:>+6.2f}%{t10:>+6.2f}%")
