"""按 chip tier × regime 拆桶看 alpha 分布 + 每日 pick 数量。

直接读 chip_picks.csv，按 tier 分组，统计：
  - 每个 tier 每天平均多少只票（实战可买量）
  - 每个 tier × regime 的 T+1/T+5/T+10 胜率
"""
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS = ROOT / "data" / "backtest" / "chip_picks.csv"


def _f(s):
    if s == "" or s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None


rows = list(csv.DictReader(open(PICKS, encoding="utf-8-sig")))
print(f"total chip picks: {len(rows)}")


# 每个 tier 每天平均多少只
tier_daily = defaultdict(lambda: defaultdict(int))
for r in rows:
    tier_daily[r["tier"]][r["date"]] += 1

print(f"\n{'tier':<6}{'dates':>8}{'mean/day':>10}{'median/day':>12}{'max/day':>9}")
for tier in sorted(tier_daily.keys()):
    counts = list(tier_daily[tier].values())
    counts.sort()
    med = counts[len(counts) // 2]
    print(f"{tier:<6}{len(counts):>8}{mean(counts):>10.1f}{med:>12}{max(counts):>9}")


def _regime_bucket(score):
    if score is None:
        return "unknown"
    if score >= 7:
        return "bull"
    if score >= 5:
        return "neutral+"
    if score >= 3:
        return "caution"
    return "bear"


print(f"\n按 (tier, regime) 拆桶：")
print(f"  {'tier':<6}{'regime':<12}{'n':>8}", end="")
for h in (1, 3, 5, 10, 20):
    print(f"  {'T+'+str(h)+'_win':>10}{'T+'+str(h)+'_avg':>10}{'T+'+str(h)+'_n':>7}", end="")
print()

buckets = defaultdict(list)
for r in rows:
    score = _f(r.get("regime_score"))
    key = (r["tier"], _regime_bucket(score))
    buckets[key].append(r)

for tier in sorted({r["tier"] for r in rows}):
    for regime in ["bull", "neutral+", "caution", "bear", "unknown"]:
        entries = buckets.get((tier, regime), [])
        if not entries:
            continue
        print(f"  {tier:<6}{regime:<12}{len(entries):>8}", end="")
        for h in (1, 3, 5, 10, 20):
            rets = [_f(e.get(f"ret_t{h}")) for e in entries]
            rets = [r for r in rets if r is not None]
            if rets:
                wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                avg = mean(rets)
                print(f"  {wr:>9.1f}% {avg:>+9.2f}% {len(rets):>7}", end="")
            else:
                print(f"  {'-':>10}{'-':>10}{0:>7}", end="")
        print()
