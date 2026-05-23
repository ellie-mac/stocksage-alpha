"""模拟 S/A 级共振组合每天会推多少只票（去重）"""
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

ROOT = Path(__file__).resolve().parent.parent.parent
PICKS_DIR = ROOT / "data" / "backtest"


def _f(s):
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_all():
    agg = {}
    for s in ["chip", "gc", "escalator", "marketcap"]:
        p = PICKS_DIR / f"{s}_picks.csv"
        if not p.exists():
            continue
        for r in csv.DictReader(open(p, encoding="utf-8-sig")):
            key = (r["date"], r["code"])
            entry = agg.setdefault(key, {"strategies": set(), "tier": {}, "mv_rank": None})
            entry["strategies"].add(s)
            entry["tier"][s] = r.get("tier", "")
            if s == "marketcap":
                rk = r.get("mv_rank")
                if rk:
                    try:
                        entry["mv_rank"] = int(rk)
                    except ValueError:
                        pass
    return agg


agg = load_all()

# 定义 S/A 级组合的判定逻辑
def in_S(e):
    """S 级：稳定 + 大样本，推荐组合"""
    s = e["strategies"]
    t = e["tier"]
    rk = e.get("mv_rank")
    # chip[C0/C1] + gc[G2]
    if {"chip", "gc"}.issubset(s) and t.get("chip") in {"C0", "C1"} and t.get("gc") == "G2":
        return True
    # escalator[E0] 单
    if s == {"escalator"} and t.get("escalator") == "E0":
        return True
    # escalator[E1] 单（数量略大）
    # 太多了不算入 S
    return False


def in_A(e):
    """A 级：小样本但精准"""
    s = e["strategies"]
    t = e["tier"]
    rk = e.get("mv_rank")
    # gc[G1]+market[mv1-20]
    if {"gc", "marketcap"}.issubset(s) and t.get("gc") == "G1" and rk and rk <= 20:
        return True
    # gc[G2]+market[mv1-20 or 21-50]
    if {"gc", "marketcap"}.issubset(s) and t.get("gc") == "G2" and rk and rk <= 50:
        return True
    # chip[C1]+gc[G0]
    if {"chip", "gc"}.issubset(s) and t.get("chip") == "C1" and t.get("gc") == "G0":
        return True
    # chip[C0/C1]+market[mv1-20]
    if {"chip", "marketcap"}.issubset(s) and t.get("chip") in {"C0", "C1"} and rk and rk <= 20:
        return True
    # 3-way: chip+gc+market[mv51-100]
    if {"chip", "gc", "marketcap"}.issubset(s) and rk and rk <= 100:
        return True
    return False


# 按 date 聚合 S/A picks
daily = defaultdict(lambda: {"S": set(), "A": set()})
for (d, c), e in agg.items():
    if in_S(e):
        daily[d]["S"].add(c)
    if in_A(e):
        daily[d]["A"].add(c)

dates = sorted(daily.keys())
n_days = len(dates)

s_counts = [len(daily[d]["S"]) for d in dates]
a_counts = [len(daily[d]["A"]) for d in dates]
both = [len(daily[d]["S"] | daily[d]["A"]) for d in dates]

def stats(counts, label):
    if not counts:
        return
    print(f"{label}: total {sum(counts)} picks 跨 {len(counts)} 日")
    print(f"  mean/天: {mean(counts):.2f}")
    print(f"  median/天: {median(counts)}")
    print(f"  max/天: {max(counts)} ({dates[counts.index(max(counts))]})")
    # 分布
    bins = defaultdict(int)
    for c in counts:
        if c == 0: bins["0"] += 1
        elif c <= 2: bins["1-2"] += 1
        elif c <= 5: bins["3-5"] += 1
        elif c <= 10: bins["6-10"] += 1
        else: bins["10+"] += 1
    print(f"  分布: " + " ".join(f"{k}={v}天" for k, v in sorted(bins.items())))

print(f"=== 跨 {n_days} 个交易日的统计 ===\n")
stats(s_counts, "S 级单日 picks (chip[C0/C1]+gc[G2] / escalator[E0])")
print()
stats(a_counts, "A 级单日 picks (各种 mv_rank≤50 共振)")
print()
stats(both, "S+A 合并单日 picks（去重）")

# 显示最近 10 天具体细节
print(f"\n=== 最近 10 天细节 ===")
print(f"  {'date':<10}{'S 级':>5}{'A 级':>5}{'合并':>5}  picks(合并)")
for d in dates[-10:]:
    s = daily[d]["S"]
    a = daily[d]["A"]
    both_set = s | a
    print(f"  {d:<10}{len(s):>5}{len(a):>5}{len(both_set):>5}  " +
          ", ".join(sorted(both_set))[:50])
