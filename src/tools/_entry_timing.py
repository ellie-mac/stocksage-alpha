"""入场时点对比分析

对每个 pick，比较 3 个入场时点（exit 固定到 D+N close 公允比较）：
  1. 当日尾盘买 (D close) — 假设推荐 14:50 提前推
  2. 次日开盘买 (D+1 open) — 当前 backtest 假设
  3. 次日尾盘买 (D+1 close) — 用户偏好

Exit 固定到 D+N close (N=1,2,3,5,7,10,15)，3 个入场退出同一时点。
注意 hold 时长不同：D close 入场最长，D+1 close 最短。但能回答"持有
到第 N 天，哪个入场最赚"。
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

PICKS_DIR = ROOT / "data" / "backtest"
EXIT_NS = [1, 2, 3, 5, 7, 10, 15]
ENTRY_LABELS = ["D close", "D+1 open", "D+1 close"]


def _f(s):
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_picks() -> dict[tuple[str, str], dict]:
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


def _select_combos(agg: dict) -> dict[str, list]:
    out = defaultdict(list)
    for (d, c), e in agg.items():
        s = e["strategies"]
        t = e["tier"]
        rk = e.get("mv_rank")
        if {"chip", "gc"}.issubset(s) and t.get("chip") in {"C0", "C1"} and t.get("gc") == "G2":
            out["chip[C0/C1]+gc[G2]"].append((d, c, e))
        if {"gc", "marketcap"}.issubset(s) and t.get("gc") in {"G1", "G2"} and rk and rk <= 20:
            out["gc[G1/G2]+market[mv1-20]"].append((d, c, e))
        if {"chip", "gc"}.issubset(s) and t.get("chip") == "C1" and t.get("gc") == "G0":
            out["chip[C1]+gc[G0]"].append((d, c, e))
        if s == {"escalator"} and t.get("escalator") == "E0":
            out["escalator[E0] 单"].append((d, c, e))
        if {"chip", "gc", "marketcap"}.issubset(s) and rk and rk <= 50:
            out["3-way+market[mv≤50]"].append((d, c, e))
    return out


def _fetch_entry_exit(code: str, pick_date: str) -> dict:
    """返回 {(entry, exit_n): pct}"""
    import fetcher as _f
    try:
        df = _f.get_price_history(code, days=60)
    except Exception:
        return {}
    if df is None or df.empty or "date" not in df.columns:
        return {}
    pick_ts = pd.to_datetime(pick_date, format="%Y%m%d")
    pre = df[df["date"] <= pick_ts]
    fwd = df[df["date"] > pick_ts].sort_values("date").reset_index(drop=True)
    if pre.empty or fwd.empty:
        return {}
    try:
        d_close = float(pre["close"].iloc[-1])
        d1_open = float(fwd["open"].iloc[0])
        d1_close = float(fwd["close"].iloc[0])
    except Exception:
        return {}
    if not (d_close > 0 and d1_open > 0 and d1_close > 0):
        return {}

    result = {}
    for n in EXIT_NS:
        if len(fwd) < n:
            continue
        exit_close = float(fwd["close"].iloc[n - 1])
        if exit_close <= 0:
            continue
        result[("D close",   n)] = (exit_close / d_close  - 1) * 100
        result[("D+1 open",  n)] = (exit_close / d1_open  - 1) * 100
        # D+1 close 入场，必须 n>=2 才有意义（同时点入场退出 = 0%）
        if n >= 2:
            result[("D+1 close", n)] = (exit_close / d1_close - 1) * 100
    return result


def _hold_days(entry: str, exit_n: int) -> str:
    """估算 hold 时长（close-to-close 等价天数）"""
    if entry == "D close":
        return f"{exit_n}d"
    if entry == "D+1 open":
        return f"~{exit_n-1}d+intra"
    if entry == "D+1 close":
        return f"{exit_n-1}d"
    return "?"


def main():
    print("载入 picks...")
    agg = load_picks()
    combos = _select_combos(agg)
    print(f"S/A 级组合: {len(combos)}")

    # 平铺去重
    flat = set()
    for picks in combos.values():
        for d, c, _ in picks:
            flat.add((d, c))
    print(f"unique (date, code): {len(flat)}, 开始 fetch...", flush=True)

    fetched: dict = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_fetch_entry_exit, c, d): (d, c) for (d, c) in flat}
        done = 0
        for fut in as_completed(futs):
            key = futs[fut]
            fetched[key] = fut.result()
            done += 1
            if done % 100 == 0:
                print(f"  fetched {done}/{len(flat)}", flush=True)

    print("\n" + "=" * 90)
    print("入场时点对比 — exit 固定到 D+N close")
    print("=" * 90)

    for label, picks in combos.items():
        sub = [fetched.get((d, c), {}) for d, c, _ in picks]
        sub = [r for r in sub if r]
        if not sub:
            continue
        n_picks = len(sub)
        # 涨停率（D+1 open vs D close）
        limit_up = sum(1 for r in sub if r.get(("D+1 open", 1)) is not None
                       and (r[("D+1 open", 1)] - r.get(("D+1 close", 2), r[("D+1 open", 1)])) > -100  # placeholder
                       and ((r.get(("D+1 open", 1), 0) is not None)))
        # 用 D+1 open / D close 计算 D+1 涨幅
        gap_pcts = []
        for r in sub:
            # D+1 open pct vs D close = (open_d1 - d_close) / d_close
            # 已经在 D close → D+1 close 里隐含，但更直接的判涨停是看 D+1 open vs D close
            # 我们这里用 D close → D+1 close 中的 open entry 数据反推近似不可行，跳过
            pass

        print(f"\n【{label}】n={n_picks} picks")
        print(f"  {'Exit':<10}{'D close 入场':>22}{'D+1 open 入场':>23}{'D+1 close 入场':>23}{'(hold ~)':<15}")
        for n in EXIT_NS:
            row_parts = []
            for entry in ENTRY_LABELS:
                key = (entry, n)
                rets = [r[key] for r in sub if key in r]
                if rets:
                    wr = sum(1 for v in rets if v > 0) / len(rets) * 100
                    avg = mean(rets)
                    row_parts.append(f"{wr:.1f}%/{avg:+.2f}%(n={len(rets)})")
                else:
                    row_parts.append("--")
            hold_dlabel = f"({n}d, {n-1}d+intra, {n-1}d)" if n >= 2 else f"({n}d, intra, --)"
            print(f"  D+{n:<8}{row_parts[0]:>22}{row_parts[1]:>23}{row_parts[2]:>23}  {hold_dlabel}")


if __name__ == "__main__":
    main()
