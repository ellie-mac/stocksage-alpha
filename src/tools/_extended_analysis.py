"""3 个补充分析：时间稳定性 + 细化 forward 曲线 + 涨停/尾盘买入对比"""
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
EXTENDED_HORIZONS = [1, 2, 3, 4, 5, 7, 10, 15]
LIMIT_PCT = 9.5  # 涨停判定阈值


def _f(s):
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_picks() -> dict[tuple[str, str], dict]:
    """读所有策略 picks.csv，按 (date, code) 聚合"""
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


def _select_top_combos(agg: dict) -> dict[str, list]:
    """挑 S/A 级组合的 picks，分桶返回"""
    out = defaultdict(list)
    for (d, c), e in agg.items():
        s = e["strategies"]
        t = e["tier"]
        rk = e.get("mv_rank")
        # chip[C0/C1]+gc[G2] (S级大样本)
        if {"chip", "gc"}.issubset(s) and t.get("chip") in {"C0", "C1"} and t.get("gc") == "G2":
            out["chip[C0/C1]+gc[G2]"].append((d, c, e))
        # gc[G1/G2]+market[mv≤20] (A级短线)
        if {"gc", "marketcap"}.issubset(s) and t.get("gc") in {"G1", "G2"} and rk and rk <= 20:
            out["gc[G1/G2]+market[mv1-20]"].append((d, c, e))
        # chip[C1]+gc[G0] (A级 T+10 王炸)
        if {"chip", "gc"}.issubset(s) and t.get("chip") == "C1" and t.get("gc") == "G0":
            out["chip[C1]+gc[G0]"].append((d, c, e))
        # escalator[E0] 单
        if s == {"escalator"} and t.get("escalator") == "E0":
            out["escalator[E0] 单"].append((d, c, e))
        # 3-way + mv≤50
        if {"chip", "gc", "marketcap"}.issubset(s) and rk and rk <= 50:
            out["3-way+market[mv≤50]"].append((d, c, e))
    return out


# ── (1) 时间稳定性 ──────────────────────────────────────────────────────────────

def time_stability(combos: dict[str, list]) -> None:
    """每个组合按 pick_date 切成 3 等份，对比 T+10 win rate"""
    print("=" * 80)
    print("【1】时间稳定性 — 80 天窗口切 3 段，看 alpha 是否衰减")
    print("=" * 80)
    print(f"\n{'组合':<32}{'window 1 (早)':>20}{'window 2 (中)':>20}{'window 3 (近)':>20}")
    # 需要重新读 picks csv 拿到 ret_t10 字段
    chip_rows = list(csv.DictReader(open(PICKS_DIR / "chip_picks.csv", encoding="utf-8-sig")))
    gc_rows = list(csv.DictReader(open(PICKS_DIR / "gc_picks.csv", encoding="utf-8-sig")))
    esc_rows = list(csv.DictReader(open(PICKS_DIR / "escalator_picks.csv", encoding="utf-8-sig")))
    mc_rows = list(csv.DictReader(open(PICKS_DIR / "marketcap_picks.csv", encoding="utf-8-sig")))
    # 建 (date, code) → ret_t10
    ret_t10 = {}
    for src in [chip_rows, gc_rows, esc_rows, mc_rows]:
        for r in src:
            v = _f(r.get("ret_t10"))
            if v is not None:
                ret_t10[(r["date"], r["code"])] = v

    for label, picks in combos.items():
        dates = sorted({d for d, _, _ in picks})
        if len(dates) < 6:
            continue
        third = len(dates) // 3
        bins = [dates[:third], dates[third:2*third], dates[2*third:]]
        win_rates = []
        for bin_dates in bins:
            bin_set = set(bin_dates)
            bin_picks = [(d, c, e) for d, c, e in picks if d in bin_set]
            rets = [ret_t10.get((d, c)) for d, c, e in bin_picks]
            rets = [r for r in rets if r is not None]
            if rets:
                wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                win_rates.append(f"{wr:.1f}%(n={len(rets)})")
            else:
                win_rates.append("--")
        print(f"{label:<32}{win_rates[0]:>20}{win_rates[1]:>20}{win_rates[2]:>20}")


# ── (4) Forward 曲线细化 + (6) 涨停 / 尾盘买入对比 ──────────────────────────────

def _fetch_extended_returns(code: str, pick_date: str) -> dict:
    """返回 dict 含：
      open_pct_t1:   D+1 vs D 的涨跌幅（判涨停用）
      open_entry_t1, t2, ..., t15:  D+1 open 入场 → T+N close 收益
      close_entry_t1, t2, ..., t15: D+1 close 入场 → T+N close 收益
    """
    import fetcher as _f
    try:
        df = _f.get_price_history(code, days=60)
    except Exception:
        return {}
    if df is None or df.empty or "date" not in df.columns:
        return {}
    pick_ts = pd.to_datetime(pick_date, format="%Y%m%d")
    pre = df[df["date"] <= pick_ts]
    if pre.empty:
        return {}
    fwd = df[df["date"] > pick_ts].sort_values("date").reset_index(drop=True)
    if fwd.empty:
        return {}
    d_close = float(pre["close"].iloc[-1])
    if d_close <= 0:
        return {}

    result = {}
    # D+1 vs D 涨跌幅（判涨停）
    open_d1 = float(fwd["open"].iloc[0]) if "open" in fwd.columns else None
    close_d1 = float(fwd["close"].iloc[0])
    if open_d1 is not None and d_close > 0:
        result["open_pct_t1"] = (open_d1 / d_close - 1) * 100
    if d_close > 0:
        result["close_pct_t1"] = (close_d1 / d_close - 1) * 100

    # D+1 open 入场（current backtest 假设）
    if open_d1 is not None and open_d1 > 0:
        for n in EXTENDED_HORIZONS:
            if len(fwd) >= n:
                target_close = float(fwd["close"].iloc[n - 1])
                result[f"open_entry_t{n}"] = (target_close / open_d1 - 1) * 100

    # D+1 close 入场（用户尾盘买入方案）
    # 入场是 D+1 收盘，T+N 是入场后第 N 个交易日的收盘
    # → 即 fwd["close"].iloc[n]  (n 是从 D+1 算 0 day 后第 n 天)
    if close_d1 > 0:
        for n in EXTENDED_HORIZONS:
            if len(fwd) > n:
                target_close = float(fwd["close"].iloc[n])
                result[f"close_entry_t{n}"] = (target_close / close_d1 - 1) * 100
    return result


def forward_curve_and_limit(combos: dict[str, list]) -> None:
    """对每个组合，拉 D+1 涨停率 + 细化 forward 曲线 + 尾盘入场对比"""
    print("\n" + "=" * 80)
    print("【4】Forward 曲线细化 + 【6】涨停 + 尾盘买入对比")
    print("=" * 80)

    # 把所有 picks 平铺，去重 (date, code)
    flat = set()
    for label, picks in combos.items():
        for d, c, e in picks:
            flat.add((d, c))
    print(f"\n需要拉 {len(flat)} 个 unique (date, code) 的 extended forward returns...", flush=True)

    fetched: dict[tuple[str, str], dict] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_extended_returns, c, d): (d, c) for (d, c) in flat}
        done = 0
        for fut in as_completed(futs):
            key = futs[fut]
            fetched[key] = fut.result()
            done += 1
            if done % 100 == 0:
                print(f"  fetched {done}/{len(flat)}", flush=True)

    # 每组合统计
    for label, picks in combos.items():
        sub = [fetched.get((d, c), {}) for d, c, _ in picks]
        sub = [r for r in sub if r]
        if not sub:
            continue
        n = len(sub)

        # 涨停率（D+1 open 较 D 涨 ≥ 9.5%）
        limit_up = sum(1 for r in sub if r.get("open_pct_t1") is not None
                       and r["open_pct_t1"] >= LIMIT_PCT)
        limit_pct = limit_up / n * 100

        print(f"\n【{label}】n={n}, D+1 涨停 {limit_up} 只 ({limit_pct:.1f}%) 买不进")

        # 入场方式 × horizon win rate
        print(f"  {'horizon':<10}{'open 入场 win/avg':>25}{'close 入场 win/avg':>25}")
        for h in EXTENDED_HORIZONS:
            # open entry stats
            open_rets = [r.get(f"open_entry_t{h}") for r in sub]
            open_rets = [v for v in open_rets if v is not None]
            close_rets = [r.get(f"close_entry_t{h}") for r in sub]
            close_rets = [v for v in close_rets if v is not None]
            def _wr_avg(rs):
                if not rs:
                    return "--"
                wr = sum(1 for v in rs if v > 0) / len(rs) * 100
                avg = mean(rs)
                return f"{wr:.1f}% / {avg:+.2f}% (n={len(rs)})"
            print(f"  T+{h:<8}{_wr_avg(open_rets):>25}{_wr_avg(close_rets):>25}")


def main():
    print("载入 picks CSV...")
    agg = load_picks()
    print(f"总 unique picks: {len(agg)}")
    combos = _select_top_combos(agg)
    print(f"S/A 级组合: {len(combos)}")
    for label, picks in combos.items():
        print(f"  {label}: {len(picks)} picks")
    print()

    time_stability(combos)
    forward_curve_and_limit(combos)


if __name__ == "__main__":
    main()
