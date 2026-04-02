#!/usr/bin/env python3
"""
Signal-level backtest — evaluates every buy/sell signal logged in
data/signals_log.json and reports forward return statistics.

What it measures
----------------
Buy signals:  for each signal (code, date, signal_price), fetch actual
  forward returns at 1 / 5 / 10 / 20 trading days.
  Hit rate = fraction where forward_ret > 0.
  Average return, median return, best/worst.

Sell signals: was the sell call right?
  "correct" = price fell below signal_price within 10d.
  Report accuracy, average avoided loss, missed gain.

Breakdowns
----------
  by run_time  (morning / midday / evening)
  by buy_score quartile  (e.g. 50–60, 60–70, 70–80, 80+)
  by regime_score quartile (bullish / neutral / bearish regime)
  factor attribution: which bullish factors appear more often in
    winning signals vs losing signals

Usage
-----
  python scripts/signal_backtest.py               # full report
  python scripts/signal_backtest.py --horizon 10  # only 10d forward
  python scripts/signal_backtest.py --json        # machine-readable JSON
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

import fetcher

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIGNALS_PATH = os.path.join(_ROOT, "data", "signals_log.json")

HORIZONS = [1, 5, 10, 20]   # trading-day forward windows to evaluate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_signals() -> list[dict]:
    try:
        with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _get_forward_return(
    code: str,
    signal_date_str: str,
    signal_price: float,
    horizon: int,
    price_cache: dict,
) -> Optional[float]:
    """
    Return the actual forward return (%) over `horizon` trading days starting
    from the day AFTER signal_date.

    Uses a process-level dict `price_cache` keyed by code to avoid redundant
    fetcher calls when the same code appears multiple times in the log.
    Returns None if we lack sufficient history.
    """
    if code not in price_cache:
        df = fetcher.get_price_history(code, 600)
        price_cache[code] = df
    df = price_cache[code]
    if df is None or df.empty or "date" not in df.columns or "close" not in df.columns:
        return None

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    signal_date = pd.Timestamp(signal_date_str)

    # Find the index of the signal date (or the closest trading day on/before it)
    idx_candidates = df.index[df["date"] <= signal_date]
    if len(idx_candidates) == 0:
        return None
    signal_idx = int(idx_candidates[-1])

    # Forward horizon: signal_idx + horizon
    fwd_idx = signal_idx + horizon
    if fwd_idx >= len(df):
        return None  # not enough history yet (signal too recent)

    fwd_price = float(pd.to_numeric(df["close"].iloc[fwd_idx], errors="coerce"))
    if np.isnan(fwd_price) or fwd_price <= 0:
        return None

    return round((fwd_price / signal_price - 1) * 100, 3)


def _quartile_label(value: Optional[float], breaks: list[float]) -> str:
    """Assign a string bucket label based on sorted breakpoints."""
    if value is None:
        return "unknown"
    for i, b in enumerate(breaks):
        if value < b:
            return f"<{b:.0f}"
    return f"≥{breaks[-1]:.0f}"


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyse_buy_signals(
    entries: list[dict],
    horizons: list[int],
    price_cache: dict,
) -> dict:
    """
    Compute forward-return statistics for all buy signals.

    Returns a nested dict:
      {
        "overall": {horizon: {n, hit_rate, mean_ret, median_ret, ...}},
        "by_run_time": {morning/midday/evening: {horizon: {...}}},
        "by_score_bucket": {label: {horizon: {...}}},
        "by_regime_bucket": {label: {horizon: {...}}},
        "factor_attribution": {factor_name: {win_count, lose_count, lift}},
        "raw": [list of per-signal dicts with forward returns]
      }
    """
    raw: list[dict] = []

    for entry in entries:
        run_time     = entry.get("run_time", "unknown")
        date_str     = entry.get("date", "")
        regime_score = entry.get("regime_score")

        for sig in entry.get("buy_signals", []):
            code   = sig.get("code", "")
            name   = sig.get("name", code)
            sp     = sig.get("signal_price")
            if not code or not sp or not date_str:
                continue
            sp = float(sp)

            fwd_rets = {}
            for h in horizons:
                fwd_rets[h] = _get_forward_return(code, date_str, sp, h, price_cache)

            raw.append({
                "code":         code,
                "name":         name,
                "date":         date_str,
                "run_time":     run_time,
                "buy_score":    sig.get("buy_score"),
                "regime_score": regime_score,
                "bullish":      sig.get("bullish", []),
                "bearish":      sig.get("bearish", []),
                "fwd_rets":     fwd_rets,
            })

    if not raw:
        return {"n_signals": 0, "note": "No buy signals with computable forward returns yet."}

    def _stats(rets: list[float]) -> dict:
        if not rets:
            return {"n": 0}
        arr = np.array(rets)
        return {
            "n":          len(arr),
            "hit_rate":   round(float(np.mean(arr > 0)) * 100, 1),
            "mean_ret":   round(float(np.mean(arr)), 3),
            "median_ret": round(float(np.median(arr)), 3),
            "best":       round(float(np.max(arr)), 3),
            "worst":      round(float(np.min(arr)), 3),
        }

    def _group_stats(rows: list[dict]) -> dict:
        result = {}
        for h in horizons:
            rets = [r["fwd_rets"][h] for r in rows if r["fwd_rets"].get(h) is not None]
            result[f"{h}d"] = _stats(rets)
        return result

    # ── Overall ──────────────────────────────────────────────────────────────
    overall = _group_stats(raw)

    # ── By run_time ──────────────────────────────────────────────────────────
    by_run_time: dict = {}
    for rt in ("morning", "midday", "evening"):
        subset = [r for r in raw if r["run_time"] == rt]
        if subset:
            by_run_time[rt] = _group_stats(subset)

    # ── By buy_score bucket ──────────────────────────────────────────────────
    score_breaks = [55, 65, 75]
    by_score: dict = {}
    for row in raw:
        label = _quartile_label(row.get("buy_score"), score_breaks)
        by_score.setdefault(label, []).append(row)
    by_score_stats = {k: _group_stats(v) for k, v in by_score.items()}

    # ── By regime_score bucket ───────────────────────────────────────────────
    regime_breaks = [4, 6, 8]
    by_regime: dict = {}
    for row in raw:
        label = _quartile_label(row.get("regime_score"), regime_breaks)
        by_regime.setdefault(label, []).append(row)
    by_regime_stats = {k: _group_stats(v) for k, v in by_regime.items()}

    # ── Factor attribution (20d or longest available horizon) ────────────────
    target_h = max(h for h in horizons)
    factor_counts: dict[str, dict] = defaultdict(lambda: {"win": 0, "lose": 0})

    for row in raw:
        fwd = row["fwd_rets"].get(target_h)
        if fwd is None:
            continue
        outcome = "win" if fwd > 0 else "lose"
        for fac_str in row.get("bullish", []):
            # bullish entries are strings like "momentum: +0.8 (price_inertia=8.2)"
            fac = fac_str.split(":")[0].strip()
            factor_counts[fac][outcome] += 1

    attribution = {}
    for fac, counts in factor_counts.items():
        total = counts["win"] + counts["lose"]
        if total < 2:
            continue
        win_rate = counts["win"] / total
        attribution[fac] = {
            "win_count":  counts["win"],
            "lose_count": counts["lose"],
            "win_rate":   round(win_rate * 100, 1),
        }
    attribution = dict(
        sorted(attribution.items(), key=lambda x: x[1]["win_rate"], reverse=True)
    )

    return {
        "n_signals":        len(raw),
        "overall":          overall,
        "by_run_time":      by_run_time,
        "by_score_bucket":  by_score_stats,
        "by_regime_bucket": by_regime_stats,
        "factor_attribution": attribution,
        "raw":              raw,
    }


def analyse_sell_signals(
    entries: list[dict],
    price_cache: dict,
) -> dict:
    """
    Evaluate sell signals: was selling the right call?
    'Correct' = price dropped below signal_price within 10 trading days.
    """
    raw: list[dict] = []

    for entry in entries:
        date_str = entry.get("date", "")
        for sig in entry.get("sell_signals", []):
            code = sig.get("code", "")
            sp   = sig.get("signal_price")
            if not code or not sp or not date_str:
                continue
            sp = float(sp)

            fwd_10 = _get_forward_return(code, date_str, sp, 10, price_cache)
            fwd_20 = _get_forward_return(code, date_str, sp, 20, price_cache)
            correct_10 = (fwd_10 is not None and fwd_10 < 0)

            raw.append({
                "code":       code,
                "name":       sig.get("name", code),
                "date":       date_str,
                "sell_score": sig.get("sell_score"),
                "pnl_pct":    sig.get("pnl_pct"),
                "cost_price": sig.get("cost_price"),
                "fwd_10":     fwd_10,
                "fwd_20":     fwd_20,
                "correct_10": correct_10,
                "reasons":    sig.get("reasons", []),
            })

    if not raw:
        return {"n_signals": 0, "note": "No sell signals with computable forward returns yet."}

    evaluated = [r for r in raw if r["fwd_10"] is not None]
    if not evaluated:
        return {"n_signals": len(raw), "note": "Signals too recent to evaluate."}

    accuracy_10 = round(np.mean([r["correct_10"] for r in evaluated]) * 100, 1)
    fwd10_vals  = [r["fwd_10"] for r in evaluated if r["fwd_10"] is not None]
    fwd20_vals  = [r["fwd_20"] for r in evaluated if r["fwd_20"] is not None]

    return {
        "n_signals":     len(raw),
        "n_evaluated":   len(evaluated),
        "accuracy_10d":  accuracy_10,   # % where price fell after sell
        "mean_fwd10":    round(float(np.mean(fwd10_vals)), 3) if fwd10_vals else None,
        "mean_fwd20":    round(float(np.mean(fwd20_vals)), 3) if fwd20_vals else None,
        "raw":           raw,
    }


# ---------------------------------------------------------------------------
# Formatting / printing
# ---------------------------------------------------------------------------

def _print_horizon_table(stats_by_horizon: dict, indent: str = "  ") -> None:
    """Print a row of horizon stats."""
    headers = ["Horizon", "N", "HitRate", "Mean%", "Median%", "Best%", "Worst%"]
    print(f"{indent}{'':>8}", "  ".join(f"{h:>8}" for h in headers[1:]))
    for label, st in stats_by_horizon.items():
        if not st or st.get("n", 0) == 0:
            continue
        row = [
            f"{st['n']:>8}",
            f"{st.get('hit_rate', 0):>7.1f}%",
            f"{st.get('mean_ret', 0):>+7.3f}%",
            f"{st.get('median_ret', 0):>+7.3f}%",
            f"{st.get('best', 0):>+7.3f}%",
            f"{st.get('worst', 0):>+7.3f}%",
        ]
        print(f"{indent}{label:>8}  " + "  ".join(row))


def _print_report(buy: dict, sell: dict) -> None:
    sep = "=" * 70
    thin = "-" * 70

    print(f"\n{sep}")
    print("SIGNAL BACKTEST REPORT")
    print(sep)

    # ── Buy signals ──────────────────────────────────────────────────────────
    print(f"\n{'BUY SIGNALS':}")
    print(thin)

    if buy.get("n_signals", 0) == 0:
        print(f"  {buy.get('note', 'No data.')}")
    else:
        print(f"  Total signals: {buy['n_signals']}")
        print()
        print("  ── Overall forward returns ──")
        _print_horizon_table(buy["overall"])

        if buy.get("by_run_time"):
            print("\n  ── By session ──")
            for rt, stats in buy["by_run_time"].items():
                print(f"    {rt}:")
                _print_horizon_table(stats, indent="      ")

        if buy.get("by_score_bucket"):
            print("\n  ── By buy_score bucket ──")
            for bucket, stats in sorted(buy["by_score_bucket"].items()):
                print(f"    score {bucket}:")
                _print_horizon_table(stats, indent="      ")

        if buy.get("by_regime_bucket"):
            print("\n  ── By regime_score bucket ──")
            for bucket, stats in sorted(buy["by_regime_bucket"].items()):
                print(f"    regime {bucket}:")
                _print_horizon_table(stats, indent="      ")

        if buy.get("factor_attribution"):
            print("\n  ── Bullish factor win rates (% of signals that ended +ve at 20d) ──")
            print(f"  {'Factor':<30} {'Wins':>5} {'Losses':>6} {'WinRate':>8}")
            print("  " + "-" * 52)
            for fac, d in list(buy["factor_attribution"].items())[:15]:
                print(f"  {fac:<30} {d['win_count']:>5} {d['lose_count']:>6} {d['win_rate']:>7.1f}%")

    # ── Sell signals ─────────────────────────────────────────────────────────
    print(f"\n{'SELL SIGNALS':}")
    print(thin)

    if sell.get("n_signals", 0) == 0:
        print(f"  {sell.get('note', 'No data.')}")
    else:
        print(f"  Total signals: {sell['n_signals']}  |  Evaluated (10d): {sell.get('n_evaluated', 0)}")
        if sell.get("accuracy_10d") is not None:
            print(f"  Accuracy (price fell within 10d):  {sell['accuracy_10d']:.1f}%")
        if sell.get("mean_fwd10") is not None:
            print(f"  Mean forward return 10d:  {sell['mean_fwd10']:+.3f}%  "
                  f"(negative = sell was right)")
        if sell.get("mean_fwd20") is not None:
            print(f"  Mean forward return 20d:  {sell['mean_fwd20']:+.3f}%")

    print()


# ---------------------------------------------------------------------------
# Adjustment suggestions
# ---------------------------------------------------------------------------

def _weight_suggestions(buy: dict) -> None:
    """
    Print simple weight adjustment hints based on factor attribution.
    Only suggests changes when the win-rate difference is meaningful (>10pp)
    and there are at least 5 signal instances to avoid noise.
    """
    attr = buy.get("factor_attribution", {})
    if not attr:
        return

    print("WEIGHT ADJUSTMENT HINTS (based on signal history)")
    print("-" * 70)
    print("  Rule: factors with win_rate > 60% may deserve a weight bump;")
    print("        factors with win_rate < 40% may need reduction or exclusion.")
    print("  Caveat: signal_log is still small — treat as directional, not definitive.\n")

    boosted, cut = [], []
    for fac, d in attr.items():
        total = d["win_count"] + d["lose_count"]
        if total < 5:
            continue
        if d["win_rate"] > 60:
            boosted.append((fac, d["win_rate"], total))
        elif d["win_rate"] < 40:
            cut.append((fac, d["win_rate"], total))

    if boosted:
        print("  Potentially boost weight:")
        for fac, wr, n in sorted(boosted, key=lambda x: -x[1]):
            print(f"    {fac:<30}  win_rate={wr:.1f}%  (n={n})")
    else:
        print("  No factors with win_rate > 60% and n ≥ 5 yet.")

    if cut:
        print("  Potentially reduce weight:")
        for fac, wr, n in sorted(cut, key=lambda x: x[1]):
            print(f"    {fac:<30}  win_rate={wr:.1f}%  (n={n})")
    else:
        print("  No factors with win_rate < 40% and n ≥ 5 yet.")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest buy/sell signals from signals_log.json")
    parser.add_argument("--horizon", type=int, default=0,
                        help="Evaluate only this forward horizon in days (0 = all)")
    parser.add_argument("--json",    action="store_true",
                        help="Output machine-readable JSON instead of text report")
    parser.add_argument("--no-raw",  action="store_true",
                        help="Omit per-signal raw rows from JSON output")
    args = parser.parse_args()

    horizons = [args.horizon] if args.horizon > 0 else HORIZONS

    entries = _load_signals()
    if not entries:
        print(f"signals_log.json is empty or missing at:\n  {SIGNALS_PATH}\n"
              f"\nSignals are recorded automatically each time monitor.py runs.\n"
              f"Run monitor.py a few times to start accumulating signal history.")
        return

    print(f"Loaded {len(entries)} run entries from signals_log.json")
    n_buy  = sum(len(e.get("buy_signals",  [])) for e in entries)
    n_sell = sum(len(e.get("sell_signals", [])) for e in entries)
    print(f"  buy signals: {n_buy}  |  sell signals: {n_sell}")
    print("Fetching price history for forward return computation...")

    price_cache: dict = {}
    buy  = analyse_buy_signals(entries, horizons, price_cache)
    sell = analyse_sell_signals(entries, price_cache)

    if args.json:
        output = {"buy": buy, "sell": sell}
        if args.no_raw:
            output["buy"].pop("raw", None)
            output["sell"].pop("raw", None)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    _print_report(buy, sell)
    _weight_suggestions(buy)


if __name__ == "__main__":
    main()
