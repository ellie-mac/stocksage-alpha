#!/usr/bin/env python3
"""给所有 backtest picks CSV 标 as-of regime score。

逻辑：拉沪深 300 历史（fetcher.get_market_regime_data 返回 1500 天），
对每个 pick_date D，slice trade_date ≤ D 拿最近 60 天，传给 score_market_regime
得到当日 regime score（1/2/4/6/8/9）。

用法：
  python -X utf8 src/backtest/regime_attach.py            # 给所有策略 CSV 加 regime 列
  python -X utf8 src/backtest/regime_attach.py --dry-run  # 打印不写

输出列名 regime_score（int 1/2/4/6/8/9）和 regime_signal（短描述）。
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

import fetcher
from factors import score_market_regime

PICKS_DIR = ROOT / "data" / "backtest"


def build_regime_lookup() -> dict[str, dict]:
    """date_str → {score, signal}。"""
    df = fetcher.get_market_regime_data()
    if df is None or df.empty:
        print("[regime_attach] 无法拉 CSI300 历史", flush=True)
        return {}
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    print(f"[regime_attach] CSI300 history: {len(df)} days, "
          f"{df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}", flush=True)

    out: dict[str, dict] = {}
    closes = df["close"].to_numpy()
    dates = df["trade_date"].to_numpy()
    for i in range(len(df)):
        if i < 60:
            continue
        # 用上溯 60 天的 close 喂给 score_market_regime
        slice_df = pd.DataFrame({"close": closes[i - 59:i + 1]})
        result = score_market_regime(slice_df)
        out[str(dates[i])] = {
            "score": result.get("score"),
            "signal": result.get("details", {}).get("signal", ""),
        }
    return out


def attach_to_csv(csv_path: Path, lookup: dict[str, dict], dry_run: bool = False) -> dict:
    """读 CSV → 加 regime_score / regime_signal 列 → 重写。返回统计。"""
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8-sig")))
    if not rows:
        return {"n": 0, "matched": 0}

    matched = 0
    for r in rows:
        d = r["date"]
        info = lookup.get(d)
        if info is None:
            r["regime_score"] = ""
            r["regime_signal"] = ""
        else:
            r["regime_score"] = info["score"]
            r["regime_signal"] = info["signal"]
            matched += 1

    if dry_run:
        return {"n": len(rows), "matched": matched}

    # 新列加到末尾
    fieldnames = list(rows[0].keys())
    # 确保 regime 列在末尾（如果之前没加过）
    for col in ["regime_score", "regime_signal"]:
        if col not in fieldnames:
            fieldnames.append(col)
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return {"n": len(rows), "matched": matched}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("[regime_attach] 构建历史 regime lookup...", flush=True)
    lookup = build_regime_lookup()
    if not lookup:
        return 1
    print(f"[regime_attach] lookup 大小: {len(lookup)} 个交易日", flush=True)

    for csv_path in sorted(PICKS_DIR.glob("*_picks.csv")):
        stats = attach_to_csv(csv_path, lookup, dry_run=args.dry_run)
        tag = "[dry-run] " if args.dry_run else ""
        print(f"{tag}{csv_path.name}: {stats['matched']}/{stats['n']} 行匹配 regime", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
