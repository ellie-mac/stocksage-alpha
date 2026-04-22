#!/usr/bin/env python3
"""
Generate data/universe_main.json — full A-share universe pre-filtered for the main strategy.

Two-stage approach:
  1. Pull full market snapshot via get_spot_em() (~5500 stocks)
  2. Apply basic quality filters to remove unsuitable stocks
  3. Save to data/universe_main.json

Filters applied:
  - Exclude ST / 退市 names
  - Total market cap >= min_cap_yi 亿 (default 20)
  - Price >= min_price 元 (default 2.0)
  - Exclude Beijing exchange stocks (BJ: 8xxxxx / 4xxxxx) — lower liquidity

Usage:
    python scripts/tools/generate_full_universe.py
    python scripts/tools/generate_full_universe.py --min-cap 10 --min-price 1.5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent.parent
SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))

OUT_PATH = ROOT / "data" / "universe_main.json"


def generate(
    min_cap_yi: float = 20.0,
    min_price: float = 2.0,
    exclude_bj: bool = True,
) -> list[str]:
    import pandas as pd
    from common import get_spot_em

    print("Fetching full market snapshot...")
    df = get_spot_em()
    if df is None or df.empty:
        print("[ERROR] Cannot fetch market data")
        sys.exit(1)
    print(f"Full market: {len(df)} stocks")

    col_code  = next((c for c in df.columns if "代码" in c), None)
    col_name  = next((c for c in df.columns if "名称" in c), None)
    col_cap   = next((c for c in df.columns if "总市值" in c), None)
    col_price = next((c for c in df.columns if "最新价" in c), None)

    if not col_code:
        print(f"[ERROR] Cannot find 代码 column. Columns: {df.columns.tolist()}")
        sys.exit(1)

    df = df.copy()
    df["_code"]  = df[col_code].astype(str).str.strip().str.zfill(6)
    df["_price"] = pd.to_numeric(df[col_price], errors="coerce") if col_price else None
    df["_cap"]   = pd.to_numeric(df[col_cap],   errors="coerce") if col_cap   else None

    mask = pd.Series(True, index=df.index)

    # Exclude ST / 退
    if col_name:
        mask &= ~df[col_name].str.contains("ST|退", na=False)

    # Min market cap
    if col_cap:
        mask &= df["_cap"].notna() & (df["_cap"] >= min_cap_yi * 1e8)

    # Min price
    if col_price:
        mask &= df["_price"].notna() & (df["_price"] >= min_price)

    # Exclude Beijing exchange (688xxx is STAR board — keep; 8xxxxx/43xxxx is BJ)
    if exclude_bj:
        mask &= ~df["_code"].str.match(r"^(8[^6]|43)")

    df = df[mask].copy()
    print(f"After filters (cap>={min_cap_yi}亿, price>={min_price}, {'no BJ' if exclude_bj else 'incl BJ'}): {len(df)} stocks")

    codes = df["_code"].tolist()
    return codes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate full A-share universe for main strategy")
    parser.add_argument("--min-cap",   type=float, default=20.0,  help="Min total market cap in 亿元 (default 20)")
    parser.add_argument("--min-price", type=float, default=2.0,   help="Min stock price in 元 (default 2.0)")
    parser.add_argument("--include-bj", action="store_true",      help="Include Beijing exchange stocks")
    parser.add_argument("--out", type=str, default=str(OUT_PATH), help="Output JSON path")
    args = parser.parse_args()

    codes = generate(
        min_cap_yi=args.min_cap,
        min_price=args.min_price,
        exclude_bj=not args.include_bj,
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(codes, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved {len(codes)} codes -> {out}")
