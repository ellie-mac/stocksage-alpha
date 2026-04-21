#!/usr/bin/env python3
"""
Generate smallcap_universe.json — a list of small-cap A-share stock codes
(market cap <= max_cap_yi 亿元) suitable for factor_analysis.py --universe.

Usage:
    python scripts/generate_smallcap_universe.py
    python scripts/generate_smallcap_universe.py --max-cap 30 --n 150

Output: scripts/smallcap_universe.json  (list of 6-digit codes)
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))


def generate(max_cap_yi: float = 50.0, max_n: int = 200, exclude_st: bool = True) -> list[str]:
    """Return up to max_n small-cap codes filtered from EM full-market snapshot."""
    import time
    import pandas as pd
    import akshare as ak

    df = None
    for attempt in range(3):
        try:
            print(f"Fetching market snapshot (attempt {attempt + 1}/3)...")
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                break
        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                time.sleep(5)

    if df is None or df.empty:
        print("[ERROR] Cannot fetch market data after 3 attempts")
        sys.exit(1)

    print(f"Full market: {len(df)} stocks")

    # Normalise column names
    col_cap  = next((c for c in df.columns if "总市值" in c), None)
    col_code = next((c for c in df.columns if "代码" in c), None)
    col_name = next((c for c in df.columns if "名称" in c), None)

    if not col_cap or not col_code:
        print(f"[ERROR] Cannot find 总市值/代码 columns. Available: {df.columns.tolist()}")
        sys.exit(1)

    df = df.copy()
    df["_cap"] = pd.to_numeric(df[col_cap], errors="coerce")
    df["_code"] = df[col_code].astype(str).str.strip().str.zfill(6)

    # Filter
    mask = df["_cap"].notna() & (df["_cap"] > 0) & (df["_cap"] <= max_cap_yi * 1e8)
    if exclude_st and col_name:
        mask &= ~df[col_name].str.contains("ST|退", na=False)
    df = df[mask].copy()
    print(f"After cap filter (<= {max_cap_yi}yi, {'no ST' if exclude_st else 'incl ST'}): {len(df)} stocks")

    # Sort by market cap descending for a more liquid small-cap sample,
    # then take up to max_n
    df = df.sort_values("_cap", ascending=False)
    codes = df["_code"].head(max_n).tolist()
    print(f"Sampled: {len(codes)} stocks")
    return codes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate small-cap universe for factor backtest")
    parser.add_argument("--max-cap", type=float, default=50.0,
                        help="Market cap upper limit in 亿元 (default 50)")
    parser.add_argument("--n",       type=int,   default=200,
                        help="Max stocks to include (default 200)")
    parser.add_argument("--include-st", action="store_true",
                        help="Include ST / 退 stocks (excluded by default)")
    parser.add_argument("--out", type=str,
                        default=os.path.join(os.path.dirname(__file__), "..", "data", "smallcap_universe.json"),
                        help="Output file path")
    args = parser.parse_args()

    codes = generate(max_cap_yi=args.max_cap, max_n=args.n, exclude_st=not args.include_st)

    out_path = args.out
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(codes, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(codes)} codes -> {out_path}")
    print("Run backtest with:")
    print(f"  python factor_analysis.py --rolling 6 --step 20 --group A "
          f"--universe {os.path.basename(out_path)} --out factor_ic_smallcap.json")
