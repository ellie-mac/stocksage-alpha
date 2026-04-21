#!/usr/bin/env python3
"""
Generate main_universe.json — CSI 300 + CSI 500 components (~500 stocks)
suitable for factor_analysis.py --universe, replacing the hardcoded TEST_UNIVERSE.

Usage:
    python scripts/generate_main_universe.py
    python scripts/generate_main_universe.py --out scripts/main_universe.json

Output: scripts/main_universe.json  (list of 6-digit codes)
"""

import argparse
import json
import os
import sys


def generate() -> list[str]:
    import akshare as ak

    codes: set[str] = set()
    for symbol, name in [("000300", "CSI300"), ("000905", "CSI500")]:
        for attempt in range(3):
            try:
                print(f"Fetching {name} components (attempt {attempt + 1}/3)...")
                df = ak.index_stock_cons(symbol=symbol)
                if df is not None and not df.empty:
                    col = next(c for c in df.columns if "\u4ee3\u7801" in c)  # 代码
                    batch = df[col].astype(str).str.strip().str.zfill(6).tolist()
                    codes.update(batch)
                    print(f"  {name}: {len(batch)} stocks")
                    break
            except Exception as e:
                print(f"  Attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    import time
                    time.sleep(3)
        else:
            print(f"[WARN] Could not fetch {name} after 3 attempts, skipping")

    result = sorted(codes)
    print(f"Total after dedup: {len(result)} stocks")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CSI 300+500 universe for factor backtest")
    parser.add_argument("--out", type=str,
                        default=os.path.join(os.path.dirname(__file__), "..", "data", "main_universe.json"),
                        help="Output file path")
    args = parser.parse_args()

    codes = generate()
    if not codes:
        print("[ERROR] No codes fetched")
        sys.exit(1)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(codes, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(codes)} codes -> {args.out}")
    print("Run backtest with:")
    print(f"  python factor_analysis.py --rolling 6 --step 20 --group A "
          f"--universe {os.path.basename(args.out)} --out factor_ic_main.json")
