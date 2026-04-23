#!/usr/bin/env python3
"""
Nightly batch job: pre-compute key financial metrics for all A-share stocks.

Output : .cache/batch_financials.csv
Columns: code, roe, gross_margin, debt_ratio, revenue_growth, profit_growth

Usage:
  python batch_financials.py              # full run, resumes if interrupted
  python batch_financials.py --max 200    # process first 200 stocks (testing)
  python batch_financials.py --no-resume  # reprocess everything from scratch

Recommended schedule (Windows Task Scheduler or cron):
  02:00 AM daily  — market is closed, API load is low
"""

import sys
import os
import time
import argparse
from datetime import datetime

import pandas as pd
import akshare as ak

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

BATCH_FILE = os.path.join(os.path.dirname(__file__), ".cache", "batch_financials.csv")
RATE_DELAY = 0.35   # seconds between API calls to avoid rate-limiting

# Map output column name -> list of possible source column names in akshare
METRIC_COLUMNS: dict[str, list[str]] = {
    "roe":            ["净资产收益率(%)", "加权净资产收益率(%)"],
    "gross_margin":   ["销售毛利率(%)", "毛利率(%)"],
    "debt_ratio":     ["资产负债率(%)", "负债率(%)"],
    "revenue_growth": ["营业收入增长率(%)", "营收增长率", "总营收同比增长率(%)"],
    "profit_growth":  ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"],
}


def _extract(df: pd.DataFrame, col_candidates: list[str]) -> float | None:
    """Return the most recent non-null value for the first matching column."""
    for col in col_candidates:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if not vals.empty:
                return float(vals.iloc[0])
    return None


def _flush(rows: list[dict], path: str, append: bool) -> None:
    """Append or write rows to the CSV output file."""
    df = pd.DataFrame(rows)
    mode = "a" if append and os.path.exists(path) else "w"
    header = not (append and os.path.exists(path))
    df.to_csv(path, mode=mode, header=header, index=False)


def run_batch(max_stocks: int | None = None, resume: bool = True) -> int:
    """Return number of stocks successfully written."""
    os.makedirs(os.path.dirname(BATCH_FILE), exist_ok=True)

    # Optionally resume from a previous interrupted run
    already_done: set[str] = set()
    if resume and os.path.exists(BATCH_FILE):
        try:
            done_df = pd.read_csv(BATCH_FILE, dtype={"code": str})
            already_done = set(done_df["code"].tolist())
            print(f"[resume] {len(already_done)} stocks already in cache")
        except Exception:
            pass

    # Full market quote to get the list of all codes
    print("Fetching full market quote list...")
    from common import get_spot_em
    spot = get_spot_em()
    all_codes: list[str] = spot["代码"].astype(str).str.zfill(6).tolist()

    if max_stocks:
        all_codes = all_codes[:max_stocks]

    pending = [c for c in all_codes if c not in already_done]
    total = len(pending)
    print(f"Stocks to process: {total}  (skipping {len(already_done)} already done)")

    buffer: list[dict] = []
    n_done = 0
    flush_every = 100   # write to disk every N stocks

    for i, code in enumerate(pending):
        try:
            df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2022")
            if df is None or df.empty:
                continue

            row: dict = {"code": code, "updated_at": datetime.now().strftime("%Y-%m-%d")}
            for out_col, candidates in METRIC_COLUMNS.items():
                row[out_col] = _extract(df, candidates)
            buffer.append(row)
            n_done += 1

        except Exception as e:
            print(f"  [skip] {code}: {type(e).__name__}: {e}")

        time.sleep(RATE_DELAY)

        if (i + 1) % flush_every == 0:
            _flush(buffer, BATCH_FILE, append=True)
            buffer = []
            pct = (i + 1) / total * 100
            print(f"  {i+1}/{total} ({pct:.1f}%)  [{datetime.now():%H:%M:%S}]")

    if buffer:
        _flush(buffer, BATCH_FILE, append=True)

    print(f"\nDone. {n_done}/{total} stocks written. Output: {BATCH_FILE}")
    return n_done


def load() -> pd.DataFrame | None:
    """
    Load the pre-computed financial snapshot.
    Returns None if the file does not exist yet.
    Callers should gracefully degrade when this returns None.
    """
    if not os.path.exists(BATCH_FILE):
        return None
    try:
        df = pd.read_csv(BATCH_FILE, dtype={"code": str})
        df["code"] = df["code"].str.zfill(6)
        return df.drop_duplicates(subset="code", keep="last").reset_index(drop=True)
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pre-compute financial metrics for all A-share stocks")
    parser.add_argument("--max", type=int, default=None, help="Limit to first N stocks (for testing)")
    parser.add_argument("--no-resume", action="store_true", help="Reprocess all stocks from scratch")
    args = parser.parse_args()

    n_done = run_batch(max_stocks=args.max, resume=not args.no_resume)
    if args.max is None and n_done == 0:
        print("[error] 0 stocks processed — likely network failure", flush=True)
        sys.exit(1)
