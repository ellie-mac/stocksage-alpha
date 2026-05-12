#!/usr/bin/env python3
"""
Nightly batch job: pre-compute key financial metrics for all A-share stocks.

Output : .cache/batch_financials.csv
Columns: code, roe, gross_margin, debt_ratio, revenue_growth, profit_growth

Data source: baostock (free, accessible from non-CN IPs)
Runs via main_Night at 22:30 daily.
"""

import sys
import os
import time
import argparse
from datetime import datetime, date

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

BATCH_FILE = os.path.join(os.path.dirname(__file__), ".cache", "batch_financials.csv")
RATE_DELAY = 0.05   # seconds between baostock calls; it's a TCP socket, not HTTP, so fast


def _latest_quarter() -> tuple[int, int]:
    """Return (year, quarter) of the most recently available quarterly report."""
    today = date.today()
    y, m = today.year, today.month
    if m >= 10:
        return y, 3
    elif m >= 8:
        return y, 2
    elif m >= 5:
        return y, 1
    else:
        return y - 1, 4


def _code_to_bs(code: str) -> str:
    return ("sh." if code.startswith("6") else "sz.") + code


def _safe_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return None if (f != f) else f
    except Exception:
        return None


def _flush(rows: list[dict], path: str, append: bool) -> None:
    df = pd.DataFrame(rows)
    mode = "a" if append and os.path.exists(path) else "w"
    header = not (append and os.path.exists(path))
    df.to_csv(path, mode=mode, header=header, index=False)


def run_batch(max_stocks: int | None = None, resume: bool = True) -> int:
    """Return number of stocks successfully written."""
    try:
        import baostock as bs
    except ImportError:
        print("[error] baostock not installed — pip install baostock", flush=True)
        return 0

    lg = bs.login()
    if lg.error_code != "0":
        print(f"[error] baostock login failed: {lg.error_msg}", flush=True)
        return 0
    print("baostock login ok", flush=True)

    os.makedirs(os.path.dirname(BATCH_FILE), exist_ok=True)

    already_done: set[str] = set()
    if resume and os.path.exists(BATCH_FILE):
        try:
            done_df = pd.read_csv(BATCH_FILE, dtype={"code": str})
            already_done = set(done_df["code"].tolist())
            print(f"[resume] {len(already_done)} stocks already in cache")
        except Exception:
            pass

    print("Fetching full market quote list...")
    from common import get_spot_em
    spot = get_spot_em()
    if spot is None or spot.empty or "代码" not in spot.columns:
        print("[error] Cannot fetch market list")
        bs.logout()
        sys.exit(1)
    raw = spot["代码"].astype(str).tolist()
    all_codes = [c.zfill(6) for c in raw if c.isdigit() and len(c) <= 6]

    if max_stocks:
        all_codes = all_codes[:max_stocks]

    pending = [c for c in all_codes if c not in already_done]
    total = len(pending)
    print(f"Stocks to process: {total}  (skipping {len(already_done)} already done)")

    if total == 0:
        print("Cache is up to date, nothing to do.")
        bs.logout()
        return 0

    year, quarter = _latest_quarter()
    print(f"Querying {year}Q{quarter} financial data", flush=True)

    # Probe: try first 3 stocks
    probe_ok = 0
    for probe_code in pending[:3]:
        try:
            rs = bs.query_profit_data(code=_code_to_bs(probe_code), year=year, quarter=quarter)
            data = []
            while rs.error_code == "0" and rs.next():
                data.append(rs.get_row_data())
            if data:
                probe_ok += 1
        except Exception:
            pass
        time.sleep(RATE_DELAY)
    if probe_ok == 0:
        print("[warn] baostock probe failed — no financial data available. Skipping batch.", flush=True)
        bs.logout()
        return 0

    buffer: list[dict] = []
    n_done = 0
    flush_every = 200

    for i, code in enumerate(pending):
        bs_code = _code_to_bs(code)
        try:
            rs_p = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
            p_rows = []
            while rs_p.error_code == "0" and rs_p.next():
                p_rows.append(rs_p.get_row_data())

            rs_g = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
            g_rows = []
            while rs_g.error_code == "0" and rs_g.next():
                g_rows.append(rs_g.get_row_data())

            rs_b = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
            b_rows = []
            while rs_b.error_code == "0" and rs_b.next():
                b_rows.append(rs_b.get_row_data())

            if not p_rows and not g_rows and not b_rows:
                continue

            p = dict(zip(rs_p.fields, p_rows[0])) if p_rows else {}
            g = dict(zip(rs_g.fields, g_rows[0])) if g_rows else {}
            b = dict(zip(rs_b.fields, b_rows[0])) if b_rows else {}

            buffer.append({
                "code":           code,
                "updated_at":     datetime.now().strftime("%Y-%m-%d"),
                "roe":            _safe_float(p.get("roeAvg")),
                "gross_margin":   _safe_float(p.get("gpMargin")),
                "debt_ratio":     _safe_float(b.get("liabilityToAsset")),
                "revenue_growth": None,
                "profit_growth":  _safe_float(g.get("YOYNI")),
            })
            n_done += 1

        except Exception as e:
            print(f"  [skip] {code}: {type(e).__name__}: {e}")

        time.sleep(RATE_DELAY)

        if (i + 1) % flush_every == 0:
            _flush(buffer, BATCH_FILE, append=True)
            buffer = []
            pct = (i + 1) / total * 100
            print(f"  {i+1}/{total} ({pct:.1f}%)  [{datetime.now():%H:%M:%S}]", flush=True)

    if buffer:
        _flush(buffer, BATCH_FILE, append=True)

    bs.logout()
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
