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
  22:30 daily via main_Night — tushare fina_indicator works from non-CN IPs
"""

import sys
import os
import json
import time
import argparse
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

BATCH_FILE = os.path.join(os.path.dirname(__file__), ".cache", "batch_financials.csv")
RATE_DELAY = 0.35  # seconds between tushare calls
_TS_FIELDS = "ts_code,ann_date,end_date,roe,grossprofit_margin,debt_to_assets,revenue_yoy,netprofit_yoy"


def _get_pro():
    try:
        import tushare as ts
        cfg_path = os.path.join(os.path.dirname(__file__), "..", "alert_config.json")
        with open(cfg_path, encoding="utf-8") as f:
            token = json.load(f).get("tushare", {}).get("token", "")
        if not token:
            return None
        ts.set_token(token)
        return ts.pro_api()
    except Exception:
        return None


def _code_to_ts(code: str) -> str:
    return code + (".SH" if code.startswith("6") else ".SZ")


def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if (f != f) else f
    except Exception:
        return None


def _fetch_one(pro, ts_code: str) -> dict | None:
    df = pro.fina_indicator(ts_code=ts_code, start_date="20220101", fields=_TS_FIELDS)
    if df is None or df.empty:
        return None
    row = df.iloc[0]
    return {
        "roe":            _safe_float(row.get("roe")),
        "gross_margin":   _safe_float(row.get("grossprofit_margin")),
        "debt_ratio":     _safe_float(row.get("debt_to_assets")),
        "revenue_growth": _safe_float(row.get("revenue_yoy")),
        "profit_growth":  _safe_float(row.get("netprofit_yoy")),
    }


def _flush(rows: list[dict], path: str, append: bool) -> None:
    df = pd.DataFrame(rows)
    mode = "a" if append and os.path.exists(path) else "w"
    header = not (append and os.path.exists(path))
    df.to_csv(path, mode=mode, header=header, index=False)


def run_batch(max_stocks: int | None = None, resume: bool = True) -> int:
    """Return number of stocks successfully written."""
    pro = _get_pro()
    if pro is None:
        print("[error] tushare Pro not available — check alert_config.json token", flush=True)
        return 0

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
        print("[error] Cannot fetch market list — EM spot data unavailable")
        sys.exit(1)
    all_codes: list[str] = spot["代码"].astype(str).str.zfill(6).tolist()

    if max_stocks:
        all_codes = all_codes[:max_stocks]

    pending = [c for c in all_codes if c not in already_done]
    total = len(pending)
    print(f"Stocks to process: {total}  (skipping {len(already_done)} already done)")

    if total == 0:
        print("Cache is up to date, nothing to do.")
        return 0

    PROBE_N = 3
    probe_ok = 0
    for probe_code in pending[:PROBE_N]:
        try:
            r = _fetch_one(pro, _code_to_ts(probe_code))
            if r is not None:
                probe_ok += 1
        except Exception:
            pass
        time.sleep(RATE_DELAY)
    if probe_ok == 0:
        print(f"[warn] Network probe failed ({PROBE_N}/{PROBE_N} stocks) — "
              f"tushare fina_indicator unreachable. Skipping batch.", flush=True)
        return 0

    buffer: list[dict] = []
    n_done = 0
    flush_every = 100

    for i, code in enumerate(pending):
        try:
            metrics = _fetch_one(pro, _code_to_ts(code))
            if metrics is None:
                continue
            row = {"code": code, "updated_at": datetime.now().strftime("%Y-%m-%d"), **metrics}
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
