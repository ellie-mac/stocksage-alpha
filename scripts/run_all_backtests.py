#!/usr/bin/env python3
"""
Orchestrate full backtest suite:
  Phase 1 (parallel): IC backtests for main / smallcap / ETF universes
  Phase 2 (sequential): Portfolio backtests for main / smallcap / ETF strategies

Usage:
    python -X utf8 scripts/run_all_backtests.py
    python -X utf8 scripts/run_all_backtests.py --ic-only
    python -X utf8 scripts/run_all_backtests.py --portfolio-only
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
DATA    = ROOT / "data"
PYTHON  = sys.executable


def _log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def _run_parallel(jobs: list[dict]) -> dict[str, bool]:
    """Launch all jobs in parallel, wait for all to finish. Returns {name: success}."""
    procs: dict[str, subprocess.Popen] = {}
    for job in jobs:
        name     = job["name"]
        cmd      = job["cmd"]
        log_path = job["log"]
        _log(f"START  {name}")
        _log(f"  cmd: {' '.join(str(c) for c in cmd)}")
        _log(f"  log: {log_path}")
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(f"--- {name} started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n\n")
        procs[name] = subprocess.Popen(
            cmd, cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )

    results: dict[str, bool] = {}
    pending = dict(procs)
    while pending:
        for name, proc in list(pending.items()):
            ret = proc.poll()
            if ret is not None:
                ok = ret == 0
                _log(f"{'DONE ' if ok else 'FAIL '} {name}  (rc={ret})")
                results[name] = ok
                del pending[name]
        if pending:
            time.sleep(30)
    return results


def _run_sequential(jobs: list[dict]) -> None:
    """Run jobs one by one, print outcome after each."""
    for job in jobs:
        name     = job["name"]
        cmd      = job["cmd"]
        log_path = job["log"]
        _log(f"START  {name}")
        _log(f"  cmd: {' '.join(str(c) for c in cmd)}")
        _log(f"  log: {log_path}")
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(f"--- {name} started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n\n")
        proc = subprocess.Popen(
            cmd, cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        proc.wait()
        ok = proc.returncode == 0
        _log(f"{'DONE ' if ok else 'FAIL '} {name}  (rc={proc.returncode})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ic-only",        action="store_true")
    parser.add_argument("--portfolio-only", action="store_true")
    args = parser.parse_args()

    # ── Phase 1: IC backtests (parallel) ──────────────────────────────────────
    ic_jobs = [
        {
            "name": "IC-main",
            "cmd": [PYTHON, "-X", "utf8", str(SCRIPTS / "factor_analysis.py"),
                    "--rolling", "6", "--step", "20", "--group", "A",
                    "--universe", str(DATA / "main_universe.json"),
                    "--out", str(DATA / "factor_ic_main.json")],
            "log": str(SCRIPTS / "factor_ic_main.log"),
        },
        {
            "name": "IC-smallcap",
            "cmd": [PYTHON, "-X", "utf8", str(SCRIPTS / "factor_analysis.py"),
                    "--rolling", "6", "--step", "20", "--group", "A",
                    "--universe", str(DATA / "smallcap_universe.json"),
                    "--out", str(DATA / "factor_ic_smallcap.json")],
            "log": str(SCRIPTS / "factor_ic_smallcap.log"),
        },
        {
            "name": "IC-etf",
            "cmd": [PYTHON, "-X", "utf8", str(SCRIPTS / "factor_analysis.py"),
                    "--rolling", "6", "--step", "20", "--group", "A",
                    "--universe", str(DATA / "etf_universe.json"),
                    "--out", str(DATA / "factor_ic_etf.json")],
            "log": str(SCRIPTS / "factor_ic_etf.log"),
        },
    ]

    # ── Phase 2: Portfolio backtests (sequential) ──────────────────────────────
    portfolio_jobs = [
        {
            "name": "BT-main",
            "cmd": [PYTHON, "-X", "utf8", str(SCRIPTS / "backtest.py"),
                    "--periods", "16", "--fwd", "10", "--workers", "6",
                    "--universe", str(DATA / "main_universe.json"),
                    "--out", str(DATA / "backtest_main_16p.json")],
            "log": str(SCRIPTS / "backtest_main_16p.log"),
        },
        {
            "name": "BT-smallcap",
            "cmd": [PYTHON, "-X", "utf8", str(SCRIPTS / "backtest.py"),
                    "--periods", "16", "--fwd", "10", "--workers", "6",
                    "--universe", str(DATA / "smallcap_universe.json"),
                    "--smallcap",
                    "--out", str(DATA / "backtest_smallcap_16p.json")],
            "log": str(SCRIPTS / "backtest_smallcap_16p.log"),
        },
        {
            "name": "BT-etf",
            "cmd": [PYTHON, "-X", "utf8", str(SCRIPTS / "etf_backtest.py"),
                    "--periods", "12", "--fwd", "10", "--workers", "4",
                    "--out", str(DATA / "backtest_etf_12p.json")],
            "log": str(SCRIPTS / "backtest_etf_12p.log"),
        },
    ]

    t0 = time.time()

    if not args.portfolio_only:
        _log("=== Phase 1: IC backtests (3 in parallel) ===")
        _run_parallel(ic_jobs)
        _log(f"IC phase done  ({(time.time()-t0)/60:.0f} min elapsed)")

    if not args.ic_only:
        _log("=== Phase 2: Portfolio backtests (sequential) ===")
        _run_sequential(portfolio_jobs)
        _log(f"Portfolio phase done  ({(time.time()-t0)/60:.0f} min elapsed)")

    _log(f"All done.  Total: {(time.time()-t0)/60:.0f} min")


if __name__ == "__main__":
    main()
