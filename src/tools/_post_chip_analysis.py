"""Wait for chip backfill PID to die, then run full analysis chain.

One-shot helper for autonomous overnight execution. Polls the given PID every
60s; once it disappears, runs:
  1. strategy_replay --strategy chip gc escalator marketcap (fresh CSVs)
  2. regime_attach (add as-of regime columns)
  3. resonance (full bucket analysis), saving stdout to data/backtest/full_report.txt

Exits 0 on success.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
LOGS = ROOT / "logs"
LOGS.mkdir(exist_ok=True)
REPORT = ROOT / "data" / "backtest" / "full_report.txt"
REPORT.parent.mkdir(parents=True, exist_ok=True)


def _is_alive(pid: int) -> bool:
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, timeout=15,
        )
        return str(pid) in r.stdout
    except Exception:
        return False


def _run(cmd: list[str], log_path: Path | None = None) -> int:
    print(f"[post_chip] running: {' '.join(cmd)}", flush=True)
    if log_path is None:
        return subprocess.call(cmd, cwd=ROOT)
    with open(log_path, "w", encoding="utf-8") as f:
        return subprocess.call(cmd, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: _post_chip_analysis.py <chip_backfill_pid>", flush=True)
        return 2
    pid = int(sys.argv[1])
    print(f"[post_chip] waiting for PID {pid} to die...", flush=True)
    while _is_alive(pid):
        time.sleep(60)
    print(f"[post_chip] PID {pid} died, starting analysis", flush=True)

    py = sys.executable
    rc = _run(
        [py, "-X", "utf8", "src/backtest/strategy_replay.py",
         "--strategy", "chip", "gc", "escalator", "marketcap",
         "--horizons", "1", "3", "5", "10", "20",
         "--start", "20260101", "--end", "20260521"],
        log_path=LOGS / "chip_replay.log",
    )
    print(f"[post_chip] strategy_replay rc={rc}", flush=True)

    rc = _run(
        [py, "-X", "utf8", "src/backtest/regime_attach.py"],
        log_path=LOGS / "regime_attach.log",
    )
    print(f"[post_chip] regime_attach rc={rc}", flush=True)

    rc = _run(
        [py, "-X", "utf8", "src/backtest/resonance.py"],
        log_path=REPORT,
    )
    print(f"[post_chip] resonance rc={rc} → {REPORT}", flush=True)
    print("[post_chip] done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
