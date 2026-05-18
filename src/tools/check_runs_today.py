#!/usr/bin/env python3
"""列出今天所有 run_manifest 任务的最新状态，标出需要重跑的项。"""
from __future__ import annotations
import os, sys
from collections import OrderedDict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "jobs"))
from db import _conn  # noqa: E402

TODAY = datetime.now().strftime("%Y-%m-%d")


def main() -> None:
    print(f"trade_date = {TODAY}\n", flush=True)
    with _conn() as conn:
        cur = conn.execute(
            """SELECT job_name, status, started_at, finished_at, duration_sec, error
                 FROM runs
                 WHERE trade_date = ?
                 ORDER BY job_name, started_at""",
            (TODAY,),
        )
        rows = list(cur.fetchall())

    if not rows:
        print("(no runs today)")
        return

    by_job: "OrderedDict[str, list]" = OrderedDict()
    for r in rows:
        by_job.setdefault(r["job_name"], []).append(r)

    print(f"{'job_name':<42} {'try':>3}  {'last_status':<10}  {'last_at':<19}  {'dur':>6}  hint")
    print("-" * 110)
    rerun: list[str] = []
    for job, attempts in by_job.items():
        last = attempts[-1]
        n = len(attempts)
        status = last["status"]
        ts = (last["finished_at"] or last["started_at"] or "")[:19]
        dur = f"{last['duration_sec']:.0f}s" if last["duration_sec"] else "-"
        if status == "succeeded":
            hint = "ok"
        elif status == "started":
            hint = "still running?"
        elif status == "crashed":
            hint = "RERUN (crashed)"
            rerun.append(job)
        elif status == "failed":
            hint = "RERUN (failed)"
            rerun.append(job)
        else:
            hint = f"? {status}"
        print(f"{job:<42} {n:>3}  {status:<10}  {ts:<19}  {dur:>6}  {hint}")

    if rerun:
        print(f"\nneed rerun ({len(rerun)}):")
        for j in rerun:
            print(f"  - {j}")
    else:
        print("\nall jobs last attempt = succeeded")


if __name__ == "__main__":
    main()
