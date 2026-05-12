#!/usr/bin/env python3
"""清除今日 nightly_scan run_manifest 记录，然后立即重跑。"""
import sys, subprocess
from pathlib import Path

ROOT = Path("C:/Users/jiapeichen/repos/stocksage-alpha")
sys.path.insert(0, str(ROOT / "src"))

from db import _conn

date = "2026-05-11"
jobs = ["nightly_scan/main_strategy", "nightly_scan/small_strategy", "nightly_scan/etf_strategy"]

with _conn() as c:
    for job in jobs:
        cur = c.execute(
            "DELETE FROM runs WHERE job_name=? AND trade_date=?",
            (job, date),
        )
        print(f"删除 {job}: {cur.rowcount} 行")

print("run_manifest 清除完成，启动 nightly_scan ...")

log_path = ROOT / "src" / "logs" / "nightly_scan.log"
python = "C:/Program Files/Python313/python.exe"

proc = subprocess.Popen(
    [python, "-X", "utf8", str(ROOT / "src" / "jobs" / "nightly_scan.py")],
    cwd=str(ROOT),
    stdout=open(str(log_path), "a", encoding="utf-8"),
    stderr=subprocess.STDOUT,
    creationflags=0x00000008,  # DETACHED_PROCESS
)
print(f"nightly_scan 已启动，PID={proc.pid}，日志: {log_path}")
print("可用 'tasklist | findstr python' 确认进程在跑")
