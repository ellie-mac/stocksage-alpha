#!/usr/bin/env python3
"""
收盘批处理 — 15:05 运行一次
1. xhs/writer.py evening（小红书收盘帖）
2. signal_tracker.py（信号后续追踪）
3. daily_perf_log.py --force（收盘胜率统计）
4. auto_tune.py --apply（仅周一：因子权重自动调优）

用法：
    python -X utf8 src/jobs/closing_batch.py
    python -X utf8 src/jobs/closing_batch.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PY   = sys.executable


def _run(label: str, cmd: list[str], timeout: int = 300) -> bool:
    print(f"[closing_batch] {label}...")
    try:
        r = subprocess.run(cmd, timeout=timeout, encoding="utf-8",
                           capture_output=True, text=True)
        out = (r.stdout or "").strip()
        err = (r.stderr or "").strip()
        if r.returncode == 0:
            if out:
                print(f"  {out[-400:]}")
            return True
        else:
            print(f"  FAILED (rc={r.returncode}): {err[:300]}")
            return False
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT after {timeout}s")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    now = datetime.now()
    print(f"[closing_batch] 开始 {now:%Y-%m-%d %H:%M}")

    # 1. XHS evening post
    writer = os.path.join(_ROOT, "xhs", "reporter.py")
    if os.path.exists(writer):
        if args.dry_run:
            print("[closing_batch] dry-run: XHS evening 跳过")
        else:
            _run("XHS evening", [_PY, "-X", "utf8", writer, "evening", "--style", "auto"],
                 timeout=120)
    else:
        print("[closing_batch] xhs/reporter.py 不存在，跳过")

    # 2. signal_tracker.py
    tracker = os.path.join(_ROOT, "src", "tools", "signal_tracker.py")
    if os.path.exists(tracker):
        _run("signal_tracker", [_PY, "-X", "utf8", tracker], timeout=300)
    else:
        print("[closing_batch] signal_tracker.py 不存在，跳过")

    # 3. daily_perf_log.py --force
    perf_log = os.path.join(_ROOT, "src", "jobs", "daily_perf_log.py")
    if os.path.exists(perf_log):
        _run("daily_perf_log", [_PY, "-X", "utf8", perf_log, "--force"], timeout=300)
    else:
        print("[closing_batch] daily_perf_log.py 不存在，跳过")

    # 4. auto_tune.py --apply（周一才跑）
    if now.weekday() == 0:
        auto_tune = os.path.join(_ROOT, "src", "jobs", "auto_tune.py")
        if os.path.exists(auto_tune):
            _run("auto_tune (Monday)", [_PY, "-X", "utf8", auto_tune, "--apply"],
                 timeout=120)
        else:
            print("[closing_batch] auto_tune.py 不存在，跳过")

    print(f"[closing_batch] 完成 {datetime.now():%H:%M}")


if __name__ == "__main__":
    main()
