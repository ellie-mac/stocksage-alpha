#!/usr/bin/env python3
"""
收盘批处理 — 15:05 运行一次
1. xhs/writer.py evening（小红书收盘帖）
2. signal_tracker.py（信号后续追踪）
3. auto_tune.py --apply（仅周一：因子权重自动调优）

注：daily_perf_log 由独立 scheduled task daily_PerfLog 16:05 负责，
不在 closing_batch 内调用，避免重复推送。

用法：
    python -X utf8 src/jobs/closing_batch.py
    python -X utf8 src/jobs/closing_batch.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PY   = sys.executable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_manifest import log_run  # noqa: E402


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
    trade_date = now.strftime("%Y-%m-%d")
    print(f"[closing_batch] 开始 {now:%Y-%m-%d %H:%M}")

    # 1. XHS evening post — 已退役（用户不用小红书，且该 reporter 顺带推 "Day N 收盘总结"
    #    微信噪音）。保留 signal_tracker + auto_tune。
    # writer = os.path.join(_ROOT, "src", "report", "reporter.py")
    # if os.path.exists(writer): ...

    # 2. signal_tracker.py
    tracker = os.path.join(_ROOT, "src", "tools", "signal_tracker.py")
    if os.path.exists(tracker):
        t0 = time.monotonic()
        ok = _run("signal_tracker", [_PY, "-X", "utf8", tracker], timeout=900)
        log_run("closing_batch/signal_tracker", trade_date, success=ok,
                duration_sec=round(time.monotonic() - t0, 1))
    else:
        print("[closing_batch] signal_tracker.py 不存在，跳过")

    # daily_perf_log 已移至独立 scheduled task daily_PerfLog（16:05），不在此处调用

    # 3. auto_tune.py --apply（周一才跑）
    if now.weekday() == 0:
        auto_tune = os.path.join(_ROOT, "src", "jobs", "auto_tune.py")
        if os.path.exists(auto_tune):
            t0 = time.monotonic()
            ok = _run("auto_tune (Monday)", [_PY, "-X", "utf8", auto_tune, "--apply"],
                      timeout=120)
            log_run("closing_batch/auto_tune", trade_date, success=ok,
                    duration_sec=round(time.monotonic() - t0, 1))
        else:
            print("[closing_batch] auto_tune.py 不存在，跳过")

    print(f"[closing_batch] 完成 {datetime.now():%H:%M}")


if __name__ == "__main__":
    main()
