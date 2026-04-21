#!/usr/bin/env python3
"""
一次性补跑：为 chip_backtest 回测补全过去6个月的筹码快照。
每隔约20交易日取一个截面，第一次跑完后后续均为缓存命中，速度极快。

用法：
    python -X utf8 scripts/chip_backfill.py
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
PYTHON  = sys.executable
SCAN    = SCRIPTS / "daily_chip_scan.py"

# 约每20交易日一个截面，覆盖过去约6个月
# 避开：国庆(10.1-7)、春节2026(2.17-23)
DATES = [
    "20251010",  # 国庆后第一个周五
    "20251107",  # 11月初
    "20251205",  # 12月初
    "20260109",  # 元旦后，春节前
    "20260227",  # 春节后第一个周五
    "20260327",  # 3月末
    # 20260420 今日已有
]


def _cached(date: str) -> bool:
    return (SCRIPTS / "cache" / "chip" / f"chip_data_ak_{date}.json").exists()


def main() -> None:
    todo = [d for d in DATES if not _cached(d)]
    if not todo:
        print("[backfill] 所有日期均已有缓存，无需补跑")
        return

    print(f"[backfill] 待补跑 {len(todo)} 个截面: {todo}")
    print("[backfill] 第一次约5-10分钟（拉缓存），后续每次约1-2分钟（缓存命中）\n")

    for date in todo:
        print(f"[backfill] ===== {date} =====", flush=True)
        result = subprocess.run(
            [PYTHON, "-X", "utf8", str(SCAN), "--ak", "--no-push", "--date", date],
            cwd=str(ROOT),
        )
        if result.returncode == 0:
            print(f"[backfill] {date} 完成 ✓\n", flush=True)
        else:
            print(f"[backfill] {date} 失败 (exit {result.returncode})\n", flush=True)

    print("[backfill] 全部完成，可运行 chip_backtest.py --step 20 做回测对比")


if __name__ == "__main__":
    main()
