#!/usr/bin/env python3
"""
夜间扫描编排器 — 依次运行主策略、小盘策略、ETF策略

用法：
    python -X utf8 scripts/nightly_scan.py
    python -X utf8 scripts/nightly_scan.py --dry-run
    python -X utf8 scripts/nightly_scan.py --only main
    python -X utf8 scripts/nightly_scan.py --only small
    python -X utf8 scripts/nightly_scan.py --only etf

Windows 任务计划示例（每日 22:00）：
    pythonw -X utf8 C:/path/to/scripts/nightly_scan.py
"""
from __future__ import annotations

import argparse
import sys
import os
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def run_main(dry_run: bool) -> None:
    print(f"\n[nightly_scan {_ts()}] === 主策略 ===")
    try:
        import main_strategy
        # patch sys.argv so argparse inside main() sees the right flags
        old_argv = sys.argv
        sys.argv = ["main_strategy.py"] + (["--dry-run"] if dry_run else [])
        try:
            main_strategy.main()
        finally:
            sys.argv = old_argv
    except Exception:
        print(f"[nightly_scan] 主策略异常:\n{traceback.format_exc()}")


def run_small(dry_run: bool) -> None:
    print(f"\n[nightly_scan {_ts()}] === 小盘策略 ===")
    try:
        import small_strategy
        old_argv = sys.argv
        sys.argv = ["small_strategy.py"] + (["--dry-run"] if dry_run else [])
        try:
            small_strategy.main()
        finally:
            sys.argv = old_argv
    except Exception:
        print(f"[nightly_scan] 小盘策略异常:\n{traceback.format_exc()}")


def run_etf(dry_run: bool) -> None:
    print(f"\n[nightly_scan {_ts()}] === ETF策略 ===")
    try:
        import etf_strategy
        old_argv = sys.argv
        sys.argv = ["etf_strategy.py"] + (["--dry-run"] if dry_run else [])
        try:
            etf_strategy.main()
        finally:
            sys.argv = old_argv
    except Exception:
        print(f"[nightly_scan] ETF策略异常:\n{traceback.format_exc()}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--only", choices=["main", "small", "etf"],
        help="只运行指定策略"
    )
    args = parser.parse_args()

    print(f"[nightly_scan {_ts()}] 开始夜间扫描 dry_run={args.dry_run}")

    if args.only == "main":
        run_main(args.dry_run)
    elif args.only == "small":
        run_small(args.dry_run)
    elif args.only == "etf":
        run_etf(args.dry_run)
    else:
        run_main(args.dry_run)
        run_small(args.dry_run)
        run_etf(args.dry_run)

    print(f"\n[nightly_scan {_ts()}] 全部完成")


if __name__ == "__main__":
    main()
