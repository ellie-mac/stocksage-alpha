#!/usr/bin/env python3
"""
夜间选股推送 — 独立于扫描，从 JSON 读取并推送微信。

与 nightly_scan.py 解耦：扫描保存 JSON（run_manifest 保护），
推送独立读 JSON（push 成功才记 succeeded；失败记 failed 可重跑）。

用法：
    python -X utf8 src/jobs/nightly_push.py --strategy main
    python -X utf8 src/jobs/nightly_push.py --strategy main --dry-run
    python -X utf8 src/jobs/nightly_push.py --strategy main --force   # 强制重推（忽略今日已推记录）
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "strategies"))
sys.path.insert(0, str(ROOT / "src" / "jobs"))

from run_manifest import start_run, finish_run  # noqa: E402


def _load_config() -> dict:
    cfg = ROOT / "alert_config.json"
    return json.loads(cfg.read_text(encoding="utf-8")) if cfg.exists() else {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True, choices=["main", "small", "etf"],
                        help="要推送的策略")
    parser.add_argument("--dry-run", action="store_true", help="打印不推送")
    parser.add_argument("--force",   action="store_true", help="忽略今日已推记录，强制重推")
    args = parser.parse_args()

    trade_date = datetime.now().strftime("%Y-%m-%d")
    job_name   = f"{args.strategy}_push/{args.strategy}_strategy"

    run_id: int | None = None
    if not args.force:
        run_id = start_run(job_name, trade_date)
        if run_id is None:
            print(f"[nightly_push] {args.strategy} 今日已推送，跳过 (--force 可强制重推)")
            sys.exit(0)

    config = _load_config()
    ok = False
    t0 = time.monotonic()
    try:
        if args.strategy == "main":
            import main_strategy
            main_strategy.push_from_json(config, args.dry_run)
        elif args.strategy == "small":
            import small_strategy
            small_strategy.push_from_json(config, args.dry_run)
        elif args.strategy == "etf":
            import etf_strategy
            etf_strategy.push_from_json(config, args.dry_run)
        ok = True
        print(f"[nightly_push] {args.strategy} 推送完成")
    except Exception:
        print(f"[nightly_push] {args.strategy} 推送失败:\n{traceback.format_exc()}")
        ok = False

    duration = round(time.monotonic() - t0, 1)
    if run_id is not None:
        finish_run(run_id, ok, duration_sec=duration)

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
