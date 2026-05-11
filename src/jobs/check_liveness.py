#!/usr/bin/env python3
"""
check_liveness.py — 夜跑存活检查，独立于 nightly_scan 的单独定时任务。

检查 data/last_run.json 是否存在且在 24h 内正常完成。
若文件缺失、过期或有失败，发送推送告警。

这是 _alert_failures() 覆盖不到的盲区：
    如果 nightly_scan 本身从未启动（VM 重启、任务调度挂掉、Python 环境损坏），
    nightly_scan 内部的所有告警逻辑都不会运行。
    本脚本由独立任务计划触发，与 nightly_scan 解耦。

Windows 任务计划设置（每日 00:30，即夜跑后约 2h）：
    Action: pythonw -X utf8 C:/path/to/src/jobs/check_liveness.py
    Trigger: 每日 00:30

用法：
    python -X utf8 src/jobs/check_liveness.py
    python -X utf8 src/jobs/check_liveness.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import push_wechat

_ROOT      = Path(__file__).resolve().parent.parent.parent
_LIVE_FILE = _ROOT / "data" / "last_run.json"
_MAX_AGE_H = 25   # 超过 25h 未完成视为异常（允许夜跑最长跑到 23:xx）


def check(dry_run: bool = False) -> bool:
    """
    检查 last_run.json。
    返回 True 表示正常，False 表示有异常并已推送告警。
    """
    now = datetime.now()

    # ── 文件缺失 ─────────────────────────────────────────────────────────────
    if not _LIVE_FILE.exists():
        _alert(
            "⚠️ 夜跑未检测到",
            f"data/last_run.json 不存在。\n"
            f"夜跑可能从未启动（VM 重启？任务调度故障？）\n"
            f"检查时间：{now:%Y-%m-%d %H:%M:%S}",
            dry_run=dry_run,
        )
        return False

    # ── 读取文件 ──────────────────────────────────────────────────────────────
    try:
        data = json.loads(_LIVE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        _alert("⚠️ 夜跑状态文件损坏", f"读取 last_run.json 失败: {e}", dry_run=dry_run)
        return False

    completed_at_str = data.get("completed_at", "")
    trade_date       = data.get("trade_date", "?")
    succeeded        = data.get("strategies_succeeded", 0)
    attempted        = data.get("strategies_attempted", 0)
    failures         = data.get("failures", [])

    # ── 完成时间过期 ──────────────────────────────────────────────────────────
    try:
        completed_at = datetime.strptime(completed_at_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        _alert("⚠️ 夜跑状态文件格式异常",
               f"completed_at 无法解析：{completed_at_str!r}", dry_run=dry_run)
        return False

    age_h = (now - completed_at).total_seconds() / 3600
    if age_h > _MAX_AGE_H:
        _alert(
            "⚠️ 夜跑超时未更新",
            f"最后完成时间：{completed_at_str}（{age_h:.1f}h 前）\n"
            f"交易日：{trade_date}\n"
            f"超过 {_MAX_AGE_H}h 未有新记录，夜跑可能中断。",
            dry_run=dry_run,
        )
        return False

    # ── 有策略失败 ────────────────────────────────────────────────────────────
    if failures:
        _alert(
            f"⚠️ 夜跑部分失败 ({succeeded}/{attempted})",
            f"交易日：{trade_date}\n"
            f"完成时间：{completed_at_str}\n"
            f"失败策略：{', '.join(failures)}",
            dry_run=dry_run,
        )
        return False

    print(f"[check_liveness] OK: {trade_date} {succeeded}/{attempted} 成功，{completed_at_str}")
    return True


def _alert(title: str, body: str, *, dry_run: bool) -> None:
    print(f"[check_liveness] ALERT: {title}")
    push_wechat(title, body, dry_run=dry_run)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    ok = check(dry_run=args.dry_run)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
