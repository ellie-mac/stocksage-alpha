#!/usr/bin/env python3
"""
notify_failure.py — 发送定时任务失败通知（微信），含跨天去重。

程序化调用（被 nightly_scan 使用）：
    from notify.notify_failure import send_failure_alert
    send_failure_alert(failed_runs)   # failed_runs 来自 get_failed_runs()

去重语义：
    - 每个 run_id 只推送一次（通过 alerts_sent 表持久化）
    - push_wechat 成功后才写入 alerts_sent；push 失败则不写，下次仍会重试
    - dry_run=True 时：跳过去重过滤、不写 alerts_sent、不实际推送

CLI 调用（手动或脚本）：
    python -X utf8 src/notify/notify_failure.py "任务名" ["详情"]
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import push_wechat
from db import _conn


def _already_alerted_ids() -> set[int]:
    """从 alerts_sent 表读取已推送过的 run_id 集合。"""
    with _conn() as conn:
        rows = conn.execute("SELECT run_id FROM alerts_sent").fetchall()
    return {r[0] for r in rows}


def _mark_alerted(run_ids: list[int]) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO alerts_sent (run_id, alerted_at) VALUES (?, ?)",
            [(rid, now) for rid in run_ids],
        )


def send_failure_alert(
    failed_runs: list[dict],
    *,
    dry_run: bool = False,
) -> int:
    """
    将失败/crashed 的 run 列表格式化后推送微信通知。
    返回实际推送条数（0 表示全部已推过）。

    去重：通过 alerts_sent 表持久化，同一 run_id 跨天不重复推送。
    dry_run=True 时跳过去重和落库，用于测试。
    """
    if not failed_runs:
        return 0

    if not dry_run:
        sent_ids = _already_alerted_ids()
        new_runs = [r for r in failed_runs if r.get("id") not in sent_ids]
    else:
        new_runs = list(failed_runs)

    if not new_runs:
        return 0

    lines = []
    for r in new_runs:
        status   = r.get("status", "failed")
        job_name = r.get("job_name", "?")
        date     = r.get("trade_date", "?")
        err_snip = (r.get("error") or "")[:120].replace("\n", " ")
        duration = r.get("duration_sec")
        dur_str  = f"  耗时 {duration}s" if duration else ""
        lines.append(f"• [{status}] {job_name} @ {date}{dur_str}")
        if err_snip:
            lines.append(f"  {err_snip}")

    count = len(new_runs)
    title = f"⚠️ 夜跑失败 ({count} 个任务)"
    body  = "\n".join(lines)

    push_wechat(title, body, dry_run=dry_run)

    # push 成功后才落库（push 抛异常则不执行）
    if not dry_run:
        alerted_ids = [r["id"] for r in new_runs if r.get("id")]
        if alerted_ids:
            try:
                _mark_alerted(alerted_ids)
            except Exception as exc:
                # 落库失败不影响本次推送结果，下次可能重复推送（可接受）
                print(f"[notify_failure] 警告：alerts_sent 写入失败（已推送），忽略: {exc}")

    return count


def main() -> None:
    task   = sys.argv[1] if len(sys.argv) > 1 else "未知任务"
    detail = sys.argv[2] if len(sys.argv) > 2 else ""

    title = f"⚠️ 任务失败: {task}"
    body  = f"任务 **{task}** 执行失败，请检查日志并考虑手动重跑。"
    if detail:
        body += f"\n\n{detail}"

    print(f"[notify] 发送失败通知: {title}", flush=True)
    push_wechat(title, body)


if __name__ == "__main__":
    main()
