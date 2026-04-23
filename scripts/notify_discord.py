#!/usr/bin/env python3
"""
notify_discord.py — 发送定时任务完成通知（Discord Webhook）

用法：
    python -X utf8 scripts/notify_discord.py "任务名" "描述"
"""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 与 setup_scheduler.py 中 TASKS 保持一致（仅用于播报剩余任务）
_SCHEDULE = [
    ("chip_Premarket",  "07:00", "筹码盘前兜底"),
    ("main_Morning",    "07:10", "主策略盘前兜底"),
    ("integrity_Check", "08:00", "数据完整性检查"),
    ("concept_Warm",    "08:30", "概念板块预热"),
    ("xhs_Morning",     "09:25", "盘前筹码推送 📱"),
    ("xhs_Midday",      "11:35", "午间筹码推送 📱"),
    ("xhs_Evening",     "15:30", "收盘筹码推送 📱"),
    ("market_Warm",     "15:35", "市场数据预热"),
    ("price_Prefetch",  "15:45", "价格历史预热"),
    ("chip_PerfLog",    "17:15", "筹码胜率对比 📱"),
    ("main_PerfLog",    "17:20", "主策略胜率对比 📱"),
    ("chip_Night",      "18:00", "筹码缓存预取"),
    ("main_Scan",       "18:30", "主策略扫盘 📱"),
    ("chip_CadScan",    "20:30", "筹码扫描推送 📱"),
    ("main_Night",      "22:30", "财务缓存预热"),
]


def _remaining_today(after_name: str) -> str:
    """Return a list of tasks scheduled after the completed one."""
    now = datetime.now().strftime("%H:%M")
    names = [n for n, _, _ in _SCHEDULE]
    try:
        idx = names.index(after_name)
    except ValueError:
        idx = -1

    remaining = [
        f"  `{t}` {n} — {desc}"
        for i, (n, t, desc) in enumerate(_SCHEDULE)
        if i > idx and t > now
    ]
    if not remaining:
        return "今日任务全部完成 🎉"
    return "📋 剩余任务:\n" + "\n".join(remaining)


def send_discord(webhook_url: str, task: str, desc: str, status: str = "") -> None:
    icon = "❌" if status == "failed" else "✅"
    word = "失败" if status == "failed" else "完成"
    lines = [f"{icon} **{task}** {word}"]
    if desc:
        lines.append(desc)
    if status != "failed":
        lines.append(_remaining_today(task))
    content = "\n".join(lines)

    data = json.dumps({"content": content}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "DiscordBot (stocksage-alpha, 1.0)",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        print(f"[notify_discord] 已发送: {task} (HTTP {r.status})", flush=True)


def main() -> None:
    task   = sys.argv[1] if len(sys.argv) > 1 else "未知任务"
    desc   = sys.argv[2] if len(sys.argv) > 2 else ""
    status = sys.argv[3] if len(sys.argv) > 3 else ""

    cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    url = cfg.get("discord", {}).get("webhook_url", "")
    if not url:
        print("[notify_discord] webhook_url 未配置，跳过", flush=True)
        return

    try:
        send_discord(url, task, desc, status)
    except Exception as e:
        print(f"[notify_discord] 发送失败: {e}", flush=True)


if __name__ == "__main__":
    main()
