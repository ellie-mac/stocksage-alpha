#!/usr/bin/env python3
"""
notify_failure.py — 发送定时任务失败通知（微信）

用法：
    python -X utf8 scripts/notify_failure.py "任务名" ["详情"]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))

from common import configure_pushplus, send_wechat


def main() -> None:
    task   = sys.argv[1] if len(sys.argv) > 1 else "未知任务"
    detail = sys.argv[2] if len(sys.argv) > 2 else ""

    cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    title = f"⚠️ StockSage 任务失败: {task}"
    body  = f"任务 **{task}** 执行失败，请检查日志并考虑手动重跑。"
    if detail:
        body += f"\n\n{detail}"

    print(f"[notify] 发送失败通知: {title}", flush=True)
    send_wechat(title, body, sendkey)


if __name__ == "__main__":
    main()
