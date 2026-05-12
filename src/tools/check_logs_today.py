#!/usr/bin/env python3
"""扫描今天有更新的日志文件，并提取最后几行判断成败。"""
import os, sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT    = Path("C:/Users/jiapeichen/repos/stocksage-alpha")
LOG_DIR = ROOT / "src" / "logs"
CUTOFF  = datetime.now() - timedelta(hours=24)

# 关注的 log -> 对应任务名
TASK_LOGS = {
    "closing_batch.log":        "closing_Batch (15:05)",
    "signal_tracker.log":       "signal_Tracker (15:25)",
    "report_morning.log":       "report_Morning",
    "report_midday.log":        "report_Midday",
    "report_evening.log":       "report_Evening",
    "nightly_scan.log":         "nightly_Scan (22:10)",
    "auto_tune.log":            "auto_Tune",
    "prefetch_price.log":       "price_Prefetch",
    "prefetch_market.log":      "market_Warm",
    "prefetch_fundflow.log":    "fundflow_Prefetch",
    "prefetch_concept.log":     "concept_Warm",
    "golden_scan.log":          "golden_Scan",
    "hot_scan.log":             "hot_Scan",
    "institution_scan.log":     "institution_Scan",
    "chip_cadscan.log":         "chip_CadScan",
    "notify_failure.log":       "notify_failure (失败通知)",
    "BroSimmons_DailyMarket.log": "BroSimmons_DailyMarket",
    "BroSimmons_DailyHotlist.log": "BroSimmons_DailyHotlist",
}

print(f"=== 日志扫描 (过去24小时) ===\n")

all_logs = {f.name.lower(): f for f in LOG_DIR.glob("*.log")}

for log_name, task_label in TASK_LOGS.items():
    path = all_logs.get(log_name.lower()) or LOG_DIR / log_name
    if not path.exists():
        continue
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    if mtime < CUTOFF:
        continue

    # Read last ~10 non-empty lines
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as e:
        lines = [f"(read error: {e})"]

    non_empty = [l for l in lines if l.strip()][-10:]

    # Guess success/failure from last lines
    tail = "\n".join(non_empty[-3:]).lower()
    if any(x in tail for x in ["error", "traceback", "failed", "exit 1", "exception"]):
        status = "❌"
    elif any(x in tail for x in ["完成", "success", "finished", "ok", "done", "succeeded"]):
        status = "✓"
    else:
        status = "?"

    print(f"{status} [{mtime.strftime('%m-%d %H:%M')}] {task_label}")
    for l in non_empty[-5:]:
        print(f"    {l[:120]}")
    print()

print("=== 未在上面出现的关注任务（24h内无日志更新）===")
for log_name, task_label in TASK_LOGS.items():
    path = all_logs.get(log_name.lower()) or LOG_DIR / log_name
    if not path.exists():
        print(f"  ❌ 无日志文件: {task_label}")
        continue
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    if mtime < CUTOFF:
        print(f"  ⚠ 超24h未更新 ({mtime.strftime('%m-%d %H:%M')}): {task_label}")
