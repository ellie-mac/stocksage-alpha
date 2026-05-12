#!/usr/bin/env python3
"""根据实际日志文件名全面扫描今日任务状态。"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT    = Path("C:/Users/jiapeichen/repos/stocksage-alpha")
LOG_DIR = ROOT / "src" / "logs"
CUTOFF  = datetime.now() - timedelta(hours=30)  # 30h 覆盖昨日下午到今晨

# 真实日志文件名 → 任务标签
TASK_LOGS = {
    "closing_batch.log":       "closing_Batch",
    "signal_tracker.log":      "signal_Tracker",
    "auto_tune.log":           "auto_Tune",
    "nightly_scan.log":        "nightly_Scan",
    "prefetch_price.log":      "price_Prefetch",
    "prefetch_market.log":     "market_Warm",
    "prefetch_fundflow.log":   "fundflow_Prefetch",
    "prefetch_concept.log":    "concept_Warm",
    "gc_scan.log":             "golden_Scan",
    "hot_scan.log":            "hot_Scan",
    "institution_scan.log":    "institution_Scan",
    "cad_pipeline.log":        "chip_CadScan",
    "chip_scan_night.log":     "chip_Night",
    "chip_scan_premarket.log": "chip_Premarket",
    "marketcap_scan.log":      "marketcap_Scan",
    "integrity_check.log":     "integrity_Check",
    "factor_analysis.log":     "factor_Analysis",
    "watchlist_monitor.log":   "watchlist_Monitor",
    "notify_failure.log":      "notify_failure",
    "xhs_morning.log":         "report_Morning(旧xhs)",
    "xhs_midday.log":          "report_Midday(旧xhs)",
    "xhs_evening.log":         "report_Evening(旧xhs)",
    "monitor.log":             "monitor",
    "monitor_scan.log":        "monitor_scan",
    "watchlist_scan.log":      "watchlist_Scan",
    "watchlist_updater.log":   "watchlist_Updater",
}

print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}  覆盖过去30h\n")

for log_name, label in TASK_LOGS.items():
    path = LOG_DIR / log_name
    if not path.exists():
        print(f"  ❌ 无文件  {label}")
        continue
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    if mtime < CUTOFF:
        print(f"  ⚠  [{mtime.strftime('%m-%d %H:%M')}] {label}  (超30h未更新)")
        continue

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    non_empty = [l for l in lines if l.strip()]
    tail3 = "\n".join(non_empty[-3:]).lower()

    if any(x in tail3 for x in ["error", "traceback", "failed", "[winError", "exception", "频率超限", "接收数据异常"]):
        status = "❌"
    elif any(x in tail3 for x in ["完成", "success", "finished", "done", "succeeded", "ok", "✓", "推送成功"]):
        status = "✓"
    else:
        status = "?"

    print(f"  {status} [{mtime.strftime('%m-%d %H:%M')}] {label}")
    for l in non_empty[-3:]:
        print(f"      {l[:120]}")
