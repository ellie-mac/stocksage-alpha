#!/usr/bin/env python3
"""
main_Morning 前置检查：若前一日 main_Scan 已更新 latest_picks.json，静默跳过。
exit 0 = 正常运行；exit 77 = 跳过（bat 收到后 exit /b 0 静默成功）。
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
p    = ROOT / "data" / "latest_picks.json"

if p.exists():
    try:
        d        = json.loads(p.read_text(encoding="utf-8"))
        ts       = d.get("timestamp", "")
        ts_date  = ts[:10].replace("-", "")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if ts_date == yesterday:
            print(f"[main_Morning] 前日 main_Scan 已更新（{ts[:16]}），静默跳过", flush=True)
            sys.exit(77)
    except Exception:
        pass

print("[main_Morning] 未找到昨日扫描记录，正常运行", flush=True)
sys.exit(0)
