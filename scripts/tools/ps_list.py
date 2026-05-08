#!/usr/bin/env python3
"""
列出 VM 上的 Python 进程，区分「我们的进程」和「其他进程」。

用法：
    python -X utf8 scripts/tools/ps_list.py
"""
import subprocess
import sys

PS = (
    "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" "
    "| Select-Object ProcessId,"
    "@{N='Mem(MB)';E={[math]::Round($_.WorkingSetSize/1MB)}},"
    "@{N='CPU(s)';E={[math]::Round($_.UserModeTime/1e7)}},"
    "@{N='Script';E={if ($_.CommandLine) {"
    "  ($_.CommandLine -split ' ') | Where-Object { $_ -like '*.py' } | Select-Object -First 1"
    "} else {''}}} "
    "| Sort-Object 'Mem(MB)' -Descending "
    "| ConvertTo-Json -AsArray"
)

result = subprocess.run(
    ["powershell", "-NonInteractive", "-Command", PS],
    capture_output=True, text=True, encoding="utf-8", errors="replace"
)

import json, re

# Strip PSReadLine noise
raw = re.sub(r"Set-PSReadLineOption.*?FullyQualifiedErrorId[^\n]*\n", "", result.stdout, flags=re.DOTALL)
raw = raw.strip()

try:
    procs = json.loads(raw) if raw else []
except Exception:
    print(result.stdout)
    sys.exit(0)

OURS = {"stocksage-alpha", "lark_agent"}

ours, others = [], []
for p in procs:
    script = p.get("Script") or ""
    if any(k in script for k in OURS):
        ours.append(p)
    else:
        others.append(p)

def _fmt(rows: list) -> None:
    if not rows:
        print("  (无)")
        return
    print(f"  {'PID':>6}  {'Mem(MB)':>7}  {'CPU(s)':>6}  Script")
    print(f"  {'─'*6}  {'─'*7}  {'─'*6}  {'─'*40}")
    for p in rows:
        script = (p.get("Script") or "").replace("\\", "/")
        # 只保留 stocksage-alpha/ 之后的部分
        if "stocksage-alpha/" in script:
            script = script.split("stocksage-alpha/")[-1]
        print(f"  {p['ProcessId']:>6}  {p['Mem(MB)']:>7}  {p['CPU(s)']:>6}  {script}")

print("=== 我们的进程 ===")
_fmt(ours)
print()
print("=== 其他 Python 进程 ===")
_fmt(others)
