#!/usr/bin/env python3
"""
列出 VM 上所有 StockSage 相关 Python 进程（脚本名 + 内存 + CPU 时长）。

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
    "} else {'N/A'}}} "
    "| Sort-Object 'Mem(MB)' -Descending "
    "| Format-Table -AutoSize"
)

result = subprocess.run(
    ["powershell", "-NonInteractive", "-Command", PS],
    capture_output=True, text=True, encoding="utf-8", errors="replace"
)
# Filter out PSReadLine warning lines
lines = [l for l in result.stdout.splitlines()
         if not l.startswith("Set-PSReadLineOption") and "PSReadLine" not in l
         and "profile.ps1" not in l and "CategoryInfo" not in l
         and "FullyQualifiedErrorId" not in l and "ArgumentException" not in l]
print("\n".join(lines))
if result.returncode != 0 and result.stderr:
    print(result.stderr[:500], file=sys.stderr)
