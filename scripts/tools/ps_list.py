#!/usr/bin/env python3
"""
列出 VM 上的 Python 进程，区分「我们的进程」和「其他进程」。

用法：
    python -X utf8 scripts/tools/ps_list.py
"""
import csv, io, re, subprocess, sys

PS = (
    "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" "
    "| Select-Object ProcessId,"
    "@{N='MemMB';E={[math]::Round($_.WorkingSetSize/1MB)}},"
    "@{N='CPUs';E={[math]::Round($_.UserModeTime/1e7)}},"
    "@{N='Script';E={if ($_.CommandLine) {"
    "  ($_.CommandLine -split ' ') | Where-Object { $_ -like '*.py' } | Select-Object -First 1"
    "} else {''}}} "
    "| Sort-Object MemMB -Descending "
    "| ConvertTo-Csv -NoTypeInformation"
)

result = subprocess.run(
    ["powershell", "-NonInteractive", "-Command", PS],
    capture_output=True, text=True, encoding="utf-8", errors="replace"
)

# Strip PSReadLine warning block (everything up to the blank line before real output)
raw = result.stdout
# Remove lines that are part of the PSReadLine error
clean_lines = [l for l in raw.splitlines()
               if not any(k in l for k in ("Set-PSReadLineOption", "PSReadLine",
                                            "profile.ps1", "CategoryInfo",
                                            "FullyQualifiedErrorId", "ArgumentException",
                                            "ParameterBindingException", "char:", "+ ~"))]
clean = "\n".join(clean_lines).strip()

try:
    reader = csv.DictReader(io.StringIO(clean))
    procs = list(reader)
except Exception as e:
    print(f"解析失败: {e}\n原始输出:\n{clean[:500]}")
    sys.exit(1)

OURS_PATHS = {"stocksage-alpha", "lark_agent"}
OURS_SCRIPTS = {
    "main_scan.py", "main_night.py", "institution_scan.py",
    "monitor.py", "prefetch.py", "factor_analysis.py",
    "backtest.py", "chip_strategy.py", "screener.py",
    "fetcher.py", "setup_scheduler.py",
    "lark_agent.py",
}

ours, others = [], []
for p in procs:
    script = p.get("Script", "").strip('"')
    name = script.replace("\\", "/").split("/")[-1]
    if any(k in script for k in OURS_PATHS) or name in OURS_SCRIPTS:
        ours.append(p)
    else:
        others.append(p)

def _fmt(rows: list) -> None:
    if not rows:
        print("  (无)")
        return
    print(f"  {'PID':>6}  {'内存MB':>6}  {'CPU秒':>5}  脚本")
    print(f"  {'─'*6}  {'─'*6}  {'─'*5}  {'─'*45}")
    for p in rows:
        script = p.get("Script", "").strip('"').replace("\\", "/")
        if "stocksage-alpha/" in script:
            script = script.split("stocksage-alpha/")[-1]
        pid  = p.get("ProcessId", "").strip('"')
        mem  = p.get("MemMB", "").strip('"')
        cpu  = p.get("CPUs", "").strip('"')
        print(f"  {pid:>6}  {mem:>6}  {cpu:>5}  {script}")

print("=== 我们的进程 ===")
_fmt(ours)
print()
print("=== 其他 Python 进程 ===")
_fmt(others)
