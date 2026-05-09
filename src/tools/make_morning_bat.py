#!/usr/bin/env python3
"""Generate tasks/run_morning_scan.bat with morning_guard check prepended."""
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent.parent
TASKS    = ROOT / "tasks"
SRC      = TASKS / "run_monitor_scan.bat"
DST      = TASKS / "run_morning_scan.bat"
PY       = r'"C:\Program Files\Python313\python.exe"'
GUARD    = ROOT / "src" / "jobs" / "morning_guard.py"
LOG      = ROOT / "src" / "logs" / "monitor_scan.log"

guard_lines = (
    f'{PY} -X utf8 "{GUARD}" >> "{LOG}" 2>&1\r\n'
    f'set _GUARD=%errorlevel%\r\n'
    f'if %_GUARD% == 77 exit /b 0\r\n'
    f'\r\n'
)

original = SRC.read_text(encoding="utf-8-sig")

# Insert guard right after the mkdir line
lines = original.splitlines(keepends=True)
out = []
inserted = False
for line in lines:
    out.append(line)
    if not inserted and "mkdir" in line and "logs" in line:
        out.append(guard_lines)
        inserted = True

if not inserted:
    # fallback: insert after @echo off block (4th line)
    out.insert(4, guard_lines)

DST.write_text("".join(out), encoding="utf-8")
print(f"Written: {DST}")
print("Content:")
print(DST.read_text(encoding="utf-8"))
