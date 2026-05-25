@echo off
rem _kill_orphan_python.bat <python_script_filename>
rem Kills any python.exe process whose command line contains the given script name.
rem Used as a prologue in run_signal_tracker.bat / run_quality_prefetch.bat to clear stale orphans.
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter 'Name=''python.exe''' | Where-Object { $_.CommandLine -like '*%~1*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
