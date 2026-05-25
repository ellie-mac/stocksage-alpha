@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\jobs\morning_guard.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\monitor_scan.log" 2>&1

set _GUARD=%errorlevel%

if %_GUARD% == 77 exit /b 0



"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "main_Scan" "主策略扫盘，更新 latest_picks.json，推送 📱" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\monitor.py" --always-send --buy-only >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\monitor_scan.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "main_Scan" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "main_Scan" "主策略扫盘，更新 latest_picks.json，推送 📱" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "main_Scan" "主策略扫盘，更新 latest_picks.json，推送 📱" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
)
