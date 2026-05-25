@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\monitor.py" --always-send >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\monitor_scan.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_failure.py" "main_Scan" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify.py" "main_Scan" "主策略扫盘，更新 latest_picks.json，推送 📱" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify.py" "main_Scan" "主策略扫盘，更新 latest_picks.json，推送 📱" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
)
