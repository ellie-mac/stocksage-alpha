@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\main_perf_log.py" --force >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\main_perf_log.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_failure.py" "main_PerfLog" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify.py" "main_PerfLog" "主策略昨日选股今日胜率对比 📱" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify.py" "main_PerfLog" "主策略昨日选股今日胜率对比 📱" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
)
