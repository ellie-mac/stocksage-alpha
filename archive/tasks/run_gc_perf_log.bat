@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\gc_perf_log.py" --force >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\gc_perf_log.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_failure.py" "gc_PerfLog" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify.py" "gc_PerfLog" "金叉共振 G0-G2 胜率统计" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify.py" "gc_PerfLog" "金叉共振 G0-G2 胜率统计" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
)
