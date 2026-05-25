@echo off
chcp 65001 > nul
echo [%DATE% %TIME%] weekly_PerfReport bat entered >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
echo [%DATE% %TIME%] weekly_PerfReport invoking python >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\jobs\weekly_perf_report.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\weekly_perf_report.log" 2>&1
echo [%DATE% %TIME%] weekly_PerfReport python exit=%errorlevel% >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
