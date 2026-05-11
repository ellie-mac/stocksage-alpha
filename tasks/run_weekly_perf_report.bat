@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\jobs\weekly_perf_report.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\weekly_perf_report.log" 2>&1
