@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs" 2>nul

"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\xhs\reporter.py" preauction >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\xhs_preauction.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_failure.py" "xhs_Morning" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_failure.log" 2>&1
)
