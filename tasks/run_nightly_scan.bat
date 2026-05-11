@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "nightly_Scan" "Nightly scan: main/small/ETF strategies" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\jobs\nightly_scan.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\nightly_scan.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "nightly_Scan" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "nightly_Scan" "Nightly scan: main/small/ETF strategies" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "nightly_Scan" "Nightly scan: main/small/ETF strategies" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
)
