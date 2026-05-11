@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "institution_Scan" "机构策略季度调仓扫描，有变化则推送 📱" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\strategies\institution_scan.py" --push-if-changed >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\institution_scan.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "institution_Scan" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "institution_Scan" "机构策略季度调仓扫描，有变化则推送 📱" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "institution_Scan" "机构策略季度调仓扫描，有变化则推送 📱" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
)
