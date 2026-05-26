@echo off
chcp 65001 > nul
title 机构扫盘
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
echo [%DATE% %TIME%] institution_Scan bat entered >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "institution_Scan" "机构扫盘 📱" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
echo [%DATE% %TIME%] institution_Scan invoking python >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\strategies\institution_scan.py" --push-if-changed >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\institution_scan.log" 2>&1
echo [%DATE% %TIME%] institution_Scan python exit=%errorlevel% >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "institution_Scan" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "institution_Scan" "机构扫盘 📱" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "institution_Scan" "机构扫盘 📱" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
)
