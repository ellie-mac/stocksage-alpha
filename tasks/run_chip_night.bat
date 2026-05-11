@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\chip\daily_scan.py" --ak --no-push >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\chip_scan_night.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "chip_Night" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "chip_Night" "收盘后预取筹码缓存（AK重算~1.5h），不推送" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "chip_Night" "收盘后预取筹码缓存（AK重算~1.5h），不推送" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
)
