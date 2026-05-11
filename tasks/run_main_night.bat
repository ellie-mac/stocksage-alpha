@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\tools\generate_full_universe.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\universe_main.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "main_Night" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "main_Night" "预热财务缓存（batch_financials），不推送" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
    exit /b 1
)
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\tools\batch_financials.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\batch_financials.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "main_Night" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "main_Night" "预热财务缓存（batch_financials），不推送" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "main_Night" "预热财务缓存（batch_financials），不推送" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
)
