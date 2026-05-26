@echo off
chcp 65001 > nul
title 参数自动调优
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
echo [%DATE% %TIME%] auto_Tune bat entered >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "auto_Tune" "参数自动调优" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
echo [%DATE% %TIME%] auto_Tune invoking python >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\jobs\auto_tune.py" --apply >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\auto_tune.log" 2>&1
echo [%DATE% %TIME%] auto_Tune python exit=%errorlevel% >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "auto_Tune" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "auto_Tune" "参数自动调优" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "auto_Tune" "参数自动调优" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
)
