@echo off
chcp 65001 > nul
title Lark会话合并
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
echo [%DATE% %TIME%] merge_Sessions bat entered >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "merge_Sessions" "Lark会话合并" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
echo [%DATE% %TIME%] merge_Sessions invoking python >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\lark-agent\merge_sessions.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\merge_sessions.log" 2>&1
echo [%DATE% %TIME%] merge_Sessions python exit=%errorlevel% >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "merge_Sessions" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "merge_Sessions" "Lark会话合并" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "merge_Sessions" "Lark会话合并" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
)
