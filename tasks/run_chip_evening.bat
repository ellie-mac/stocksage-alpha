@echo off
chcp 65001 > nul
title 小红书收盘推送
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "xhs_Evening" "小红书收盘推送" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\report\reporter.py" evening >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\xhs_evening.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "xhs_Evening" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "xhs_Evening" "小红书收盘推送" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "xhs_Evening" "小红书收盘推送" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_discord.log" 2>&1
)
