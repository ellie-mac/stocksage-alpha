@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\xhs\chip_writer.py" midday >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\chip_writer_midday.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_failure.py" "xhs_Midday" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_discord.py" "xhs_Midday" "小红书午间筹码分析推送 📱" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\notify_discord.py" "xhs_Midday" "小红书午间筹码分析推送 📱" >> "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs\notify_discord.log" 2>&1
)
