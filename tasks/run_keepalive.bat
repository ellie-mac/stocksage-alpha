@echo off
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
echo keepalive {%DATE% %TIME%} >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\keepalive.log" 2>&1
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "bot_Keepalive" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
)
