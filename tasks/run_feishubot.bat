@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
:loop
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\stock-bot\feishu_bot.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\stock-bot\feishu_bot.log" 2>&1
echo [%date% %time%] StockSage_FeishuBot exited, restarting in 10s... >> "C:\Users\jiapeichen\repos\stocksage-alpha\stock-bot\feishu_bot.log" 2>&1
timeout /t 10 /nobreak > nul
goto loop
