@echo off
title StockSage Discord Bot
cd /d C:\Users\jiapeichen\repos\stocksage-alpha
:loop
echo [%date% %time%] Starting discord_bot.py...
python -X utf8 stock-bot\discord_bot.py
echo [%date% %time%] Bot exited (code %errorlevel%), restarting in 10s...
timeout /t 10 /nobreak
goto loop
