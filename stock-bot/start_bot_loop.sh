#!/usr/bin/env bash
cd /c/Users/jiapeichen/repos/stocksage-alpha
while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting discord_bot.py..."
    python -X utf8 stock-bot/discord_bot.py >> scripts/logs/discord_service.log 2>&1
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Bot exited (code $?), restarting in 10s..." >> scripts/logs/discord_service.log
    sleep 10
done
