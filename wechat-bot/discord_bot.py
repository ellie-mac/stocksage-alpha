"""
StockSage Discord Bot
=====================
通过 Discord 远程控制 StockSage，支持任意网络环境。

配置:
    在 wechat-bot/config.json 的 "discord" 节填入:
        bot_token    - Discord Developer Portal → Bot → Token
        allowed_ids  - 只接受这些用户 ID 的命令（留空则接受所有人）
                       首次发消息时日志会打印你的 user_id

启动:
    python wechat-bot/discord_bot.py

支持命令 (在 Discord 频道或 DM 发送):
    帮助 / help          列出所有命令
    状态 / status        查看进程 & 日志
    持仓                 触发持仓盈亏推送（微信）
    信号 / 扫盘          立即扫描买卖信号（微信）
    今日推荐 / 推荐       当日选股结果（直接回复）
    日志 [N]             monitor 最近 N 行日志（默认 20）
    重启 monitor         重启 monitor.py 循环
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import discord

ROOT    = Path(__file__).resolve().parent.parent
BOT_DIR = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

_executor = ThreadPoolExecutor(max_workers=4)

# ── Config ────────────────────────────────────────────────────────────────────
def _cfg() -> dict:
    return json.loads((BOT_DIR / "config.json").read_text(encoding="utf-8"))

def _dc_cfg() -> dict:
    return _cfg().get("discord", {})

def _bot_token() -> str:
    t = _dc_cfg().get("bot_token", "")
    if not t or t.startswith("Discord"):
        raise RuntimeError("请先在 wechat-bot/config.json 填入 discord.bot_token")
    return t

def _allowed_ids() -> set[int]:
    ids = _dc_cfg().get("allowed_ids", [])
    return {int(i) for i in ids} if ids else set()

# ── Sync command handlers (run in thread pool) ────────────────────────────────
_HELP = """**StockSage 可用命令**

• `帮助` — 显示此帮助
• `状态` — 系统进程 & 最近日志
• `持仓` — 触发持仓盈亏推送（结果发微信）
• `信号` / `扫盘` — 立即扫描买卖信号（结果发微信）
• `今日推荐` — 当日选股结果（直接回复）
• `日志 [N]` — monitor 最近 N 条日志
• `重启 monitor` — 重启 monitor.py 循环
"""

def _h_status() -> str:
    lines = [f"**系统状态** @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]
    r = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/V"],
        capture_output=True, text=True
    )
    procs = []
    for line in r.stdout.strip().splitlines()[1:]:
        parts = line.strip('"').split('","')
        if len(parts) >= 8:
            procs.append(f"  PID {parts[1]} | CPU {parts[7]} | {parts[4]}")
    if procs:
        lines.append("**Python 进程:**")
        lines.extend(procs)
    else:
        lines.append("无运行中的 Python 进程")

    log_path = SCRIPTS / "monitor_loop.log"
    if log_path.exists():
        tail = log_path.read_bytes()[-3000:].decode("utf-8", errors="replace")
        last = [l for l in tail.splitlines() if l.strip()][-5:]
        lines.append("\n**Monitor 最近日志:**")
        lines.append("```")
        lines.extend(last)
        lines.append("```")
    else:
        lines.append("\nmonitor_loop.log 不存在（monitor 可能未运行）")
    return "\n".join(lines)


def _h_holdings() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--sell-only", "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return "已触发持仓推送，结果稍后发送到微信 📱"


def _h_scan() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return "已触发扫盘，结果稍后发送到微信 📱"


def _h_picks() -> str:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        return "latest_picks.json 不存在，今日可能尚未选股。"
    data  = json.loads(picks_path.read_text(encoding="utf-8"))
    items = data.get("results", [])
    date  = data.get("date", "?")
    lines = [f"**今日推荐** ({date})\n"]
    for i, s in enumerate(items[:10], 1):
        name  = s.get("name") or s.get("code", "?")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {name}  _(得分 {score:.3f})_")
    return "\n".join(lines)


def _h_logs(n: int = 20) -> str:
    log_path = SCRIPTS / "monitor_loop.log"
    if not log_path.exists():
        return "monitor_loop.log 不存在。"
    tail = log_path.read_bytes()[-8000:].decode("utf-8", errors="replace")
    last = [l for l in tail.splitlines() if l.strip()][-n:]
    body = "\n".join(last) or "(空)"
    # Discord code block limit ~2000 chars
    if len(body) > 1800:
        body = "..." + body[-1800:]
    return f"**日志 -{n}**\n```\n{body}\n```"


def _h_restart() -> str:
    r = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/V"],
        capture_output=True, text=True
    )
    for line in r.stdout.strip().splitlines()[1:]:
        parts = line.strip('"').split('","')
        if len(parts) >= 9 and "monitor" in parts[8].lower():
            try:
                import signal
                os.kill(int(parts[1]), signal.SIGTERM)
            except Exception:
                pass
    import time; time.sleep(2)
    log_path = SCRIPTS / "monitor_loop.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Restarted by Discord bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
    )
    return "monitor.py 已重启 ✅"


def _dispatch_sync(text: str) -> str:
    t = text.strip().lstrip("/")
    if t in ("帮助", "help", "？", "?"):
        return _HELP
    elif t in ("状态", "status"):
        return _h_status()
    elif t in ("持仓",):
        return _h_holdings()
    elif t in ("信号", "扫盘", "scan"):
        return _h_scan()
    elif t in ("今日推荐", "推荐", "picks"):
        return _h_picks()
    elif t.startswith("日志"):
        parts = t.split()
        n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20
        return _h_logs(n)
    elif t in ("重启 monitor", "重启monitor", "restart monitor", "重启"):
        return _h_restart()
    else:
        return f"未知命令: `{t}`\n\n发送 `帮助` 查看可用命令。"


# ── Discord client ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True          # requires privileged intent in Dev Portal
client  = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f"[StockSage Discord Bot] 已登录: {client.user} (id={client.user.id})")
    print("  在 Discord 发 '帮助' 开始使用")
    if not _allowed_ids():
        print("  未设置 allowed_ids，接受所有人的消息")


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

    text    = (message.content or "").strip()
    user_id = message.author.id
    allowed = _allowed_ids()

    if not text:
        return

    print(f"[MSG] user={message.author} id={user_id} text={text!r}")

    if allowed and user_id not in allowed:
        print(f"  → 未授权，忽略（将 {user_id} 加入 config.json discord.allowed_ids 以授权）")
        await message.reply("❌ 未授权")
        return

    if not allowed:
        print(f"  → 提示: 将 {user_id} 加入 config.json discord.allowed_ids 以限制访问")

    # Acknowledge immediately, run handler in thread pool
    async with message.channel.typing():
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(_executor, _dispatch_sync, text)

    # Discord message limit = 2000 chars
    MAX = 1990
    chunks = [result[i:i+MAX] for i in range(0, len(result), MAX)]
    for chunk in chunks:
        await message.reply(chunk)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    client.run(_bot_token())
