"""
StockSage Discord Bot  (with Claude AI)
========================================
支持自然语言对话 + 固定命令两种模式。

配置 (wechat-bot/config.json):
    discord.bot_token    - Discord Developer Portal → Bot → Token
    discord.allowed_ids  - 只接受这些用户 ID（留空=接受所有人）
    claude.api_key       - Anthropic API Key（留空=仅固定命令模式）

启动:
    python -X utf8 wechat-bot/discord_bot.py

固定命令 (Claude API 未配置时也能用):
    帮助 / help          列出所有命令
    状态 / status        查看进程 & 日志
    持仓                 触发持仓盈亏推送
    信号 / 扫盘          立即扫描买卖信号
    今日推荐 / 推荐       当日选股结果
    日志 [N]             monitor 最近 N 行日志
    重启 monitor         重启 monitor.py 循环
    回测 [N期] [main|smallcap]   启动回测，如: 回测 16期 main

对话模式 (需配置 claude.api_key):
    直接用自然语言提问或下指令即可，Claude 会自动理解并执行。
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import discord

# Ensure stdout/stderr handle UTF-8 on Windows
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

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

def _claude_api_key() -> str:
    return _cfg().get("claude", {}).get("api_key", "")

# ── Command handlers ──────────────────────────────────────────────────────────
_HELP = """**StockSage 可用命令**

• `帮助` — 显示此帮助
• `状态` — 系统进程 & 最近日志
• `持仓` — 触发持仓盈亏推送
• `信号` / `扫盘` — 立即扫描买卖信号
• `今日推荐` — 当日选股结果
• `日志 [N]` — monitor 最近 N 条日志
• `重启 monitor` — 重启 monitor.py 循环
• `回测 [N期] [main|smallcap]` — 启动回测，例: `回测 16期 main`

💬 配置了 Claude API Key 后可直接用自然语言对话。
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


def _h_backtest(periods: int = 16, universe: str = "main", workers: int = 8) -> str:
    # Check for already-running backtest
    r = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/V"],
        capture_output=True, text=True
    )
    for line in r.stdout.strip().splitlines()[1:]:
        parts = line.strip('"').split('","')
        if len(parts) >= 9 and "backtest" in parts[8].lower():
            return f"⚠️ 已有回测进程在运行（PID {parts[1]}），请等待完成或先停止。"

    universe_map = {
        "main":     SCRIPTS / "main_universe.json",
        "smallcap": SCRIPTS / "smallcap_universe.json",
    }
    universe_file = universe_map.get(universe, universe_map["main"])
    if not universe_file.exists():
        return f"❌ 股票池文件不存在: {universe_file}"

    out_file  = ROOT / "data" / f"backtest_{universe}_{periods}p.json"
    log_path  = SCRIPTS / f"backtest_{universe}_{periods}p.log"

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- Backtest started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        f.write(f"    universe={universe_file.name}, periods={periods}, workers={workers}\n\n")

    proc = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "backtest.py"),
         "--periods", str(periods),
         "--universe", str(universe_file),
         "--out", str(out_file),
         "--workers", str(workers)],
        cwd=str(ROOT),
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
    )
    return (
        f"✅ 回测已启动 (PID {proc.pid})\n"
        f"• 股票池: {universe} ({universe_file.name})\n"
        f"• 期数: {periods}  Workers: {workers}\n"
        f"• 输出: {out_file.name}\n"
        f"• 日志: {log_path.name}\n"
        f"用 `日志` 命令或 `状态` 跟踪进度（预计每期 ~20 min）"
    )


def _h_read_file(path: str) -> str:
    """Read a project file for Claude to answer questions."""
    try:
        fp = (ROOT / path).resolve()
        # Safety: only allow files inside the project root
        if not str(fp).startswith(str(ROOT)):
            return "❌ 不允许读取项目目录之外的文件"
        if not fp.exists():
            return f"文件不存在: {path}"
        content = fp.read_text(encoding="utf-8", errors="replace")
        if len(content) > 2000:
            content = content[:2000] + "\n... (已截断)"
        return content
    except Exception as e:
        return f"读取失败: {e}"


# ── Simple command dispatch (no Claude needed) ────────────────────────────────
def _dispatch_sync(text: str) -> str | None:
    """Return a reply string for known commands, or None to hand off to Claude."""
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
    elif t.startswith("回测") or t.startswith("backtest"):
        # Parse: 回测 [N期] [main|smallcap]
        parts = t.split()
        periods  = 16
        universe = "main"
        for p in parts[1:]:
            if p.replace("期", "").isdigit():
                periods = int(p.replace("期", ""))
            elif p in ("main", "smallcap", "小盘", "主"):
                universe = "smallcap" if p in ("smallcap", "小盘") else "main"
        return _h_backtest(periods=periods, universe=universe)

    return None  # hand off to Claude


# ── Claude AI tool executor ───────────────────────────────────────────────────
_CLAUDE_TOOLS = [
    {
        "name": "get_status",
        "description": "获取系统状态：Python进程列表和monitor最近日志",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_scan",
        "description": "立即触发买卖信号扫描（结果发微信）",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_holdings",
        "description": "触发持仓盈亏推送（结果发微信）",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_picks",
        "description": "获取今日选股推荐列表",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_logs",
        "description": "获取monitor运行日志",
        "input_schema": {
            "type": "object",
            "properties": {
                "n": {"type": "integer", "description": "显示行数，默认15"}
            },
            "required": [],
        },
    },
    {
        "name": "restart_monitor",
        "description": "重启monitor.py循环",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_backtest",
        "description": "启动股票回测任务（后台运行，需要数小时）",
        "input_schema": {
            "type": "object",
            "properties": {
                "periods":  {"type": "integer", "description": "回测期数，默认16"},
                "universe": {"type": "string",  "description": "股票池：main（主策略705股）或 smallcap（小盘股），默认main"},
                "workers":  {"type": "integer", "description": "并行worker数，默认8"},
            },
            "required": [],
        },
    },
    {
        "name": "read_file",
        "description": "读取项目文件内容，用于回答关于代码、配置、数据的问题",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "相对于项目根目录的文件路径，如 scripts/research.py"}
            },
            "required": ["path"],
        },
    },
]

_CLAUDE_SYSTEM = f"""你是 StockSage Alpha 的智能助手，运行在用户的 Windows 量化交易服务器上。
你可以控制 StockSage 系统、回答代码和策略问题、分析数据。

项目根目录: {ROOT}
主要脚本: scripts/  数据目录: data/  配置: stock-bot/config.json

规则：
- 用中文回复，回复尽量简短（2-4句话），不要废话
- 需要文件内容时先用 read_file 读取再回答
- 回测任务耗时数小时，启动后告知日志文件名
"""

# Per-channel conversation history: channel_id -> list of messages
_history: dict[int, list[dict]] = {}
_MAX_HISTORY = 10  # keep last 10 messages (~5 turns)


_TOOL_RESULT_LIMIT = 1500  # max chars fed back to Claude per tool call

def _execute_tool(name: str, inputs: dict) -> str:
    if name == "get_status":
        result = _h_status()
    elif name == "run_scan":
        result = _h_scan()
    elif name == "get_holdings":
        result = _h_holdings()
    elif name == "get_picks":
        result = _h_picks()
    elif name == "get_logs":
        result = _h_logs(inputs.get("n", 15))
    elif name == "restart_monitor":
        result = _h_restart()
    elif name == "run_backtest":
        result = _h_backtest(
            periods=inputs.get("periods", 16),
            universe=inputs.get("universe", "main"),
            workers=inputs.get("workers", 8),
        )
    elif name == "read_file":
        result = _h_read_file(inputs.get("path", ""))
    else:
        return f"未知工具: {name}"
    # Trim oversized results before sending back to Claude
    if len(result) > _TOOL_RESULT_LIMIT:
        result = result[:_TOOL_RESULT_LIMIT] + "\n...(已截断)"
    return result


def _claude_dispatch(channel_id: int, text: str) -> str:
    try:
        import anthropic
    except ImportError:
        return "❌ anthropic 包未安装，运行: pip install anthropic"

    api_key = _claude_api_key()
    if not api_key:
        return "❌ 未配置 claude.api_key，请在 wechat-bot/config.json 填入 Anthropic API Key"

    client = anthropic.Anthropic(api_key=api_key)

    history = _history.setdefault(channel_id, [])
    history.append({"role": "user", "content": text})

    messages = history.copy()

    # Agentic loop (tool use)
    for _ in range(5):  # max 5 rounds
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system=_CLAUDE_SYSTEM,
            tools=_CLAUDE_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    print(f"[TOOL] {block.name}({block.input})", flush=True)
                    result = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
        else:
            # Final text response
            reply = "".join(
                block.text for block in response.content if hasattr(block, "text")
            ) or "(无回复)"

            # Save to history (keep last N turns)
            history.append({"role": "assistant", "content": reply})
            if len(history) > _MAX_HISTORY:
                _history[channel_id] = history[-_MAX_HISTORY:]

            return reply

    return "❌ Claude 未能在限定轮次内完成回复，请重试"


# ── Discord client ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
client  = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f"[StockSage] Logged in as {client.user} (id={client.user.id})", flush=True)
    claude_ok = bool(_claude_api_key())
    print(f"[StockSage] Claude AI: {'enabled' if claude_ok else 'disabled (no api_key)'}", flush=True)
    if not _allowed_ids():
        print("[StockSage] allowed_ids empty — accepting all users", flush=True)


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

    text    = (message.content or "").strip()
    user_id = message.author.id
    allowed = _allowed_ids()

    if not text:
        return

    print(f"[MSG] user={message.author} id={user_id} text={text!r}", flush=True)

    if allowed and user_id not in allowed:
        print(f"  -> unauthorized (add {user_id} to discord.allowed_ids)", flush=True)
        await message.reply("❌ 未授权")
        return

    if not allowed:
        print(f"  -> tip: add {user_id} to discord.allowed_ids to restrict access", flush=True)

    async with message.channel.typing():
        loop = asyncio.get_event_loop()

        # Try fixed commands first (fast, no API call)
        result = await loop.run_in_executor(_executor, _dispatch_sync, text)

        # Fall back to Claude AI
        if result is None:
            if _claude_api_key():
                channel_id = message.channel.id
                result = await loop.run_in_executor(
                    _executor, _claude_dispatch, channel_id, text
                )
            else:
                result = f"未知命令: `{text}`\n\n发送 `帮助` 查看可用命令。\n💡 配置 `claude.api_key` 后可直接用自然语言对话。"

    MAX = 1990
    for chunk in [result[i:i+MAX] for i in range(0, len(result), MAX)]:
        await message.reply(chunk)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    client.run(_bot_token())
