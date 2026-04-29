"""
StockSage Alpha — Discord Bot
==============================
Discord-specific transport layer. All business logic lives in bot_common.py.

配置 (stock-bot/config.json):
    discord.bot_token    - Discord Developer Portal → Bot → Token
    discord.allowed_ids  - 只接受这些用户 ID（留空=接受所有人）
    claude.api_key       - Anthropic API key for AI dispatch

启动:
    python -X utf8 stock-bot/discord_bot.py
"""
from __future__ import annotations

import asyncio
import io
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import discord

import bot_common as bc

# UTF-8 stdout/stderr on Windows
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

BOT_DIR = Path(__file__).resolve().parent

_executor = ThreadPoolExecutor(max_workers=4)

# ── Config ────────────────────────────────────────────────────────────────────
_CFG_CACHE: dict | None = None

def _cfg() -> dict:
    global _CFG_CACHE
    if _CFG_CACHE is None:
        _CFG_CACHE = json.loads((BOT_DIR / "config.json").read_text(encoding="utf-8"))
    return _CFG_CACHE

def _dc_cfg() -> dict:
    return _cfg().get("discord", {})

def _bot_token() -> str:
    t = _dc_cfg().get("bot_token", "")
    if not t or t.startswith("Discord"):
        raise RuntimeError("请先在 stock-bot/config.json 填入 discord.bot_token")
    return t

def _allowed_ids() -> set[int]:
    ids = _dc_cfg().get("allowed_ids", [])
    return {int(i) for i in ids} if ids else set()

def _anthropic_key() -> str:
    return _cfg().get("claude", {}).get("api_key", "")

# ── Dispatch ──────────────────────────────────────────────────────────────────
def _dispatch(text: str) -> str:
    t = text.strip().lstrip("/")

    result = bc.dispatch_command(t)
    if result is not None:
        return result

    return bc.dispatch_ai(t, _anthropic_key())

# ── Discord client ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
client  = discord.Client(intents=intents)

_MSG_MAX = 1990


@client.event
async def on_ready():
    print(f"[StockSage Discord] logged in as {client.user} (id={client.user.id})", flush=True)
    if not _allowed_ids():
        print("[StockSage Discord] allowed_ids empty — accepting all users", flush=True)


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return
    if message.author.bot or message.webhook_id is not None:
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

    loop = asyncio.get_event_loop()

    # For AI dispatch (slow), show typing indicator and heartbeat
    t = text.strip().lstrip("/")
    quick = bc.dispatch_command(t)
    if quick is not None:
        for chunk in [quick[i:i+_MSG_MAX] for i in range(0, len(quick), _MSG_MAX)]:
            await message.reply(chunk)
        return

    # AI path: show pending message with heartbeat
    pending = await message.reply("⏳ 处理中…")
    start = loop.time()

    async def _heartbeat():
        while True:
            await asyncio.sleep(60)
            elapsed = int((loop.time() - start) / 60)
            try:
                await pending.edit(content=f"⏳ 仍在思考中…（已 {elapsed} 分钟）")
            except Exception:
                pass

    hb = asyncio.create_task(_heartbeat())
    try:
        result = await loop.run_in_executor(_executor, bc.dispatch_ai, t, _anthropic_key())
    except Exception as e:
        result = f"❌ 执行出错: {e}"
    finally:
        hb.cancel()
        try:
            await pending.delete()
        except Exception:
            pass

    chunks = [result[i:i+_MSG_MAX] for i in range(0, len(result), _MSG_MAX)]
    for i, chunk in enumerate(chunks):
        prefix = f"{message.author.mention}\n" if i == 0 else ""
        await message.channel.send(prefix + chunk)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    token = _bot_token()
    while True:
        try:
            client.run(token, reconnect=True)
        except discord.errors.LoginFailure as e:
            print(f"[StockSage Discord] Fatal: login failed ({e}), not retrying", flush=True)
            break
        except KeyboardInterrupt:
            print("[StockSage Discord] stopped by user", flush=True)
            break
        except Exception as e:
            print(f"[StockSage Discord] crashed: {e!r}, restarting in 10s…", flush=True)
            time.sleep(10)
        else:
            print("[StockSage Discord] disconnected, reconnecting in 5s…", flush=True)
            time.sleep(5)
