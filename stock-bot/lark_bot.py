"""
StockSage Alpha — Feishu Bot
============================
Feishu-specific transport layer. All business logic lives in bot_common.py.

配置 (stock-bot/feishu_config.json):
    feishu.app_id          飞书应用 App ID
    feishu.app_secret      飞书应用 App Secret
    feishu.allowed_open_ids  只接受这些用户 open_id（留空=全部）
    feishu.notify_chat_id  定时任务通知发送到哪个 chat_id（空=不推）

启动:
    python -X utf8 stock-bot/lark_bot.py
"""
from __future__ import annotations

import io
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)

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
        _CFG_CACHE = json.loads((BOT_DIR / "feishu_config.json").read_text(encoding="utf-8-sig"))
    return _CFG_CACHE

def _fs_cfg() -> dict:
    return _cfg().get("feishu", {})

def _allowed_open_ids() -> set[str]:
    ids = _fs_cfg().get("allowed_open_ids", [])
    return set(ids) if ids else set()

def _anthropic_key() -> str:
    try:
        return json.loads((BOT_DIR / "config.json").read_text(encoding="utf-8-sig"))["claude"]["api_key"]
    except Exception:
        return ""

# ── Feishu API client ─────────────────────────────────────────────────────────
_client: lark.Client | None = None

_MSG_LIMIT = 4000

def _chunks(text: str, size: int = _MSG_LIMIT) -> list[str]:
    return [text[i:i+size] for i in range(0, len(text), size)]

def reply_text(message_id: str, text: str) -> None:
    if not text:
        text = "(无输出)"
    for chunk in _chunks(text):
        req = (
            ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                ReplyMessageRequestBody.builder()
                .msg_type("text")
                .content(json.dumps({"text": chunk}))
                .build()
            )
            .build()
        )
        _client.im.v1.message.reply(req)

def send_to_chat(chat_id: str, text: str) -> None:
    if not text or not chat_id:
        return
    for chunk in _chunks(text):
        req = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": chunk}))
                .build()
            )
            .build()
        )
        _client.im.v1.message.create(req)

# ── Feishu-specific handlers ──────────────────────────────────────────────────
def _h_restart_bot() -> str:
    # bat file's :loop auto-restarts after 10s — just exit cleanly
    def _do():
        time.sleep(2)
        os._exit(0)
    threading.Thread(target=_do, daemon=False).start()
    return "Feishu bot 正在重启，10秒后自动恢复 ✅"

# ── Dispatch ──────────────────────────────────────────────────────────────────
def _dispatch(text: str) -> str:
    t = text.strip()

    # Feishu-only: restart this bot process
    if t in ("重启", "重启bot", "重启 bot", "restart bot", "rb"):
        return _h_restart_bot()

    result = bc.dispatch_command(t)
    if result is not None:
        return result

    return bc.dispatch_ai(t, _anthropic_key())

# ── Feishu event handler ──────────────────────────────────────────────────────
# 防 websocket 重连后 Feishu 服务器重发积压消息导致重复回复 / 回复过时消息：
#   1. 跳过 create_time 早于 _STALE_THRESHOLD_SEC 之前的消息（reconnect 时的 backlog）
#   2. 已处理 message_id LRU 去重（同一消息只回一次）
_STALE_THRESHOLD_SEC = 300       # 超过 5 分钟前的消息视为过时，丢弃
from collections import deque
_PROCESSED_IDS: deque = deque(maxlen=500)
_PROCESSED_IDS_SET: set[str] = set()


def _already_processed(mid: str) -> bool:
    if mid in _PROCESSED_IDS_SET:
        return True
    if len(_PROCESSED_IDS) >= _PROCESSED_IDS.maxlen:
        old = _PROCESSED_IDS.popleft()
        _PROCESSED_IDS_SET.discard(old)
    _PROCESSED_IDS.append(mid)
    _PROCESSED_IDS_SET.add(mid)
    return False


def _on_message_receive(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    event  = data.event
    sender = event.sender
    msg    = event.message

    if sender.sender_type != "user":
        return

    allowed = _allowed_open_ids()
    open_id = sender.sender_id.open_id
    if allowed and open_id not in allowed:
        return

    if msg.message_type != "text":
        return

    # 过时消息丢弃（reconnect 时服务器会重推积压消息）
    try:
        create_ms = int(msg.create_time)
        age_sec = (time.time() * 1000 - create_ms) / 1000
        if age_sec > _STALE_THRESHOLD_SEC:
            print(f"[MSG-SKIP-STALE] mid={msg.message_id} age={age_sec:.0f}s", flush=True)
            return
    except (ValueError, TypeError, AttributeError):
        pass  # create_time 解析失败，正常处理（不阻塞）

    # 消息 ID 去重（防服务器重推同一消息）
    if _already_processed(msg.message_id):
        print(f"[MSG-SKIP-DUP] mid={msg.message_id}", flush=True)
        return

    try:
        content = json.loads(msg.content)
        text    = content.get("text", "").strip()
    except Exception:
        return

    text = re.sub(r"@_user_\S*\s*", "", text).strip()
    if not text:
        return

    message_id = msg.message_id
    chat_id    = msg.chat_id
    print(f"[MSG] open_id={open_id} chat_id={chat_id} text={text!r}", flush=True)

    def handle():
        if text.lower() in ("chatid", "chat_id"):
            reply_text(message_id, f"chat_id: {chat_id}\nopen_id: {open_id}")
            return
        reply_text(message_id, _dispatch(text))

    threading.Thread(target=handle, daemon=True).start()

# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    global _client
    fs = _fs_cfg()
    app_id     = fs["app_id"]
    app_secret = fs["app_secret"]

    _client = lark.Client.builder().app_id(app_id).app_secret(app_secret).build()

    event_handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_on_message_receive)
        .build()
    )

    while True:
        try:
            ws = lark.ws.Client(
                app_id, app_secret,
                event_handler=event_handler,
                log_level=lark.LogLevel.WARNING,
            )
            print(f"[StockSage Feishu] starting (app_id={app_id})", flush=True)
            ws.start()
        except KeyboardInterrupt:
            print("[StockSage Feishu] stopped by user", flush=True)
            break
        except Exception as e:
            print(f"[StockSage Feishu] crashed: {e!r}, restarting in 10s…", flush=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
