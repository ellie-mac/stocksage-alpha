#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lark_agent.py — Feishu bots via GitHub Copilot Chat API
Replaces cc-connect.exe. Uses lark-oapi for WebSocket, gh CLI for auth.
"""
from __future__ import annotations

import base64
import hashlib
import http
import json
import logging
import queue
import re
import subprocess
import sys
import threading
import time
import types
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import requests

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        print("需要 Python 3.11+ 或 pip install tomli", flush=True)
        sys.exit(1)

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("lark_agent")

BASE_DIR = Path(__file__).parent
COPILOT_URL = "https://api.githubcopilot.com/chat/completions"
MAX_HISTORY = 80
MAX_TOOL_ITER = 12

# Matches Windows file paths like C:\Users\... or D:\data\file.xlsx
_FILE_PATH_RE = re.compile(r'[A-Za-z]:\\[^\s\n"\']{5,}')


# ── GitHub Copilot client ─────────────────────────────────────────────────────

class CopilotClient:
    def __init__(self, model: str):
        self.model = model
        self._token: Optional[str] = None
        self._token_ts: float = 0.0

    def _token_fresh(self) -> str:
        now = time.time()
        if self._token and (now - self._token_ts) < 3000:
            return self._token
        r = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode != 0:
            raise RuntimeError(f"gh auth token failed: {r.stderr.strip()}")
        self._token = r.stdout.strip()
        self._token_ts = now
        return self._token

    def complete(
        self, messages: list, tools: list | None = None, tool_choice="auto"
    ) -> dict:
        for attempt in range(3):
            try:
                token = self._token_fresh()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Copilot-Integration-Id": "vscode-chat",
                }
                token_key = "max_completion_tokens" if self.model.startswith("gpt") else "max_tokens"
                body: dict = {
                    "model": self.model,
                    "messages": messages,
                    token_key: 4096,
                    "stream": False,
                }
                if tools:
                    body["tools"] = tools
                    body["tool_choice"] = tool_choice

                resp = requests.post(
                    COPILOT_URL, headers=headers, json=body, timeout=120
                )
                if resp.status_code == 401:
                    self._token = None
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                log.warning(f"API error (retry {attempt+1}): {e}")
                time.sleep(2)
        raise RuntimeError("Copilot API failed after 3 retries")


# ── Session ───────────────────────────────────────────────────────────────────

class Session:
    def __init__(self, name: str, data_dir: Path):
        self.name = name
        sess_dir = data_dir / "sessions"
        sess_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        h8 = hashlib.md5(name.encode("utf-8")).hexdigest()[:8]
        matches = list(sess_dir.glob(f"*{h8}.json"))
        if matches:
            self._path = matches[0]
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {}
        else:
            safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
            self._path = sess_dir / f"{safe}_{h8}.json"
            self._data = {}

        self._data.setdefault("sessions", {})
        if "s1" not in self._data["sessions"]:
            self._data["sessions"]["s1"] = {"history": [], "agent_session_id": ""}

    def get_history(self) -> list:
        with self._lock:
            return list(self._data["sessions"]["s1"].get("history") or [])

    def append(
        self,
        role: str,
        content,
        tool_calls: list | None = None,
        tool_call_id: str | None = None,
        name: str | None = None,
    ) -> None:
        entry: dict = {
            "role": role,
            "content": content,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        if tool_calls is not None:
            entry["tool_calls"] = tool_calls
        if tool_call_id is not None:
            entry["tool_call_id"] = tool_call_id
        if name is not None:
            entry["name"] = name
        with self._lock:
            self._data["sessions"]["s1"]["history"].append(entry)
            self._save_locked()

    def _save_locked(self) -> None:
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(self._path)


# ── Tools ─────────────────────────────────────────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read file contents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "start_line": {"type": "integer"},
                    "end_line": {"type": "integer"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_dir",
            "description": "List files in a directory.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write content to a file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                    "mode": {"type": "string", "enum": ["overwrite", "append"]},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_command",
            "description": "Execute a shell command.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "cwd": {"type": "string"},
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_file",
            "description": (
                "MANDATORY: Send a file or image to the user in this Feishu chat. "
                "You MUST call this function — do NOT reply with text saying you will send or have sent a file. "
                "Call this immediately when the user asks to send, share, forward, or show any file or image. "
                "Extract the file path from the user's message and pass it as 'path'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute or relative path to the file"},
                },
                "required": ["path"],
            },
        },
    },
]


# ── Permission Manager ────────────────────────────────────────────────────────

class PermManager:
    def __init__(self):
        self._always: dict[str, set] = {}
        self._pending: dict[str, tuple] = {}
        self._lock = threading.Lock()

    def has_always(self, user_id: str, tool: str) -> bool:
        with self._lock:
            return tool in self._always.get(user_id, set())

    def grant_always(self, user_id: str, tool: str) -> None:
        with self._lock:
            self._always.setdefault(user_id, set()).add(tool)

    def register(self, token: str) -> tuple[threading.Event, dict]:
        ev = threading.Event()
        holder: dict = {"result": None}
        with self._lock:
            self._pending[token] = (ev, holder)
        return ev, holder

    def resolve(self, token: str, result: str, user_id: str, tool: str) -> bool:
        with self._lock:
            item = self._pending.pop(token, None)
        if not item:
            return False
        ev, holder = item
        holder["result"] = result
        if result == "allow_all":
            self.grant_always(user_id, tool)
        ev.set()
        return True


# ── Card Callback HTTP Server ─────────────────────────────────────────────────

class CardCallbackServer:
    """Minimal HTTP server that receives Feishu card button callbacks.

    Configure each bot app's 卡片请求网址 to http://<VM_IP>:9810/
    """

    def __init__(self, perm: PermManager, port: int = 9810):
        self._perm = perm
        self._port = port

    def start(self) -> None:
        perm = self._perm

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self_h) -> None:
                try:
                    length = int(self_h.headers.get("Content-Length", 0))
                    raw = self_h.rfile.read(length)
                    data = json.loads(raw.decode("utf-8"))

                    # URL verification handshake
                    if data.get("type") == "url_verification":
                        resp_bytes = json.dumps({"challenge": data.get("challenge", "")}).encode()
                        self_h.send_response(200)
                        self_h.send_header("Content-Type", "application/json; charset=utf-8")
                        self_h.end_headers()
                        self_h.wfile.write(resp_bytes)
                        return

                    # Card action callback
                    action = data.get("action", {})
                    val = action.get("value") or {}
                    token = val.get("token", "")
                    act   = val.get("action", "")
                    tool  = val.get("tool", "")
                    op    = data.get("operator", {})
                    uid   = op.get("open_id", "") or op.get("user_id", "")

                    perm.resolve(token, act, uid, tool)

                    _LABELS = {"allow": "已允许", "allow_all": "已始终允许", "deny": "已拒绝"}
                    resp_body = json.dumps({
                        "toast": {
                            "type": "success" if act != "deny" else "info",
                            "content": _LABELS.get(act, act),
                        }
                    }, ensure_ascii=False).encode("utf-8")
                    self_h.send_response(200)
                    self_h.send_header("Content-Type", "application/json; charset=utf-8")
                    self_h.end_headers()
                    self_h.wfile.write(resp_body)

                except Exception as e:
                    log.warning(f"card callback error: {e}")
                    self_h.send_response(500)
                    self_h.end_headers()

            def log_message(self_h, *args) -> None:
                pass  # suppress default HTTP access logs

        server = HTTPServer(("0.0.0.0", self._port), _Handler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        log.info(f"Card callback server listening on port {self._port}")


# ── EventDispatcher card.action.trigger patch ────────────────────────────────

def _patch_event_handler_for_cards(handler: lark.EventDispatcherHandler, perm: "PermManager") -> None:
    """Patch EventDispatcherHandler.do_without_validation to handle card.action.trigger.

    Feishu delivers card button clicks as MessageType.EVENT with event_type
    'card.action.trigger'. The SDK raises 'processor not found' because no
    handler is registered for that type. We intercept before the lookup,
    resolve the permission token, and return the toast dict. The ws.Client
    base64-encodes the return value and sends it back to Feishu automatically.
    """
    _orig = handler.do_without_validation

    def _patched(payload: bytes):
        try:
            data = json.loads(payload.decode("utf-8"))
            hdr = data.get("header", {})
            if hdr.get("event_type") == "card.action.trigger":
                event  = data.get("event", {})
                action = event.get("action", {})
                val    = action.get("value") or {}
                token  = val.get("token", "")
                act    = val.get("action", "")
                tool   = val.get("tool", "")
                op     = event.get("operator", {})
                uid    = op.get("open_id", "")
                perm.resolve(token, act, uid, tool)
                _LABELS = {"allow": "已允许", "allow_all": "已始终允许", "deny": "已拒绝"}
                _ICONS  = {"allow": "✅", "allow_all": "✅", "deny": "❌"}
                label = _LABELS.get(act, act)
                icon  = _ICONS.get(act, "")
                return {
                    "toast": {
                        "type": "success" if act != "deny" else "info",
                        "content": label,
                    },
                    "card": {
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"{icon} {label}（{tool}）",
                                },
                            }
                        ]
                    },
                }
        except Exception as e:
            log.warning(f"card action handler error: {e}")
        return _orig(payload)

    handler.do_without_validation = _patched


# ── Tool Executor ─────────────────────────────────────────────────────────────

class ToolExecutor:
    def __init__(self, work_dir: str, send_card_fn, perm: PermManager, send_file_fn=None):
        self.work_dir = Path(work_dir)
        self._send_card = send_card_fn
        self._send_file = send_file_fn
        self.perm = perm

    def run(self, tool: str, args: dict, user_id: str, chat_id: str) -> str:
        if tool == "read_file":
            return self._read(**args)
        if tool == "list_dir":
            return self._listdir(**args)
        if tool == "write_file":
            result = self._guarded(tool, args, user_id, chat_id, self._write)
            if self._send_file and "已写入" in result:
                file_path = self._resolve(args.get("path", ""))
                try:
                    self._send_file(chat_id, file_path)
                except Exception as e:
                    log.warning(f"send_file after write error: {e}")
            return result
        if tool == "execute_command":
            return self._guarded(tool, args, user_id, chat_id, self._exec)
        if tool == "send_file":
            return self._do_send_file(chat_id, **args)
        return f"Unknown tool: {tool}"

    def _guarded(self, tool: str, args: dict, user_id: str, chat_id: str, fn) -> str:
        if self.perm.has_always(user_id, tool):
            return fn(**args)

        import secrets
        token = secrets.token_hex(8)
        if tool == "write_file":
            preview = f"写入文件: {args.get('path')}\n```\n{str(args.get('content',''))[:300]}\n```"
        else:
            preview = f"执行命令:\n```\n{args.get('command','')}\n```\n目录: {args.get('cwd', str(self.work_dir))}"

        card = self._build_card(token, preview, tool)
        self._send_card(chat_id, card)

        ev, holder = self.perm.register(token)
        ev.wait(timeout=60)

        decision = holder.get("result")
        if decision is None:
            return "操作超时（60s），用户未响应，已跳过"
        if decision == "deny":
            return "用户拒绝了此操作"
        return fn(**args)

    def _build_card(self, token: str, preview: str, tool: str) -> dict:
        return {
            "config": {"wide_screen_mode": False},
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"🔐 **授权请求**\n{preview}",
                    },
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "允许一次"},
                            "type": "primary",
                            "size": "small",
                            "value": {"action": "allow", "token": token, "tool": tool},
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "始终允许"},
                            "type": "default",
                            "size": "small",
                            "value": {"action": "allow_all", "token": token, "tool": tool},
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "拒绝"},
                            "type": "danger",
                            "size": "small",
                            "value": {"action": "deny", "token": token, "tool": tool},
                        },
                    ],
                },
            ],
        }

    def _resolve(self, path: str) -> Path:
        p = Path(path)
        return p if p.is_absolute() else self.work_dir / p

    def _read(self, path: str, start_line: int | None = None, end_line: int | None = None) -> str:
        p = self._resolve(path)
        if not p.exists():
            return f"文件不存在: {p}"
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            return f"读取失败: {e}"
        if start_line or end_line:
            lines = text.splitlines(keepends=True)
            s = (start_line or 1) - 1
            e = end_line or len(lines)
            text = "".join(lines[s:e])
        return text

    def _do_send_file(self, chat_id: str, path: str) -> str:
        if not self._send_file:
            return "文件发送功能未配置"
        file_path = self._resolve(path)
        if not file_path.exists():
            return f"文件不存在: {file_path}"
        try:
            self._send_file(chat_id, file_path)
            return f"已发送文件: {file_path.name}"
        except Exception as e:
            return f"发送失败: {e}"

    def _listdir(self, path: str) -> str:
        p = self._resolve(path)
        if not p.exists():
            return f"目录不存在: {p}"
        try:
            items = sorted(p.iterdir())
            rows = []
            for item in items:
                kind = "DIR " if item.is_dir() else "FILE"
                size = f"{item.stat().st_size:>10}" if item.is_file() else "          "
                rows.append(f"{kind}  {size}  {item.name}")
            return "\n".join(rows) or "(empty)"
        except Exception as e:
            return f"列目录失败: {e}"

    def _write(self, path: str, content: str, mode: str = "overwrite") -> str:
        p = self._resolve(path)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            if mode == "append":
                with open(p, "a", encoding="utf-8") as f:
                    f.write(content)
            else:
                p.write_text(content, encoding="utf-8")
            return f"已写入 {p} ({len(content)} 字符)"
        except Exception as e:
            return f"写入失败: {e}"

    def _exec(self, command: str, cwd: str | None = None) -> str:
        wd = Path(cwd) if cwd else self.work_dir
        try:
            r = subprocess.run(
                command, shell=True, capture_output=True,
                text=True, timeout=60, cwd=str(wd),
            )
            parts = []
            if r.stdout.strip():
                parts.append(r.stdout.strip())
            if r.stderr.strip():
                parts.append(f"[stderr]\n{r.stderr.strip()}")
            return "\n".join(parts) or "(无输出)"
        except subprocess.TimeoutExpired:
            return "命令超时 (60s)"
        except Exception as e:
            return f"执行失败: {e}"


# ── Agent ─────────────────────────────────────────────────────────────────────

class BotAgent:
    def __init__(
        self,
        name: str,
        work_dir: str,
        session: Session,
        copilot: CopilotClient,
        tool_exec: ToolExecutor,
    ):
        self.name = name
        self.work_dir = Path(work_dir)
        self.session = session
        self.copilot = copilot
        self.tool_exec = tool_exec
        self._log = logging.getLogger(f"agent.{name}")

    def _system_prompt(self) -> str:
        memory_file = self.work_dir / "memory.md"
        mem = ""
        if memory_file.exists():
            try:
                mem = memory_file.read_text(encoding="utf-8", errors="replace")[:8000]
            except Exception:
                pass
        base = (
            f"你是{self.name}，运行在飞书上的 AI 助手。"
            f"工作目录：{self.work_dir}。"
            "可用工具：read_file、list_dir、send_file（自动执行）；"
            "write_file、execute_command（需要用户确认）。"
            "【重要】用户要你发文件时，必须立即调用 send_file 工具，"
            "绝对不能只用文字回复，不能说'我会发给你'，必须真正调用工具发送。"
        )
        if mem:
            base += f"\n\n---\n# Memory\n{mem}"
        return base


    def _to_api_msgs(self, new_content: str) -> list:
        sys_msg = {"role": "system", "content": self._system_prompt()}
        history = self.session.get_history()
        api_hist = []
        for e in history[-MAX_HISTORY:]:
            m: dict = {"role": e["role"]}
            if e.get("content") is not None:
                m["content"] = e["content"]
            if e.get("tool_calls"):
                m["tool_calls"] = e["tool_calls"]
            if e.get("tool_call_id"):
                m["tool_call_id"] = e["tool_call_id"]
            if e.get("name"):
                m["name"] = e["name"]
            api_hist.append(m)
        return [sys_msg] + api_hist + [{"role": "user", "content": new_content}]

    def process(self, user_id: str, chat_id: str, content: str) -> str:
        messages = self._to_api_msgs(content)
        self.session.append("user", content)

        for iteration in range(MAX_TOOL_ITER):
            try:
                resp = self.copilot.complete(messages, tools=TOOLS)
            except Exception as e:
                self._log.error(f"API error: {e}")
                err = f"抱歉，调用 AI 出错：{e}"
                self.session.append("assistant", err)
                return err

            choice = resp["choices"][0]
            finish = choice.get("finish_reason", "")
            msg = choice["message"]

            if finish == "tool_calls" or msg.get("tool_calls"):
                tcs = msg.get("tool_calls") or []
                if not tcs:
                    reply = msg.get("content") or ""
                    self.session.append("assistant", reply)
                    return reply
                self.session.append("assistant", msg.get("content"), tool_calls=tcs)
                messages.append(
                    {"role": "assistant", "content": msg.get("content"), "tool_calls": tcs}
                )
                for tc in tcs:
                    tc_id = tc["id"]
                    fn = tc["function"]["name"]
                    try:
                        fn_args = json.loads(tc["function"]["arguments"])
                    except Exception:
                        fn_args = {}
                    self._log.info(f"Tool: {fn}({list(fn_args.keys())})")
                    result = self.tool_exec.run(fn, fn_args, user_id, chat_id)
                    self.session.append("tool", result, tool_call_id=tc_id, name=fn)
                    messages.append(
                        {"role": "tool", "tool_call_id": tc_id, "name": fn, "content": result}
                    )
                continue

            reply = msg.get("content") or ""
            self.session.append("assistant", reply)
            return reply

        stop = "（工具调用次数超限，已停止）"
        self.session.append("assistant", stop)
        return stop


# ── Feishu Bot ────────────────────────────────────────────────────────────────

class FeishuBot:
    def __init__(self, proj: dict, data_dir: Path, perm: PermManager):
        self.name: str = proj["name"]
        opts = proj["agent"]["options"]
        args_list: list = opts.get("args", [])
        try:
            idx = args_list.index("--model")
            self.model: str = args_list[idx + 1]
        except (ValueError, IndexError):
            self.model = "claude-opus-4.7"

        self.work_dir: str = opts["work_dir"]

        plat = proj["platforms"][0]["options"]
        self.app_id: str = plat["app_id"]
        self.app_secret: str = plat["app_secret"]

        self._log = logging.getLogger(f"bot.{self.name}")
        self._perm = perm

        self._client = (
            lark.Client.builder()
            .app_id(self.app_id)
            .app_secret(self.app_secret)
            .build()
        )
        self._session = Session(self.name, data_dir)
        self._copilot = CopilotClient(self.model)
        self._tool_exec = ToolExecutor(self.work_dir, self._send_card, perm, self._send_file)
        self._agent = BotAgent(
            name=self.name,
            work_dir=self.work_dir,
            session=self._session,
            copilot=self._copilot,
            tool_exec=self._tool_exec,
        )
        self._qs: dict[str, queue.Queue] = {}
        self._qs_lock = threading.Lock()

    # ── send helpers ───────────────────────────────────────────────────────────

    _IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
    _FILE_TYPE_MAP = {
        ".pdf": "pdf", ".doc": "doc", ".docx": "doc",
        ".xls": "xls", ".xlsx": "xls", ".ppt": "ppt", ".pptx": "ppt",
        ".mp4": "mp4", ".mp3": "mp3",
    }

    def _send_file(self, chat_id: str, file_path: Path) -> None:
        if not file_path.exists():
            self._log.warning(f"send_file: {file_path} not found")
            return
        ext = file_path.suffix.lower()
        try:
            if ext in self._IMAGE_EXTS:
                with open(file_path, "rb") as fh:
                    req = (
                        lark.im.v1.CreateImageRequest.builder()
                        .request_body(
                            lark.im.v1.CreateImageRequestBody.builder()
                            .image_type("message")
                            .image(fh)
                            .build()
                        )
                        .build()
                    )
                    r = self._client.im.v1.image.create(req)
                if not r.success():
                    self._log.warning(f"image upload failed: {r.msg}")
                    return
                content = json.dumps({"image_key": r.data.image_key}, ensure_ascii=False)
                msg_type = "image"
            else:
                file_type = self._FILE_TYPE_MAP.get(ext, "stream")
                with open(file_path, "rb") as fh:
                    req = (
                        lark.im.v1.CreateFileRequest.builder()
                        .request_body(
                            lark.im.v1.CreateFileRequestBody.builder()
                            .file_type(file_type)
                            .file_name(file_path.name)
                            .file(fh)
                            .build()
                        )
                        .build()
                    )
                    r = self._client.im.v1.file.create(req)
                if not r.success():
                    self._log.warning(f"file upload failed: {r.msg}")
                    return
                content = json.dumps({"file_key": r.data.file_key}, ensure_ascii=False)
                msg_type = "file"

            req2 = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type(msg_type)
                    .content(content)
                    .build()
                )
                .build()
            )
            r2 = self._client.im.v1.message.create(req2)
            if not r2.success():
                self._log.warning(f"send file msg failed: {r2.msg}")
            else:
                self._log.info(f"sent file {file_path.name} to {chat_id}")
        except Exception as e:
            self._log.error(f"_send_file error: {e}", exc_info=True)

    def _send_text(self, chat_id: str, text: str) -> None:
        MAX_LEN = 4000
        chunks = [text[i:i + MAX_LEN] for i in range(0, max(1, len(text)), MAX_LEN)]
        for chunk in chunks:
            card = {
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": chunk}}
                ]
            }
            req = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("interactive")
                    .content(json.dumps(card, ensure_ascii=False))
                    .build()
                )
                .build()
            )
            r = self._client.im.v1.message.create(req)
            if not r.success():
                self._log.error(f"send_text failed: {r.msg}")

    def _send_card(self, chat_id: str, card: dict) -> None:
        req = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("interactive")
                .content(json.dumps(card, ensure_ascii=False))
                .build()
            )
            .build()
        )
        r = self._client.im.v1.message.create(req)
        if not r.success():
            self._log.error(f"send_card failed: {r.msg}")

    # ── event handlers ─────────────────────────────────────────────────────────

    def _react(self, message_id: str) -> None:
        # Run in a background thread to avoid blocking the asyncio event loop
        threading.Thread(target=self._react_worker, args=(message_id,), daemon=True).start()

    def _react_worker(self, message_id: str) -> None:
        if not message_id:
            return
        try:
            req = (
                lark.im.v1.CreateMessageReactionRequest.builder()
                .message_id(message_id)
                .request_body(
                    lark.im.v1.CreateMessageReactionRequestBody.builder()
                    .reaction_type(
                        lark.im.v1.Emoji.builder().emoji_type("THUMBSUP").build()
                    )
                    .build()
                )
                .build()
            )
            r = self._client.im.v1.message_reaction.create(req)
            if not r.success():
                self._log.warning(f"react failed [{message_id}]: code={r.code} msg={r.msg}")
        except Exception as e:
            self._log.warning(f"react error [{message_id}]: {e}")

    def _on_message(self, data: lark.im.v1.P2ImMessageReceiveV1) -> None:
        try:
            msg = data.event.message
            sender = data.event.sender
            sender_id = ""
            if sender and sender.sender_id:
                sender_id = sender.sender_id.open_id or ""
            chat_id = msg.chat_id or ""
            chat_type = msg.chat_type or "p2p"

            if msg.message_type != "text":
                return

            try:
                content_obj = json.loads(msg.content)
            except Exception:
                return

            text: str = content_obj.get("text", "").strip()

            if chat_type == "group":
                mentions = msg.mentions or []
                if not mentions:
                    return
                mentioned_me = any(
                    getattr(m, "key", "") in text for m in mentions
                )
                if not mentioned_me:
                    return
                for m in mentions:
                    key = getattr(m, "key", "")
                    if key:
                        text = text.replace(key, "").strip()

            if not text:
                return

            self._log.info(f"msg from {sender_id}: {text[:60]}")
            self._react(msg.message_id)
            q = self._get_queue(sender_id)
            q.put((sender_id, chat_id, text))
        except Exception as e:
            self._log.error(f"_on_message error: {e}", exc_info=True)

    def _on_card_action(self, data) -> dict:
        try:
            val = data.action.value or {}
            token = val.get("token", "")
            action = val.get("action", "")
            tool = val.get("tool", "")
            user_id = ""
            op = getattr(data, "operator", None)
            if op:
                user_id = getattr(op, "open_id", "") or ""
            resolved = self._perm.resolve(token, action, user_id, tool)
            if resolved:
                labels = {
                    "allow": "已允许",
                    "allow_all": "已始终允许",
                    "deny": "已拒绝",
                }
                label = labels.get(action, action)
                return {
                    "toast": {
                        "type": "success" if action != "deny" else "info",
                        "content": label,
                    }
                }
        except Exception as e:
            self._log.error(f"_on_card_action error: {e}")
        return {}

    # ── queue processing ───────────────────────────────────────────────────────

    def _get_queue(self, user_id: str) -> queue.Queue:
        with self._qs_lock:
            if user_id not in self._qs:
                q: queue.Queue = queue.Queue()
                self._qs[user_id] = q
                t = threading.Thread(
                    target=self._drain_queue, args=(user_id, q), daemon=True
                )
                t.start()
            return self._qs[user_id]

    def _drain_queue(self, user_id: str, q: queue.Queue) -> None:
        while True:
            try:
                sender_id, chat_id, text = q.get(timeout=600)
            except queue.Empty:
                continue
            try:
                reply = self._agent.process(sender_id, chat_id, text)
                self._send_text(chat_id, reply)
            except Exception as e:
                self._log.error(f"process error: {e}", exc_info=True)
                try:
                    self._send_text(chat_id, f"处理出错：{e}")
                except Exception:
                    pass
            finally:
                q.task_done()

    # ── async ws runner ────────────────────────────────────────────────────────

    async def run_forever(self) -> None:
        """Run this bot's WebSocket connection as an asyncio coroutine.
        All bots share the single event loop created by asyncio.run().
        We bypass ws.Client.start() (which calls loop.run_until_complete and
        blocks) and call the underlying coroutines directly after patching
        the lark_oapi.ws.client module-level loop variable.
        """
        import lark_oapi.ws.client as _ws_mod
        import asyncio

        while True:
            try:
                self._log.info("连接 WebSocket ...")
                event_handler = (
                    lark.EventDispatcherHandler.builder("", "")
                    .register_p2_im_message_receive_v1(self._on_message)
                    .build()
                )
                # Patch event handler to handle card.action.trigger over WebSocket
                _patch_event_handler_for_cards(event_handler, self._perm)
                ws = lark.ws.Client(
                    self.app_id,
                    self.app_secret,
                    event_handler=event_handler,
                    log_level=lark.LogLevel.ERROR,
                )
                # Patch module-level loop so ws internal code uses the running loop
                _ws_mod.loop = asyncio.get_running_loop()
                await ws._connect()
                self._log.info("WebSocket 已连接，等待消息")
                asyncio.create_task(ws._ping_loop())
                await _ws_mod._select()   # suspends forever (sleeps 3600s in loop)
            except Exception as e:
                self._log.error(f"WebSocket 断开: {e}，10s 后重连")
                await asyncio.sleep(10)


# ── Main ──────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    cfg = BASE_DIR / "config.toml"
    with open(cfg, "rb") as f:
        return tomllib.load(f)


async def _main_async() -> None:
    import asyncio

    cfg = load_config()
    data_dir = Path(cfg["data_dir"])
    perm = PermManager()

    card_port = cfg.get("card_callback_port", 9810)
    CardCallbackServer(perm, port=card_port).start()

    bots: list[FeishuBot] = []
    for proj in cfg.get("projects", []):
        try:
            bot = FeishuBot(proj, data_dir, perm)
            bots.append(bot)
            log.info(f"初始化: {bot.name} [{bot.model}]")
        except Exception as e:
            log.error(f"初始化 bot {proj.get('name')} 失败: {e}")

    if not bots:
        log.error("没有可用的 bot，退出")
        sys.exit(1)

    log.info(f"共 {len(bots)} 个 bot，启动中...")
    await asyncio.gather(*[bot.run_forever() for bot in bots])


def main() -> None:
    try:
        import asyncio
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        log.info("收到中断，退出")


if __name__ == "__main__":
    main()
