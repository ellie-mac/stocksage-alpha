"""
StockSage Telegram Bot
======================
通过 Telegram 远程控制 StockSage，支持任意网络环境。

配置:
    在 wechat-bot/config.json 的 "telegram" 节填入:
        bot_token     - @BotFather 给的 token
        allowed_ids   - 只接受这些 chat_id 的命令（留空则接受所有人）
                        首次运行时发任意消息，日志会打印你的 chat_id

启动:
    python wechat-bot/telegram_bot.py

支持命令:
    帮助 / help          列出所有命令
    状态 / status        查看进程 & 日志
    持仓                 触发持仓盈亏推送
    信号 / 扫盘          立即扫描买卖信号
    今日推荐 / 推荐       当日选股结果
    日志 [N]             monitor 最近 N 行日志（默认 20）
    重启 monitor         重启 monitor.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

ROOT    = Path(__file__).resolve().parent.parent
BOT_DIR = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

# ── Config ────────────────────────────────────────────────────────────────────
def _cfg() -> dict:
    return json.loads((BOT_DIR / "config.json").read_text(encoding="utf-8"))

def _tg_cfg() -> dict:
    return _cfg().get("telegram", {})

def _bot_token() -> str:
    t = _tg_cfg().get("bot_token", "")
    if not t or t.startswith("从@"):
        raise RuntimeError("请先在 wechat-bot/config.json 填入 telegram.bot_token")
    return t

def _allowed_ids() -> set[int]:
    ids = _tg_cfg().get("allowed_ids", [])
    return {int(i) for i in ids} if ids else set()

# ── Telegram API ──────────────────────────────────────────────────────────────
def _api(method: str, token: str, **params) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        r = requests.post(url, json=params, timeout=10)
        return r.json()
    except Exception as e:
        print(f"[WARN] Telegram API error: {e}")
        return {}

def _send(token: str, chat_id: int, text: str) -> None:
    # Split long messages
    MAX = 4000
    chunks = [text[i:i+MAX] for i in range(0, len(text), MAX)]
    for chunk in chunks:
        _api("sendMessage", token,
             chat_id=chat_id,
             text=chunk,
             parse_mode="Markdown")

# ── Command handlers ──────────────────────────────────────────────────────────
_HELP = """*StockSage 可用命令*

• `帮助` — 显示此帮助
• `状态` — 系统进程 & 最近日志
• `持仓` — 触发持仓盈亏推送
• `信号` / `扫盘` — 立即扫描买卖信号
• `今日推荐` — 当日选股结果
• `日志 [N]` — monitor 最近 N 条日志
• `重启 monitor` — 重启 monitor.py 循环
"""

def _h_status(token: str, chat_id: int) -> None:
    lines = [f"*系统状态* @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]
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
        lines.append("*Python 进程:*")
        lines.extend(procs)
    else:
        lines.append("无运行中的 Python 进程")

    log_path = SCRIPTS / "monitor_loop.log"
    if log_path.exists():
        tail = log_path.read_bytes()[-3000:].decode("utf-8", errors="replace")
        last = [l for l in tail.splitlines() if l.strip()][-5:]
        lines.append("\n*Monitor 最近日志:*")
        lines.extend(f"`{l}`" for l in last)
    else:
        lines.append("\nmonitor\\_loop.log 不存在（monitor 可能未运行）")

    _send(token, chat_id, "\n".join(lines))


def _h_holdings(token: str, chat_id: int) -> None:
    _send(token, chat_id, "正在触发持仓推送，稍候…")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--sell-only", "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


def _h_scan(token: str, chat_id: int) -> None:
    _send(token, chat_id, "正在扫描买卖信号，稍候…")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


def _h_picks(token: str, chat_id: int) -> None:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        _send(token, chat_id, "latest\\_picks.json 不存在，今日可能尚未选股。")
        return
    data  = json.loads(picks_path.read_text(encoding="utf-8"))
    items = data.get("results", [])
    date  = data.get("date", "?")
    lines = [f"*今日推荐* ({date})\n"]
    for i, s in enumerate(items[:10], 1):
        name  = s.get("name") or s.get("code", "?")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {name}  _(得分 {score:.3f})_")
    _send(token, chat_id, "\n".join(lines))


def _h_logs(token: str, chat_id: int, n: int = 20) -> None:
    log_path = SCRIPTS / "monitor_loop.log"
    if not log_path.exists():
        _send(token, chat_id, "monitor\\_loop.log 不存在。")
        return
    tail  = log_path.read_bytes()[-8000:].decode("utf-8", errors="replace")
    last  = [l for l in tail.splitlines() if l.strip()][-n:]
    body  = "\n".join(last) or "(空)"
    _send(token, chat_id, f"*日志 -{n}*\n```\n{body}\n```")


def _h_restart(token: str, chat_id: int) -> None:
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
                print(f"[INFO] killed monitor PID {parts[1]}")
            except Exception:
                pass
    time.sleep(2)
    log_path = SCRIPTS / "monitor_loop.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Restarted by Telegram bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
    )
    _send(token, chat_id, "monitor.py 已重启 ✅")


# ── Dispatch ──────────────────────────────────────────────────────────────────
def _dispatch(token: str, chat_id: int, text: str) -> None:
    t = text.strip().lstrip("/")
    try:
        if t in ("帮助", "help", "start", "?", "？"):
            _send(token, chat_id, _HELP)
        elif t in ("状态", "status"):
            _h_status(token, chat_id)
        elif t in ("持仓",):
            _h_holdings(token, chat_id)
        elif t in ("信号", "扫盘", "scan"):
            _h_scan(token, chat_id)
        elif t in ("今日推荐", "推荐", "picks"):
            _h_picks(token, chat_id)
        elif t.startswith("日志"):
            parts = t.split()
            n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20
            _h_logs(token, chat_id, n)
        elif t in ("重启 monitor", "重启monitor", "restart monitor", "重启"):
            _h_restart(token, chat_id)
        else:
            _send(token, chat_id,
                  f"未知命令: `{t}`\n\n发送 `帮助` 查看可用命令。")
    except Exception as e:
        _send(token, chat_id, f"❌ 执行出错: {e}")


# ── Polling loop ──────────────────────────────────────────────────────────────
def run() -> None:
    token      = _bot_token()
    allowed    = _allowed_ids()
    offset     = 0

    print(f"[StockSage Telegram Bot] 启动，长轮询中…")
    if allowed:
        print(f"  允许的 chat_id: {allowed}")
    else:
        print("  未设置 allowed_ids，接受所有人的消息")
    print("  在 Telegram 发 '帮助' 开始使用\n")

    while True:
        try:
            data = _api("getUpdates", token,
                        offset=offset, timeout=30, allowed_updates=["message"])
            if not data.get("ok"):
                time.sleep(5)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                if not msg:
                    continue

                chat_id = msg.get("chat", {}).get("id")
                text    = (msg.get("text") or "").strip()
                user    = msg.get("from", {})

                if not chat_id or not text:
                    continue

                print(f"[MSG] chat_id={chat_id} user={user.get('username','?')} text={text!r}")

                # Authorization check
                if allowed and chat_id not in allowed:
                    print(f"  → 未授权，忽略（你的 chat_id={chat_id}，加入 config.json allowed_ids 以授权）")
                    _send(token, chat_id, "❌ 未授权")
                    continue

                # First-time helper: print chat_id
                if not allowed:
                    print(f"  → 提示: 将 {chat_id} 加入 config.json allowed_ids 以限制访问")

                threading.Thread(
                    target=_dispatch, args=(token, chat_id, text), daemon=True
                ).start()

        except KeyboardInterrupt:
            print("\n[StockSage Telegram Bot] 停止")
            break
        except Exception as e:
            print(f"[WARN] polling error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run()
