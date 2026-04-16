"""
StockSage WeChat Bot Server
===========================
企业微信自建应用 callback server + 简单 REST API 触发器

启动方式:
    python wechat-bot/server.py

企业微信配置:
    1. 登录 work.weixin.qq.com → 应用管理 → 自建 → 创建应用
    2. 在"接收消息"中设置 URL = http://YOUR_IP:5050/wechat
    3. 把 Token 和 EncodingAESKey 填入 wechat-bot/config.json
    4. 设置可见范围为你自己

支持命令 (微信发送):
    帮助 / help      - 列出所有命令
    状态 / status    - 查看系统状态（进程、最近日志）
    持仓             - 推送当前持仓盈亏
    信号 / 扫盘      - 立即触发一次 monitor 扫描
    今日推荐          - 推送 latest_picks.json
    日志 [N]         - 显示 monitor 最近 N 条日志（默认20）
    重启 monitor     - 重启 monitor.py 循环
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from flask import Flask, request, Response

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parent.parent          # stocksage-alpha/
BOT_DIR   = Path(__file__).resolve().parent                 # wechat-bot/
CFG_PATH  = BOT_DIR / "config.json"
ALERT_CFG = ROOT / "alert_config.json"
SCRIPTS   = ROOT / "scripts"

sys.path.insert(0, str(SCRIPTS))

# ── Config ────────────────────────────────────────────────────────────────────
def _load_config() -> dict:
    if not CFG_PATH.exists():
        raise FileNotFoundError(f"缺少配置文件: {CFG_PATH}\n请先复制 config.json.example 并填入企业微信凭据")
    return json.loads(CFG_PATH.read_text(encoding="utf-8"))

def _load_alert_cfg() -> dict:
    if not ALERT_CFG.exists():
        return {}
    return json.loads(ALERT_CFG.read_text(encoding="utf-8"))

# ── WeChat push (outbound via PushPlus/ServerChan) ────────────────────────────
def _push(title: str, body: str) -> None:
    """Send reply back to user via existing PushPlus / ServerChan."""
    try:
        from common import send_wechat, configure_pushplus
        cfg = _load_alert_cfg()
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        send_wechat(title, body, sendkey)
    except Exception as e:
        print(f"[WARN] push failed: {e}")

# ── WeCom signature verification ──────────────────────────────────────────────
def _verify_signature(token: str, timestamp: str, nonce: str, signature: str) -> bool:
    items = sorted([token, timestamp, nonce])
    s = hashlib.sha1("".join(items).encode("utf-8")).hexdigest()
    return s == signature

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/wechat", methods=["GET"])
def wechat_verify():
    """WeCom URL verification handshake."""
    cfg          = _load_config()
    token        = cfg.get("wechat", {}).get("token", "")
    timestamp    = request.args.get("timestamp", "")
    nonce        = request.args.get("nonce", "")
    signature    = request.args.get("msg_signature") or request.args.get("signature", "")
    echostr      = request.args.get("echostr", "")

    if _verify_signature(token, timestamp, nonce, signature):
        print(f"[OK] WeCom URL verified at {datetime.now():%H:%M:%S}")
        return Response(echostr, mimetype="text/plain")
    print("[WARN] WeCom signature mismatch")
    return Response("forbidden", status=403)


@app.route("/wechat", methods=["POST"])
def wechat_receive():
    """Receive WeCom message, dispatch command in background."""
    cfg       = _load_config()
    token     = cfg.get("wechat", {}).get("token", "")
    timestamp = request.args.get("timestamp", "")
    nonce     = request.args.get("nonce", "")
    signature = request.args.get("msg_signature") or request.args.get("signature", "")

    if not _verify_signature(token, timestamp, nonce, signature):
        print("[WARN] WeCom signature mismatch on POST")
        return Response("forbidden", status=403)

    try:
        xml_str = request.data.decode("utf-8")
        root    = ET.fromstring(xml_str)
        msg_type  = (root.findtext("MsgType") or "").strip()
        content   = (root.findtext("Content") or "").strip()
        from_user = (root.findtext("FromUserName") or "").strip()
    except Exception as e:
        print(f"[WARN] XML parse error: {e}")
        return Response("", status=200)

    if msg_type == "text" and content:
        print(f"[MSG] from={from_user} content={content!r}")
        threading.Thread(
            target=_dispatch, args=(content,), daemon=True
        ).start()

    return Response("", status=200)


@app.route("/api/cmd", methods=["GET", "POST"])
def api_trigger():
    """
    Simple REST trigger (fallback — no WeCom needed).
    GET  /api/cmd?token=SECRET&cmd=状态
    POST /api/cmd  JSON: {"token": "SECRET", "cmd": "信号"}
    """
    cfg    = _load_config()
    secret = cfg.get("api_token", "")
    if not secret:
        return Response("api_token not configured", status=500)

    if request.method == "GET":
        token = request.args.get("token", "")
        cmd   = request.args.get("cmd", "")
    else:
        data  = request.get_json(force=True, silent=True) or {}
        token = data.get("token", "")
        cmd   = data.get("cmd", "")

    if token != secret:
        return Response("forbidden", status=403)
    if not cmd:
        return Response("missing cmd", status=400)

    print(f"[API] cmd={cmd!r}")
    threading.Thread(target=_dispatch, args=(cmd,), daemon=True).start()
    return Response(json.dumps({"status": "dispatched", "cmd": cmd}),
                    mimetype="application/json")


# ── Command dispatch ──────────────────────────────────────────────────────────

_CMD_HELP = """**StockSage 可用命令**

- `帮助` — 显示此帮助
- `状态` — 系统进程 & 最近日志
- `持仓` — 触发持仓盈亏推送
- `信号` / `扫盘` — 立即扫描买卖信号
- `今日推荐` — 当日选股结果
- `日志 [N]` — monitor 最近 N 条日志
- `重启 monitor` — 重启 monitor.py 循环
"""

def _dispatch(text: str) -> None:
    t = text.strip()
    try:
        if t in ("帮助", "help", "？", "?"):
            _push("[StockSage Bot]", _CMD_HELP)

        elif t in ("状态", "status"):
            _cmd_status()

        elif t in ("持仓",):
            _cmd_holdings()

        elif t in ("信号", "扫盘", "scan"):
            _cmd_scan()

        elif t in ("今日推荐", "推荐", "picks"):
            _cmd_picks()

        elif t.startswith("日志"):
            parts = t.split()
            n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20
            _cmd_logs(n)

        elif t in ("重启 monitor", "重启monitor", "restart monitor"):
            _cmd_restart_monitor()

        else:
            _push("[StockSage Bot]", f"未知命令: `{t}`\n\n发送 `帮助` 查看可用命令。")

    except Exception as e:
        _push("[StockSage Bot ❌]", f"命令执行出错: {e}")


def _cmd_status() -> None:
    lines = [f"**系统状态** @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]

    # Running python processes
    r = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/V"],
        capture_output=True, text=True
    )
    procs = []
    for line in r.stdout.strip().splitlines()[1:]:
        parts = line.strip('"').split('","')
        if len(parts) >= 8:
            pid, cpu = parts[1], parts[7]
            procs.append(f"  PID {pid} | CPU {cpu}")
    if procs:
        lines.append("**Python 进程:**")
        lines.extend(procs)
    else:
        lines.append("无运行中的 Python 进程")

    # Latest monitor log (last 5 lines)
    log_path = SCRIPTS / "monitor_loop.log"
    if log_path.exists():
        tail = log_path.read_bytes()[-3000:].decode("utf-8", errors="replace")
        last_lines = [l for l in tail.splitlines() if l.strip()][-5:]
        lines.append("\n**Monitor 最近日志:**")
        lines.extend(f"  {l}" for l in last_lines)
    else:
        lines.append("\nmonitor_loop.log 不存在（monitor 可能未运行）")

    _push("[StockSage 状态]", "\n".join(lines))


def _cmd_holdings() -> None:
    _push("[StockSage]", "正在触发持仓推送，请稍候…")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--sell-only", "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


def _cmd_scan() -> None:
    _push("[StockSage]", "正在扫描买卖信号，请稍候…")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


def _cmd_picks() -> None:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        _push("[StockSage]", "latest_picks.json 不存在，可能今日尚未运行选股。")
        return
    data = json.loads(picks_path.read_text(encoding="utf-8"))
    results = data.get("results", [])
    date    = data.get("date", "?")
    lines   = [f"**今日推荐** ({date})\n"]
    for i, s in enumerate(results[:10], 1):
        name  = s.get("name") or s.get("code", "?")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {name}  (得分 {score:.3f})")
    _push("[StockSage 选股]", "\n".join(lines))


def _cmd_logs(n: int = 20) -> None:
    log_path = SCRIPTS / "monitor_loop.log"
    if not log_path.exists():
        _push("[StockSage]", "monitor_loop.log 不存在。")
        return
    tail = log_path.read_bytes()[-8000:].decode("utf-8", errors="replace")
    last = [l for l in tail.splitlines() if l.strip()][-n:]
    body = "\n".join(last) or "(空)"
    _push(f"[StockSage 日志 -{n}]", f"```\n{body}\n```")


def _cmd_restart_monitor() -> None:
    # Kill existing monitor
    r = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/V"],
        capture_output=True, text=True
    )
    for line in r.stdout.strip().splitlines()[1:]:
        parts = line.strip('"').split('","')
        if len(parts) >= 9 and "monitor" in parts[8].lower():
            pid = int(parts[1])
            try:
                import os, signal
                os.kill(pid, signal.SIGTERM)
                print(f"[INFO] killed monitor PID {pid}")
            except Exception:
                pass

    time.sleep(2)
    log_path = SCRIPTS / "monitor_loop.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Restarted by wechat-bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")

    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
    )
    _push("[StockSage]", "monitor.py 已重启 ✅")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    cfg  = _load_config()
    port = cfg.get("port", 5050)
    print(f"[StockSage Bot] 启动于 http://0.0.0.0:{port}")
    print(f"  WeCom callback: http://YOUR_IP:{port}/wechat")
    print(f"  REST API:       http://YOUR_IP:{port}/api/cmd?token=SECRET&cmd=状态")
    app.run(host="0.0.0.0", port=port, debug=False)
