#!/usr/bin/env python3
"""
notify_discord.py — 发送定时任务通知（Discord Webhook + 飞书）

用法：
    python -X utf8 scripts/notify_discord.py "任务名" "描述" [status]
    status: "" = 完成, "started" = 开始, "failed" = 失败
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_FAILURES_PATH = ROOT / "data" / "task_failures.json"

_SCHEDULE = [
    ("chip_Premarket",  "07:00", "筹码盘前兜底"),
    ("main_Morning",    "07:10", "主策略盘前兜底"),
    ("integrity_Check", "08:00", "数据完整性检查"),
    ("concept_Warm",    "08:30", "概念板块预热"),
    ("xhs_Morning",     "09:25", "盘前筹码推送 📱"),
    ("xhs_Midday",      "11:35", "午间筹码推送 📱"),
    ("xhs_Evening",     "15:30", "收盘筹码推送 📱"),
    ("market_Warm",     "15:35", "市场数据预热"),
    ("price_Prefetch",  "15:45", "价格历史预热"),
    ("daily_PerfLog",   "16:00", "收盘胜率对比 📱"),
    ("chip_Night",      "18:00", "筹码缓存预取"),
    ("main_Scan",       "18:30", "主策略扫盘 📱"),
    ("gc_Scan",         "19:30", "金叉策略扫描 📱"),
    ("chip_CadScan",    "20:30", "筹码扫描推送 📱"),
    ("main_Night",      "22:30", "财务缓存预热"),
]


def _load_failures() -> dict:
    try:
        return json.loads(_FAILURES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_failures(d: dict) -> None:
    _FAILURES_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")


def _pending_retries_line(plain: bool = False) -> str:
    failures = _load_failures()
    if not failures:
        return ""
    names = " · ".join(failures.keys())
    return f"⚠️ 待重跑: {names}"


def _remaining_today(after_name: str, plain: bool = False) -> str:
    now = datetime.now().strftime("%H:%M")
    names = [n for n, _, _ in _SCHEDULE]
    try:
        idx = names.index(after_name)
    except ValueError:
        idx = -1
    fmt = "{t} {n} — {desc}" if plain else "  `{t}` {n} — {desc}"
    remaining = [
        fmt.format(t=t, n=n, desc=desc)
        for i, (n, t, desc) in enumerate(_SCHEDULE)
        if i > idx and t > now
    ]
    if not remaining:
        return "今日任务全部完成 🎉"
    return "📋 剩余任务:\n" + "\n".join(remaining)


# ── Discord ───────────────────────────────────────────────────────────────────

def send_discord(webhook_url: str, task: str, desc: str, status: str = "") -> None:
    if status == "started":
        icon, word = "🚀", "开始"
    elif status == "failed":
        icon, word = "❌", "失败"
    else:
        icon, word = "✅", "完成"

    lines = [f"{icon} **{task}** {word}"]
    if desc:
        lines.append(desc)
    retry_line = _pending_retries_line()
    if retry_line:
        lines.append(retry_line)
    if status not in ("started", "failed"):
        lines.append(_remaining_today(task))
    content = "\n".join(lines)

    data = json.dumps({"content": content}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "DiscordBot (stocksage-alpha, 1.0)",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        print(f"[notify_discord] 已发送: {task}/{status or 'ok'} (HTTP {r.status})", flush=True)


# ── Feishu ────────────────────────────────────────────────────────────────────

def _feishu_token(app_id: str, app_secret: str) -> str:
    data = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        resp = json.loads(r.read().decode("utf-8"))
    if resp.get("code") != 0:
        raise RuntimeError(f"feishu token error: {resp}")
    return resp["tenant_access_token"]


def send_feishu(chat_id: str, token: str, task: str, desc: str, status: str) -> None:
    if status == "started":
        icon, word = "🚀", "开始"
    elif status == "failed":
        icon, word = "❌", "失败"
    else:
        icon, word = "✅", "完成"

    lines = [f"{icon} {task} {word}"]
    if desc:
        lines.append(desc)
    retry_line = _pending_retries_line(plain=True)
    if retry_line:
        lines.append(retry_line)
    if status not in ("started", "failed"):
        lines.append(_remaining_today(task, plain=True))
    text = "\n".join(lines)

    body = json.dumps({
        "receive_id": chat_id,
        "msg_type":   "text",
        "content":    json.dumps({"text": text}),
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
        data=body,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        resp = json.loads(r.read().decode("utf-8"))
    if resp.get("code") != 0:
        raise RuntimeError(f"feishu send error: {resp}")
    print(f"[notify_feishu] 已发送: {task}/{status or 'ok'}", flush=True)


def _feishu_cfg() -> tuple[str, str, str]:
    cfg_path = ROOT / "stock-bot" / "feishu_config.json"
    if not cfg_path.exists():
        return "", "", ""
    cfg     = json.loads(cfg_path.read_text(encoding="utf-8-sig")).get("feishu", {})
    return cfg.get("app_id", ""), cfg.get("app_secret", ""), cfg.get("notify_chat_id", "")


def _try_feishu(task: str, desc: str, status: str) -> None:
    app_id, secret, chat_id = _feishu_cfg()
    if not (app_id and secret and chat_id):
        return
    try:
        token = _feishu_token(app_id, secret)
        send_feishu(chat_id, token, task, desc, status)
    except Exception as e:
        print(f"[notify_feishu] 发送失败: {e}", flush=True)


def push_feishu_card(title: str, lines: list[str]) -> None:
    """Push an interactive card to the Feishu notify chat. Best-effort, never raises."""
    app_id, secret, chat_id = _feishu_cfg()
    if not (app_id and secret and chat_id):
        return
    try:
        token = _feishu_token(app_id, secret)
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue",
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "\n".join(lines)},
                }
            ],
        }
        body = json.dumps({
            "receive_id": chat_id,
            "msg_type":   "interactive",
            "content":    json.dumps(card, ensure_ascii=False),
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read().decode("utf-8"))
        if resp.get("code") != 0:
            print(f"[notify_feishu] card推送失败: {resp}", flush=True)
        else:
            print("[notify_feishu] card推送成功", flush=True)
    except Exception as e:
        print(f"[notify_feishu] card推送失败: {e}", flush=True)


def push_feishu_content(text: str) -> None:
    """Push plain-text content to the Feishu notify chat. Best-effort, never raises."""
    app_id, secret, chat_id = _feishu_cfg()
    if not (app_id and secret and chat_id):
        return
    try:
        token = _feishu_token(app_id, secret)
        body = json.dumps({
            "receive_id": chat_id,
            "msg_type":   "text",
            "content":    json.dumps({"text": text}),
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read().decode("utf-8"))
        if resp.get("code") != 0:
            print(f"[notify_feishu] 内容推送失败: {resp}", flush=True)
        else:
            print("[notify_feishu] 内容推送成功", flush=True)
    except Exception as e:
        print(f"[notify_feishu] 内容推送失败: {e}", flush=True)


# ── main ──────────────────────────────────────────────────────────────────────

def _update_failures(task: str, status: str) -> None:
    failures = _load_failures()
    if status == "failed":
        failures[task] = datetime.now().strftime("%H:%M")
    elif task in failures:
        del failures[task]
    _save_failures(failures)


def main() -> None:
    task   = sys.argv[1] if len(sys.argv) > 1 else "未知任务"
    desc   = sys.argv[2] if len(sys.argv) > 2 else ""
    status = sys.argv[3] if len(sys.argv) > 3 else ""

    _update_failures(task, status)

    # cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    # url = cfg.get("discord", {}).get("webhook_url", "")
    # delays = [5, 15, 30]
    # if url:
    #     for attempt, delay in enumerate([0] + delays, 1):
    #         if delay:
    #             time.sleep(delay)
    #         try:
    #             send_discord(url, task, desc, status)
    #             break
    #         except Exception as e:
    #             if attempt <= len(delays):
    #                 print(f"[notify_discord] 失败({attempt}次), {delays[attempt-1]}s后重试: {e}", flush=True)
    #             else:
    #                 print(f"[notify_discord] 放弃: {e}", flush=True)
    # else:
    #     print("[notify_discord] webhook_url 未配置，跳过", flush=True)

    _try_feishu(task, desc, status)


if __name__ == "__main__":
    main()
