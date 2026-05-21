#!/usr/bin/env python3
"""
notify.py — 飞书定时任务通知

用法：
    python -X utf8 src/notify/notify.py "任务名" "描述" [status]
    status: "" = 完成, "started" = 开始, "failed" = 失败
"""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
_FAILURES_PATH = ROOT / "data" / "task_failures.json"

# notify.py "剩余任务" 显示用的日程清单 — 派生自 src/task_schedule.py。
# 唯一 source: 编辑 task_schedule.ALL_TASKS。
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from task_schedule import notify_schedule as _notify_schedule
_SCHEDULE = _notify_schedule()


def _load_failures() -> dict:
    """读取 UI 装饰用的 task_failures.json（不是状态机，仅给 _pending_retries_line 用）。

    顺手清理 stale entries：今日 00:00 之前的记录视为已过期（重启次日重计）。
    """
    sys.path.insert(0, str(ROOT / "src"))
    from common import read_json
    raw = read_json(_FAILURES_PATH, default={})
    today_00 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # value 是 "HH:MM" — 若 HH 比当前 hour 大很多，可能是昨天的（保守做：保留 24h 内的）
    # 由于只存 HH:MM 信息不够判断日期，保守保留所有，等成功消息触发清理
    return raw if isinstance(raw, dict) else {}


def _save_failures(d: dict) -> None:
    """原子写入 — 用 common.write_json，避免 reader 读到半残文件。"""
    sys.path.insert(0, str(ROOT / "src"))
    from common import write_json
    write_json(_FAILURES_PATH, d, atomic=True)


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


def _split_plain_divs(lines: list[str], max_chars: int = 1800) -> list[dict]:
    """把 lines 按字符数切段，每段生成一个 plain_text div（正确渲染 \\n 换行）。"""
    divs, buf = [], []
    buf_len = 0
    for ln in (l.rstrip() for l in lines):
        addition = len(ln) + 1  # +1 for \n
        if buf and buf_len + addition > max_chars:
            divs.append({"tag": "div", "text": {"tag": "plain_text", "content": "\n".join(buf)}})
            buf, buf_len = [], 0
        buf.append(ln)
        buf_len += addition
    if buf:
        divs.append({"tag": "div", "text": {"tag": "plain_text", "content": "\n".join(buf)}})
    return divs or [{"tag": "div", "text": {"tag": "plain_text", "content": ""}}]


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
            "elements": _split_plain_divs(lines),
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


def push_feishu_image(image_path, caption: str = "") -> None:
    """上传 PNG/JPG 图片到飞书并发到 notify chat。两步：image upload → 发 image 消息。

    image_path: 本地图片路径（str 或 Path）。caption 当前未使用（image 消息无 caption；
    需要的话先调 push_feishu_content 发文字、再调本函数发图）。失败只 print，不抛。
    """
    from pathlib import Path as _P
    app_id, secret, chat_id = _feishu_cfg()
    if not (app_id and secret and chat_id):
        return
    p = _P(image_path)
    if not p.exists():
        print(f"[notify_feishu] image not found: {p}", flush=True)
        return
    try:
        token = _feishu_token(app_id, secret)
        # Step 1: upload image (multipart/form-data)
        boundary = "----stocksageImgBoundary7E1A"
        parts: list[bytes] = []
        parts.append(f"--{boundary}\r\n".encode())
        parts.append(b'Content-Disposition: form-data; name="image_type"\r\n\r\nmessage\r\n')
        parts.append(f"--{boundary}\r\n".encode())
        parts.append(f'Content-Disposition: form-data; name="image"; filename="{p.name}"\r\n'.encode())
        parts.append(b"Content-Type: image/png\r\n\r\n")
        parts.append(p.read_bytes())
        parts.append(f"\r\n--{boundary}--\r\n".encode())
        body = b"".join(parts)
        req = urllib.request.Request(
            "https://open.feishu.cn/open-apis/im/v1/images",
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  f"multipart/form-data; boundary={boundary}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                up = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")[:500]
            print(f"[notify_feishu] 上传 HTTP {e.code}: {err_body}", flush=True)
            return
        if up.get("code") != 0:
            print(f"[notify_feishu] 图片上传失败: {up}", flush=True)
            return
        image_key = up["data"]["image_key"]

        # Step 2: send image message
        msg_body = json.dumps({
            "receive_id": chat_id,
            "msg_type":   "image",
            "content":    json.dumps({"image_key": image_key}),
        }).encode("utf-8")
        req2 = urllib.request.Request(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            data=msg_body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req2, timeout=10) as r:
            resp = json.loads(r.read().decode("utf-8"))
        if resp.get("code") != 0:
            print(f"[notify_feishu] 图片消息发送失败: {resp}", flush=True)
        else:
            print("[notify_feishu] 图片推送成功", flush=True)
    except Exception as e:
        print(f"[notify_feishu] 图片推送异常: {e}", flush=True)


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

    # task_failures.json 仍要更新（用于汇报 / 重跑提示），但只在失败时推送飞书。
    # started/ok 不再发飞书，避免噪音；汇总信息由 task_summary 在 23:00 一次推送。
    _update_failures(task, status)
    if status == "failed":
        _try_feishu(task, desc, status)
    else:
        print(f"[notify_feishu] 跳过 {task}/{status or 'ok'}（仅失败才推送，汇总在 23:00）", flush=True)


if __name__ == "__main__":
    main()
