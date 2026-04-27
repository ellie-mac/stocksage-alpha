"""
notify_feishu.py — 定时任务完成/失败通知推送到飞书
用法:
    python -X utf8 scripts/notify_feishu.py "任务名" "描述" [failed]
"""
import json
import sys
import time
from datetime import date
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
BOT_DIR = ROOT / "stock-bot"
CONFIG  = BOT_DIR / "feishu_config.json"

_TASK_DESC = {
    "chip_Premarket":  "盘前数据兜底",
    "main_Morning":    "主策略盘前兜底",
    "xhs_Morning":     "量化早报",
    "xhs_Midday":      "午间快报",
    "xhs_Evening":     "收盘总结",
    "daily_PerfLog":   "三合一收盘胜率",
    "market_Warm":     "市场数据预热",
    "price_Prefetch":  "价格缓存预热",
    "chip_Night":      "夜间筹码扫描",
    "main_Scan":       "主策略扫盘",
    "gc_Scan":         "金叉策略扫描",
    "chip_CadScan":    "筹码三模型扫描",
    "main_Night":      "夜间数据预热",
}


def _get_remaining_tasks() -> str:
    """List tasks that haven't run today yet."""
    names_list = "','".join(_TASK_DESC.keys())
    ps = (
        f"$today = (Get-Date).Date;"
        f"$names = @('{names_list}');"
        "Get-ScheduledTask | Where-Object { $names -contains $_.TaskName } | ForEach-Object {"
        "  $info = $_ | Get-ScheduledTaskInfo -ErrorAction SilentlyContinue;"
        "  $lr = $info.LastRunTime;"
        "  $done = $lr -and $lr -ge $today -and $lr -le (Get-Date);"
        "  if (-not $done) { Write-Output $_.TaskName }"
        "}"
    )
    try:
        import subprocess
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, timeout=15, encoding="utf-8", errors="replace",
        )
        names = [l.strip() for l in r.stdout.splitlines() if l.strip()]
        if not names:
            return "所有定时任务已完成 ✅"
        descs = [_TASK_DESC.get(n, n) for n in names]
        return "待执行: " + "、".join(descs)
    except Exception:
        return ""


def _send(chat_id: str, text: str, app_id: str, app_secret: str) -> bool:
    try:
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
        client = lark.Client.builder().app_id(app_id).app_secret(app_secret).build()
        req = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": text}))
                .build()
            )
            .build()
        )
        resp = client.im.v1.message.create(req)
        return resp.success()
    except Exception as e:
        print(f"[notify_feishu] send error: {e}", file=sys.stderr)
        return False


def main():
    if len(sys.argv) < 3:
        print("用法: notify_feishu.py <任务名> <描述> [failed]", file=sys.stderr)
        sys.exit(1)

    if not CONFIG.exists():
        print(f"[notify_feishu] config not found: {CONFIG}", file=sys.stderr)
        sys.exit(0)

    cfg       = json.loads(CONFIG.read_text(encoding="utf-8"))
    fs        = cfg.get("feishu", {})
    app_id    = fs.get("app_id", "")
    app_secret= fs.get("app_secret", "")
    chat_id   = fs.get("notify_chat_id", "")

    if not chat_id or not app_id or not app_secret:
        print("[notify_feishu] notify_chat_id / app_id / app_secret not configured", file=sys.stderr)
        sys.exit(0)

    task_name = sys.argv[1]
    desc      = sys.argv[2]
    failed    = len(sys.argv) > 3 and sys.argv[3].lower() in ("failed", "fail", "error")

    icon   = "❌" if failed else "✅"
    status = "失败" if failed else "完成"
    now    = time.strftime("%H:%M")
    text   = f"{icon} [{now}] {task_name} {status}\n{desc}"

    remaining = _get_remaining_tasks()
    if remaining:
        text += f"\n{remaining}"

    for attempt in range(3):
        if _send(chat_id, text, app_id, app_secret):
            print(f"[notify_feishu] sent ok (attempt {attempt+1})", flush=True)
            break
        wait = [5, 15, 30][attempt]
        print(f"[notify_feishu] send failed, retry in {wait}s…", file=sys.stderr)
        time.sleep(wait)


if __name__ == "__main__":
    main()
