#!/usr/bin/env python3
"""定时任务今日状态汇报 — 每天 12:30/16:45/22:35 各一次，飞书推送一张汇总卡片。

通过 task_probe.log 找当日 task `python exit=N` 行，对照 task_schedule.ALL_TASKS
判断每个**可观测**任务（slot != None）的状态。slot=None 的任务（如 institution_Scan、
watchlist_Updater 等没有 probe 的）会在卡片末尾标 "(no probe)"。

替代旧设计："每个任务 started/ok/failed 都发飞书" → 3 次汇总 + 失败实时单推。

用法：
    python -X utf8 src/jobs/task_summary.py [label]
    label 可选，会出现在 Feishu 卡片标题，默认按当前时间猜（中午/收盘/晚上）。
"""
from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

from task_schedule import ALL_TASKS  # noqa: E402

PROBE_LOG = ROOT / "src" / "logs" / "task_probe.log"

# 产物兜底：probe 找不到时，看任务的关键输出文件今日是否更新。
# 不是所有任务都有可观测产物（如 _Prefetch、_Warm 类只更新缓存），这里只列
# 主流 scanner / report 类。
TASK_OUTPUT_FILES: dict[str, list[Path]] = {
    "escalator_Scan":    [ROOT / "data" / "escalator_latest.json"],
    "sideways_Scan":     [ROOT / "data" / "sideways_latest.json"],
    "golden_Scan":       [ROOT / "data" / "golden_cross_latest.json"],
    "hot_Scan":          [ROOT / "data" / "hot_scan_latest.json"],
    "chip_Night":        [ROOT / "data" / "chip_scan_latest.json"],
    "chip_CadScan":      [ROOT / "data" / "chip_cad_latest.json"],
    "marketcap_Scan":    [ROOT / "data" / "marketcap_latest.json"],
    "main_Scan":         [ROOT / "data" / "latest_picks.json"],
    "evening_Strategy":  [ROOT / "data" / "latest_picks.json"],
    "cffex_CiticAM":     [ROOT / "data" / "cffex_citic_latest.json"],
    "escalator_PerfLog": [ROOT / "data" / "escalator_perf.json",
                          ROOT / "data" / "escalator_daily_perf.json"],
    "strategy_Compare":  [ROOT / "data" / "strategy_compare.json",
                          ROOT / "data" / "strategy_compare_latest.json"],
    "daily_PerfLog":     [ROOT / "data" / "main_daily_perf.json",
                          ROOT / "data" / "chip_daily_perf.json",
                          ROOT / "data" / "gc_daily_perf.json"],
    "watchlist_Updater": [ROOT / "data" / "watchlist_dynamic.json"],
}


def _parse_probe_today(today_yyyymmdd: str) -> dict[str, dict]:
    """解析 task_probe.log，返回 {task_name: {bat_entered, invoking, exit_code, exit_at}}.

    探针行格式（bat 写入）: `[星期X YYYY/MM/DD HH:MM:SS.ms] task_name action`
    其中 action 是 "bat entered" / "invoking python" / "python exit=N"
    """
    out: dict[str, dict] = {}
    if not PROBE_LOG.exists():
        return out

    # 探针行例子（bat 写入，前缀含 %DATE% 可能带星期）:
    #   [周三 2026/05/20 18:30:01.53] main_Scan bat entered
    # 也兼容 yyyy-mm-dd 或 mm/dd/yyyy；只要 \[ ... \] 内能找到 YYYY 和 HH:MM 即可。
    rx = re.compile(
        r"\[.*?(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})\s+(\d{1,2}):(\d{2})[:.]?\d*\.?\d*\s*\]"
        r"\s+(\w+)\s+(.+)"
    )
    target_padded = (today_yyyymmdd[:4], today_yyyymmdd[4:6], today_yyyymmdd[6:8])

    for raw in PROBE_LOG.read_text(encoding="utf-8", errors="replace").splitlines():
        m = rx.search(raw)
        if not m:
            continue
        y, mo, d, hh, mm, task, action = m.groups()
        # tolerate "5" vs "05" — compare zero-padded
        if (y, mo.zfill(2), d.zfill(2)) != target_padded:
            continue
        entry = out.setdefault(task, {
            "bat_entered": None, "invoking": None,
            "exit_code": None, "exit_at": None,
        })
        hhmm = f"{int(hh):02d}:{mm}"
        action = action.strip()
        if action.startswith("bat entered"):
            entry["bat_entered"] = hhmm
        elif action.startswith("invoking python"):
            entry["invoking"] = hhmm
        elif "python exit=" in action:
            entry["exit_at"] = hhmm
            mc = re.search(r"exit=(-?\d+)", action)
            if mc:
                entry["exit_code"] = int(mc.group(1))
    return out


def _check_output_files(task_name: str, today_yyyymmdd: str) -> str | None:
    """产物兜底：检查任务的关键输出文件是否今日更新。

    返回 "HH:MM"（最新文件 mtime）若至少一个文件今日 fresh，否则 None。
    """
    paths = TASK_OUTPUT_FILES.get(task_name)
    if not paths:
        return None
    today = datetime.strptime(today_yyyymmdd, "%Y%m%d").date()
    latest_mtime = 0.0
    for p in paths:
        if p.exists():
            mtime = p.stat().st_mtime
            if datetime.fromtimestamp(mtime).date() == today and mtime > latest_mtime:
                latest_mtime = mtime
    return datetime.fromtimestamp(latest_mtime).strftime("%H:%M") if latest_mtime else None


def _classify(task: dict, probe_info: dict | None, now_hhmm: str, today: str) -> tuple[str, str]:
    """返回 (emoji, note)。emoji ∈ {✅ ❌ ⏳ ⏰ ❓ ·}"""
    sched_time = task["time"]
    if probe_info is None:
        # 探针找不到 → 看产物兜底
        out_mtime = _check_output_files(task["name"], today)
        if out_mtime:
            return "✅", f"{out_mtime} (产物)"   # 标 "产物" 区分 probe 来源
        # 时间还没到：不管 slot 都显示 ⏰，不要因为 slot=None 就吃掉
        if sched_time > now_hhmm:
            return "⏰", "未到时间"
        # 时间过了但无 probe：slot=None 多半是没装 probe 的手写 bat，标 · no probe
        if task["slot"] is None:
            return "·", "no probe"
        return "❓", "应运行但无记录"
    if probe_info["exit_code"] is None:
        if probe_info["invoking"]:
            return "⏳", f"进行中 {probe_info['invoking']}"
        return "⏳", f"卡在 bat {probe_info['bat_entered']}"
    if probe_info["exit_code"] == 0:
        return "✅", probe_info["exit_at"] or ""
    # 即使 exit≠0，也检查产物：可能像 monitor.py 那样工作做完了 socket 收尾报错
    out_mtime = _check_output_files(task["name"], today)
    if out_mtime:
        return "✅", f"{out_mtime} (产物，exit={probe_info['exit_code']})"
    return "❌", f"exit={probe_info['exit_code']} @ {probe_info['exit_at']}"


def get_today_statuses() -> list[tuple[dict, str, str]]:
    """公共 API：返回 [(task, emoji, note), ...]，按 time 升序，仅含 display=True & !disabled。

    供 main() 推送汇报、bot_common /t 即时查询共用，确保两路输出一致。
    """
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    now_hhmm = now.strftime("%H:%M")
    probe = _parse_probe_today(today)
    out: list[tuple[dict, str, str]] = []
    for t in sorted(ALL_TASKS, key=lambda x: x["time"]):
        if t.get("disabled"):
            continue
        if not t.get("display", True):
            continue
        info = probe.get(t["name"])
        emoji, note = _classify(t, info, now_hhmm, today)
        out.append((t, emoji, note))
    return out


def _guess_label(now_hhmm: str) -> str:
    if now_hhmm < "13:00":
        return "中午"
    if now_hhmm < "20:00":
        return "收盘"
    return "晚上"


def main() -> int:
    label = sys.argv[1] if len(sys.argv) > 1 else ""
    now = datetime.now()
    now_hhmm = now.strftime("%H:%M")
    today = now.strftime("%Y%m%d")
    if not label:
        label = _guess_label(now_hhmm)

    statuses = get_today_statuses()
    rows: list[str] = []
    ok = fail = stuck = pending = missing = 0
    for t, emoji, note in statuses:
        rows.append(f"{emoji} {t['time']} {t['name']:<22} {note}")
        if emoji == "✅":  ok += 1
        elif emoji == "❌": fail += 1
        elif emoji == "⏳": stuck += 1
        elif emoji == "⏰": pending += 1
        elif emoji == "❓": missing += 1

    title = (f"任务汇报·{label} {now.strftime('%m/%d %H:%M')}  "
             f"✅{ok} ❌{fail} ⏳{stuck} ⏰{pending} ❓{missing}")

    print(title)
    for r in rows:
        print(r)

    try:
        from notify.notify import push_feishu_card
        push_feishu_card(title, rows)
    except Exception as e:
        print(f"[task_summary] 飞书推送失败: {e}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
