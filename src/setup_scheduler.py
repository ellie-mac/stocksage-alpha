#!/usr/bin/env python3
"""
src/setup_scheduler.py — 一次性运行，注册 Windows 定时任务

用法（从仓库根目录，管理员权限运行）:
    python src/setup_scheduler.py           # 注册所有定时任务
    python src/setup_scheduler.py --remove  # 删除任务
    python src/setup_scheduler.py --status  # 查看任务状态
"""

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SCRIPTS   = REPO_ROOT / "src"
TASKS_DIR = REPO_ROOT / "tasks"
PYTHON    = sys.executable
LOGS_DIR  = SCRIPTS / "logs"

# ── Script paths ─────────────────────────────────────────────────────────────
FEISHU_BOT    = REPO_ROOT / "stock-bot" / "lark_bot.py"
DISCORD_BOT   = REPO_ROOT / "stock-bot" / "discord_bot.py"
BOT_LOGS      = REPO_ROOT / "stock-bot"
REPORTER      = SCRIPTS   / "report" / "reporter.py"
DAILY_SCAN    = SCRIPTS   / "chip" / "daily_scan.py"
MONITOR       = SCRIPTS   / "monitor.py"
BATCH_FIN     = SCRIPTS   / "tools" / "batch_financials.py"
GEN_UNIVERSE  = SCRIPTS   / "tools" / "generate_full_universe.py"
DAILY_PERF_LOG    = SCRIPTS   / "jobs" / "daily_perf_log.py"
CAD_PIPELINE      = SCRIPTS   / "chip" / "pipeline.py"
GOLDEN_CROSS_SCAN = SCRIPTS   / "strategies" / "golden_cross_scan.py"
PREFETCH          = SCRIPTS   / "jobs" / "prefetch.py"
INTEGRITY_CHECK   = SCRIPTS   / "jobs" / "integrity_check.py"
NOTIFY_FAIL       = SCRIPTS   / "notify" / "notify_failure.py"
NOTIFY            = SCRIPTS   / "notify" / "notify.py"
HOT_RANK_LOGGER   = SCRIPTS   / "tools" / "hot_rank_logger.py"
HOT_SCAN          = SCRIPTS   / "strategies" / "hot_scan.py"
SIDEWAYS_SCAN     = SCRIPTS   / "strategies" / "sideways_scan.py"
MARKETCAP_SCAN    = SCRIPTS   / "strategies" / "marketcap_strategy.py"
EVENING_STRATEGY  = SCRIPTS   / "jobs"       / "evening_strategy.py"
PREFETCH_QUALITY  = SCRIPTS   / "jobs"       / "prefetch_quality.py"
CFFEX_CITIC       = SCRIPTS   / "jobs"       / "cffex_citic_positions.py"

# ── Bot startup tasks (At Logon trigger) ─────────────────────────────────────
BOT_TASKS = [
    ("StockSage_LarkBot",     FEISHU_BOT,  BOT_LOGS / "lark_bot.log"),
]

# ── Watchdog tasks (periodic repeat) ─────────────────────────────────────────
FEISHU_BOTS_DIR = Path.home() / "repos" / "lark-agent"
WATCHDOG_TASKS = [
    ("StockSage_LarkAgent_Watchdog", FEISHU_BOTS_DIR / "watchdog.ps1", 5),
]


def _bot_bat(name: str, script: Path, log: Path) -> tuple[Path, str]:
    bat_path = TASKS_DIR / f"run_{name.lower().replace('stocksage_', '')}.bat"
    content = (
        f'@echo off\n'
        f'chcp 65001 > nul\n'
        f'title {name.replace("StockSage_", "").replace("stocksage_", "")}\n'
        f'cd /d "{REPO_ROOT}"\n'
        f':loop\n'
        f'"{PYTHON}" -X utf8 "{script}" >> "{log}" 2>&1\n'
        f'echo [%date% %time%] {name} exited, restarting in 10s... >> "{log}" 2>&1\n'
        f'timeout /t 10 /nobreak > nul\n'
        f'goto loop\n'
    )
    return bat_path, content


OLD_TASKS = [
    "StockSage_Morning",
    "StockSage_Midday",
    "StockSage_Evening",
    "StockSage_ChipMorning",
    "StockSage_ChipMidday",
    "StockSage_ChipEvening",
    "StockSage_ChipPerfLog",
    "StockSage_CadScan",
    "StockSage_MainMorning",
    "xhs_ChipMorning", "xhs_ChipMidday", "xhs_ChipEvening",
    "xhs_ChipPerfLog", "xhs_CadScan", "xhs_MainMorning", "xhs_MonitorScan",
    "ss_ChipMorning", "ss_ChipMidday", "ss_ChipEvening",
    "ss_ChipPerfLog", "ss_CadScan", "ss_CadmScan", "ss_MainMorning", "ss_MonitorScan",
    "main_Morning",
    "morning_Push",
    "xhs_Morning", "xhs_Midday", "xhs_Evening",
]

# ── Scheduled tasks ───────────────────────────────────────────────────────────
TASKS = [
    ("integrity_Check", "08:00", "integrity_check", "每小时数据完整性检查（首次通过后当日跳过）", False),
    ("cffex_CiticAM",   "08:00", "cffex_citic",    "中信期货四大股指空单跟踪 📱", True),
    ("concept_Warm",    "08:30", "concept_warm",   "预热概念板块反查 map（~30s），不推送", False),
    ("report_Morning",  "09:25", "chip_morning",   "盘前选股报告推送 📱", True),
    ("hot_Rank_0935",   "09:35", "hot_rank",       "热榜快照 09:35 落盘（开盘情绪极值）", False),
    ("hot_Rank_1000",   "10:00", "hot_rank",       "热榜快照 10:00 落盘", False),
    ("hot_Rank_1100",   "11:00", "hot_rank",       "热榜快照 11:00 落盘", False),
    ("report_Midday",   "11:35", "chip_midday",    "午间行情报告推送 📱", True),
    ("hot_Rank_1330",   "13:30", "hot_rank",       "热榜快照 13:30 落盘", False),
    ("hot_Rank_1430",   "14:30", "hot_rank",       "热榜快照 14:30 落盘", False),
    ("report_Evening",  "15:30", "chip_evening",   "收盘报告推送 📱", True),
    ("market_Warm",     "15:35", "market_warm",    "预热市场数据：CSI300/PE/申万/停牌表，不推送", False),
    ("marketcap_Scan",  "15:45", "marketcap_scan", "市值策略扫盘 📱", True),
    ("daily_PerfLog",   "16:05", "daily_perf_log", "多策略收盘胜率统计 📱", True),
    ("hot_Scan",        "16:35", "hot_scan",       "热榜策略扫描，更新 hot_scan_latest.json 推送 📱", True),
    ("price_Prefetch",  "17:00", "price_prefetch", "预热全市场价格历史缓存（~1-1.5h），不推送", False),
    ("fundflow_Prefetch","17:30", "fundflow_prefetch","预热全市场资金流向缓存（~20min），不推送", False),
    ("chip_Night",      "18:00", "chip_night",     "收盘后预取筹码缓存（AK重算~1.5h），不推送", False),
    ("main_Scan",       "18:30", "monitor_scan",   "主/ETF/小盘策略扫盘 📱", True),
    ("quality_Prefetch", "19:00", "quality_prefetch","预热全市场质量指标 (amt/vol_ratio)，不推送", False),
    ("golden_Scan",     "19:30", "gc_scan",        "金叉策略扫描（全A股7项指标共振）推送 📱", True),
    ("sideways_Scan",   "20:00", "sideways_scan",  "横盘策略扫描（全市场+流动性+量比≥0.5）推送 📱", True),
    ("chip_CadScan",    "21:00", "cad_scan",       "筹码扫描 cah/cadm/cad，三者共有T1-T4推送 📱", True),
    ("evening_Strategy", "22:00", "evening_strategy","多策略汇总·晚间（七路合并推送） 📱", True),
]


def _scheduled_bat(task_name: str, slot: str, desc: str):
    log = LOGS_DIR
    notify_cmd = (f'"{PYTHON}" -X utf8 "{NOTIFY_FAIL}" "{task_name}"'
                  f' >> "{log}\\notify_failure.log" 2>&1')
    discord_start_cmd = (f'"{PYTHON}" -X utf8 "{NOTIFY}" "{task_name}" "{desc}" "started"'
                         f' >> "{log}\\notify_discord.log" 2>&1')
    discord_ok_cmd = (f'"{PYTHON}" -X utf8 "{NOTIFY}" "{task_name}" "{desc}"'
                      f' >> "{log}\\notify_discord.log" 2>&1')
    discord_fail_cmd = (f'"{PYTHON}" -X utf8 "{NOTIFY}" "{task_name}" "{desc}" "failed"'
                        f' >> "{log}\\notify_discord.log" 2>&1')

    if slot == "keepalive":
        path = TASKS_DIR / "run_keepalive.bat"
        cmd = f'echo keepalive {{%DATE% %TIME%}} >> "{log}\\keepalive.log" 2>&1'
    elif slot == "cad_scan":
        path = TASKS_DIR / "run_cad_scan.bat"
        cmd = f'"{PYTHON}" -X utf8 "{CAD_PIPELINE}" >> "{log}\\cad_pipeline.log" 2>&1'
    elif slot == "chip_night":
        path = TASKS_DIR / "run_chip_night.bat"
        cmd = f'"{PYTHON}" -X utf8 "{DAILY_SCAN}" --ak --no-push >> "{log}\\chip_scan_night.log" 2>&1'
    elif slot == "chip_premarket":
        path = TASKS_DIR / "run_chip_premarket.bat"
        cmd = f'"{PYTHON}" -X utf8 "{DAILY_SCAN}" --ak --no-push >> "{log}\\chip_scan_premarket.log" 2>&1'
    elif slot in ("chip_morning", "chip_midday", "chip_evening"):
        phase = slot.split("_")[1]
        path = TASKS_DIR / f"run_chip_{phase}.bat"
        cmd = f'"{PYTHON}" -X utf8 "{REPORTER}" {phase} --style auto >> "{log}\\xhs_{phase}.log" 2>&1'
    elif slot == "daily_perf_log":
        path = TASKS_DIR / "run_daily_perf_log.bat"
        cmd = f'"{PYTHON}" -X utf8 "{DAILY_PERF_LOG}" --force >> "{log}\\daily_perf_log.log" 2>&1'
    elif slot == "evening_strategy":
        path = TASKS_DIR / "run_evening_strategy.bat"
        cmd = f'"{PYTHON}" -X utf8 "{EVENING_STRATEGY}" --push >> "{log}\\evening_strategy.log" 2>&1'
    elif slot == "quality_prefetch":
        path = TASKS_DIR / "run_quality_prefetch.bat"
        cmd = f'"{PYTHON}" -X utf8 "{PREFETCH_QUALITY}" --force >> "{log}\\prefetch_quality.log" 2>&1'
    elif slot == "monitor_scan":
        path = TASKS_DIR / "run_monitor_scan.bat"
        cmd = f'"{PYTHON}" -X utf8 "{MONITOR}" --always-send >> "{log}\\monitor_scan.log" 2>&1'
    elif slot == "market_warm":
        path = TASKS_DIR / "run_market_warm.bat"
        cmd = f'"{PYTHON}" -X utf8 "{PREFETCH}" --market >> "{log}\\prefetch_market.log" 2>&1'
    elif slot == "price_prefetch":
        path = TASKS_DIR / "run_price_prefetch.bat"
        cmd = f'"{PYTHON}" -X utf8 "{PREFETCH}" --price --force >> "{log}\\prefetch_price.log" 2>&1'
    elif slot == "concept_warm":
        path = TASKS_DIR / "run_concept_warm.bat"
        cmd = f'"{PYTHON}" -X utf8 "{PREFETCH}" --concept >> "{log}\\prefetch_concept.log" 2>&1'
    elif slot == "fundflow_prefetch":
        path = TASKS_DIR / "run_fundflow_prefetch.bat"
        cmd = f'"{PYTHON}" -X utf8 "{PREFETCH}" --fundflow --force >> "{log}\\prefetch_fundflow.log" 2>&1'
    elif slot == "integrity_check":
        path = TASKS_DIR / "run_integrity_check.bat"
        cmd = f'"{PYTHON}" -X utf8 "{INTEGRITY_CHECK}" >> "{log}\\integrity_check.log" 2>&1'
    elif slot == "gc_scan":
        path = TASKS_DIR / "run_gc_scan.bat"
        cmd = f'"{PYTHON}" -X utf8 "{GOLDEN_CROSS_SCAN}" --push >> "{log}\\gc_scan.log" 2>&1'
    elif slot == "hot_rank":
        path = TASKS_DIR / "run_hot_rank_logger.bat"
        cmd = f'"{PYTHON}" -X utf8 "{HOT_RANK_LOGGER}" >> "{log}\\hot_rank_logger.log" 2>&1'
    elif slot == "hot_scan":
        path = TASKS_DIR / "run_hot_scan.bat"
        cmd = f'"{PYTHON}" -X utf8 "{HOT_SCAN}" --push >> "{log}\\hot_scan.log" 2>&1'
    elif slot == "sideways_scan":
        path = TASKS_DIR / "run_sideways_scan.bat"
        cmd = f'"{PYTHON}" -X utf8 "{SIDEWAYS_SCAN}" --push >> "{log}\\sideways_scan.log" 2>&1'
    elif slot == "marketcap_scan":
        path = TASKS_DIR / "run_marketcap_scan.bat"
        cmd = f'"{PYTHON}" -X utf8 "{MARKETCAP_SCAN}" --push >> "{log}\\marketcap_scan.log" 2>&1'
    elif slot == "cffex_citic":
        path = TASKS_DIR / "run_cffex_citic_am.bat"
        cmd = f'"{PYTHON}" -X utf8 "{CFFEX_CITIC}" --push >> "{log}\\cffex_citic.log" 2>&1'
    else:
        raise ValueError(f"Unknown slot: {slot}")

    title_text = desc.split(" 📱")[0].split("（")[0].strip() if desc else task_name
    probe_log = f'{log}\\task_probe.log'
    probe_entered = f'echo [%DATE% %TIME%] {task_name} bat entered >> "{probe_log}"'
    probe_invoking = f'echo [%DATE% %TIME%] {task_name} invoking python >> "{probe_log}"'
    probe_exit = f'echo [%DATE% %TIME%] {task_name} python exit=%errorlevel% >> "{probe_log}"'
    content = (
        f'@echo off\n'
        f'chcp 65001 > nul\n'
        f'title {title_text}\n'
        f'cd /d "{REPO_ROOT}"\n'
        f'mkdir "{LOGS_DIR}" 2>nul\n'
        f'{probe_entered}\n'
        f'{discord_start_cmd}\n'
        f'{probe_invoking}\n'
        f'{cmd}\n'
        f'{probe_exit}\n'
        f'if errorlevel 1 (\n'
        f'    {notify_cmd}\n'
        f'    {discord_fail_cmd}\n'
        f') else (\n'
        f'    {discord_ok_cmd}\n'
        f')\n'
    )
    return path, content


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, shell=False)


def _create_daily_task(name: str, time_str: str, bat_path: Path):
    cmd = [
        "schtasks", "/Create", "/TN", name, "/TR", str(bat_path),
        "/SC", "DAILY", "/ST", time_str, "/RL", "HIGHEST", "/F"
    ]
    return _run(cmd)


def _query_task(name: str):
    return _run(["schtasks", "/Query", "/TN", name, "/V", "/FO", "LIST"])


def _delete_task(name: str):
    return _run(["schtasks", "/Delete", "/TN", name, "/F"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--remove", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    if args.status:
        for name, time_str, slot, desc, push in TASKS:
            q = _query_task(name)
            if q.returncode != 0:
                print(f"{'📱 ' if push else '   '}{time_str:>5}  {name:<35} {desc}  [未注册]")
            else:
                next_run = "N/A"
                status = "Unknown"
                for line in q.stdout.splitlines():
                    if line.startswith("Next Run Time:"):
                        next_run = line.split(":", 1)[1].strip()
                    elif line.startswith("Status:"):
                        status = line.split(":", 1)[1].strip()
                print(f"{'📱 ' if push else '   '}{time_str:>5}  {name:<35} {desc}  [Next Run Time: {next_run}]")
                print(f"{'📱 ' if push else '   '}{time_str:>5}  {name:<35} {desc}  [Status: {status}]")
        return

    if args.remove:
        for name in OLD_TASKS + [x[0] for x in TASKS] + [x[0] for x in BOT_TASKS] + [x[0] for x in WATCHDOG_TASKS]:
            _delete_task(name)
        print("已删除相关任务")
        return

    for name, script, log in BOT_TASKS:
        bat, content = _bot_bat(name, script, log)
        bat.write_text(content, encoding="utf-8")

    for name, time_str, slot, desc, push in TASKS:
        bat, content = _scheduled_bat(name, slot, desc)
        bat.write_text(content, encoding="utf-8")
        res = _create_daily_task(name, time_str, bat)
        if res.returncode == 0:
            print(f"[OK] {name} @ {time_str}")
        else:
            print(f"[FAIL] {name} @ {time_str}: {res.stderr or res.stdout}")

    print("定时任务注册完成")


if __name__ == "__main__":
    main()
