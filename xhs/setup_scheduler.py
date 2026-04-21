#!/usr/bin/env python3
"""
xhs/setup_scheduler.py — 一次性运行，注册 Windows 定时任务

用法（从仓库根目录，管理员权限运行）:
    python xhs/setup_scheduler.py           # 注册三个定时任务
    python xhs/setup_scheduler.py --remove  # 删除任务
    python xhs/setup_scheduler.py --status  # 查看任务状态
"""

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
XHS_DIR   = Path(__file__).parent
PYTHON    = sys.executable
WRITER    = REPO_ROOT / "xhs" / "writer.py"

# 旧任务（仅用于删除）
OLD_TASKS = [
    "StockSage_Morning",
    "StockSage_Midday",
    "StockSage_Evening",
    "StockSage_Night",
]

# 新筹码三段式任务
CHIP_WRITER = REPO_ROOT / "xhs" / "chip_writer.py"
TASKS = [
    ("StockSage_ChipMorning", "09:25", "morning"),
    ("StockSage_ChipMidday",  "11:35", "midday"),
    ("StockSage_ChipEvening", "15:10", "evening"),
]


def create_bat(slot: str) -> Path:
    """Create a .bat launcher for the given slot (gitignored)."""
    bat  = XHS_DIR / f"run_chip_{slot}.bat"
    log  = REPO_ROOT / "scripts" / "logs" / f"chip_writer_{slot}.log"
    bat.write_text(
        f'@echo off\n'
        f'cd /d "{REPO_ROOT}"\n'
        f'mkdir "{REPO_ROOT}\\scripts\\logs" 2>nul\n'
        f'"{PYTHON}" -X utf8 "{CHIP_WRITER}" {slot} >> "{log}" 2>&1\n',
        encoding="utf-8",
    )
    return bat


def register():
    print(f"Python : {PYTHON}")
    print(f"Script : {CHIP_WRITER}")
    print()
    # 先删旧任务
    for name in OLD_TASKS:
        subprocess.run(f'schtasks /delete /tn "{name}" /f',
                       shell=True, capture_output=True, text=True)
    # 注册新任务（/it = 仅交互式会话，防止睡眠补跑）
    for name, time, slot in TASKS:
        bat = create_bat(slot)
        cmd = (f'schtasks /create /tn "{name}" /tr "{bat}" /sc daily /st {time}'
               f' /f /rl HIGHEST')
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅  {name}  每天 {time}")
        else:
            print(f"❌  {name}  失败: {result.stderr.strip()}")
    print()
    print("完成。chip_writer.py 内置超时检查，超出窗口自动跳过，不会补跑。")


def remove():
    all_names = [n for n, _, _ in TASKS] + OLD_TASKS
    for name in all_names:
        result = subprocess.run(
            f'schtasks /delete /tn "{name}" /f',
            shell=True, capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"✅  {name} 已删除")
        else:
            print(f"❌  {name}: {result.stderr.strip()}")


def status():
    for name, time, _ in (TASKS + [(n, "?", "") for n in OLD_TASKS]):
        result = subprocess.run(
            f'schtasks /query /tn "{name}" /fo LIST',
            shell=True, capture_output=True, text=True,
        )
        if result.returncode == 0:
            # Extract status line
            for line in result.stdout.splitlines():
                if "状态" in line or "Status" in line or "下次运行" in line or "Next Run" in line:
                    print(f"  {name}: {line.strip()}")
        else:
            print(f"  {name}: 未注册")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="注册/删除 StockSage 定时任务")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--remove", action="store_true", help="删除所有定时任务")
    group.add_argument("--status", action="store_true", help="查看任务状态")
    args = parser.parse_args()

    if args.remove:
        remove()
    elif args.status:
        status()
    else:
        register()
