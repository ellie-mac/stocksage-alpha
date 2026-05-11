"""
fix_bat_paths.py — 修复 tasks/ 下 bat 文件里因重构产生的旧路径引用。

重构后路径变化：
  src\notify.py          → src\notify\notify.py
  src\notify_failure.py  → src\notify\notify_failure.py
  src\golden_cross_scan.py → src\strategies\golden_cross_scan.py
  src\daily_perf_log.py  → src\jobs\daily_perf_log.py  (仅根级，不改 jobs\ 下)

用法（在 repo 根目录执行）：
  python src/tools/fix_bat_paths.py
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_TASKS = _ROOT / "tasks"

REPLACEMENTS = [
    # notify 包装层
    (r"src\notify.py",          r"src\notify\notify.py"),
    (r"src\notify_failure.py",  r"src\notify\notify_failure.py"),
    # 策略主脚本
    (r"src\golden_cross_scan.py", r"src\strategies\golden_cross_scan.py"),
]


def fix_file(path: Path) -> bool:
    text = path.read_text(encoding="utf-8", errors="replace")
    new = text
    for old, repl in REPLACEMENTS:
        new = new.replace(old, repl)
    # daily_perf_log: 只替换根级（排除已正确的 jobs\ 前缀）
    new = new.replace(r"src\daily_perf_log.py", r"src\jobs\daily_perf_log.py")
    # 避免二次替换 jobs\jobs\
    new = new.replace(r"src\jobs\jobs\daily_perf_log.py", r"src\jobs\daily_perf_log.py")
    if new != text:
        path.write_text(new, encoding="utf-8")
        return True
    return False


def main() -> None:
    if not _TASKS.exists():
        print(f"[fix_bat_paths] tasks/ not found: {_TASKS}")
        return
    fixed, ok = 0, 0
    for bat in sorted(_TASKS.glob("*.bat")):
        if fix_file(bat):
            print(f"  FIXED: {bat.name}")
            fixed += 1
        else:
            ok += 1
    print(f"\n[fix_bat_paths] 完成：{fixed} 个已修复，{ok} 个无需修改")


if __name__ == "__main__":
    main()
