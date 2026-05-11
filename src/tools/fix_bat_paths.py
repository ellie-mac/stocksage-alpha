r"""
fix_bat_paths.py — 修复 tasks/ 下 bat 文件里因重构产生的旧路径引用。

重构后路径变化：
  src\notify.py            -> src\notify\notify.py
  src\notify_failure.py    -> src\notify\notify_failure.py
  src\golden_cross_scan.py -> src\strategies\golden_cross_scan.py
  src\daily_perf_log.py    -> src\jobs\daily_perf_log.py  (仅根级)
  src\prefetch.py          -> src\jobs\prefetch.py
  src\integrity_check.py   -> src\jobs\integrity_check.py
  src\daily_chip_scan.py   -> src\chip\daily_scan.py
  src\run_cad_pipeline.py  -> src\chip\pipeline.py

用法（在 repo 根目录执行）：
  python src/tools/fix_bat_paths.py
"""
from __future__ import annotations

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_TASKS = _ROOT / "tasks"

REPLACEMENTS = [
    # notify 包装层
    (r"src\notify.py",           r"src\notify\notify.py"),
    (r"src\notify_failure.py",   r"src\notify\notify_failure.py"),
    # 策略主脚本
    (r"src\golden_cross_scan.py",  r"src\strategies\golden_cross_scan.py"),
    # prefetch / jobs
    (r"src\prefetch.py",         r"src\jobs\prefetch.py"),
    (r"src\integrity_check.py",  r"src\jobs\integrity_check.py"),
    # chip 模块
    (r"src\daily_chip_scan.py",  r"src\chip\daily_scan.py"),
    (r"src\run_cad_pipeline.py", r"src\chip\pipeline.py"),
]

# 二次替换防护：避免 jobs\jobs\ 或 chip\chip\ 双重前缀
_DEDUP = [
    (r"src\jobs\jobs\\",   r"src\jobs\\"),
    (r"src\chip\chip\\",   r"src\chip\\"),
    (r"src\notify\notify\notify.py",       r"src\notify\notify.py"),
    (r"src\notify\notify\notify_failure.py", r"src\notify\notify_failure.py"),
]

# factor_analysis: 加 --workers 1 防止 V8 并发崩溃
_ANALYSIS_OLD = r'src\factors\analysis.py"'
_ANALYSIS_NEW = r'src\factors\analysis.py" --workers 1'


def fix_file(path: Path) -> bool:
    text = path.read_text(encoding="utf-8", errors="replace")
    new = text
    for old, repl in REPLACEMENTS:
        new = new.replace(old, repl)
    # daily_perf_log: 只替换根级（排除已正确的 jobs\ 前缀）
    import re
    new = re.sub(r"src\\daily_perf_log\.py", r"src\\jobs\\daily_perf_log.py", new)
    # 防止二次替换
    for bad, good in _DEDUP:
        new = new.replace(bad, good)
    # factor_analysis: 只在还没有 --workers 的行上加
    if _ANALYSIS_OLD in new and "--workers" not in new:
        new = new.replace(_ANALYSIS_OLD, _ANALYSIS_NEW)
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
