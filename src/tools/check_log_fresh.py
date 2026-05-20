#!/usr/bin/env python3
"""检查指定文件是否在过去 N 秒内有写入（按 mtime）；用于 bat 通过产物判定任务成功。

用法：
    python check_log_fresh.py <path> [max_sec=300]
exit 0 = 新鲜（视为成功）；exit 1 = 过期或不存在（视为失败）

设计动机：monitor.py / nightly_scan.py 等长跑任务在主任务完成后，cleanup
阶段可能因 socket / akshare / baostock 收尾遇 [WinError 10054] 等异常导致
exit code 非 0，触发 bat 误报失败。改用产物 mtime 判定能避开这类误报。
"""
from __future__ import annotations
import os
import sys
import time


def main() -> int:
    if len(sys.argv) < 2:
        return 2
    path = sys.argv[1]
    max_sec = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    if not os.path.exists(path):
        return 1
    age = time.time() - os.path.getmtime(path)
    return 0 if age < max_sec else 1


if __name__ == "__main__":
    sys.exit(main())
