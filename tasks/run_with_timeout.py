"""
超时包装器 — 以子进程运行命令，超时后杀掉并以 exit 1 退出。

用法（在 bat 文件中）：
    python tasks\run_with_timeout.py <timeout_sec> <command...>

示例：
    python tasks\run_with_timeout.py 5400 python -X utf8 src\strategies\golden_cross_scan.py --push

子进程继承父进程的 stdout/stderr，bat 的 >> log 2>&1 重定向对子进程同样有效。
"""
import subprocess
import sys


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: run_with_timeout.py <timeout_sec> <cmd...>", flush=True)
        sys.exit(2)

    timeout_sec = int(sys.argv[1])
    cmd = sys.argv[2:]

    try:
        result = subprocess.run(cmd, timeout=timeout_sec)
        sys.exit(result.returncode)
    except subprocess.TimeoutExpired:
        print(f"[timeout] 超时 {timeout_sec}s，进程已终止", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
