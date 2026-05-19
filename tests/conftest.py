"""pytest 公共 fixtures"""
from __future__ import annotations

import sys
import os

# 把 src/ 加入 path，让所有测试能 import 项目模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "jobs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "strategies"))


class InlineProcess:
    """替换 multiprocessing.Process：在同一进程内执行 worker 函数。
    避免 mock 无法传播到子进程的问题。"""
    def __init__(self, target, args=(), kwargs=None, daemon=False):
        self._target = target
        self._args = args
        self.exitcode = None

    def start(self):
        try:
            self._target(*self._args)
            self.exitcode = 0
        except Exception:
            self.exitcode = 1

    def join(self, timeout=None): pass
    def is_alive(self): return False
    def terminate(self): pass
    def kill(self): pass


class SyncQueue:
    """替换 multiprocessing.Queue：使用 stdlib queue.Queue 的同步版本。
    multiprocessing.Queue 内部 feeder 线程是异步的，同进程内 put() 后立即 get_nowait() 可能为空。"""
    def __init__(self):
        import queue
        self._q = queue.Queue()

    def put(self, item, block=True, timeout=None):
        self._q.put(item, block=block, timeout=timeout)

    def get(self, block=True, timeout=None):
        return self._q.get(block=block, timeout=timeout)

    def get_nowait(self):
        import queue
        return self._q.get_nowait()

    def empty(self): return self._q.empty()
    def close(self): pass
    def join_thread(self): pass
    def cancel_join_thread(self): pass
