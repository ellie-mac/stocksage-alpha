"""
结构化日志 — stdlib logging + contextvars 实现 run_id 全链路透传。

用法：
    from logger import get_logger, bind_run_id

    # 在 start_run 后绑定
    run_id = start_run(job_name, trade_date)
    bind_run_id(run_id)

    # 任意子模块直接使用，无需手动传 run_id
    log = get_logger(__name__)
    log.info("strategy_started", extra={"strategy": "main"})
    # → {"ts":"2099-01-01 22:00:01","level":"INFO","run_id":42,"logger":"strategies.base",
    #    "event":"strategy_started","strategy":"main"}
"""
from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime
from typing import Any

_run_id_var: ContextVar[int | None] = ContextVar("run_id", default=None)


def bind_run_id(run_id: int | None) -> None:
    """在当前上下文（线程/协程）绑定 run_id；pass None 清除。"""
    _run_id_var.set(run_id)


def get_run_id() -> int | None:
    return _run_id_var.get()


# ── JSON formatter ────────────────────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        d: dict[str, Any] = {
            "ts":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "level":  record.levelname,
            "run_id": _run_id_var.get(),
            "logger": record.name,
            "event":  record.getMessage(),
        }
        # 把 extra= 里的字段展开进顶层
        for k, v in record.__dict__.items():
            if k not in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            }:
                d[k] = v
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, ensure_ascii=False, default=str)


# ── 全局 handler 初始化（只做一次）─────────────────────────────────────────────

_initialized = False

def _init_logging() -> None:
    global _initialized
    if _initialized:
        return
    _initialized = True

    root = logging.getLogger()
    if root.handlers:
        return  # 已被外部配置，不覆盖

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)


_init_logging()


# ── 公共入口 ──────────────────────────────────────────────────────────────────

def get_logger(name: str = "stocksage") -> logging.Logger:
    return logging.getLogger(name)
