"""
logger 模块基础测试：run_id 绑定/读取/清除
"""
from __future__ import annotations

import importlib
import json
import logging


def _fresh_logger():
    import logger as _mod
    importlib.reload(_mod)
    return _mod


def test_bind_and_get_run_id():
    lg = _fresh_logger()
    assert lg.get_run_id() is None
    lg.bind_run_id(99)
    assert lg.get_run_id() == 99
    lg.bind_run_id(None)
    assert lg.get_run_id() is None


def test_json_formatter_includes_run_id(capsys):
    import sys, io
    lg = _fresh_logger()
    lg.bind_run_id(42)

    # 用一个独立 handler 捕获输出
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(lg._JsonFormatter())
    logger = logging.getLogger("test_logger_fmt")
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    logger.info("hello_test")

    line = buf.getvalue().strip()
    assert line, "should have output"
    parsed = json.loads(line)
    assert parsed["run_id"] == 42
    assert parsed["event"] == "hello_test"
    assert parsed["level"] == "INFO"

    lg.bind_run_id(None)
