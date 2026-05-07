#!/usr/bin/env python3
"""兼容 shim — 实际逻辑已迁移到 notify.py"""
from notify import *  # noqa: F401, F403
from notify import main
if __name__ == "__main__":
    main()
