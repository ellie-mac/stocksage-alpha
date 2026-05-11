"""pytest 公共 fixtures"""
from __future__ import annotations

import sys
import os

# 把 src/ 加入 path，让所有测试能 import 项目模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "jobs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "strategies"))
