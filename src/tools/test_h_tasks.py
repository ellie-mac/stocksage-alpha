#!/usr/bin/env python3
"""Quick test: invoke bot_common.h_tasks() and print result."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "stock-bot"))
sys.path.insert(0, str(ROOT / "src"))

from bot_common import h_tasks

print(h_tasks())
