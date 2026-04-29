#!/usr/bin/env python3
"""Shim — CAD logic has moved into chip_strategy.py --cad."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from chip_strategy import main
main()
