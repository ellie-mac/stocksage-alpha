#!/usr/bin/env python3
from pathlib import Path
lines = Path("C:/Users/jiapeichen/repos/stocksage-alpha/src/jobs/nightly_scan.py").read_text(encoding="utf-8").splitlines()
for i, line in enumerate(lines[88:140], start=89):
    print(f"{i:3d}: {line}")
