#!/usr/bin/env python3
"""Show top picks from escalator_latest.json."""
import json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent.parent
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10
d = json.loads((ROOT / "data" / "escalator_latest.json").read_text(encoding="utf-8"))
print(f"date={d.get('date')}  total={len(d.get('all_picks', []))}")
for p in d.get("all_picks", [])[:N]:
    print(f"  {p['tier']}  {p['code']} {p['name']:<10}  "
          f"R²={p['r2']:.3f}  slope={p['slope_pct']:+5.1f}%  amp={p['daily_amp']:.1f}%  "
          f"dd={p['max_drawdown']:+.1f}%  ¥{p['close']}  ({p.get('industry', '')})")
