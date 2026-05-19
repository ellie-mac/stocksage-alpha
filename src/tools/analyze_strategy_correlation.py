#!/usr/bin/env python3
"""一次性：分析 7 路 evening_strategy 源的 pairwise Jaccard 相关性。

读取当日 latest 文件（main/small/gc/chip/marketcap/hot/sideways），
计算每对的 Jaccard = |A∩B| / |A∪B|，>= 0.3 视为高重合（信号可能冗余）。

用法:
    python -X utf8 src/tools/analyze_strategy_correlation.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "jobs"))

from evening_strategy import (  # noqa: E402
    _load_main, _load_gc, _load_chip, _load_hot, _load_sideways, _load_marketcap,
    _check_sources,
)


def _picks_to_codes(picks: list[dict]) -> set[str]:
    return {p["code"] for p in picks if p.get("code")}


_TAG_LABEL = {
    "主": "main", "小": "small", "叉": "gc", "筹": "chip",
    "市": "marketcap", "热": "hot", "横": "sideways",
}


def main() -> None:
    status = _check_sources(max_days=1)
    main_picks, small_picks = _load_main()
    sources: dict[str, set[str]] = {
        "主": _picks_to_codes(main_picks),
        "小": _picks_to_codes(small_picks),
        "叉": _picks_to_codes(_load_gc()),
        "筹": _picks_to_codes(_load_chip()),
        "市": _picks_to_codes(_load_marketcap()),
        "热": _picks_to_codes(_load_hot()),
        "横": _picks_to_codes(_load_sideways()),
    }
    tags = list(sources.keys())
    print(f"date: {datetime.now().date()}\n")

    print("source size & status:")
    print(f"{'tag':<5}{'size':>6}  status")
    for t in tags:
        s = status.get(t, {})
        st = s.get("status", "?")
        extra = f"({s.get('age_days', '?')}d)" if st == "stale" else ""
        print(f"{t} {_TAG_LABEL[t]:<8}{len(sources[t]):>6}  {st}{extra}")

    print()
    print("pairwise Jaccard matrix:")
    print(f"{'':<6}" + "".join(f"{t:>8}" for t in tags))
    for i, t1 in enumerate(tags):
        row = [f"{t1:<6}"]
        for j, t2 in enumerate(tags):
            if j < i:
                row.append(f"{'':>8}")
            elif j == i:
                row.append(f"{'—':>8}")
            else:
                a, b = sources[t1], sources[t2]
                if not a or not b:
                    row.append(f"{'-':>8}")
                    continue
                jc = len(a & b) / len(a | b)
                marker = "!" if jc >= 0.3 else " "
                row.append(f"{jc:>7.2f}{marker}")
        print("".join(row))

    print()
    print("high-overlap pairs (Jaccard >= 0.3):")
    found = False
    for i, t1 in enumerate(tags):
        for j, t2 in enumerate(tags):
            if j <= i:
                continue
            a, b = sources[t1], sources[t2]
            if not a or not b:
                continue
            inter = len(a & b)
            union = len(a | b)
            if union == 0:
                continue
            jc = inter / union
            if jc >= 0.3:
                print(f"  {t1}({_TAG_LABEL[t1]}) ↔ {t2}({_TAG_LABEL[t2]}): "
                      f"J={jc:.3f}  |A∩B|={inter}  |A|={len(a)}  |B|={len(b)}")
                found = True
    if not found:
        print("  (none)")

    print()
    print("note: Jaccard < 0.1 = 互补; 0.1-0.3 = 弱重合; >= 0.3 = 高重合（信号可能冗余）")


if __name__ == "__main__":
    main()
