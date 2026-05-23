"""检查今日 chip/gc ∩ market[mv 21-50] 是否有 picks（用于验证 TOP_N=50 是否解锁新规则）"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
mc = json.loads((ROOT / "data" / "marketcap_latest.json").read_text(encoding="utf-8"))
chip = json.loads((ROOT / "data" / "chip_scan_latest.json").read_text(encoding="utf-8"))
gc = json.loads((ROOT / "data" / "golden_cross_latest.json").read_text(encoding="utf-8"))

mc_codes = {p["code"]: (p.get("mv_rank"), p.get("name", "")) for p in mc.get("picks", []) if p.get("mv_rank")}
chip_codes = {p["code"]: p.get("tier", "") for p in chip.get("all_picks", [])}
gc_codes = {}
for tier_name, tier_picks in gc.get("tiers", {}).items():
    for p in tier_picks:
        c = str(p.get("code", ""))[-6:]
        gc_codes[c] = tier_name

print(f"marketcap_latest: {len(mc_codes)} picks (rank 1-{max(v[0] for v in mc_codes.values())})")
print(f"chip_scan: {len(chip_codes)} picks")
print(f"gc: {len(gc_codes)} picks")

# overlap by mv_rank bucket
for lo, hi in [(1, 20), (21, 50)]:
    mc_bucket = {c: mc_codes[c] for c in mc_codes if lo <= mc_codes[c][0] <= hi}
    chip_overlap = [(c, mc_bucket[c][0], mc_bucket[c][1], chip_codes[c]) for c in mc_bucket if c in chip_codes]
    gc_overlap = [(c, mc_bucket[c][0], mc_bucket[c][1], gc_codes[c]) for c in mc_bucket if c in gc_codes]
    three_way = [(c, mc_bucket[c][0], mc_bucket[c][1], chip_codes[c], gc_codes[c]) for c in mc_bucket if c in chip_codes and c in gc_codes]
    print(f"\nmv rank {lo}-{hi} ({len(mc_bucket)} 只市值池):")
    print(f"  ∩ chip:    {len(chip_overlap)}")
    for c, rk, n, t in chip_overlap[:5]:
        print(f"    {c} {n} rank{rk} chip[{t}]")
    print(f"  ∩ gc:      {len(gc_overlap)}")
    for c, rk, n, t in gc_overlap[:5]:
        print(f"    {c} {n} rank{rk} gc[{t}]")
    print(f"  ∩ chip ∩ gc (3-way): {len(three_way)}")
    for c, rk, n, ct, gt in three_way[:5]:
        print(f"    {c} {n} rank{rk} chip[{ct}]+gc[{gt}]")
