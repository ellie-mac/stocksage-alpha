#!/usr/bin/env python3
"""
一次性修复：给四个户部尚书计划任务加 -NoProfile，消除 PS profile 报错导致的退出码污染。
在 VM 上运行：python src/tools/fix_task_noprofile.py
"""
import subprocess
import os

TASKS = [
    "DailyMarket_Update",
    "DailyMA_Update",
    "DailyPortfolio_Update",
    "DailyAnnouncements_Scan",
]

TMP_ORIG  = r"C:\Windows\Temp\task_orig.xml"
TMP_FIXED = r"C:\Windows\Temp\task_fixed.xml"

for name in TASKS:
    # 导出 XML
    r = subprocess.run(["schtasks", "/query", "/xml", "/tn", name], capture_output=True)
    if r.returncode != 0:
        print(f"{name}: export failed - {r.stderr}")
        continue

    xml = r.stdout.decode("utf-16-le", errors="replace")

    if "-NoProfile" in xml:
        print(f"{name}: already has -NoProfile, skipped")
        continue

    fixed = xml.replace("-NonInteractive", "-NoProfile -NonInteractive")

    with open(TMP_FIXED, "w", encoding="utf-16") as f:
        f.write(fixed)

    subprocess.run(["schtasks", "/delete", "/tn", name, "/f"], capture_output=True)
    r2 = subprocess.run(["schtasks", "/create", "/tn", name, "/xml", TMP_FIXED, "/f"], capture_output=True)
    msg = r2.stdout.decode("gbk", errors="replace").strip() or r2.stderr.decode("gbk", errors="replace").strip()
    print(f"{name}: {msg}")

# 验证
print("\n=== 验证 ===")
for name in TASKS:
    r = subprocess.run(["schtasks", "/query", "/xml", "/tn", name], capture_output=True)
    xml = r.stdout.decode("utf-16-le", errors="replace")
    import re
    m = re.search(r"<Arguments>(.*?)</Arguments>", xml)
    args = m.group(1) if m else "?"
    status = "✓" if "-NoProfile" in args else "✗"
    print(f"  {status} {name}: {args[:80]}")
