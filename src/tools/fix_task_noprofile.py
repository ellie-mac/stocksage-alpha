#!/usr/bin/env python3
"""
修复：给四个户部尚书计划任务加 -NoProfile，消除 PS profile 报错导致的退出码污染。
若任务不存在则从零创建。
在 VM 上运行：python src/tools/fix_task_noprofile.py
"""
import subprocess
import re

BASE = r"C:\Users\jiapeichen\repos\me\life\scripts"

TASKS = [
    ("DailyMarket_Update",      "18:02", "daily_market.ps1"),
    ("DailyMA_Update",          "18:00", "update_ma.ps1"),
    ("DailyPortfolio_Update",   "18:04", "daily_portfolio.ps1"),
    ("DailyAnnouncements_Scan", "18:06", "daily_announcements.ps1"),
]

TMP = r"C:\Windows\Temp\task_fixed.xml"


def query_xml(name: str) -> str | None:
    r = subprocess.run(["schtasks", "/query", "/xml", "/tn", name], capture_output=True)
    if r.returncode != 0:
        return None
    raw = r.stdout
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    return raw.decode("utf-16-le", errors="replace")


def create_fresh(name: str, time: str, script: str) -> str:
    tr = f"PowerShell.exe -NoProfile -NonInteractive -WindowStyle Hidden -File {BASE}\\{script}"
    r = subprocess.run([
        "schtasks", "/create",
        "/tn", name,
        "/tr", tr,
        "/sc", "daily",
        "/st", time,
        "/sd", "2026/05/14",
        "/du", "0072:00",
        "/f",
    ], capture_output=True)
    out = (r.stdout or r.stderr).decode("gbk", errors="replace").strip()
    return out


def patch_xml(name: str) -> str:
    xml = query_xml(name)
    if xml is None:
        return "not found"
    if "-NoProfile" in xml:
        return "already has -NoProfile"
    fixed = xml.replace("-NonInteractive", "-NoProfile -NonInteractive")
    with open(TMP, "wb") as f:
        f.write(fixed.encode("utf-16"))
    subprocess.run(["schtasks", "/delete", "/tn", name, "/f"], capture_output=True)
    r = subprocess.run(["schtasks", "/create", "/tn", name, "/xml", TMP, "/f"], capture_output=True)
    return (r.stdout or r.stderr).decode("gbk", errors="replace").strip()


for name, time, script in TASKS:
    xml = query_xml(name)
    if xml is None:
        # 任务不存在，从零创建
        msg = create_fresh(name, time, script)
        print(f"[CREATE] {name}: {msg}")
    else:
        msg = patch_xml(name)
        print(f"[PATCH]  {name}: {msg}")

# 验证
print("\n=== 验证 ===")
for name, _, _ in TASKS:
    xml = query_xml(name)
    if xml is None:
        print(f"  ✗ {name}: NOT FOUND")
        continue
    m = re.search(r"<Arguments>(.*?)</Arguments>", xml)
    args = m.group(1) if m else "?"
    ok = "✓" if "-NoProfile" in args else "✗"
    print(f"  {ok} {name}: {args[:90]}")
