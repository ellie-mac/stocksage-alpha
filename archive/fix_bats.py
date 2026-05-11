"""One-shot script: update 12 outdated .bat files from scripts/ paths to src/ paths."""
from pathlib import Path

TASKS = Path(r"C:\Users\jiapeichen\repos\stocksage-alpha\tasks")

REPLACEMENTS = [
    ("scripts\\notify_failure.py",           "src\\notify\\notify_failure.py"),
    ("scripts\\notify_discord.py",            "src\\notify\\notify.py"),
    ("scripts\\notify.py",                    "src\\notify\\notify.py"),
    ("scripts\\hot_scan.py",                  "src\\strategies\\hot_scan.py"),
    ("scripts\\monitor.py",                   "src\\monitor.py"),
    ("scripts\\morning_guard.py",             "src\\jobs\\morning_guard.py"),
    ("scripts\\prefetch.py",                  "src\\jobs\\prefetch.py"),
    ("scripts\\tools\\hot_rank_logger.py",    "src\\tools\\hot_rank_logger.py"),
    ("scripts\\factor_analysis.py",           "src\\factors\\analysis.py"),
    ("scripts\\chip_perf_log.py",             "src\\jobs\\daily_perf_log.py"),
    ("scripts\\gc_perf_log.py",               "src\\jobs\\daily_perf_log.py"),
    ("scripts\\main_perf_log.py",             "src\\jobs\\daily_perf_log.py"),
    ("xhs\\chip_writer.py",                   "src\\report\\reporter.py"),
    ("xhs\\reporter.py",                      "src\\report\\reporter.py"),
    ("xhs\\writer.py",                        "src\\report\\reporter.py"),
    ("scripts\\logs\\",                       "src\\logs\\"),
    # mkdir line
    (r'mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\scripts\logs"',
     r'mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs"'),
    # factor_analysis log path that uses top-level logs/
    (r"logs\factor_analysis.log",             r"src\logs\factor_analysis.log"),
]

TARGETS = [
    "run_chip_midday.bat", "run_chip_perf_log.bat", "run_factor_analysis.bat",
    "run_fundflow_prefetch.bat", "run_gc_perf_log.bat", "run_hot_rank_logger.bat",
    "run_hot_scan.bat", "run_main_perf_log.bat", "run_monitor_scan.bat",
    "run_morning_scan.bat", "run_xhs_morning.bat", "run_xhs_preauction.bat",
]

for name in TARGETS:
    p = TASKS / name
    text = p.read_text(encoding="utf-8-sig")
    original = text
    for old, new in REPLACEMENTS:
        text = text.replace(old, new)
    if text != original:
        p.write_text(text, encoding="utf-8")
        print(f"Updated: {name}")
    else:
        print(f"No change: {name}")

print("Done.")
