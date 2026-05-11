"""串行运行今天 17:10 以前的所有 StockSage 定时任务，记录结果。"""
import subprocess
import time
from datetime import datetime
from pathlib import Path

TASKS = Path(r"C:\Users\jiapeichen\repos\stocksage-alpha\tasks")
LOG   = Path(r"C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\serial_run.log")
LOG.parent.mkdir(parents=True, exist_ok=True)

# 按计划时间排序，factor_Analysis 耗时极长单独给更长超时
TASK_LIST = [
    ("factor_Analysis",  "run_factor_analysis.bat",   120),  # 2min 够看import是否正常
    ("chip_Premarket",   "run_chip_premarket.bat",     300),
    ("integrity_Check",  "run_integrity_check.bat",    300),
    ("concept_Warm",     "run_concept_warm.bat",       300),
    ("institution_Scan", "run_institution_scan.bat",   300),
    ("hot_Rank_0935",    "run_hot_rank_logger.bat",    180),
    ("hot_Rank_1000",    "run_hot_rank_logger.bat",    180),
    ("hot_Rank_1100",    "run_hot_rank_logger.bat",    180),
    ("xhs_Midday",       "run_xhs_midday.bat",         300),
    ("hot_Rank_1330",    "run_hot_rank_logger.bat",    180),
    ("hot_Rank_1430",    "run_hot_rank_logger.bat",    180),
    ("watchlist_Scan",   "run_watchlist_scan.bat",     300),
    ("closing_Batch",    "run_closing_batch.bat",      300),
    ("xhs_Evening",      "run_chip_evening.bat",       300),
    ("market_Warm",      "run_market_warm.bat",        300),
    ("price_Prefetch",   "run_price_prefetch.bat",     300),
]

results = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

log(f"=== 串行任务验证开始 {datetime.now():%Y-%m-%d %H:%M:%S} ===")

for task_name, bat_file, timeout in TASK_LIST:
    bat_path = TASKS / bat_file
    log(f">>> {task_name} ({bat_file}) 超时={timeout}s")
    t0 = time.time()
    try:
        proc = subprocess.run(
            ["cmd", "/c", str(bat_path)],
            timeout=timeout,
            capture_output=False,
            text=True,
        )
        elapsed = time.time() - t0
        status = "OK" if proc.returncode == 0 else f"FAIL(rc={proc.returncode})"
        log(f"    {status}  耗时={elapsed:.0f}s")
        results.append((task_name, status, elapsed))
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        log(f"    TIMEOUT({timeout}s)  任务仍在后台运行")
        results.append((task_name, f"TIMEOUT>{timeout}s", elapsed))
    except Exception as e:
        log(f"    ERROR: {e}")
        results.append((task_name, f"ERROR:{e}", 0))

log("\n=== 汇总 ===")
for name, status, elapsed in results:
    log(f"  {name:<25} {status}  ({elapsed:.0f}s)")
log("=== 完成 ===")
