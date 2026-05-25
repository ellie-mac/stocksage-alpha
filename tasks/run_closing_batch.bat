@echo off
chcp 65001 > nul
echo [%DATE% %TIME%] closing_Batch bat entered >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "closing_Batch" "Closing batch: XHS evening / signal_tracker / perf_log / auto_tune" "started" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
echo [%DATE% %TIME%] closing_Batch invoking python >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\jobs\closing_batch.py" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\closing_batch.log" 2>&1
echo [%DATE% %TIME%] closing_Batch python exit=%errorlevel% >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
if errorlevel 1 (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "closing_Batch" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "closing_Batch" "Closing batch: XHS evening / signal_tracker / perf_log / auto_tune" "failed" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
) else (
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify.py" "closing_Batch" "Closing batch: XHS evening / signal_tracker / perf_log / auto_tune" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify.log" 2>&1
)
