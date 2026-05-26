@echo off
chcp 65001 > nul
title 概念成分股缓存更新
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
mkdir "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs" 2>nul

:: 检查今天是否已成功更新过 (标记文件: logs/constituents_done_YYYYMMDD.flag)
for /f "tokens=1-3 delims=/" %%a in ("%DATE%") do set TODAY=%%a%%b%%c
:: 兼容不同日期格式
if "%TODAY%"=="" for /f "tokens=2 delims==" %%i in ('wmic os get localdatetime /value') do set DT=%%i
if not "%DT%"=="" set TODAY=%DT:~0,8%
set FLAG="C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\constituents_done_%TODAY%.flag"
if exist %FLAG% (
    echo [%DATE% %TIME%] concept_constituents skipped - already done today >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
    exit /b 0
)

echo [%DATE% %TIME%] concept_constituents bat entered >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\concept\concept_constituents.py" update --top 30 >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\concept_constituents.log" 2>&1
if errorlevel 1 (
    echo [%DATE% %TIME%] concept_constituents FAILED >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
    "C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\notify\notify_failure.py" "concept_constituents" >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\notify_failure.log" 2>&1
) else (
    echo [%DATE% %TIME%] concept_constituents SUCCESS >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\task_probe.log"
    echo done > %FLAG%
)
