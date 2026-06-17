@echo off
chcp 65001 >nul
cd /d C:\Users\jiapeichen\repos\stocksage-alpha

set LOG_DIR=src\logs
if not exist %LOG_DIR% mkdir %LOG_DIR%
set LOG=%LOG_DIR%\concept_rotation_job.log

echo [%date% %time%] ===== 概念轮动任务开始 ===== >> %LOG%

python -X utf8 src/concept/concept_rotation_job.py --mode auto --no-push >> %LOG% 2>&1

if %errorlevel% equ 0 (
    echo [%date% %time%] 成功 >> %LOG%
) else (
    echo [%date% %time%] 失败 errorlevel=%errorlevel% >> %LOG%
)
