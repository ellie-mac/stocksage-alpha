@echo off
chcp 65001 > nul
cd /d "C:\Users\jiapeichen\repos\stocksage-alpha"
"C:\Program Files\Python313\python.exe" -X utf8 "C:\Users\jiapeichen\repos\stocksage-alpha\src\report\reporter.py" preauction --style auto >> "C:\Users\jiapeichen\repos\stocksage-alpha\src\logs\xhs_preauction.log" 2>&1
