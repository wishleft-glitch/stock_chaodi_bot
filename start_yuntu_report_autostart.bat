@echo off
setlocal
cd /d %~dp0

if not exist .env.yuntu exit /b 1

python scripts\yuntu_bid_report.py --schedule >> yuntu_bid_report_autostart.log 2>&1
