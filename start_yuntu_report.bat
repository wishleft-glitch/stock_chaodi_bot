@echo off
setlocal
cd /d %~dp0

if not exist .env.yuntu (
  echo Missing .env.yuntu
  echo Copy .env.yuntu.example to .env.yuntu and fill SMTP settings first.
  pause
  exit /b 1
)

python scripts\yuntu_bid_report.py --schedule
pause
