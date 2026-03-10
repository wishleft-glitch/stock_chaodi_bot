@echo off
setlocal

cd /d %~dp0
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" quant_bot_free.py >> quant_bot_runner.log 2>&1
) else (
  where py >nul 2>nul
  if %errorlevel%==0 (
    py -3 quant_bot_free.py >> quant_bot_runner.log 2>&1
  ) else (
    python quant_bot_free.py >> quant_bot_runner.log 2>&1
  )
)

endlocal
