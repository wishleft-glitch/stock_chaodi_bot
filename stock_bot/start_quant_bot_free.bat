@echo off
setlocal

cd /d %~dp0
"C:\Users\xiongyu\AppData\Local\Programs\Python\Python312\python.exe" quant_bot_free.py >> quant_bot_runner.log 2>&1

endlocal
