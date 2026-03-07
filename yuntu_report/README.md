# Yuntu Report

Daily OceanEngine Yuntu bid ranking automation.

## Files

- `scripts/yuntu_bid_report.py`: main scraper + mail sender
- `scripts/init_yuntu_login.py`: login profile initializer
- `.env.yuntu.example`: environment template
- `init_yuntu_login.bat`: open browser and save login state
- `start_yuntu_report.bat`: interactive scheduler start
- `start_yuntu_report_autostart.bat`: no-prompt launcher for Windows autostart

## Setup

```powershell
cd yuntu_report
pip install -r requirements.txt
playwright install chromium
copy .env.yuntu.example .env.yuntu
```

Then fill SMTP settings in `.env.yuntu`, run `init_yuntu_login.bat` once, and use `start_yuntu_report.bat` for normal scheduling.
