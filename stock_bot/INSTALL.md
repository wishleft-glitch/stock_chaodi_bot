# Install In 5 Minutes

## 1. Install Python

Install Python 3.10 or newer and make sure `python` or `py` works in terminal.

## 2. Open This Folder

```powershell
cd stock_bot
```

## 3. Install Dependencies

```powershell
pip install -r requirements.txt
```

## 4. Create Local Config

```powershell
copy .env.example .env
```

Then fill:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional if using different bots per market:

- `US_TELEGRAM_BOT_TOKEN`
- `US_TELEGRAM_CHAT_ID`
- `CN_TELEGRAM_BOT_TOKEN`
- `CN_TELEGRAM_CHAT_ID`
- `HK_TELEGRAM_BOT_TOKEN`
- `HK_TELEGRAM_CHAT_ID`

## 5. Test Once

```powershell
python quant_bot_free.py --run-once all
```

## 6. Start Scheduled Mode

```powershell
python quant_bot_free.py
```

## 7. Optional Windows Autostart

```powershell
start_quant_bot_free.bat
```

## Notes

- `.env` is local only
- If Telegram tokens were exposed before, rotate them first
