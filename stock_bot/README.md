# Stock Bot

Stock screening bot with Telegram notifications.

## Included Files

- `quant_bot_free.py`: main script
- `requirements.txt`: Python dependencies
- `.env.example`: environment template
- `start_quant_bot_free.bat`: Windows launcher

## New Device Setup

1. Install Python 3.10+.
2. Open a terminal in `stock_bot/`.
3. Install dependencies:

```powershell
pip install -r requirements.txt
```

4. Create local config:

```powershell
copy .env.example .env
```

5. Fill the Telegram variables in `.env`:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `US_TELEGRAM_BOT_TOKEN`
- `US_TELEGRAM_CHAT_ID`
- `CN_TELEGRAM_BOT_TOKEN`
- `CN_TELEGRAM_CHAT_ID`
- `HK_TELEGRAM_BOT_TOKEN`
- `HK_TELEGRAM_CHAT_ID`

If all markets use the same bot, the default `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are enough.

## Run Once

```powershell
python quant_bot_free.py --run-once us
python quant_bot_free.py --run-once asia
python quant_bot_free.py --run-once all
```

## Scheduled Run

```powershell
python quant_bot_free.py
```

Current schedule:

- US stocks: Tue-Sat `08:00` Asia/Shanghai
- CN stocks: Mon-Fri `20:00` Asia/Shanghai
- HK stocks: Mon-Fri `20:00` Asia/Shanghai

## Windows Autostart

Use `start_quant_bot_free.bat` after Python is available in `PATH`.

## Notes

- Do not commit `.env`
- Rotate Telegram tokens if they were ever exposed
