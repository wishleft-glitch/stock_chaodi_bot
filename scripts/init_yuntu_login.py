import time
import os
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


ROOT_DIR = Path(__file__).resolve().parent.parent
TARGET_URL = (
    "https://yuntu.oceanengine.com/yuntu_brand/game/insight/bid_price"
    "?aadvid=1715399344232456"
)


def main() -> None:
    load_dotenv(ROOT_DIR / ".env.yuntu", override=False)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR", r"C:\playwright-yuntu-profile"))
    user_data_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            viewport={"width": 1600, "height": 1000},
            locale="zh-CN",
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
        print("Browser opened. Please log in to Yuntu in this window, then press Ctrl+C here to stop.")
        try:
            while True:
                time.sleep(2)
        except KeyboardInterrupt:
            pass
        finally:
            context.close()


if __name__ == "__main__":
    main()
