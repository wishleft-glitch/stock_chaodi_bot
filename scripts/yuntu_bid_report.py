import json
import logging
import os
import smtplib
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import schedule
from dotenv import load_dotenv
from playwright.sync_api import BrowserContext, Page, sync_playwright


TARGET_URL = (
    "https://yuntu.oceanengine.com/yuntu_brand/game/insight/bid_price"
    "?aadvid=1715399344232456"
)
DEFAULT_RECIPIENT = "xiongyu@corp.netease.com"
DEFAULT_RUN_TIME = "16:00"
DEFAULT_TOP_N = 50
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_RETRY_START_HOUR = 16
DEFAULT_RETRY_END_HOUR = 23
STATE_FILE = Path(__file__).with_name("yuntu_bid_report_state.json")
ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILES = [
    ROOT_DIR / ".env.yuntu",
    ROOT_DIR / ".env",
]
LABEL_INDUSTRY = "\u884c\u4e1a"
LABEL_BID_RANKING = "\u7ade\u4ef7\u699c"
LABEL_PRODUCT_TYPE = "\u4ea7\u54c1\u5f62\u6001"
LABEL_APP = "APP"
LABEL_APP_TITLE = "App"
LABEL_YESTERDAY = "\u6628\u65e5"
LABEL_YESTERDAY_ALT = "\u6628\u5929"
LABEL_DAILY = "\u65e5\u699c"
LABEL_CONFIRM = "\u786e\u5b9a"
LABEL_CONFIRM_ALT = "\u786e\u8ba4"
LABEL_DONE = "\u5b8c\u6210"
LABEL_SEARCH = "\u67e5\u8be2"
LABEL_DATE = "\u65e5\u671f"
LABEL_TIME = "\u65f6\u95f4"
LABEL_START = "\u5f00\u59cb"
LABEL_END = "\u7ed3\u675f"
COL_RANK = "\u6392\u540d"
COL_CHANGE = "\u73af\u6bd4"
COL_GAME = "\u6e38\u620f"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_env_files() -> None:
    for env_file in DEFAULT_ENV_FILES:
        if env_file.exists():
            load_dotenv(env_file, override=False)


@dataclass
class EmailConfig:
    smtp_host: str
    smtp_port: int
    sender_email: str
    sender_password: str
    recipient_email: str
    use_tls: bool = True
    use_ssl: bool = False


@dataclass
class ScraperConfig:
    user_data_dir: Optional[str]
    storage_state_path: Optional[str]
    cookies_path: Optional[str]
    headless: bool
    top_n: int
    timezone: str


class DataNotReadyError(Exception):
    pass


def load_email_config() -> EmailConfig:
    return EmailConfig(
        smtp_host=os.getenv("SMTP_HOST", "smtp.example.com"),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        sender_email=os.getenv("SMTP_SENDER", "YOUR_EMAIL@example.com"),
        sender_password=os.getenv("SMTP_PASSWORD", "YOUR_PASSWORD"),
        recipient_email=os.getenv("MAIL_TO", DEFAULT_RECIPIENT),
        use_tls=os.getenv("SMTP_USE_TLS", "true").lower() == "true",
        use_ssl=os.getenv("SMTP_USE_SSL", "false").lower() == "true",
    )


def load_scraper_config() -> ScraperConfig:
    return ScraperConfig(
        user_data_dir=os.getenv("PLAYWRIGHT_USER_DATA_DIR"),
        storage_state_path=os.getenv("PLAYWRIGHT_STORAGE_STATE"),
        cookies_path=os.getenv("PLAYWRIGHT_COOKIES_PATH"),
        headless=os.getenv("PLAYWRIGHT_HEADLESS", "false").lower() == "true",
        top_n=int(os.getenv("TOP_N", str(DEFAULT_TOP_N))),
        timezone=os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE),
    )


def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("Failed to read state file: %s", STATE_FILE)
        return {}


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def mark_sent(report_date: str) -> None:
    state = load_state()
    state["last_sent_report_date"] = report_date
    state["last_sent_at"] = datetime.now().isoformat(timespec="seconds")
    save_state(state)


def was_sent(report_date: str) -> bool:
    return load_state().get("last_sent_report_date") == report_date


def send_email(subject: str, html_body: str, email_config: EmailConfig) -> None:
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = email_config.sender_email
    message["To"] = email_config.recipient_email
    message.attach(MIMEText(html_body, "html", "utf-8"))

    smtp_cls = smtplib.SMTP_SSL if email_config.use_ssl else smtplib.SMTP
    with smtp_cls(email_config.smtp_host, email_config.smtp_port, timeout=30) as server:
        if email_config.use_tls and not email_config.use_ssl:
            server.starttls()
        if email_config.sender_email and email_config.sender_password:
            server.login(email_config.sender_email, email_config.sender_password)
        server.sendmail(
            email_config.sender_email,
            [email_config.recipient_email],
            message.as_string(),
        )


def build_html_table(df: pd.DataFrame, report_date: str) -> str:
    style = """
    <style>
      body { font-family: Arial, sans-serif; color: #1f2937; }
      h2 { margin-bottom: 12px; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #d1d5db; padding: 8px 10px; text-align: left; }
      th { background: #f3f4f6; }
      tr:nth-child(even) { background: #f9fafb; }
      .meta { margin-bottom: 16px; color: #4b5563; }
    </style>
    """
    table_html = df.to_html(index=False, escape=False, border=0)
    return f"""
    <html>
      <head>{style}</head>
      <body>
        <h2>\u5de8\u91cf\u4e91\u56fe APP \u7ade\u4ef7\u699c\u65e5\u62a5</h2>
        <div class="meta">\u7edf\u8ba1\u65e5\u671f\uff1a{report_date}</div>
        {table_html}
      </body>
    </html>
    """


def build_failure_html(report_date: str, error_message: str) -> str:
    return f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2>\u5de8\u91cf\u4e91\u56fe APP \u7ade\u4ef7\u699c\u6293\u53d6\u5931\u8d25</h2>
        <p>\u7edf\u8ba1\u65e5\u671f\uff1a{report_date}</p>
        <p>\u9519\u8bef\u4fe1\u606f\uff1a</p>
        <pre style="background:#f4f4f4;padding:12px;border:1px solid #ddd;">{error_message}</pre>
        <p>\u8bf7\u68c0\u67e5\u9875\u9762\u7ed3\u6784\u3001\u767b\u5f55\u6001\u6216\u7b5b\u9009\u63a7\u4ef6\u662f\u5426\u53d1\u751f\u53d8\u5316\u3002</p>
      </body>
    </html>
    """


def build_not_ready_html(report_date: str, reason: str) -> str:
    return f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2>\u5de8\u91cf\u4e91\u56fe APP \u7ade\u4ef7\u699c\u672a\u66f4\u65b0</h2>
        <p>\u76ee\u6807\u65e5\u671f\uff1a{report_date}</p>
        <p>\u8bf4\u660e\uff1a{reason}</p>
      </body>
    </html>
    """


def get_yesterday() -> date:
    return date.today() - timedelta(days=1)


def locator_is_visible(page: Page, selector: str) -> bool:
    try:
        return page.locator(selector).first.is_visible(timeout=1500)
    except Exception:
        return False


def click_first_visible(page: Page, selectors: Iterable[str], timeout_ms: int = 15000) -> None:
    last_error: Optional[Exception] = None
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=timeout_ms)
            locator.click(timeout=timeout_ms)
            logging.info("Clicked selector: %s", selector)
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Failed to click any selector: {list(selectors)}; last_error={last_error}")


def fill_first_visible(page: Page, selectors: Iterable[str], value: str, timeout_ms: int = 15000) -> None:
    last_error: Optional[Exception] = None
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=timeout_ms)
            locator.fill(value, timeout=timeout_ms)
            logging.info("Filled selector: %s with value: %s", selector, value)
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Failed to fill any selector: {list(selectors)}; last_error={last_error}")


def maybe_apply_cookies(context: BrowserContext, cookies_path: Optional[str], base_url: str) -> None:
    if not cookies_path:
        return

    cookies_file = Path(cookies_path)
    if not cookies_file.exists():
        raise FileNotFoundError(f"Cookies file not found: {cookies_file}")

    with cookies_file.open("r", encoding="utf-8") as fp:
        cookies = json.load(fp)

    normalized = []
    for cookie in cookies:
        normalized.append(
            {
                "name": cookie["name"],
                "value": cookie["value"],
                "domain": cookie.get("domain") or ".oceanengine.com",
                "path": cookie.get("path", "/"),
                "expires": cookie.get("expires", -1),
                "httpOnly": cookie.get("httpOnly", False),
                "secure": cookie.get("secure", True),
                "sameSite": cookie.get("sameSite", "Lax"),
                "url": cookie.get("url", base_url),
            }
        )
    context.add_cookies(normalized)
    logging.info("Loaded %s cookies from %s", len(normalized), cookies_file)


def launch_context(playwright: Any, scraper_config: ScraperConfig) -> Tuple[BrowserContext, Optional[Any]]:
    browser_type = playwright.chromium
    if scraper_config.user_data_dir:
        logging.info("Launching persistent context with user data dir: %s", scraper_config.user_data_dir)
        context = browser_type.launch_persistent_context(
            user_data_dir=scraper_config.user_data_dir,
            headless=scraper_config.headless,
            viewport={"width": 1600, "height": 1000},
            locale="zh-CN",
        )
        return context, None

    logging.info("Launching standard browser context")
    browser = browser_type.launch(headless=scraper_config.headless)
    context_kwargs: Dict[str, Any] = {
        "viewport": {"width": 1600, "height": 1000},
        "locale": "zh-CN",
    }
    if scraper_config.storage_state_path and Path(scraper_config.storage_state_path).exists():
        context_kwargs["storage_state"] = scraper_config.storage_state_path

    context = browser.new_context(**context_kwargs)
    maybe_apply_cookies(context, scraper_config.cookies_path, TARGET_URL)
    return context, browser


def wait_for_table(page: Page) -> None:
    selectors = [
        "table tbody tr",
        ".byted-table-tbody tr",
        "[class*='table'] tbody tr",
        "[role='rowgroup'] [role='row']",
    ]

    last_error: Optional[Exception] = None
    for selector in selectors:
        try:
            page.locator(selector).first.wait_for(state="visible", timeout=20000)
            logging.info("Detected data table using selector: %s", selector)
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Data table did not load. last_error={last_error}")


def set_yesterday_filter(page: Page, report_date: date) -> None:
    report_text = report_date.strftime("%Y-%m-%d")
    try:
        input_values = page.evaluate(
            """
            () => Array.from(document.querySelectorAll('input'))
              .map((el) => (el.value || '').trim())
              .filter(Boolean)
            """
        )
        if report_text in input_values:
            logging.info("Date input already matches yesterday: %s", report_text)
            return
    except Exception:
        input_values = []

    direct_date_inputs = [
        f"input[value='{report_text}']",
        f"input[placeholder*='{LABEL_TIME}']",
        f"input[placeholder*='{LABEL_DATE}']",
    ]
    for selector in direct_date_inputs:
        try:
            locator = page.locator(selector).first
            if locator.count():
                locator.wait_for(state="visible", timeout=3000)
                current = locator.input_value(timeout=1000).strip()
                if current == report_text:
                    logging.info("Date input already matches yesterday via direct selector: %s", report_text)
                    return
                locator.fill(report_text, timeout=5000)
                page.keyboard.press("Enter")
                page.wait_for_timeout(1000)
                logging.info("Filled direct date input with %s", report_text)
                return
        except Exception:
            continue

    # Prefer quick shortcuts if the date component exposes the yesterday preset.
    quick_selectors = [
        f"text={LABEL_YESTERDAY}",
        f"text={LABEL_YESTERDAY_ALT}",
        f"button:has-text('{LABEL_YESTERDAY}')",
        f"li:has-text('{LABEL_YESTERDAY}')",
        f"[role='option']:has-text('{LABEL_YESTERDAY}')",
    ]
    for selector in quick_selectors:
        try:
            if locator_is_visible(page, selector):
                page.locator(selector).first.click(timeout=3000)
                logging.info("Selected quick date shortcut with selector: %s", selector)
                return
        except Exception:
            pass

    date_trigger_selectors = [
        f"text={LABEL_DAILY}",
        "[class*='picker'] input",
        f"[placeholder*='{LABEL_DATE}']",
        f"[placeholder*='{LABEL_TIME}']",
        "input[readonly]",
    ]
    click_first_visible(page, date_trigger_selectors, timeout_ms=12000)
    time.sleep(1)

    # Try direct input to start/end date fields.
    input_selectors = [
        ".arco-picker input",
        ".byted-picker input",
        f"input[placeholder*='{LABEL_START}']",
        f"input[placeholder*='{LABEL_END}']",
        f"input[placeholder*='{LABEL_DATE}']",
    ]
    visible_inputs = []
    for selector in input_selectors:
        try:
            count = page.locator(selector).count()
            for index in range(count):
                candidate = page.locator(selector).nth(index)
                if candidate.is_visible():
                    visible_inputs.append(candidate)
        except Exception:
            continue

    if len(visible_inputs) >= 2:
        visible_inputs[0].fill(report_text)
        visible_inputs[1].fill(report_text)
        page.keyboard.press("Enter")
        logging.info("Filled date range inputs with %s", report_text)
        return

    if len(visible_inputs) == 1:
        visible_inputs[0].fill(report_text)
        page.keyboard.press("Enter")
        logging.info("Filled single date input with %s", report_text)
        return

    raise RuntimeError(
        "Failed to set date filter to yesterday. Inspect the page and update date selectors."
    )


def apply_filters(page: Page, report_date: date) -> None:
    page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("networkidle", timeout=30000)

    # The direct URL already lands on the bid ranking page in the current UI, but keep
    # a lightweight confirmation click for resilience if the left-side list renders later.
    for selector in [
        f"text={LABEL_BID_RANKING}",
        f"[class*='menu-item']:has-text('{LABEL_BID_RANKING}')",
        f"[class*='list'] :text('{LABEL_BID_RANKING}')",
    ]:
        try:
            if locator_is_visible(page, selector):
                page.locator(selector).first.click(timeout=3000)
                time.sleep(1)
                break
        except Exception:
            continue

    product_group = page.locator("div.biz_insight-radio-group").first
    product_group.wait_for(state="visible", timeout=15000)
    product_group.locator("span.biz_insight-radio-filled").nth(1).click(force=True, timeout=15000)
    page.wait_for_timeout(1500)
    checked_text = product_group.locator("span.biz_insight-radio-filled-checked").first.inner_text()
    if LABEL_APP_TITLE not in checked_text:
        raise RuntimeError(f"Failed to switch product type to {LABEL_APP_TITLE}. Current: {checked_text}")

    set_yesterday_filter(page, report_date)

    apply_buttons = [
        f"button:has-text('{LABEL_CONFIRM}')",
        f"button:has-text('{LABEL_CONFIRM_ALT}')",
        f"button:has-text('{LABEL_DONE}')",
        f"button:has-text('{LABEL_SEARCH}')",
    ]
    for selector in apply_buttons:
        try:
            if locator_is_visible(page, selector):
                page.locator(selector).first.click(timeout=3000)
                logging.info("Clicked apply button: %s", selector)
                break
        except Exception:
            continue

    page.wait_for_load_state("networkidle", timeout=30000)
    wait_for_table(page)


def page_contains_report_date(page: Page, report_date: date) -> bool:
    variants = {
        report_date.strftime("%Y-%m-%d"),
        report_date.strftime("%Y/%m/%d"),
        report_date.strftime("%Y.%m.%d"),
        report_date.strftime("%m-%d"),
        report_date.strftime("%m/%d"),
    }
    content = page.content()
    for variant in variants:
        if variant in content:
            logging.info("Detected report date marker in page content: %s", variant)
            return True
    try:
        input_count = page.locator("input").count()
        for index in range(input_count):
            value = page.locator("input").nth(index).input_value(timeout=500)
            if value in variants:
                logging.info("Detected report date marker in date input value: %s", value)
                return True
    except Exception:
        pass
    return False


def normalize_record(record: Dict[str, Any]) -> Optional[Dict[str, str]]:
    rank_keys = [COL_RANK, "rank", "\u5e8f\u53f7"]
    change_keys = [COL_CHANGE, "\u540c\u6bd4", "mom", "dod", "change", "\u6da8\u8dcc\u5e45"]
    name_keys = [COL_GAME, "\u6e38\u620f\u540d\u79f0", "name", "app", "\u4ea7\u54c1", "title"]

    def pick(keys: List[str]) -> Optional[str]:
        for key, value in record.items():
            lowered = str(key).strip().lower()
            for expected in keys:
                if expected.lower() in lowered and str(value).strip():
                    return str(value).strip()
        return None

    normalized = {
        COL_RANK: pick(rank_keys),
        COL_CHANGE: pick(change_keys),
        COL_GAME: pick(name_keys),
    }
    if all(normalized.values()):
        return normalized
    return None


def extract_table_data(page: Page, top_n: int) -> List[Dict[str, str]]:
    script = """
    () => {
      const text = (el) => (el?.innerText || el?.textContent || '').trim();
      const tables = Array.from(document.querySelectorAll('table'));
      for (const table of tables) {
        const headers = Array.from(table.querySelectorAll('thead th')).map(text);
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        if (!rows.length) continue;
        const records = rows.map((row) => {
          const cells = Array.from(row.querySelectorAll('td'));
          const obj = {
            __row_class: row.className || '',
          };
          cells.forEach((cell, index) => {
            const key = headers[index] || `col_${index}`;
            obj[key] = text(cell);
            obj[`__html_${index}`] = cell.innerHTML || '';
          });
          return obj;
        });
        return records;
      }

      const rowNodes = Array.from(document.querySelectorAll("[role='rowgroup'] [role='row']"));
      if (rowNodes.length) {
        return rowNodes.map((row) => {
          const cells = Array.from(row.querySelectorAll("[role='cell']"));
          const obj = {};
          cells.forEach((cell, index) => {
            obj[`col_${index}`] = text(cell);
          });
          return obj;
        });
      }
      return [];
    }
    """
    raw_rows = page.evaluate(script)
    logging.info("Extracted %s raw rows from page", len(raw_rows))
    parsed_rows: List[Dict[str, str]] = []
    medal_map = {
        "rank-first": "1",
        "rank-second": "2",
        "rank-third": "3",
    }

    for row in raw_rows:
        row_class = str(row.get("__row_class", ""))
        if "pinnedRow" in row_class:
            continue

        rank_html = str(row.get("__html_0", ""))
        rank_text = str(row.get(COL_RANK) or row.get("col_0") or "").strip()
        if not rank_text:
            for marker, rank in medal_map.items():
                if marker in rank_html:
                    rank_text = rank
                    break

        change_html = str(row.get("__html_1", ""))
        raw_change_text = str(row.get(COL_CHANGE) or row.get("col_1") or "").strip()
        change_text = normalize_change_value(raw_change_text, change_html)
        game_text = str(row.get(COL_GAME) or row.get("col_2") or "").strip()

        if rank_text and change_text and game_text:
            parsed_rows.append(
                {
                    COL_RANK: rank_text,
                    COL_CHANGE: change_text,
                    COL_GAME: game_text,
                }
            )

    if parsed_rows:
        parsed_rows.sort(key=lambda item: int(item[COL_RANK]) if item[COL_RANK].isdigit() else 9999)
        return parsed_rows[:top_n]

    raise RuntimeError("Unable to extract rank / game / change columns from the table.")


def normalize_change_value(raw_change_text: str, change_html: str) -> str:
    value = raw_change_text.strip()
    compact_html = change_html.lower()

    if not value or value == "-- 0":
        return "0"

    numeric = "".join(char for char in value if char.isdigit())
    if not numeric:
        return "0"

    if "uptriangle" in compact_html:
        return f"+{numeric}"
    if "downtriangle" in compact_html:
        return f"-{numeric}"
    return numeric


def scrape_report(scraper_config: ScraperConfig) -> pd.DataFrame:
    report_date = get_yesterday()
    with sync_playwright() as playwright:
        context, browser = launch_context(playwright, scraper_config)
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(20000)
            apply_filters(page, report_date)
            if not page_contains_report_date(page, report_date):
                raise DataNotReadyError(
                    f"Yesterday data marker {report_date.strftime('%Y-%m-%d')} was not found on the page."
                )
            rows = extract_table_data(page, scraper_config.top_n)
            df = pd.DataFrame(rows, columns=[COL_RANK, COL_CHANGE, COL_GAME])
            if df.empty:
                raise RuntimeError("No report data found after extraction.")
            return df
        finally:
            context.close()
            if browser is not None:
                browser.close()


def run_job(send_not_ready_email: bool = False) -> bool:
    email_config = load_email_config()
    scraper_config = load_scraper_config()
    report_date = get_yesterday().strftime("%Y-%m-%d")
    subject = f"\u3010\u6bcf\u65e5\u62a5\u544a\u3011\u5de8\u91cf\u4e91\u56feAPP\u7ade\u4ef7\u699c - {report_date}"

    if was_sent(report_date):
        logging.info("Report for %s was already sent. Skipping.", report_date)
        return True

    try:
        df = scrape_report(scraper_config)
        html_body = build_html_table(df, report_date)
        send_email(subject, html_body, email_config)
        mark_sent(report_date)
        logging.info("Report email sent successfully to %s", email_config.recipient_email)
        return True
    except DataNotReadyError as exc:
        logging.warning("Yesterday data is not ready yet: %s", exc)
        if send_not_ready_email:
            not_ready_subject = (
                f"\u3010\u6570\u636e\u672a\u66f4\u65b0\u63d0\u9192\u3011"
                f"\u5de8\u91cf\u4e91\u56feAPP\u7ade\u4ef7\u699c - {report_date}"
            )
            not_ready_html = build_not_ready_html(report_date, str(exc))
            send_email(not_ready_subject, not_ready_html, email_config)
        return False
    except Exception as exc:
        logging.exception("Daily report job failed")
        failure_subject = (
            f"\u3010\u6293\u53d6\u5931\u8d25\u544a\u8b66\u3011"
            f"\u5de8\u91cf\u4e91\u56feAPP\u7ade\u4ef7\u699c - {report_date}"
        )
        failure_html = build_failure_html(report_date, str(exc))
        try:
            send_email(failure_subject, failure_html, email_config)
            logging.info("Failure alert email sent successfully")
        except Exception:
            logging.exception("Failed to send failure alert email")
        raise


def scheduled_check() -> None:
    now = datetime.now()
    report_date = get_yesterday().strftime("%Y-%m-%d")
    retry_start = int(os.getenv("RETRY_START_HOUR", str(DEFAULT_RETRY_START_HOUR)))
    retry_end = int(os.getenv("RETRY_END_HOUR", str(DEFAULT_RETRY_END_HOUR)))

    if now.hour < retry_start or now.hour > retry_end:
        logging.info("Outside retry window %02d:00-%02d:59. Skipping.", retry_start, retry_end)
        return

    if was_sent(report_date):
        logging.info("Report already sent for %s. No further retries needed.", report_date)
        return

    success = run_job(send_not_ready_email=False)
    if success:
        logging.info("Scheduled check completed and report was sent.")
    else:
        logging.info("Scheduled check completed. Data not ready; will retry on next hourly run.")


def run_scheduler(run_time: str) -> None:
    minute = run_time.split(":", 1)[1] if ":" in run_time else "00"
    schedule.every().hour.at(f":{minute}").do(scheduled_check)
    logging.info(
        "Scheduler started. Checks will begin from %s and then retry hourly until sent.",
        run_time,
    )
    while True:
        schedule.run_pending()
        time.sleep(30)


def print_usage() -> None:
    print(
        "Usage:\n"
        "  python scripts/yuntu_bid_report.py --run-once\n"
        "  python scripts/yuntu_bid_report.py --schedule\n\n"
        "Optional environment variables:\n"
        "  SMTP_HOST, SMTP_PORT, SMTP_SENDER, SMTP_PASSWORD, SMTP_USE_TLS, SMTP_USE_SSL, MAIL_TO\n"
        "  PLAYWRIGHT_USER_DATA_DIR, PLAYWRIGHT_STORAGE_STATE, PLAYWRIGHT_COOKIES_PATH\n"
        "  PLAYWRIGHT_HEADLESS, TOP_N, REPORT_TIMEZONE, RUN_TIME\n"
        "  RETRY_START_HOUR, RETRY_END_HOUR\n"
    )


def main() -> None:
    load_env_files()
    setup_logging()
    run_time = os.getenv("RUN_TIME", DEFAULT_RUN_TIME)

    if "--run-once" in sys.argv:
        run_job()
        return

    if "--schedule" in sys.argv:
        run_scheduler(run_time)
        return

    print_usage()


if __name__ == "__main__":
    main()
