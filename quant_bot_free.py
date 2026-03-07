#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
免费开源数据版量化选股与 Telegram 推送脚本

数据源：
1. 美股：finvizfinance + yfinance
2. A 股 / 港股：akshare
3. 推送：Telegram Bot API
4. 调度：APScheduler
"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import socket
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional, Sequence

import numpy as np
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from finvizfinance.screener.overview import Overview
import akshare as ak
import yfinance as yf

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.8
    from backports.zoneinfo import ZoneInfo


load_dotenv()


# =========================
# 全局配置区
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
US_TELEGRAM_BOT_TOKEN = os.getenv("US_TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
US_TELEGRAM_CHAT_ID = os.getenv("US_TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)
CN_TELEGRAM_BOT_TOKEN = os.getenv("CN_TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
CN_TELEGRAM_CHAT_ID = os.getenv("CN_TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)
HK_TELEGRAM_BOT_TOKEN = os.getenv("HK_TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
HK_TELEGRAM_CHAT_ID = os.getenv("HK_TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)

LOG_FILE = "quant_bot.log"
STATE_FILE = "quant_bot_state.json"
MARKET_CAP_CACHE_FILE = "market_cap_cache.json"
TIMEZONE = ZoneInfo("Asia/Shanghai")
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

# 市值门槛
US_MIN_MARKET_CAP = 10_000_000_000       # 100 亿美元
CN_MIN_MARKET_CAP = 10_000_000_000       # 100 亿人民币
HK_MIN_MARKET_CAP = 10_000_000_000       # 100 亿港币

# 指标参数
CCI_PERIOD = 36
CCI_EMA_SPAN = 4
CCI_CROSS_LEVEL = -150
CCI_CONFIRMATION_BUFFER = 5.0
MIN_KLINE_ROWS = 50

# 请求频率控制
US_HISTORY_BATCH_SIZE = 60
US_HISTORY_SLEEP_SECONDS = 1.2
US_EARNINGS_SLEEP_SECONDS = 0.3
ASIA_HISTORY_SLEEP_SECONDS = 0.35
ASIA_HISTORY_BATCH_SIZE = 50
AKSHARE_SPOT_TIMEOUT_SECONDS = 25
SINGLE_INSTANCE_PORT = 47281
MARKET_CAP_CACHE_TTL_DAYS = 7
CN_FALLBACK_TURNOVER_LIMIT = 400
HK_FALLBACK_TURNOVER_LIMIT = 250


_singleton_socket: Optional[socket.socket] = None
_market_cap_cache: Optional[Dict[str, Dict[str, object]]] = None


def setup_logging() -> None:
    """初始化日志，同时输出到本地文件和控制台。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def release_single_instance() -> None:
    """释放单实例锁。"""
    global _singleton_socket
    if _singleton_socket is not None:
        try:
            _singleton_socket.close()
        except Exception:
            pass
        finally:
            _singleton_socket = None
    flush_market_cap_cache()


def ensure_single_instance() -> None:
    """
    使用本机环回端口做单实例保护。

    原理：
    - 第一个启动的进程会成功绑定固定端口；
    - 后续重复启动的进程由于端口已被占用，会立即退出。

    这样可以避免：
    - 登录自启后，用户又手动运行一次
    - 启动文件夹重复触发多个实例
    - 多个调度器同时向 Telegram 重复推送
    """
    global _singleton_socket
    try:
        singleton_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        singleton_socket.bind(("127.0.0.1", SINGLE_INSTANCE_PORT))
        singleton_socket.listen(1)
        _singleton_socket = singleton_socket
        atexit.register(release_single_instance)
        logging.info("单实例保护已生效，锁端口: %s", SINGLE_INSTANCE_PORT)
    except OSError:
        raise SystemExit(f"检测到 quant_bot_free.py 已在运行，当前实例退出。锁端口: {SINGLE_INSTANCE_PORT}")


def load_runtime_state() -> Dict[str, Dict[str, str]]:
    """读取运行状态，用于避免重复推送同一交易日的数据。"""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception:
        logging.exception("读取状态文件失败，将使用空状态。")
        return {}


def save_runtime_state(state: Dict[str, Dict[str, str]]) -> None:
    """保存运行状态到本地 JSON 文件。"""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("保存状态文件失败。")


def load_market_cap_cache() -> Dict[str, Dict[str, object]]:
    """读取市值缓存，降低单股补市值的重复请求成本。"""
    global _market_cap_cache
    if _market_cap_cache is not None:
        return _market_cap_cache
    if not os.path.exists(MARKET_CAP_CACHE_FILE):
        _market_cap_cache = {}
        return _market_cap_cache
    try:
        with open(MARKET_CAP_CACHE_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        _market_cap_cache = data if isinstance(data, dict) else {}
        return _market_cap_cache
    except Exception:
        logging.exception("读取市值缓存失败，将使用空缓存。")
        _market_cap_cache = {}
        return _market_cap_cache


def save_market_cap_cache(cache: Dict[str, Dict[str, object]]) -> None:
    """保存市值缓存。"""
    try:
        with open(MARKET_CAP_CACHE_FILE, "w", encoding="utf-8") as file:
            json.dump(cache, file, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("保存市值缓存失败。")


def flush_market_cap_cache() -> None:
    """将内存中的市值缓存刷到磁盘。"""
    global _market_cap_cache
    if _market_cap_cache is None:
        return
    save_market_cap_cache(_market_cap_cache)


def get_cached_market_cap(cache_key: str) -> Optional[float]:
    """读取未过期的市值缓存。"""
    cache = load_market_cap_cache()
    item = cache.get(cache_key)
    if not item:
        return None
    try:
        saved_at = pd.to_datetime(item.get("saved_at"), errors="coerce")
        market_cap = safe_to_float(item.get("market_cap"))
        if pd.isna(saved_at) or market_cap is None:
            return None
        if isinstance(saved_at, pd.Timestamp):
            if saved_at.tzinfo is None:
                saved_at = saved_at.tz_localize(TIMEZONE)
            else:
                saved_at = saved_at.tz_convert(TIMEZONE)
        if pd.Timestamp.now(tz=TIMEZONE) - saved_at > pd.Timedelta(days=MARKET_CAP_CACHE_TTL_DAYS):
            return None
        return market_cap
    except Exception:
        return None


def set_cached_market_cap(cache_key: str, market_cap: float) -> None:
    """写入市值缓存。"""
    cache = load_market_cap_cache()
    cache[cache_key] = {
        "market_cap": market_cap,
        "saved_at": datetime.now(TIMEZONE).isoformat(),
    }


def should_skip_push(market_key: str, signal_date: Optional[str]) -> bool:
    """
    判断是否应跳过推送。

    逻辑：
    - 如果本次没有识别出交易日，则不做去重，继续发送
    - 如果识别出的最近交易日和上次已发送交易日相同，则跳过
    """
    if not signal_date:
        return False

    state = load_runtime_state()
    last_signal_date = state.get(market_key, {}).get("last_signal_date")
    return last_signal_date == signal_date


def mark_push_sent(market_key: str, signal_date: Optional[str]) -> None:
    """记录该市场最近一次已发送的交易日。"""
    if not signal_date:
        return
    state = load_runtime_state()
    state[market_key] = {"last_signal_date": signal_date}
    save_runtime_state(state)


def chunked(items: Sequence[str], size: int) -> Iterator[List[str]]:
    """按固定大小切分列表，便于批量请求。"""
    for index in range(0, len(items), size):
        yield list(items[index:index + size])


def safe_to_float(value: object) -> Optional[float]:
    """稳健转 float，失败返回 None。"""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_finviz_market_cap(value: object) -> Optional[float]:
    """
    将 Finviz 返回的市值文本转换成数值。

    典型格式：
    - 3.12T
    - 825.40B
    - 12.6M
    """
    if value is None:
        return None
    text = str(value).strip().upper().replace(",", "")
    if not text or text == "-":
        return None

    multiplier_map = {
        "T": 1_000_000_000_000,
        "B": 1_000_000_000,
        "M": 1_000_000,
        "K": 1_000,
    }

    suffix = text[-1]
    if suffix in multiplier_map:
        return float(text[:-1]) * multiplier_map[suffix]
    return safe_to_float(text)


@contextmanager
def akshare_no_proxy() -> Iterator[None]:
    """
    临时清空代理环境变量。

    当前机器上 Eastmoney / AkShare 请求会继承系统代理，
    如果代理不可用，A 股和港股接口会直接报 ProxyError。
    因此这里在 AkShare 请求期间暂时移除代理变量，结束后恢复。
    """
    proxy_keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]
    original_env = {key: os.environ.get(key) for key in proxy_keys}
    try:
        for key in proxy_keys:
            os.environ.pop(key, None)
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def markdown_v2_escape(text: str) -> str:
    """转义 Telegram MarkdownV2 特殊字符。"""
    special_chars = r"_*[]()~`>#+-=|{}.!"
    escaped = []
    for char in text:
        if char in special_chars:
            escaped.append("\\" + char)
        else:
            escaped.append(char)
    return "".join(escaped)


def send_telegram_message(
    message: str,
    bot_token: str,
    chat_id: str,
    parse_mode: str = "MarkdownV2",
) -> None:
    """发送 Telegram 消息。未配置该市场凭证时仅记录日志。"""
    if not bot_token or not chat_id:
        logging.warning("未配置目标市场的 Telegram BOT_TOKEN 或 CHAT_ID，本次仅记录日志，不发送消息。")
        return

    url = TELEGRAM_API_URL.format(token=bot_token)
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            logging.error("Telegram 返回失败: %s", data)
    except Exception:
        logging.exception("Telegram 推送失败")


def normalize_kline_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    将不同数据源返回的列统一重命名为策略计算需要的标准列。

    统一后的标准列：
    - open
    - high
    - low
    - close

    AkShare 的历史 K 线通常返回中文列名：
    - 开盘 -> open
    - 最高 -> high
    - 最低 -> low
    - 收盘 -> close

    yfinance 返回英文大写列名：
    - Open -> open
    - High -> high
    - Low -> low
    - Close -> close

    这里统一重命名后，后续的技术指标计算函数就不需要关心来源市场，
    从而实现“美股 / A 股 / 港股共用同一套指标计算逻辑”。
    """
    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
    }

    normalized_df = df.copy()
    normalized_df = normalized_df.rename(columns=rename_map)

    required_columns = ["open", "high", "low", "close"]
    missing_columns = [column for column in required_columns if column not in normalized_df.columns]
    if missing_columns:
        raise ValueError(f"K 线数据缺少必要列: {missing_columns}")

    normalized_df = normalized_df[required_columns].apply(pd.to_numeric, errors="coerce")
    normalized_df = normalized_df.dropna(subset=required_columns).reset_index(drop=True)
    return normalized_df


def calculate_cci_bottom_signal(kline_df: pd.DataFrame) -> bool:
    """
    计算自定义 CCI 抄底信号。

    计算顺序严格按需求：

    1. PRICE1：
       如果 (high > close 且 close > open)
       或 (high > open 且 open >= close)
       则 PRICE1 = close，否则 PRICE1 = high

    2. PRICE2：
       如果 (low < close 且 close < open)
       或 (low < open 且 open <= close)
       则 PRICE2 = close，否则 PRICE2 = low

    3. TYP：
       TYP = PRICE1 + (PRICE2 / 3)

    4. CCI：
       CCI = (TYP - MA(TYP, 36)) / (0.015 * AVEDEV(TYP, 36))

    5. CCI1：
       CCI1 = EMA(CCI, 4)

    6. 触发条件：
       昨日 CCI1 <= -150 且 今日 CCI1 > -150
    """
    if kline_df is None or len(kline_df) < MIN_KLINE_ROWS:
        return False

    df = normalize_kline_columns(kline_df)
    if len(df) < MIN_KLINE_ROWS:
        return False

    open_series = df["open"]
    high_series = df["high"]
    low_series = df["low"]
    close_series = df["close"]

    # PRICE1：对于上影线/阳线组合，优先取 close；否则取 high。
    price1_condition = ((high_series > close_series) & (close_series > open_series)) | (
        (high_series > open_series) & (open_series >= close_series)
    )
    df["PRICE1"] = np.where(price1_condition, close_series, high_series)

    # PRICE2：对于下影线/阴线组合，优先取 close；否则取 low。
    price2_condition = ((low_series < close_series) & (close_series < open_series)) | (
        (low_series < open_series) & (open_series <= close_series)
    )
    df["PRICE2"] = np.where(price2_condition, close_series, low_series)

    # TYP 是本策略定义的自定义价格，不是传统 CCI 的 TP。
    df["TYP"] = df["PRICE1"] + (df["PRICE2"] / 3.0)

    typ_ma = df["TYP"].rolling(CCI_PERIOD).mean()

    # 使用 rolling.apply + 平均绝对离差，兼容新版 pandas。
    typ_avedev = df["TYP"].rolling(CCI_PERIOD).apply(
        lambda values: np.mean(np.abs(values - np.mean(values))),
        raw=True,
    )

    denominator = 0.015 * typ_avedev.replace(0, np.nan)
    df["CCI"] = (df["TYP"] - typ_ma) / denominator

    # CCI1 是 CCI 的 4 周期 EMA 平滑结果。
    df["CCI1"] = df["CCI"].ewm(span=CCI_EMA_SPAN, adjust=False).mean()

    latest_two = df["CCI1"].dropna().tail(2)
    if len(latest_two) < 2:
        return False

    yesterday_cci1 = latest_two.iloc[0]
    today_cci1 = latest_two.iloc[1]

    # 免费数据源在阈值附近容易出现轻微偏差，这里增加一个很小的确认缓冲，
    # 避免仅比 -150 高出零点几的边缘值被误判为有效上穿。
    threshold_today = CCI_CROSS_LEVEL + CCI_CONFIRMATION_BUFFER
    return yesterday_cci1 <= CCI_CROSS_LEVEL and today_cci1 > threshold_today


def format_earnings_date(value: object) -> Optional[str]:
    """将日期对象或字符串统一格式化为 YYYY/MM/DD。"""
    if value is None:
        return None

    if isinstance(value, list) and value:
        value = value[0]

    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp) and parsed.tzinfo is not None:
            parsed = parsed.tz_convert(TIMEZONE)
        return parsed.strftime("%Y/%m/%d")
    except Exception:
        return None


def extract_latest_trade_date(kline_df: pd.DataFrame) -> Optional[str]:
    """
    从 K 线 DataFrame 中尽量提取最近交易日。

    兼容来源：
    - yfinance: Date / Datetime 列或 DatetimeIndex
    - akshare: 日期 / time_key 列
    """
    if kline_df is None or kline_df.empty:
        return None

    candidate_series = None
    for column in ["Date", "Datetime", "日期", "time_key"]:
        if column in kline_df.columns:
            candidate_series = pd.to_datetime(kline_df[column], errors="coerce")
            break

    if candidate_series is None:
        index = kline_df.index
        if isinstance(index, pd.DatetimeIndex):
            candidate_series = pd.Series(index)

    if candidate_series is None:
        return None

    candidate_series = candidate_series.dropna()
    if candidate_series.empty:
        return None
    return candidate_series.iloc[-1].strftime("%Y/%m/%d")


def normalize_cn_code(value: object) -> str:
    """统一 A 股代码到 6 位数字。"""
    text = str(value).strip().lower()
    digits = "".join(char for char in text if char.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def normalize_hk_code(value: object) -> str:
    """统一港股代码到 5 位数字。"""
    text = str(value).strip().lower()
    digits = "".join(char for char in text if char.isdigit())
    return digits[-5:].zfill(5) if digits else ""


def format_cn_yfinance_ticker(code: str) -> str:
    """将 A 股 6 位代码转换成 yfinance 可识别的 ticker。"""
    normalized = normalize_cn_code(code)
    if normalized.startswith(("600", "601", "603", "605", "688", "689", "510", "511", "512", "513", "515", "518", "588")):
        return f"{normalized}.SS"
    return f"{normalized}.SZ"


def format_hk_yfinance_ticker(code: str) -> str:
    """将港股 5 位代码转换成 yfinance 可识别的 4 位/更多位 ticker。"""
    return f"{int(code):04d}.HK"


def fetch_us_universe() -> pd.DataFrame:
    """
    使用 Finviz 获取总市值大于 100 亿美元的美股列表。

    Finviz Screener 会直接完成第一层市值过滤，因此后续 yfinance
    只需要处理大市值股票，能显著减少请求量。
    """
    logging.info("开始通过 Finviz 获取美股大市值股票池。")
    overview = Overview()
    overview.set_filter(filters_dict={"Market Cap.": "+Large (over $10bln)"})
    df = overview.screener_view(order="Market Cap.", limit=5000, verbose=0, ascend=False)
    if df is None or df.empty:
        return pd.DataFrame(columns=["code", "name", "market_cap"])

    result = pd.DataFrame(
        {
            "code": df["Ticker"].astype(str).str.upper(),
            "name": df["Company"].astype(str),
            "market_cap": df["Market Cap"].apply(parse_finviz_market_cap),
        }
    )
    result = result.dropna(subset=["market_cap"])
    result = result[result["market_cap"] >= US_MIN_MARKET_CAP].reset_index(drop=True)
    logging.info("美股大市值股票池数量: %s", len(result))
    return result


def extract_us_histories(codes: Sequence[str]) -> Dict[str, pd.DataFrame]:
    """
    使用 yfinance 批量下载历史日 K 线。

    返回格式：
    {
        "AAPL": DataFrame,
        "MSFT": DataFrame,
    }
    """
    histories: Dict[str, pd.DataFrame] = {}
    for batch_codes in chunked(list(codes), US_HISTORY_BATCH_SIZE):
        logging.info("开始批量拉取美股历史 K 线，批次数量: %s", len(batch_codes))
        try:
            raw_df = yf.download(
                tickers=batch_codes,
                period="4mo",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=True,
                timeout=20,
            )
        except Exception:
            logging.exception("批量拉取美股历史 K 线失败，本批次跳过。")
            time.sleep(US_HISTORY_SLEEP_SECONDS)
            continue

        if raw_df is None or raw_df.empty:
            time.sleep(US_HISTORY_SLEEP_SECONDS)
            continue

        # 多股票下载时返回 MultiIndex 列；单股票时返回普通列。
        if isinstance(raw_df.columns, pd.MultiIndex):
            for code in batch_codes:
                try:
                    code_df = raw_df[code].copy()
                    code_df = code_df.reset_index(drop=False)
                    histories[code] = code_df
                except Exception:
                    continue
        else:
            code = batch_codes[0]
            histories[code] = raw_df.copy().reset_index(drop=False)

        time.sleep(US_HISTORY_SLEEP_SECONDS)
    return histories


def extract_yfinance_histories(symbol_map: Dict[str, str], batch_size: int = ASIA_HISTORY_BATCH_SIZE) -> Dict[str, pd.DataFrame]:
    """
    批量拉取 A 股/港股历史 K 线。

    `symbol_map` 的 key 是业务侧代码，value 是 yfinance ticker。
    返回结果仍然使用业务侧代码做 key，便于后续和股票池直接关联。
    """
    histories: Dict[str, pd.DataFrame] = {}
    code_items = list(symbol_map.items())
    for batch_items in chunked(code_items, batch_size):
        batch_symbol_map = dict(batch_items)
        batch_tickers = list(batch_symbol_map.values())
        logging.info("开始批量拉取亚洲市场历史 K 线，批次数量: %s", len(batch_tickers))
        try:
            raw_df = yf.download(
                tickers=batch_tickers,
                period="4mo",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=True,
                timeout=20,
            )
        except Exception:
            logging.exception("批量拉取亚洲市场历史 K 线失败，本批次稍后走逐只回退。")
            time.sleep(US_HISTORY_SLEEP_SECONDS)
            continue

        if raw_df is None or raw_df.empty:
            time.sleep(US_HISTORY_SLEEP_SECONDS)
            continue

        if isinstance(raw_df.columns, pd.MultiIndex):
            for code, ticker in batch_items:
                try:
                    ticker_df = raw_df[ticker].copy().reset_index(drop=False)
                    if ticker_df.empty:
                        continue
                    histories[code] = ticker_df
                except Exception:
                    continue
        else:
            code, _ticker = batch_items[0]
            histories[code] = raw_df.copy().reset_index(drop=False)

        time.sleep(US_HISTORY_SLEEP_SECONDS)
    return histories


def fetch_us_earnings_date(code: str) -> Optional[str]:
    """
    使用 yfinance 获取下一次财报日期。

    优先顺序：
    1. earnings_dates 中第一个未来日期
    2. calendar 中的 Earnings Date
    """
    try:
        ticker = yf.Ticker(code)

        earnings_dates = getattr(ticker, "earnings_dates", None)
        if isinstance(earnings_dates, pd.DataFrame) and not earnings_dates.empty:
            future_index = [
                index for index in earnings_dates.index
                if pd.to_datetime(index, errors="coerce") >= pd.Timestamp.now(tz="UTC")
            ]
            if future_index:
                return format_earnings_date(future_index[0])

        calendar = ticker.calendar
        if isinstance(calendar, dict):
            return format_earnings_date(calendar.get("Earnings Date"))
        if isinstance(calendar, pd.DataFrame) and not calendar.empty:
            for column in calendar.columns:
                if "earnings" in str(column).lower():
                    return format_earnings_date(calendar.iloc[0][column])
    except Exception:
        logging.exception("获取美股 %s 财报日期失败。", code)
    return None


def scan_us_market() -> tuple[List[Dict[str, object]], Optional[str]]:
    """执行美股扫描，并返回最近交易日。"""
    universe_df = fetch_us_universe()
    if universe_df.empty:
        return [], None

    histories = extract_us_histories(universe_df["code"].tolist())
    winners: List[Dict[str, object]] = []
    latest_trade_date: Optional[str] = None

    for _, row in universe_df.iterrows():
        code = row["code"]
        history_df = histories.get(code)
        if history_df is None or history_df.empty:
            continue

        try:
            trade_date = extract_latest_trade_date(history_df)
            if trade_date and (latest_trade_date is None or trade_date > latest_trade_date):
                latest_trade_date = trade_date
            if calculate_cci_bottom_signal(history_df):
                winners.append(
                    {
                        "code": code,
                        "name": row["name"],
                        "market_cap": row["market_cap"],
                        "earnings_time": None,
                    }
                )
        except Exception:
            logging.exception("计算美股 %s 技术信号失败。", code)

    # 仅对最终命中的股票补抓财报时间，减少 yfinance 慢调用。
    for item in winners:
        item["earnings_time"] = fetch_us_earnings_date(str(item["code"]))
        time.sleep(US_EARNINGS_SLEEP_SECONDS)

    winners.sort(key=lambda item: float(item["market_cap"]), reverse=True)
    logging.info("美股最终命中数量: %s", len(winners))
    return winners, latest_trade_date


def get_cn_disclosure_period_candidates() -> List[str]:
    """
    根据当前日期给出 A 股披露预约表的候选期次。

    AkShare 的披露接口按“某一期财报”查询，因此这里给出一个
    较实用的候选列表，逐个尝试；只要命中一份可用表即可。
    """
    now = datetime.now(TIMEZONE)
    year = now.year
    return [
        f"{year - 1}年报",
        f"{year}一季报",
        f"{year}中报",
        f"{year}三季报",
        f"{year}年报",
    ]


def fetch_a_share_earnings_map() -> Dict[str, str]:
    """
    获取 A 股财报预约披露时间映射。

    返回：
    {
        "000001": "2026/03/21",
        ...
    }
    """
    earnings_map: Dict[str, str] = {}
    for period in get_cn_disclosure_period_candidates():
        try:
            with akshare_no_proxy():
                disclosure_df = ak.stock_report_disclosure(market="沪深京", period=period)
            if disclosure_df is None or disclosure_df.empty:
                continue

            logging.info("A 股披露预约表获取成功，期次: %s", period)
            for _, row in disclosure_df.iterrows():
                code = str(row.get("股票代码", "")).zfill(6)
                for column in ["实际披露", "三次变更", "二次变更", "初次变更", "首次预约"]:
                    formatted = format_earnings_date(row.get(column))
                    if formatted:
                        earnings_map[code] = formatted
                        break
            if earnings_map:
                return earnings_map
        except Exception:
            logging.exception("获取 A 股披露预约表失败，期次: %s", period)
    return earnings_map


def fetch_cn_spot_df() -> pd.DataFrame:
    """获取 A 股实时行情并筛选总市值大于 100 亿人民币。"""
    with akshare_no_proxy():
        spot_df = ak.stock_zh_a_spot_em()

    result = pd.DataFrame(
        {
            "code": spot_df["代码"].astype(str).str.zfill(6),
            "name": spot_df["名称"].astype(str),
            "market_cap": pd.to_numeric(spot_df["总市值"], errors="coerce"),
        }
    )
    result = result.dropna(subset=["market_cap"])
    result = result[result["market_cap"] >= CN_MIN_MARKET_CAP].reset_index(drop=True)
    logging.info("A 股大市值股票池数量: %s", len(result))
    return result


def fetch_cn_market_cap_from_individual_info(code: str) -> Optional[float]:
    """用东方财富单股资料接口补充 A 股总市值。"""
    cache_key = f"cn:{code}"
    cached_value = get_cached_market_cap(cache_key)
    if cached_value is not None:
        return cached_value

    try:
        with akshare_no_proxy():
            info_df = ak.stock_individual_info_em(symbol=code, timeout=15)
        if info_df is None or info_df.empty:
            return None
        mapping = dict(zip(info_df["item"], info_df["value"]))
        market_cap = safe_to_float(mapping.get("总市值"))
        if market_cap is not None:
            set_cached_market_cap(cache_key, market_cap)
        return market_cap
    except Exception:
        logging.exception("获取 A 股 %s 单股市值失败。", code)
        return None


def fetch_cn_market_cap_from_yfinance(code: str) -> Optional[float]:
    """用 yfinance 补 A 股总市值。"""
    cache_key = f"cn:{code}"
    cached_value = get_cached_market_cap(cache_key)
    if cached_value is not None:
        return cached_value

    try:
        ticker = yf.Ticker(format_cn_yfinance_ticker(code))
        fast_info = ticker.fast_info
        market_cap = safe_to_float(fast_info.get("marketCap"))
        if market_cap is None:
            shares = safe_to_float(fast_info.get("shares"))
            last_price = safe_to_float(fast_info.get("lastPrice"))
            if shares is not None and last_price is not None:
                market_cap = shares * last_price
        if market_cap is not None:
            set_cached_market_cap(cache_key, market_cap)
        return market_cap
    except Exception:
        logging.exception("通过 yfinance 获取 A 股 %s 市值失败。", code)
        return None


def fetch_cn_spot_df_fallback() -> pd.DataFrame:
    """
    A 股股票池备用源。

    主源 Eastmoney 全量现货接口不稳定时：
    1. 使用新浪现货列表拿到全部股票及成交额；
    2. 按成交额排序，优先处理更活跃的股票；
    3. 使用单股资料接口逐只补总市值；
    4. 结合本地缓存，避免每天都全量重查。
    """
    logging.info("A 股启用备用股票池：新浪现货 + yfinance/单股资料补市值。")
    with akshare_no_proxy():
        spot_df = ak.stock_zh_a_spot()

    fallback_df = pd.DataFrame(
        {
            "code": spot_df["代码"].apply(normalize_cn_code),
            "name": spot_df["名称"].astype(str),
            "turnover": pd.to_numeric(spot_df["成交额"], errors="coerce"),
        }
    )
    fallback_df = fallback_df.dropna(subset=["turnover"])
    fallback_df = fallback_df[fallback_df["code"].str.len() == 6]
    fallback_df = fallback_df.sort_values("turnover", ascending=False)

    cache = load_market_cap_cache()
    cached_large_codes = [
        key.split(":", 1)[1]
        for key, item in cache.items()
        if key.startswith("cn:") and safe_to_float(item.get("market_cap")) and safe_to_float(item.get("market_cap")) >= CN_MIN_MARKET_CAP
    ]

    candidate_codes = list(dict.fromkeys(
        fallback_df.head(CN_FALLBACK_TURNOVER_LIMIT)["code"].tolist() + cached_large_codes
    ))
    base_info_map = fallback_df.drop_duplicates(subset=["code"]).set_index("code")["name"].to_dict()

    rows: List[Dict[str, object]] = []
    total = len(candidate_codes)
    for index, code in enumerate(candidate_codes, start=1):
        market_cap = fetch_cn_market_cap_from_yfinance(code)
        if market_cap is None:
            market_cap = fetch_cn_market_cap_from_individual_info(code)
        if market_cap is not None and market_cap >= CN_MIN_MARKET_CAP:
            rows.append(
                {
                    "code": code,
                    "name": base_info_map.get(code, code),
                    "market_cap": market_cap,
                }
            )
        if index % 100 == 0:
            logging.info("A 股备用股票池补市值进度: %s/%s", index, total)
        time.sleep(0.08)

    result = pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)
    logging.info("A 股备用股票池筛出数量: %s", len(result))
    return result


def fetch_hk_spot_df() -> pd.DataFrame:
    """获取港股实时行情并筛选总市值大于 100 亿港币。"""
    with akshare_no_proxy():
        spot_df = ak.stock_hk_spot_em()

    market_cap_column = None
    for candidate in ["总市值", "市值", "总市值-港币"]:
        if candidate in spot_df.columns:
            market_cap_column = candidate
            break
    if market_cap_column is None:
        raise ValueError(f"港股实时行情中未找到市值列，现有列: {spot_df.columns.tolist()}")

    result = pd.DataFrame(
        {
            "code": spot_df["代码"].astype(str).str.extract(r"(\d+)")[0].str.zfill(5),
            "name": spot_df["名称"].astype(str),
            "market_cap": pd.to_numeric(spot_df[market_cap_column], errors="coerce"),
        }
    )
    result = result.dropna(subset=["market_cap"])
    result = result[result["market_cap"] >= HK_MIN_MARKET_CAP].reset_index(drop=True)
    logging.info("港股大市值股票池数量: %s", len(result))
    return result


def fetch_hk_market_cap_from_yfinance(code: str) -> Optional[float]:
    """
    用 yfinance 补港股总市值。

    对港股 ticker，直接读取 marketCap 字段不稳定，因此改用：
    market_cap = shares * lastPrice
    """
    cache_key = f"hk:{code}"
    cached_value = get_cached_market_cap(cache_key)
    if cached_value is not None:
        return cached_value

    try:
        ticker = yf.Ticker(format_hk_yfinance_ticker(code))
        fast_info = ticker.fast_info
        shares = safe_to_float(fast_info.get("shares"))
        last_price = safe_to_float(fast_info.get("lastPrice"))
        if shares is None or last_price is None:
            return None
        market_cap = shares * last_price
        set_cached_market_cap(cache_key, market_cap)
        return market_cap
    except Exception:
        logging.exception("通过 yfinance 获取港股 %s 市值失败。", code)
        return None


def fetch_hk_spot_df_fallback() -> pd.DataFrame:
    """
    港股股票池备用源。

    主源 Eastmoney 失败时：
    1. 使用新浪港股现货列表；
    2. 按成交额排序优先处理更活跃股票；
    3. 使用 yfinance fast_info 的 shares * lastPrice 补总市值；
    4. 本地缓存命中的大市值股票会优先保留。
    """
    logging.info("港股启用备用股票池：新浪现货 + yfinance 补市值。")
    with akshare_no_proxy():
        spot_df = ak.stock_hk_spot()

    fallback_df = pd.DataFrame(
        {
            "code": spot_df["代码"].apply(normalize_hk_code),
            "name": spot_df["中文名称"].astype(str),
            "turnover": pd.to_numeric(spot_df["成交额"], errors="coerce"),
        }
    )
    fallback_df = fallback_df.dropna(subset=["turnover"])
    fallback_df = fallback_df[fallback_df["code"].str.len() == 5]
    fallback_df = fallback_df.sort_values("turnover", ascending=False)

    cache = load_market_cap_cache()
    cached_large_codes = [
        key.split(":", 1)[1]
        for key, item in cache.items()
        if key.startswith("hk:") and safe_to_float(item.get("market_cap")) and safe_to_float(item.get("market_cap")) >= HK_MIN_MARKET_CAP
    ]

    candidate_codes = list(dict.fromkeys(
        fallback_df.head(HK_FALLBACK_TURNOVER_LIMIT)["code"].tolist() + cached_large_codes
    ))
    base_info_map = fallback_df.drop_duplicates(subset=["code"]).set_index("code")["name"].to_dict()

    rows: List[Dict[str, object]] = []
    total = len(candidate_codes)
    for index, code in enumerate(candidate_codes, start=1):
        market_cap = fetch_hk_market_cap_from_yfinance(code)
        if market_cap is not None and market_cap >= HK_MIN_MARKET_CAP:
            rows.append(
                {
                    "code": code,
                    "name": base_info_map.get(code, code),
                    "market_cap": market_cap,
                }
            )
        if index % 100 == 0:
            logging.info("港股备用股票池补市值进度: %s/%s", index, total)
        time.sleep(0.08)

    result = pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)
    logging.info("港股备用股票池筛出数量: %s", len(result))
    return result


def fetch_cn_history(code: str) -> pd.DataFrame:
    """拉取单只 A 股前复权日 K。"""
    end_date = datetime.now(TIMEZONE).strftime("%Y%m%d")
    start_date = (datetime.now(TIMEZONE) - timedelta(days=120)).strftime("%Y%m%d")
    try:
        with akshare_no_proxy():
            return ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
                timeout=AKSHARE_SPOT_TIMEOUT_SECONDS,
            )
    except Exception:
        ticker = yf.Ticker(format_cn_yfinance_ticker(code))
        return ticker.history(period="4mo", interval="1d", auto_adjust=False).reset_index(drop=False)


def fetch_hk_history(code: str) -> pd.DataFrame:
    """拉取单只港股日 K，优先尝试前复权，失败则回退到不复权。"""
    end_date = datetime.now(TIMEZONE).strftime("%Y%m%d")
    start_date = (datetime.now(TIMEZONE) - timedelta(days=120)).strftime("%Y%m%d")
    with akshare_no_proxy():
        try:
            return ak.stock_hk_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
        except Exception:
            try:
                return ak.stock_hk_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
            except Exception:
                ticker = yf.Ticker(format_hk_yfinance_ticker(code))
                return ticker.history(period="4mo", interval="1d", auto_adjust=False).reset_index(drop=False)


def scan_cn_market() -> tuple[List[Dict[str, object]], Optional[str]]:
    """执行 A 股扫描，并返回最近交易日。"""
    try:
        spot_df = fetch_cn_spot_df()
    except Exception:
        logging.exception("获取 A 股实时行情失败，尝试启用备用股票池。")
        try:
            spot_df = fetch_cn_spot_df_fallback()
        except Exception:
            logging.exception("A 股备用股票池也失败，本次 A 股扫描返回空结果。")
            return [], None

    earnings_map = fetch_a_share_earnings_map()
    winners: List[Dict[str, object]] = []
    latest_trade_date: Optional[str] = None
    histories = extract_yfinance_histories(
        {str(code): format_cn_yfinance_ticker(str(code)) for code in spot_df["code"].tolist()}
    )

    for _, row in spot_df.iterrows():
        code = str(row["code"])
        used_fallback_fetch = False
        try:
            hist_df = histories.get(code)
            if hist_df is None or hist_df.empty:
                used_fallback_fetch = True
                hist_df = fetch_cn_history(code)
            trade_date = extract_latest_trade_date(hist_df)
            if trade_date and (latest_trade_date is None or trade_date > latest_trade_date):
                latest_trade_date = trade_date
            if calculate_cci_bottom_signal(hist_df):
                winners.append(
                    {
                        "code": code,
                        "name": row["name"],
                        "market_cap": row["market_cap"],
                        "earnings_time": earnings_map.get(code),
                    }
                )
        except Exception:
            logging.exception("处理 A 股 %s 失败，跳过。", code)
        finally:
            if used_fallback_fetch:
                time.sleep(ASIA_HISTORY_SLEEP_SECONDS)

    winners.sort(key=lambda item: float(item["market_cap"]), reverse=True)
    logging.info("A 股最终命中数量: %s", len(winners))
    return winners, latest_trade_date


def scan_hk_market() -> tuple[List[Dict[str, object]], Optional[str]]:
    """执行港股扫描，并返回最近交易日。财报时间优先返回 None。"""
    try:
        spot_df = fetch_hk_spot_df()
    except Exception:
        logging.exception("获取港股实时行情失败，尝试启用备用股票池。")
        try:
            spot_df = fetch_hk_spot_df_fallback()
        except Exception:
            logging.exception("港股备用股票池也失败，本次港股扫描返回空结果。")
            return [], None

    winners: List[Dict[str, object]] = []
    latest_trade_date: Optional[str] = None
    histories = extract_yfinance_histories(
        {str(code): format_hk_yfinance_ticker(str(code)) for code in spot_df["code"].tolist()}
    )

    for _, row in spot_df.iterrows():
        code = str(row["code"])
        used_fallback_fetch = False
        try:
            hist_df = histories.get(code)
            if hist_df is None or hist_df.empty:
                used_fallback_fetch = True
                hist_df = fetch_hk_history(code)
            trade_date = extract_latest_trade_date(hist_df)
            if trade_date and (latest_trade_date is None or trade_date > latest_trade_date):
                latest_trade_date = trade_date
            if calculate_cci_bottom_signal(hist_df):
                winners.append(
                    {
                        "code": code,
                        "name": row["name"],
                        "market_cap": row["market_cap"],
                        "earnings_time": None,
                    }
                )
        except Exception:
            logging.exception("处理港股 %s 失败，跳过。", code)
        finally:
            if used_fallback_fetch:
                time.sleep(ASIA_HISTORY_SLEEP_SECONDS)

    winners.sort(key=lambda item: float(item["market_cap"]), reverse=True)
    logging.info("港股最终命中数量: %s", len(winners))
    return winners, latest_trade_date


def build_market_message(market_name: str, results: Sequence[Dict[str, object]]) -> str:
    """组装 Telegram MarkdownV2 消息。"""
    date_text = markdown_v2_escape(datetime.now(TIMEZONE).strftime("%Y-%m-%d"))
    title = f"*{date_text} {markdown_v2_escape(market_name)} 抄底信号*"

    if not results:
        return f"{title}\n今日 {markdown_v2_escape(market_name)} 无符合抄底条件的股票"

    lines = [
        title,
        f"筛选数量：*{len(results)}*",
        "",
    ]

    for index, item in enumerate(results, start=1):
        code = markdown_v2_escape(str(item["code"]))
        name = markdown_v2_escape(str(item["name"]))
        earnings_time = item.get("earnings_time")
        if earnings_time:
            earnings_text = markdown_v2_escape(str(earnings_time))
            lines.append(f"{index}\\. {code} \\({name}\\) \\- 财报发布时间：{earnings_text}")
        else:
            lines.append(f"{index}\\. {code} \\({name}\\)")

    return "\n".join(lines)


def run_us_job(force_push: bool = False) -> List[Dict[str, object]]:
    """美股任务：北京时间周二到周六 08:00。"""
    logging.info("开始执行美股免费数据扫描任务。")
    results, signal_date = scan_us_market()
    if not force_push and should_skip_push("us", signal_date):
        logging.info("美股最近交易日 %s 已推送过，本次跳过。", signal_date)
        return results
    send_telegram_message(
        build_market_message("美股", results),
        bot_token=US_TELEGRAM_BOT_TOKEN,
        chat_id=US_TELEGRAM_CHAT_ID,
    )
    mark_push_sent("us", signal_date)
    return results


def run_cn_job(force_push: bool = False) -> List[Dict[str, object]]:
    """A 股任务：北京时间周一到周五 20:00。"""
    logging.info("开始执行 A股 免费数据扫描任务。")
    results, signal_date = scan_cn_market()
    if not force_push and should_skip_push("cn", signal_date):
        logging.info("A股最近交易日 %s 已推送过，本次跳过。", signal_date)
        return results
    send_telegram_message(
        build_market_message("A股", results),
        bot_token=CN_TELEGRAM_BOT_TOKEN,
        chat_id=CN_TELEGRAM_CHAT_ID,
    )
    mark_push_sent("cn", signal_date)
    flush_market_cap_cache()
    return results


def run_hk_job(force_push: bool = False) -> List[Dict[str, object]]:
    """港股任务：北京时间周一到周五 20:00。"""
    logging.info("开始执行 港股 免费数据扫描任务。")
    results, signal_date = scan_hk_market()
    if not force_push and should_skip_push("hk", signal_date):
        logging.info("港股最近交易日 %s 已推送过，本次跳过。", signal_date)
        return results
    send_telegram_message(
        build_market_message("港股", results),
        bot_token=HK_TELEGRAM_BOT_TOKEN,
        chat_id=HK_TELEGRAM_CHAT_ID,
    )
    mark_push_sent("hk", signal_date)
    flush_market_cap_cache()
    return results


def run_manual_once(target: str) -> None:
    """手动执行一次，便于本地调试。"""
    target = target.lower()
    if target == "us":
        run_us_job(force_push=True)
        return
    if target == "asia":
        run_cn_job(force_push=True)
        run_hk_job(force_push=True)
        return
    if target == "all":
        run_us_job(force_push=True)
        run_cn_job(force_push=True)
        run_hk_job(force_push=True)
        return
    raise ValueError(f"不支持的运行目标: {target}")


def build_scheduler() -> BlockingScheduler:
    """创建北京时间调度器。"""
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        run_us_job,
        trigger=CronTrigger(hour=8, minute=0, day_of_week="tue-sat", timezone=TIMEZONE),
        id="us_job",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        run_cn_job,
        trigger=CronTrigger(hour=20, minute=0, day_of_week="mon-fri", timezone=TIMEZONE),
        id="cn_job",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        run_hk_job,
        trigger=CronTrigger(hour=20, minute=0, day_of_week="mon-fri", timezone=TIMEZONE),
        id="hk_job",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=1800,
    )
    return scheduler


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="免费开源数据版量化选股与 Telegram 推送脚本")
    parser.add_argument(
        "--run-once",
        choices=["us", "asia", "all"],
        help="手动立即运行一次选股逻辑，不进入阻塞调度模式。",
    )
    return parser.parse_args()


def main() -> None:
    """程序入口。"""
    setup_logging()
    ensure_single_instance()
    args = parse_args()

    if args.run_once:
        logging.info("手动运行模式: %s", args.run_once)
        run_manual_once(args.run_once)
        return

    scheduler = build_scheduler()
    logging.info("调度器已启动：美股 周二到周六 08:00；A股 周一到周五 20:00；港股 周一到周五 20:00。")
    scheduler.start()


if __name__ == "__main__":
    main()
