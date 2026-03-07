# Quant Bot Free

基于免费开源数据源的自动股票筛选与 Telegram 推送脚本。

当前实现：
- 美股：`finvizfinance + yfinance`
- A股：`akshare + yfinance` 回退
- 港股：`akshare + yfinance` 回退
- 推送：Telegram Bot API
- 调度：APScheduler

## 主要文件

- `quant_bot_free.py`：主脚本
- `requirements.txt`：依赖列表
- `start_quant_bot_free.bat`：Windows 启动脚本
- `.env.example`：环境变量模板

## 环境准备

1. 安装 Python 3.8+
2. 安装依赖

```powershell
pip install -r requirements.txt
```

3. 复制环境变量模板并填写真实值

```powershell
copy .env.example .env
```

## 需要配置的环境变量

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `US_TELEGRAM_BOT_TOKEN`
- `US_TELEGRAM_CHAT_ID`
- `CN_TELEGRAM_BOT_TOKEN`
- `CN_TELEGRAM_CHAT_ID`
- `HK_TELEGRAM_BOT_TOKEN`
- `HK_TELEGRAM_CHAT_ID`

说明：
- 如果三路市场使用同一个机器人，可以只填默认的 `TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID`
- 如果三路市场分别使用不同机器人，则分别填写 `US/CN/HK` 对应变量

## 手动运行

只跑美股：

```powershell
python quant_bot_free.py --run-once us
```

只跑 A股和港股：

```powershell
python quant_bot_free.py --run-once asia
```

全跑一遍：

```powershell
python quant_bot_free.py --run-once all
```

## 常驻运行

```powershell
python quant_bot_free.py
```

当前调度规则：
- 美股：北京时间周二到周六 `08:00`
- A股：北京时间周一到周五 `20:00`
- 港股：北京时间周一到周五 `20:00`

## Windows 登录后自动启动

项目内提供了：

- `start_quant_bot_free.bat`

你可以把它放到当前用户的 Windows 启动文件夹，实现“登录后自动启动”。

## 注意事项

- 不要把 `.env` 上传到 Git 仓库
- Telegram token 已暴露过时，应先去 BotFather 重置
- 免费数据源偶尔会波动，脚本已尽量做回退与异常跳过，但不能保证 100% 稳定
