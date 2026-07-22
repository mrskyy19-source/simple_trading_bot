# Enterprise Intelligent Trading Bot v3.0

An async, enterprise-grade trading bot for Linux/Kali that connects to MetaTrader 5 (MT5) via a Windows bridge, with structured logging, local persistence, and configurable broker settings.

## Architecture

MT5 is Windows-only, so this bot runs on Linux and talks to a **Windows bridge** process that keeps MT5 open and logged in, exposing a local REST API the bot consumes over the network.

```
[Linux/Kali: trading_bot_intelligent.py]  <--HTTP-->  [Windows bridge + MT5, logged in]
```

## Requirements

- A Windows machine/VM running the MT5 terminal, **open and logged in**, with the bridge service running
- Python 3 on the Linux/Kali side with:
  - `aiohttp`, `certifi` — async HTTP + TLS
  - `numpy` — numerical/indicator calculations
  - `sqlite3` (stdlib) — local trade/state persistence
- A `.env` file (Linux side) pointing at the bridge:
  ```
  BROKER_API_URL=http://172.24.48.1:8000
  BROKER_API_KEY=your_key_here
  ACCOUNT_ID=your_account_id
  ```

## Configuration

Settings are loaded via environment variables into a `BotConfig` dataclass, including:

| Variable | Purpose | Default |
|---|---|---|
| `BROKER` | Broker/execution mode | `rest` |
| `BROKER_API_KEY` | API key for the bridge | `ignored` |
| `ACCOUNT_ID` | Trading account identifier | *(empty)* |
| `BROKER_API_URL` | Windows bridge address | `http://172.24.48.1:8000` |
| `USE_DEMO` | Toggle demo/live trading | — |

## Features

- Fully async (`asyncio` + `aiohttp`) execution loop
- Rotating file logs (`RotatingFileHandler`) for durable, size-capped logging
- Local SQLite persistence for trade/session state
- SSL/TLS-secured bridge communication
- Graceful shutdown via signal handling

## Setup

```bash
pip install aiohttp certifi numpy
```

Create `.env` on the Linux/Kali side with your `BROKER_API_URL` and credentials, confirm the Windows bridge + MT5 are running and logged in, then:

```bash
python trading_bot_intelligent.py
```

## ⚠️ Risk Disclaimer

Automated trading carries substantial financial risk, including the potential loss of capital. This bot is provided for research, development, and personal use. Always test thoroughly on a demo account before connecting to a live trading account, and never trade with funds you cannot afford to lose.
