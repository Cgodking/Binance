# Binance Futures Shadow Research Bot

Single-file Python application for Binance USD-M Futures market monitoring, deterministic shadow trading, historical replay, and walk-forward strategy validation.

## Safety status

- `DRY_RUN=true` is mandatory.
- `SHADOW_MODE=true` is mandatory.
- `LIVE_TRADING=false` is mandatory.
- The current engine rejects configurations that attempt to enable live trading.
- Runtime state, API credentials, logs, historical data, and research reports are intentionally excluded from Git.

The current cost-aware trend-pullback candidate did not pass its training gate. The repository must not be treated as evidence of a profitable strategy or as approval for live trading.

## Environment

- Python 3.10+
- Linux recommended for server deployment

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
```

Set a strong `WEB_PASSWORD` or `WEB_AUTH_TOKEN` before exposing the Flask dashboard. Keep Binance API credentials out of Git.

## Run

```bash
python3 binance_futures_zscore_bot.py
```

The dashboard listens on port `5055` by default.

## Tests

```bash
python3 -m unittest -v test_binance_futures_zscore_bot.py
```

## Historical research

Regime and strategy replay:

```bash
python3 binance_futures_zscore_bot.py replay --days 180
```

Frozen cost-aware trend-pullback candidate with independent LONG and SHORT validation:

```bash
python3 binance_futures_zscore_bot.py candidate-replay --days 360
```

Historical replay uses public market data and writes generated artifacts to ignored local directories.

## Server helpers

`install_binancep_commands.sh` installs the interactive configuration and process helper commands used by the server deployment. Review it before sourcing or installing it on another host.
