#!/usr/bin/env bash
set -euo pipefail

BASHRC="/root/.bashrc"
BACKUP="/root/.bashrc.bak.binancep.$(date +%Y%m%d%H%M%S)"

cp "$BASHRC" "$BACKUP"

python3 - <<'PY'
from pathlib import Path

path = Path("/root/.bashrc")
text = path.read_text(encoding="utf-8")
start = "# binance-zscore-bot-commands"
end = "# binance-zscore-bot-commands-end"

block = r'''
# binance-zscore-bot-commands
binanceconfig() {
  local app_dir="/root/Binance"
  local env_file="$app_dir/.env"
  mkdir -p "$app_dir"

  echo "Configure Binance Z-score bot."
  echo "Secrets are written to $env_file with chmod 600 and are not printed."

  local api_key api_secret testnet dry_run live_trading web_host web_port web_username web_password symbol
  read -r -p "BINANCE_API_KEY: " api_key
  read -r -s -p "BINANCE_API_SECRET: " api_secret
  echo
  read -r -p "SYMBOL [BTCUSDT]: " symbol
  symbol="${symbol:-BTCUSDT}"
  read -r -p "TESTNET [true]: " testnet
  testnet="${testnet:-true}"
  read -r -p "DRY_RUN [true]: " dry_run
  dry_run="${dry_run:-true}"
  read -r -p "LIVE_TRADING [false]: " live_trading
  live_trading="${live_trading:-false}"
  read -r -p "WEB_HOST for phone access [0.0.0.0]: " web_host
  web_host="${web_host:-0.0.0.0}"
  read -r -p "WEB_PORT [5055]: " web_port
  web_port="${web_port:-5055}"
  read -r -p "WEB_USERNAME [admin]: " web_username
  web_username="${web_username:-admin}"
  read -r -s -p "WEB_PASSWORD (required for phone access, min 8 chars): " web_password
  echo

  if [ ${#web_password} -lt 8 ]; then
    echo "binanceconfig: WEB_PASSWORD must be at least 8 characters long." >&2
    return 1
  fi

  BINANCE_CONFIG_FILE="$env_file" \
  BINANCE_CONFIG_API_KEY="$api_key" \
  BINANCE_CONFIG_API_SECRET="$api_secret" \
  BINANCE_CONFIG_SYMBOL="$symbol" \
  BINANCE_CONFIG_TESTNET="$testnet" \
  BINANCE_CONFIG_DRY_RUN="$dry_run" \
  BINANCE_CONFIG_LIVE_TRADING="$live_trading" \
  BINANCE_CONFIG_WEB_HOST="$web_host" \
  BINANCE_CONFIG_WEB_PORT="$web_port" \
  BINANCE_CONFIG_WEB_USERNAME="$web_username" \
  BINANCE_CONFIG_WEB_PASSWORD="$web_password" \
  python3 - <<'BINANCE_CONFIG_PY'
import os
import shlex
from pathlib import Path

env_file = Path(os.environ["BINANCE_CONFIG_FILE"])
values = {
    "BINANCE_API_KEY": os.environ.get("BINANCE_CONFIG_API_KEY", ""),
    "BINANCE_API_SECRET": os.environ.get("BINANCE_CONFIG_API_SECRET", ""),
    "SYMBOL": os.environ.get("BINANCE_CONFIG_SYMBOL", "BTCUSDT").upper(),
    "INTERVAL": "1m",
    "LOOKBACK": "50",
    "UPPER_Z": "2.0",
    "LOWER_Z": "-2.0",
    "EXIT_Z": "0.3",
    "STOP_LOSS_PCT": "0.005",
    "TAKE_PROFIT_PCT": "0.005",
    "LEVERAGE": "20",
    "MAX_MARGIN_USDT": "1.5",
    "MAX_NOTIONAL_USDT": "30.0",
    "LOOP_SLEEP_SECONDS": "5",
    "TESTNET": os.environ.get("BINANCE_CONFIG_TESTNET", "true").lower(),
    "DRY_RUN": os.environ.get("BINANCE_CONFIG_DRY_RUN", "true").lower(),
    "LIVE_TRADING": os.environ.get("BINANCE_CONFIG_LIVE_TRADING", "false").lower(),
    "WEB_HOST": os.environ.get("BINANCE_CONFIG_WEB_HOST", "0.0.0.0"),
    "WEB_PORT": os.environ.get("BINANCE_CONFIG_WEB_PORT", "5055"),
    "WEB_USERNAME": os.environ.get("BINANCE_CONFIG_WEB_USERNAME", "admin"),
    "WEB_PASSWORD": os.environ.get("BINANCE_CONFIG_WEB_PASSWORD", ""),
}

content = "\n".join(f"{key}={shlex.quote(str(value))}" for key, value in values.items()) + "\n"
env_file.write_text(content, encoding="utf-8")
env_file.chmod(0o600)
BINANCE_CONFIG_PY

  unset api_key api_secret web_password
  echo "binanceconfig: wrote $env_file"
  echo "binanceconfig: restart with: binancestop && binancep"
}

binancep() {
  bian
  local app_dir="/root/Binance"
  local log_file="${BINANCE_BOT_LOG:-/root/Binance/bot.log}"
  local pid_file="${BINANCE_BOT_PID:-/root/Binance/bot.pid}"

  if [ ! -f "$app_dir/binance_futures_zscore_bot.py" ]; then
    echo "binancep: missing $app_dir/binance_futures_zscore_bot.py" >&2
    return 1
  fi

  mkdir -p "$app_dir"
  if [ -f "$app_dir/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$app_dir/.env"
    set +a
  fi

  local host="${WEB_HOST:-127.0.0.1}"
  local port="${WEB_PORT:-5055}"
  local auth_args=()
  if [ -n "${WEB_PASSWORD:-}" ]; then
    auth_args=(-u "${WEB_USERNAME:-admin}:${WEB_PASSWORD}")
  fi

  if pgrep -f "$app_dir/binance_futures_zscore_bot.py" >/dev/null 2>&1; then
    echo "binancep: bot is already running"
  else
    cd "$app_dir" || return 1
    WEB_HOST="$host" WEB_PORT="$port" nohup python3 "$app_dir/binance_futures_zscore_bot.py" >> "$log_file" 2>&1 &
    echo $! > "$pid_file"
    echo "binancep: started bot pid $(cat "$pid_file")"
  fi

  sleep 2
  if curl --noproxy 127.0.0.1,localhost -fsS --max-time 5 "${auth_args[@]}" "http://127.0.0.1:${port}/api/status" >/dev/null 2>&1; then
    echo "binancep: web monitor is ready at http://${host}:${port}"
  else
    echo "binancep: bot started, but web monitor is not ready yet; check $log_file"
  fi
}

binancestop() {
  local app_dir="/root/Binance"
  local pid_file="${BINANCE_BOT_PID:-/root/Binance/bot.pid}"
  local pid=""

  if [ -f "$pid_file" ]; then
    pid="$(cat "$pid_file")"
  fi

  if [ -n "$pid" ] && kill "$pid" >/dev/null 2>&1; then
    sleep 3
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
      echo "binancestop: force stopped pid $pid"
    else
      echo "binancestop: stopped pid $pid"
    fi
    rm -f "$pid_file"
    return 0
  fi

  local matches
  matches="$(pgrep -f "$app_dir/binance_futures_zscore_bot.py" || true)"
  if [ -n "$matches" ]; then
    pkill -TERM -f "$app_dir/binance_futures_zscore_bot.py" >/dev/null 2>&1 || true
    sleep 3
    pkill -KILL -f "$app_dir/binance_futures_zscore_bot.py" >/dev/null 2>&1 || true
    echo "binancestop: stopped matching bot process"
  else
    echo "binancestop: no bot process found"
  fi
}

binancelog() {
  local log_file="${BINANCE_BOT_LOG:-/root/Binance/bot.log}"
  touch "$log_file"
  tail -f "$log_file"
}
# binance-zscore-bot-commands-end
'''

if start in text and end in text:
    before, rest = text.split(start, 1)
    _, after = rest.split(end, 1)
    text = before.rstrip() + "\n" + block.strip() + "\n" + after.lstrip()
else:
    text = text.rstrip() + "\n\n" + block.strip() + "\n"

path.write_text(text, encoding="utf-8")
PY

echo "installed binancep commands"
echo "backup: $BACKUP"
