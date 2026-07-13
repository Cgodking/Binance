import csv
import argparse
import hashlib
import io
import json
import logging
import math
import os
import signal
import sys
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    from binance.client import Client
except ImportError:
    Client = None


LOGGER = logging.getLogger("binance_futures_zscore_bot")
STOP_REQUESTED = False
DEFAULT_API_TIMEOUT_SECONDS = 10
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BASE_SLEEP_SECONDS = 1.0
DEFAULT_HISTORY_SIZE = 500
CANDLE_SETTLEMENT_SECONDS = 2.0
HISTORICAL_KLINE_COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "close_time"]
REPLAY_TRAIN_SHARE = 0.50
REPLAY_VALIDATION_SHARE = 0.25
REPLAY_MIN_TRAIN_PAIR_TRADES = 20
REPLAY_MIN_VALIDATION_PAIR_TRADES = 10
REPLAY_MIN_TEST_TRADES = 100
REPLAY_MIN_NET_PROFIT_FACTOR = 1.10
REPLAY_MAX_DRAWDOWN_USDT = 0.20
CANDIDATE_STRATEGY_NAME = "COST_AWARE_STABLE_TREND_PULLBACK"
CANDIDATE_DIRECTIONS = ("LONG", "SHORT")
ELITE_STRATEGY_NAME = "ELITE_LONG_STRATEGY"
SHADOW_PROBE_STRATEGY_NAME = "SHADOW_PROBE_STRATEGY"
ROUTER_NAME = "REGIME_STRATEGY_ROUTER"
MEAN_REVERSION_STRATEGY_NAME = "MEAN_REVERSION_STRATEGY"
TREND_FOLLOW_LONG_STRATEGY_NAME = "TREND_FOLLOW_LONG_STRATEGY"
TREND_FOLLOW_SHORT_STRATEGY_NAME = "TREND_FOLLOW_SHORT_STRATEGY"
BREAKOUT_STRATEGY_NAME = "BREAKOUT_STRATEGY"
NO_TRADE_STRATEGY_NAME = "NO_TRADE"
BASELINE_LOGGING_ONLY = "BASELINE_LOGGING_ONLY"
EXECUTABLE_STRATEGIES = (
    MEAN_REVERSION_STRATEGY_NAME,
    TREND_FOLLOW_LONG_STRATEGY_NAME,
    TREND_FOLLOW_SHORT_STRATEGY_NAME,
)
ACTIVE_STRATEGY = ROUTER_NAME
ACTIVE_STRATEGIES = EXECUTABLE_STRATEGIES + (BASELINE_LOGGING_ONLY,)
LOGGING_ONLY_STRATEGIES = (
    ELITE_STRATEGY_NAME,
    SHADOW_PROBE_STRATEGY_NAME,
    BREAKOUT_STRATEGY_NAME,
    BASELINE_LOGGING_ONLY,
)
NON_ACTIVE_STRATEGY_BLOCKED = "NON_ACTIVE_STRATEGY_BLOCKED"
NON_ROUTER_EXECUTION_BLOCKED = "NON_ROUTER_EXECUTION_BLOCKED"
MISSING_REGIME_TAG_BLOCKED = "MISSING_REGIME_TAG_BLOCKED"
INVALID_CONTAMINATED_DATASET = "INVALID_CONTAMINATED_DATASET"
ACTIVE_EXECUTION_SOURCE = "regime_strategy_router"
ELITE_SIGNAL_ORIGIN = "elite_signal"
ROUTER_SIGNAL_ORIGIN = "regime_router_signal"

SIGNAL_FIELDNAMES = [
    "timestamp",
    "strategy",
    "router_strategy",
    "regime",
    "current_regime",
    "symbol",
    "close_price",
    "previous_close",
    "zscore",
    "previous_zscore",
    "atr_pct",
    "atr_bucket",
    "trend_bucket",
    "trend_slope_pct",
    "ema20",
    "ema50",
    "ema_slope_pct",
    "ema_state",
    "atr_percentile",
    "price_distance_ema_pct",
    "price_distance_vwap_pct",
    "reversion_status",
    "regime_stability_score",
    "stability_state",
    "regime_persistence_candles",
    "regime_switch_rate",
    "atr_coefficient_of_variation",
    "ema_slope_consistency",
    "time_window_id",
    "window_trade_count",
    "window_expectancy",
    "window_profit_factor",
    "window_is_profitable",
    "trade_allowed_by_market_gate",
    "market_gate_enforced",
    "market_gate_rejection_reason",
    "trade_decision",
    "entry_reason",
    "no_trade_reason",
    "rejection_reason",
    "failed_filter",
    "direction",
    "probe_trade_decision",
    "probe_rejection_reason",
]

TRADE_FIELDNAMES = [
    "strategy_id",
    "strategy_name",
    "execution_source",
    "signal_origin",
    "regime_at_entry",
    "entry_time",
    "entry_price",
    "exit_time",
    "exit_price",
    "entry_zscore",
    "exit_zscore",
    "entry_atr_pct",
    "entry_atr_percentile",
    "entry_atr_bucket",
    "entry_trend_bucket",
    "entry_ema20",
    "entry_ema50",
    "entry_ema_state",
    "entry_reversion_confirmed",
    "entry_reason",
    "side",
    "quantity",
    "entry_notional",
    "exit_notional",
    "gross_pnl",
    "fees",
    "slippage",
    "net_pnl",
    "holding_seconds",
    "exit_reason",
    "result",
    "strategy",
    "symbol",
]

CANDIDATE_TRADE_FIELDNAMES = [
    "strategy_id",
    "side",
    "signal_time",
    "entry_time",
    "entry_price",
    "exit_time",
    "exit_price",
    "quantity",
    "entry_notional",
    "exit_notional",
    "stop_price",
    "take_profit_price",
    "entry_ema20",
    "entry_ema50",
    "entry_ema_spread_pct",
    "entry_ema_slope_consistency",
    "entry_trend_persistence",
    "entry_atr_15m",
    "entry_atr_1m_pct",
    "entry_atr_1m_percentile",
    "pullback_distance_atr",
    "gross_pnl",
    "fees",
    "slippage",
    "net_pnl",
    "mfe_pct",
    "mae_pct",
    "mfe_usdt",
    "mae_usdt",
    "time_to_mfe_seconds",
    "time_to_mae_seconds",
    "mfe_capture_ratio",
    "holding_seconds",
    "exit_reason",
    "result",
    "symbol",
]


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    api_key: str = ""
    api_secret: str = ""
    symbol: str = "SOLUSDT"
    interval: str = "1m"
    lookback: int = 50
    min_entry_abs_z: float = 2.05
    max_entry_abs_z: float = 2.60
    lower_z: float = -2.05
    upper_z: float = 2.05
    exit_z: float = 0.10
    min_z_reversion_delta: float = 0.15
    elite_allowed_atr_buckets: Tuple[str, ...] = ("medium",)
    elite_allowed_trend_buckets: Tuple[str, ...] = ("medium",)
    stop_loss_pct: float = 0.0038
    take_profit_pct: float = 0.006
    leverage: int = 10
    max_margin_usdt: float = 1.0
    max_notional_usdt: float = 10.0
    loop_sleep_seconds: float = 15.0
    dry_run: bool = True
    testnet: bool = True
    live_trading: bool = False
    shadow_mode: bool = True
    taker_fee_rate: float = 0.0005
    shadow_slippage_bps: float = 2.0
    bot_state_file: str = "bot_state.json"
    shadow_state_file: str = "shadow_state.json"
    shadow_signal_log_file: str = "shadow_signals.jsonl"
    shadow_trade_log_file: str = "shadow_trades.jsonl"
    exchange_filters_cache_file: str = "exchange_filters_cache.json"
    exchange_filters_cache_ttl_seconds: float = 900.0
    decision_report_every_completed_trades: int = 50
    trend_interval: str = "15m"
    trend_ema_period: int = 20
    trend_slope_lookback: int = 3
    trend_slope_threshold_pct: float = 0.002
    atr_lookback: int = 14
    low_atr_percentile: float = 0.40
    max_atr_pct: float = 0.008
    regime_fast_ema_period: int = 20
    regime_slow_ema_period: int = 50
    regime_slope_lookback: int = 5
    regime_high_atr_percentile: float = 0.70
    regime_low_atr_percentile: float = 0.30
    regime_mean_reversion_max_slope_pct: float = 0.0015
    regime_mean_reversion_max_distance_pct: float = 0.006
    regime_oscillation_lookback: int = 24
    regime_min_ema_crosses: int = 2
    breakout_lookback: int = 20
    web_host: str = "0.0.0.0"
    web_port: int = 5055
    web_auth_enabled: bool = True
    web_username: str = "admin"
    web_password: str = ""
    web_auth_token: str = ""
    active_strategies: Tuple[str, ...] = ACTIVE_STRATEGIES
    api_timeout_seconds: int = DEFAULT_API_TIMEOUT_SECONDS
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    shadow_probe_enabled: bool = True
    shadow_probe_min_abs_z: float = 0.40
    shadow_probe_max_abs_z: float = 4.00
    shadow_probe_allow_short: bool = True
    shadow_probe_max_holding_minutes: int = 120
    market_gate_enforced: bool = False
    regime_stability_window: int = 20
    min_regime_persistence_candles: int = 10
    max_regime_switch_rate: float = 0.25
    max_atr_coefficient_of_variation: float = 0.35
    min_ema_slope_consistency: float = 0.70
    time_window_hours: int = 2
    min_trades_per_window: int = 20
    min_profit_window_profit_factor: float = 1.05


@dataclass(frozen=True)
class CandidateReplayParameters:
    strategy_name: str = CANDIDATE_STRATEGY_NAME
    trend_fast_ema_period: int = 20
    trend_slow_ema_period: int = 50
    trend_atr_period: int = 14
    trend_persistence_bars: int = 4
    slope_consistency_window: int = 5
    min_slope_consistency: float = 0.80
    min_ema_spread_pct: float = 0.0020
    atr_1m_period: int = 14
    atr_percentile_window: int = 240
    max_atr_percentile: float = 0.70
    max_pullback_distance_atr: float = 0.35
    stop_loss_pct: float = 0.0055
    take_profit_pct: float = 0.0100
    max_holding_minutes: int = 240
    cooldown_minutes: int = 15
    fixed_notional_usdt: float = 10.0
    maximum_tp_cost_ratio: float = 0.15
    minimum_train_trades: int = 100
    minimum_out_of_sample_trades: int = 40
    minimum_net_profit_factor: float = 1.15
    maximum_drawdown_usdt: float = 0.20
    maximum_cost_to_gross_profit_ratio: float = 0.30


@dataclass(frozen=True)
class ExchangeFilters:
    step_size: float
    tick_size: float
    min_qty: float
    max_qty: float
    min_notional: float
    quantity_precision: int
    price_precision: int


@dataclass(frozen=True)
class ZScoreResult:
    latest_close: float
    previous_close: float
    mean: float
    std: float
    zscore: float
    previous_zscore: float
    latest_closed_time: str


@dataclass(frozen=True)
class MarketRegimeSnapshot:
    timestamp: str
    regime: str
    router_strategy: str
    no_trade_reason: str
    ema20: float
    ema50: float
    ema_slope_pct: float
    atr_pct: float
    atr_percentile: float
    atr_bucket: str
    price_distance_ema_pct: float
    price_distance_vwap_pct: float
    ema_state: str
    trend_bucket: str = "unknown"


@dataclass(frozen=True)
class EliteSignalDecision:
    timestamp: str
    close_price: float
    previous_close: float
    zscore: float
    previous_zscore: float
    atr_pct: float
    atr_bucket: str
    trend_bucket: str
    trend_slope_pct: float
    direction: str
    trade_decision: str
    rejection_reason: str
    failed_filter: str
    reversion_confirmed: bool

    def to_signal_row(self, config: Config) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "strategy": ELITE_STRATEGY_NAME,
            "symbol": config.symbol,
            "close_price": self.close_price,
            "previous_close": self.previous_close,
            "zscore": self.zscore,
            "previous_zscore": self.previous_zscore,
            "atr_pct": self.atr_pct,
            "atr_bucket": self.atr_bucket,
            "trend_bucket": self.trend_bucket,
            "trend_slope_pct": self.trend_slope_pct,
            "reversion_status": "CONFIRMED" if self.reversion_confirmed else "NOT_CONFIRMED",
            "trade_decision": self.trade_decision,
            "rejection_reason": self.rejection_reason,
            "failed_filter": self.failed_filter,
            "direction": self.direction,
        }


@dataclass(frozen=True)
class StrategySignalDecision:
    timestamp: str
    strategy: str
    regime: str
    router_strategy: str
    close_price: float
    previous_close: float
    zscore: float
    previous_zscore: float
    atr_pct: float
    atr_percentile: float
    atr_bucket: str
    trend_bucket: str
    trend_slope_pct: float
    ema20: float
    ema50: float
    ema_slope_pct: float
    ema_state: str
    price_distance_ema_pct: float
    price_distance_vwap_pct: float
    direction: str
    trade_decision: str
    entry_reason: str
    rejection_reason: str
    failed_filter: str
    reversion_confirmed: bool
    no_trade_reason: str = ""
    market_gate: Dict[str, Any] = field(default_factory=dict)

    def to_signal_row(self, config: Config) -> Dict[str, Any]:
        row = {
            "timestamp": self.timestamp,
            "strategy": self.strategy,
            "router_strategy": self.router_strategy,
            "regime": self.regime,
            "current_regime": self.regime,
            "symbol": config.symbol,
            "close_price": self.close_price,
            "previous_close": self.previous_close,
            "zscore": self.zscore,
            "previous_zscore": self.previous_zscore,
            "atr_pct": self.atr_pct,
            "atr_bucket": self.atr_bucket,
            "trend_bucket": self.trend_bucket,
            "trend_slope_pct": self.trend_slope_pct,
            "ema20": self.ema20,
            "ema50": self.ema50,
            "ema_slope_pct": self.ema_slope_pct,
            "ema_state": self.ema_state,
            "atr_percentile": self.atr_percentile,
            "price_distance_ema_pct": self.price_distance_ema_pct,
            "price_distance_vwap_pct": self.price_distance_vwap_pct,
            "reversion_status": "CONFIRMED" if self.reversion_confirmed else "NOT_CONFIRMED",
            "trade_decision": self.trade_decision,
            "entry_reason": self.entry_reason,
            "no_trade_reason": self.no_trade_reason,
            "rejection_reason": self.rejection_reason,
            "failed_filter": self.failed_filter,
            "direction": self.direction,
        }
        row.update(default_market_gate_snapshot(config, self.timestamp))
        row.update(self.market_gate)
        return row


@dataclass
class RuntimeState:
    state_file: str = field(default_factory=lambda: os.getenv("BOT_STATE_FILE", "bot_state.json"))
    persist_events: bool = True
    status: str = "starting"
    paused: bool = False
    last_error: str = ""
    started_at: str = field(default_factory=lambda: utc_now_text())
    last_update: str = ""
    balance: float = 0.0
    latest_close: Optional[float] = None
    latest_zscore: Optional[float] = None
    latest_closed_candle_time: str = ""
    atr_pct: Optional[float] = None
    atr_bucket: str = ""
    trend_bucket: str = ""
    trend_slope_pct: Optional[float] = None
    leverage: int = 10
    margin_type: str = "ISOLATED"
    config_snapshot: Dict[str, Any] = field(default_factory=dict)
    orders: List[Dict[str, Any]] = field(default_factory=list)
    positions: List[Dict[str, Any]] = field(default_factory=list)
    zscore_history: List[Dict[str, Any]] = field(default_factory=list)
    close_history: List[Dict[str, Any]] = field(default_factory=list)
    shadow: Dict[str, Any] = field(default_factory=dict)
    lock: threading.RLock = field(default_factory=threading.RLock)

    def __post_init__(self) -> None:
        if not self.shadow:
            self.shadow = default_shadow_state()
        if self.persist_events:
            self._load_state()

    def _load_state(self) -> None:
        path = Path(self.state_file)
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            LOGGER.warning("Failed to load runtime state file %s: %s", path, exc)
            return
        if payload.get("engine") != "elite_shadow_v1":
            return
        with self.lock:
            self.shadow = payload.get("shadow") or default_shadow_state()
            ensure_shadow_schema(self.shadow)
            self.orders = list(payload.get("orders") or [])[:50]
            self.positions = list(payload.get("positions") or [])

    def _save_state(self) -> None:
        if not self.persist_events:
            return
        path = Path(self.state_file)
        payload = {
            "engine": "elite_shadow_v1",
            "saved_at": utc_now_text(),
            "orders": self.orders[:50],
            "positions": self.positions,
            "shadow": self.shadow,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_suffix(path.suffix + ".tmp")
            temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, path)
        except Exception as exc:
            LOGGER.error("Failed to save runtime state file %s: %s", path, exc)

    def update_config(self, config: Config) -> None:
        with self.lock:
            self.config_snapshot = safe_config_dict(config)
            self.leverage = config.leverage
            self.shadow["enabled"] = config.shadow_mode
            self.shadow["active_strategies"] = list(ACTIVE_STRATEGIES)
            self.shadow["active_execution_strategy"] = ACTIVE_STRATEGY
            self.shadow["market_gate_enforced"] = config.market_gate_enforced

    def set_status(self, status: str) -> None:
        with self.lock:
            self.status = status
            self.last_update = utc_now_text()

    def set_error(self, message: str) -> None:
        with self.lock:
            self.status = "error"
            self.last_error = str(message)
            self.last_update = utc_now_text()

    def clear_error(self) -> None:
        with self.lock:
            self.last_error = ""
            self.last_update = utc_now_text()

    def set_paused(self, paused: bool) -> None:
        with self.lock:
            self.paused = bool(paused)
            self.status = "paused" if paused else "running"
            self.last_update = utc_now_text()

    def is_paused(self) -> bool:
        with self.lock:
            return self.paused

    def update_market_snapshot(
        self,
        config: Config,
        balance: float,
        z_result: ZScoreResult,
        atr_pct: float,
        atr_bucket_value: str,
        trend_bucket_value: str,
        trend_slope_pct: float,
    ) -> None:
        with self.lock:
            self.balance = float(balance)
            self.latest_close = z_result.latest_close
            self.latest_zscore = z_result.zscore
            self.latest_closed_candle_time = z_result.latest_closed_time
            self.atr_pct = atr_pct
            self.atr_bucket = atr_bucket_value
            self.trend_bucket = trend_bucket_value
            self.trend_slope_pct = trend_slope_pct
            self.last_update = utc_now_text()
            self.close_history.append({"time": z_result.latest_closed_time, "value": z_result.latest_close})
            self.zscore_history.append({"time": z_result.latest_closed_time, "value": z_result.zscore})
            self.close_history = self.close_history[-DEFAULT_HISTORY_SIZE:]
            self.zscore_history = self.zscore_history[-DEFAULT_HISTORY_SIZE:]
            self.config_snapshot = safe_config_dict(config)

    def record_order(self, order: Dict[str, Any]) -> None:
        with self.lock:
            self.orders.insert(0, order)
            self.orders = self.orders[:50]

    def _sync_shadow_positions_locked(self, config: Config) -> None:
        positions: List[Dict[str, Any]] = []
        for key in ("router_position",):
            position = self.shadow.get(key)
            if not position:
                continue
            positions.append(
                {
                    "symbol": config.symbol,
                    "side": str(position.get("side", "LONG")).upper(),
                    "quantity": float(position.get("quantity", 0.0) or 0.0),
                    "entry_price": float(position.get("entry_price", 0.0) or 0.0),
                    "unrealized_pnl": 0.0,
                    "margin_type": "isolated",
                    "position_side": "BOTH",
                    "simulated": True,
                    "strategy": str(position.get("strategy", "")),
                    "regime_at_entry": str(position.get("regime_at_entry", "")),
                }
            )
        self.positions = positions
        self.shadow["open_position_count"] = len(positions)

    def process_router_signal(
        self,
        config: Config,
        decision: StrategySignalDecision,
        filters: ExchangeFilters,
    ) -> None:
        signal_row = decision.to_signal_row(config)
        signal_row["probe_trade_decision"] = "REJECT"
        signal_row["probe_rejection_reason"] = ""
        with self.lock:
            ensure_shadow_schema(self.shadow)
            if decision.timestamp and decision.timestamp == self.shadow.get("last_signal_timestamp"):
                return
            self.shadow["last_signal_timestamp"] = decision.timestamp
            self.shadow["candidate_signals"] += 1
            existing_position = self.shadow.get("router_position")
            existing_elite = self.shadow.get("elite_long_position")
            existing_probe = self.shadow.get("shadow_probe_position")

            if existing_elite:
                record_execution_rejection(
                    self.shadow,
                    str(existing_elite.get("strategy", ELITE_STRATEGY_NAME)),
                    NON_ACTIVE_STRATEGY_BLOCKED,
                    signal_origin="legacy_elite_long_position",
                )
                self.shadow["elite_long_position"] = None
                existing_elite = None

            if existing_probe:
                record_execution_rejection(
                    self.shadow,
                    SHADOW_PROBE_STRATEGY_NAME,
                    NON_ACTIVE_STRATEGY_BLOCKED,
                    signal_origin="legacy_shadow_probe_position",
                )
                self.shadow["shadow_probe_position"] = None
                existing_probe = None

            if existing_position:
                exit_reason = get_router_exit_reason(existing_position, decision, config)
                if exit_reason:
                    trade = close_router_shadow_position(existing_position, decision, exit_reason, config)
                    self._record_trade_locked(config, trade)
                    self.shadow["router_position"] = None
                    if self.persist_events:
                        maybe_write_decision_report(self.shadow, config)
                elif decision.trade_decision == "ENTER":
                    signal_row["trade_decision"] = "REJECT"
                    signal_row["failed_filter"] = "position"
                    signal_row["rejection_reason"] = "router_position is already open"

            has_open_position = bool(self.shadow.get("router_position"))

            if not has_open_position and signal_row["trade_decision"] == "ENTER":
                if config.market_gate_enforced and not bool(signal_row.get("trade_allowed_by_market_gate", False)):
                    signal_row["trade_decision"] = "REJECT"
                    signal_row["failed_filter"] = "market_gate"
                    signal_row["rejection_reason"] = str(
                        signal_row.get("market_gate_rejection_reason") or "market gate blocked entry"
                    )
                elif decision.strategy not in EXECUTABLE_STRATEGIES:
                    record_execution_rejection(
                        self.shadow,
                        decision.strategy,
                        NON_ACTIVE_STRATEGY_BLOCKED,
                        execution_source=ACTIVE_EXECUTION_SOURCE,
                        signal_origin=ROUTER_SIGNAL_ORIGIN,
                    )
                    signal_row["trade_decision"] = "REJECT"
                    signal_row["failed_filter"] = "execution"
                    signal_row["rejection_reason"] = (
                        f"{decision.strategy} is logging-only and cannot open shadow positions"
                    )
                else:
                    plan = calculate_order_quantity(
                        balance=self.balance,
                        price=decision.close_price,
                        filters=filters,
                        config=config,
                    )
                    if plan <= 0 or not validate_order(plan, decision.close_price, filters, config):
                        signal_row["trade_decision"] = "REJECT"
                        signal_row["failed_filter"] = "order_size"
                        signal_row["rejection_reason"] = "shadow order size does not satisfy exchange filters"
                    else:
                        position = open_router_shadow_position(config, decision, plan)
                        self.shadow["router_position"] = position
                        has_open_position = True

            if not has_open_position:
                probe_allowed, probe_reason = shadow_probe_router_log_reason(config, decision)
                if probe_allowed:
                    signal_row["probe_trade_decision"] = "LOG_ONLY"
                signal_row["probe_rejection_reason"] = probe_reason
            else:
                signal_row["probe_rejection_reason"] = "router_position is already open"

            self._sync_shadow_positions_locked(config)
            self.shadow["current_regime"] = decision.regime
            self.shadow["router_active_strategy"] = decision.strategy
            self.shadow["current_regime_stability_score"] = signal_row.get("regime_stability_score")
            self.shadow["current_stability_state"] = signal_row.get("stability_state")
            self.shadow["current_time_window_id"] = signal_row.get("time_window_id")
            self.shadow["current_trade_allowed_by_market_gate"] = signal_row.get("trade_allowed_by_market_gate")
            self.shadow["current_market_gate_rejection_reason"] = signal_row.get("market_gate_rejection_reason")
            self.shadow["market_gate_enforced"] = config.market_gate_enforced
            self.shadow["latest_signal"] = dict(signal_row)
            self.shadow["last_update"] = utc_now_text()
            if self.persist_events:
                append_signal_logs(config, signal_row)
                save_shadow_state(config, self.shadow)
                self._save_state()

    def process_elite_signal(
        self,
        config: Config,
        decision: EliteSignalDecision,
        filters: ExchangeFilters,
    ) -> None:
        regime = MarketRegimeSnapshot(
            timestamp=decision.timestamp,
            regime="MEAN_REVERTING",
            router_strategy=MEAN_REVERSION_STRATEGY_NAME,
            no_trade_reason="",
            ema20=decision.close_price,
            ema50=decision.close_price,
            ema_slope_pct=decision.trend_slope_pct,
            atr_pct=decision.atr_pct,
            atr_percentile=0.0,
            atr_bucket=decision.atr_bucket,
            price_distance_ema_pct=0.0,
            price_distance_vwap_pct=0.0,
            ema_state="LEGACY_ADAPTER",
            trend_bucket=decision.trend_bucket,
        )
        router_decision = StrategySignalDecision(
            timestamp=decision.timestamp,
            strategy=MEAN_REVERSION_STRATEGY_NAME if decision.trade_decision == "ENTER" else NO_TRADE_STRATEGY_NAME,
            regime=regime.regime,
            router_strategy=regime.router_strategy,
            close_price=decision.close_price,
            previous_close=decision.previous_close,
            zscore=decision.zscore,
            previous_zscore=decision.previous_zscore,
            atr_pct=decision.atr_pct,
            atr_percentile=0.0,
            atr_bucket=decision.atr_bucket,
            trend_bucket=decision.trend_bucket,
            trend_slope_pct=decision.trend_slope_pct,
            ema20=decision.close_price,
            ema50=decision.close_price,
            ema_slope_pct=decision.trend_slope_pct,
            ema_state="LEGACY_ADAPTER",
            price_distance_ema_pct=0.0,
            price_distance_vwap_pct=0.0,
            direction=decision.direction,
            trade_decision=decision.trade_decision,
            entry_reason="legacy elite adapter",
            rejection_reason=decision.rejection_reason,
            failed_filter=decision.failed_filter,
            reversion_confirmed=decision.reversion_confirmed,
        )
        self.process_router_signal(config, router_decision, filters)

    def _record_trade_locked(self, config: Config, trade: Dict[str, Any]) -> bool:
        normalize_trade_attribution(trade)
        strategy_name = shadow_trade_strategy(trade)
        if strategy_name not in EXECUTABLE_STRATEGIES:
            record_execution_rejection(
                self.shadow,
                strategy_name,
                NON_ACTIVE_STRATEGY_BLOCKED,
                execution_source=str(trade.get("execution_source", "")),
                signal_origin=str(trade.get("signal_origin", "")),
            )
            LOGGER.warning("%s rejected_strategy=%s", NON_ACTIVE_STRATEGY_BLOCKED, strategy_name)
            return False
        if str(trade.get("execution_source", "")) != ACTIVE_EXECUTION_SOURCE:
            record_execution_rejection(
                self.shadow,
                strategy_name,
                NON_ROUTER_EXECUTION_BLOCKED,
                execution_source=str(trade.get("execution_source", "")),
                signal_origin=str(trade.get("signal_origin", "")),
            )
            LOGGER.warning("%s rejected_strategy=%s", NON_ROUTER_EXECUTION_BLOCKED, strategy_name)
            return False
        if not str(trade.get("regime_at_entry", "")).strip():
            record_execution_rejection(
                self.shadow,
                strategy_name,
                MISSING_REGIME_TAG_BLOCKED,
                execution_source=str(trade.get("execution_source", "")),
                signal_origin=str(trade.get("signal_origin", "")),
            )
            LOGGER.warning("%s rejected_strategy=%s", MISSING_REGIME_TAG_BLOCKED, strategy_name)
            return False
        self.shadow["completed_trades"] += 1
        self.shadow["trades"].append(trade)
        self.shadow["trades"] = self.shadow["trades"][-2000:]
        self.shadow["recent_trades"].insert(0, trade)
        self.shadow["recent_trades"] = self.shadow["recent_trades"][:20]
        recompute_shadow_metrics(self.shadow, config)
        if self.persist_events:
            append_trade_logs(config, trade)
        return True

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            shadow_copy = json.loads(json.dumps(self.shadow))
            ensure_shadow_schema(shadow_copy)
            return {
                "status": self.status,
                "paused": self.paused,
                "last_error": self.last_error,
                "started_at": self.started_at,
                "last_update": self.last_update,
                "balance": self.balance,
                "latest_close": self.latest_close,
                "latest_zscore": self.latest_zscore,
                "latest_closed_candle_time": self.latest_closed_candle_time,
                "atr_pct": self.atr_pct,
                "atr_bucket": self.atr_bucket,
                "trend_bucket": self.trend_bucket,
                "trend_slope_pct": self.trend_slope_pct,
                "leverage": self.leverage,
                "margin_type": self.margin_type,
                "config": dict(self.config_snapshot),
                "orders": list(self.orders),
                "positions": list(self.positions),
                "zscore_history": list(self.zscore_history),
                "close_history": list(self.close_history),
                "shadow": shadow_copy,
            }


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigError(f"{name} must be a boolean value")


def parse_int_env(name: str, default: int, minimum: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return value


def parse_float_env(name: str, default: float, minimum: Optional[float] = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return value


def parse_bucket_tuple_env(name: str, default: Tuple[str, ...], allowed: Sequence[str]) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    values = tuple(item.strip().lower() for item in raw.split(",") if item.strip())
    if not values:
        raise ConfigError(f"{name} must contain at least one bucket")
    invalid = [value for value in values if value not in allowed]
    if invalid:
        raise ConfigError(f"{name} contains unsupported buckets: {','.join(invalid)}")
    return values


def load_config() -> Config:
    if load_dotenv is not None:
        load_dotenv()

    config = Config(
        api_key=os.getenv("BINANCE_API_KEY", "").strip(),
        api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
        symbol=os.getenv("SYMBOL", "SOLUSDT").strip().upper() or "SOLUSDT",
        interval=os.getenv("INTERVAL", "1m").strip() or "1m",
        lookback=parse_int_env("LOOKBACK", 50, minimum=5),
        min_entry_abs_z=parse_float_env("MIN_ENTRY_ABS_Z", 2.05, minimum=0.0),
        max_entry_abs_z=parse_float_env("MAX_ENTRY_ABS_Z", 2.60, minimum=0.0),
        lower_z=parse_float_env("LOWER_Z", -2.05),
        upper_z=parse_float_env("UPPER_Z", 2.05),
        exit_z=parse_float_env("EXIT_Z", 0.10, minimum=0.0),
        min_z_reversion_delta=parse_float_env("MIN_Z_REVERSION_DELTA", 0.15, minimum=0.0),
        elite_allowed_atr_buckets=parse_bucket_tuple_env(
            "ELITE_ALLOWED_ATR_BUCKETS",
            ("medium",),
            ("low", "medium", "high", "unknown"),
        ),
        elite_allowed_trend_buckets=parse_bucket_tuple_env(
            "ELITE_ALLOWED_TREND_BUCKETS",
            ("medium",),
            ("weak", "medium", "strong", "unknown"),
        ),
        stop_loss_pct=parse_float_env("STOP_LOSS_PCT", 0.0038, minimum=0.0),
        take_profit_pct=parse_float_env("TAKE_PROFIT_PCT", 0.006, minimum=0.0),
        leverage=parse_int_env("LEVERAGE", 10, minimum=1),
        max_margin_usdt=parse_float_env("MAX_MARGIN_USDT", 1.0, minimum=0.0),
        max_notional_usdt=parse_float_env("MAX_NOTIONAL_USDT", 10.0, minimum=0.0),
        loop_sleep_seconds=parse_float_env("LOOP_SLEEP_SECONDS", 15.0, minimum=1.0),
        dry_run=parse_bool_env("DRY_RUN", True),
        testnet=parse_bool_env("TESTNET", True),
        live_trading=parse_bool_env("LIVE_TRADING", False),
        shadow_mode=parse_bool_env("SHADOW_MODE", True),
        taker_fee_rate=parse_float_env("TAKER_FEE_RATE", 0.0005, minimum=0.0),
        shadow_slippage_bps=parse_float_env("SHADOW_SLIPPAGE_BPS", 2.0, minimum=0.0),
        bot_state_file=os.getenv("BOT_STATE_FILE", "bot_state.json").strip() or "bot_state.json",
        shadow_state_file=os.getenv("SHADOW_STATE_FILE", "shadow_state.json").strip() or "shadow_state.json",
        shadow_signal_log_file=os.getenv("SHADOW_SIGNAL_LOG_FILE", "shadow_signals.jsonl").strip() or "shadow_signals.jsonl",
        shadow_trade_log_file=os.getenv("SHADOW_TRADE_LOG_FILE", "shadow_trades.jsonl").strip() or "shadow_trades.jsonl",
        exchange_filters_cache_file=os.getenv("EXCHANGE_FILTERS_CACHE_FILE", "exchange_filters_cache.json").strip() or "exchange_filters_cache.json",
        exchange_filters_cache_ttl_seconds=parse_float_env(
            "EXCHANGE_FILTERS_CACHE_TTL_SECONDS",
            900.0,
            minimum=1.0,
        ),
        decision_report_every_completed_trades=parse_int_env("DECISION_REPORT_EVERY_COMPLETED_TRADES", 50, minimum=1),
        trend_interval=os.getenv("TREND_INTERVAL", "15m").strip() or "15m",
        trend_ema_period=parse_int_env("TREND_EMA_PERIOD", 20, minimum=2),
        trend_slope_lookback=parse_int_env("TREND_SLOPE_LOOKBACK", 3, minimum=1),
        trend_slope_threshold_pct=parse_float_env("TREND_SLOPE_THRESHOLD_PCT", 0.002, minimum=0.0),
        atr_lookback=parse_int_env("ATR_LOOKBACK", 14, minimum=2),
        low_atr_percentile=parse_float_env("LOW_ATR_PERCENTILE", 0.40, minimum=0.0),
        max_atr_pct=parse_float_env("MAX_ATR_PCT", 0.008, minimum=0.0),
        regime_fast_ema_period=parse_int_env("REGIME_FAST_EMA_PERIOD", 20, minimum=2),
        regime_slow_ema_period=parse_int_env("REGIME_SLOW_EMA_PERIOD", 50, minimum=3),
        regime_slope_lookback=parse_int_env("REGIME_SLOPE_LOOKBACK", 5, minimum=1),
        regime_high_atr_percentile=parse_float_env("REGIME_HIGH_ATR_PERCENTILE", 0.70, minimum=0.0),
        regime_low_atr_percentile=parse_float_env("REGIME_LOW_ATR_PERCENTILE", 0.30, minimum=0.0),
        regime_mean_reversion_max_slope_pct=parse_float_env("REGIME_MEAN_REVERSION_MAX_SLOPE_PCT", 0.0015, minimum=0.0),
        regime_mean_reversion_max_distance_pct=parse_float_env("REGIME_MEAN_REVERSION_MAX_DISTANCE_PCT", 0.006, minimum=0.0),
        regime_oscillation_lookback=parse_int_env("REGIME_OSCILLATION_LOOKBACK", 24, minimum=3),
        regime_min_ema_crosses=parse_int_env("REGIME_MIN_EMA_CROSSES", 2, minimum=0),
        breakout_lookback=parse_int_env("BREAKOUT_LOOKBACK", 20, minimum=2),
        web_host=os.getenv("WEB_HOST", "0.0.0.0").strip() or "0.0.0.0",
        web_port=parse_int_env("WEB_PORT", 5055, minimum=1),
        web_auth_enabled=parse_bool_env("WEB_AUTH_ENABLED", True),
        web_username=os.getenv("WEB_USERNAME", "admin").strip() or "admin",
        web_password=os.getenv("WEB_PASSWORD", "").strip(),
        web_auth_token=os.getenv("WEB_AUTH_TOKEN", "").strip(),
        api_timeout_seconds=parse_int_env("API_TIMEOUT_SECONDS", DEFAULT_API_TIMEOUT_SECONDS, minimum=1),
        retry_attempts=parse_int_env("RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS, minimum=1),
        shadow_probe_enabled=parse_bool_env("SHADOW_PROBE_ENABLED", True),
        shadow_probe_min_abs_z=parse_float_env("SHADOW_PROBE_MIN_ABS_Z", 0.40, minimum=0.0),
        shadow_probe_max_abs_z=parse_float_env("SHADOW_PROBE_MAX_ABS_Z", 4.00, minimum=0.0),
        shadow_probe_allow_short=parse_bool_env("SHADOW_PROBE_ALLOW_SHORT", True),
        shadow_probe_max_holding_minutes=parse_int_env("SHADOW_PROBE_MAX_HOLDING_MINUTES", 120, minimum=1),
        market_gate_enforced=parse_bool_env("MARKET_GATE_ENFORCED", False),
        regime_stability_window=parse_int_env("REGIME_STABILITY_WINDOW", 20, minimum=5),
        min_regime_persistence_candles=parse_int_env("MIN_REGIME_PERSISTENCE_CANDLES", 10, minimum=1),
        max_regime_switch_rate=parse_float_env("MAX_REGIME_SWITCH_RATE", 0.25, minimum=0.0),
        max_atr_coefficient_of_variation=parse_float_env("MAX_ATR_COEFFICIENT_OF_VARIATION", 0.35, minimum=0.0),
        min_ema_slope_consistency=parse_float_env("MIN_EMA_SLOPE_CONSISTENCY", 0.70, minimum=0.0),
        time_window_hours=parse_int_env("TIME_WINDOW_HOURS", 2, minimum=1),
        min_trades_per_window=parse_int_env("MIN_TRADES_PER_WINDOW", 20, minimum=1),
        min_profit_window_profit_factor=parse_float_env("MIN_PROFIT_WINDOW_PROFIT_FACTOR", 1.05, minimum=0.0),
    )

    if config.max_entry_abs_z < config.min_entry_abs_z:
        raise ConfigError("MAX_ENTRY_ABS_Z must be >= MIN_ENTRY_ABS_Z")
    if config.shadow_probe_max_abs_z < config.shadow_probe_min_abs_z:
        raise ConfigError("SHADOW_PROBE_MAX_ABS_Z must be >= SHADOW_PROBE_MIN_ABS_Z")
    if config.low_atr_percentile > 1:
        raise ConfigError("LOW_ATR_PERCENTILE must be <= 1")
    if config.regime_high_atr_percentile > 1:
        raise ConfigError("REGIME_HIGH_ATR_PERCENTILE must be <= 1")
    if config.regime_low_atr_percentile > 1:
        raise ConfigError("REGIME_LOW_ATR_PERCENTILE must be <= 1")
    if config.regime_low_atr_percentile >= config.regime_high_atr_percentile:
        raise ConfigError("REGIME_LOW_ATR_PERCENTILE must be lower than REGIME_HIGH_ATR_PERCENTILE")
    if config.regime_fast_ema_period >= config.regime_slow_ema_period:
        raise ConfigError("REGIME_FAST_EMA_PERIOD must be lower than REGIME_SLOW_EMA_PERIOD")
    if config.time_window_hours > 24 or 24 % config.time_window_hours != 0:
        raise ConfigError("TIME_WINDOW_HOURS must divide 24")
    if config.min_ema_slope_consistency > 1:
        raise ConfigError("MIN_EMA_SLOPE_CONSISTENCY must be <= 1")
    if config.max_regime_switch_rate > 1:
        raise ConfigError("MAX_REGIME_SWITCH_RATE must be <= 1")
    if config.live_trading:
        raise ConfigError("LIVE_TRADING must remain false in this shadow validation engine")
    if not config.dry_run:
        raise ConfigError("DRY_RUN must remain true in this shadow validation engine")
    if not config.shadow_mode:
        raise ConfigError("SHADOW_MODE must remain true in this shadow validation engine")
    return config


def safe_config_dict(config: Config) -> Dict[str, Any]:
    payload = asdict(config)
    payload["api_key"] = bool(config.api_key)
    payload["api_secret"] = bool(config.api_secret)
    return payload


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def retry_api_call(label: str, func: Callable[..., Any], *args: Any, attempts: int = DEFAULT_RETRY_ATTEMPTS, **kwargs: Any) -> Any:
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            sleep_seconds = DEFAULT_RETRY_BASE_SLEEP_SECONDS * attempt
            LOGGER.warning("%s failed on attempt %s/%s: %s", label, attempt, attempts, exc)
            time.sleep(sleep_seconds)
    raise RuntimeError(f"{label} failed after {attempts} attempts: {last_error}")


def build_client(config: Config) -> Any:
    if Client is None:
        raise ConfigError("python-binance is required. Install it with: pip install python-binance")
    return Client(
        config.api_key or None,
        config.api_secret or None,
        testnet=config.testnet,
        requests_params={"timeout": config.api_timeout_seconds},
    )


def exchange_filters_to_dict(filters: ExchangeFilters) -> Dict[str, Any]:
    return {
        "step_size": filters.step_size,
        "tick_size": filters.tick_size,
        "min_qty": filters.min_qty,
        "max_qty": filters.max_qty,
        "min_notional": filters.min_notional,
        "quantity_precision": filters.quantity_precision,
        "price_precision": filters.price_precision,
    }


def exchange_filters_from_dict(payload: Dict[str, Any]) -> ExchangeFilters:
    return ExchangeFilters(
        step_size=float(payload["step_size"]),
        tick_size=float(payload["tick_size"]),
        min_qty=float(payload["min_qty"]),
        max_qty=float(payload["max_qty"]),
        min_notional=float(payload["min_notional"]),
        quantity_precision=int(payload["quantity_precision"]),
        price_precision=int(payload["price_precision"]),
    )


def load_cached_exchange_filters(symbol: str, cache_file: str) -> Optional[ExchangeFilters]:
    path = Path(cache_file)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        symbol_payload = payload.get(symbol.upper())
        if not isinstance(symbol_payload, dict):
            return None
        return exchange_filters_from_dict(symbol_payload)
    except Exception as exc:
        LOGGER.warning("Could not load exchange filter cache %s: %s", path, exc)
        return None


def exchange_filters_cache_is_fresh(cache_file: str, ttl_seconds: float, now_epoch: Optional[float] = None) -> bool:
    path = Path(cache_file)
    if not path.exists() or ttl_seconds <= 0:
        return False
    now_value = time.time() if now_epoch is None else float(now_epoch)
    return max(0.0, now_value - path.stat().st_mtime) <= ttl_seconds


def save_cached_exchange_filters(symbol: str, filters: ExchangeFilters, cache_file: str) -> None:
    path = Path(cache_file)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, Any] = {}
        if path.exists():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
        payload[symbol.upper()] = exchange_filters_to_dict(filters)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")
        os.replace(temp_path, path)
    except Exception as exc:
        LOGGER.warning("Could not write exchange filter cache %s: %s", path, exc)


def get_exchange_filters(client: Any, symbol: str, config: Optional[Config] = None) -> ExchangeFilters:
    cache_file = (
        config.exchange_filters_cache_file
        if config is not None
        else os.getenv("EXCHANGE_FILTERS_CACHE_FILE", "exchange_filters_cache.json")
    )
    cached = load_cached_exchange_filters(symbol, cache_file)
    if (
        config is not None
        and cached is not None
        and exchange_filters_cache_is_fresh(cache_file, config.exchange_filters_cache_ttl_seconds)
    ):
        return cached
    try:
        info = retry_api_call("Fetch futures exchange info", client.futures_exchange_info)
    except Exception as exc:
        if cached is not None:
            LOGGER.warning("Using cached exchange filters for %s after exchangeInfo failure: %s", symbol, exc)
            return cached
        raise
    for item in info.get("symbols", []):
        if item.get("symbol") != symbol:
            continue
        filters = {entry.get("filterType"): entry for entry in item.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", {})
        notional = filters.get("MIN_NOTIONAL", {})
        price_filter = filters.get("PRICE_FILTER", {})
        step_size = float((market_lot or lot).get("stepSize", lot.get("stepSize", "0.001")))
        min_qty = float((market_lot or lot).get("minQty", lot.get("minQty", "0.0")))
        max_qty = float((market_lot or lot).get("maxQty", lot.get("maxQty", "100000000")))
        min_notional = float(notional.get("notional", notional.get("minNotional", "0.0")))
        tick_size = float(price_filter.get("tickSize", "0.01"))
        parsed = ExchangeFilters(
            step_size=step_size,
            tick_size=tick_size,
            min_qty=min_qty,
            max_qty=max_qty,
            min_notional=min_notional,
            quantity_precision=int(item.get("quantityPrecision", 3)),
            price_precision=int(item.get("pricePrecision", 2)),
        )
        save_cached_exchange_filters(symbol, parsed, cache_file)
        return parsed
    raise ConfigError(f"Symbol {symbol} was not found in futures exchange info")


def fetch_klines(client: Any, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    raw = retry_api_call(
        f"Fetch klines for {symbol}",
        client.futures_klines,
        symbol=symbol,
        interval=interval,
        limit=limit,
    )
    return klines_to_dataframe(raw)


def klines_to_dataframe(raw: Sequence[Sequence[Any]], now_ms: Optional[int] = None) -> pd.DataFrame:
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]
    df = pd.DataFrame(raw, columns=columns)
    if df.empty:
        return pd.DataFrame(columns=HISTORICAL_KLINE_COLUMNS)
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
    df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce").astype("int64")
    closed_before_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    df = df[df["close_time"] < closed_before_ms].copy()
    df.dropna(subset=["open", "high", "low", "close"], inplace=True)
    df.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
    df.sort_values("open_time", inplace=True)
    return df[HISTORICAL_KLINE_COLUMNS].reset_index(drop=True)


def load_historical_kline_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=HISTORICAL_KLINE_COLUMNS)
    frame = pd.read_csv(path)
    missing = [column for column in HISTORICAL_KLINE_COLUMNS if column not in frame.columns]
    if missing:
        raise ConfigError(f"Historical kline cache is missing columns: {','.join(missing)}")
    for column in HISTORICAL_KLINE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame.dropna(subset=HISTORICAL_KLINE_COLUMNS, inplace=True)
    frame["open_time"] = frame["open_time"].astype("int64")
    frame["close_time"] = frame["close_time"].astype("int64")
    frame.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
    frame.sort_values("open_time", inplace=True)
    return frame[HISTORICAL_KLINE_COLUMNS].reset_index(drop=True)


def write_historical_kline_cache(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    frame[HISTORICAL_KLINE_COLUMNS].to_csv(temp_path, index=False)
    os.replace(temp_path, path)


def missing_kline_ranges(
    frame: pd.DataFrame,
    start_ms: int,
    end_ms: int,
    interval_ms: int,
) -> List[Tuple[int, int]]:
    first_open_ms = (int(start_ms) // interval_ms) * interval_ms
    last_open_ms = ((int(end_ms) - interval_ms + 1) // interval_ms) * interval_ms
    if last_open_ms < first_open_ms:
        return []
    available = set(
        int(value)
        for value in frame.loc[
            (frame["open_time"] >= first_open_ms) & (frame["open_time"] <= last_open_ms),
            "open_time",
        ].tolist()
    )
    ranges: List[Tuple[int, int]] = []
    range_start: Optional[int] = None
    previous_missing: Optional[int] = None
    for open_time_ms in range(first_open_ms, last_open_ms + 1, interval_ms):
        if open_time_ms in available:
            if range_start is not None and previous_missing is not None:
                ranges.append((range_start, previous_missing + interval_ms - 1))
                range_start = None
                previous_missing = None
            continue
        if range_start is None:
            range_start = open_time_ms
        previous_missing = open_time_ms
    if range_start is not None and previous_missing is not None:
        ranges.append((range_start, previous_missing + interval_ms - 1))
    return ranges


def fetch_historical_klines_range(
    client: Any,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    retry_attempts: int,
) -> pd.DataFrame:
    period_seconds = interval_seconds(interval)
    if period_seconds is None:
        raise ConfigError(f"Unsupported replay interval: {interval}")
    interval_ms = period_seconds * 1000
    cursor = int(start_ms)
    pages: List[pd.DataFrame] = []
    current_time_ms = int(time.time() * 1000)
    while cursor <= end_ms:
        raw = retry_api_call(
            f"Fetch historical klines for {symbol}",
            client.futures_klines,
            symbol=symbol,
            interval=interval,
            startTime=cursor,
            endTime=int(end_ms),
            limit=1500,
            attempts=retry_attempts,
        )
        if not raw:
            break
        page = klines_to_dataframe(raw, now_ms=min(current_time_ms, int(end_ms) + 1))
        page = page[(page["open_time"] >= cursor) & (page["close_time"] <= end_ms)].copy()
        if page.empty:
            break
        pages.append(page)
        next_cursor = int(page["open_time"].iloc[-1]) + interval_ms
        if next_cursor <= cursor:
            raise RuntimeError("Historical kline pagination did not advance")
        cursor = next_cursor
    if not pages:
        return pd.DataFrame(columns=HISTORICAL_KLINE_COLUMNS)
    combined = pd.concat(pages, ignore_index=True)
    combined.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
    combined.sort_values("open_time", inplace=True)
    return combined[HISTORICAL_KLINE_COLUMNS].reset_index(drop=True)


def ensure_historical_kline_cache(
    client: Any,
    config: Config,
    start_ms: int,
    end_ms: int,
    cache_path: Path,
) -> pd.DataFrame:
    period_seconds = interval_seconds(config.interval)
    if period_seconds is None:
        raise ConfigError(f"Unsupported replay interval: {config.interval}")
    interval_ms = period_seconds * 1000
    cached = load_historical_kline_cache(cache_path)
    ranges = missing_kline_ranges(cached, start_ms, end_ms, interval_ms)
    downloaded: List[pd.DataFrame] = []
    for missing_start, missing_end in ranges:
        LOGGER.info(
            "Downloading historical klines symbol=%s start=%s end=%s",
            config.symbol,
            candle_time_text(missing_start + interval_ms - 1),
            candle_time_text(missing_end),
        )
        frame = fetch_historical_klines_range(
            client,
            config.symbol,
            config.interval,
            missing_start,
            min(missing_end, end_ms),
            config.retry_attempts,
        )
        if frame.empty:
            raise RuntimeError("Binance returned no data for a required historical kline range")
        downloaded.append(frame)
    if downloaded:
        frames = ([cached] if not cached.empty else []) + downloaded
        cached = pd.concat(frames, ignore_index=True)
        cached.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
        cached.sort_values("open_time", inplace=True)
        cached.reset_index(drop=True, inplace=True)
        write_historical_kline_cache(cache_path, cached)
    remaining = missing_kline_ranges(cached, start_ms, end_ms, interval_ms)
    if remaining:
        raise RuntimeError(f"Historical kline cache still contains {len(remaining)} missing ranges")
    selected = cached[(cached["open_time"] >= start_ms) & (cached["close_time"] <= end_ms)].copy()
    if selected.empty:
        raise RuntimeError("Historical kline cache does not cover the requested replay period")
    return selected.reset_index(drop=True)


def candle_time_text(close_time_ms: int) -> str:
    return datetime.fromtimestamp(close_time_ms / 1000.0, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def compute_zscore(klines: pd.DataFrame, lookback: int) -> ZScoreResult:
    if len(klines) < lookback + 1:
        raise ConfigError(f"Need at least {lookback + 1} closed candles to compute current and previous z-score")
    closes = klines["close"].astype(float).to_numpy()
    window = closes[-lookback:]
    previous_window = closes[-lookback - 1 : -1]
    mean = float(np.mean(window))
    std = float(np.std(window, ddof=0))
    previous_mean = float(np.mean(previous_window))
    previous_std = float(np.std(previous_window, ddof=0))
    if std <= 0 or previous_std <= 0:
        raise ConfigError("Z-score standard deviation is zero")
    zscore = float((closes[-1] - mean) / std)
    previous_zscore = float((closes[-2] - previous_mean) / previous_std)
    return ZScoreResult(
        latest_close=float(closes[-1]),
        previous_close=float(closes[-2]),
        mean=mean,
        std=std,
        zscore=zscore,
        previous_zscore=previous_zscore,
        latest_closed_time=candle_time_text(int(klines.iloc[-1]["close_time"])),
    )


def compute_atr_bucket(klines: pd.DataFrame, config: Config) -> Tuple[float, str]:
    if len(klines) < config.atr_lookback + 5:
        return 0.0, "unknown"
    high = klines["high"].astype(float)
    low = klines["low"].astype(float)
    close = klines["close"].astype(float)
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.rolling(config.atr_lookback).mean()
    atr_pct_series = atr / close
    atr_pct_series = atr_pct_series.dropna()
    if atr_pct_series.empty:
        return 0.0, "unknown"
    latest_atr_pct = float(atr_pct_series.iloc[-1])
    low_cutoff = float(atr_pct_series.quantile(config.low_atr_percentile))
    high_cutoff = float(atr_pct_series.quantile(0.80))
    if latest_atr_pct <= low_cutoff and latest_atr_pct <= config.max_atr_pct:
        bucket = "low"
    elif latest_atr_pct >= high_cutoff or latest_atr_pct > config.max_atr_pct:
        bucket = "high"
    else:
        bucket = "medium"
    return latest_atr_pct, bucket


def compute_trend_bucket(klines: pd.DataFrame, config: Config) -> Tuple[float, str]:
    required = config.trend_ema_period + config.trend_slope_lookback + 2
    if len(klines) < required:
        return 0.0, "unknown"
    close = klines["close"].astype(float)
    ema = close.ewm(span=config.trend_ema_period, adjust=False).mean()
    current_ema = float(ema.iloc[-1])
    previous_ema = float(ema.iloc[-1 - config.trend_slope_lookback])
    if previous_ema == 0:
        return 0.0, "unknown"
    slope_pct = (current_ema - previous_ema) / previous_ema
    magnitude = abs(slope_pct)
    if magnitude < config.trend_slope_threshold_pct:
        bucket = "weak"
    elif magnitude < config.trend_slope_threshold_pct * 2:
        bucket = "medium"
    else:
        bucket = "strong"
    return float(slope_pct), bucket


def compute_atr_percentile(klines: pd.DataFrame, config: Config) -> Tuple[float, float, str]:
    required = max(config.atr_lookback + 5, config.regime_slow_ema_period + 2)
    if len(klines) < required:
        return 0.0, 0.0, "unknown"
    high = klines["high"].astype(float)
    low = klines["low"].astype(float)
    close = klines["close"].astype(float)
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_pct = (true_range.rolling(config.atr_lookback).mean() / close).dropna()
    if atr_pct.empty:
        return 0.0, 0.0, "unknown"
    latest = float(atr_pct.iloc[-1])
    percentile = float((atr_pct <= latest).mean())
    if percentile <= config.regime_low_atr_percentile and latest <= config.max_atr_pct:
        bucket = "low"
    elif percentile >= config.regime_high_atr_percentile or latest > config.max_atr_pct:
        bucket = "high"
    else:
        bucket = "medium"
    return latest, percentile, bucket


def count_sign_crosses(values: Sequence[float]) -> int:
    previous = 0
    crosses = 0
    for value in values:
        current = 1 if value > 0 else -1 if value < 0 else 0
        if current == 0:
            continue
        if previous != 0 and current != previous:
            crosses += 1
        previous = current
    return crosses


def clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, float(value)))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def time_window_id(timestamp: str, hours: int) -> str:
    parsed = parse_time_text(str(timestamp or ""))
    if parsed is None or hours <= 0:
        return "UNKNOWN"
    start_hour = (parsed.hour // hours) * hours
    end_hour = start_hour + hours
    return f"{start_hour:02d}:00-{end_hour:02d}:00 UTC"


def default_market_gate_snapshot(config: Config, timestamp: str = "") -> Dict[str, Any]:
    window_id = time_window_id(timestamp, config.time_window_hours)
    return {
        "regime_stability_score": 0.0,
        "stability_state": "UNSTABLE",
        "regime_persistence_candles": 0,
        "regime_switch_rate": 1.0,
        "atr_coefficient_of_variation": 1.0,
        "ema_slope_consistency": 0.0,
        "time_window_id": window_id,
        "window_trade_count": 0,
        "window_expectancy": None,
        "window_profit_factor": None,
        "window_is_profitable": False,
        "trade_allowed_by_market_gate": False,
        "market_gate_enforced": config.market_gate_enforced,
        "market_gate_rejection_reason": "INSUFFICIENT_WINDOW_TRADES",
    }


def closed_candle_atr_pct_series(klines: pd.DataFrame, config: Config) -> pd.Series:
    if len(klines) < config.atr_lookback + 2:
        return pd.Series(dtype=float)
    high = klines["high"].astype(float)
    low = klines["low"].astype(float)
    close = klines["close"].astype(float)
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return (true_range.rolling(config.atr_lookback).mean() / close).dropna()


def classify_regime_from_series(
    fast_ema: float,
    slow_ema: float,
    ema_slope_pct: float,
    config: Config,
) -> str:
    if fast_ema > slow_ema and ema_slope_pct > config.trend_slope_threshold_pct:
        return "TREND_UP"
    if fast_ema < slow_ema and ema_slope_pct < -config.trend_slope_threshold_pct:
        return "TREND_DOWN"
    if abs(ema_slope_pct) <= config.regime_mean_reversion_max_slope_pct:
        return "MEAN_REVERTING"
    return "UNSTABLE"


def evaluate_regime_stability(klines: pd.DataFrame, config: Config, regime: MarketRegimeSnapshot) -> Dict[str, Any]:
    required = max(
        config.regime_slow_ema_period + config.regime_slope_lookback + config.regime_stability_window + 2,
        config.atr_lookback + config.regime_stability_window + 2,
    )
    if not isinstance(klines, pd.DataFrame) or "close" not in klines or len(klines) < required:
        return {
            "regime_stability_score": 0.0,
            "stability_state": "UNSTABLE",
            "regime_persistence_candles": 0,
            "regime_switch_rate": 1.0,
            "atr_coefficient_of_variation": 1.0,
            "ema_slope_consistency": 0.0,
        }

    close = klines["close"].astype(float)
    fast = close.ewm(span=config.regime_fast_ema_period, adjust=False).mean()
    slow = close.ewm(span=config.regime_slow_ema_period, adjust=False).mean()
    slope = fast.pct_change(periods=config.regime_slope_lookback).fillna(0.0)
    window = max(1, config.regime_stability_window)
    fast_tail = fast.tail(window)
    slow_tail = slow.tail(window)
    slope_tail = slope.tail(window)
    regime_series = [
        classify_regime_from_series(float(fast_value), float(slow_value), float(slope_value), config)
        for fast_value, slow_value, slope_value in zip(fast_tail, slow_tail, slope_tail)
    ]
    latest_regime = regime_series[-1] if regime_series else "UNSTABLE"
    persistence = 0
    for value in reversed(regime_series):
        if value != latest_regime:
            break
        persistence += 1
    switches = sum(1 for left, right in zip(regime_series, regime_series[1:]) if left != right)
    switch_rate = switches / max(len(regime_series) - 1, 1)

    atr_tail = closed_candle_atr_pct_series(klines, config).tail(window)
    atr_mean = float(atr_tail.mean()) if not atr_tail.empty else 0.0
    atr_std = float(atr_tail.std(ddof=0)) if not atr_tail.empty else 0.0
    atr_cv = 1.0 if atr_mean <= 0 else atr_std / atr_mean

    slope_values = [float(value) for value in slope_tail]
    if regime.regime in {"TREND_UP", "TREND_DOWN"}:
        expected_sign = 1 if regime.regime == "TREND_UP" else -1
        consistent = [
            value
            for value in slope_values
            if (value > config.trend_slope_threshold_pct and expected_sign > 0)
            or (value < -config.trend_slope_threshold_pct and expected_sign < 0)
        ]
        slope_consistency = len(consistent) / max(len(slope_values), 1)
    elif regime.regime == "MEAN_REVERTING":
        consistent = [value for value in slope_values if abs(value) <= config.regime_mean_reversion_max_slope_pct]
        slope_consistency = len(consistent) / max(len(slope_values), 1)
    else:
        slope_consistency = 0.0

    persistence_score = clamp(persistence / max(config.min_regime_persistence_candles, 1))
    switch_score = clamp(1.0 - switch_rate / max(config.max_regime_switch_rate, 0.000001))
    atr_score = clamp(1.0 - atr_cv / max(config.max_atr_coefficient_of_variation, 0.000001))
    slope_score = clamp(slope_consistency)
    score = clamp((persistence_score + switch_score + atr_score + slope_score) / 4.0)

    stable_enough = (
        persistence >= config.min_regime_persistence_candles
        and switch_rate <= config.max_regime_switch_rate
        and atr_cv <= config.max_atr_coefficient_of_variation
        and slope_consistency >= config.min_ema_slope_consistency
        and score >= 0.70
    )
    if stable_enough and regime.regime in {"TREND_UP", "TREND_DOWN"}:
        state = "STABLE_TREND"
    elif stable_enough and regime.regime == "MEAN_REVERTING":
        state = "STABLE_MEAN_REVERTING"
    else:
        state = "UNSTABLE"

    return {
        "regime_stability_score": score,
        "stability_state": state,
        "regime_persistence_candles": persistence,
        "regime_switch_rate": float(switch_rate),
        "atr_coefficient_of_variation": float(atr_cv),
        "ema_slope_consistency": float(slope_consistency),
    }


def summarize_trade_group(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(trades)
    net_pnl = sum(safe_float(trade.get("net_pnl")) for trade in trades)
    wins = [trade for trade in trades if safe_float(trade.get("net_pnl")) > 0]
    losses = [trade for trade in trades if safe_float(trade.get("net_pnl")) <= 0]
    gross_win = sum(safe_float(trade.get("net_pnl")) for trade in wins)
    gross_loss = abs(sum(safe_float(trade.get("net_pnl")) for trade in losses))
    if gross_loss > 0:
        profit_factor: Optional[float] = gross_win / gross_loss
    elif gross_win > 0:
        profit_factor = 999999999.0
    else:
        profit_factor = None
    regime_distribution = Counter(str(trade.get("regime_at_entry", "UNKNOWN") or "UNKNOWN") for trade in trades)
    return {
        "trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / total if total else None,
        "net_pnl": net_pnl,
        "expectancy": net_pnl / total if total else None,
        "profit_factor": profit_factor,
        "regime_distribution": dict(regime_distribution),
    }


def build_time_window_analytics(trades: Sequence[Dict[str, Any]], config: Config) -> Dict[str, Any]:
    windows: Dict[str, List[Dict[str, Any]]] = {}
    for start in range(0, 24, config.time_window_hours):
        windows[f"{start:02d}:00-{start + config.time_window_hours:02d}:00 UTC"] = []
    for trade in trades:
        windows.setdefault(time_window_id(str(trade.get("entry_time", "")), config.time_window_hours), []).append(trade)

    window_summary: Dict[str, Dict[str, Any]] = {}
    for window_id, rows in windows.items():
        summary = summarize_trade_group(rows)
        net_profit_factor = summary["net_profit_factor"]
        expectancy = summary["expectancy"]
        is_profitable = (
            summary["trades"] >= config.min_trades_per_window
            and (
                (expectancy is not None and expectancy > 0)
                or (
                    net_profit_factor is not None
                    and net_profit_factor > config.min_profit_window_profit_factor
                )
            )
        )
        summary["is_profitable"] = is_profitable
        summary["time_window_id"] = window_id
        window_summary[window_id] = summary

    traded_windows = [summary for summary in window_summary.values() if summary["trades"] > 0]
    profitable = [summary for summary in traded_windows if summary["is_profitable"]]
    profitable.sort(key=lambda row: (safe_float(row.get("expectancy")), safe_float(row.get("net_pnl"))), reverse=True)
    worst = sorted(traded_windows, key=lambda row: (safe_float(row.get("expectancy")), safe_float(row.get("net_pnl"))))
    return {
        "windows": window_summary,
        "profitable_windows": profitable[:5],
        "worst_windows": worst[:5],
    }


def build_regime_time_window_performance(trades: Sequence[Dict[str, Any]], config: Config) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for trade in trades:
        key = f"{str(trade.get('regime_at_entry', 'UNKNOWN') or 'UNKNOWN')} / {time_window_id(str(trade.get('entry_time', '')), config.time_window_hours)}"
        groups.setdefault(key, []).append(trade)
    return {key: summarize_trade_group(rows) for key, rows in sorted(groups.items())}


def evaluate_market_gate(
    klines: pd.DataFrame,
    config: Config,
    regime: MarketRegimeSnapshot,
    timestamp: str,
    shadow: Dict[str, Any],
) -> Dict[str, Any]:
    gate = default_market_gate_snapshot(config, timestamp)
    stability = evaluate_regime_stability(klines, config, regime)
    gate.update(stability)
    analytics = build_time_window_analytics(shadow.get("trades") or [], config)
    current_window = gate["time_window_id"]
    window_summary = analytics["windows"].get(current_window, summarize_trade_group([]))
    gate["window_trade_count"] = int(window_summary.get("trades", 0) or 0)
    gate["window_expectancy"] = window_summary.get("expectancy")
    gate["window_profit_factor"] = window_summary.get("net_profit_factor")
    gate["window_is_profitable"] = bool(window_summary.get("is_profitable", False))
    stable = gate["stability_state"] in {"STABLE_TREND", "STABLE_MEAN_REVERTING"}
    profitable = bool(gate["window_is_profitable"])
    gate["trade_allowed_by_market_gate"] = stable and profitable
    if not stable:
        gate["market_gate_rejection_reason"] = "UNSTABLE_REGIME"
    elif not profitable:
        gate["market_gate_rejection_reason"] = "UNPROFITABLE_TIME_WINDOW"
    else:
        gate["market_gate_rejection_reason"] = ""
    gate["market_gate_enforced"] = config.market_gate_enforced
    return gate


def attach_market_gate_to_decision(
    decision: StrategySignalDecision,
    market_gate: Dict[str, Any],
) -> StrategySignalDecision:
    return replace(decision, market_gate=dict(market_gate))


def detect_market_regime(klines: pd.DataFrame, config: Config) -> MarketRegimeSnapshot:
    required = max(
        config.regime_slow_ema_period + config.regime_slope_lookback + 2,
        config.atr_lookback + 5,
        config.regime_oscillation_lookback + 2,
    )
    if len(klines) < required:
        return MarketRegimeSnapshot(
            timestamp="",
            regime="NO_TRADE",
            router_strategy=NO_TRADE_STRATEGY_NAME,
            no_trade_reason="not enough closed candles for regime detection",
            ema20=0.0,
            ema50=0.0,
            ema_slope_pct=0.0,
            atr_pct=0.0,
            atr_percentile=0.0,
            atr_bucket="unknown",
            price_distance_ema_pct=0.0,
            price_distance_vwap_pct=0.0,
            ema_state="UNKNOWN",
            trend_bucket="unknown",
        )

    close = klines["close"].astype(float)
    volume = klines["volume"].astype(float) if "volume" in klines else pd.Series([1.0] * len(klines))
    ema_fast = close.ewm(span=config.regime_fast_ema_period, adjust=False).mean()
    ema_slow = close.ewm(span=config.regime_slow_ema_period, adjust=False).mean()
    current_close = float(close.iloc[-1])
    previous_fast = float(ema_fast.iloc[-1 - config.regime_slope_lookback])
    ema20 = float(ema_fast.iloc[-1])
    ema50 = float(ema_slow.iloc[-1])
    ema_slope_pct = 0.0 if previous_fast == 0 else (ema20 - previous_fast) / previous_fast
    atr_pct, atr_percentile, atr_bucket = compute_atr_percentile(klines, config)
    vwap_denominator = float(volume.tail(config.regime_slow_ema_period).sum())
    if vwap_denominator > 0:
        vwap = float((close.tail(config.regime_slow_ema_period) * volume.tail(config.regime_slow_ema_period)).sum() / vwap_denominator)
    else:
        vwap = ema50
    distance_ema = 0.0 if ema50 == 0 else (current_close - ema50) / ema50
    distance_vwap = 0.0 if vwap == 0 else (current_close - vwap) / vwap
    fast_minus_slow = ema20 - ema50
    ema_state = "EMA20_ABOVE_EMA50" if fast_minus_slow > 0 else "EMA20_BELOW_EMA50" if fast_minus_slow < 0 else "EMA20_EQUALS_EMA50"
    slope_abs = abs(ema_slope_pct)
    if slope_abs < config.trend_slope_threshold_pct:
        trend_bucket = "weak"
    elif slope_abs < config.trend_slope_threshold_pct * 2:
        trend_bucket = "medium"
    else:
        trend_bucket = "strong"

    oscillation_window = close.tail(config.regime_oscillation_lookback) - ema_fast.tail(config.regime_oscillation_lookback)
    crosses = count_sign_crosses([float(value) for value in oscillation_window])
    trend_up = ema20 > ema50 and ema_slope_pct > config.trend_slope_threshold_pct
    trend_down = ema20 < ema50 and ema_slope_pct < -config.trend_slope_threshold_pct
    high_volatility = atr_percentile >= config.regime_high_atr_percentile
    low_volatility = atr_percentile <= config.regime_low_atr_percentile and atr_bucket == "low"
    mean_reverting = (
        slope_abs <= config.regime_mean_reversion_max_slope_pct
        and abs(distance_ema) <= config.regime_mean_reversion_max_distance_pct
        and abs(distance_vwap) <= config.regime_mean_reversion_max_distance_pct
        and crosses >= config.regime_min_ema_crosses
    )

    regime = "NO_TRADE"
    router_strategy = NO_TRADE_STRATEGY_NAME
    no_trade_reason = "conflicting regime signals"
    if trend_up:
        regime = "TREND_UP"
        router_strategy = TREND_FOLLOW_LONG_STRATEGY_NAME
        no_trade_reason = ""
    elif trend_down:
        regime = "TREND_DOWN"
        router_strategy = TREND_FOLLOW_SHORT_STRATEGY_NAME
        no_trade_reason = ""
    elif high_volatility:
        regime = "HIGH_VOLATILITY"
        router_strategy = BREAKOUT_STRATEGY_NAME
        no_trade_reason = ""
    elif mean_reverting:
        regime = "MEAN_REVERTING"
        router_strategy = MEAN_REVERSION_STRATEGY_NAME
        no_trade_reason = ""
    elif low_volatility:
        regime = "LOW_VOLATILITY"
        router_strategy = NO_TRADE_STRATEGY_NAME
        no_trade_reason = "low volatility without mean-reversion structure"

    return MarketRegimeSnapshot(
        timestamp=candle_time_text(int(klines.iloc[-1]["close_time"])) if "close_time" in klines else "",
        regime=regime,
        router_strategy=router_strategy,
        no_trade_reason=no_trade_reason,
        ema20=ema20,
        ema50=ema50,
        ema_slope_pct=float(ema_slope_pct),
        atr_pct=atr_pct,
        atr_percentile=atr_percentile,
        atr_bucket=atr_bucket,
        price_distance_ema_pct=float(distance_ema),
        price_distance_vwap_pct=float(distance_vwap),
        ema_state=ema_state,
        trend_bucket=trend_bucket,
    )


def recent_breakout_bounds(klines: pd.DataFrame, config: Config) -> Tuple[float, float]:
    if len(klines) < config.breakout_lookback + 1:
        close = klines["close"].astype(float)
        if close.empty:
            return 0.0, 0.0
        value = float(close.iloc[-1])
        return value, value
    previous = klines.iloc[-1 - config.breakout_lookback : -1]
    return float(previous["high"].astype(float).max()), float(previous["low"].astype(float).min())


def build_strategy_rejection(
    config: Config,
    regime: MarketRegimeSnapshot,
    timestamp: str,
    close_price: float,
    previous_close: float,
    zscore: float,
    previous_zscore: float,
    direction: str,
    failed_filter: str,
    reason: str,
    reversion_confirmed: bool = False,
) -> StrategySignalDecision:
    del config
    return StrategySignalDecision(
        timestamp=timestamp,
        strategy=regime.router_strategy,
        regime=regime.regime,
        router_strategy=regime.router_strategy,
        close_price=float(close_price),
        previous_close=float(previous_close),
        zscore=float(zscore),
        previous_zscore=float(previous_zscore),
        atr_pct=regime.atr_pct,
        atr_percentile=regime.atr_percentile,
        atr_bucket=regime.atr_bucket,
        trend_bucket=regime.trend_bucket,
        trend_slope_pct=regime.ema_slope_pct,
        ema20=regime.ema20,
        ema50=regime.ema50,
        ema_slope_pct=regime.ema_slope_pct,
        ema_state=regime.ema_state,
        price_distance_ema_pct=regime.price_distance_ema_pct,
        price_distance_vwap_pct=regime.price_distance_vwap_pct,
        direction=direction,
        trade_decision="REJECT",
        entry_reason="",
        rejection_reason=reason,
        failed_filter=failed_filter,
        reversion_confirmed=reversion_confirmed,
        no_trade_reason=regime.no_trade_reason,
    )


def build_strategy_entry(
    regime: MarketRegimeSnapshot,
    timestamp: str,
    close_price: float,
    previous_close: float,
    zscore: float,
    previous_zscore: float,
    direction: str,
    entry_reason: str,
    reversion_confirmed: bool,
) -> StrategySignalDecision:
    return StrategySignalDecision(
        timestamp=timestamp,
        strategy=regime.router_strategy,
        regime=regime.regime,
        router_strategy=regime.router_strategy,
        close_price=float(close_price),
        previous_close=float(previous_close),
        zscore=float(zscore),
        previous_zscore=float(previous_zscore),
        atr_pct=regime.atr_pct,
        atr_percentile=regime.atr_percentile,
        atr_bucket=regime.atr_bucket,
        trend_bucket=regime.trend_bucket,
        trend_slope_pct=regime.ema_slope_pct,
        ema20=regime.ema20,
        ema50=regime.ema50,
        ema_slope_pct=regime.ema_slope_pct,
        ema_state=regime.ema_state,
        price_distance_ema_pct=regime.price_distance_ema_pct,
        price_distance_vwap_pct=regime.price_distance_vwap_pct,
        direction=direction,
        trade_decision="ENTER",
        entry_reason=entry_reason,
        rejection_reason="",
        failed_filter="",
        reversion_confirmed=reversion_confirmed,
        no_trade_reason="",
    )


def route_strategy(
    config: Config,
    regime: MarketRegimeSnapshot,
    timestamp: str,
    close_price: float,
    previous_close: float,
    zscore: float,
    previous_zscore: float,
    recent_high: float,
    recent_low: float,
) -> StrategySignalDecision:
    direction = "LONG" if zscore < 0 else "SHORT" if zscore > 0 else "NONE"
    strategy = regime.router_strategy

    if regime.regime == "NO_TRADE" or strategy == NO_TRADE_STRATEGY_NAME:
        return build_strategy_rejection(
            config,
            regime,
            timestamp,
            close_price,
            previous_close,
            zscore,
            previous_zscore,
            direction,
            "regime",
            regime.no_trade_reason or "router selected no-trade regime",
        )

    if strategy == MEAN_REVERSION_STRATEGY_NAME:
        abs_z = abs(float(zscore))
        long_reversion = (
            zscore < 0
            and previous_zscore <= -2.20
            and zscore > previous_zscore + config.min_z_reversion_delta
            and close_price > previous_close
        )
        short_reversion = (
            zscore > 0
            and previous_zscore >= 2.20
            and zscore < previous_zscore - config.min_z_reversion_delta
            and close_price < previous_close
        )
        reversion_confirmed = long_reversion or short_reversion
        if regime.atr_bucket != "low":
            return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, direction, "atr", "mean reversion requires low ATR", reversion_confirmed)
        if not (2.20 <= abs_z <= 2.80):
            return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, direction, "zscore", "mean reversion z-score is outside 2.20-2.80", reversion_confirmed)
        if not reversion_confirmed:
            return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, direction, "reversion", "mean reversion confirmation failed", reversion_confirmed)
        side = "LONG" if long_reversion else "SHORT"
        return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, side, "z-score deviation confirmed back toward mean", True)

    if strategy == TREND_FOLLOW_LONG_STRATEGY_NAME:
        pullback_reclaim = previous_close <= regime.ema20 <= close_price and close_price > previous_close
        breakout = recent_high > 0 and close_price >= recent_high and close_price > previous_close
        if not (regime.ema20 > regime.ema50):
            return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, "LONG", "ema", "trend long requires EMA20 above EMA50")
        if pullback_reclaim:
            return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, "LONG", "trend pullback reclaimed EMA20", False)
        if breakout:
            return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, "LONG", "trend breakout above recent high", False)
        return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, "LONG", "entry", "trend long needs pullback reclaim or breakout")

    if strategy == TREND_FOLLOW_SHORT_STRATEGY_NAME:
        pullback_reject = previous_close >= regime.ema20 >= close_price and close_price < previous_close
        breakdown = recent_low > 0 and close_price <= recent_low and close_price < previous_close
        if not (regime.ema20 < regime.ema50):
            return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, "SHORT", "ema", "trend short requires EMA20 below EMA50")
        if pullback_reject:
            return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, "SHORT", "trend pullback rejected EMA20", False)
        if breakdown:
            return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, "SHORT", "trend breakdown below recent low", False)
        return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, "SHORT", "entry", "trend short needs pullback rejection or breakdown")

    if strategy == BREAKOUT_STRATEGY_NAME:
        if regime.atr_bucket != "high":
            return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, direction, "atr", "breakout requires high volatility")
        if recent_high > 0 and close_price >= recent_high and close_price > previous_close:
            return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, "LONG", "high-volatility breakout above recent high", False)
        if recent_low > 0 and close_price <= recent_low and close_price < previous_close:
            return build_strategy_entry(regime, timestamp, close_price, previous_close, zscore, previous_zscore, "SHORT", "high-volatility breakdown below recent low", False)
        return build_strategy_rejection(config, regime, timestamp, close_price, previous_close, zscore, previous_zscore, direction, "entry", "breakout requires close beyond recent range")

    return build_strategy_rejection(
        config,
        regime,
        timestamp,
        close_price,
        previous_close,
        zscore,
        previous_zscore,
        direction,
        "strategy",
        f"unsupported router strategy {strategy}",
    )


def evaluate_elite_signal(
    config: Config,
    timestamp: str,
    close_price: float,
    previous_close: float,
    zscore: float,
    previous_zscore: float,
    atr_pct: float,
    atr_bucket: str,
    trend_bucket: str,
    trend_slope_pct: float,
) -> EliteSignalDecision:
    direction = "LONG" if zscore < 0 else "SHORT" if zscore > 0 else "NONE"
    abs_z = abs(float(zscore))
    reversion_confirmed = bool(
        zscore < 0
        and previous_zscore <= -config.min_entry_abs_z
        and zscore > previous_zscore + config.min_z_reversion_delta
        and close_price > previous_close
    )

    failed_filter = ""
    rejection_reason = ""
    trade_decision = "ENTER"

    if direction != "LONG":
        failed_filter = "direction"
        rejection_reason = "short signals are disabled; elite strategy is long-only"
    elif not (config.min_entry_abs_z <= abs_z <= config.max_entry_abs_z):
        failed_filter = "zscore"
        rejection_reason = (
            f"absolute z-score {abs_z:.6f} is outside "
            f"{config.min_entry_abs_z:.6f}-{config.max_entry_abs_z:.6f}"
        )
    elif atr_bucket not in config.elite_allowed_atr_buckets:
        failed_filter = "atr"
        rejection_reason = (
            f"ATR bucket must be one of {','.join(config.elite_allowed_atr_buckets)}, got {atr_bucket}"
        )
    elif trend_bucket not in config.elite_allowed_trend_buckets:
        failed_filter = "trend"
        rejection_reason = (
            f"trend bucket must be one of {','.join(config.elite_allowed_trend_buckets)}, got {trend_bucket}"
        )
    elif not reversion_confirmed:
        failed_filter = "reversion"
        rejection_reason = "basic reversion confirmation failed"

    if failed_filter:
        trade_decision = "REJECT"

    return EliteSignalDecision(
        timestamp=timestamp,
        close_price=float(close_price),
        previous_close=float(previous_close),
        zscore=float(zscore),
        previous_zscore=float(previous_zscore),
        atr_pct=float(atr_pct),
        atr_bucket=str(atr_bucket),
        trend_bucket=str(trend_bucket),
        trend_slope_pct=float(trend_slope_pct),
        direction=direction,
        trade_decision=trade_decision,
        rejection_reason=rejection_reason,
        failed_filter=failed_filter,
        reversion_confirmed=reversion_confirmed,
    )


def decimal_floor(value: float, step: float) -> float:
    if step <= 0:
        return value
    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    return float((value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN) * step_decimal)


def calculate_order_quantity(balance: float, price: float, filters: ExchangeFilters, config: Config) -> float:
    del balance
    if price <= 0:
        return 0.0
    max_notional_from_margin = config.max_margin_usdt * config.leverage
    target_notional = min(config.max_notional_usdt, max_notional_from_margin)
    if target_notional < filters.min_notional:
        target_notional = filters.min_notional
    quantity = decimal_floor(target_notional / price, filters.step_size)
    quantity = min(quantity, filters.max_qty)
    if quantity < filters.min_qty:
        return 0.0
    if quantity * price < filters.min_notional:
        return 0.0
    return round(quantity, filters.quantity_precision)


def validate_order(quantity: float, price: float, filters: ExchangeFilters, config: Config) -> bool:
    del config
    if quantity <= 0 or price <= 0:
        return False
    if quantity < filters.min_qty or quantity > filters.max_qty:
        return False
    notional = quantity * price
    if notional < filters.min_notional:
        return False
    rounded = decimal_floor(quantity, filters.step_size)
    return abs(rounded - quantity) < max(filters.step_size / 10.0, 1e-12)


def place_market_order(
    client: Any,
    config: Config,
    side: str,
    quantity: float,
    filters: ExchangeFilters,
    reduce_only: bool,
    state: Optional[RuntimeState],
    reason: str,
) -> Dict[str, Any]:
    del client, filters
    if config.live_trading or not config.dry_run or not config.shadow_mode:
        raise ConfigError("Real order execution is disabled in the regime router shadow validation engine")
    order = {
        "symbol": config.symbol,
        "side": side,
        "type": "MARKET",
        "quantity": f"{quantity:.8f}".rstrip("0").rstrip("."),
        "reduce_only": bool(reduce_only),
        "status": "SIMULATED",
        "simulated": True,
        "reason": reason,
        "time": utc_now_text(),
    }
    if state is not None:
        state.record_order(order)
    LOGGER.info("Shadow-only order simulation: %s", order)
    return order


def close_position(
    client: Any,
    config: Config,
    position: Dict[str, Any],
    filters: ExchangeFilters,
    state: Optional[RuntimeState],
    reason: str,
) -> Dict[str, Any]:
    side = "SELL" if str(position.get("side", "")).upper() == "LONG" else "BUY"
    quantity = float(position.get("quantity", 0.0) or 0.0)
    return place_market_order(client, config, side, quantity, filters, True, state, reason)


def default_shadow_state() -> Dict[str, Any]:
    return {
        "engine": "regime_router_shadow_v1",
        "enabled": True,
        "active_strategies": list(ACTIVE_STRATEGIES),
        "logging_only_strategies": list(LOGGING_ONLY_STRATEGIES),
        "active_execution_strategy": ACTIVE_STRATEGY,
        "dataset_valid": True,
        "execution_contamination_detected": False,
        "contaminated_trade_count": 0,
        "execution_rejections": [],
        "last_execution_rejection": {},
        "candidate_signals": 0,
        "last_signal_timestamp": "",
        "completed_trades": 0,
        "open_position_count": 0,
        "current_regime": "NO_TRADE",
        "router_active_strategy": NO_TRADE_STRATEGY_NAME,
        "router_position": None,
        "elite_long_position": None,
        "shadow_probe_position": None,
        "latest_signal": {},
        "current_regime_stability_score": 0.0,
        "current_stability_state": "UNSTABLE",
        "current_time_window_id": "UNKNOWN",
        "current_trade_allowed_by_market_gate": False,
        "current_market_gate_rejection_reason": "",
        "market_gate_enforced": False,
        "time_window_performance": {},
        "profitable_windows": [],
        "worst_windows": [],
        "regime_time_window_performance": {},
        "latest_decision_report": {},
        "last_decision_report_completed_trades": 0,
        "trades": [],
        "recent_trades": [],
        "win_rate": None,
        "net_pnl_usdt": 0.0,
        "gross_pnl_usdt": 0.0,
        "fees_usdt": 0.0,
        "slippage_usdt": 0.0,
        "expectancy_usdt": None,
        "profit_factor": None,
        "max_drawdown_usdt": 0.0,
        "average_win_usdt": None,
        "average_loss_usdt": None,
        "best_streak": 0,
        "worst_streak": 0,
        "viability_status": "INSUFFICIENT_DATA",
        "regime_distribution": {},
        "strategy_performance": {},
        "regime_performance": {},
        "strategy_performance_per_regime": {},
        "best_regime_strategy_pair": None,
        "worst_regime_strategy_pair": None,
        "last_update": "",
    }


def shadow_trade_strategy(trade: Dict[str, Any]) -> str:
    return str(trade.get("strategy_id") or trade.get("strategy") or "")


def normalize_trade_attribution(trade: Dict[str, Any]) -> None:
    strategy = shadow_trade_strategy(trade)
    if not strategy:
        strategy = "UNKNOWN"
    trade["strategy_id"] = strategy
    trade["strategy_name"] = strategy
    trade["strategy"] = strategy
    trade.setdefault("execution_source", "")
    trade.setdefault("signal_origin", "")


def record_execution_rejection(
    shadow: Dict[str, Any],
    rejected_strategy: str,
    reason: str,
    execution_source: str = "shadow_execution_gate",
    signal_origin: str = "unknown",
) -> None:
    entry = {
        "timestamp": utc_now_text(),
        "rejected_strategy": str(rejected_strategy or "UNKNOWN"),
        "reason": reason,
        "active_strategy": ACTIVE_STRATEGY,
        "execution_source": execution_source,
        "signal_origin": signal_origin,
    }
    rejections = shadow.setdefault("execution_rejections", [])
    if not isinstance(rejections, list):
        rejections = []
    rejections.insert(0, entry)
    shadow["execution_rejections"] = rejections[:50]
    shadow["last_execution_rejection"] = entry


def detect_execution_contamination(shadow: Dict[str, Any]) -> None:
    trades = shadow.get("trades") or []
    contaminated = [
        trade
        for trade in trades
        if shadow_trade_strategy(trade) not in EXECUTABLE_STRATEGIES
        or str(trade.get("execution_source", "")) != ACTIVE_EXECUTION_SOURCE
        or not str(trade.get("regime_at_entry", "")).strip()
    ]
    count = len(contaminated)
    shadow["contaminated_trade_count"] = count
    shadow["execution_contamination_detected"] = count > 0
    shadow["dataset_valid"] = count == 0
    if count > 0 and shadow.get("last_contamination_logged_count") != count:
        LOGGER.error(
            "EXECUTION CONTAMINATION DETECTED contaminated_trade_count=%s active_strategy=%s",
            count,
            ACTIVE_STRATEGY,
        )
        shadow["last_contamination_logged_count"] = count


def ensure_shadow_schema(shadow: Dict[str, Any]) -> None:
    defaults = default_shadow_state()
    for key, value in defaults.items():
        shadow.setdefault(key, value)
    if shadow.get("engine") != "regime_router_shadow_v1":
        shadow.clear()
        shadow.update(defaults)
    if not isinstance(shadow.get("trades"), list):
        shadow["trades"] = []
    if not isinstance(shadow.get("recent_trades"), list):
        shadow["recent_trades"] = []
    shadow["active_strategies"] = list(ACTIVE_STRATEGIES)
    shadow["logging_only_strategies"] = list(LOGGING_ONLY_STRATEGIES)
    shadow["active_execution_strategy"] = ACTIVE_STRATEGY
    shadow["enabled"] = True
    if shadow.get("elite_long_position"):
        record_execution_rejection(
            shadow,
            ELITE_STRATEGY_NAME,
            NON_ACTIVE_STRATEGY_BLOCKED,
            signal_origin="legacy_elite_long_position",
        )
        shadow["elite_long_position"] = None
    if shadow.get("shadow_probe_position"):
        record_execution_rejection(
            shadow,
            SHADOW_PROBE_STRATEGY_NAME,
            NON_ACTIVE_STRATEGY_BLOCKED,
            signal_origin="legacy_shadow_probe_position",
        )
        shadow["shadow_probe_position"] = None
    detect_execution_contamination(shadow)


def open_elite_shadow_position(config: Config, decision: EliteSignalDecision, quantity: float) -> Dict[str, Any]:
    return {
        "strategy_id": ACTIVE_STRATEGY,
        "execution_source": ACTIVE_EXECUTION_SOURCE,
        "signal_origin": ELITE_SIGNAL_ORIGIN,
        "strategy": ELITE_STRATEGY_NAME,
        "symbol": config.symbol,
        "side": "LONG",
        "entry_time": decision.timestamp,
        "entry_price": decision.close_price,
        "entry_zscore": decision.zscore,
        "entry_atr_pct": decision.atr_pct,
        "entry_atr_bucket": decision.atr_bucket,
        "entry_trend_bucket": decision.trend_bucket,
        "entry_reversion_confirmed": decision.reversion_confirmed,
        "quantity": quantity,
        "entry_notional": quantity * decision.close_price,
    }


def open_router_shadow_position(config: Config, decision: StrategySignalDecision, quantity: float) -> Dict[str, Any]:
    if decision.strategy not in EXECUTABLE_STRATEGIES:
        raise ConfigError(f"Router cannot open unsupported strategy {decision.strategy}")
    return {
        "strategy_id": decision.strategy,
        "strategy_name": decision.strategy,
        "execution_source": ACTIVE_EXECUTION_SOURCE,
        "signal_origin": ROUTER_SIGNAL_ORIGIN,
        "strategy": decision.strategy,
        "symbol": config.symbol,
        "side": decision.direction,
        "regime_at_entry": decision.regime,
        "entry_time": decision.timestamp,
        "entry_price": decision.close_price,
        "entry_zscore": decision.zscore,
        "entry_atr_pct": decision.atr_pct,
        "entry_atr_percentile": decision.atr_percentile,
        "entry_atr_bucket": decision.atr_bucket,
        "entry_trend_bucket": decision.trend_bucket,
        "entry_ema20": decision.ema20,
        "entry_ema50": decision.ema50,
        "entry_ema_state": decision.ema_state,
        "entry_reversion_confirmed": decision.reversion_confirmed,
        "entry_reason": decision.entry_reason,
        "quantity": quantity,
        "entry_notional": quantity * decision.close_price,
    }


def shadow_probe_entry_reason(config: Config, decision: EliteSignalDecision) -> Tuple[bool, str]:
    if not config.shadow_probe_enabled:
        return False, "shadow probe is disabled"
    if decision.direction == "NONE":
        return False, "z-score direction is neutral"
    if decision.direction == "SHORT" and not config.shadow_probe_allow_short:
        return False, "shadow probe short entries are disabled"
    abs_z = abs(decision.zscore)
    if not (config.shadow_probe_min_abs_z <= abs_z <= config.shadow_probe_max_abs_z):
        return False, (
            f"probe absolute z-score {abs_z:.6f} is outside "
            f"{config.shadow_probe_min_abs_z:.6f}-{config.shadow_probe_max_abs_z:.6f}"
        )
    return True, ""


def shadow_probe_router_log_reason(config: Config, decision: StrategySignalDecision) -> Tuple[bool, str]:
    if not config.shadow_probe_enabled:
        return False, "shadow probe is disabled"
    if decision.direction == "NONE":
        return False, "z-score direction is neutral"
    if decision.direction == "SHORT" and not config.shadow_probe_allow_short:
        return False, "shadow probe short entries are disabled"
    abs_z = abs(decision.zscore)
    if not (config.shadow_probe_min_abs_z <= abs_z <= config.shadow_probe_max_abs_z):
        return False, (
            f"probe absolute z-score {abs_z:.6f} is outside "
            f"{config.shadow_probe_min_abs_z:.6f}-{config.shadow_probe_max_abs_z:.6f}"
        )
    return True, "shadow probe is logging-only; router controls execution"


def open_shadow_probe_position(config: Config, decision: EliteSignalDecision, quantity: float) -> Dict[str, Any]:
    del config, decision, quantity
    raise ConfigError("SHADOW_PROBE_STRATEGY is logging-only and cannot open shadow positions")


def get_elite_exit_reason(position: Dict[str, Any], decision: EliteSignalDecision, config: Config) -> str:
    entry_price = float(position.get("entry_price", 0.0) or 0.0)
    if entry_price <= 0:
        return "invalid entry price"
    pnl_pct = (decision.close_price - entry_price) / entry_price
    if pnl_pct <= -config.stop_loss_pct:
        return "stop loss"
    if pnl_pct >= config.take_profit_pct:
        return "take profit"
    if decision.zscore >= -config.exit_z:
        return "mean reversion exit"
    return ""


def get_shadow_probe_exit_reason(position: Dict[str, Any], decision: EliteSignalDecision, config: Config) -> str:
    entry_price = float(position.get("entry_price", 0.0) or 0.0)
    if entry_price <= 0:
        return "invalid entry price"
    side = str(position.get("side", "")).upper()
    if side == "SHORT":
        pnl_pct = (entry_price - decision.close_price) / entry_price
        if pnl_pct <= -config.stop_loss_pct:
            return "stop loss"
        if pnl_pct >= config.take_profit_pct:
            return "take profit"
        if decision.zscore <= config.exit_z:
            return "mean reversion exit"
    else:
        pnl_pct = (decision.close_price - entry_price) / entry_price
        if pnl_pct <= -config.stop_loss_pct:
            return "stop loss"
        if pnl_pct >= config.take_profit_pct:
            return "take profit"
        if decision.zscore >= -config.exit_z:
            return "mean reversion exit"
    max_seconds = config.shadow_probe_max_holding_minutes * 60
    if max_seconds > 0 and holding_seconds(str(position.get("entry_time", "")), decision.timestamp) >= max_seconds:
        return "max holding time"
    return ""


def get_router_exit_reason(position: Dict[str, Any], decision: StrategySignalDecision, config: Config) -> str:
    entry_price = float(position.get("entry_price", 0.0) or 0.0)
    if entry_price <= 0:
        return "invalid entry price"
    side = str(position.get("side", "")).upper()
    strategy = str(position.get("strategy", ""))
    if side == "SHORT":
        pnl_pct = (entry_price - decision.close_price) / entry_price
        if pnl_pct <= -config.stop_loss_pct:
            return "stop loss"
        if pnl_pct >= config.take_profit_pct:
            return "take profit"
        if strategy == MEAN_REVERSION_STRATEGY_NAME and decision.zscore <= config.exit_z:
            return "mean reversion exit"
        if strategy in {TREND_FOLLOW_SHORT_STRATEGY_NAME, BREAKOUT_STRATEGY_NAME} and decision.close_price > decision.ema20:
            return "trend invalidation exit" if strategy == TREND_FOLLOW_SHORT_STRATEGY_NAME else "breakout failure exit"
    else:
        pnl_pct = (decision.close_price - entry_price) / entry_price
        if pnl_pct <= -config.stop_loss_pct:
            return "stop loss"
        if pnl_pct >= config.take_profit_pct:
            return "take profit"
        if strategy == MEAN_REVERSION_STRATEGY_NAME and decision.zscore >= -config.exit_z:
            return "mean reversion exit"
        if strategy in {TREND_FOLLOW_LONG_STRATEGY_NAME, BREAKOUT_STRATEGY_NAME} and decision.close_price < decision.ema20:
            return "trend invalidation exit" if strategy == TREND_FOLLOW_LONG_STRATEGY_NAME else "breakout failure exit"
    return ""


def parse_time_text(value: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M UTC"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def holding_seconds(entry_time: str, exit_time: str) -> float:
    entry = parse_time_text(entry_time)
    exit_dt = parse_time_text(exit_time)
    if entry is None or exit_dt is None:
        return 0.0
    return max((exit_dt - entry).total_seconds(), 0.0)


def close_elite_shadow_position(
    position: Dict[str, Any],
    decision: EliteSignalDecision,
    exit_reason: str,
    config: Config,
) -> Dict[str, Any]:
    quantity = float(position.get("quantity", 0.0) or 0.0)
    entry_price = float(position.get("entry_price", 0.0) or 0.0)
    exit_price = decision.close_price
    entry_notional = quantity * entry_price
    exit_notional = quantity * exit_price
    gross_pnl = (exit_price - entry_price) * quantity
    fees = (entry_notional + exit_notional) * config.taker_fee_rate
    slippage = (entry_notional + exit_notional) * config.shadow_slippage_bps / 10000.0
    net_pnl = gross_pnl - fees - slippage
    trade = {
        "strategy_id": ACTIVE_STRATEGY,
        "execution_source": str(position.get("execution_source", ACTIVE_EXECUTION_SOURCE)),
        "signal_origin": str(position.get("signal_origin", ELITE_SIGNAL_ORIGIN)),
        "entry_time": position.get("entry_time", ""),
        "entry_price": entry_price,
        "exit_time": decision.timestamp,
        "exit_price": exit_price,
        "entry_zscore": float(position.get("entry_zscore", 0.0) or 0.0),
        "exit_zscore": decision.zscore,
        "entry_atr_pct": float(position.get("entry_atr_pct", 0.0) or 0.0),
        "entry_atr_bucket": str(position.get("entry_atr_bucket", "")),
        "entry_trend_bucket": str(position.get("entry_trend_bucket", "")),
        "entry_reversion_confirmed": bool(position.get("entry_reversion_confirmed", False)),
        "side": str(position.get("side", "LONG")),
        "quantity": quantity,
        "entry_notional": entry_notional,
        "exit_notional": exit_notional,
        "gross_pnl": gross_pnl,
        "fees": fees,
        "slippage": slippage,
        "net_pnl": net_pnl,
        "holding_seconds": holding_seconds(str(position.get("entry_time", "")), decision.timestamp),
        "exit_reason": exit_reason,
        "result": "WIN" if net_pnl > 0 else "LOSS",
        "strategy": ELITE_STRATEGY_NAME,
        "symbol": config.symbol,
    }
    return trade


def close_router_shadow_position(
    position: Dict[str, Any],
    decision: StrategySignalDecision,
    exit_reason: str,
    config: Config,
) -> Dict[str, Any]:
    quantity = float(position.get("quantity", 0.0) or 0.0)
    entry_price = float(position.get("entry_price", 0.0) or 0.0)
    exit_price = decision.close_price
    side = str(position.get("side", "LONG")).upper()
    entry_notional = quantity * entry_price
    exit_notional = quantity * exit_price
    if side == "SHORT":
        gross_pnl = (entry_price - exit_price) * quantity
    else:
        gross_pnl = (exit_price - entry_price) * quantity
    fees = (entry_notional + exit_notional) * config.taker_fee_rate
    slippage = (entry_notional + exit_notional) * config.shadow_slippage_bps / 10000.0
    net_pnl = gross_pnl - fees - slippage
    strategy = str(position.get("strategy", "UNKNOWN"))
    return {
        "strategy_id": strategy,
        "strategy_name": strategy,
        "execution_source": str(position.get("execution_source", ACTIVE_EXECUTION_SOURCE)),
        "signal_origin": str(position.get("signal_origin", ROUTER_SIGNAL_ORIGIN)),
        "regime_at_entry": str(position.get("regime_at_entry", "")),
        "entry_time": position.get("entry_time", ""),
        "entry_price": entry_price,
        "exit_time": decision.timestamp,
        "exit_price": exit_price,
        "entry_zscore": float(position.get("entry_zscore", 0.0) or 0.0),
        "exit_zscore": decision.zscore,
        "entry_atr_pct": float(position.get("entry_atr_pct", 0.0) or 0.0),
        "entry_atr_percentile": float(position.get("entry_atr_percentile", 0.0) or 0.0),
        "entry_atr_bucket": str(position.get("entry_atr_bucket", "")),
        "entry_trend_bucket": str(position.get("entry_trend_bucket", "")),
        "entry_ema20": float(position.get("entry_ema20", 0.0) or 0.0),
        "entry_ema50": float(position.get("entry_ema50", 0.0) or 0.0),
        "entry_ema_state": str(position.get("entry_ema_state", "")),
        "entry_reversion_confirmed": bool(position.get("entry_reversion_confirmed", False)),
        "entry_reason": str(position.get("entry_reason", "")),
        "side": side,
        "quantity": quantity,
        "entry_notional": entry_notional,
        "exit_notional": exit_notional,
        "gross_pnl": gross_pnl,
        "fees": fees,
        "slippage": slippage,
        "net_pnl": net_pnl,
        "holding_seconds": holding_seconds(str(position.get("entry_time", "")), decision.timestamp),
        "exit_reason": exit_reason,
        "result": "WIN" if net_pnl > 0 else "LOSS",
        "strategy": strategy,
        "symbol": config.symbol,
    }


def close_shadow_probe_position(
    position: Dict[str, Any],
    decision: EliteSignalDecision,
    exit_reason: str,
    config: Config,
) -> Dict[str, Any]:
    del position, decision, exit_reason, config
    raise ConfigError("SHADOW_PROBE_STRATEGY is logging-only and cannot close shadow positions")


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def append_csv(path: Path, row: Dict[str, Any], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    if exists:
        try:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.reader(handle)
                existing_header = next(reader, [])
            if list(existing_header) != list(fieldnames):
                archived = path.with_name(f"{path.stem}.legacy_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}{path.suffix}")
                path.replace(archived)
                exists = False
                LOGGER.warning("Archived legacy CSV schema from %s to %s", path, archived)
        except Exception as exc:
            LOGGER.warning("Could not inspect CSV schema for %s: %s", path, exc)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def append_signal_logs(config: Config, row: Dict[str, Any]) -> None:
    jsonl_path = Path(config.shadow_signal_log_file)
    csv_path = jsonl_path.with_suffix(".csv")
    append_jsonl(jsonl_path, row)
    append_csv(csv_path, row, SIGNAL_FIELDNAMES)
    if row.get("trade_decision") == "REJECT":
        LOGGER.info(
            "Router signal rejected: strategy=%s regime=%s failed_filter=%s reason=%s zscore=%.6f atr_bucket=%s",
            row.get("strategy"),
            row.get("regime"),
            row.get("failed_filter"),
            row.get("rejection_reason"),
            float(row.get("zscore", 0.0) or 0.0),
            row.get("atr_bucket"),
        )
    else:
        LOGGER.info(
            "Router signal accepted: strategy=%s regime=%s zscore=%.6f close=%.8f atr_bucket=%s",
            row.get("strategy"),
            row.get("regime"),
            float(row.get("zscore", 0.0) or 0.0),
            float(row.get("close_price", 0.0) or 0.0),
            row.get("atr_bucket"),
        )


def append_trade_logs(config: Config, trade: Dict[str, Any]) -> None:
    jsonl_path = Path(config.shadow_trade_log_file)
    csv_path = jsonl_path.with_suffix(".csv")
    append_jsonl(jsonl_path, trade)
    append_csv(csv_path, trade, TRADE_FIELDNAMES)
    LOGGER.info(
        "Router shadow trade closed: strategy=%s regime=%s result=%s net_pnl=%.8f reason=%s",
        trade.get("strategy"),
        trade.get("regime_at_entry"),
        trade.get("result"),
        float(trade.get("net_pnl", 0.0) or 0.0),
        trade.get("exit_reason"),
    )


def save_shadow_state(config: Config, shadow: Dict[str, Any]) -> None:
    path = Path(config.shadow_state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(shadow, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")
    os.replace(temp_path, path)


def best_and_worst_streak(trades: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    best = 0
    worst = 0
    current_win = 0
    current_loss = 0
    for trade in trades:
        if float(trade.get("net_pnl", 0.0) or 0.0) > 0:
            current_win += 1
            current_loss = 0
        else:
            current_loss += 1
            current_win = 0
        best = max(best, current_win)
        worst = max(worst, current_loss)
    return best, worst


def compute_max_drawdown(trades: Sequence[Dict[str, Any]]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for trade in trades:
        equity += float(trade.get("net_pnl", 0.0) or 0.0)
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def compute_net_profit_factor(trades: Sequence[Dict[str, Any]]) -> Optional[float]:
    net_wins = sum(max(float(trade.get("net_pnl", 0.0) or 0.0), 0.0) for trade in trades)
    net_losses = abs(sum(min(float(trade.get("net_pnl", 0.0) or 0.0), 0.0) for trade in trades))
    if net_losses > 0:
        return net_wins / net_losses
    if net_wins > 0:
        return 999999999.0
    return None


def summarize_trade_group(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    trade_list = list(trades)
    total = len(trade_list)
    wins = [trade for trade in trade_list if float(trade.get("net_pnl", 0.0) or 0.0) > 0]
    losses = [trade for trade in trade_list if float(trade.get("net_pnl", 0.0) or 0.0) <= 0]
    net_pnl = sum(float(trade.get("net_pnl", 0.0) or 0.0) for trade in trade_list)
    gross_pnl = sum(float(trade.get("gross_pnl", 0.0) or 0.0) for trade in trade_list)
    gross_win = sum(max(float(trade.get("gross_pnl", 0.0) or 0.0), 0.0) for trade in trade_list)
    gross_loss = abs(sum(min(float(trade.get("gross_pnl", 0.0) or 0.0), 0.0) for trade in trade_list))
    profit_factor = None
    if gross_loss > 0:
        profit_factor = gross_win / gross_loss
    elif gross_win > 0:
        profit_factor = 999999999.0
    return {
        "trades": total,
        "win_rate": len(wins) / total if total else None,
        "net_pnl": net_pnl,
        "gross_pnl": gross_pnl,
        "fees": sum(float(trade.get("fees", 0.0) or 0.0) for trade in trade_list),
        "slippage": sum(float(trade.get("slippage", 0.0) or 0.0) for trade in trade_list),
        "expectancy": net_pnl / total if total else None,
        "profit_factor": profit_factor,
        "net_profit_factor": compute_net_profit_factor(trade_list),
        "max_drawdown": compute_max_drawdown(trade_list),
        "average_win": sum(float(trade.get("net_pnl", 0.0) or 0.0) for trade in wins) / len(wins) if wins else None,
        "average_loss": sum(float(trade.get("net_pnl", 0.0) or 0.0) for trade in losses) / len(losses) if losses else None,
    }


def grouped_trade_summary(trades: Sequence[Dict[str, Any]], field: str) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for trade in trades:
        key = str(trade.get(field, "") or "UNKNOWN")
        groups.setdefault(key, []).append(trade)
    return {key: summarize_trade_group(value) for key, value in sorted(groups.items())}


def regime_strategy_pair_summary(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for trade in trades:
        regime = str(trade.get("regime_at_entry", "") or "UNKNOWN")
        strategy = shadow_trade_strategy(trade) or "UNKNOWN"
        groups.setdefault(f"{regime}|{strategy}", []).append(trade)
    summaries = {key: summarize_trade_group(value) for key, value in sorted(groups.items())}
    ranked = sorted(
        summaries.items(),
        key=lambda item: (
            -1 if item[1].get("expectancy") is None else float(item[1].get("expectancy") or 0.0),
            int(item[1].get("trades", 0) or 0),
        ),
        reverse=True,
    )
    return {
        "pairs": summaries,
        "best_regime_strategy_pair": ranked[0][0] if ranked else None,
        "worst_regime_strategy_pair": ranked[-1][0] if ranked else None,
    }


def build_decision_report(trades: Sequence[Dict[str, Any]], max_drawdown: Optional[float] = None) -> Dict[str, Any]:
    trade_list = list(trades)
    total = len(trade_list)
    wins = [trade for trade in trade_list if float(trade.get("net_pnl", 0.0) or 0.0) > 0]
    losses = [trade for trade in trade_list if float(trade.get("net_pnl", 0.0) or 0.0) <= 0]
    net_pnl = sum(float(trade.get("net_pnl", 0.0) or 0.0) for trade in trade_list)
    gross_pnl = sum(float(trade.get("gross_pnl", 0.0) or 0.0) for trade in trade_list)
    fees = sum(float(trade.get("fees", 0.0) or 0.0) for trade in trade_list)
    slippage = sum(float(trade.get("slippage", 0.0) or 0.0) for trade in trade_list)
    gross_win = sum(max(float(trade.get("gross_pnl", 0.0) or 0.0), 0.0) for trade in trade_list)
    gross_loss = abs(sum(min(float(trade.get("gross_pnl", 0.0) or 0.0), 0.0) for trade in trade_list))
    profit_factor = None
    if gross_loss > 0:
        profit_factor = gross_win / gross_loss
    elif gross_win > 0:
        profit_factor = 999999999.0
    expectancy = net_pnl / total if total else None
    drawdown = compute_max_drawdown(trade_list) if max_drawdown is None else float(max_drawdown)
    average_win = (
        sum(float(trade.get("net_pnl", 0.0) or 0.0) for trade in wins) / len(wins)
        if wins
        else None
    )
    average_loss = (
        sum(float(trade.get("net_pnl", 0.0) or 0.0) for trade in losses) / len(losses)
        if losses
        else None
    )
    best_streak, worst_streak = best_and_worst_streak(trade_list)
    strategy_summary = grouped_trade_summary(trade_list, "strategy")
    regime_summary = grouped_trade_summary(trade_list, "regime_at_entry")
    pair_summary = regime_strategy_pair_summary(trade_list)
    if expectancy is not None and expectancy <= 0:
        viability_status = "NOT_VIABLE"
    elif (
        expectancy is not None
        and expectancy > 0
        and profit_factor is not None
        and profit_factor > 1.1
        and total >= 100
        and drawdown < 0.2
    ):
        viability_status = "CANDIDATE_FOR_MICRO_TEST"
    else:
        viability_status = "INSUFFICIENT_DATA"
    return {
        "generated_at": utc_now_text(),
        "strategy": ACTIVE_STRATEGY,
        "active_execution_strategy": ACTIVE_STRATEGY,
        "dataset_valid": True,
        "execution_contamination_detected": False,
        "contaminated_trade_count": 0,
        "total_trades": total,
        "regime_distribution": {key: value["trades"] for key, value in regime_summary.items()},
        "strategy_performance": strategy_summary,
        "regime_performance": regime_summary,
        "strategy_performance_per_regime": pair_summary["pairs"],
        "best_regime_strategy_pair": pair_summary["best_regime_strategy_pair"],
        "worst_regime_strategy_pair": pair_summary["worst_regime_strategy_pair"],
        "win_rate": len(wins) / total if total else None,
        "net_pnl": net_pnl,
        "gross_pnl": gross_pnl,
        "fees": fees,
        "slippage": slippage,
        "expectancy_per_trade": expectancy,
        "profit_factor": profit_factor,
        "net_profit_factor": compute_net_profit_factor(trade_list),
        "max_drawdown": drawdown,
        "average_win": average_win,
        "average_loss": average_loss,
        "best_streak": best_streak,
        "worst_streak": worst_streak,
        "viability_status": viability_status,
    }


def recompute_shadow_metrics(shadow: Dict[str, Any], config: Optional[Config] = None) -> None:
    ensure_shadow_schema(shadow)
    detect_execution_contamination(shadow)
    if config is None:
        config = Config()
    trades = list(shadow.get("trades") or [])
    report = build_decision_report(trades)
    if not shadow.get("dataset_valid", True):
        report["dataset_valid"] = False
        report["execution_contamination_detected"] = True
        report["contaminated_trade_count"] = int(shadow.get("contaminated_trade_count", 0) or 0)
        report["viability_status"] = INVALID_CONTAMINATED_DATASET
    shadow["completed_trades"] = len(trades)
    shadow["win_rate"] = report["win_rate"]
    shadow["net_pnl_usdt"] = report["net_pnl"]
    shadow["gross_pnl_usdt"] = report["gross_pnl"]
    shadow["fees_usdt"] = report["fees"]
    shadow["slippage_usdt"] = report["slippage"]
    shadow["expectancy_usdt"] = report["expectancy_per_trade"]
    shadow["profit_factor"] = report["profit_factor"]
    shadow["max_drawdown_usdt"] = report["max_drawdown"]
    shadow["average_win_usdt"] = report["average_win"]
    shadow["average_loss_usdt"] = report["average_loss"]
    shadow["best_streak"] = report["best_streak"]
    shadow["worst_streak"] = report["worst_streak"]
    shadow["viability_status"] = report["viability_status"]
    shadow["regime_distribution"] = report["regime_distribution"]
    shadow["strategy_performance"] = report["strategy_performance"]
    shadow["regime_performance"] = report["regime_performance"]
    shadow["strategy_performance_per_regime"] = report["strategy_performance_per_regime"]
    shadow["best_regime_strategy_pair"] = report["best_regime_strategy_pair"]
    shadow["worst_regime_strategy_pair"] = report["worst_regime_strategy_pair"]
    time_window_analytics = build_time_window_analytics(trades, config)
    shadow["time_window_performance"] = time_window_analytics["windows"]
    shadow["profitable_windows"] = time_window_analytics["profitable_windows"]
    shadow["worst_windows"] = time_window_analytics["worst_windows"]
    shadow["regime_time_window_performance"] = build_regime_time_window_performance(trades, config)


def shadow_artifact_dir(config: Config) -> Path:
    paths = [
        Path(config.shadow_state_file),
        Path(config.shadow_signal_log_file),
        Path(config.shadow_trade_log_file),
    ]
    for path in paths:
        if path.parent != Path("."):
            return path.parent
    return Path(".")


def write_decision_report(config: Config, report: Dict[str, Any]) -> None:
    reports_dir = shadow_artifact_dir(config) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    latest_path = reports_dir / "latest.json"
    history_path = reports_dir / f"history_{report['total_trades']}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    payload = json.dumps(report, indent=2, sort_keys=True, allow_nan=False)
    latest_path.write_text(payload, encoding="utf-8")
    history_path.write_text(payload, encoding="utf-8")


def maybe_write_decision_report(shadow: Dict[str, Any], config: Config) -> None:
    ensure_shadow_schema(shadow)
    completed = int(shadow.get("completed_trades", 0) or 0)
    last = int(shadow.get("last_decision_report_completed_trades", 0) or 0)
    interval = max(1, config.decision_report_every_completed_trades)
    latest_path = shadow_artifact_dir(config) / "reports" / "latest.json"
    report_missing = not latest_path.exists()
    if completed <= 0 or completed % interval != 0 or (completed == last and not report_missing):
        return
    report = build_decision_report(shadow.get("trades") or [])
    if not shadow.get("dataset_valid", True):
        report["dataset_valid"] = False
        report["execution_contamination_detected"] = True
        report["contaminated_trade_count"] = int(shadow.get("contaminated_trade_count", 0) or 0)
        report["viability_status"] = INVALID_CONTAMINATED_DATASET
    write_decision_report(config, report)
    shadow["latest_decision_report"] = report
    shadow["last_decision_report_completed_trades"] = completed


def fetch_balance(client: Any, config: Config) -> float:
    if not config.api_key or not config.api_secret:
        return 0.0
    try:
        balances = retry_api_call(
            "Fetch futures account balance",
            client.futures_account_balance,
            attempts=config.retry_attempts,
        )
        for row in balances:
            if row.get("asset") == "USDT":
                return float(row.get("balance", 0.0) or 0.0)
    except Exception as exc:
        LOGGER.warning("Could not fetch USDT futures balance: %s", exc)
    return 0.0


def required_kline_counts(config: Config) -> Tuple[int, int]:
    primary_count = max(
        config.lookback + config.atr_lookback + 20,
        config.regime_slow_ema_period + config.regime_slope_lookback + 20,
        config.breakout_lookback + 5,
        140,
    )
    trend_count = max(config.trend_ema_period + config.trend_slope_lookback + 20, 80)
    return primary_count, trend_count


def evaluate_router_candle(
    config: Config,
    state: RuntimeState,
    filters: ExchangeFilters,
    klines: pd.DataFrame,
    trend_klines: pd.DataFrame,
    balance: float,
    allowed_pairs: Optional[Set[str]] = None,
    include_market_gate_analytics: bool = True,
) -> StrategySignalDecision:
    z_result = compute_zscore(klines, config.lookback)
    regime = detect_market_regime(klines, config)
    trend_slope_pct, trend_bucket_value = compute_trend_bucket(trend_klines, config)
    state.update_market_snapshot(
        config,
        balance,
        z_result,
        regime.atr_pct,
        regime.atr_bucket,
        trend_bucket_value,
        trend_slope_pct,
    )
    recent_high, recent_low = recent_breakout_bounds(klines, config)
    decision = route_strategy(
        config=config,
        regime=regime,
        timestamp=z_result.latest_closed_time,
        close_price=z_result.latest_close,
        previous_close=z_result.previous_close,
        zscore=z_result.zscore,
        previous_zscore=z_result.previous_zscore,
        recent_high=recent_high,
        recent_low=recent_low,
    )
    if allowed_pairs is not None and decision.trade_decision == "ENTER":
        pair = f"{decision.regime}|{decision.strategy}"
        if pair not in allowed_pairs:
            decision = replace(
                decision,
                trade_decision="REJECT",
                failed_filter="research_pair_gate",
                rejection_reason=f"RESEARCH_PAIR_BLOCKED:{pair}",
            )
    if include_market_gate_analytics:
        market_gate = evaluate_market_gate(
            klines,
            config,
            regime,
            z_result.latest_closed_time,
            state.snapshot()["shadow"],
        )
        decision = attach_market_gate_to_decision(decision, market_gate)
    state.process_router_signal(config, decision, filters)
    return decision


def run_once(client: Any, config: Config, state: RuntimeState) -> None:
    state.update_config(config)
    if state.is_paused():
        state.set_status("paused")
        return

    filters = get_exchange_filters(client, config.symbol, config)
    required_klines, required_trend_klines = required_kline_counts(config)
    klines = fetch_klines(client, config.symbol, config.interval, required_klines)
    trend_klines = fetch_klines(
        client,
        config.symbol,
        config.trend_interval,
        required_trend_klines,
    )
    balance = fetch_balance(client, config)
    evaluate_router_candle(config, state, filters, klines, trend_klines, balance)
    state.clear_error()
    state.set_status("running")


def resolve_app_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parent / path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")
    os.replace(temp_path, path)


def write_replay_trades(path: Path, trades: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRADE_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade)
    os.replace(temp_path, path)


def aligned_replay_end_ms(interval: str, end_time: Optional[str] = None) -> int:
    period_seconds = interval_seconds(interval)
    if period_seconds is None:
        raise ConfigError(f"Unsupported replay interval: {interval}")
    interval_ms = period_seconds * 1000
    if end_time:
        normalized = end_time.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ConfigError("--end-time must be an ISO-8601 UTC timestamp") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        boundary_ms = int(parsed.astimezone(timezone.utc).timestamp() * 1000)
    else:
        boundary_ms = int(time.time() * 1000)
    return (boundary_ms // interval_ms) * interval_ms


def replay_period_boundaries(days: int, end_exclusive_ms: int) -> Dict[str, Tuple[int, int]]:
    if days < 4:
        raise ConfigError("Historical replay requires at least 4 days")
    day_ms = 24 * 60 * 60 * 1000
    train_days = max(1, int(days * REPLAY_TRAIN_SHARE))
    validation_days = max(1, int(days * REPLAY_VALIDATION_SHARE))
    test_days = days - train_days - validation_days
    if test_days < 1:
        raise ConfigError("Historical replay split leaves no test period")
    train_start = end_exclusive_ms - days * day_ms
    train_end = train_start + train_days * day_ms
    validation_end = train_end + validation_days * day_ms
    return {
        "train": (train_start, train_end),
        "validation": (train_end, validation_end),
        "test": (validation_end, end_exclusive_ms),
    }


def resample_closed_klines(frame: pd.DataFrame, source_interval: str, target_interval: str) -> pd.DataFrame:
    source_seconds = interval_seconds(source_interval)
    target_seconds = interval_seconds(target_interval)
    if source_seconds is None or target_seconds is None or target_seconds % source_seconds != 0:
        raise ConfigError("Replay trend interval must be an exact multiple of the primary interval")
    ratio = target_seconds // source_seconds
    target_ms = target_seconds * 1000
    working = frame[HISTORICAL_KLINE_COLUMNS].copy()
    working["bucket"] = working["open_time"].astype("int64") // target_ms
    grouped = working.groupby("bucket", sort=True)
    result = grouped.agg(
        open_time=("open_time", "min"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        close_time=("close_time", "max"),
        candle_count=("open_time", "count"),
    ).reset_index(drop=True)
    expected_open = (result["open_time"].astype("int64") // target_ms) * target_ms
    expected_close = expected_open + target_ms - 1
    complete = (
        (result["candle_count"] == ratio)
        & (result["open_time"].astype("int64") == expected_open)
        & (result["close_time"].astype("int64") == expected_close)
    )
    result = result.loc[complete, HISTORICAL_KLINE_COLUMNS].copy()
    result["open_time"] = result["open_time"].astype("int64")
    result["close_time"] = result["close_time"].astype("int64")
    return result.reset_index(drop=True)


def replay_segment(
    config: Config,
    filters: ExchangeFilters,
    all_klines: pd.DataFrame,
    segment_start_ms: int,
    segment_end_ms: int,
    allowed_pairs: Optional[Set[str]],
) -> Dict[str, Any]:
    primary_count, trend_count = required_kline_counts(config)
    trend_frame = resample_closed_klines(all_klines, config.interval, config.trend_interval)
    trend_close_times = trend_frame["close_time"].astype("int64").to_numpy()
    close_times = all_klines["close_time"].astype("int64").to_numpy()
    indexes = np.flatnonzero((close_times >= segment_start_ms) & (close_times < segment_end_ms))
    state = RuntimeState(state_file="", persist_events=False)
    state.update_config(config)
    evaluated = 0
    last_decision: Optional[StrategySignalDecision] = None
    for position, candle_index_value in enumerate(indexes, start=1):
        candle_index = int(candle_index_value)
        if candle_index + 1 < primary_count:
            continue
        current_close_time = int(close_times[candle_index])
        trend_end = int(np.searchsorted(trend_close_times, current_close_time, side="right"))
        if trend_end < trend_count:
            continue
        primary_window = all_klines.iloc[candle_index - primary_count + 1 : candle_index + 1].reset_index(drop=True)
        trend_window = trend_frame.iloc[trend_end - trend_count : trend_end].reset_index(drop=True)
        last_decision = evaluate_router_candle(
            config,
            state,
            filters,
            primary_window,
            trend_window,
            balance=0.0,
            allowed_pairs=allowed_pairs,
            include_market_gate_analytics=False,
        )
        evaluated += 1
        if position % 10000 == 0:
            LOGGER.info(
                "Historical replay progress evaluated=%s total=%s timestamp=%s",
                evaluated,
                len(indexes),
                last_decision.timestamp,
            )
    trades = list(state.shadow.get("trades") or [])
    report = build_decision_report(trades)
    report["time_window_performance"] = build_time_window_analytics(trades, config)
    report["segment_start"] = candle_time_text(segment_start_ms)
    report["segment_end_exclusive"] = candle_time_text(segment_end_ms)
    report["evaluated_candles"] = evaluated
    report["allowed_regime_strategy_pairs"] = sorted(allowed_pairs) if allowed_pairs is not None else None
    report["open_position_excluded"] = bool(state.shadow.get("router_position"))
    report["last_evaluated_timestamp"] = last_decision.timestamp if last_decision is not None else None
    return {"report": report, "trades": trades}


def skipped_replay_segment(
    config: Config,
    segment_start_ms: int,
    segment_end_ms: int,
    allowed_pairs: Set[str],
    reason: str,
) -> Dict[str, Any]:
    report = build_decision_report([])
    report["time_window_performance"] = build_time_window_analytics([], config)
    report["segment_start"] = candle_time_text(segment_start_ms)
    report["segment_end_exclusive"] = candle_time_text(segment_end_ms)
    report["evaluated_candles"] = 0
    report["allowed_regime_strategy_pairs"] = sorted(allowed_pairs)
    report["open_position_excluded"] = False
    report["last_evaluated_timestamp"] = None
    report["skipped_reason"] = reason
    return {"report": report, "trades": []}


def select_candidate_pairs(
    trades: Sequence[Dict[str, Any]],
    minimum_trades: int,
    minimum_net_profit_factor: float = REPLAY_MIN_NET_PROFIT_FACTOR,
) -> List[Dict[str, Any]]:
    summaries = regime_strategy_pair_summary(trades)["pairs"]
    candidates: List[Dict[str, Any]] = []
    for pair, summary in summaries.items():
        expectancy = summary.get("expectancy")
        net_profit_factor = summary.get("net_profit_factor")
        if int(summary.get("trades", 0) or 0) < minimum_trades:
            continue
        if expectancy is None or float(expectancy) <= 0:
            continue
        if net_profit_factor is None or float(net_profit_factor) <= minimum_net_profit_factor:
            continue
        candidates.append({"pair": pair, **summary})
    candidates.sort(
        key=lambda row: (float(row.get("expectancy") or 0.0), int(row.get("trades") or 0)),
        reverse=True,
    )
    return candidates


def classify_replay_viability(report: Dict[str, Any], candidate_pairs: Sequence[Dict[str, Any]]) -> str:
    if not candidate_pairs:
        return "NO_VALIDATED_CANDIDATE"
    trades = int(report.get("total_trades", 0) or 0)
    expectancy = report.get("expectancy_per_trade")
    net_profit_factor = report.get("net_profit_factor")
    drawdown = float(report.get("max_drawdown", 0.0) or 0.0)
    if expectancy is None or float(expectancy) <= 0:
        return "NOT_VIABLE"
    if net_profit_factor is None or float(net_profit_factor) <= REPLAY_MIN_NET_PROFIT_FACTOR:
        return "NOT_VIABLE"
    if trades < REPLAY_MIN_TEST_TRADES or drawdown >= REPLAY_MAX_DRAWDOWN_USDT:
        return "INSUFFICIENT_OUT_OF_SAMPLE_DATA"
    return "CANDIDATE_FOR_FORWARD_SHADOW"


def build_public_replay_client(config: Config) -> Any:
    if Client is None:
        raise ConfigError("python-binance is required. Install it with: pip install python-binance")
    kwargs = {
        "testnet": False,
        "requests_params": {"timeout": config.api_timeout_seconds},
    }
    try:
        return Client(None, None, ping=False, **kwargs)
    except TypeError:
        return Client(None, None, **kwargs)


def run_historical_replay(
    config: Config,
    days: int,
    end_time: Optional[str],
    cache_file: str,
    output_dir_value: str,
) -> Dict[str, Any]:
    if config.live_trading or not config.dry_run or not config.shadow_mode:
        raise ConfigError("Historical replay requires LIVE_TRADING=false, DRY_RUN=true, and SHADOW_MODE=true")
    replay_config = replace(config, market_gate_enforced=False)
    end_exclusive_ms = aligned_replay_end_ms(config.interval, end_time)
    periods = replay_period_boundaries(days, end_exclusive_ms)
    primary_count, trend_count = required_kline_counts(config)
    primary_seconds = interval_seconds(config.interval)
    trend_seconds = interval_seconds(config.trend_interval)
    if primary_seconds is None or trend_seconds is None:
        raise ConfigError("Replay intervals are invalid")
    warmup_ms = max(primary_count * primary_seconds, trend_count * trend_seconds) * 1000
    download_start_ms = periods["train"][0] - warmup_ms - 2 * trend_seconds * 1000
    cache_path = resolve_app_path(cache_file)
    output_dir = resolve_app_path(output_dir_value)
    client = build_public_replay_client(config)
    filters = get_exchange_filters(client, config.symbol, config)
    all_klines = ensure_historical_kline_cache(
        client,
        replay_config,
        download_start_ms,
        end_exclusive_ms - 1,
        cache_path,
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / "runs" / run_id
    LOGGER.info("Historical replay train segment started")
    train = replay_segment(replay_config, filters, all_klines, *periods["train"], allowed_pairs=None)
    train_candidates = select_candidate_pairs(
        train["trades"],
        minimum_trades=REPLAY_MIN_TRAIN_PAIR_TRADES,
    )
    train_pairs = {str(row["pair"]) for row in train_candidates}

    LOGGER.info("Historical replay validation segment started candidate_pairs=%s", len(train_pairs))
    if train_pairs:
        validation = replay_segment(
            replay_config,
            filters,
            all_klines,
            *periods["validation"],
            allowed_pairs=train_pairs,
        )
    else:
        validation = skipped_replay_segment(
            replay_config,
            *periods["validation"],
            allowed_pairs=set(),
            reason="NO_TRAIN_CANDIDATE",
        )
    validated_candidates = select_candidate_pairs(
        validation["trades"],
        minimum_trades=REPLAY_MIN_VALIDATION_PAIR_TRADES,
    )
    validated_pairs = {str(row["pair"]) for row in validated_candidates}

    LOGGER.info("Historical replay test segment started validated_pairs=%s", len(validated_pairs))
    if validated_pairs:
        test = replay_segment(
            replay_config,
            filters,
            all_klines,
            *periods["test"],
            allowed_pairs=validated_pairs,
        )
    else:
        test = skipped_replay_segment(
            replay_config,
            *periods["test"],
            allowed_pairs=set(),
            reason="NO_VALIDATED_CANDIDATE",
        )
    viability_status = classify_replay_viability(test["report"], validated_candidates)
    summary = {
        "engine": "historical_regime_strategy_walk_forward_v1",
        "generated_at": utc_now_text(),
        "run_id": run_id,
        "symbol": config.symbol,
        "interval": config.interval,
        "trend_interval": config.trend_interval,
        "days": days,
        "shadow_mode_only": True,
        "dry_run": True,
        "live_trading": False,
        "account_state_changed": False,
        "market_gate_enforced_during_replay": False,
        "source_sha256": file_sha256(Path(__file__).resolve()),
        "cache_file": str(cache_path),
        "cache_sha256": file_sha256(cache_path),
        "cached_candles": len(all_klines),
        "periods": {
            name: {
                "start": candle_time_text(start),
                "end_exclusive": candle_time_text(end),
            }
            for name, (start, end) in periods.items()
        },
        "selection_rules": {
            "train_minimum_pair_trades": REPLAY_MIN_TRAIN_PAIR_TRADES,
            "validation_minimum_pair_trades": REPLAY_MIN_VALIDATION_PAIR_TRADES,
            "minimum_net_profit_factor": REPLAY_MIN_NET_PROFIT_FACTOR,
            "test_minimum_trades": REPLAY_MIN_TEST_TRADES,
            "maximum_drawdown_usdt": REPLAY_MAX_DRAWDOWN_USDT,
        },
        "train_candidates": train_candidates,
        "validated_candidates": validated_candidates,
        "train": train["report"],
        "validation": validation["report"],
        "test": test["report"],
        "viability_status": viability_status,
        "recommendation": (
            "CONTINUE_FORWARD_SHADOW_VALIDATION"
            if viability_status == "CANDIDATE_FOR_FORWARD_SHADOW"
            else "DO_NOT_TRADE_LIVE"
        ),
    }
    write_replay_trades(run_dir / "trades_train.csv", train["trades"])
    write_replay_trades(run_dir / "trades_validation.csv", validation["trades"])
    write_replay_trades(run_dir / "trades_test.csv", test["trades"])
    write_json_atomic(run_dir / "summary.json", summary)
    write_json_atomic(output_dir / "latest.json", summary)
    return summary


def parse_replay_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic historical regime and strategy validation")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--cache-file", default="historical_data/SOLUSDT_1m.csv")
    parser.add_argument("--output-dir", default="historical_replay")
    return parser.parse_args(list(arguments))


def historical_replay_main(arguments: Sequence[str]) -> None:
    args = parse_replay_args(arguments)
    config = load_config()
    summary = run_historical_replay(
        config,
        days=args.days,
        end_time=args.end_time,
        cache_file=args.cache_file,
        output_dir_value=args.output_dir,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False))


def candidate_true_range(frame: pd.DataFrame) -> pd.Series:
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    previous_close = frame["close"].astype(float).shift(1)
    return pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)


def build_candidate_feature_frame(
    all_klines: pd.DataFrame,
    params: CandidateReplayParameters,
    source_interval: str = "1m",
    trend_interval: str = "15m",
) -> pd.DataFrame:
    primary = all_klines[HISTORICAL_KLINE_COLUMNS].copy().sort_values("close_time").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume"):
        primary[column] = pd.to_numeric(primary[column], errors="coerce")
    primary["atr_1m"] = candidate_true_range(primary).rolling(params.atr_1m_period).mean()
    primary["atr_1m_pct"] = primary["atr_1m"] / primary["close"]
    primary["atr_1m_percentile"] = (
        primary["atr_1m_pct"]
        .rolling(params.atr_percentile_window, min_periods=params.atr_percentile_window)
        .rank(pct=True)
    )

    trend = resample_closed_klines(primary, source_interval, trend_interval)
    for column in ("open", "high", "low", "close", "volume"):
        trend[column] = pd.to_numeric(trend[column], errors="coerce")
    trend["trend_ema20"] = trend["close"].ewm(span=params.trend_fast_ema_period, adjust=False).mean()
    trend["trend_ema50"] = trend["close"].ewm(span=params.trend_slow_ema_period, adjust=False).mean()
    trend["trend_atr_15m"] = candidate_true_range(trend).rolling(params.trend_atr_period).mean()
    trend["trend_ema_spread_pct"] = (
        (trend["trend_ema20"] - trend["trend_ema50"]).abs() / trend["close"]
    )
    trend["trend_side"] = np.select(
        [trend["trend_ema20"] > trend["trend_ema50"], trend["trend_ema20"] < trend["trend_ema50"]],
        [1, -1],
        default=0,
    ).astype(int)
    side_groups = trend["trend_side"].ne(trend["trend_side"].shift()).cumsum()
    trend["trend_persistence"] = trend.groupby(side_groups).cumcount() + 1
    trend.loc[trend["trend_side"] == 0, "trend_persistence"] = 0
    ema_change = trend["trend_ema20"].diff()
    trend["long_slope_consistency"] = (
        ema_change.gt(0).rolling(params.slope_consistency_window).mean()
    )
    trend["short_slope_consistency"] = (
        ema_change.lt(0).rolling(params.slope_consistency_window).mean()
    )
    trend["trend_feature_close_time"] = trend["close_time"].astype("int64")

    feature_columns = [
        "close_time",
        "trend_feature_close_time",
        "trend_ema20",
        "trend_ema50",
        "trend_atr_15m",
        "trend_ema_spread_pct",
        "trend_side",
        "trend_persistence",
        "long_slope_consistency",
        "short_slope_consistency",
    ]
    features = pd.merge_asof(
        primary.sort_values("close_time"),
        trend[feature_columns].sort_values("close_time"),
        on="close_time",
        direction="backward",
        allow_exact_matches=True,
    )
    return features.reset_index(drop=True)


def candidate_pullback_distance_atr(row: Any, direction: str) -> Optional[float]:
    try:
        ema20 = float(row["trend_ema20"])
        atr_15m = float(row["trend_atr_15m"])
        extreme = float(row["low"] if direction == "LONG" else row["high"])
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (ema20, atr_15m, extreme)) or atr_15m <= 0:
        return None
    return abs(extreme - ema20) / atr_15m


def evaluate_candidate_signal(
    current: Any,
    previous: Any,
    direction: str,
    params: CandidateReplayParameters,
) -> Tuple[bool, str, Optional[float]]:
    if direction not in CANDIDATE_DIRECTIONS:
        raise ConfigError(f"Unsupported candidate direction: {direction}")
    try:
        current_close_time = int(current["close_time"])
        trend_close_time = int(current["trend_feature_close_time"])
        trend_side = int(current["trend_side"])
        persistence = int(current["trend_persistence"])
        spread_pct = float(current["trend_ema_spread_pct"])
        atr_percentile = float(current["atr_1m_percentile"])
        ema20 = float(current["trend_ema20"])
        close_price = float(current["close"])
        previous_high = float(previous["high"])
        previous_low = float(previous["low"])
        slope_consistency = float(
            current["long_slope_consistency" if direction == "LONG" else "short_slope_consistency"]
        )
    except (KeyError, TypeError, ValueError):
        return False, "MISSING_FEATURES", None
    required_values = (
        spread_pct,
        atr_percentile,
        ema20,
        close_price,
        previous_high,
        previous_low,
        slope_consistency,
    )
    if not all(math.isfinite(value) for value in required_values):
        return False, "MISSING_FEATURES", None
    if trend_close_time > current_close_time:
        return False, "FUTURE_TREND_CANDLE_BLOCKED", None
    expected_side = 1 if direction == "LONG" else -1
    if trend_side != expected_side:
        return False, "TREND_DIRECTION_MISMATCH", None
    if persistence < params.trend_persistence_bars:
        return False, "INSUFFICIENT_TREND_PERSISTENCE", None
    if slope_consistency < params.min_slope_consistency:
        return False, "INCONSISTENT_EMA_SLOPE", None
    if spread_pct < params.min_ema_spread_pct:
        return False, "WEAK_EMA_SEPARATION", None
    if atr_percentile > params.max_atr_percentile:
        return False, "HIGH_ATR_PERCENTILE", None

    distances = [
        value
        for value in (
            candidate_pullback_distance_atr(current, direction),
            candidate_pullback_distance_atr(previous, direction),
        )
        if value is not None
    ]
    pullback_distance = min(distances) if distances else None
    if pullback_distance is None:
        return False, "MISSING_PULLBACK_ATR", None
    if pullback_distance > params.max_pullback_distance_atr:
        return False, "PULLBACK_OUTSIDE_ATR_BAND", pullback_distance
    if direction == "LONG" and not (close_price > ema20 and close_price > previous_high):
        return False, "LONG_RECLAIM_NOT_CONFIRMED", pullback_distance
    if direction == "SHORT" and not (close_price < ema20 and close_price < previous_low):
        return False, "SHORT_REJECTION_NOT_CONFIRMED", pullback_distance
    return True, "", pullback_distance


def candidate_estimated_tp_cost_ratio(config: Config, params: CandidateReplayParameters) -> float:
    per_side_slippage = config.shadow_slippage_bps / 10000.0
    round_trip_cost_pct = 2.0 * config.taker_fee_rate + 2.0 * per_side_slippage
    if params.take_profit_pct <= 0:
        return math.inf
    return round_trip_cost_pct / params.take_profit_pct


def candidate_order_quantity(
    entry_price: float,
    filters: ExchangeFilters,
    params: CandidateReplayParameters,
) -> float:
    if entry_price <= 0 or params.fixed_notional_usdt < filters.min_notional:
        return 0.0
    quantity = decimal_floor(params.fixed_notional_usdt / entry_price, filters.step_size)
    quantity = min(quantity, filters.max_qty)
    if quantity < filters.min_qty or quantity * entry_price < filters.min_notional:
        return 0.0
    return round(quantity, filters.quantity_precision)


def candidate_excursion_pct(side: str, entry_price: float, high: float, low: float) -> Tuple[float, float]:
    if entry_price <= 0:
        return 0.0, 0.0
    if side == "LONG":
        favorable = max((high - entry_price) / entry_price, 0.0)
        adverse = max((entry_price - low) / entry_price, 0.0)
    elif side == "SHORT":
        favorable = max((entry_price - low) / entry_price, 0.0)
        adverse = max((high - entry_price) / entry_price, 0.0)
    else:
        raise ConfigError(f"Unsupported candidate direction: {side}")
    return favorable, adverse


def update_candidate_excursions(position: Dict[str, Any], row: Any) -> None:
    favorable, adverse = candidate_excursion_pct(
        str(position["side"]),
        float(position["entry_price"]),
        float(row["high"]),
        float(row["low"]),
    )
    elapsed = max((int(row["open_time"]) - int(position["entry_open_time_ms"])) / 1000.0, 0.0)
    if favorable > float(position["mfe_pct"]):
        position["mfe_pct"] = favorable
        position["time_to_mfe_seconds"] = elapsed
    if adverse > float(position["mae_pct"]):
        position["mae_pct"] = adverse
        position["time_to_mae_seconds"] = elapsed


def close_candidate_position(
    position: Dict[str, Any],
    row: Any,
    exit_price: float,
    exit_reason: str,
    config: Config,
) -> Dict[str, Any]:
    side = str(position["side"])
    entry_price = float(position["entry_price"])
    quantity = float(position["quantity"])
    entry_notional = quantity * entry_price
    exit_notional = quantity * exit_price
    if side == "LONG":
        gross_pnl = (exit_price - entry_price) * quantity
    else:
        gross_pnl = (entry_price - exit_price) * quantity
    fees = (entry_notional + exit_notional) * config.taker_fee_rate
    slippage = (entry_notional + exit_notional) * config.shadow_slippage_bps / 10000.0
    net_pnl = gross_pnl - fees - slippage
    mfe_pct = float(position["mfe_pct"])
    mae_pct = float(position["mae_pct"])
    gross_return_pct = gross_pnl / entry_notional if entry_notional > 0 else 0.0
    return {
        "strategy_id": CANDIDATE_STRATEGY_NAME,
        "side": side,
        "signal_time": position["signal_time"],
        "entry_time": position["entry_time"],
        "entry_price": entry_price,
        "exit_time": candle_time_text(int(row["close_time"])),
        "exit_price": exit_price,
        "quantity": quantity,
        "entry_notional": entry_notional,
        "exit_notional": exit_notional,
        "stop_price": position["stop_price"],
        "take_profit_price": position["take_profit_price"],
        "entry_ema20": position["entry_ema20"],
        "entry_ema50": position["entry_ema50"],
        "entry_ema_spread_pct": position["entry_ema_spread_pct"],
        "entry_ema_slope_consistency": position["entry_ema_slope_consistency"],
        "entry_trend_persistence": position["entry_trend_persistence"],
        "entry_atr_15m": position["entry_atr_15m"],
        "entry_atr_1m_pct": position["entry_atr_1m_pct"],
        "entry_atr_1m_percentile": position["entry_atr_1m_percentile"],
        "pullback_distance_atr": position["pullback_distance_atr"],
        "gross_pnl": gross_pnl,
        "fees": fees,
        "slippage": slippage,
        "net_pnl": net_pnl,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "mfe_usdt": mfe_pct * entry_notional,
        "mae_usdt": mae_pct * entry_notional,
        "time_to_mfe_seconds": position["time_to_mfe_seconds"],
        "time_to_mae_seconds": position["time_to_mae_seconds"],
        "mfe_capture_ratio": gross_return_pct / mfe_pct if mfe_pct > 0 else None,
        "holding_seconds": max(
            (int(row["close_time"]) - int(position["entry_open_time_ms"])) / 1000.0,
            0.0,
        ),
        "exit_reason": exit_reason,
        "result": "WIN" if net_pnl > 0 else "LOSS",
        "symbol": config.symbol,
    }


def candidate_replay_direction(
    config: Config,
    filters: ExchangeFilters,
    features: pd.DataFrame,
    segment_start_ms: int,
    segment_end_ms: int,
    direction: str,
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    if direction not in CANDIDATE_DIRECTIONS:
        raise ConfigError(f"Unsupported candidate direction: {direction}")
    close_times = features["close_time"].astype("int64").to_numpy()
    indexes = np.flatnonzero((close_times >= segment_start_ms) & (close_times < segment_end_ms))
    trades: List[Dict[str, Any]] = []
    rejection_counts: Counter = Counter()
    signal_count = 0
    pending: Optional[Dict[str, Any]] = None
    position: Optional[Dict[str, Any]] = None
    cooldown_until_ms = 0

    for index_value in indexes:
        index = int(index_value)
        row = features.iloc[index]
        entered_this_candle = False
        exited_this_candle = False

        if pending is not None and int(pending["entry_index"]) == index:
            entry_price = float(row["open"])
            quantity = candidate_order_quantity(entry_price, filters, params)
            if quantity > 0:
                side = str(pending["direction"])
                stop_price = entry_price * (1.0 - params.stop_loss_pct if side == "LONG" else 1.0 + params.stop_loss_pct)
                take_profit_price = entry_price * (
                    1.0 + params.take_profit_pct if side == "LONG" else 1.0 - params.take_profit_pct
                )
                position = {
                    **pending,
                    "side": side,
                    "entry_time": candle_time_text(int(row["open_time"])),
                    "entry_open_time_ms": int(row["open_time"]),
                    "entry_price": entry_price,
                    "quantity": quantity,
                    "stop_price": stop_price,
                    "take_profit_price": take_profit_price,
                    "mfe_pct": 0.0,
                    "mae_pct": 0.0,
                    "time_to_mfe_seconds": 0.0,
                    "time_to_mae_seconds": 0.0,
                }
            else:
                rejection_counts["INVALID_EXCHANGE_FILTER_QUANTITY"] += 1
            pending = None
            entered_this_candle = True

        if position is not None:
            side = str(position["side"])
            stop_price = float(position["stop_price"])
            take_profit_price = float(position["take_profit_price"])
            stop_hit = float(row["low"]) <= stop_price if side == "LONG" else float(row["high"]) >= stop_price
            target_hit = (
                float(row["high"]) >= take_profit_price
                if side == "LONG"
                else float(row["low"]) <= take_profit_price
            )
            exit_price: Optional[float] = None
            exit_reason = ""
            if stop_hit:
                position["mae_pct"] = max(float(position["mae_pct"]), params.stop_loss_pct)
                position["time_to_mae_seconds"] = max(
                    (int(row["open_time"]) - int(position["entry_open_time_ms"])) / 1000.0,
                    0.0,
                )
                exit_price = stop_price
                exit_reason = "STOP_LOSS_SAME_CANDLE_CONSERVATIVE" if target_hit else "STOP_LOSS"
            elif target_hit:
                position["mfe_pct"] = max(float(position["mfe_pct"]), params.take_profit_pct)
                position["time_to_mfe_seconds"] = max(
                    (int(row["open_time"]) - int(position["entry_open_time_ms"])) / 1000.0,
                    0.0,
                )
                exit_price = take_profit_price
                exit_reason = "TAKE_PROFIT"
            else:
                update_candidate_excursions(position, row)
                trend_side = int(row["trend_side"]) if pd.notna(row["trend_side"]) else 0
                trend_invalidated = (side == "LONG" and trend_side == -1) or (side == "SHORT" and trend_side == 1)
                held_ms = int(row["close_time"]) - int(position["entry_open_time_ms"])
                if trend_invalidated:
                    exit_price = float(row["close"])
                    exit_reason = "COMPLETED_15M_TREND_INVALIDATION"
                elif held_ms >= params.max_holding_minutes * 60 * 1000:
                    exit_price = float(row["close"])
                    exit_reason = "MAX_HOLDING_TIME"
            if exit_price is not None:
                trades.append(close_candidate_position(position, row, exit_price, exit_reason, config))
                position = None
                cooldown_until_ms = int(row["close_time"]) + params.cooldown_minutes * 60 * 1000
                exited_this_candle = True

        if position is not None or pending is not None or entered_this_candle or exited_this_candle:
            continue
        if int(row["close_time"]) < cooldown_until_ms or index <= 0:
            continue
        previous = features.iloc[index - 1]
        accepted, reason, pullback_distance = evaluate_candidate_signal(row, previous, direction, params)
        if not accepted:
            rejection_counts[reason] += 1
            continue
        next_index = index + 1
        if next_index >= len(features):
            rejection_counts["NO_NEXT_CANDLE"] += 1
            continue
        next_row = features.iloc[next_index]
        if int(next_row["open_time"]) != int(row["close_time"]) + 1:
            rejection_counts["NEXT_CANDLE_GAP"] += 1
            continue
        if int(next_row["open_time"]) >= segment_end_ms:
            rejection_counts["NEXT_CANDLE_OUTSIDE_SEGMENT"] += 1
            continue
        signal_count += 1
        pending = {
            "entry_index": next_index,
            "direction": direction,
            "signal_time": candle_time_text(int(row["close_time"])),
            "entry_ema20": float(row["trend_ema20"]),
            "entry_ema50": float(row["trend_ema50"]),
            "entry_ema_spread_pct": float(row["trend_ema_spread_pct"]),
            "entry_ema_slope_consistency": float(
                row["long_slope_consistency" if direction == "LONG" else "short_slope_consistency"]
            ),
            "entry_trend_persistence": int(row["trend_persistence"]),
            "entry_atr_15m": float(row["trend_atr_15m"]),
            "entry_atr_1m_pct": float(row["atr_1m_pct"]),
            "entry_atr_1m_percentile": float(row["atr_1m_percentile"]),
            "pullback_distance_atr": float(pullback_distance or 0.0),
        }

    report = build_candidate_direction_report(trades, params.minimum_train_trades, params)
    report.update(
        {
            "direction": direction,
            "segment_start": candle_time_text(segment_start_ms),
            "segment_end_exclusive": candle_time_text(segment_end_ms),
            "evaluated_candles": int(len(indexes)),
            "accepted_signals": signal_count,
            "rejection_counts": dict(sorted(rejection_counts.items())),
            "open_position_excluded": position is not None,
            "pending_signal_excluded": pending is not None,
        }
    )
    return {"report": report, "trades": trades}


def candidate_distribution(values: Sequence[float]) -> Dict[str, Optional[float]]:
    clean = np.asarray([float(value) for value in values if math.isfinite(float(value))], dtype=float)
    if clean.size == 0:
        return {"mean": None, "median": None, "p25": None, "p75": None, "p90": None}
    return {
        "mean": float(np.mean(clean)),
        "median": float(np.median(clean)),
        "p25": float(np.quantile(clean, 0.25)),
        "p75": float(np.quantile(clean, 0.75)),
        "p90": float(np.quantile(clean, 0.90)),
    }


def candidate_direction_gate(
    report: Dict[str, Any],
    minimum_trades: int,
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    failures: List[str] = []
    if int(report.get("trades", 0) or 0) < minimum_trades:
        failures.append("INSUFFICIENT_TRADES")
    expectancy = report.get("expectancy")
    if expectancy is None or float(expectancy) <= 0:
        failures.append("NON_POSITIVE_EXPECTANCY")
    net_profit_factor = report.get("net_profit_factor")
    if net_profit_factor is None or float(net_profit_factor) <= params.minimum_net_profit_factor:
        failures.append("NET_PROFIT_FACTOR_BELOW_THRESHOLD")
    if float(report.get("max_drawdown", 0.0) or 0.0) >= params.maximum_drawdown_usdt:
        failures.append("DRAWDOWN_LIMIT_EXCEEDED")
    cost_ratio = report.get("cost_to_gross_profit_ratio")
    if cost_ratio is None or float(cost_ratio) > params.maximum_cost_to_gross_profit_ratio:
        failures.append("COST_RATIO_EXCEEDED")
    return {
        "eligible": not failures,
        "minimum_trades": minimum_trades,
        "failure_reasons": failures,
    }


def build_candidate_direction_report(
    trades: Sequence[Dict[str, Any]],
    minimum_trades: int,
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    trade_list = list(trades)
    base = summarize_trade_group(trade_list)
    gross_profit = sum(max(float(trade.get("gross_pnl", 0.0) or 0.0), 0.0) for trade in trade_list)
    total_cost = float(base["fees"]) + float(base["slippage"])
    cost_ratio = total_cost / gross_profit if gross_profit > 0 else None
    report = {
        **base,
        "cost_to_gross_profit_ratio": cost_ratio,
        "mfe_pct_distribution": candidate_distribution([trade.get("mfe_pct", 0.0) for trade in trade_list]),
        "mae_pct_distribution": candidate_distribution([trade.get("mae_pct", 0.0) for trade in trade_list]),
        "mfe_usdt_distribution": candidate_distribution([trade.get("mfe_usdt", 0.0) for trade in trade_list]),
        "mae_usdt_distribution": candidate_distribution([trade.get("mae_usdt", 0.0) for trade in trade_list]),
        "holding_seconds_distribution": candidate_distribution(
            [trade.get("holding_seconds", 0.0) for trade in trade_list]
        ),
        "exit_reason_counts": dict(Counter(str(trade.get("exit_reason", "UNKNOWN")) for trade in trade_list)),
    }
    report["gate"] = candidate_direction_gate(report, minimum_trades, params)
    return report


def skipped_candidate_direction(
    direction: str,
    segment_start_ms: int,
    segment_end_ms: int,
    reason: str,
    minimum_trades: int,
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    report = build_candidate_direction_report([], minimum_trades, params)
    report.update(
        {
            "direction": direction,
            "segment_start": candle_time_text(segment_start_ms),
            "segment_end_exclusive": candle_time_text(segment_end_ms),
            "evaluated_candles": 0,
            "accepted_signals": 0,
            "rejection_counts": {},
            "open_position_excluded": False,
            "pending_signal_excluded": False,
            "skipped_reason": reason,
        }
    )
    return {"report": report, "trades": []}


def write_candidate_trades(path: Path, trades: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_TRADE_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade)
    os.replace(temp_path, path)


def run_candidate_replay(
    config: Config,
    days: int,
    end_time: Optional[str],
    cache_file: str,
    output_dir_value: str,
    params: Optional[CandidateReplayParameters] = None,
) -> Dict[str, Any]:
    if config.live_trading or not config.dry_run or not config.shadow_mode:
        raise ConfigError("Candidate replay requires LIVE_TRADING=false, DRY_RUN=true, and SHADOW_MODE=true")
    params = params or CandidateReplayParameters()
    estimated_tp_cost_ratio = candidate_estimated_tp_cost_ratio(config, params)
    if estimated_tp_cost_ratio > params.maximum_tp_cost_ratio:
        raise ConfigError(
            f"Estimated round-trip costs consume {estimated_tp_cost_ratio:.6f} of gross take-profit, "
            f"above the {params.maximum_tp_cost_ratio:.6f} limit"
        )

    end_exclusive_ms = aligned_replay_end_ms(config.interval, end_time)
    periods = replay_period_boundaries(days, end_exclusive_ms)
    source_seconds = interval_seconds(config.interval)
    trend_seconds = interval_seconds(config.trend_interval)
    if source_seconds is None or trend_seconds is None:
        raise ConfigError("Candidate replay intervals are invalid")
    warmup_ms = max(
        params.atr_percentile_window * source_seconds,
        (params.trend_slow_ema_period + params.slope_consistency_window + params.trend_atr_period) * trend_seconds,
    ) * 1000
    download_start_ms = periods["train"][0] - warmup_ms - 2 * trend_seconds * 1000
    cache_path = resolve_app_path(cache_file)
    output_dir = resolve_app_path(output_dir_value)
    client = build_public_replay_client(config)
    filters = get_exchange_filters(client, config.symbol, config)
    all_klines = ensure_historical_kline_cache(
        client,
        config,
        download_start_ms,
        end_exclusive_ms - 1,
        cache_path,
    )
    features = build_candidate_feature_frame(all_klines, params, config.interval, config.trend_interval)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / "runs" / run_id

    train: Dict[str, Dict[str, Any]] = {}
    for direction in CANDIDATE_DIRECTIONS:
        result = candidate_replay_direction(config, filters, features, *periods["train"], direction, params)
        result["report"]["gate"] = candidate_direction_gate(
            result["report"], params.minimum_train_trades, params
        )
        train[direction] = result
    train_eligible = {
        direction for direction, result in train.items() if bool(result["report"]["gate"]["eligible"])
    }

    validation: Dict[str, Dict[str, Any]] = {}
    for direction in CANDIDATE_DIRECTIONS:
        if direction in train_eligible:
            result = candidate_replay_direction(
                config, filters, features, *periods["validation"], direction, params
            )
            result["report"]["gate"] = candidate_direction_gate(
                result["report"], params.minimum_out_of_sample_trades, params
            )
        else:
            result = skipped_candidate_direction(
                direction,
                *periods["validation"],
                reason="TRAIN_DIRECTION_NOT_ELIGIBLE",
                minimum_trades=params.minimum_out_of_sample_trades,
                params=params,
            )
        validation[direction] = result
    validated_directions = {
        direction
        for direction, result in validation.items()
        if direction in train_eligible and bool(result["report"]["gate"]["eligible"])
    }

    test: Dict[str, Dict[str, Any]] = {}
    for direction in CANDIDATE_DIRECTIONS:
        if direction in validated_directions:
            result = candidate_replay_direction(config, filters, features, *periods["test"], direction, params)
            result["report"]["gate"] = candidate_direction_gate(
                result["report"], params.minimum_out_of_sample_trades, params
            )
        else:
            result = skipped_candidate_direction(
                direction,
                *periods["test"],
                reason="VALIDATION_DIRECTION_NOT_ELIGIBLE",
                minimum_trades=params.minimum_out_of_sample_trades,
                params=params,
            )
        test[direction] = result
    test_eligible = {
        direction
        for direction, result in test.items()
        if direction in validated_directions and bool(result["report"]["gate"]["eligible"])
    }

    if test_eligible:
        viability_status = "CANDIDATE_FOR_FORWARD_SHADOW"
    elif not train_eligible:
        viability_status = "NO_TRAIN_DIRECTION"
    elif not validated_directions:
        viability_status = "NO_VALIDATED_DIRECTION"
    else:
        viability_status = "NOT_VIABLE_OUT_OF_SAMPLE"
    summary = {
        "engine": "cost_aware_stable_trend_pullback_walk_forward_v1",
        "generated_at": utc_now_text(),
        "run_id": run_id,
        "strategy": CANDIDATE_STRATEGY_NAME,
        "symbol": config.symbol,
        "interval": config.interval,
        "trend_interval": config.trend_interval,
        "days": days,
        "shadow_mode_only": True,
        "dry_run": True,
        "live_trading": False,
        "account_state_changed": False,
        "forward_shadow_execution_changed": False,
        "ui_changed": False,
        "source_sha256": file_sha256(Path(__file__).resolve()),
        "cache_file": str(cache_path),
        "cache_sha256": file_sha256(cache_path),
        "cached_candles": len(all_klines),
        "parameters": asdict(params),
        "estimated_round_trip_cost_to_gross_tp_ratio": estimated_tp_cost_ratio,
        "periods": {
            name: {"start": candle_time_text(start), "end_exclusive": candle_time_text(end)}
            for name, (start, end) in periods.items()
        },
        "direction_selection": {
            "train_eligible": sorted(train_eligible),
            "validation_eligible": sorted(validated_directions),
            "test_eligible": sorted(test_eligible),
            "aggregate_results_are_not_used_for_direction_selection": True,
        },
        "train": {direction: result["report"] for direction, result in train.items()},
        "validation": {direction: result["report"] for direction, result in validation.items()},
        "test": {direction: result["report"] for direction, result in test.items()},
        "viability_status": viability_status,
        "recommendation": (
            "CONTINUE_FORWARD_SHADOW_VALIDATION" if test_eligible else "DO_NOT_TRADE_LIVE"
        ),
    }
    for segment_name, segment in (("train", train), ("validation", validation), ("test", test)):
        for direction, result in segment.items():
            write_candidate_trades(
                run_dir / f"trades_{segment_name}_{direction.lower()}.csv",
                result["trades"],
            )
    write_json_atomic(run_dir / "summary.json", summary)
    write_json_atomic(output_dir / "latest.json", summary)
    return summary


def parse_candidate_replay_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the frozen cost-aware trend pullback candidate replay")
    parser.add_argument("--days", type=int, default=360)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--cache-file", default="historical_data/SOLUSDT_1m.csv")
    parser.add_argument("--output-dir", default="candidate_replay")
    return parser.parse_args(list(arguments))


def candidate_replay_main(arguments: Sequence[str]) -> None:
    args = parse_candidate_replay_args(arguments)
    config = load_config()
    summary = run_candidate_replay(
        config,
        days=args.days,
        end_time=args.end_time,
        cache_file=args.cache_file,
        output_dir_value=args.output_dir,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False))


def render_history_plot(state: RuntimeState, plot_type: str) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib.figure import Figure

    snapshot = state.snapshot()
    if plot_type == "zscore":
        data = snapshot["zscore_history"]
        title = "Z-score History"
        ylabel = "Z-score"
    else:
        data = snapshot["close_history"]
        title = "Recent Close Prices"
        ylabel = "Close"
    figure = Figure(figsize=(8, 4), dpi=120)
    axis = figure.subplots()
    if data:
        x_values = list(range(len(data)))
        y_values = [float(item["value"]) for item in data]
        labels = [str(item.get("time", "")) for item in data]
        axis.plot(x_values, y_values, linewidth=1.4)
        step = max(1, math.ceil(len(x_values) / 6))
        tick_indexes = x_values[::step]
        if x_values[-1] not in tick_indexes:
            tick_indexes.append(x_values[-1])
        axis.set_xticks(tick_indexes)
        axis.set_xticklabels([labels[index] for index in tick_indexes], rotation=30, ha="right", fontsize=8)
        if plot_type == "zscore":
            axis.axhline(2.30, color="red", linestyle="--", linewidth=0.8)
            axis.axhline(-2.30, color="green", linestyle="--", linewidth=0.8)
            axis.axhline(-2.60, color="green", linestyle=":", linewidth=0.8)
            axis.axhline(0.0, color="black", linestyle=":", linewidth=0.8)
    else:
        axis.text(0.5, 0.5, "No data yet", ha="center", va="center", transform=axis.transAxes)
    axis.set_title(title)
    axis.set_xlabel("Time")
    axis.set_ylabel(ylabel)
    axis.grid(True, alpha=0.3)
    figure.tight_layout()
    buffer = io.BytesIO()
    figure.savefig(buffer, format="png")
    return buffer.getvalue()


DASHBOARD_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>币安U本位合约监控</title>
  <style>
    :root {
      color-scheme: light;
      font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", sans-serif;
      --bg: #eef2f5;
      --surface: #ffffff;
      --surface-soft: #f8fafb;
      --line: #d9e1ea;
      --line-strong: #bcc9d6;
      --ink: #14202b;
      --muted: #607080;
      --muted-strong: #425466;
      --nav: #101820;
      --nav-soft: #172431;
      --accent: #1769aa;
      --accent-soft: #e5f1fa;
      --good: #0f7b4f;
      --good-soft: #e2f4ec;
      --warn: #9a6700;
      --warn-soft: #fff2cc;
      --bad: #b42318;
      --bad-soft: #fde7e4;
      --mono: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
      background: var(--bg);
      color: var(--ink);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      max-width: 100%;
      overflow-x: hidden;
      background: var(--bg);
    }
    .layout {
      display: grid;
      grid-template-columns: 248px minmax(0, 1fr);
      min-height: 100vh;
    }
    aside {
      position: sticky;
      top: 0;
      height: 100vh;
      background: var(--nav);
      color: #eef5fb;
      padding: 26px 20px;
      border-right: 1px solid rgba(255,255,255,0.06);
    }
    .brand-mark {
      display: grid;
      grid-template-columns: 42px 1fr;
      gap: 12px;
      align-items: center;
      margin-bottom: 28px;
    }
    .brand-square {
      width: 42px;
      height: 42px;
      border: 1px solid rgba(255,255,255,0.22);
      background: #213141;
      display: grid;
      place-items: center;
      font-family: var(--mono);
      font-weight: 800;
      letter-spacing: 0.04em;
      border-radius: 8px;
    }
    aside h1 {
      font-size: 18px;
      line-height: 1.35;
      margin: 0;
      text-wrap: balance;
    }
    .side-caption {
      margin: 4px 0 0;
      color: #9eb2c4;
      font-size: 12px;
      letter-spacing: 0.02em;
    }
    nav { display: grid; gap: 4px; }
    aside a {
      display: block;
      color: #c8d6e2;
      text-decoration: none;
      padding: 10px 12px;
      border-radius: 7px;
      font-size: 14px;
    }
    aside a:hover,
    aside a:focus {
      background: var(--nav-soft);
      color: #ffffff;
      outline: none;
    }
    .side-note {
      position: absolute;
      left: 20px;
      right: 20px;
      bottom: 22px;
      padding-top: 14px;
      border-top: 1px solid rgba(255,255,255,0.12);
      color: #9eb2c4;
      font-size: 12px;
      line-height: 1.7;
    }
    main {
      min-width: 0;
      max-width: 100%;
      overflow-x: hidden;
      padding: 22px 24px 36px;
    }
    .topbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 18px;
      align-items: start;
      margin-bottom: 14px;
    }
    .page-title h2 {
      margin: 0;
      font-size: 26px;
      line-height: 1.25;
      letter-spacing: 0;
      text-wrap: balance;
    }
    .page-title p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .mode-stack {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
      min-width: 0;
    }
    .mode-badge,
    .status-pill {
      display: inline-flex;
      min-height: 30px;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 5px 10px;
      border: 1px solid var(--line);
      background: var(--surface);
      color: var(--muted-strong);
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
      max-width: 100%;
    }
    .mode-badge.safe,
    .status-pill.running {
      background: var(--good-soft);
      border-color: #b8decf;
      color: var(--good);
    }
    .mode-badge.locked,
    .status-pill.paused {
      background: var(--warn-soft);
      border-color: #f3d27a;
      color: var(--warn);
    }
    .status-pill.error {
      background: var(--bad-soft);
      border-color: #f2b8b0;
      color: var(--bad);
    }
    .safety-strip {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: center;
      background: #fff6d6;
      border: 1px solid #ead38a;
      color: #573d00;
      padding: 12px 14px;
      border-radius: 8px;
      margin-bottom: 14px;
      min-width: 0;
    }
    .safety-strip strong {
      display: block;
      font-size: 14px;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .safety-strip span {
      display: block;
      margin-top: 2px;
      font-size: 12px;
      color: #735612;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .actions {
      display: flex;
      gap: 8px;
      justify-content: flex-end;
      flex-wrap: wrap;
    }
    form { display: inline; margin: 0; }
    button {
      min-height: 36px;
      border: 1px solid #0f5f9f;
      border-radius: 7px;
      padding: 8px 13px;
      background: var(--accent);
      color: white;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary {
      background: #ffffff;
      color: var(--accent);
      border-color: #b6cde0;
    }
    button:hover { filter: brightness(0.97); }
    section {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      min-width: 0;
      max-width: 100%;
    }
    section + section { margin-top: 14px; }
    .section-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 13px;
      flex-wrap: wrap;
    }
    h3 {
      margin: 0;
      font-size: 17px;
      line-height: 1.3;
      letter-spacing: 0;
    }
    .section-note,
    .muted {
      color: var(--muted);
      font-size: 12px;
    }
    .kpi-strip {
      display: grid;
      grid-template-columns: 1.15fr 1fr 1fr 1fr 1fr;
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric {
      min-width: 0;
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 13px 14px;
    }
    .metric.compact {
      background: var(--surface-soft);
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.3;
    }
    .metric strong {
      display: block;
      min-height: 27px;
      margin-top: 6px;
      font-family: var(--mono);
      font-size: 21px;
      line-height: 1.25;
      overflow-wrap: anywhere;
    }
    .metric strong[data-testid="latest-zscore"],
    .metric strong[data-testid="balance"] {
      font-size: 19px;
    }
    .metric small {
      display: block;
      margin-top: 5px;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.4;
    }
    .metric .status-pill {
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      font-size: 12px;
    }
    .workspace {
      display: grid;
      grid-template-columns: minmax(340px, 0.92fr) minmax(520px, 1.35fr);
      gap: 14px;
      align-items: start;
      min-width: 0;
    }
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .strategy-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .signal-summary {
      display: grid;
      grid-template-columns: 0.9fr 0.9fr 1.25fr 1.85fr;
      gap: 10px;
      margin-bottom: 12px;
    }
    .signal-summary .metric strong {
      font-size: 15px;
    }
    .signal-summary .metric strong[data-testid="rejection-reason"] {
      display: -webkit-box;
      max-height: 60px;
      overflow: hidden;
      -webkit-box-orient: vertical;
      -webkit-line-clamp: 3;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      font-size: 13px;
      line-height: 1.5;
    }
    table {
      border-collapse: collapse;
      width: 100%;
      font-size: 13px;
    }
    th, td {
      border-bottom: 1px solid #e7edf3;
      text-align: left;
      padding: 9px 8px;
      vertical-align: top;
    }
    th {
      background: #f7f9fb;
      color: #34495e;
      font-weight: 700;
      white-space: nowrap;
    }
    td {
      color: #22313f;
      font-family: var(--mono);
      overflow-wrap: anywhere;
    }
    .table-wrap {
      overflow-x: auto;
      border: 1px solid #e7edf3;
      border-radius: 8px;
      max-width: 100%;
    }
    .table-wrap table th,
    .table-wrap table td {
      border-bottom: 1px solid #e7edf3;
    }
    .plots {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .plot-frame {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      background: var(--surface-soft);
    }
    .plot-title {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      color: var(--muted-strong);
      font-size: 12px;
      font-weight: 700;
    }
    .plots img {
      display: block;
      width: 100%;
      min-height: 260px;
      background: #ffffff;
    }
    @media (max-width: 1180px) {
      .kpi-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .workspace { grid-template-columns: 1fr; }
      .strategy-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 860px) {
      .layout { grid-template-columns: 1fr; }
      aside {
        position: static;
        height: auto;
        padding: 18px;
      }
      .side-note { position: static; margin-top: 16px; }
      nav { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      main { padding: 16px; }
      .topbar,
      .safety-strip { grid-template-columns: 1fr; }
      .mode-stack,
      .actions { justify-content: flex-start; }
      .mode-badge {
        width: 100%;
        white-space: normal;
        overflow-wrap: anywhere;
        justify-content: flex-start;
      }
      .metric-grid,
      .signal-summary,
      .strategy-grid,
      .plots { grid-template-columns: 1fr; }
    }
    @media (max-width: 520px) {
      nav { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kpi-strip { grid-template-columns: 1fr; }
      .mode-stack { display: grid; grid-template-columns: 1fr; }
      .page-title p,
      .safety-strip strong,
      .safety-strip span {
        word-break: break-all;
      }
      .page-title h2 { font-size: 22px; }
      .section-head .section-note { display: none; }
      section { padding: 13px; }
      th, td { padding: 8px 6px; }
    }
  </style>
</head>
<body>
  <div class="layout">
    <aside>
      <div class="brand-mark">
        <div class="brand-square">ZS</div>
        <div>
          <h1>币安U本位合约监控</h1>
          <p class="side-caption">Z-score 影子验证台</p>
        </div>
      </div>
      <nav>
        <a href="#status">运行总览</a>
        <a href="#account">账户与行情</a>
        <a href="#strategy">策略验证</a>
        <a href="#signal">信号筛选</a>
        <a href="#trades">影子交易</a>
        <a href="#plots">实时图表</a>
      </nav>
      <div class="side-note">
        <strong>安全边界</strong><br>
        仅监控与影子验证，不执行真实订单。
      </div>
    </aside>
    <main>
      <div class="topbar">
        <div class="page-title">
          <h2>交易策略影子监控台</h2>
          <p>自动刷新运行状态、策略信号、虚拟交易与图表。</p>
        </div>
        <div class="mode-stack">
          <span class="mode-badge safe">SHADOW MODE</span>
          <span class="mode-badge safe">DRY_RUN = <span data-testid="dry-run">{{ snapshot.config.dry_run }}</span></span>
          <span class="mode-badge locked">LIVE_TRADING = <span data-testid="live-trading">{{ snapshot.config.live_trading }}</span></span>
        </div>
      </div>

      <div class="safety-strip" data-testid="safety-strip">
        <div>
          <strong>安全状态：当前为影子验证模式，不会真实下单。</strong>
          <span>真实下单通道保持关闭，影子验证持续运行。</span>
        </div>
        <div class="actions">
          <form method="post" action="/pause"><button class="secondary" type="submit">暂停</button></form>
          <form method="post" action="/resume"><button type="submit">继续运行</button></form>
        </div>
      </div>

      <section id="status">
        <div class="section-head">
          <h3>运行总览</h3>
          <span class="section-note">每 4 秒刷新一次</span>
        </div>
        <div class="kpi-strip">
          <div class="metric compact"><span>运行状态</span><strong><span class="status-pill {{ snapshot.status }}" data-testid="status">{{ snapshot.status }}</span></strong><small>暂停后仍可查看已有数据</small></div>
          <div class="metric compact"><span>交易对</span><strong data-testid="symbol">{{ snapshot.config.symbol }}</strong><small>当前监控标的</small></div>
          <div class="metric compact"><span>USDT 余额</span><strong data-testid="balance">{{ "%.6f"|format(snapshot.balance) if snapshot.balance is number else snapshot.balance }}</strong><small>只读余额展示</small></div>
          <div class="metric compact"><span>最新 Z-score</span><strong data-testid="latest-zscore">{{ "%.8f"|format(snapshot.latest_zscore) if snapshot.latest_zscore is number else snapshot.latest_zscore }}</strong><small>基于已收盘 1m K 线</small></div>
          <div class="metric compact"><span>虚拟持仓</span><strong data-testid="elite-open-position">{{ snapshot.shadow.open_position_count }}</strong><small>router shadow position</small></div>
          <div class="metric compact"><span>执行控制器</span><strong data-testid="active-execution-strategy">{{ snapshot.shadow.active_execution_strategy }}</strong><small>唯一成交入口</small></div>
          <div class="metric compact"><span>市场状态</span><strong data-testid="current-regime">{{ snapshot.shadow.current_regime }}</strong><small>regime classifier</small></div>
          <div class="metric compact"><span>路由策略</span><strong data-testid="router-active-strategy">{{ snapshot.shadow.router_active_strategy }}</strong><small>每根 K 线最多一个</small></div>
        </div>
      </section>

      <div class="workspace">
        <div>
          <section id="account">
            <div class="section-head">
              <h3>账户与行情</h3>
              <span class="section-note" data-testid="latest-candle">{{ snapshot.latest_closed_candle_time }}</span>
            </div>
            <div class="metric-grid">
              <div class="metric"><span>是否暂停</span><strong data-testid="paused">{{ snapshot.paused }}</strong></div>
              <div class="metric"><span>影子模式</span><strong data-testid="shadow-mode">{{ snapshot.config.shadow_mode }}</strong></div>
              <div class="metric"><span>杠杆</span><strong data-testid="leverage">{{ snapshot.leverage }}x</strong></div>
              <div class="metric"><span>保证金模式</span><strong data-testid="margin-type">{{ snapshot.margin_type }}</strong></div>
              <div class="metric"><span>最新收盘价</span><strong data-testid="latest-close">{{ "%.4f"|format(snapshot.latest_close) if snapshot.latest_close is number else snapshot.latest_close }}</strong></div>
              <div class="metric"><span>候选信号数</span><strong data-testid="candidate-signals">{{ snapshot.shadow.candidate_signals }}</strong></div>
              <div class="metric"><span>ATR 桶</span><strong data-testid="atr-bucket">{{ snapshot.atr_bucket }}</strong><small data-testid="atr-pct">{{ "%.8f"|format(snapshot.atr_pct) if snapshot.atr_pct is number else snapshot.atr_pct }}</small></div>
              <div class="metric"><span>趋势桶</span><strong data-testid="trend-bucket">{{ snapshot.trend_bucket }}</strong><small data-testid="trend-slope">{{ "%.8f"|format(snapshot.trend_slope_pct) if snapshot.trend_slope_pct is number else snapshot.trend_slope_pct }}</small></div>
            </div>
          </section>

          <section id="strategy">
            <div class="section-head">
              <h3>策略验证</h3>
              <span class="section-note">REGIME_STRATEGY_ROUTER</span>
            </div>
            <div class="strategy-grid">
              <div class="metric"><span>净收益</span><strong data-testid="elite-net-pnl">{{ snapshot.shadow.net_pnl_usdt }}</strong></div>
              <div class="metric"><span>胜率</span><strong data-testid="elite-win-rate">{{ snapshot.shadow.win_rate }}</strong></div>
              <div class="metric"><span>单笔期望</span><strong data-testid="elite-expectancy">{{ snapshot.shadow.expectancy_usdt }}</strong></div>
              <div class="metric"><span>利润因子</span><strong data-testid="elite-profit-factor">{{ snapshot.shadow.profit_factor }}</strong></div>
              <div class="metric"><span>最大回撤</span><strong data-testid="elite-drawdown">{{ snapshot.shadow.max_drawdown_usdt }}</strong></div>
              <div class="metric"><span>完成交易</span><strong data-testid="elite-total-trades">{{ snapshot.shadow.completed_trades }}</strong></div>
              <div class="metric"><span>可行性</span><strong data-testid="elite-viability">{{ snapshot.shadow.viability_status }}</strong></div>
              <div class="metric"><span>最近错误</span><strong data-testid="last-error">{{ snapshot.last_error }}</strong></div>
              <div class="metric"><span>数据集有效</span><strong data-testid="dataset-valid">{{ snapshot.shadow.dataset_valid }}</strong><small data-testid="contaminated-count">{{ snapshot.shadow.contaminated_trade_count }}</small></div>
              <div class="metric"><span>最佳组合</span><strong data-testid="best-regime-strategy-pair">{{ snapshot.shadow.best_regime_strategy_pair }}</strong></div>
              <div class="metric"><span>最差组合</span><strong data-testid="worst-regime-strategy-pair">{{ snapshot.shadow.worst_regime_strategy_pair }}</strong></div>
            </div>
          </section>

          <section id="market-gate">
            <div class="section-head">
              <h3>Market Gate Analytics</h3>
              <span class="section-note">Observation only unless MARKET_GATE_ENFORCED=true</span>
            </div>
            <div class="strategy-grid">
              <div class="metric"><span>Stability</span><strong data-testid="current-stability-state">{{ snapshot.shadow.current_stability_state }}</strong></div>
              <div class="metric"><span>Score</span><strong data-testid="current-regime-stability-score">{{ snapshot.shadow.current_regime_stability_score }}</strong></div>
              <div class="metric"><span>Time Window</span><strong data-testid="current-time-window-id">{{ snapshot.shadow.current_time_window_id }}</strong></div>
              <div class="metric"><span>Gate Allowed</span><strong data-testid="current-gate-allowed">{{ snapshot.shadow.current_trade_allowed_by_market_gate }}</strong></div>
              <div class="metric"><span>Gate Enforced</span><strong data-testid="market-gate-enforced">{{ snapshot.shadow.market_gate_enforced }}</strong></div>
              <div class="metric"><span>Gate Reason</span><strong data-testid="current-market-gate-rejection">{{ snapshot.shadow.current_market_gate_rejection_reason }}</strong></div>
              <div class="metric"><span>Window Trades</span><strong data-testid="current-window-trades">{{ snapshot.shadow.latest_signal.get("window_trade_count", "") }}</strong></div>
              <div class="metric"><span>Window Expectancy</span><strong data-testid="current-window-expectancy">{{ snapshot.shadow.latest_signal.get("window_expectancy", "") }}</strong></div>
              <div class="metric"><span>Window Profit Factor</span><strong data-testid="current-window-profit-factor">{{ snapshot.shadow.latest_signal.get("window_profit_factor", "") }}</strong></div>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr><th>Window</th><th>Trades</th><th>Expectancy</th><th>Profit Factor</th><th>Net PnL</th><th>Win Rate</th></tr></thead>
                <tbody id="profitable-windows-body">
                {% for row in snapshot.shadow.profitable_windows %}
                  <tr><td>{{ row.time_window_id }}</td><td>{{ row.trades }}</td><td>{{ row.expectancy }}</td><td>{{ row.profit_factor }}</td><td>{{ row.net_pnl }}</td><td>{{ row.win_rate }}</td></tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr><th>Worst Window</th><th>Trades</th><th>Expectancy</th><th>Profit Factor</th><th>Net PnL</th><th>Win Rate</th></tr></thead>
                <tbody id="worst-windows-body">
                {% for row in snapshot.shadow.worst_windows %}
                  <tr><td>{{ row.time_window_id }}</td><td>{{ row.trades }}</td><td>{{ row.expectancy }}</td><td>{{ row.profit_factor }}</td><td>{{ row.net_pnl }}</td><td>{{ row.win_rate }}</td></tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </section>
        </div>

        <div>
          <section id="signal">
            <div class="section-head">
              <h3>信号筛选</h3>
              <span class="section-note">最新一次闭合 K 线评估</span>
            </div>
            <div class="signal-summary">
              <div class="metric compact"><span>决策</span><strong data-testid="signal-decision">{{ snapshot.shadow.latest_signal.get("trade_decision", "") }}</strong></div>
              <div class="metric compact"><span>失败过滤</span><strong data-testid="failed-filter">{{ snapshot.shadow.latest_signal.get("failed_filter", "") }}</strong></div>
              <div class="metric compact"><span>确认回归</span><strong data-testid="reversion-status">{{ snapshot.shadow.latest_signal.get("reversion_status", "") }}</strong></div>
              <div class="metric compact"><span>拒绝原因</span><strong data-testid="rejection-reason">{{ snapshot.shadow.latest_signal.get("rejection_reason", "") }}</strong></div>
            </div>
            <div class="table-wrap">
              <table>
                <tbody id="latest-signal-body">
                {% for key, value in snapshot.shadow.latest_signal.items() %}
                  <tr><th>{{ key }}</th><td>{{ value }}</td></tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </section>

          <section id="trades">
            <div class="section-head">
              <h3>影子交易</h3>
              <span class="section-note">最近 20 笔虚拟成交</span>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr><th>策略</th><th>状态</th><th>开仓时间</th><th>平仓时间</th><th>开仓价</th><th>平仓价</th><th>净收益</th><th>原因</th><th>结果</th></tr>
                </thead>
                <tbody id="trades-body">
                {% for trade in snapshot.shadow.recent_trades %}
                  <tr>
                    <td>{{ trade.strategy }}</td>
                    <td>{{ trade.regime_at_entry }}</td>
                    <td>{{ trade.entry_time }}</td>
                    <td>{{ trade.exit_time }}</td>
                    <td>{{ trade.entry_price }}</td>
                    <td>{{ trade.exit_price }}</td>
                    <td>{{ trade.net_pnl }}</td>
                    <td>{{ trade.exit_reason }}</td>
                    <td>{{ trade.result }}</td>
                  </tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      </div>

      <section id="plots">
        <div class="section-head">
          <h3>实时图表</h3>
          <span class="section-note">图片缓存已自动刷新</span>
        </div>
        <div class="plots">
          <div class="plot-frame">
            <div class="plot-title"><span>Z-score 历史</span><span>均值回归观察</span></div>
            <img id="zscore-plot" src="/plot/zscore.png" alt="Z-score 历史">
          </div>
          <div class="plot-frame">
            <div class="plot-title"><span>近期收盘价</span><span>1m K 线收盘</span></div>
            <img id="closes-plot" src="/plot/closes.png" alt="近期收盘价">
          </div>
        </div>
      </section>
    </main>
  </div>
  <script>
    function fmt(value) {
      if (value === null || value === undefined || value === "") return "";
      if (typeof value === "number") return Number.isFinite(value) ? value.toFixed(8) : String(value);
      return String(value);
    }
    function setText(selector, value) {
      const node = document.querySelector(selector);
      if (node) node.textContent = fmt(value);
    }
    function setStatusClass(value) {
      const node = document.querySelector('[data-testid="status"]');
      if (!node) return;
      node.classList.remove("running", "paused", "error");
      if (value) node.classList.add(String(value));
    }
    function escapeHtml(value) {
      return fmt(value).replace(/[&<>"']/g, (char) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#039;"
      }[char]));
    }
    async function refreshDashboard() {
      const response = await fetch("/api/status", {cache: "no-store"});
      if (!response.ok) return;
      const snapshot = await response.json();
      const shadow = snapshot.shadow || {};
      const config = snapshot.config || {};
      setText('[data-testid="status"]', snapshot.status);
      setStatusClass(snapshot.status);
      setText('[data-testid="paused"]', snapshot.paused);
      setText('[data-testid="dry-run"]', config.dry_run);
      setText('[data-testid="live-trading"]', config.live_trading);
      setText('[data-testid="shadow-mode"]', config.shadow_mode);
      setText('[data-testid="started-at"]', snapshot.started_at);
      setText('[data-testid="last-update"]', snapshot.last_update);
      setText('[data-testid="balance"]', snapshot.balance);
      setText('[data-testid="symbol"]', config.symbol);
      setText('[data-testid="last-error"]', snapshot.last_error);
      setText('[data-testid="leverage"]', `${snapshot.leverage || ""}x`);
      setText('[data-testid="margin-type"]', snapshot.margin_type);
      setText('[data-testid="latest-close"]', snapshot.latest_close);
      setText('[data-testid="latest-zscore"]', snapshot.latest_zscore);
      setText('[data-testid="latest-candle"]', snapshot.latest_closed_candle_time);
      setText('[data-testid="atr-bucket"]', snapshot.atr_bucket);
      setText('[data-testid="atr-pct"]', snapshot.atr_pct);
      setText('[data-testid="trend-bucket"]', snapshot.trend_bucket);
      setText('[data-testid="trend-slope"]', snapshot.trend_slope_pct);
      setText('[data-testid="elite-net-pnl"]', shadow.net_pnl_usdt);
      setText('[data-testid="elite-win-rate"]', shadow.win_rate);
      setText('[data-testid="elite-expectancy"]', shadow.expectancy_usdt);
      setText('[data-testid="elite-profit-factor"]', shadow.profit_factor);
      setText('[data-testid="elite-drawdown"]', shadow.max_drawdown_usdt);
      setText('[data-testid="elite-total-trades"]', shadow.completed_trades);
      setText('[data-testid="elite-viability"]', shadow.viability_status);
      setText('[data-testid="elite-open-position"]', shadow.open_position_count);
      setText('[data-testid="active-execution-strategy"]', shadow.active_execution_strategy);
      setText('[data-testid="current-regime"]', shadow.current_regime);
      setText('[data-testid="router-active-strategy"]', shadow.router_active_strategy);
      setText('[data-testid="dataset-valid"]', shadow.dataset_valid);
      setText('[data-testid="contaminated-count"]', shadow.contaminated_trade_count);
      setText('[data-testid="best-regime-strategy-pair"]', shadow.best_regime_strategy_pair);
      setText('[data-testid="worst-regime-strategy-pair"]', shadow.worst_regime_strategy_pair);
      setText('[data-testid="candidate-signals"]', shadow.candidate_signals);
      setText('[data-testid="current-stability-state"]', shadow.current_stability_state);
      setText('[data-testid="current-regime-stability-score"]', shadow.current_regime_stability_score);
      setText('[data-testid="current-time-window-id"]', shadow.current_time_window_id);
      setText('[data-testid="current-gate-allowed"]', shadow.current_trade_allowed_by_market_gate);
      setText('[data-testid="market-gate-enforced"]', shadow.market_gate_enforced);
      setText('[data-testid="current-market-gate-rejection"]', shadow.current_market_gate_rejection_reason);
      const latest = shadow.latest_signal || {};
      setText('[data-testid="signal-decision"]', latest.trade_decision);
      setText('[data-testid="failed-filter"]', latest.failed_filter);
      setText('[data-testid="reversion-status"]', latest.reversion_status);
      setText('[data-testid="rejection-reason"]', latest.rejection_reason);
      setText('[data-testid="current-window-trades"]', latest.window_trade_count);
      setText('[data-testid="current-window-expectancy"]', latest.window_expectancy);
      setText('[data-testid="current-window-profit-factor"]', latest.window_profit_factor);
      const signalBody = document.getElementById("latest-signal-body");
      if (signalBody) {
        signalBody.innerHTML = Object.keys(latest).map((key) => `<tr><th>${escapeHtml(key)}</th><td>${escapeHtml(latest[key])}</td></tr>`).join("");
      }
      function renderWindowRows(elementId, rows) {
        const body = document.getElementById(elementId);
        if (!body) return;
        body.innerHTML = (rows || []).map((row) => `
          <tr>
            <td>${escapeHtml(row.time_window_id)}</td>
            <td>${escapeHtml(row.trades)}</td>
            <td>${escapeHtml(row.expectancy)}</td>
            <td>${escapeHtml(row.profit_factor)}</td>
            <td>${escapeHtml(row.net_pnl)}</td>
            <td>${escapeHtml(row.win_rate)}</td>
          </tr>`).join("");
      }
      renderWindowRows("profitable-windows-body", shadow.profitable_windows);
      renderWindowRows("worst-windows-body", shadow.worst_windows);
      const tradesBody = document.getElementById("trades-body");
      if (tradesBody) {
        const trades = shadow.recent_trades || [];
        tradesBody.innerHTML = trades.map((trade) => `
          <tr>
            <td>${escapeHtml(trade.strategy)}</td>
            <td>${escapeHtml(trade.regime_at_entry)}</td>
            <td>${escapeHtml(trade.entry_time)}</td>
            <td>${escapeHtml(trade.exit_time)}</td>
            <td>${escapeHtml(trade.entry_price)}</td>
            <td>${escapeHtml(trade.exit_price)}</td>
            <td>${escapeHtml(trade.net_pnl)}</td>
            <td>${escapeHtml(trade.exit_reason)}</td>
            <td>${escapeHtml(trade.result)}</td>
          </tr>`).join("");
      }
      const stamp = Date.now();
      const zPlot = document.getElementById("zscore-plot");
      const cPlot = document.getElementById("closes-plot");
      if (zPlot) zPlot.src = `/plot/zscore.png?t=${stamp}`;
      if (cPlot) cPlot.src = `/plot/closes.png?t=${stamp}`;
    }
    window.addEventListener("load", () => {
      refreshDashboard().catch(() => {});
      window.setInterval(() => refreshDashboard().catch(() => {}), 4000);
    });
  </script>
</body>
</html>
"""


def create_flask_app(state: RuntimeState, client: Any = None, config: Optional[Config] = None) -> Any:
    del client
    try:
        from flask import Flask, Response, jsonify, redirect, render_template_string, request, url_for
    except ImportError as exc:
        raise ConfigError("Flask is required. Install it with: pip install Flask") from exc

    active_config = config or load_config()
    app = Flask(__name__)

    def auth_required() -> Response:
        return Response(
            "Authentication required.",
            401,
            {"WWW-Authenticate": 'Basic realm="Elite Shadow Monitor"'},
        )

    @app.before_request
    def enforce_auth() -> Optional[Response]:
        if not active_config.web_auth_enabled:
            return None
        if not active_config.web_password and not active_config.web_auth_token:
            return None
        token = request.headers.get("X-Auth-Token", "") or request.args.get("token", "")
        if active_config.web_auth_token and token == active_config.web_auth_token:
            return None
        auth = request.authorization
        if auth is None:
            return auth_required()
        if auth.username == active_config.web_username and auth.password == active_config.web_password:
            return None
        return auth_required()

    @app.get("/")
    def index() -> str:
        return render_template_string(DASHBOARD_TEMPLATE, snapshot=state.snapshot())

    @app.get("/api/status")
    def api_status() -> Any:
        return jsonify(state.snapshot())

    @app.post("/pause")
    def pause() -> Any:
        state.set_paused(True)
        LOGGER.info("Bot paused from web interface.")
        return redirect(url_for("index"))

    @app.post("/resume")
    def resume() -> Any:
        state.set_paused(False)
        LOGGER.info("Bot resumed from web interface.")
        return redirect(url_for("index"))

    @app.get("/plot/zscore.png")
    def zscore_plot() -> Response:
        return Response(render_history_plot(state, "zscore"), mimetype="image/png")

    @app.get("/plot/closes.png")
    def closes_plot() -> Response:
        return Response(render_history_plot(state, "closes"), mimetype="image/png")

    return app


def request_shutdown(signum: int, frame: Any) -> None:
    del frame
    global STOP_REQUESTED
    STOP_REQUESTED = True
    LOGGER.info("Shutdown signal received: %s", signum)


def install_signal_handlers() -> None:
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)


def interval_seconds(interval: str) -> Optional[int]:
    value = str(interval or "").strip()
    if len(value) < 2 or not value[:-1].isdigit():
        return None
    multiplier = {"m": 60, "h": 3600, "d": 86400}.get(value[-1])
    count = int(value[:-1])
    if multiplier is None or count <= 0:
        return None
    return count * multiplier


def next_bot_loop_delay(
    config: Config,
    cycle_failed: bool,
    paused: bool,
    now_epoch: Optional[float] = None,
) -> float:
    if cycle_failed or paused:
        return config.loop_sleep_seconds
    period_seconds = interval_seconds(config.interval)
    if period_seconds is None:
        return config.loop_sleep_seconds
    now_value = time.time() if now_epoch is None else float(now_epoch)
    next_boundary = (math.floor(now_value / period_seconds) + 1) * period_seconds
    return max(1.0, next_boundary + CANDLE_SETTLEMENT_SECONDS - now_value)


def bot_loop_thread(client: Any, config: Config, state: RuntimeState) -> None:
    while not STOP_REQUESTED:
        cycle_failed = False
        try:
            run_once(client, config, state)
        except Exception as exc:
            cycle_failed = True
            LOGGER.exception("Recoverable loop error: %s", exc)
            state.set_error(str(exc))
        if STOP_REQUESTED:
            break
        time.sleep(
            next_bot_loop_delay(
                config,
                cycle_failed=cycle_failed,
                paused=state.is_paused(),
            )
        )
    state.set_status("stopped")
    LOGGER.info("Bot loop stopped.")


def log_startup(config: Config) -> None:
    LOGGER.info("Starting regime router shadow validation engine.")
    LOGGER.info("Symbol=%s interval=%s lookback=%s", config.symbol, config.interval, config.lookback)
    LOGGER.info("Active strategies=%s", ",".join(config.active_strategies))
    LOGGER.info("Active execution controller=%s", ACTIVE_STRATEGY)
    LOGGER.info("Logging-only strategies=%s", ",".join(LOGGING_ONLY_STRATEGIES))
    LOGGER.info("DRY_RUN=%s TESTNET=%s LIVE_TRADING=%s SHADOW_MODE=%s", config.dry_run, config.testnet, config.live_trading, config.shadow_mode)
    LOGGER.info(
        "Closed-candle scheduling interval=%s settlement_seconds=%s exchange_filter_cache_ttl_seconds=%s",
        config.interval,
        CANDLE_SETTLEMENT_SECONDS,
        config.exchange_filters_cache_ttl_seconds,
    )
    LOGGER.info("No real order execution is available in this engine.")


def main() -> None:
    configure_logging()
    state: Optional[RuntimeState] = None
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "replay":
            historical_replay_main(sys.argv[2:])
            return
        if len(sys.argv) > 1 and sys.argv[1] == "candidate-replay":
            candidate_replay_main(sys.argv[2:])
            return
        config = load_config()
        state = RuntimeState(state_file=config.bot_state_file)
        state.update_config(config)
        maybe_write_decision_report(state.shadow, config)
        log_startup(config)
        install_signal_handlers()
        client = build_client(config)
        worker = threading.Thread(target=bot_loop_thread, args=(client, config, state), daemon=True)
        worker.start()
        app = create_flask_app(state, client=client, config=config)
        app.run(host=config.web_host, port=config.web_port, threaded=True, debug=False, use_reloader=False)
    except ConfigError as exc:
        LOGGER.error("Configuration error: %s", exc)
        if state is not None:
            state.set_error(str(exc))
        sys.exit(2)
    except KeyboardInterrupt:
        request_shutdown(signal.SIGINT, None)
    except Exception as exc:
        LOGGER.exception("Fatal startup error: %s", exc)
        if state is not None:
            state.set_error(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
