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
from decimal import Decimal, ROUND_DOWN, ROUND_UP
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
FUNDING_RATE_COLUMNS = ["funding_time", "funding_rate", "mark_price"]
REPLAY_TRAIN_SHARE = 0.50
REPLAY_VALIDATION_SHARE = 0.25
REPLAY_MIN_TRAIN_PAIR_TRADES = 20
REPLAY_MIN_VALIDATION_PAIR_TRADES = 10
REPLAY_MIN_TEST_TRADES = 100
REPLAY_MIN_NET_PROFIT_FACTOR = 1.10
REPLAY_MAX_DRAWDOWN_USDT = 0.20
CANDIDATE_STRATEGY_NAME = "COST_AWARE_STABLE_TREND_PULLBACK"
CANDIDATE_PROFILE_V3 = "v3_5m_15m"
CANDIDATE_PROFILE_V4 = "v4_15m_1h"
CANDIDATE_PROFILE_V5 = "v5_30m_2h"
CANDIDATE_PROFILE_V6 = "v6_long_cost_gate_30m_2h"
CANDIDATE_PROFILE_V7 = "v7_cost_aware_breakout_15m_1h_4h"
CANDIDATE_PROFILE_V8 = "v8_cost_aware_pullback_15m_1h_4h"
CANDIDATE_PROFILE_V9 = "v9_cross_asset_pullback_15m_1h_4h"
FEASIBILITY_SCAN_PROFILE = "v10_feasibility_universe_scanner"
V10_MOMENTUM_PROFILE = "v10_cross_sectional_momentum_rotation_4h"
V10_MOMENTUM_STRATEGY = "CROSS_SECTIONAL_MOMENTUM_ROTATION_V10"
CANDIDATE_REPLAY_PROFILES = (
    CANDIDATE_PROFILE_V3,
    CANDIDATE_PROFILE_V4,
    CANDIDATE_PROFILE_V5,
    CANDIDATE_PROFILE_V6,
    CANDIDATE_PROFILE_V7,
    CANDIDATE_PROFILE_V8,
    CANDIDATE_PROFILE_V9,
)
CANDIDATE_DIRECTIONS = ("LONG", "SHORT")
CANDIDATE_TREND_LONG_NAME = "TREND_PULLBACK_LONG"
CANDIDATE_TREND_SHORT_NAME = "TREND_PULLBACK_SHORT"
CANDIDATE_BREAKOUT_LONG_NAME = "COST_AWARE_TREND_BREAKOUT_V7_LONG"
CANDIDATE_BREAKOUT_SHORT_NAME = "COST_AWARE_TREND_BREAKOUT_V7_SHORT"
CANDIDATE_REGIME_PULLBACK_LONG_NAME = "COST_AWARE_TREND_PULLBACK_V8_LONG"
CANDIDATE_REGIME_PULLBACK_SHORT_NAME = "COST_AWARE_TREND_PULLBACK_V8_SHORT"
CANDIDATE_CROSS_ASSET_PULLBACK_LONG_NAME = "CROSS_ASSET_TREND_PULLBACK_V9_LONG"
CANDIDATE_CROSS_ASSET_PULLBACK_SHORT_NAME = "CROSS_ASSET_TREND_PULLBACK_V9_SHORT"
CANDIDATE_MEAN_REVERSION_NAME = "MEAN_REVERSION_INDEPENDENT"
CANDIDATE_LEDGER_NAMES = (
    CANDIDATE_TREND_LONG_NAME,
    CANDIDATE_TREND_SHORT_NAME,
)
LEGACY_CANDIDATE_LEDGER_NAMES = (
    CANDIDATE_MEAN_REVERSION_NAME,
)
SUPPORTED_CANDIDATE_LEDGER_NAMES = CANDIDATE_LEDGER_NAMES + LEGACY_CANDIDATE_LEDGER_NAMES
SUPPORTED_CANDIDATE_LEDGER_NAMES += (
    CANDIDATE_BREAKOUT_LONG_NAME,
    CANDIDATE_BREAKOUT_SHORT_NAME,
    CANDIDATE_REGIME_PULLBACK_LONG_NAME,
    CANDIDATE_REGIME_PULLBACK_SHORT_NAME,
    CANDIDATE_CROSS_ASSET_PULLBACK_LONG_NAME,
    CANDIDATE_CROSS_ASSET_PULLBACK_SHORT_NAME,
)
CANDIDATE_REGIME_PULLBACK_LEDGER_NAMES = (
    CANDIDATE_REGIME_PULLBACK_LONG_NAME,
    CANDIDATE_REGIME_PULLBACK_SHORT_NAME,
    CANDIDATE_CROSS_ASSET_PULLBACK_LONG_NAME,
    CANDIDATE_CROSS_ASSET_PULLBACK_SHORT_NAME,
)
CANDIDATE_HIGHER_TIMEFRAME_LEDGER_NAMES = (
    CANDIDATE_BREAKOUT_LONG_NAME,
    CANDIDATE_BREAKOUT_SHORT_NAME,
) + CANDIDATE_REGIME_PULLBACK_LEDGER_NAMES
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
    "risk_budget_usdt",
    "estimated_net_loss_usdt",
    "minimum_order_estimated_loss_usdt",
    "risk_sizing_skip_reason",
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
    "spread_cost",
    "funding_pnl",
    "execution_cost",
    "net_pnl",
    "sizing_mode",
    "risk_budget_usdt",
    "estimated_net_loss_usdt",
    "gap_exit",
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
    "entry_time_ms",
    "entry_price",
    "exit_time",
    "exit_time_ms",
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
    "entry_trend_atr",
    "entry_execution_interval",
    "entry_atr_execution",
    "entry_atr_execution_pct",
    "entry_atr_execution_percentile",
    "pullback_distance_atr",
    "initial_stop_distance_pct",
    "initial_take_profit_distance_pct",
    "entry_zscore",
    "entry_regime_interval",
    "entry_regime_fast_ema",
    "entry_regime_slow_ema",
    "entry_regime_slope_pct",
    "entry_donchian_high",
    "entry_donchian_low",
    "entry_volume_ratio",
    "expected_reward_to_cost_ratio",
    "expected_net_reward_to_risk_ratio",
    "gross_pnl",
    "fees",
    "slippage",
    "spread_cost",
    "funding_pnl",
    "execution_cost",
    "net_pnl",
    "sizing_mode",
    "risk_budget_usdt",
    "estimated_net_loss_usdt",
    "gap_exit",
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
    "entry_momentum_score",
    "entry_fast_return",
    "entry_slow_return",
    "entry_realized_volatility",
    "entry_cross_section_rank",
    "entry_universe_size",
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
    max_net_loss_per_trade_usdt: float = 0.02
    max_risk_per_trade_pct: float = 0.01
    max_account_margin_fraction: float = 0.50
    gap_risk_buffer_pct: float = 0.0010
    loop_sleep_seconds: float = 15.0
    dry_run: bool = True
    testnet: bool = True
    live_trading: bool = False
    shadow_mode: bool = True
    taker_fee_rate: float = 0.0005
    shadow_slippage_bps: float = 2.0
    shadow_spread_bps: float = 1.0
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
    forward_shadow_execution_enabled: bool = False
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
    profile_name: str = CANDIDATE_PROFILE_V4
    engine_name: str = "trend_pullback_15m_1h_rolling_walk_forward_v4"
    default_replay_days: int = 600
    source_interval: str = "1m"
    execution_interval: str = "15m"
    trend_interval: str = "1h"
    regime_interval: Optional[str] = None
    strategy_ids: Tuple[str, ...] = CANDIDATE_LEDGER_NAMES
    trend_fast_ema_period: int = 20
    trend_slow_ema_period: int = 50
    trend_atr_period: int = 14
    trend_persistence_bars: int = 4
    slope_consistency_window: int = 5
    min_slope_consistency: float = 0.80
    min_ema_spread_pct: float = 0.0020
    regime_fast_ema_period: int = 50
    regime_slow_ema_period: int = 200
    regime_slope_lookback: int = 3
    breakout_enabled: bool = False
    donchian_lookback: int = 20
    volume_lookback: int = 20
    volume_expansion_multiplier: float = 1.30
    atr_execution_period: int = 14
    atr_percentile_window: int = 96
    max_atr_percentile: float = 0.70
    max_pullback_distance_atr: float = 0.70
    stop_loss_atr_multiple: float = 1.50
    take_profit_atr_multiple: float = 3.00
    target_reward_to_risk_multiple: Optional[float] = None
    minimum_stop_loss_pct: float = 0.0055
    maximum_stop_loss_pct: float = 0.0200
    minimum_take_profit_pct: float = 0.0120
    maximum_take_profit_pct: float = 0.0400
    max_holding_minutes: int = 480
    cooldown_minutes: int = 60
    fixed_notional_usdt: float = 10.0
    starting_equity_usdt: float = 5.0
    executable_max_net_loss_per_trade_usdt: float = 0.05
    executable_max_risk_per_trade_pct: float = 0.01
    walk_forward_train_days: int = 180
    walk_forward_validation_days: int = 60
    walk_forward_test_days: int = 60
    walk_forward_step_days: int = 60
    embargo_minutes: int = 240
    minimum_walk_forward_windows: int = 6
    cost_stress_multipliers: Tuple[float, ...] = (1.0, 1.5, 2.0)
    bootstrap_samples: int = 1000
    bootstrap_block_trades: int = 8
    maximum_tp_cost_ratio: float = 0.20
    minimum_reward_to_cost_ratio: float = 0.0
    minimum_net_reward_to_risk_ratio: float = 0.0
    anticipated_funding_cost_pct: float = 0.0
    minimum_train_trades: int = 100
    minimum_validation_trades: Optional[int] = None
    minimum_out_of_sample_trades: int = 100
    minimum_positive_window_fraction: float = 0.60
    minimum_net_profit_factor: float = 1.15
    minimum_train_net_profit_factor: Optional[float] = None
    minimum_validation_net_profit_factor: Optional[float] = None
    minimum_test_net_profit_factor: Optional[float] = None
    maximum_drawdown_usdt: float = 0.20
    enforce_fixed_notional_drawdown: bool = True
    maximum_risk_sized_drawdown_usdt: float = 0.20
    maximum_cost_to_gross_profit_ratio: float = 0.30
    requires_fresh_forward_validation: bool = True
    train_cost_gate_enabled: bool = False
    cost_gate_stress_multiplier: float = 1.50
    cost_gate_min_bucket_trades: int = 30
    cost_gate_min_validation_trades: int = 20
    cost_gate_min_net_profit_factor: float = 1.15
    minimum_risk_sized_coverage: float = 0.0
    minimum_cross_symbol_qualified_fraction: float = 0.75
    portfolio_symbols: Tuple[str, ...] = ("SOLUSDT", "BTCUSDT", "ETHUSDT", "BNBUSDT")
    portfolio_symbol_qualification_mode: str = "all_strategies"


def candidate_replay_parameters_for_profile(profile_name: str) -> CandidateReplayParameters:
    normalized = str(profile_name or "").strip().lower()
    base = CandidateReplayParameters()
    if normalized == CANDIDATE_PROFILE_V4:
        return base
    if normalized == CANDIDATE_PROFILE_V3:
        return replace(
            base,
            profile_name=CANDIDATE_PROFILE_V3,
            engine_name="trend_pullback_5m_15m_rolling_walk_forward_v3",
            default_replay_days=600,
            execution_interval="5m",
            trend_interval="15m",
            atr_percentile_window=288,
            minimum_stop_loss_pct=0.0035,
            maximum_stop_loss_pct=0.0120,
            minimum_take_profit_pct=0.0080,
            maximum_take_profit_pct=0.0240,
            max_holding_minutes=120,
            cooldown_minutes=15,
            executable_max_net_loss_per_trade_usdt=0.04,
            walk_forward_train_days=120,
            walk_forward_validation_days=30,
            walk_forward_test_days=30,
            walk_forward_step_days=30,
        )
    if normalized == CANDIDATE_PROFILE_V5:
        return replace(
            base,
            profile_name=CANDIDATE_PROFILE_V5,
            engine_name="trend_pullback_30m_2h_rolling_walk_forward_v5",
            default_replay_days=900,
            execution_interval="30m",
            trend_interval="2h",
            atr_percentile_window=48,
            minimum_stop_loss_pct=0.0060,
            maximum_stop_loss_pct=0.0250,
            minimum_take_profit_pct=0.0150,
            maximum_take_profit_pct=0.0500,
            max_holding_minutes=960,
            cooldown_minutes=120,
            walk_forward_train_days=270,
            walk_forward_validation_days=90,
            walk_forward_test_days=90,
            walk_forward_step_days=90,
            embargo_minutes=960,
        )
    if normalized == CANDIDATE_PROFILE_V6:
        v5 = candidate_replay_parameters_for_profile(CANDIDATE_PROFILE_V5)
        return replace(
            v5,
            profile_name=CANDIDATE_PROFILE_V6,
            engine_name="long_cost_gate_30m_2h_rolling_walk_forward_v6",
            source_interval="30m",
            strategy_ids=(CANDIDATE_TREND_LONG_NAME,),
            fixed_notional_usdt=100.0,
            train_cost_gate_enabled=True,
            minimum_risk_sized_coverage=0.50,
        )
    if normalized == CANDIDATE_PROFILE_V7:
        return replace(
            base,
            strategy_name="COST_AWARE_TREND_BREAKOUT_V7",
            profile_name=CANDIDATE_PROFILE_V7,
            engine_name="cost_aware_breakout_15m_1h_4h_rolling_walk_forward_v7",
            default_replay_days=720,
            source_interval="15m",
            execution_interval="15m",
            trend_interval="1h",
            regime_interval="4h",
            strategy_ids=(CANDIDATE_BREAKOUT_LONG_NAME, CANDIDATE_BREAKOUT_SHORT_NAME),
            trend_fast_ema_period=50,
            trend_slow_ema_period=200,
            breakout_enabled=True,
            stop_loss_atr_multiple=1.0,
            take_profit_atr_multiple=3.0,
            target_reward_to_risk_multiple=3.0,
            minimum_stop_loss_pct=0.0070,
            maximum_stop_loss_pct=0.0300,
            minimum_take_profit_pct=0.0210,
            maximum_take_profit_pct=0.0900,
            max_holding_minutes=72 * 60,
            cooldown_minutes=240,
            fixed_notional_usdt=100.0,
            walk_forward_train_days=180,
            walk_forward_validation_days=90,
            walk_forward_test_days=90,
            walk_forward_step_days=90,
            embargo_minutes=72 * 60,
            minimum_walk_forward_windows=5,
            minimum_train_trades=50,
            minimum_validation_trades=30,
            minimum_out_of_sample_trades=50,
            minimum_train_net_profit_factor=1.20,
            minimum_validation_net_profit_factor=1.15,
            minimum_test_net_profit_factor=1.15,
            minimum_reward_to_cost_ratio=5.0,
            minimum_net_reward_to_risk_ratio=2.0,
            anticipated_funding_cost_pct=0.0003,
            train_cost_gate_enabled=False,
            enforce_fixed_notional_drawdown=False,
            maximum_risk_sized_drawdown_usdt=0.20,
            minimum_risk_sized_coverage=0.50,
        )
    if normalized == CANDIDATE_PROFILE_V8:
        return replace(
            base,
            strategy_name="COST_AWARE_TREND_PULLBACK_V8",
            profile_name=CANDIDATE_PROFILE_V8,
            engine_name="cost_aware_pullback_15m_1h_4h_rolling_walk_forward_v8",
            default_replay_days=720,
            source_interval="15m",
            execution_interval="15m",
            trend_interval="1h",
            regime_interval="4h",
            strategy_ids=(
                CANDIDATE_REGIME_PULLBACK_LONG_NAME,
                CANDIDATE_REGIME_PULLBACK_SHORT_NAME,
            ),
            trend_fast_ema_period=20,
            trend_slow_ema_period=50,
            trend_persistence_bars=6,
            slope_consistency_window=6,
            min_slope_consistency=2.0 / 3.0,
            min_ema_spread_pct=0.0015,
            max_atr_percentile=0.80,
            max_pullback_distance_atr=0.75,
            stop_loss_atr_multiple=1.25,
            take_profit_atr_multiple=3.125,
            target_reward_to_risk_multiple=2.5,
            minimum_stop_loss_pct=0.0060,
            maximum_stop_loss_pct=0.0250,
            minimum_take_profit_pct=0.0150,
            maximum_take_profit_pct=0.0625,
            max_holding_minutes=48 * 60,
            cooldown_minutes=240,
            fixed_notional_usdt=100.0,
            walk_forward_train_days=180,
            walk_forward_validation_days=90,
            walk_forward_test_days=90,
            walk_forward_step_days=90,
            embargo_minutes=48 * 60,
            minimum_walk_forward_windows=5,
            minimum_train_trades=50,
            minimum_validation_trades=30,
            minimum_out_of_sample_trades=30,
            minimum_train_net_profit_factor=1.20,
            minimum_validation_net_profit_factor=1.15,
            minimum_test_net_profit_factor=1.10,
            minimum_reward_to_cost_ratio=5.0,
            minimum_net_reward_to_risk_ratio=1.5,
            anticipated_funding_cost_pct=0.0003,
            train_cost_gate_enabled=False,
            enforce_fixed_notional_drawdown=False,
            maximum_risk_sized_drawdown_usdt=0.20,
            minimum_risk_sized_coverage=0.50,
        )
    if normalized == CANDIDATE_PROFILE_V9:
        v8 = candidate_replay_parameters_for_profile(CANDIDATE_PROFILE_V8)
        return replace(
            v8,
            strategy_name="CROSS_ASSET_TREND_PULLBACK_V9",
            profile_name=CANDIDATE_PROFILE_V9,
            engine_name="cross_asset_pullback_15m_1h_4h_rolling_walk_forward_v9",
            strategy_ids=(
                CANDIDATE_CROSS_ASSET_PULLBACK_LONG_NAME,
                CANDIDATE_CROSS_ASSET_PULLBACK_SHORT_NAME,
            ),
            portfolio_symbols=("BTCUSDT", "ETHUSDT"),
            minimum_cross_symbol_qualified_fraction=1.0,
            portfolio_symbol_qualification_mode="any_strategy",
        )
    raise ConfigError(f"Unsupported candidate replay profile: {profile_name}")


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
class FeasibilityScanParameters:
    balance_usdt: float = 5.0
    max_net_loss_usdt: float = 0.05
    max_risk_pct: float = 0.01
    primary_stop_loss_pct: float = 0.01
    stop_loss_scenarios: Tuple[float, ...] = (0.006, 0.01, 0.015)
    capital_scenarios_usdt: Tuple[float, ...] = (
        5.0,
        7.5,
        10.0,
        15.0,
        20.0,
        25.0,
        50.0,
        100.0,
    )
    minimum_quote_volume_usdt: float = 10_000_000.0
    maximum_book_spread_pct: float = 0.001
    recommended_symbol_limit: int = 25


@dataclass(frozen=True)
class MomentumRotationParameters:
    strategy_name: str = V10_MOMENTUM_STRATEGY
    profile_name: str = V10_MOMENTUM_PROFILE
    default_replay_days: int = 720
    interval: str = "4h"
    symbols: Tuple[str, ...] = (
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "DOGEUSDT",
        "ADAUSDT",
        "ZECUSDT",
    )
    fast_momentum_bars: int = 42
    slow_momentum_bars: int = 180
    volatility_lookback_bars: int = 42
    ema_period: int = 50
    minimum_cross_section_size: int = 4
    minimum_absolute_score: float = 1.0
    stop_loss_atr_multiple: float = 1.5
    reward_to_risk_multiple: float = 2.5
    minimum_stop_loss_pct: float = 0.01
    maximum_stop_loss_pct: float = 0.03
    maximum_holding_bars: int = 42
    fixed_notional_usdt: float = 100.0
    capital_scenarios_usdt: Tuple[float, ...] = (5.0, 7.5, 10.0, 15.0)
    qualification_capital_usdt: float = 10.0
    max_risk_pct: float = 0.01
    max_account_margin_fraction: float = 0.50
    anticipated_funding_cost_pct: float = 0.0003
    minimum_reward_to_cost_ratio: float = 5.0
    walk_forward_train_days: int = 180
    walk_forward_validation_days: int = 90
    walk_forward_test_days: int = 90
    walk_forward_step_days: int = 90
    embargo_minutes: int = 7 * 24 * 60
    minimum_walk_forward_windows: int = 5
    minimum_train_trades: int = 20
    minimum_validation_trades: int = 10
    minimum_test_trades: int = 30
    minimum_train_profit_factor: float = 1.20
    minimum_validation_profit_factor: float = 1.15
    minimum_test_profit_factor: float = 1.15
    minimum_positive_test_window_fraction: float = 0.60
    minimum_capital_execution_coverage: float = 0.80
    maximum_qualification_drawdown_usdt: float = 0.20
    cost_stress_multipliers: Tuple[float, ...] = (1.0, 1.5, 2.0)
    bootstrap_samples: int = 1000
    bootstrap_block_trades: int = 8


@dataclass(frozen=True)
class RiskSizingPlan:
    quantity: float
    target_notional: float
    risk_budget_usdt: float
    estimated_net_loss_usdt: float
    minimum_order_estimated_loss_usdt: float
    loss_rate: float
    skip_reason: str


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
                if not config.forward_shadow_execution_enabled:
                    signal_row["trade_decision"] = "REJECT"
                    signal_row["failed_filter"] = "research_phase"
                    signal_row["rejection_reason"] = "FORWARD_SHADOW_EXECUTION_DISABLED"
                elif config.market_gate_enforced and not bool(signal_row.get("trade_allowed_by_market_gate", False)):
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
                    sizing = build_risk_sizing_plan(
                        balance=self.balance,
                        price=decision.close_price,
                        filters=filters,
                        config=config,
                    )
                    signal_row["risk_budget_usdt"] = sizing.risk_budget_usdt
                    signal_row["estimated_net_loss_usdt"] = sizing.estimated_net_loss_usdt
                    signal_row["minimum_order_estimated_loss_usdt"] = (
                        sizing.minimum_order_estimated_loss_usdt
                    )
                    signal_row["risk_sizing_skip_reason"] = sizing.skip_reason
                    if sizing.quantity <= 0 or not validate_order(
                        sizing.quantity, decision.close_price, filters, config
                    ):
                        signal_row["trade_decision"] = "REJECT"
                        signal_row["failed_filter"] = "order_size"
                        signal_row["rejection_reason"] = (
                            sizing.skip_reason or "shadow order size does not satisfy exchange filters"
                        )
                    else:
                        position = open_router_shadow_position(config, decision, sizing.quantity)
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
        max_net_loss_per_trade_usdt=parse_float_env(
            "MAX_NET_LOSS_PER_TRADE_USDT", 0.02, minimum=0.0
        ),
        max_risk_per_trade_pct=parse_float_env("MAX_RISK_PER_TRADE_PCT", 0.01, minimum=0.0),
        max_account_margin_fraction=parse_float_env(
            "MAX_ACCOUNT_MARGIN_FRACTION", 0.50, minimum=0.0
        ),
        gap_risk_buffer_pct=parse_float_env("GAP_RISK_BUFFER_PCT", 0.0010, minimum=0.0),
        loop_sleep_seconds=parse_float_env("LOOP_SLEEP_SECONDS", 15.0, minimum=1.0),
        dry_run=parse_bool_env("DRY_RUN", True),
        testnet=parse_bool_env("TESTNET", True),
        live_trading=parse_bool_env("LIVE_TRADING", False),
        shadow_mode=parse_bool_env("SHADOW_MODE", True),
        taker_fee_rate=parse_float_env("TAKER_FEE_RATE", 0.0005, minimum=0.0),
        shadow_slippage_bps=parse_float_env("SHADOW_SLIPPAGE_BPS", 2.0, minimum=0.0),
        shadow_spread_bps=parse_float_env("SHADOW_SPREAD_BPS", 1.0, minimum=0.0),
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
        forward_shadow_execution_enabled=parse_bool_env(
            "FORWARD_SHADOW_EXECUTION_ENABLED",
            False,
        ),
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
    if config.max_risk_per_trade_pct > 1:
        raise ConfigError("MAX_RISK_PER_TRADE_PCT must be <= 1")
    if config.max_account_margin_fraction > 1:
        raise ConfigError("MAX_ACCOUNT_MARGIN_FRACTION must be <= 1")
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


def exchange_filters_from_symbol_info(item: Dict[str, Any]) -> ExchangeFilters:
    filters = {entry.get("filterType"): entry for entry in item.get("filters", [])}
    lot = filters.get("LOT_SIZE", {})
    market_lot = filters.get("MARKET_LOT_SIZE", {})
    market_step = float(market_lot.get("stepSize", "0") or 0.0)
    lot_source = market_lot if market_step > 0 else lot
    notional = filters.get("MIN_NOTIONAL", {})
    price_filter = filters.get("PRICE_FILTER", {})
    return ExchangeFilters(
        step_size=float(lot_source.get("stepSize", lot.get("stepSize", "0.001"))),
        tick_size=float(price_filter.get("tickSize", "0.01")),
        min_qty=float(lot_source.get("minQty", lot.get("minQty", "0.0"))),
        max_qty=float(lot_source.get("maxQty", lot.get("maxQty", "100000000"))),
        min_notional=float(notional.get("notional", notional.get("minNotional", "0.0"))),
        quantity_precision=int(item.get("quantityPrecision", 3)),
        price_precision=int(item.get("pricePrecision", 2)),
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
        parsed = exchange_filters_from_symbol_info(item)
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


def load_funding_rate_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=FUNDING_RATE_COLUMNS)
    frame = pd.read_csv(path)
    missing = [column for column in FUNDING_RATE_COLUMNS if column not in frame.columns]
    if missing:
        raise ConfigError(f"Funding rate cache is missing columns: {','.join(missing)}")
    for column in FUNDING_RATE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame.dropna(subset=FUNDING_RATE_COLUMNS, inplace=True)
    frame["funding_time"] = frame["funding_time"].astype("int64")
    frame.drop_duplicates(subset=["funding_time"], keep="last", inplace=True)
    frame.sort_values("funding_time", inplace=True)
    return frame[FUNDING_RATE_COLUMNS].reset_index(drop=True)


def write_funding_rate_cache(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    frame[FUNDING_RATE_COLUMNS].to_csv(temp_path, index=False)
    os.replace(temp_path, path)


def fetch_funding_rates_range(
    client: Any,
    symbol: str,
    start_ms: int,
    end_ms: int,
    retry_attempts: int,
) -> pd.DataFrame:
    cursor = int(start_ms)
    rows: List[Dict[str, Any]] = []
    while cursor <= end_ms:
        payload = retry_api_call(
            f"Fetch funding rates for {symbol}",
            client.futures_funding_rate,
            symbol=symbol,
            startTime=cursor,
            endTime=int(end_ms),
            limit=1000,
            attempts=retry_attempts,
        )
        if not payload:
            break
        for item in payload:
            funding_time = int(item.get("fundingTime", 0) or 0)
            if funding_time < start_ms or funding_time > end_ms:
                continue
            rows.append(
                {
                    "funding_time": funding_time,
                    "funding_rate": float(item.get("fundingRate", 0.0) or 0.0),
                    "mark_price": float(item.get("markPrice", 0.0) or 0.0),
                }
            )
        last_time = max(int(item.get("fundingTime", 0) or 0) for item in payload)
        next_cursor = last_time + 1
        if next_cursor <= cursor:
            raise RuntimeError("Funding rate pagination did not advance")
        cursor = next_cursor
        if len(payload) < 1000:
            break
    if not rows:
        return pd.DataFrame(columns=FUNDING_RATE_COLUMNS)
    frame = pd.DataFrame(rows, columns=FUNDING_RATE_COLUMNS)
    frame.drop_duplicates(subset=["funding_time"], keep="last", inplace=True)
    frame.sort_values("funding_time", inplace=True)
    return frame.reset_index(drop=True)


def ensure_funding_rate_cache(
    client: Any,
    config: Config,
    start_ms: int,
    end_ms: int,
    cache_path: Path,
) -> pd.DataFrame:
    cached = load_funding_rate_cache(cache_path)
    twelve_hours_ms = 12 * 60 * 60 * 1000
    covered = (
        not cached.empty
        and int(cached["funding_time"].min()) <= start_ms + twelve_hours_ms
        and int(cached["funding_time"].max()) >= end_ms - twelve_hours_ms
    )
    if not covered:
        downloaded = fetch_funding_rates_range(
            client,
            config.symbol,
            start_ms,
            end_ms,
            config.retry_attempts,
        )
        if downloaded.empty:
            raise RuntimeError("Binance returned no funding data for the requested replay period")
        frames = ([cached] if not cached.empty else []) + [downloaded]
        cached = pd.concat(frames, ignore_index=True)
        cached.drop_duplicates(subset=["funding_time"], keep="last", inplace=True)
        cached.sort_values("funding_time", inplace=True)
        cached.reset_index(drop=True, inplace=True)
        write_funding_rate_cache(cache_path, cached)
    selected = cached[
        (cached["funding_time"] >= start_ms) & (cached["funding_time"] <= end_ms)
    ].copy()
    if selected.empty:
        raise RuntimeError("Funding rate cache does not cover the requested replay period")
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


def decimal_ceil(value: float, step: float) -> float:
    if step <= 0:
        return value
    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    return float((value_decimal / step_decimal).to_integral_value(rounding=ROUND_UP) * step_decimal)


def round_trip_execution_cost_rate(config: Config, multiplier: float = 1.0) -> float:
    per_side_slippage = config.shadow_slippage_bps / 10000.0
    per_side_spread = config.shadow_spread_bps / 10000.0
    return 2.0 * multiplier * (config.taker_fee_rate + per_side_slippage + per_side_spread)


def estimated_worst_case_loss_usdt(
    notional: float,
    stop_loss_pct: float,
    config: Config,
    multiplier: float = 1.0,
) -> float:
    if notional <= 0:
        return 0.0
    loss_rate = (
        stop_loss_pct
        + config.gap_risk_buffer_pct
        + round_trip_execution_cost_rate(config, multiplier)
    )
    return notional * loss_rate


def build_risk_sizing_plan(
    balance: float,
    price: float,
    filters: ExchangeFilters,
    config: Config,
    stop_loss_pct: Optional[float] = None,
) -> RiskSizingPlan:
    stop_pct = config.stop_loss_pct if stop_loss_pct is None else float(stop_loss_pct)
    loss_rate = stop_pct + config.gap_risk_buffer_pct + round_trip_execution_cost_rate(config)
    empty = RiskSizingPlan(0.0, 0.0, 0.0, 0.0, 0.0, loss_rate, "")
    if balance <= 0:
        return replace(empty, skip_reason="INVALID_OR_EMPTY_BALANCE")
    if price <= 0 or loss_rate <= 0:
        return replace(empty, skip_reason="INVALID_PRICE_OR_LOSS_RATE")

    absolute_budget = config.max_net_loss_per_trade_usdt
    percentage_budget = balance * config.max_risk_per_trade_pct
    budgets = [value for value in (absolute_budget, percentage_budget) if value > 0]
    if not budgets:
        return replace(empty, skip_reason="RISK_BUDGET_DISABLED")
    risk_budget = min(budgets)

    minimum_quantity = decimal_ceil(
        max(filters.min_qty, filters.min_notional / price),
        filters.step_size,
    )
    minimum_notional = minimum_quantity * price
    minimum_loss = estimated_worst_case_loss_usdt(minimum_notional, stop_pct, config)
    base = replace(
        empty,
        risk_budget_usdt=risk_budget,
        minimum_order_estimated_loss_usdt=minimum_loss,
    )
    if minimum_quantity > filters.max_qty:
        return replace(base, skip_reason="MINIMUM_QUANTITY_EXCEEDS_MAXIMUM")
    if minimum_loss > risk_budget + 1e-12:
        return replace(base, skip_reason="MINIMUM_ORDER_EXCEEDS_RISK_BUDGET")

    max_notional_from_risk = risk_budget / loss_rate
    configured_margin = min(config.max_margin_usdt, balance * config.max_account_margin_fraction)
    max_notional_from_margin = configured_margin * config.leverage
    target_notional = min(
        config.max_notional_usdt,
        max_notional_from_margin,
        max_notional_from_risk,
    )
    quantity = decimal_floor(target_notional / price, filters.step_size)
    quantity = min(quantity, filters.max_qty)
    if quantity < minimum_quantity:
        return replace(base, skip_reason="SIZED_QUANTITY_BELOW_EXCHANGE_MINIMUM")
    actual_notional = quantity * price
    estimated_loss = estimated_worst_case_loss_usdt(actual_notional, stop_pct, config)
    if estimated_loss > risk_budget + 1e-12:
        return replace(base, skip_reason="SIZED_ORDER_EXCEEDS_RISK_BUDGET")
    return RiskSizingPlan(
        quantity=round(quantity, filters.quantity_precision),
        target_notional=actual_notional,
        risk_budget_usdt=risk_budget,
        estimated_net_loss_usdt=estimated_loss,
        minimum_order_estimated_loss_usdt=minimum_loss,
        loss_rate=loss_rate,
        skip_reason="",
    )


def calculate_order_quantity(balance: float, price: float, filters: ExchangeFilters, config: Config) -> float:
    return build_risk_sizing_plan(balance, price, filters, config).quantity


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
        "spread_cost_usdt": 0.0,
        "funding_pnl_usdt": 0.0,
        "execution_cost_usdt": 0.0,
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
    spread_cost = (entry_notional + exit_notional) * config.shadow_spread_bps / 10000.0
    execution_cost = fees + slippage + spread_cost
    net_pnl = gross_pnl - execution_cost
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
        "spread_cost": spread_cost,
        "funding_pnl": 0.0,
        "execution_cost": execution_cost,
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
    spread_cost = (entry_notional + exit_notional) * config.shadow_spread_bps / 10000.0
    execution_cost = fees + slippage + spread_cost
    net_pnl = gross_pnl - execution_cost
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
        "spread_cost": spread_cost,
        "funding_pnl": 0.0,
        "execution_cost": execution_cost,
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
        "spread_cost": sum(float(trade.get("spread_cost", 0.0) or 0.0) for trade in trade_list),
        "funding_pnl": sum(float(trade.get("funding_pnl", 0.0) or 0.0) for trade in trade_list),
        "execution_cost": sum(
            float(
                trade.get(
                    "execution_cost",
                    float(trade.get("fees", 0.0) or 0.0)
                    + float(trade.get("slippage", 0.0) or 0.0)
                    + float(trade.get("spread_cost", 0.0) or 0.0),
                )
                or 0.0
            )
            for trade in trade_list
        ),
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
    spread_cost = sum(float(trade.get("spread_cost", 0.0) or 0.0) for trade in trade_list)
    funding_pnl = sum(float(trade.get("funding_pnl", 0.0) or 0.0) for trade in trade_list)
    execution_cost = sum(
        float(
            trade.get(
                "execution_cost",
                float(trade.get("fees", 0.0) or 0.0)
                + float(trade.get("slippage", 0.0) or 0.0)
                + float(trade.get("spread_cost", 0.0) or 0.0),
            )
            or 0.0
        )
        for trade in trade_list
    )
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
        "spread_cost": spread_cost,
        "funding_pnl": funding_pnl,
        "execution_cost": execution_cost,
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


def normalize_shadow_trade_costs(trade: Dict[str, Any], config: Config) -> None:
    entry_notional = float(trade.get("entry_notional", 0.0) or 0.0)
    exit_notional = float(trade.get("exit_notional", 0.0) or 0.0)
    round_trip_notional = entry_notional + exit_notional
    fees = float(trade.get("fees", round_trip_notional * config.taker_fee_rate) or 0.0)
    slippage = float(
        trade.get(
            "slippage",
            round_trip_notional * config.shadow_slippage_bps / 10000.0,
        )
        or 0.0
    )
    spread_cost = float(
        trade.get(
            "spread_cost",
            round_trip_notional * config.shadow_spread_bps / 10000.0,
        )
        or 0.0
    )
    funding_pnl = float(trade.get("funding_pnl", 0.0) or 0.0)
    gross_pnl = float(trade.get("gross_pnl", 0.0) or 0.0)
    execution_cost = fees + slippage + spread_cost
    net_pnl = gross_pnl + funding_pnl - execution_cost
    trade.update(
        {
            "fees": fees,
            "slippage": slippage,
            "spread_cost": spread_cost,
            "funding_pnl": funding_pnl,
            "execution_cost": execution_cost,
            "net_pnl": net_pnl,
            "result": "WIN" if net_pnl > 0 else "LOSS",
        }
    )


def recompute_shadow_metrics(shadow: Dict[str, Any], config: Optional[Config] = None) -> None:
    ensure_shadow_schema(shadow)
    detect_execution_contamination(shadow)
    if config is None:
        config = Config()
    trades = list(shadow.get("trades") or [])
    for trade in trades:
        normalize_shadow_trade_costs(trade, config)
    report = build_decision_report(trades)
    if not shadow.get("dataset_valid", True):
        report["dataset_valid"] = False
        report["execution_contamination_detected"] = True
        report["contaminated_trade_count"] = int(shadow.get("contaminated_trade_count", 0) or 0)
        report["viability_status"] = INVALID_CONTAMINATED_DATASET
    shadow["completed_trades"] = len(trades)
    shadow["trades"] = trades
    shadow["recent_trades"] = list(reversed(trades[-20:]))
    shadow["win_rate"] = report["win_rate"]
    shadow["net_pnl_usdt"] = report["net_pnl"]
    shadow["gross_pnl_usdt"] = report["gross_pnl"]
    shadow["fees_usdt"] = report["fees"]
    shadow["slippage_usdt"] = report["slippage"]
    shadow["spread_cost_usdt"] = report["spread_cost"]
    shadow["funding_pnl_usdt"] = report["funding_pnl"]
    shadow["execution_cost_usdt"] = report["execution_cost"]
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
    shadow["latest_decision_report"] = report


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


def rolling_walk_forward_boundaries(
    days: int,
    end_exclusive_ms: int,
    params: CandidateReplayParameters,
) -> List[Dict[str, Any]]:
    day_ms = 24 * 60 * 60 * 1000
    minute_ms = 60 * 1000
    span_days = (
        params.walk_forward_train_days
        + params.walk_forward_validation_days
        + params.walk_forward_test_days
    )
    if days < span_days:
        raise ConfigError(
            f"Candidate replay requires at least {span_days} days for one walk-forward window"
        )
    if params.walk_forward_step_days <= 0 or params.embargo_minutes < 0:
        raise ConfigError("Walk-forward step must be positive and embargo must be non-negative")
    train_ms = params.walk_forward_train_days * day_ms
    validation_ms = params.walk_forward_validation_days * day_ms
    test_ms = params.walk_forward_test_days * day_ms
    step_ms = params.walk_forward_step_days * day_ms
    embargo_ms = params.embargo_minutes * minute_ms
    span_ms = train_ms + validation_ms + test_ms
    available_ms = days * day_ms
    window_count = int((available_ms - span_ms) // step_ms) + 1
    overall_start_ms = end_exclusive_ms - (span_ms + (window_count - 1) * step_ms)
    windows: List[Dict[str, Any]] = []
    raw_start_ms = overall_start_ms
    while raw_start_ms + train_ms + validation_ms + test_ms <= end_exclusive_ms:
        raw_train_end = raw_start_ms + train_ms
        raw_validation_end = raw_train_end + validation_ms
        raw_test_end = raw_validation_end + test_ms
        train = (raw_start_ms, raw_train_end - embargo_ms)
        validation = (raw_train_end, raw_validation_end - embargo_ms)
        test = (raw_validation_end, raw_test_end)
        if train[0] >= train[1] or validation[0] >= validation[1] or test[0] >= test[1]:
            raise ConfigError("Walk-forward embargo leaves an empty segment")
        windows.append(
            {
                "window_id": f"WF_{len(windows) + 1:02d}",
                "train": train,
                "validation": validation,
                "test": test,
                "embargo_minutes": params.embargo_minutes,
            }
        )
        raw_start_ms += step_ms
    return windows


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


def public_market_rows_by_symbol(payload: Any) -> Dict[str, Dict[str, Any]]:
    rows = payload if isinstance(payload, list) else [payload]
    return {
        str(row.get("symbol", "")).upper(): row
        for row in rows
        if isinstance(row, dict) and row.get("symbol")
    }


def feasibility_scenario_key(stop_loss_pct: float) -> str:
    return f"stop_{float(stop_loss_pct):.6f}"


def capital_scenario_key(balance_usdt: float) -> str:
    return f"balance_{float(balance_usdt):.2f}"


def validate_feasibility_scan_parameters(params: FeasibilityScanParameters) -> None:
    if params.balance_usdt <= 0:
        raise ConfigError("Feasibility scan balance must be positive")
    if params.max_net_loss_usdt <= 0 or not 0 < params.max_risk_pct <= 1:
        raise ConfigError("Feasibility scan risk limits are invalid")
    if params.primary_stop_loss_pct <= 0:
        raise ConfigError("Feasibility scan primary stop loss must be positive")
    if not params.stop_loss_scenarios or any(value <= 0 for value in params.stop_loss_scenarios):
        raise ConfigError("Feasibility scan stop loss scenarios must be positive")
    if not params.capital_scenarios_usdt or any(value <= 0 for value in params.capital_scenarios_usdt):
        raise ConfigError("Feasibility scan capital scenarios must be positive")
    if params.minimum_quote_volume_usdt < 0 or params.maximum_book_spread_pct < 0:
        raise ConfigError("Feasibility scan liquidity limits cannot be negative")
    if params.recommended_symbol_limit <= 0:
        raise ConfigError("Feasibility scan symbol limit must be positive")


def build_feasibility_scan_rows(
    exchange_info: Dict[str, Any],
    orderbook_tickers: Any,
    market_tickers: Any,
    config: Config,
    params: FeasibilityScanParameters,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    validate_feasibility_scan_parameters(params)
    books = public_market_rows_by_symbol(orderbook_tickers)
    tickers = public_market_rows_by_symbol(market_tickers)
    scenario_values = tuple(sorted(set(params.stop_loss_scenarios + (params.primary_stop_loss_pct,))))
    rows: List[Dict[str, Any]] = []
    errors: Dict[str, str] = {}

    for item in exchange_info.get("symbols", []):
        symbol = str(item.get("symbol", "")).upper()
        if (
            not symbol
            or item.get("status") != "TRADING"
            or item.get("contractType") != "PERPETUAL"
            or item.get("quoteAsset") != "USDT"
            or item.get("marginAsset") != "USDT"
        ):
            continue
        try:
            filters = exchange_filters_from_symbol_info(item)
            if filters.step_size <= 0 or filters.max_qty <= 0:
                raise ConfigError("invalid quantity filters")
            book = books.get(symbol, {})
            ticker = tickers.get(symbol, {})
            bid_price = float(book.get("bidPrice", 0.0) or 0.0)
            ask_price = float(book.get("askPrice", 0.0) or 0.0)
            last_price = float(ticker.get("lastPrice", 0.0) or 0.0)
            if bid_price > 0 and ask_price >= bid_price:
                price = (bid_price + ask_price) / 2.0
                book_spread_pct = (ask_price - bid_price) / price
            elif last_price > 0:
                price = last_price
                book_spread_pct = float("nan")
            else:
                raise ConfigError("missing valid public price")
            quote_volume = float(ticker.get("quoteVolume", 0.0) or 0.0)
            if quote_volume <= 0:
                base_volume = float(ticker.get("volume", 0.0) or 0.0)
                quote_volume = base_volume * price

            observed_per_side_spread_bps = (
                book_spread_pct * 5000.0 if math.isfinite(book_spread_pct) else 0.0
            )
            scan_config = replace(
                config,
                max_net_loss_per_trade_usdt=params.max_net_loss_usdt,
                max_risk_per_trade_pct=params.max_risk_pct,
                shadow_spread_bps=max(config.shadow_spread_bps, observed_per_side_spread_bps),
            )
            risk_budget = min(params.max_net_loss_usdt, params.balance_usdt * params.max_risk_pct)
            minimum_quantity = decimal_ceil(
                max(filters.min_qty, filters.min_notional / price),
                filters.step_size,
            )
            minimum_valid_notional = minimum_quantity * price
            non_stop_loss_rate = (
                scan_config.gap_risk_buffer_pct + round_trip_execution_cost_rate(scan_config)
            )
            maximum_affordable_stop_loss_pct = max(
                0.0,
                risk_budget / minimum_valid_notional - non_stop_loss_rate,
            ) if minimum_valid_notional > 0 else 0.0

            scenario_results: Dict[str, Dict[str, Any]] = {}
            for stop_loss_pct in scenario_values:
                plan = build_risk_sizing_plan(
                    params.balance_usdt,
                    price,
                    filters,
                    scan_config,
                    stop_loss_pct=stop_loss_pct,
                )
                liquidity_reasons: List[str] = []
                if not math.isfinite(book_spread_pct):
                    liquidity_reasons.append("MISSING_ORDERBOOK_SPREAD")
                elif book_spread_pct > params.maximum_book_spread_pct:
                    liquidity_reasons.append("BOOK_SPREAD_ABOVE_LIMIT")
                if quote_volume < params.minimum_quote_volume_usdt:
                    liquidity_reasons.append("QUOTE_VOLUME_BELOW_LIMIT")
                rejection_reasons = ([plan.skip_reason] if plan.skip_reason else []) + liquidity_reasons
                minimum_order_loss = estimated_worst_case_loss_usdt(
                    minimum_valid_notional,
                    stop_loss_pct,
                    scan_config,
                )
                minimum_required_balance_by_risk = minimum_order_loss / params.max_risk_pct
                required_margin = minimum_valid_notional / config.leverage
                minimum_required_balance_by_margin = (
                    required_margin / config.max_account_margin_fraction
                )
                minimum_required_balance = max(
                    minimum_required_balance_by_risk,
                    minimum_required_balance_by_margin,
                )
                capital_independent_reasons = list(liquidity_reasons)
                if minimum_quantity > filters.max_qty:
                    capital_independent_reasons.append("MINIMUM_QUANTITY_EXCEEDS_MAXIMUM")
                capital_scenarios: Dict[str, Dict[str, Any]] = {}
                for capital in sorted(set(params.capital_scenarios_usdt)):
                    capital_reasons = list(capital_independent_reasons)
                    if capital + 1e-12 < minimum_required_balance:
                        capital_reasons.append("CAPITAL_BELOW_REQUIRED_MINIMUM")
                    capital_scenarios[capital_scenario_key(capital)] = {
                        "balance_usdt": capital,
                        "risk_budget_usdt": capital * params.max_risk_pct,
                        "margin_budget_usdt": capital * config.max_account_margin_fraction,
                        "feasible": not capital_reasons,
                        "rejection_reasons": capital_reasons,
                    }
                scenario_results[feasibility_scenario_key(stop_loss_pct)] = {
                    "stop_loss_pct": stop_loss_pct,
                    "quantity": plan.quantity,
                    "target_notional_usdt": plan.target_notional,
                    "estimated_net_loss_usdt": plan.estimated_net_loss_usdt,
                    "minimum_order_estimated_loss_usdt": plan.minimum_order_estimated_loss_usdt,
                    "risk_budget_usdt": plan.risk_budget_usdt,
                    "risk_budget_usage": (
                        plan.minimum_order_estimated_loss_usdt / risk_budget if risk_budget > 0 else None
                    ),
                    "risk_sizing_skip_reason": plan.skip_reason,
                    "rejection_reasons": rejection_reasons,
                    "feasible": not rejection_reasons,
                    "minimum_required_balance_usdt": minimum_required_balance,
                    "minimum_required_balance_by_risk_usdt": minimum_required_balance_by_risk,
                    "minimum_required_balance_by_margin_usdt": minimum_required_balance_by_margin,
                    "required_max_net_loss_usdt": minimum_order_loss,
                    "required_max_margin_usdt": required_margin,
                    "required_max_notional_usdt": minimum_valid_notional,
                    "capital_independent_rejection_reasons": capital_independent_reasons,
                    "capital_scenarios": capital_scenarios,
                }

            primary = scenario_results[feasibility_scenario_key(params.primary_stop_loss_pct)]
            rows.append(
                {
                    "symbol": symbol,
                    "price": price,
                    "bid_price": bid_price,
                    "ask_price": ask_price,
                    "book_spread_pct": book_spread_pct if math.isfinite(book_spread_pct) else None,
                    "quote_volume_24h_usdt": quote_volume,
                    "step_size": filters.step_size,
                    "min_qty": filters.min_qty,
                    "max_qty": filters.max_qty,
                    "min_notional_filter_usdt": filters.min_notional,
                    "minimum_valid_quantity": minimum_quantity,
                    "minimum_valid_notional_usdt": minimum_valid_notional,
                    "risk_budget_usdt": risk_budget,
                    "maximum_affordable_stop_loss_pct": maximum_affordable_stop_loss_pct,
                    "primary_stop_loss_pct": params.primary_stop_loss_pct,
                    "minimum_order_estimated_loss_usdt": primary["minimum_order_estimated_loss_usdt"],
                    "primary_minimum_required_balance_usdt": primary["minimum_required_balance_usdt"],
                    "primary_required_max_net_loss_usdt": primary["required_max_net_loss_usdt"],
                    "primary_required_max_margin_usdt": primary["required_max_margin_usdt"],
                    "primary_required_max_notional_usdt": primary["required_max_notional_usdt"],
                    "risk_budget_usage": primary["risk_budget_usage"],
                    "primary_rejection_reasons": primary["rejection_reasons"],
                    "primary_feasible": primary["feasible"],
                    "stop_loss_scenarios": scenario_results,
                }
            )
        except Exception as exc:
            errors[symbol or f"UNKNOWN_{len(errors) + 1}"] = str(exc)

    rows.sort(
        key=lambda row: (
            not bool(row["primary_feasible"]),
            -float(row["quote_volume_24h_usdt"]),
            float(row["book_spread_pct"] if row["book_spread_pct"] is not None else math.inf),
        )
    )
    return rows, errors


def write_feasibility_scan_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "symbol",
        "price",
        "bid_price",
        "ask_price",
        "book_spread_pct",
        "quote_volume_24h_usdt",
        "step_size",
        "min_qty",
        "max_qty",
        "min_notional_filter_usdt",
        "minimum_valid_quantity",
        "minimum_valid_notional_usdt",
        "risk_budget_usdt",
        "maximum_affordable_stop_loss_pct",
        "primary_stop_loss_pct",
        "minimum_order_estimated_loss_usdt",
        "primary_minimum_required_balance_usdt",
        "primary_required_max_net_loss_usdt",
        "primary_required_max_margin_usdt",
        "primary_required_max_notional_usdt",
        "risk_budget_usage",
        "primary_feasible",
        "primary_rejection_reasons",
        "stop_loss_scenarios",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for source in rows:
            row = dict(source)
            row["primary_rejection_reasons"] = ";".join(source["primary_rejection_reasons"])
            row["stop_loss_scenarios"] = json.dumps(
                source["stop_loss_scenarios"], sort_keys=True, allow_nan=False
            )
            writer.writerow(row)
    os.replace(temp_path, path)


def run_feasibility_scan(
    config: Config,
    params: FeasibilityScanParameters,
    output_dir_value: str,
    client: Any = None,
) -> Dict[str, Any]:
    if config.live_trading or not config.dry_run or not config.shadow_mode:
        raise ConfigError("Feasibility scan requires LIVE_TRADING=false, DRY_RUN=true, and SHADOW_MODE=true")
    validate_feasibility_scan_parameters(params)
    public_client = client or build_public_replay_client(config)
    exchange_info = retry_api_call(
        "Fetch futures exchange info for feasibility scan",
        public_client.futures_exchange_info,
        attempts=config.retry_attempts,
    )
    orderbook_tickers = retry_api_call(
        "Fetch futures order book tickers for feasibility scan",
        public_client.futures_orderbook_ticker,
        attempts=config.retry_attempts,
    )
    market_tickers = retry_api_call(
        "Fetch futures 24h tickers for feasibility scan",
        public_client.futures_ticker,
        attempts=config.retry_attempts,
    )
    rows, errors = build_feasibility_scan_rows(
        exchange_info,
        orderbook_tickers,
        market_tickers,
        config,
        params,
    )
    feasible_rows = [row for row in rows if row["primary_feasible"]]
    recommended_rows = feasible_rows[: params.recommended_symbol_limit]
    scenario_summary: Dict[str, Dict[str, Any]] = {}
    scenario_values = tuple(sorted(set(params.stop_loss_scenarios + (params.primary_stop_loss_pct,))))
    for stop_loss_pct in scenario_values:
        key = feasibility_scenario_key(stop_loss_pct)
        feasible_symbols = [row["symbol"] for row in rows if row["stop_loss_scenarios"][key]["feasible"]]
        structurally_eligible = [
            row
            for row in rows
            if not row["stop_loss_scenarios"][key]["capital_independent_rejection_reasons"]
        ]
        structurally_eligible.sort(
            key=lambda row: row["stop_loss_scenarios"][key]["minimum_required_balance_usdt"]
        )
        capital_curve: Dict[str, Dict[str, Any]] = {}
        for capital in sorted(set(params.capital_scenarios_usdt)):
            capital_key = capital_scenario_key(capital)
            capital_symbols = [
                row["symbol"]
                for row in rows
                if row["stop_loss_scenarios"][key]["capital_scenarios"][capital_key]["feasible"]
            ]
            capital_curve[capital_key] = {
                "balance_usdt": capital,
                "feasible_symbol_count": len(capital_symbols),
                "feasible_symbols": capital_symbols[: params.recommended_symbol_limit],
            }
        sol_row = next((row for row in rows if row["symbol"] == "SOLUSDT"), None)
        scenario_summary[key] = {
            "stop_loss_pct": stop_loss_pct,
            "feasible_symbol_count": len(feasible_symbols),
            "feasible_symbols": feasible_symbols[: params.recommended_symbol_limit],
            "minimum_required_balance_for_any_eligible_symbol_usdt": (
                structurally_eligible[0]["stop_loss_scenarios"][key]["minimum_required_balance_usdt"]
                if structurally_eligible
                else None
            ),
            "lowest_capital_symbols": [
                {
                    "symbol": row["symbol"],
                    "minimum_required_balance_usdt": row["stop_loss_scenarios"][key][
                        "minimum_required_balance_usdt"
                    ],
                }
                for row in structurally_eligible[: params.recommended_symbol_limit]
            ],
            "solusdt_minimum_required_balance_usdt": (
                sol_row["stop_loss_scenarios"][key]["minimum_required_balance_usdt"]
                if sol_row is not None
                else None
            ),
            "capital_curve": capital_curve,
        }

    generated_at = utc_now_text()
    report = {
        "engine": FEASIBILITY_SCAN_PROFILE,
        "generated_at": generated_at,
        "parameters": asdict(params),
        "cost_assumptions": {
            "taker_fee_rate_per_side": config.taker_fee_rate,
            "minimum_slippage_bps_per_side": config.shadow_slippage_bps,
            "minimum_spread_bps_per_side": config.shadow_spread_bps,
            "gap_risk_buffer_pct": config.gap_risk_buffer_pct,
            "observed_book_spread_used_when_higher": True,
        },
        "capital_exploration_policy": {
            "risk_fraction_remains_fixed": params.max_risk_pct,
            "max_net_loss_scales_with_balance": True,
            "max_margin_scales_with_account_margin_fraction": config.max_account_margin_fraction,
            "required_max_notional_is_reported_per_symbol": True,
            "simulation_only": True,
        },
        "analyzed_symbol_count": len(rows),
        "market_data_error_count": len(errors),
        "market_data_errors": errors,
        "primary_feasible_symbol_count": len(feasible_rows),
        "recommended_research_symbols": [row["symbol"] for row in recommended_rows],
        "scenario_summary": scenario_summary,
        "rows": rows,
        "research_decision": (
            "RESEARCH_UNIVERSE_AVAILABLE"
            if recommended_rows
            else "NO_FEASIBLE_UNIVERSE_AT_PRIMARY_STOP"
        ),
        "shadow_mode_only": True,
        "dry_run": True,
        "live_trading": False,
        "api_credentials_used": False,
        "account_state_changed": False,
        "eligible_for_micro_live_test": False,
        "source_sha256": file_sha256(Path(__file__).resolve()),
    }
    output_dir = resolve_app_path(output_dir_value)
    compact_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    write_json_atomic(output_dir / "latest.json", report)
    write_json_atomic(output_dir / f"history_{compact_timestamp}.json", report)
    write_feasibility_scan_csv(output_dir / "latest.csv", rows)
    write_feasibility_scan_csv(output_dir / f"history_{compact_timestamp}.csv", rows)
    return report


def validate_momentum_rotation_parameters(params: MomentumRotationParameters) -> None:
    if len(params.symbols) < params.minimum_cross_section_size:
        raise ConfigError("V10 momentum universe is smaller than the minimum cross-section")
    if len(set(params.symbols)) != len(params.symbols):
        raise ConfigError("V10 momentum universe contains duplicate symbols")
    if any(not str(symbol).endswith("USDT") for symbol in params.symbols):
        raise ConfigError("V10 momentum universe must contain only USDT symbols")
    if params.interval != "4h":
        raise ConfigError("V10 momentum interval is frozen at 4h")
    if min(
        params.fast_momentum_bars,
        params.slow_momentum_bars,
        params.volatility_lookback_bars,
        params.ema_period,
        params.maximum_holding_bars,
    ) <= 0:
        raise ConfigError("V10 momentum lookbacks must be positive")
    if params.fast_momentum_bars >= params.slow_momentum_bars:
        raise ConfigError("V10 fast momentum lookback must be shorter than slow momentum")
    if not 0 < params.max_risk_pct <= 1 or not 0 < params.max_account_margin_fraction <= 1:
        raise ConfigError("V10 capital risk limits are invalid")
    if params.qualification_capital_usdt not in params.capital_scenarios_usdt:
        raise ConfigError("V10 qualification capital must be one of the frozen capital scenarios")
    if params.minimum_stop_loss_pct <= 0 or params.maximum_stop_loss_pct < params.minimum_stop_loss_pct:
        raise ConfigError("V10 stop loss bounds are invalid")
    if params.reward_to_risk_multiple <= 1:
        raise ConfigError("V10 reward-to-risk multiple must exceed one")


def build_momentum_feature_frame(
    frame: pd.DataFrame,
    params: MomentumRotationParameters,
) -> pd.DataFrame:
    validate_momentum_rotation_parameters(params)
    features = frame[HISTORICAL_KLINE_COLUMNS].copy().sort_values("close_time").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume"):
        features[column] = pd.to_numeric(features[column], errors="coerce")
    close = features["close"].astype(float)
    log_return = np.log(close / close.shift(1))
    per_bar_volatility = log_return.rolling(params.volatility_lookback_bars).std(ddof=0)
    fast_risk = per_bar_volatility * math.sqrt(params.fast_momentum_bars)
    slow_risk = per_bar_volatility * math.sqrt(params.slow_momentum_bars)
    features["fast_return"] = close.pct_change(params.fast_momentum_bars)
    features["slow_return"] = close.pct_change(params.slow_momentum_bars)
    features["realized_volatility"] = per_bar_volatility
    features["momentum_score"] = (
        0.40 * features["fast_return"] / fast_risk.replace(0.0, np.nan)
        + 0.60 * features["slow_return"] / slow_risk.replace(0.0, np.nan)
    )
    features["ema"] = close.ewm(span=params.ema_period, adjust=False).mean()
    features["atr"] = candidate_true_range(features).rolling(14).mean()
    features["trend_side"] = np.select(
        [
            (close > features["ema"])
            & (features["fast_return"] > 0)
            & (features["slow_return"] > 0),
            (close < features["ema"])
            & (features["fast_return"] < 0)
            & (features["slow_return"] < 0),
        ],
        [1, -1],
        default=0,
    ).astype(int)
    return features


def momentum_snapshot_candidates(
    features_by_symbol: Dict[str, pd.DataFrame],
    close_time_ms: int,
    params: MomentumRotationParameters,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for symbol in params.symbols:
        frame = features_by_symbol.get(symbol)
        if frame is None or frame.empty:
            continue
        matches = frame[frame["close_time"].astype("int64") == int(close_time_ms)]
        if matches.empty:
            continue
        row = matches.iloc[-1]
        values = [
            float(row.get("momentum_score", math.nan)),
            float(row.get("fast_return", math.nan)),
            float(row.get("slow_return", math.nan)),
            float(row.get("realized_volatility", math.nan)),
            float(row.get("atr", math.nan)),
        ]
        if not all(math.isfinite(value) for value in values) or values[3] <= 0 or values[4] <= 0:
            continue
        direction_value = int(row.get("trend_side", 0) or 0)
        score = values[0]
        if direction_value == 0 or abs(score) < params.minimum_absolute_score:
            continue
        if direction_value > 0 and score <= 0:
            continue
        if direction_value < 0 and score >= 0:
            continue
        rows.append(
            {
                "symbol": symbol,
                "side": "LONG" if direction_value > 0 else "SHORT",
                "momentum_score": score,
                "fast_return": values[1],
                "slow_return": values[2],
                "realized_volatility": values[3],
                "atr": values[4],
                "close": float(row["close"]),
                "ema": float(row["ema"]),
            }
        )
    if len(rows) < params.minimum_cross_section_size:
        return []
    rows.sort(key=lambda item: abs(float(item["momentum_score"])), reverse=True)
    for rank, row in enumerate(rows, start=1):
        row["cross_section_rank"] = rank
        row["universe_size"] = len(rows)
    return rows


def is_daily_momentum_rebalance(close_time_ms: int) -> bool:
    day_ms = 24 * 60 * 60 * 1000
    return (int(close_time_ms) + 1) % day_ms == 0


def momentum_fixed_order_quantity(
    entry_price: float,
    filters: ExchangeFilters,
    fixed_notional_usdt: float,
) -> float:
    if entry_price <= 0 or fixed_notional_usdt <= 0:
        return 0.0
    quantity = decimal_floor(fixed_notional_usdt / entry_price, filters.step_size)
    quantity = min(quantity, filters.max_qty)
    if quantity < filters.min_qty or quantity * entry_price < filters.min_notional:
        return 0.0
    return round(quantity, filters.quantity_precision)


def momentum_exit_decision(
    position: Dict[str, Any],
    row: Any,
) -> Optional[Tuple[float, str, bool]]:
    side = str(position["side"])
    stop_price = float(position["stop_price"])
    take_profit_price = float(position["take_profit_price"])
    open_price = float(row["open"])
    high_price = float(row["high"])
    low_price = float(row["low"])
    if side == "LONG":
        if open_price <= stop_price:
            return open_price, "STOP_LOSS", True
        if open_price >= take_profit_price:
            return open_price, "TAKE_PROFIT", True
        if low_price <= stop_price:
            return stop_price, "STOP_LOSS", False
        if high_price >= take_profit_price:
            return take_profit_price, "TAKE_PROFIT", False
    else:
        if open_price >= stop_price:
            return open_price, "STOP_LOSS", True
        if open_price <= take_profit_price:
            return open_price, "TAKE_PROFIT", True
        if high_price >= stop_price:
            return stop_price, "STOP_LOSS", False
        if low_price <= take_profit_price:
            return take_profit_price, "TAKE_PROFIT", False
    return None


def momentum_close_position(
    position: Dict[str, Any],
    row: Any,
    exit_price: float,
    exit_reason: str,
    config: Config,
    funding_rates: Optional[pd.DataFrame],
    cost_multiplier: float = 1.0,
    gap_exit: bool = False,
) -> Dict[str, Any]:
    symbol_config = replace(config, symbol=str(position["symbol"]))
    trade = close_candidate_position(
        position,
        row,
        exit_price,
        exit_reason,
        symbol_config,
        funding_rates=funding_rates,
        cost_multiplier=cost_multiplier,
        gap_exit=gap_exit,
    )
    trade.update(
        {
            "entry_momentum_score": position["entry_momentum_score"],
            "entry_fast_return": position["entry_fast_return"],
            "entry_slow_return": position["entry_slow_return"],
            "entry_realized_volatility": position["entry_realized_volatility"],
            "entry_cross_section_rank": position["entry_cross_section_rank"],
            "entry_universe_size": position["entry_universe_size"],
        }
    )
    return trade


def momentum_rotation_replay(
    config: Config,
    filters_by_symbol: Dict[str, ExchangeFilters],
    features_by_symbol: Dict[str, pd.DataFrame],
    funding_by_symbol: Dict[str, pd.DataFrame],
    segment_start_ms: int,
    segment_end_ms: int,
    params: MomentumRotationParameters,
    cost_multiplier: float = 1.0,
) -> Dict[str, Any]:
    validate_momentum_rotation_parameters(params)
    lookups: Dict[str, Dict[int, Any]] = {}
    timeline_values: Set[int] = set()
    for symbol, frame in features_by_symbol.items():
        selected = frame[
            (frame["close_time"].astype("int64") >= int(segment_start_ms))
            & (frame["close_time"].astype("int64") < int(segment_end_ms))
        ]
        lookups[symbol] = {
            int(row["close_time"]): row
            for _, row in selected.iterrows()
        }
        timeline_values.update(lookups[symbol])
    timeline = sorted(timeline_values)
    trades: List[Dict[str, Any]] = []
    rejections: Counter = Counter()
    position: Optional[Dict[str, Any]] = None
    pending: Optional[Dict[str, Any]] = None
    accepted_signals = 0

    for timeline_index, close_time_ms in enumerate(timeline):
        if pending is not None and int(pending["entry_close_time_ms"]) == close_time_ms:
            symbol = str(pending["symbol"])
            row = lookups.get(symbol, {}).get(close_time_ms)
            filters = filters_by_symbol.get(symbol)
            if row is None or filters is None:
                rejections["MISSING_ENTRY_CANDLE_OR_FILTERS"] += 1
            else:
                entry_price = float(row["open"])
                quantity = momentum_fixed_order_quantity(
                    entry_price,
                    filters,
                    params.fixed_notional_usdt,
                )
                stop_loss_pct = min(
                    max(
                        float(pending["atr"]) / entry_price * params.stop_loss_atr_multiple,
                        params.minimum_stop_loss_pct,
                    ),
                    params.maximum_stop_loss_pct,
                )
                take_profit_pct = stop_loss_pct * params.reward_to_risk_multiple
                expected_cost_rate = (
                    round_trip_execution_cost_rate(config)
                    + params.anticipated_funding_cost_pct
                )
                if quantity <= 0:
                    rejections["FIXED_NOTIONAL_BELOW_EXCHANGE_MINIMUM"] += 1
                elif take_profit_pct / expected_cost_rate < params.minimum_reward_to_cost_ratio:
                    rejections["EXPECTED_REWARD_TO_COST_BELOW_THRESHOLD"] += 1
                else:
                    side = str(pending["side"])
                    stop_price = (
                        entry_price * (1.0 - stop_loss_pct)
                        if side == "LONG"
                        else entry_price * (1.0 + stop_loss_pct)
                    )
                    take_profit_price = (
                        entry_price * (1.0 + take_profit_pct)
                        if side == "LONG"
                        else entry_price * (1.0 - take_profit_pct)
                    )
                    position = {
                        "strategy_id": params.strategy_name,
                        "symbol": symbol,
                        "side": side,
                        "signal_time": pending["signal_time"],
                        "entry_time": candle_time_text(int(row["open_time"])),
                        "entry_open_time_ms": int(row["open_time"]),
                        "entry_price": entry_price,
                        "quantity": quantity,
                        "stop_price": stop_price,
                        "take_profit_price": take_profit_price,
                        "initial_stop_distance_pct": stop_loss_pct,
                        "initial_take_profit_distance_pct": take_profit_pct,
                        "entry_ema20": pending["ema"],
                        "entry_ema50": pending["ema"],
                        "entry_ema_spread_pct": abs(entry_price - float(pending["ema"])) / entry_price,
                        "entry_ema_slope_consistency": None,
                        "entry_trend_persistence": None,
                        "entry_trend_atr": pending["atr"],
                        "entry_execution_interval": params.interval,
                        "entry_atr_execution": pending["atr"],
                        "entry_atr_execution_pct": float(pending["atr"]) / entry_price,
                        "entry_atr_execution_percentile": None,
                        "pullback_distance_atr": None,
                        "expected_reward_to_cost_ratio": take_profit_pct / expected_cost_rate,
                        "expected_net_reward_to_risk_ratio": (
                            (take_profit_pct - expected_cost_rate)
                            / (stop_loss_pct + expected_cost_rate)
                        ),
                        "mfe_pct": 0.0,
                        "mae_pct": 0.0,
                        "time_to_mfe_seconds": None,
                        "time_to_mae_seconds": None,
                        "holding_bars": 0,
                        "entry_momentum_score": pending["momentum_score"],
                        "entry_fast_return": pending["fast_return"],
                        "entry_slow_return": pending["slow_return"],
                        "entry_realized_volatility": pending["realized_volatility"],
                        "entry_cross_section_rank": pending["cross_section_rank"],
                        "entry_universe_size": pending["universe_size"],
                    }
                    accepted_signals += 1
            pending = None

        if position is not None:
            symbol = str(position["symbol"])
            row = lookups.get(symbol, {}).get(close_time_ms)
            if row is not None:
                decision = momentum_exit_decision(position, row)
                if decision is None:
                    update_candidate_excursions(position, row)
                    position["holding_bars"] = int(position["holding_bars"]) + 1
                    if int(position["holding_bars"]) >= params.maximum_holding_bars:
                        decision = (float(row["close"]), "MAX_HOLDING_TIME", False)
                if decision is not None:
                    exit_price, exit_reason, gap_exit = decision
                    entry_price = float(position["entry_price"])
                    exit_excursion = abs(exit_price - entry_price) / entry_price
                    elapsed = max(
                        (int(row["open_time"]) - int(position["entry_open_time_ms"])) / 1000.0,
                        0.0,
                    )
                    if exit_reason == "TAKE_PROFIT" and exit_excursion > float(position["mfe_pct"]):
                        position["mfe_pct"] = exit_excursion
                        position["time_to_mfe_seconds"] = elapsed
                    if exit_reason == "STOP_LOSS" and exit_excursion > float(position["mae_pct"]):
                        position["mae_pct"] = exit_excursion
                        position["time_to_mae_seconds"] = elapsed
                    trades.append(
                        momentum_close_position(
                            position,
                            row,
                            exit_price,
                            exit_reason,
                            config,
                            funding_by_symbol.get(symbol),
                            cost_multiplier=cost_multiplier,
                            gap_exit=gap_exit,
                        )
                    )
                    position = None

        if not is_daily_momentum_rebalance(close_time_ms):
            continue
        candidates = momentum_snapshot_candidates(features_by_symbol, close_time_ms, params)
        selected = candidates[0] if candidates else None
        if position is not None:
            same_position = (
                selected is not None
                and str(position["symbol"]) == str(selected["symbol"])
                and str(position["side"]) == str(selected["side"])
            )
            if not same_position:
                symbol = str(position["symbol"])
                row = lookups.get(symbol, {}).get(close_time_ms)
                if row is not None:
                    trades.append(
                        momentum_close_position(
                            position,
                            row,
                            float(row["close"]),
                            "ROTATION" if selected is not None else "SIGNAL_INVALIDATED",
                            config,
                            funding_by_symbol.get(symbol),
                            cost_multiplier=cost_multiplier,
                        )
                    )
                    position = None
        if position is None and selected is not None and timeline_index + 1 < len(timeline):
            pending = {
                **selected,
                "signal_time": candle_time_text(close_time_ms),
                "entry_close_time_ms": timeline[timeline_index + 1],
            }
        elif selected is None:
            rejections["NO_ELIGIBLE_CROSS_SECTION"] += 1

    report = summarize_trade_group(trades)
    report.update(
        {
            "strategy_name": params.strategy_name,
            "evaluated_candles": len(timeline),
            "accepted_signals": accepted_signals,
            "rejection_counts": dict(sorted(rejections.items())),
            "open_position_excluded": position is not None,
            "pending_signal_excluded": pending is not None,
        }
    )
    return {"report": report, "trades": trades}


def deduplicate_momentum_trades(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: Dict[Tuple[str, str, str, int, int], Dict[str, Any]] = {}
    for trade in trades:
        key = (
            str(trade.get("symbol", "")),
            str(trade.get("side", "")),
            str(trade.get("strategy_id", "")),
            int(trade.get("entry_time_ms", 0) or 0),
            int(trade.get("exit_time_ms", 0) or 0),
        )
        unique.setdefault(key, dict(trade))
    return sorted(
        unique.values(),
        key=lambda trade: (
            int(trade.get("entry_time_ms", 0) or 0),
            int(trade.get("exit_time_ms", 0) or 0),
        ),
    )


def slice_momentum_trades(
    trades: Sequence[Dict[str, Any]],
    start_ms: int,
    end_ms: int,
) -> List[Dict[str, Any]]:
    return [
        dict(trade)
        for trade in trades
        if int(start_ms) <= int(trade.get("entry_time_ms", 0) or 0) < int(end_ms)
    ]


def build_momentum_trade_report(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    trade_list = list(trades)
    report = summarize_trade_group(trade_list)
    report.update(
        {
            "by_symbol": grouped_trade_summary(trade_list, "symbol"),
            "by_direction": grouped_trade_summary(trade_list, "side"),
            "exit_reason_counts": dict(
                Counter(str(trade.get("exit_reason", "UNKNOWN")) for trade in trade_list)
            ),
            "holding_seconds_distribution": candidate_distribution(
                [trade.get("holding_seconds", 0.0) for trade in trade_list]
            ),
            "mfe_pct_distribution": candidate_distribution(
                [trade.get("mfe_pct", 0.0) for trade in trade_list]
            ),
            "mae_pct_distribution": candidate_distribution(
                [trade.get("mae_pct", 0.0) for trade in trade_list]
            ),
        }
    )
    return report


def momentum_segment_gate(
    report: Dict[str, Any],
    minimum_trades: int,
    minimum_profit_factor: float,
) -> Dict[str, Any]:
    failures: List[str] = []
    if int(report.get("trades", 0) or 0) < minimum_trades:
        failures.append("INSUFFICIENT_TRADES")
    if report.get("expectancy") is None or float(report["expectancy"]) <= 0:
        failures.append("NON_POSITIVE_EXPECTANCY")
    if (
        report.get("net_profit_factor") is None
        or float(report["net_profit_factor"]) <= minimum_profit_factor
    ):
        failures.append("NET_PROFIT_FACTOR_BELOW_THRESHOLD")
    return {
        "eligible": not failures,
        "minimum_trades": minimum_trades,
        "minimum_net_profit_factor": minimum_profit_factor,
        "failure_reasons": failures,
    }


def momentum_capital_overlay(
    trades: Sequence[Dict[str, Any]],
    filters_by_symbol: Dict[str, ExchangeFilters],
    config: Config,
    params: MomentumRotationParameters,
    starting_capital_usdt: float,
) -> Dict[str, Any]:
    starting_capital = float(starting_capital_usdt)
    if starting_capital <= 0:
        raise ConfigError("V10 capital overlay requires positive starting capital")
    risk_config = replace(
        config,
        max_net_loss_per_trade_usdt=starting_capital * params.max_risk_pct,
        max_risk_per_trade_pct=params.max_risk_pct,
        max_margin_usdt=starting_capital * params.max_account_margin_fraction,
        max_account_margin_fraction=params.max_account_margin_fraction,
        max_notional_usdt=starting_capital * config.leverage,
        gap_risk_buffer_pct=config.gap_risk_buffer_pct + params.anticipated_funding_cost_pct,
    )
    equity = starting_capital
    accepted: List[Dict[str, Any]] = []
    skip_reasons: Counter = Counter()
    source_trades = sorted(
        [dict(trade) for trade in trades],
        key=lambda trade: int(trade.get("entry_time_ms", 0) or 0),
    )
    monetary_fields = (
        "gross_pnl",
        "fees",
        "slippage",
        "spread_cost",
        "funding_pnl",
        "execution_cost",
        "net_pnl",
        "mfe_usdt",
        "mae_usdt",
    )
    for source in source_trades:
        symbol = str(source.get("symbol", ""))
        filters = filters_by_symbol.get(symbol)
        entry_price = float(source.get("entry_price", 0.0) or 0.0)
        stop_loss_pct = float(source.get("initial_stop_distance_pct", 0.0) or 0.0)
        if filters is None:
            skip_reasons["MISSING_EXCHANGE_FILTERS"] += 1
            continue
        plan = build_risk_sizing_plan(
            equity,
            entry_price,
            filters,
            risk_config,
            stop_loss_pct=stop_loss_pct,
        )
        if plan.skip_reason:
            skip_reasons[candidate_risk_skip_reason(plan.skip_reason)] += 1
            continue
        fixed_quantity = float(source.get("quantity", 0.0) or 0.0)
        if fixed_quantity <= 0 or plan.quantity <= 0:
            skip_reasons["INVALID_SOURCE_OR_SIZED_QUANTITY"] += 1
            continue
        scale = plan.quantity / fixed_quantity
        trade = dict(source)
        for field_name in monetary_fields:
            trade[field_name] = float(source.get(field_name, 0.0) or 0.0) * scale
        trade["quantity"] = plan.quantity
        trade["entry_notional"] = plan.quantity * entry_price
        trade["exit_notional"] = plan.quantity * float(source.get("exit_price", 0.0) or 0.0)
        trade["sizing_mode"] = "risk_sized"
        trade["risk_budget_usdt"] = plan.risk_budget_usdt
        trade["estimated_net_loss_usdt"] = plan.estimated_net_loss_usdt
        trade["result"] = "WIN" if float(trade["net_pnl"]) > 0 else "LOSS"
        accepted.append(trade)
        equity += float(trade["net_pnl"])
        if equity <= 0:
            skip_reasons["CAPITAL_DEPLETED"] += len(source_trades) - len(accepted)
            break
    report = build_momentum_trade_report(accepted)
    report.update(
        {
            "starting_capital_usdt": starting_capital,
            "ending_capital_usdt": equity,
            "source_trade_count": len(source_trades),
            "execution_coverage": len(accepted) / len(source_trades) if source_trades else 0.0,
            "skip_reasons": dict(sorted(skip_reasons.items())),
            "maximum_absolute_loss_cap_usdt": starting_capital * params.max_risk_pct,
            "maximum_margin_cap_usdt": starting_capital * params.max_account_margin_fraction,
            "maximum_notional_cap_usdt": starting_capital * config.leverage,
        }
    )
    return {"report": report, "trades": accepted}


def run_momentum_rotation_replay(
    config: Config,
    days: int,
    end_time: Optional[str],
    cache_dir_value: str,
    output_dir_value: str,
    params: Optional[MomentumRotationParameters] = None,
    client: Any = None,
) -> Dict[str, Any]:
    if config.live_trading or not config.dry_run or not config.shadow_mode:
        raise ConfigError("V10 replay requires LIVE_TRADING=false, DRY_RUN=true, and SHADOW_MODE=true")
    params = params or MomentumRotationParameters()
    validate_momentum_rotation_parameters(params)
    end_exclusive_ms = aligned_replay_end_ms(params.interval, end_time)
    windows = rolling_walk_forward_boundaries(days, end_exclusive_ms, params)
    interval_value_seconds = interval_seconds(params.interval)
    if interval_value_seconds is None:
        raise ConfigError("V10 replay interval is invalid")
    warmup_bars = max(
        params.slow_momentum_bars,
        params.volatility_lookback_bars,
        params.ema_period,
    ) + 2
    master_start_ms = int(windows[0]["train"][0])
    download_start_ms = master_start_ms - warmup_bars * interval_value_seconds * 1000
    cache_dir = resolve_app_path(cache_dir_value)
    output_dir = resolve_app_path(output_dir_value)
    public_client = client or build_public_replay_client(config)
    filters_by_symbol: Dict[str, ExchangeFilters] = {}
    features_by_symbol: Dict[str, pd.DataFrame] = {}
    funding_by_symbol: Dict[str, pd.DataFrame] = {}
    cache_files: Dict[str, Dict[str, Any]] = {}

    for symbol in params.symbols:
        symbol_config = replace(config, symbol=symbol, interval=params.interval)
        kline_cache = cache_dir / f"{symbol}_{params.interval}.csv"
        funding_cache = cache_dir / f"{symbol}_funding.csv"
        filters_by_symbol[symbol] = get_exchange_filters(public_client, symbol, symbol_config)
        klines = ensure_historical_kline_cache(
            public_client,
            symbol_config,
            download_start_ms,
            end_exclusive_ms - 1,
            kline_cache,
        )
        funding = ensure_funding_rate_cache(
            public_client,
            symbol_config,
            master_start_ms,
            end_exclusive_ms - 1,
            funding_cache,
        )
        features_by_symbol[symbol] = build_momentum_feature_frame(klines, params)
        funding_by_symbol[symbol] = funding
        cache_files[symbol] = {
            "klines": str(kline_cache),
            "klines_sha256": file_sha256(kline_cache),
            "funding": str(funding_cache),
            "funding_sha256": file_sha256(funding_cache),
            "candles": len(klines),
            "funding_rows": len(funding),
        }

    master = momentum_rotation_replay(
        config,
        filters_by_symbol,
        features_by_symbol,
        funding_by_symbol,
        master_start_ms,
        end_exclusive_ms,
        params,
    )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / "runs" / run_id
    write_candidate_trades(run_dir / "master_fixed.csv", master["trades"])

    aggregate: Dict[str, List[Dict[str, Any]]] = {
        "train": [],
        "validation": [],
        "test": [],
    }
    window_reports: List[Dict[str, Any]] = []
    positive_test_windows = 0
    for window in windows:
        segment_reports: Dict[str, Any] = {}
        for segment_name in ("train", "validation", "test"):
            start_ms, end_ms = window[segment_name]
            segment_trades = slice_momentum_trades(master["trades"], start_ms, end_ms)
            aggregate[segment_name].extend(segment_trades)
            segment_report = build_momentum_trade_report(segment_trades)
            segment_report.update(
                {
                    "start": candle_time_text(start_ms),
                    "end_exclusive": candle_time_text(end_ms),
                }
            )
            segment_reports[segment_name] = segment_report
            if (
                segment_name == "test"
                and segment_report.get("expectancy") is not None
                and float(segment_report["expectancy"]) > 0
            ):
                positive_test_windows += 1
        window_reports.append(
            {
                "window_id": window["window_id"],
                "embargo_minutes": window["embargo_minutes"],
                **segment_reports,
            }
        )

    aggregate = {
        segment_name: deduplicate_momentum_trades(segment_trades)
        for segment_name, segment_trades in aggregate.items()
    }
    train_report = build_momentum_trade_report(aggregate["train"])
    validation_report = build_momentum_trade_report(aggregate["validation"])
    test_report = build_momentum_trade_report(aggregate["test"])
    train_gate = momentum_segment_gate(
        train_report,
        params.minimum_train_trades,
        params.minimum_train_profit_factor,
    )
    validation_gate = momentum_segment_gate(
        validation_report,
        params.minimum_validation_trades,
        params.minimum_validation_profit_factor,
    )
    test_gate = momentum_segment_gate(
        test_report,
        params.minimum_test_trades,
        params.minimum_test_profit_factor,
    )
    cost_stress = build_cost_stress_report(aggregate["test"], params.cost_stress_multipliers)
    bootstrap = block_bootstrap_expectancy(
        aggregate["test"],
        params.bootstrap_samples,
        params.bootstrap_block_trades,
    )
    capital_results: Dict[str, Dict[str, Any]] = {}
    for capital in params.capital_scenarios_usdt:
        result = momentum_capital_overlay(
            aggregate["test"],
            filters_by_symbol,
            config,
            params,
            capital,
        )
        key = capital_scenario_key(capital)
        capital_results[key] = result["report"]
        write_candidate_trades(run_dir / f"test_{key}_risk_sized.csv", result["trades"])

    positive_window_fraction = (
        positive_test_windows / len(windows) if windows else 0.0
    )
    qualification_key = capital_scenario_key(params.qualification_capital_usdt)
    qualification_capital = capital_results[qualification_key]
    failures: List[str] = []
    if len(windows) < params.minimum_walk_forward_windows:
        failures.append("INSUFFICIENT_WALK_FORWARD_WINDOWS")
    if not train_gate["eligible"]:
        failures.append("TRAIN_GATE_FAILED")
    if not validation_gate["eligible"]:
        failures.append("VALIDATION_GATE_FAILED")
    if not test_gate["eligible"]:
        failures.append("TEST_GATE_FAILED")
    if positive_window_fraction < params.minimum_positive_test_window_fraction:
        failures.append("POSITIVE_TEST_WINDOW_FRACTION_BELOW_THRESHOLD")
    stressed_15x = cost_stress.get("1.5x", {})
    if (
        stressed_15x.get("expectancy") is None
        or float(stressed_15x["expectancy"]) <= 0
        or stressed_15x.get("net_profit_factor") is None
        or float(stressed_15x["net_profit_factor"]) <= params.minimum_test_profit_factor
    ):
        failures.append("FAILED_1_5X_COST_STRESS")
    if bootstrap.get("lower_95") is None or float(bootstrap["lower_95"]) <= 0:
        failures.append("BOOTSTRAP_EXPECTANCY_LOWER_BOUND_NOT_POSITIVE")
    if (
        float(qualification_capital.get("execution_coverage", 0.0) or 0.0)
        < params.minimum_capital_execution_coverage
    ):
        failures.append("QUALIFICATION_CAPITAL_COVERAGE_BELOW_THRESHOLD")
    if float(qualification_capital.get("net_pnl", 0.0) or 0.0) <= 0:
        failures.append("QUALIFICATION_CAPITAL_NET_PNL_NOT_POSITIVE")
    if (
        float(qualification_capital.get("max_drawdown", 0.0) or 0.0)
        >= params.maximum_qualification_drawdown_usdt
    ):
        failures.append("QUALIFICATION_CAPITAL_DRAWDOWN_EXCEEDED")
    historical_gate_passed = not failures
    viability_status = (
        "CANDIDATE_FOR_FORWARD_SHADOW" if historical_gate_passed else "NOT_VIABLE"
    )

    summary = {
        "engine": params.profile_name,
        "strategy_name": params.strategy_name,
        "generated_at": utc_now_text(),
        "run_id": run_id,
        "days": days,
        "symbols": list(params.symbols),
        "parameters": asdict(params),
        "cache_files": cache_files,
        "walk_forward": {
            "window_count": len(windows),
            "positive_test_windows": positive_test_windows,
            "positive_test_window_fraction": positive_window_fraction,
            "windows": window_reports,
        },
        "master": master["report"],
        "train": {**train_report, "gate": train_gate},
        "validation": {**validation_report, "gate": validation_gate},
        "test": {**test_report, "gate": test_gate},
        "test_cost_stress": cost_stress,
        "test_block_bootstrap_expectancy": bootstrap,
        "capital_results": capital_results,
        "qualification_capital_key": qualification_key,
        "failure_reasons": failures,
        "historical_gate_passed": historical_gate_passed,
        "viability_status": viability_status,
        "eligible_for_forward_shadow_validation": historical_gate_passed,
        "eligible_for_micro_live_test": False,
        "fresh_forward_validation_required": True,
        "recommendation": (
            "BEGIN_FORWARD_SHADOW_VALIDATION" if historical_gate_passed else "DO_NOT_TRADE_LIVE"
        ),
        "shadow_mode_only": True,
        "dry_run": True,
        "live_trading": False,
        "api_credentials_used": False,
        "account_state_changed": False,
        "source_sha256": file_sha256(Path(__file__).resolve()),
    }
    write_candidate_trades(run_dir / "train_fixed.csv", aggregate["train"])
    write_candidate_trades(run_dir / "validation_fixed.csv", aggregate["validation"])
    write_candidate_trades(run_dir / "test_fixed.csv", aggregate["test"])
    write_json_atomic(run_dir / "summary.json", summary)
    write_json_atomic(output_dir / "latest.json", summary)
    return summary


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
    trend_interval: Optional[str] = None,
    config: Optional[Config] = None,
) -> pd.DataFrame:
    feature_config = config or Config()
    candidate_trend_interval = trend_interval or params.trend_interval
    source = all_klines[HISTORICAL_KLINE_COLUMNS].copy().sort_values("close_time").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume"):
        source[column] = pd.to_numeric(source[column], errors="coerce")
    if params.execution_interval == source_interval:
        primary = source.copy()
    else:
        primary = resample_closed_klines(source, source_interval, params.execution_interval)
    for column in ("open", "high", "low", "close", "volume"):
        primary[column] = pd.to_numeric(primary[column], errors="coerce")
    primary["atr_execution"] = candidate_true_range(primary).rolling(
        params.atr_execution_period
    ).mean()
    primary["atr_execution_pct"] = primary["atr_execution"] / primary["close"]
    primary["atr_execution_percentile"] = (
        primary["atr_execution_pct"]
        .rolling(params.atr_percentile_window, min_periods=params.atr_percentile_window)
        .rank(pct=True)
    )
    primary["donchian_high"] = (
        primary["high"].shift(1).rolling(params.donchian_lookback, min_periods=params.donchian_lookback).max()
    )
    primary["donchian_low"] = (
        primary["low"].shift(1).rolling(params.donchian_lookback, min_periods=params.donchian_lookback).min()
    )
    prior_volume_sma = (
        primary["volume"].shift(1).rolling(params.volume_lookback, min_periods=params.volume_lookback).mean()
    ).replace(0.0, np.nan)
    primary["volume_ratio"] = primary["volume"] / prior_volume_sma
    rolling_close = primary["close"].rolling(feature_config.lookback)
    rolling_std = rolling_close.std(ddof=0).replace(0.0, np.nan)
    primary["candidate_zscore"] = (primary["close"] - rolling_close.mean()) / rolling_std
    primary["mr_ema20"] = primary["close"].ewm(
        span=feature_config.regime_fast_ema_period,
        adjust=False,
    ).mean()
    primary["mr_ema50"] = primary["close"].ewm(
        span=feature_config.regime_slow_ema_period,
        adjust=False,
    ).mean()
    primary["mr_ema_slope_pct"] = primary["mr_ema20"].pct_change(
        periods=feature_config.regime_slope_lookback
    )
    vwap_window = feature_config.regime_slow_ema_period
    rolling_volume = primary["volume"].rolling(vwap_window).sum().replace(0.0, np.nan)
    primary["mr_vwap"] = (
        (primary["close"] * primary["volume"]).rolling(vwap_window).sum() / rolling_volume
    )
    primary["mr_distance_ema_pct"] = (
        (primary["close"] - primary["mr_ema50"]) / primary["mr_ema50"]
    )
    primary["mr_distance_vwap_pct"] = (
        (primary["close"] - primary["mr_vwap"]) / primary["mr_vwap"]
    )
    oscillation_sign = np.sign(primary["close"] - primary["mr_ema20"])
    nonzero_sign = pd.Series(oscillation_sign, index=primary.index).replace(0.0, np.nan).ffill()
    cross_event = nonzero_sign.ne(nonzero_sign.shift()).astype(float)
    cross_event.loc[nonzero_sign.isna() | nonzero_sign.shift().isna()] = 0.0
    primary["mr_ema_crosses"] = cross_event.rolling(
        feature_config.regime_oscillation_lookback,
        min_periods=feature_config.regime_oscillation_lookback,
    ).sum()
    primary["mr_atr_bucket"] = np.select(
        [
            (primary["atr_execution_percentile"] <= feature_config.regime_low_atr_percentile)
            & (primary["atr_execution_pct"] <= feature_config.max_atr_pct),
            (primary["atr_execution_percentile"] >= feature_config.regime_high_atr_percentile)
            | (primary["atr_execution_pct"] > feature_config.max_atr_pct),
        ],
        ["low", "high"],
        default="medium",
    )
    primary["mr_regime_confirmed"] = (
        primary["mr_ema_slope_pct"].abs()
        <= feature_config.regime_mean_reversion_max_slope_pct
    ) & (
        primary["mr_distance_ema_pct"].abs()
        <= feature_config.regime_mean_reversion_max_distance_pct
    ) & (
        primary["mr_distance_vwap_pct"].abs()
        <= feature_config.regime_mean_reversion_max_distance_pct
    ) & (
        primary["mr_ema_crosses"] >= feature_config.regime_min_ema_crosses
    )

    trend = resample_closed_klines(primary, params.execution_interval, candidate_trend_interval)
    for column in ("open", "high", "low", "close", "volume"):
        trend[column] = pd.to_numeric(trend[column], errors="coerce")
    trend["trend_ema20"] = trend["close"].ewm(span=params.trend_fast_ema_period, adjust=False).mean()
    trend["trend_ema50"] = trend["close"].ewm(span=params.trend_slow_ema_period, adjust=False).mean()
    trend["trend_fast_ema"] = trend["trend_ema20"]
    trend["trend_slow_ema"] = trend["trend_ema50"]
    trend["trend_atr"] = candidate_true_range(trend).rolling(params.trend_atr_period).mean()
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
        "trend_fast_ema",
        "trend_slow_ema",
        "trend_atr",
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
    if params.regime_interval:
        regime = resample_closed_klines(primary, params.execution_interval, params.regime_interval)
        for column in ("open", "high", "low", "close", "volume"):
            regime[column] = pd.to_numeric(regime[column], errors="coerce")
        regime["regime_fast_ema"] = regime["close"].ewm(
            span=params.regime_fast_ema_period,
            adjust=False,
        ).mean()
        regime["regime_slow_ema"] = regime["close"].ewm(
            span=params.regime_slow_ema_period,
            adjust=False,
        ).mean()
        regime["regime_slope_pct"] = regime["regime_fast_ema"].pct_change(
            periods=params.regime_slope_lookback
        )
        regime["regime_side"] = np.select(
            [
                (regime["regime_fast_ema"] > regime["regime_slow_ema"])
                & (regime["regime_slope_pct"] > 0),
                (regime["regime_fast_ema"] < regime["regime_slow_ema"])
                & (regime["regime_slope_pct"] < 0),
            ],
            [1, -1],
            default=0,
        ).astype(int)
        regime["regime_feature_close_time"] = regime["close_time"].astype("int64")
        regime_columns = [
            "close_time",
            "regime_feature_close_time",
            "regime_fast_ema",
            "regime_slow_ema",
            "regime_slope_pct",
            "regime_side",
        ]
        features = pd.merge_asof(
            features.sort_values("close_time"),
            regime[regime_columns].sort_values("close_time"),
            on="close_time",
            direction="backward",
            allow_exact_matches=True,
        )
    return features.reset_index(drop=True)


def candidate_pullback_distance_atr(row: Any, direction: str) -> Optional[float]:
    try:
        ema20 = float(row["trend_ema20"])
        atr_execution = float(row["atr_execution"])
        extreme = float(row["low"] if direction == "LONG" else row["high"])
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (ema20, atr_execution, extreme)) or atr_execution <= 0:
        return None
    return abs(extreme - ema20) / atr_execution


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
        atr_percentile = float(current["atr_execution_percentile"])
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


def candidate_strategy_direction(strategy_id: str) -> str:
    if strategy_id in (
        CANDIDATE_TREND_LONG_NAME,
        CANDIDATE_BREAKOUT_LONG_NAME,
        CANDIDATE_REGIME_PULLBACK_LONG_NAME,
        CANDIDATE_CROSS_ASSET_PULLBACK_LONG_NAME,
    ):
        return "LONG"
    if strategy_id in (
        CANDIDATE_TREND_SHORT_NAME,
        CANDIDATE_BREAKOUT_SHORT_NAME,
        CANDIDATE_REGIME_PULLBACK_SHORT_NAME,
        CANDIDATE_CROSS_ASSET_PULLBACK_SHORT_NAME,
    ):
        return "SHORT"
    raise ConfigError(f"Candidate strategy has no fixed direction: {strategy_id}")


def evaluate_breakout_candidate_signal(
    current: Any,
    direction: str,
    params: CandidateReplayParameters,
) -> Tuple[bool, str]:
    if direction not in CANDIDATE_DIRECTIONS:
        raise ConfigError(f"Unsupported candidate direction: {direction}")
    try:
        current_close_time = int(current["close_time"])
        trend_close_time = int(current["trend_feature_close_time"])
        regime_close_time = int(current["regime_feature_close_time"])
        trend_side = int(current["trend_side"])
        regime_side = int(current["regime_side"])
        close_price = float(current["close"])
        donchian_high = float(current["donchian_high"])
        donchian_low = float(current["donchian_low"])
        volume_ratio = float(current["volume_ratio"])
    except (KeyError, TypeError, ValueError):
        return False, "MISSING_BREAKOUT_FEATURES"
    if not all(
        math.isfinite(value)
        for value in (close_price, donchian_high, donchian_low, volume_ratio)
    ):
        return False, "MISSING_BREAKOUT_FEATURES"
    if trend_close_time > current_close_time or regime_close_time > current_close_time:
        return False, "FUTURE_HIGHER_TIMEFRAME_CANDLE_BLOCKED"
    expected_side = 1 if direction == "LONG" else -1
    if regime_side != expected_side:
        return False, "REGIME_DIRECTION_MISMATCH"
    if trend_side != expected_side:
        return False, "TREND_DIRECTION_MISMATCH"
    if volume_ratio <= params.volume_expansion_multiplier:
        return False, "VOLUME_EXPANSION_NOT_CONFIRMED"
    if direction == "LONG" and close_price <= donchian_high:
        return False, "DONCHIAN_HIGH_NOT_BROKEN"
    if direction == "SHORT" and close_price >= donchian_low:
        return False, "DONCHIAN_LOW_NOT_BROKEN"
    return True, ""


def evaluate_regime_pullback_candidate_signal(
    current: Any,
    previous: Any,
    direction: str,
    params: CandidateReplayParameters,
) -> Tuple[bool, str, Optional[float]]:
    if direction not in CANDIDATE_DIRECTIONS:
        raise ConfigError(f"Unsupported candidate direction: {direction}")
    try:
        current_close_time = int(current["close_time"])
        regime_close_time = int(current["regime_feature_close_time"])
        regime_side = int(current["regime_side"])
    except (KeyError, TypeError, ValueError):
        return False, "MISSING_REGIME_FEATURES", None
    if regime_close_time > current_close_time:
        return False, "FUTURE_HIGHER_TIMEFRAME_CANDLE_BLOCKED", None
    expected_side = 1 if direction == "LONG" else -1
    if regime_side != expected_side:
        return False, "REGIME_DIRECTION_MISMATCH", None
    return evaluate_candidate_signal(current, previous, direction, params)


def evaluate_mean_reversion_candidate_signal(
    current: Any,
    previous: Any,
    config: Config,
) -> Tuple[bool, str, str]:
    try:
        zscore = float(current["candidate_zscore"])
        previous_zscore = float(previous["candidate_zscore"])
        close_price = float(current["close"])
        previous_close = float(previous["close"])
        atr_bucket = str(current["mr_atr_bucket"])
        regime_confirmed = bool(current["mr_regime_confirmed"])
    except (KeyError, TypeError, ValueError):
        return False, "MISSING_MEAN_REVERSION_FEATURES", "NONE"
    if not all(math.isfinite(value) for value in (zscore, previous_zscore, close_price, previous_close)):
        return False, "MISSING_MEAN_REVERSION_FEATURES", "NONE"
    if not regime_confirmed:
        return False, "MEAN_REVERSION_REGIME_NOT_CONFIRMED", "NONE"
    if atr_bucket != "low":
        return False, "MEAN_REVERSION_REQUIRES_LOW_ATR", "NONE"
    if not (2.20 <= abs(zscore) <= 2.80):
        return False, "MEAN_REVERSION_ZSCORE_OUTSIDE_RANGE", "NONE"
    long_reversion = (
        previous_zscore <= -2.20
        and zscore < 0
        and zscore > previous_zscore + config.min_z_reversion_delta
        and close_price > previous_close
    )
    short_reversion = (
        previous_zscore >= 2.20
        and zscore > 0
        and zscore < previous_zscore - config.min_z_reversion_delta
        and close_price < previous_close
    )
    if long_reversion:
        return True, "", "LONG"
    if short_reversion:
        return True, "", "SHORT"
    return False, "MEAN_REVERSION_CONFIRMATION_FAILED", "NONE"


def candidate_strategy_profile(
    strategy_id: str,
    config: Config,
    params: CandidateReplayParameters,
) -> Tuple[float, float, int]:
    if strategy_id == CANDIDATE_MEAN_REVERSION_NAME:
        return config.stop_loss_pct, config.take_profit_pct, config.shadow_probe_max_holding_minutes
    return params.minimum_stop_loss_pct, params.minimum_take_profit_pct, params.max_holding_minutes


def candidate_entry_risk_profile(
    entry_price: float,
    atr_execution: float,
    params: CandidateReplayParameters,
) -> Optional[Tuple[float, float]]:
    if entry_price <= 0 or atr_execution <= 0:
        return None
    atr_pct = atr_execution / entry_price
    if not math.isfinite(atr_pct) or atr_pct <= 0:
        return None
    stop_loss_pct = min(
        max(atr_pct * params.stop_loss_atr_multiple, params.minimum_stop_loss_pct),
        params.maximum_stop_loss_pct,
    )
    if params.target_reward_to_risk_multiple is not None:
        take_profit_pct = stop_loss_pct * params.target_reward_to_risk_multiple
    else:
        take_profit_pct = atr_pct * params.take_profit_atr_multiple
    take_profit_pct = min(
        max(take_profit_pct, params.minimum_take_profit_pct),
        params.maximum_take_profit_pct,
    )
    return stop_loss_pct, take_profit_pct


def candidate_expected_entry_cost_rate(
    config: Config,
    params: CandidateReplayParameters,
) -> float:
    return round_trip_execution_cost_rate(config) + params.anticipated_funding_cost_pct


def candidate_entry_cost_metrics(
    stop_loss_pct: float,
    take_profit_pct: float,
    config: Config,
    params: CandidateReplayParameters,
) -> Dict[str, float]:
    expected_cost_rate = candidate_expected_entry_cost_rate(config, params)
    reward_to_cost_ratio = (
        take_profit_pct / expected_cost_rate if expected_cost_rate > 0 else math.inf
    )
    net_reward = take_profit_pct - expected_cost_rate
    net_risk = stop_loss_pct + expected_cost_rate
    net_reward_to_risk_ratio = net_reward / net_risk if net_risk > 0 else 0.0
    return {
        "expected_cost_rate": expected_cost_rate,
        "reward_to_cost_ratio": reward_to_cost_ratio,
        "net_reward_to_risk_ratio": net_reward_to_risk_ratio,
    }


def candidate_estimated_tp_cost_ratio(config: Config, params: CandidateReplayParameters) -> float:
    round_trip_cost_pct = candidate_expected_entry_cost_rate(config, params)
    if params.minimum_take_profit_pct <= 0:
        return math.inf
    return round_trip_cost_pct / params.minimum_take_profit_pct


def candidate_risk_config(config: Config, params: CandidateReplayParameters) -> Config:
    return replace(
        config,
        max_net_loss_per_trade_usdt=params.executable_max_net_loss_per_trade_usdt,
        max_risk_per_trade_pct=params.executable_max_risk_per_trade_pct,
        gap_risk_buffer_pct=config.gap_risk_buffer_pct + params.anticipated_funding_cost_pct,
    )


def candidate_risk_skip_reason(reason: str) -> str:
    if reason in {
        "MINIMUM_ORDER_EXCEEDS_RISK_BUDGET",
        "SIZED_QUANTITY_BELOW_EXCHANGE_MINIMUM",
        "MINIMUM_QUANTITY_EXCEEDS_MAXIMUM",
    }:
        return "TRADE_SKIPPED_MIN_NOTIONAL"
    return reason or "INVALID_EXCHANGE_FILTER_QUANTITY"


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


def candidate_funding_pnl(
    position: Dict[str, Any],
    exit_time_ms: int,
    funding_rates: Optional[pd.DataFrame],
) -> float:
    if funding_rates is None or funding_rates.empty:
        return 0.0
    entry_time_ms = int(position["entry_open_time_ms"])
    relevant = funding_rates[
        (funding_rates["funding_time"] > entry_time_ms)
        & (funding_rates["funding_time"] <= int(exit_time_ms))
    ]
    if relevant.empty:
        return 0.0
    quantity = float(position["quantity"])
    entry_price = float(position["entry_price"])
    side_sign = -1.0 if str(position["side"]) == "LONG" else 1.0
    total = 0.0
    for row in relevant.itertuples(index=False):
        mark_price = float(row.mark_price) if float(row.mark_price) > 0 else entry_price
        total += side_sign * quantity * mark_price * float(row.funding_rate)
    return total


def close_candidate_position(
    position: Dict[str, Any],
    row: Any,
    exit_price: float,
    exit_reason: str,
    config: Config,
    funding_rates: Optional[pd.DataFrame] = None,
    cost_multiplier: float = 1.0,
    gap_exit: bool = False,
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
    round_trip_notional = entry_notional + exit_notional
    fees = round_trip_notional * config.taker_fee_rate * cost_multiplier
    slippage = round_trip_notional * config.shadow_slippage_bps / 10000.0 * cost_multiplier
    spread_cost = round_trip_notional * config.shadow_spread_bps / 10000.0 * cost_multiplier
    funding_pnl = candidate_funding_pnl(position, int(row["close_time"]), funding_rates)
    execution_cost = fees + slippage + spread_cost
    net_pnl = gross_pnl + funding_pnl - execution_cost
    mfe_pct = float(position["mfe_pct"])
    mae_pct = float(position["mae_pct"])
    gross_return_pct = gross_pnl / entry_notional if entry_notional > 0 else 0.0
    return {
        "strategy_id": str(position.get("strategy_id", CANDIDATE_STRATEGY_NAME)),
        "side": side,
        "signal_time": position["signal_time"],
        "entry_time": position["entry_time"],
        "entry_time_ms": int(position["entry_open_time_ms"]),
        "entry_price": entry_price,
        "exit_time": candle_time_text(int(row["close_time"])),
        "exit_time_ms": int(row["close_time"]),
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
        "entry_trend_atr": position["entry_trend_atr"],
        "entry_execution_interval": position["entry_execution_interval"],
        "entry_atr_execution": position["entry_atr_execution"],
        "entry_atr_execution_pct": position["entry_atr_execution_pct"],
        "entry_atr_execution_percentile": position["entry_atr_execution_percentile"],
        "pullback_distance_atr": position["pullback_distance_atr"],
        "initial_stop_distance_pct": position["initial_stop_distance_pct"],
        "initial_take_profit_distance_pct": position["initial_take_profit_distance_pct"],
        "entry_zscore": position.get("entry_zscore"),
        "entry_regime_interval": position.get("entry_regime_interval"),
        "entry_regime_fast_ema": position.get("entry_regime_fast_ema"),
        "entry_regime_slow_ema": position.get("entry_regime_slow_ema"),
        "entry_regime_slope_pct": position.get("entry_regime_slope_pct"),
        "entry_donchian_high": position.get("entry_donchian_high"),
        "entry_donchian_low": position.get("entry_donchian_low"),
        "entry_volume_ratio": position.get("entry_volume_ratio"),
        "expected_reward_to_cost_ratio": position.get("expected_reward_to_cost_ratio"),
        "expected_net_reward_to_risk_ratio": position.get(
            "expected_net_reward_to_risk_ratio"
        ),
        "gross_pnl": gross_pnl,
        "fees": fees,
        "slippage": slippage,
        "spread_cost": spread_cost,
        "funding_pnl": funding_pnl,
        "execution_cost": execution_cost,
        "net_pnl": net_pnl,
        "sizing_mode": str(position.get("sizing_mode", "fixed_notional")),
        "risk_budget_usdt": float(position.get("risk_budget_usdt", 0.0) or 0.0),
        "estimated_net_loss_usdt": float(position.get("estimated_net_loss_usdt", 0.0) or 0.0),
        "gap_exit": bool(gap_exit),
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


def candidate_replay_strategy(
    config: Config,
    filters: ExchangeFilters,
    features: pd.DataFrame,
    segment_start_ms: int,
    segment_end_ms: int,
    strategy_id: str,
    params: CandidateReplayParameters,
    funding_rates: Optional[pd.DataFrame] = None,
    cost_multiplier: float = 1.0,
    sizing_mode: str = "fixed_notional",
    starting_equity_usdt: Optional[float] = None,
) -> Dict[str, Any]:
    if strategy_id not in SUPPORTED_CANDIDATE_LEDGER_NAMES:
        raise ConfigError(f"Unsupported candidate strategy: {strategy_id}")
    if sizing_mode not in {"fixed_notional", "risk_sized"}:
        raise ConfigError(f"Unsupported candidate sizing mode: {sizing_mode}")
    default_stop_loss_pct, default_take_profit_pct, max_holding_minutes = candidate_strategy_profile(
        strategy_id,
        config,
        params,
    )
    risk_config = candidate_risk_config(config, params)
    close_times = features["close_time"].astype("int64").to_numpy()
    indexes = np.flatnonzero((close_times >= segment_start_ms) & (close_times < segment_end_ms))
    trades: List[Dict[str, Any]] = []
    rejection_counts: Counter = Counter()
    signal_count = 0
    pending: Optional[Dict[str, Any]] = None
    position: Optional[Dict[str, Any]] = None
    cooldown_until_ms = 0
    equity = float(params.starting_equity_usdt if starting_equity_usdt is None else starting_equity_usdt)

    for index_value in indexes:
        index = int(index_value)
        row = features.iloc[index]
        entered_this_candle = False
        exited_this_candle = False

        if pending is not None and int(pending["entry_index"]) == index:
            entry_price = float(row["open"])
            if strategy_id == CANDIDATE_MEAN_REVERSION_NAME:
                entry_risk_profile = (default_stop_loss_pct, default_take_profit_pct)
            else:
                entry_risk_profile = candidate_entry_risk_profile(
                    entry_price,
                    float(pending.get("entry_atr_execution", 0.0) or 0.0),
                    params,
                )
            if entry_risk_profile is None:
                rejection_counts["INVALID_ATR_RISK_PROFILE"] += 1
                pending = None
                entered_this_candle = True
                continue
            stop_loss_pct, take_profit_pct = entry_risk_profile
            entry_cost_metrics = candidate_entry_cost_metrics(
                stop_loss_pct,
                take_profit_pct,
                config,
                params,
            )
            if (
                params.minimum_reward_to_cost_ratio > 0
                and entry_cost_metrics["reward_to_cost_ratio"]
                < params.minimum_reward_to_cost_ratio
            ):
                rejection_counts["EXPECTED_REWARD_TO_COST_BELOW_THRESHOLD"] += 1
                pending = None
                entered_this_candle = True
                continue
            if (
                params.minimum_net_reward_to_risk_ratio > 0
                and entry_cost_metrics["net_reward_to_risk_ratio"]
                < params.minimum_net_reward_to_risk_ratio
            ):
                rejection_counts["NET_REWARD_TO_RISK_BELOW_THRESHOLD"] += 1
                pending = None
                entered_this_candle = True
                continue
            sizing = None
            if sizing_mode == "risk_sized":
                sizing = build_risk_sizing_plan(
                    equity,
                    entry_price,
                    filters,
                    risk_config,
                    stop_loss_pct=stop_loss_pct,
                )
                quantity = sizing.quantity
            else:
                quantity = candidate_order_quantity(entry_price, filters, params)
            if quantity > 0:
                side = str(pending["direction"])
                stop_price = entry_price * (1.0 - stop_loss_pct if side == "LONG" else 1.0 + stop_loss_pct)
                take_profit_price = entry_price * (
                    1.0 + take_profit_pct if side == "LONG" else 1.0 - take_profit_pct
                )
                position = {
                    **pending,
                    "strategy_id": strategy_id,
                    "side": side,
                    "entry_time": candle_time_text(int(row["open_time"])),
                    "entry_open_time_ms": int(row["open_time"]),
                    "entry_price": entry_price,
                    "quantity": quantity,
                    "stop_price": stop_price,
                    "take_profit_price": take_profit_price,
                    "initial_stop_distance_pct": stop_loss_pct,
                    "initial_take_profit_distance_pct": take_profit_pct,
                    "sizing_mode": sizing_mode,
                    "risk_budget_usdt": float(sizing.risk_budget_usdt if sizing is not None else 0.0),
                    "estimated_net_loss_usdt": float(
                        sizing.estimated_net_loss_usdt if sizing is not None else 0.0
                    ),
                    "expected_reward_to_cost_ratio": entry_cost_metrics[
                        "reward_to_cost_ratio"
                    ],
                    "expected_net_reward_to_risk_ratio": entry_cost_metrics[
                        "net_reward_to_risk_ratio"
                    ],
                    "mfe_pct": 0.0,
                    "mae_pct": 0.0,
                    "time_to_mfe_seconds": 0.0,
                    "time_to_mae_seconds": 0.0,
                }
            else:
                raw_skip_reason = sizing.skip_reason if sizing is not None else ""
                rejection_counts[candidate_risk_skip_reason(raw_skip_reason)] += 1
            pending = None
            entered_this_candle = True

        if position is not None:
            side = str(position["side"])
            stop_price = float(position["stop_price"])
            take_profit_price = float(position["take_profit_price"])
            stop_loss_pct = float(position["initial_stop_distance_pct"])
            take_profit_pct = float(position["initial_take_profit_distance_pct"])
            open_price = float(row["open"])
            stop_gap = open_price <= stop_price if side == "LONG" else open_price >= stop_price
            target_gap = open_price >= take_profit_price if side == "LONG" else open_price <= take_profit_price
            stop_hit = float(row["low"]) <= stop_price if side == "LONG" else float(row["high"]) >= stop_price
            target_hit = (
                float(row["high"]) >= take_profit_price
                if side == "LONG"
                else float(row["low"]) <= take_profit_price
            )
            exit_price: Optional[float] = None
            exit_reason = ""
            gap_exit = False
            if stop_gap:
                favorable, adverse = candidate_excursion_pct(
                    side,
                    float(position["entry_price"]),
                    open_price,
                    open_price,
                )
                position["mfe_pct"] = max(float(position["mfe_pct"]), favorable)
                position["mae_pct"] = max(float(position["mae_pct"]), adverse)
                exit_price = open_price
                exit_reason = "STOP_LOSS_GAP"
                gap_exit = True
            elif target_gap:
                position["mfe_pct"] = max(float(position["mfe_pct"]), take_profit_pct)
                exit_price = take_profit_price
                exit_reason = "TAKE_PROFIT_GAP_CONSERVATIVE"
                gap_exit = True
            elif stop_hit:
                position["mae_pct"] = max(float(position["mae_pct"]), stop_loss_pct)
                position["time_to_mae_seconds"] = max(
                    (int(row["open_time"]) - int(position["entry_open_time_ms"])) / 1000.0,
                    0.0,
                )
                exit_price = stop_price
                exit_reason = "STOP_LOSS_SAME_CANDLE_CONSERVATIVE" if target_hit else "STOP_LOSS"
            elif target_hit:
                position["mfe_pct"] = max(float(position["mfe_pct"]), take_profit_pct)
                position["time_to_mfe_seconds"] = max(
                    (int(row["open_time"]) - int(position["entry_open_time_ms"])) / 1000.0,
                    0.0,
                )
                exit_price = take_profit_price
                exit_reason = "TAKE_PROFIT"
            else:
                update_candidate_excursions(position, row)
                trend_side = int(row["trend_side"]) if pd.notna(row["trend_side"]) else 0
                expected_side = 1 if side == "LONG" else -1
                if strategy_id in CANDIDATE_HIGHER_TIMEFRAME_LEDGER_NAMES:
                    regime_side = int(row["regime_side"]) if pd.notna(row.get("regime_side")) else 0
                    trend_invalidated = (
                        trend_side != expected_side or regime_side != expected_side
                    )
                    trend_exit_reason = "COMPLETED_HIGHER_TIMEFRAME_TREND_INVALIDATION"
                else:
                    trend_invalidated = strategy_id != CANDIDATE_MEAN_REVERSION_NAME and (
                        (side == "LONG" and trend_side == -1)
                        or (side == "SHORT" and trend_side == 1)
                    )
                    trend_exit_reason = "COMPLETED_15M_TREND_INVALIDATION"
                held_ms = int(row["close_time"]) - int(position["entry_open_time_ms"])
                if trend_invalidated:
                    exit_price = float(row["close"])
                    exit_reason = trend_exit_reason
                elif strategy_id == CANDIDATE_MEAN_REVERSION_NAME and pd.notna(row.get("candidate_zscore")):
                    current_zscore = float(row["candidate_zscore"])
                    mean_exit = (
                        side == "LONG" and current_zscore >= -config.exit_z
                    ) or (
                        side == "SHORT" and current_zscore <= config.exit_z
                    )
                    if mean_exit:
                        exit_price = float(row["close"])
                        exit_reason = "ZSCORE_MEAN_REVERSION"
                if exit_price is None and held_ms >= max_holding_minutes * 60 * 1000:
                    exit_price = float(row["close"])
                    exit_reason = "MAX_HOLDING_TIME"
            if exit_price is not None:
                trade = close_candidate_position(
                    position,
                    row,
                    exit_price,
                    exit_reason,
                    config,
                    funding_rates=funding_rates,
                    cost_multiplier=cost_multiplier,
                    gap_exit=gap_exit,
                )
                trades.append(trade)
                if sizing_mode == "risk_sized":
                    equity += float(trade["net_pnl"])
                position = None
                cooldown_until_ms = int(row["close_time"]) + params.cooldown_minutes * 60 * 1000
                exited_this_candle = True

        if position is not None or pending is not None or entered_this_candle or exited_this_candle:
            continue
        if int(row["close_time"]) < cooldown_until_ms or index <= 0:
            continue
        previous = features.iloc[index - 1]
        if strategy_id == CANDIDATE_MEAN_REVERSION_NAME:
            accepted, reason, direction = evaluate_mean_reversion_candidate_signal(
                row,
                previous,
                config,
            )
            pullback_distance = 0.0
        elif strategy_id in (
            CANDIDATE_BREAKOUT_LONG_NAME,
            CANDIDATE_BREAKOUT_SHORT_NAME,
        ):
            direction = candidate_strategy_direction(strategy_id)
            accepted, reason = evaluate_breakout_candidate_signal(row, direction, params)
            pullback_distance = 0.0
        elif strategy_id in CANDIDATE_REGIME_PULLBACK_LEDGER_NAMES:
            direction = candidate_strategy_direction(strategy_id)
            accepted, reason, pullback_distance = evaluate_regime_pullback_candidate_signal(
                row,
                previous,
                direction,
                params,
            )
        else:
            direction = candidate_strategy_direction(strategy_id)
            accepted, reason, pullback_distance = evaluate_candidate_signal(
                row,
                previous,
                direction,
                params,
            )
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
        slope_column = "long_slope_consistency" if direction == "LONG" else "short_slope_consistency"
        slope_value = float(row[slope_column]) if pd.notna(row[slope_column]) else 0.0
        zscore_value = float(row["candidate_zscore"]) if pd.notna(row.get("candidate_zscore")) else None
        pending = {
            "entry_index": next_index,
            "direction": direction,
            "signal_time": candle_time_text(int(row["close_time"])),
            "entry_ema20": float(row["trend_ema20"]),
            "entry_ema50": float(row["trend_ema50"]),
            "entry_ema_spread_pct": float(row["trend_ema_spread_pct"]),
            "entry_ema_slope_consistency": slope_value,
            "entry_trend_persistence": int(row["trend_persistence"]),
            "entry_trend_atr": float(row["trend_atr"]),
            "entry_execution_interval": params.execution_interval,
            "entry_atr_execution": float(row["atr_execution"]),
            "entry_atr_execution_pct": float(row["atr_execution_pct"]),
            "entry_atr_execution_percentile": float(row["atr_execution_percentile"]),
            "pullback_distance_atr": float(pullback_distance or 0.0),
            "entry_zscore": zscore_value,
            "entry_regime_interval": params.regime_interval,
            "entry_regime_fast_ema": (
                float(row["regime_fast_ema"])
                if pd.notna(row.get("regime_fast_ema"))
                else None
            ),
            "entry_regime_slow_ema": (
                float(row["regime_slow_ema"])
                if pd.notna(row.get("regime_slow_ema"))
                else None
            ),
            "entry_regime_slope_pct": (
                float(row["regime_slope_pct"])
                if pd.notna(row.get("regime_slope_pct"))
                else None
            ),
            "entry_donchian_high": (
                float(row["donchian_high"])
                if pd.notna(row.get("donchian_high"))
                else None
            ),
            "entry_donchian_low": (
                float(row["donchian_low"])
                if pd.notna(row.get("donchian_low"))
                else None
            ),
            "entry_volume_ratio": (
                float(row["volume_ratio"])
                if pd.notna(row.get("volume_ratio"))
                else None
            ),
        }

    report = build_candidate_direction_report(trades, params.minimum_train_trades, params)
    report.update(
        {
            "strategy_id": strategy_id,
            "direction": (
                candidate_strategy_direction(strategy_id)
                if strategy_id != CANDIDATE_MEAN_REVERSION_NAME
                else "BOTH"
            ),
            "sizing_mode": sizing_mode,
            "cost_multiplier": cost_multiplier,
            "starting_equity_usdt": float(
                params.starting_equity_usdt if starting_equity_usdt is None else starting_equity_usdt
            ),
            "ending_equity_usdt": equity,
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


def candidate_replay_direction(
    config: Config,
    filters: ExchangeFilters,
    features: pd.DataFrame,
    segment_start_ms: int,
    segment_end_ms: int,
    direction: str,
    params: CandidateReplayParameters,
    funding_rates: Optional[pd.DataFrame] = None,
    cost_multiplier: float = 1.0,
    sizing_mode: str = "fixed_notional",
    starting_equity_usdt: Optional[float] = None,
) -> Dict[str, Any]:
    if direction not in CANDIDATE_DIRECTIONS:
        raise ConfigError(f"Unsupported candidate direction: {direction}")
    strategy_id = (
        CANDIDATE_TREND_LONG_NAME if direction == "LONG" else CANDIDATE_TREND_SHORT_NAME
    )
    return candidate_replay_strategy(
        config,
        filters,
        features,
        segment_start_ms,
        segment_end_ms,
        strategy_id,
        params,
        funding_rates=funding_rates,
        cost_multiplier=cost_multiplier,
        sizing_mode=sizing_mode,
        starting_equity_usdt=starting_equity_usdt,
    )


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


def apply_candidate_cost_stress(
    trades: Sequence[Dict[str, Any]],
    multiplier_value: float,
) -> List[Dict[str, Any]]:
    multiplier = float(multiplier_value)
    if multiplier <= 0:
        raise ConfigError("Cost stress multipliers must be positive")
    stressed: List[Dict[str, Any]] = []
    for trade in trades:
        fees = float(trade.get("fees", 0.0) or 0.0) * multiplier
        slippage = float(trade.get("slippage", 0.0) or 0.0) * multiplier
        spread_cost = float(trade.get("spread_cost", 0.0) or 0.0) * multiplier
        funding_pnl = float(trade.get("funding_pnl", 0.0) or 0.0)
        gross_pnl = float(trade.get("gross_pnl", 0.0) or 0.0)
        execution_cost = fees + slippage + spread_cost
        net_pnl = gross_pnl + funding_pnl - execution_cost
        stressed.append(
            {
                **trade,
                "fees": fees,
                "slippage": slippage,
                "spread_cost": spread_cost,
                "execution_cost": execution_cost,
                "net_pnl": net_pnl,
                "result": "WIN" if net_pnl > 0 else "LOSS",
            }
        )
    return stressed


def build_cost_stress_report(
    trades: Sequence[Dict[str, Any]],
    multipliers: Sequence[float],
) -> Dict[str, Dict[str, Any]]:
    reports: Dict[str, Dict[str, Any]] = {}
    for multiplier_value in multipliers:
        multiplier = float(multiplier_value)
        stressed = apply_candidate_cost_stress(trades, multiplier)
        report = summarize_trade_group(stressed)
        report["cost_multiplier"] = multiplier
        reports[f"{multiplier:g}x"] = report
    return reports


def block_bootstrap_expectancy(
    trades: Sequence[Dict[str, Any]],
    samples: int,
    block_size: int,
    seed: int = 20260714,
) -> Dict[str, Optional[float]]:
    pnl = np.asarray([float(trade.get("net_pnl", 0.0) or 0.0) for trade in trades], dtype=float)
    if pnl.size == 0 or samples <= 0 or block_size <= 0:
        return {
            "samples": 0,
            "block_size": block_size,
            "lower_95": None,
            "median": None,
            "upper_95": None,
        }
    rng = np.random.default_rng(seed)
    block_count = int(math.ceil(pnl.size / block_size))
    means = np.empty(samples, dtype=float)
    offsets = np.arange(block_size)
    for sample_index in range(samples):
        starts = rng.integers(0, pnl.size, size=block_count)
        indexes = ((starts[:, None] + offsets[None, :]) % pnl.size).reshape(-1)[: pnl.size]
        means[sample_index] = float(np.mean(pnl[indexes]))
    return {
        "samples": int(samples),
        "block_size": int(block_size),
        "lower_95": float(np.quantile(means, 0.025)),
        "median": float(np.quantile(means, 0.50)),
        "upper_95": float(np.quantile(means, 0.975)),
    }


def candidate_cost_gate_labels(trade: Dict[str, Any]) -> Dict[str, str]:
    def finite_value(field_name: str) -> Optional[float]:
        try:
            value = float(trade.get(field_name))
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) else None

    atr_percentile = finite_value("entry_atr_execution_percentile")
    ema_spread = finite_value("entry_ema_spread_pct")
    pullback_distance = finite_value("pullback_distance_atr")
    slope_consistency = finite_value("entry_ema_slope_consistency")
    return {
        "atr_percentile": (
            "low"
            if atr_percentile is not None and atr_percentile <= 0.33
            else "medium"
            if atr_percentile is not None and atr_percentile <= 0.66
            else "high"
            if atr_percentile is not None
            else "unknown"
        ),
        "ema_spread": (
            "narrow"
            if ema_spread is not None and ema_spread < 0.004
            else "medium"
            if ema_spread is not None and ema_spread < 0.008
            else "wide"
            if ema_spread is not None
            else "unknown"
        ),
        "pullback_distance": (
            "shallow"
            if pullback_distance is not None and pullback_distance <= 0.35
            else "balanced"
            if pullback_distance is not None and pullback_distance <= 0.55
            else "deep"
            if pullback_distance is not None
            else "unknown"
        ),
        "slope_consistency": (
            "baseline"
            if slope_consistency is not None and slope_consistency < 0.90
            else "strong"
            if slope_consistency is not None and slope_consistency < 0.999999
            else "perfect"
            if slope_consistency is not None
            else "unknown"
        ),
    }


def fit_candidate_train_cost_gate(
    train_trades: Sequence[Dict[str, Any]],
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    dimensions = ("atr_percentile", "ema_spread", "pullback_distance", "slope_consistency")
    labels_by_trade = [(trade, candidate_cost_gate_labels(trade)) for trade in train_trades]
    dimension_reports: Dict[str, Dict[str, Any]] = {}
    allowed_labels: Dict[str, List[str]] = {}
    for dimension_index, dimension in enumerate(dimensions):
        observed_labels = sorted({labels[dimension] for _, labels in labels_by_trade})
        bucket_reports: Dict[str, Any] = {}
        accepted: List[str] = []
        for label_index, label in enumerate(observed_labels):
            bucket_trades = [trade for trade, labels in labels_by_trade if labels[dimension] == label]
            stressed = apply_candidate_cost_stress(bucket_trades, params.cost_gate_stress_multiplier)
            report = summarize_trade_group(stressed)
            bootstrap = block_bootstrap_expectancy(
                stressed,
                params.bootstrap_samples,
                params.bootstrap_block_trades,
                seed=20260714 + dimension_index * 100 + label_index,
            )
            failures: List[str] = []
            if label == "unknown":
                failures.append("UNKNOWN_BUCKET")
            if len(bucket_trades) < params.cost_gate_min_bucket_trades:
                failures.append("INSUFFICIENT_TRAIN_BUCKET_TRADES")
            if report.get("expectancy") is None or float(report["expectancy"]) <= 0:
                failures.append("NON_POSITIVE_STRESSED_EXPECTANCY")
            if (
                report.get("net_profit_factor") is None
                or float(report["net_profit_factor"]) <= params.cost_gate_min_net_profit_factor
            ):
                failures.append("STRESSED_NET_PROFIT_FACTOR_BELOW_THRESHOLD")
            if bootstrap.get("lower_95") is None or float(bootstrap["lower_95"]) <= 0:
                failures.append("STRESSED_BOOTSTRAP_LOWER_BOUND_NOT_POSITIVE")
            eligible = not failures
            if eligible:
                accepted.append(label)
            bucket_reports[label] = {
                "eligible": eligible,
                "failure_reasons": failures,
                "stressed_report": report,
                "stressed_bootstrap": bootstrap,
            }
        allowed_labels[dimension] = accepted
        dimension_reports[dimension] = bucket_reports
    gate_usable = all(allowed_labels[dimension] for dimension in dimensions)
    return {
        "gate_usable": gate_usable,
        "training_trade_count": len(train_trades),
        "stress_multiplier": params.cost_gate_stress_multiplier,
        "minimum_bucket_trades": params.cost_gate_min_bucket_trades,
        "minimum_net_profit_factor": params.cost_gate_min_net_profit_factor,
        "allowed_labels": allowed_labels,
        "dimension_reports": dimension_reports,
        "bucket_definitions_frozen": True,
    }


def filter_candidate_trades_by_cost_gate(
    trades: Sequence[Dict[str, Any]],
    gate: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not bool(gate.get("gate_usable")):
        return []
    allowed_labels = gate.get("allowed_labels", {})
    filtered: List[Dict[str, Any]] = []
    for trade in trades:
        labels = candidate_cost_gate_labels(trade)
        if all(labels.get(dimension) in set(values) for dimension, values in allowed_labels.items()):
            filtered.append(dict(trade))
    return filtered


def validate_candidate_train_cost_gate(
    validation_trades: Sequence[Dict[str, Any]],
    gate: Dict[str, Any],
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    filtered = filter_candidate_trades_by_cost_gate(validation_trades, gate)
    stressed = apply_candidate_cost_stress(filtered, params.cost_gate_stress_multiplier)
    report = summarize_trade_group(stressed)
    bootstrap = block_bootstrap_expectancy(
        stressed,
        params.bootstrap_samples,
        params.bootstrap_block_trades,
        seed=20260715,
    )
    failures: List[str] = []
    if not bool(gate.get("gate_usable")):
        failures.append("TRAIN_COST_GATE_NOT_USABLE")
    if len(filtered) < params.cost_gate_min_validation_trades:
        failures.append("INSUFFICIENT_GATED_VALIDATION_TRADES")
    if report.get("expectancy") is None or float(report["expectancy"]) <= 0:
        failures.append("NON_POSITIVE_GATED_VALIDATION_EXPECTANCY")
    if (
        report.get("net_profit_factor") is None
        or float(report["net_profit_factor"]) <= params.cost_gate_min_net_profit_factor
    ):
        failures.append("GATED_VALIDATION_NET_PROFIT_FACTOR_BELOW_THRESHOLD")
    if bootstrap.get("lower_95") is None or float(bootstrap["lower_95"]) <= 0:
        failures.append("GATED_VALIDATION_BOOTSTRAP_LOWER_BOUND_NOT_POSITIVE")
    return {
        "eligible": not failures,
        "failure_reasons": failures,
        "raw_trade_count": len(validation_trades),
        "gated_trade_count": len(filtered),
        "stressed_report": report,
        "stressed_bootstrap": bootstrap,
        "gated_trades": filtered,
    }


def apply_risk_sizing_overlay(
    trades: Sequence[Dict[str, Any]],
    filters: ExchangeFilters,
    config: Config,
    params: CandidateReplayParameters,
    strategy_id: str,
) -> Dict[str, Any]:
    equity = float(params.starting_equity_usdt)
    risk_config = candidate_risk_config(config, params)
    resized: List[Dict[str, Any]] = []
    skipped: Counter = Counter()
    scale_fields = (
        "gross_pnl",
        "fees",
        "slippage",
        "spread_cost",
        "funding_pnl",
        "execution_cost",
        "net_pnl",
        "mfe_usdt",
        "mae_usdt",
    )
    for trade in trades:
        entry_price = float(trade.get("entry_price", 0.0) or 0.0)
        original_quantity = float(trade.get("quantity", 0.0) or 0.0)
        fallback_stop_loss_pct, _, _ = candidate_strategy_profile(strategy_id, config, params)
        stop_loss_pct = float(
            trade.get("initial_stop_distance_pct", fallback_stop_loss_pct)
            or fallback_stop_loss_pct
        )
        sizing = build_risk_sizing_plan(
            equity,
            entry_price,
            filters,
            risk_config,
            stop_loss_pct=stop_loss_pct,
        )
        if sizing.quantity <= 0 or original_quantity <= 0:
            skipped[
                candidate_risk_skip_reason(sizing.skip_reason)
                if sizing.skip_reason
                else "INVALID_SOURCE_QUANTITY"
            ] += 1
            continue
        scale = sizing.quantity / original_quantity
        resized_trade = dict(trade)
        resized_trade["quantity"] = sizing.quantity
        resized_trade["entry_notional"] = sizing.quantity * entry_price
        resized_trade["exit_notional"] = sizing.quantity * float(trade.get("exit_price", 0.0) or 0.0)
        resized_trade["sizing_mode"] = "risk_sized_overlay"
        resized_trade["risk_budget_usdt"] = sizing.risk_budget_usdt
        resized_trade["estimated_net_loss_usdt"] = sizing.estimated_net_loss_usdt
        for field in scale_fields:
            resized_trade[field] = float(trade.get(field, 0.0) or 0.0) * scale
        resized_trade["result"] = "WIN" if float(resized_trade["net_pnl"]) > 0 else "LOSS"
        resized.append(resized_trade)
        equity += float(resized_trade["net_pnl"])
    report = build_candidate_direction_report(resized, params.minimum_out_of_sample_trades, params)
    total_signals = len(resized) + int(sum(skipped.values()))
    execution_coverage = len(resized) / total_signals if total_signals else 0.0
    report.update(
        {
            "strategy_id": strategy_id,
            "sizing_mode": "risk_sized_overlay",
            "starting_equity_usdt": params.starting_equity_usdt,
            "ending_equity_usdt": equity,
            "maximum_net_loss_per_trade_usdt": params.executable_max_net_loss_per_trade_usdt,
            "maximum_risk_per_trade_pct": params.executable_max_risk_per_trade_pct,
            "skipped_trade_count": int(sum(skipped.values())),
            "source_signal_count": total_signals,
            "execution_coverage": execution_coverage,
            "skip_reasons": dict(sorted(skipped.items())),
            "signal_schedule_source": "fixed_notional_replay",
        }
    )
    return {"report": report, "trades": resized}


def candidate_direction_gate(
    report: Dict[str, Any],
    minimum_trades: int,
    params: CandidateReplayParameters,
    minimum_net_profit_factor: Optional[float] = None,
) -> Dict[str, Any]:
    profit_factor_threshold = (
        params.minimum_net_profit_factor
        if minimum_net_profit_factor is None
        else minimum_net_profit_factor
    )
    failures: List[str] = []
    if int(report.get("trades", 0) or 0) < minimum_trades:
        failures.append("INSUFFICIENT_TRADES")
    expectancy = report.get("expectancy")
    if expectancy is None or float(expectancy) <= 0:
        failures.append("NON_POSITIVE_EXPECTANCY")
    net_profit_factor = report.get("net_profit_factor")
    if net_profit_factor is None or float(net_profit_factor) <= profit_factor_threshold:
        failures.append("NET_PROFIT_FACTOR_BELOW_THRESHOLD")
    if (
        params.enforce_fixed_notional_drawdown
        and float(report.get("max_drawdown", 0.0) or 0.0) >= params.maximum_drawdown_usdt
    ):
        failures.append("DRAWDOWN_LIMIT_EXCEEDED")
    cost_ratio = report.get("cost_to_gross_profit_ratio")
    if cost_ratio is None or float(cost_ratio) > params.maximum_cost_to_gross_profit_ratio:
        failures.append("COST_RATIO_EXCEEDED")
    return {
        "eligible": not failures,
        "minimum_trades": minimum_trades,
        "minimum_net_profit_factor": profit_factor_threshold,
        "failure_reasons": failures,
    }


def build_candidate_direction_report(
    trades: Sequence[Dict[str, Any]],
    minimum_trades: int,
    params: CandidateReplayParameters,
    minimum_net_profit_factor: Optional[float] = None,
) -> Dict[str, Any]:
    trade_list = list(trades)
    base = summarize_trade_group(trade_list)
    gross_profit = sum(max(float(trade.get("gross_pnl", 0.0) or 0.0), 0.0) for trade in trade_list)
    total_cost = float(base["fees"]) + float(base["slippage"]) + float(base["spread_cost"])
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
    report["gate"] = candidate_direction_gate(
        report,
        minimum_trades,
        params,
        minimum_net_profit_factor=minimum_net_profit_factor,
    )
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


def deduplicate_candidate_trades(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for trade in trades:
        key = (
            str(trade.get("strategy_id", "")),
            str(trade.get("side", "")),
            str(trade.get("entry_time", "")),
            str(trade.get("exit_time", "")),
        )
        unique.setdefault(key, dict(trade))
    return sorted(
        unique.values(),
        key=lambda trade: (str(trade.get("entry_time", "")), str(trade.get("exit_time", ""))),
    )


def candidate_trade_timestamp_ms(trade: Dict[str, Any], field: str) -> Optional[int]:
    value = trade.get(f"{field}_ms")
    if value is not None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    parsed = parse_time_text(str(trade.get(field, "")))
    return int(parsed.timestamp() * 1000) if parsed is not None else None


def slice_candidate_replay_result(
    master_result: Dict[str, Any],
    strategy_id: str,
    segment_start_ms: int,
    segment_end_ms: int,
    params: CandidateReplayParameters,
    evaluated_candles: int,
) -> Dict[str, Any]:
    trades: List[Dict[str, Any]] = []
    excluded_boundary_trades = 0
    for trade in master_result["trades"]:
        entry_ms = candidate_trade_timestamp_ms(trade, "entry_time")
        exit_ms = candidate_trade_timestamp_ms(trade, "exit_time")
        if entry_ms is None or exit_ms is None:
            excluded_boundary_trades += 1
            continue
        if segment_start_ms <= entry_ms and exit_ms < segment_end_ms:
            trades.append(trade)
        elif entry_ms < segment_end_ms and exit_ms >= segment_start_ms:
            excluded_boundary_trades += 1
    report = build_candidate_direction_report(trades, params.minimum_out_of_sample_trades, params)
    report.update(
        {
            "strategy_id": strategy_id,
            "segment_start": candle_time_text(segment_start_ms),
            "segment_end_exclusive": candle_time_text(segment_end_ms),
            "evaluated_candles": int(evaluated_candles),
            "completed_signals": len(trades),
            "boundary_crossing_or_unparseable_trades_excluded": excluded_boundary_trades,
            "continuous_master_replay": True,
        }
    )
    return {"report": report, "trades": trades}


def build_walk_forward_strategy_validation(
    strategy_id: str,
    fixed_trades_by_segment: Dict[str, Sequence[Dict[str, Any]]],
    risk_test_result: Dict[str, Any],
    window_reports_by_segment: Dict[str, Sequence[Dict[str, Any]]],
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    minimum_validation_trades = (
        params.minimum_out_of_sample_trades
        if params.minimum_validation_trades is None
        else params.minimum_validation_trades
    )
    minimum_train_profit_factor = (
        params.minimum_net_profit_factor
        if params.minimum_train_net_profit_factor is None
        else params.minimum_train_net_profit_factor
    )
    minimum_validation_profit_factor = (
        params.minimum_net_profit_factor
        if params.minimum_validation_net_profit_factor is None
        else params.minimum_validation_net_profit_factor
    )
    minimum_test_profit_factor = (
        params.minimum_net_profit_factor
        if params.minimum_test_net_profit_factor is None
        else params.minimum_test_net_profit_factor
    )
    fixed_test_trades = list(fixed_trades_by_segment["test"])
    fixed_report = build_candidate_direction_report(
        fixed_test_trades,
        params.minimum_out_of_sample_trades,
        params,
        minimum_net_profit_factor=minimum_test_profit_factor,
    )
    cost_stress = build_cost_stress_report(fixed_test_trades, params.cost_stress_multipliers)
    bootstrap = block_bootstrap_expectancy(
        fixed_test_trades,
        params.bootstrap_samples,
        params.bootstrap_block_trades,
    )
    positive_windows = sum(
        1
        for report in window_reports_by_segment["test"]
        if report.get("expectancy") is not None and float(report["expectancy"]) > 0
    )
    total_windows = len(window_reports_by_segment["test"])
    stressed_15x = cost_stress.get("1.5x", {})
    risk_report = dict(risk_test_result["report"])
    train_report = build_candidate_direction_report(
        fixed_trades_by_segment["train"],
        params.minimum_train_trades,
        params,
        minimum_net_profit_factor=minimum_train_profit_factor,
    )
    validation_report = build_candidate_direction_report(
        fixed_trades_by_segment["validation"],
        minimum_validation_trades,
        params,
        minimum_net_profit_factor=minimum_validation_profit_factor,
    )
    validation_stress = build_cost_stress_report(
        fixed_trades_by_segment["validation"],
        params.cost_stress_multipliers,
    )
    validation_15x = validation_stress.get("1.5x", {})
    validation_positive_windows = sum(
        1
        for report in window_reports_by_segment["validation"]
        if report.get("expectancy") is not None and float(report["expectancy"]) > 0
    )
    validation_window_count = len(window_reports_by_segment["validation"])
    selection_failures: List[str] = []
    if int(train_report.get("trades", 0) or 0) < params.minimum_train_trades:
        selection_failures.append("INSUFFICIENT_TRAIN_TRADES")
    if train_report.get("expectancy") is None or float(train_report["expectancy"]) <= 0:
        selection_failures.append("NON_POSITIVE_TRAIN_EXPECTANCY")
    if (
        train_report.get("net_profit_factor") is None
        or float(train_report["net_profit_factor"]) <= minimum_train_profit_factor
    ):
        selection_failures.append("TRAIN_NET_PROFIT_FACTOR_BELOW_THRESHOLD")
    if int(validation_report.get("trades", 0) or 0) < minimum_validation_trades:
        selection_failures.append("INSUFFICIENT_VALIDATION_TRADES")
    if validation_report.get("expectancy") is None or float(validation_report["expectancy"]) <= 0:
        selection_failures.append("NON_POSITIVE_VALIDATION_EXPECTANCY")
    validation_positive_fraction = (
        validation_positive_windows / validation_window_count
        if validation_window_count
        else 0.0
    )
    if validation_positive_fraction < params.minimum_positive_window_fraction:
        selection_failures.append("VALIDATION_NOT_POSITIVE_IN_MAJORITY_OF_WINDOWS")
    if (
        validation_15x.get("net_profit_factor") is None
        or float(validation_15x["net_profit_factor"]) <= minimum_validation_profit_factor
        or float(validation_15x.get("net_pnl", 0.0) or 0.0) <= 0
    ):
        selection_failures.append("VALIDATION_FAILED_1_5X_COST_STRESS")
    failures: List[str] = []
    if selection_failures:
        failures.append("STRATEGY_NOT_PRESELECTED_BY_TRAIN_AND_VALIDATION")
    if total_windows < params.minimum_walk_forward_windows:
        failures.append("INSUFFICIENT_WALK_FORWARD_WINDOWS")
    positive_window_fraction = positive_windows / total_windows if total_windows else 0.0
    if positive_window_fraction < params.minimum_positive_window_fraction:
        failures.append("POSITIVE_EXPECTANCY_NOT_PRESENT_IN_MAJORITY_OF_WINDOWS")
    if int(fixed_report.get("trades", 0) or 0) < params.minimum_out_of_sample_trades:
        failures.append("INSUFFICIENT_OUT_OF_SAMPLE_TRADES")
    if fixed_report.get("expectancy") is None or float(fixed_report["expectancy"]) <= 0:
        failures.append("NON_POSITIVE_FIXED_NOTIONAL_EXPECTANCY")
    if (
        stressed_15x.get("net_profit_factor") is None
        or float(stressed_15x["net_profit_factor"]) <= minimum_test_profit_factor
        or float(stressed_15x.get("net_pnl", 0.0) or 0.0) <= 0
    ):
        failures.append("FAILED_1_5X_COST_STRESS")
    if bootstrap.get("lower_95") is None or float(bootstrap["lower_95"]) <= 0:
        failures.append("BOOTSTRAP_EXPECTANCY_LOWER_BOUND_NOT_POSITIVE")
    if (
        params.enforce_fixed_notional_drawdown
        and float(fixed_report.get("max_drawdown", 0.0) or 0.0) >= params.maximum_drawdown_usdt
    ):
        failures.append("DRAWDOWN_LIMIT_EXCEEDED")
    if int(risk_report.get("trades", 0) or 0) == 0:
        failures.append("NO_RISK_SIZED_ORDERS_FIT_EXCHANGE_FILTERS")
    elif float(risk_report.get("net_pnl", 0.0) or 0.0) <= 0:
        failures.append("NON_POSITIVE_RISK_SIZED_NET_PNL")
    if (
        float(risk_report.get("max_drawdown", 0.0) or 0.0)
        >= params.maximum_risk_sized_drawdown_usdt
    ):
        failures.append("RISK_SIZED_DRAWDOWN_LIMIT_EXCEEDED")
    if (
        params.minimum_risk_sized_coverage > 0
        and float(risk_report.get("execution_coverage", 0.0) or 0.0)
        < params.minimum_risk_sized_coverage
    ):
        failures.append("RISK_SIZED_EXECUTION_COVERAGE_BELOW_THRESHOLD")
    historical_gate_passed = not failures
    return {
        "strategy_id": strategy_id,
        "preselection": {
            "eligible": not selection_failures,
            "failure_reasons": selection_failures,
            "train": train_report,
            "validation": validation_report,
            "validation_cost_stress": validation_stress,
            "validation_positive_windows": validation_positive_windows,
            "validation_window_count": validation_window_count,
            "validation_positive_window_fraction": validation_positive_fraction,
        },
        "fixed_notional": fixed_report,
        "risk_sized": risk_report,
        "cost_stress": cost_stress,
        "block_bootstrap_expectancy": bootstrap,
        "walk_forward_consistency": {
            "total_test_windows": total_windows,
            "positive_expectancy_windows": positive_windows,
            "positive_window_fraction": positive_window_fraction if total_windows else None,
            "window_test_reports": list(window_reports_by_segment["test"]),
        },
        "historical_gate_passed": historical_gate_passed,
        "eligible_for_forward_shadow_validation": historical_gate_passed,
        "eligible_for_micro_live_test": historical_gate_passed
        and not params.requires_fresh_forward_validation,
        "failure_reasons": failures,
    }


def run_candidate_replay(
    config: Config,
    days: int,
    end_time: Optional[str],
    cache_file: str,
    output_dir_value: str,
    params: Optional[CandidateReplayParameters] = None,
    funding_cache_file: Optional[str] = None,
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

    source_interval = params.source_interval
    market_data_config = replace(config, interval=source_interval)
    end_exclusive_ms = aligned_replay_end_ms(source_interval, end_time)
    windows = rolling_walk_forward_boundaries(days, end_exclusive_ms, params)
    source_seconds = interval_seconds(source_interval)
    execution_seconds = interval_seconds(params.execution_interval)
    trend_seconds = interval_seconds(params.trend_interval)
    regime_seconds = interval_seconds(params.regime_interval) if params.regime_interval else None
    if source_seconds is None or execution_seconds is None or trend_seconds is None:
        raise ConfigError("Candidate replay intervals are invalid")
    if execution_seconds % source_seconds != 0 or trend_seconds % execution_seconds != 0:
        raise ConfigError("Candidate replay intervals must be exact multiples of each other")
    if regime_seconds is not None and regime_seconds % execution_seconds != 0:
        raise ConfigError("Candidate regime interval must be an exact multiple of execution interval")
    warmup_seconds = max(
        params.atr_percentile_window * execution_seconds,
        (params.trend_slow_ema_period + params.slope_consistency_window + params.trend_atr_period) * trend_seconds,
    )
    if regime_seconds is not None:
        warmup_seconds = max(
            warmup_seconds,
            (params.regime_slow_ema_period + params.regime_slope_lookback) * regime_seconds,
        )
    warmup_ms = warmup_seconds * 1000
    download_start_ms = int(windows[0]["train"][0]) - warmup_ms - 2 * trend_seconds * 1000
    cache_path = resolve_app_path(cache_file)
    funding_cache_path = resolve_app_path(
        funding_cache_file or f"historical_data/{config.symbol}_funding.csv"
    )
    output_dir = resolve_app_path(output_dir_value)
    client = build_public_replay_client(config)
    filters = get_exchange_filters(client, config.symbol, config)
    all_klines = ensure_historical_kline_cache(
        client,
        market_data_config,
        download_start_ms,
        end_exclusive_ms - 1,
        cache_path,
    )
    funding_rates = ensure_funding_rate_cache(
        client,
        config,
        int(windows[0]["train"][0]),
        end_exclusive_ms - 1,
        funding_cache_path,
    )
    features = build_candidate_feature_frame(
        all_klines,
        params,
        source_interval,
        params.trend_interval,
        config=config,
    )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / "runs" / run_id
    master_start_ms = int(windows[0]["train"][0])
    master_results: Dict[str, Dict[str, Any]] = {}
    for strategy_id in params.strategy_ids:
        LOGGER.info("Candidate continuous master replay started strategy=%s", strategy_id)
        master_results[strategy_id] = candidate_replay_strategy(
            config,
            filters,
            features,
            master_start_ms,
            end_exclusive_ms,
            strategy_id,
            params,
            funding_rates=funding_rates,
        )
        write_candidate_trades(
            run_dir / f"master_{strategy_id.lower()}_fixed.csv",
            master_results[strategy_id]["trades"],
        )
    feature_close_times = features["close_time"].astype("int64").to_numpy()
    aggregate_fixed: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        strategy_id: {segment: [] for segment in ("train", "validation", "test")}
        for strategy_id in params.strategy_ids
    }
    aggregate_raw: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        strategy_id: {segment: [] for segment in ("train", "validation", "test")}
        for strategy_id in params.strategy_ids
    }
    window_reports: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        strategy_id: {segment: [] for segment in ("train", "validation", "test")}
        for strategy_id in params.strategy_ids
    }
    window_summaries: List[Dict[str, Any]] = []

    for window in windows:
        window_id = str(window["window_id"])
        LOGGER.info("Candidate walk-forward window started window=%s", window_id)
        window_summary: Dict[str, Any] = {
            "window_id": window_id,
            "embargo_minutes": params.embargo_minutes,
            "periods": {},
            "strategies": {},
        }
        for segment_name in ("train", "validation", "test"):
            start_ms, end_ms = window[segment_name]
            window_summary["periods"][segment_name] = {
                "start": candle_time_text(start_ms),
                "end_exclusive": candle_time_text(end_ms),
            }
        for strategy_id in params.strategy_ids:
            strategy_summary: Dict[str, Any] = {}
            raw_results: Dict[str, Dict[str, Any]] = {}
            for segment_name in ("train", "validation", "test"):
                start_ms, end_ms = window[segment_name]
                evaluated_candles = int(
                    np.count_nonzero(
                        (feature_close_times >= start_ms) & (feature_close_times < end_ms)
                    )
                )
                raw_results[segment_name] = slice_candidate_replay_result(
                    master_results[strategy_id],
                    strategy_id,
                    start_ms,
                    end_ms,
                    params,
                    evaluated_candles,
                )

            selected_trades = {
                segment_name: list(raw_results[segment_name]["trades"])
                for segment_name in ("train", "validation", "test")
            }
            cost_gate_summary: Dict[str, Any] = {"enabled": False}
            if params.train_cost_gate_enabled:
                train_gate = fit_candidate_train_cost_gate(selected_trades["train"], params)
                gated_train = filter_candidate_trades_by_cost_gate(selected_trades["train"], train_gate)
                validation_gate = validate_candidate_train_cost_gate(
                    selected_trades["validation"],
                    train_gate,
                    params,
                )
                gated_validation = list(validation_gate.pop("gated_trades"))
                gated_test = (
                    filter_candidate_trades_by_cost_gate(selected_trades["test"], train_gate)
                    if bool(validation_gate["eligible"])
                    else []
                )
                selected_trades = {
                    "train": gated_train,
                    "validation": gated_validation,
                    "test": gated_test,
                }
                cost_gate_summary = {
                    "enabled": True,
                    "selection_source": "TRAIN_ONLY",
                    "validation_source": "VALIDATION_ONLY",
                    "test_data_used_for_selection": False,
                    "train_gate": train_gate,
                    "validation_gate": validation_gate,
                    "test_evaluation_authorized": bool(validation_gate["eligible"]),
                    "raw_test_trade_count": len(raw_results["test"]["trades"]),
                    "gated_test_trade_count": len(gated_test),
                }

            for segment_name in ("train", "validation", "test"):
                start_ms, end_ms = window[segment_name]
                if segment_name == "train":
                    minimum_trades = params.minimum_train_trades
                    minimum_profit_factor = params.minimum_train_net_profit_factor
                elif segment_name == "validation":
                    minimum_trades = (
                        params.minimum_out_of_sample_trades
                        if params.minimum_validation_trades is None
                        else params.minimum_validation_trades
                    )
                    minimum_profit_factor = params.minimum_validation_net_profit_factor
                else:
                    minimum_trades = params.minimum_out_of_sample_trades
                    minimum_profit_factor = params.minimum_test_net_profit_factor
                fixed_report = build_candidate_direction_report(
                    selected_trades[segment_name],
                    minimum_trades,
                    params,
                    minimum_net_profit_factor=minimum_profit_factor,
                )
                fixed_report.update(
                    {
                        "strategy_id": strategy_id,
                        "segment_start": candle_time_text(start_ms),
                        "segment_end_exclusive": candle_time_text(end_ms),
                        "evaluated_candles": raw_results[segment_name]["report"].get(
                            "evaluated_candles", 0
                        ),
                        "raw_trade_count": len(raw_results[segment_name]["trades"]),
                        "train_cost_gate_applied": params.train_cost_gate_enabled,
                    }
                )
                risk_result = apply_risk_sizing_overlay(
                    selected_trades[segment_name],
                    filters,
                    config,
                    params,
                    strategy_id,
                )
                strategy_summary[segment_name] = {
                    "fixed_notional": fixed_report,
                    "risk_sized": risk_result["report"],
                }
                if params.train_cost_gate_enabled:
                    write_candidate_trades(
                        run_dir / window_id / f"{strategy_id.lower()}_{segment_name}_raw.csv",
                        raw_results[segment_name]["trades"],
                    )
                write_candidate_trades(
                    run_dir / window_id / f"{strategy_id.lower()}_{segment_name}_fixed.csv",
                    selected_trades[segment_name],
                )
                write_candidate_trades(
                    run_dir / window_id / f"{strategy_id.lower()}_{segment_name}_risk.csv",
                    risk_result["trades"],
                )
                aggregate_fixed[strategy_id][segment_name].extend(selected_trades[segment_name])
                aggregate_raw[strategy_id][segment_name].extend(
                    raw_results[segment_name]["trades"]
                )
                window_reports[strategy_id][segment_name].append(fixed_report)
            strategy_summary["cost_gate"] = cost_gate_summary
            window_summary["strategies"][strategy_id] = strategy_summary
        window_summaries.append(window_summary)

    validation: Dict[str, Dict[str, Any]] = {}
    for strategy_id in params.strategy_ids:
        deduplicated = {
            segment_name: deduplicate_candidate_trades(aggregate_fixed[strategy_id][segment_name])
            for segment_name in ("train", "validation", "test")
        }
        raw_deduplicated = {
            segment_name: deduplicate_candidate_trades(aggregate_raw[strategy_id][segment_name])
            for segment_name in ("train", "validation", "test")
        }
        risk_test_result = apply_risk_sizing_overlay(
            deduplicated["test"],
            filters,
            config,
            params,
            strategy_id,
        )
        validation[strategy_id] = build_walk_forward_strategy_validation(
            strategy_id,
            deduplicated,
            risk_test_result,
            window_reports[strategy_id],
            params,
        )
        validation[strategy_id]["raw_baseline_not_used_for_selection"] = True
        validation[strategy_id]["raw_baseline"] = {
            segment_name: build_candidate_direction_report(
                raw_deduplicated[segment_name],
                params.minimum_train_trades
                if segment_name == "train"
                else (
                    params.minimum_out_of_sample_trades
                    if segment_name == "test" or params.minimum_validation_trades is None
                    else params.minimum_validation_trades
                ),
                params,
                minimum_net_profit_factor=(
                    params.minimum_train_net_profit_factor
                    if segment_name == "train"
                    else params.minimum_validation_net_profit_factor
                    if segment_name == "validation"
                    else params.minimum_test_net_profit_factor
                ),
            )
            for segment_name in ("train", "validation", "test")
        }
        for segment_name in ("train", "validation", "test"):
            write_candidate_trades(
                run_dir / f"aggregate_{segment_name}_{strategy_id.lower()}_fixed.csv",
                deduplicated[segment_name],
            )
            if params.train_cost_gate_enabled:
                write_candidate_trades(
                    run_dir / f"aggregate_{segment_name}_{strategy_id.lower()}_raw.csv",
                    raw_deduplicated[segment_name],
                )
        write_candidate_trades(
            run_dir / f"oos_test_{strategy_id.lower()}_risk.csv",
            risk_test_result["trades"],
        )

    forward_shadow_candidates = sorted(
        strategy_id
        for strategy_id, result in validation.items()
        if bool(result["eligible_for_forward_shadow_validation"])
    )
    viability_status = (
        "ELIGIBLE_FOR_FORWARD_SHADOW_VALIDATION"
        if forward_shadow_candidates
        else "NOT_VIABLE_OR_INSUFFICIENT_OUT_OF_SAMPLE_EVIDENCE"
    )
    summary = {
        "engine": params.engine_name,
        "profile_name": params.profile_name,
        "generated_at": utc_now_text(),
        "run_id": run_id,
        "strategies": list(params.strategy_ids),
        "legacy_strategies_frozen": list(LEGACY_CANDIDATE_LEDGER_NAMES),
        "symbol": config.symbol,
        "source_interval": source_interval,
        "execution_interval": params.execution_interval,
        "trend_interval": params.trend_interval,
        "regime_interval": params.regime_interval,
        "days": days,
        "shadow_mode_only": True,
        "dry_run": True,
        "live_trading": False,
        "account_state_changed": False,
        "forward_shadow_execution_enabled": config.forward_shadow_execution_enabled,
        "ui_changed": False,
        "strategy_parameters_frozen": True,
        "train_cost_gate_enabled": params.train_cost_gate_enabled,
        "test_data_used_for_cost_gate_selection": False,
        "historical_profile_selection_is_not_live_evidence": True,
        "fresh_forward_validation_required": params.requires_fresh_forward_validation,
        "continuous_master_replay_per_strategy": True,
        "master_replay_count": len(params.strategy_ids),
        "window_metrics_are_sliced_from_master_replays": True,
        "fixed_notional_view_purpose": "normalized_strategy_edge_validation",
        "risk_sized_view_purpose": "five_usdt_exchange_feasibility_and_account_risk_validation",
        "source_sha256": file_sha256(Path(__file__).resolve()),
        "cache_file": str(cache_path),
        "cache_sha256": file_sha256(cache_path),
        "funding_cache_file": str(funding_cache_path),
        "funding_cache_sha256": file_sha256(funding_cache_path),
        "cached_candles": len(all_klines),
        "cached_funding_rates": len(funding_rates),
        "parameters": asdict(params),
        "estimated_round_trip_cost_to_gross_tp_ratio": estimated_tp_cost_ratio,
        "walk_forward": {
            "window_count": len(windows),
            "minimum_required_windows": params.minimum_walk_forward_windows,
            "train_days": params.walk_forward_train_days,
            "validation_days": params.walk_forward_validation_days,
            "test_days": params.walk_forward_test_days,
            "step_days": params.walk_forward_step_days,
            "embargo_minutes": params.embargo_minutes,
        },
        "windows": window_summaries,
        "out_of_sample_validation": validation,
        "eligible_strategies": forward_shadow_candidates,
        "forward_shadow_candidate_strategies": forward_shadow_candidates,
        "viability_status": viability_status,
        "recommendation": (
            "BEGIN_FORWARD_SHADOW_VALIDATION"
            if forward_shadow_candidates
            else "DO_NOT_TRADE_LIVE"
        ),
    }
    write_json_atomic(run_dir / "summary.json", summary)
    write_json_atomic(output_dir / "latest.json", summary)
    return summary


def normalize_candidate_symbols(symbols: Sequence[str]) -> Tuple[str, ...]:
    normalized: List[str] = []
    for symbol_value in symbols:
        symbol = str(symbol_value or "").strip().upper()
        if not symbol or not symbol.isalnum() or not symbol.endswith("USDT"):
            raise ConfigError(f"Invalid USD-M candidate symbol: {symbol_value}")
        if symbol not in normalized:
            normalized.append(symbol)
    if not normalized:
        raise ConfigError("At least one candidate symbol is required")
    return tuple(normalized)


def run_candidate_portfolio_replay(
    config: Config,
    symbols: Sequence[str],
    days: int,
    end_time: Optional[str],
    output_dir_value: str,
    params: CandidateReplayParameters,
) -> Dict[str, Any]:
    if config.live_trading or not config.dry_run or not config.shadow_mode:
        raise ConfigError("Portfolio replay requires LIVE_TRADING=false, DRY_RUN=true, and SHADOW_MODE=true")
    normalized_symbols = normalize_candidate_symbols(symbols)
    qualification_mode = params.portfolio_symbol_qualification_mode
    if qualification_mode not in {"all_strategies", "any_strategy"}:
        raise ConfigError(f"Unsupported portfolio symbol qualification mode: {qualification_mode}")
    output_dir = resolve_app_path(output_dir_value)
    symbol_summaries: Dict[str, Any] = {}
    errors: Dict[str, str] = {}
    qualified_symbols: List[str] = []
    qualified_strategy_pairs: List[Dict[str, str]] = []
    for symbol in normalized_symbols:
        symbol_config = replace(config, symbol=symbol)
        symbol_output = output_dir / symbol
        try:
            result = run_candidate_replay(
                symbol_config,
                days=days,
                end_time=end_time,
                cache_file=f"historical_data/{symbol}_{params.source_interval}.csv",
                funding_cache_file=f"historical_data/{symbol}_funding.csv",
                output_dir_value=str(symbol_output),
                params=params,
            )
        except Exception as exc:
            LOGGER.exception("Candidate portfolio replay failed symbol=%s", symbol)
            errors[symbol] = f"{type(exc).__name__}: {exc}"
            continue
        strategy_summaries: Dict[str, Any] = {}
        strategy_qualifications: List[bool] = []
        qualified_strategy_ids: List[str] = []
        for strategy_id in params.strategy_ids:
            validation = result["out_of_sample_validation"][strategy_id]
            fixed = validation["fixed_notional"]
            risk = validation["risk_sized"]
            raw_test = validation["raw_baseline"]["test"]
            strategy_qualified = bool(validation["eligible_for_forward_shadow_validation"])
            strategy_qualifications.append(strategy_qualified)
            if strategy_qualified:
                qualified_strategy_ids.append(strategy_id)
                qualified_strategy_pairs.append({"symbol": symbol, "strategy_id": strategy_id})
            strategy_summaries[strategy_id] = {
                "historical_gate_passed": bool(validation["historical_gate_passed"]),
                "eligible_for_forward_shadow_validation": strategy_qualified,
                "failure_reasons": list(validation["failure_reasons"]),
                "test_trades": int(fixed.get("trades", 0) or 0),
                "test_net_pnl": fixed.get("net_pnl"),
                "test_expectancy": fixed.get("expectancy"),
                "test_net_profit_factor": fixed.get("net_profit_factor"),
                "raw_test_trades": int(raw_test.get("trades", 0) or 0),
                "raw_test_net_pnl": raw_test.get("net_pnl"),
                "raw_test_expectancy": raw_test.get("expectancy"),
                "raw_test_net_profit_factor": raw_test.get("net_profit_factor"),
                "positive_window_fraction": validation["walk_forward_consistency"].get(
                    "positive_window_fraction"
                ),
                "risk_sized_trades": int(risk.get("trades", 0) or 0),
                "risk_sized_execution_coverage": float(risk.get("execution_coverage", 0.0) or 0.0),
                "risk_sized_net_pnl": risk.get("net_pnl"),
            }
        symbol_qualified = (
            all(strategy_qualifications)
            if qualification_mode == "all_strategies"
            else any(strategy_qualifications)
        )
        if symbol_qualified:
            qualified_symbols.append(symbol)
        symbol_summaries[symbol] = {
            "report_file": str(symbol_output / "latest.json"),
            "viability_status": result["viability_status"],
            "qualified": symbol_qualified,
            "qualified_strategy_ids": qualified_strategy_ids,
            "strategies": strategy_summaries,
        }
    minimum_qualified_symbols = max(
        1,
        int(math.ceil(len(normalized_symbols) * params.minimum_cross_symbol_qualified_fraction)),
    )
    portfolio_qualified = (
        not errors
        and len(qualified_symbols) >= minimum_qualified_symbols
        and len(symbol_summaries) == len(normalized_symbols)
    )
    summary = {
        "engine": "cross_symbol_candidate_portfolio_replay_v1",
        "generated_at": utc_now_text(),
        "profile_name": params.profile_name,
        "profile_engine": params.engine_name,
        "symbols": list(normalized_symbols),
        "days": days,
        "source_interval": params.source_interval,
        "execution_interval": params.execution_interval,
        "trend_interval": params.trend_interval,
        "regime_interval": params.regime_interval,
        "strategies": list(params.strategy_ids),
        "symbol_summaries": symbol_summaries,
        "errors": errors,
        "qualified_symbols": qualified_symbols,
        "qualified_strategy_pairs": qualified_strategy_pairs,
        "symbol_qualification_mode": qualification_mode,
        "minimum_qualified_symbols": minimum_qualified_symbols,
        "cross_symbol_qualified_fraction": (
            len(qualified_symbols) / len(normalized_symbols) if normalized_symbols else 0.0
        ),
        "portfolio_historical_gate_passed": portfolio_qualified,
        "eligible_for_forward_shadow_validation": portfolio_qualified,
        "eligible_for_micro_live_test": False,
        "fresh_forward_validation_required": True,
        "shadow_mode_only": True,
        "dry_run": True,
        "live_trading": False,
        "account_state_changed": False,
        "recommendation": (
            "BEGIN_FORWARD_SHADOW_VALIDATION" if portfolio_qualified else "DO_NOT_TRADE_LIVE"
        ),
        "source_sha256": file_sha256(Path(__file__).resolve()),
        "parameters": asdict(params),
    }
    write_json_atomic(output_dir / "latest.json", summary)
    return summary


def parse_candidate_replay_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run independent rolling walk-forward strategy validation")
    parser.add_argument("--profile", choices=CANDIDATE_REPLAY_PROFILES, default=CANDIDATE_PROFILE_V8)
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--cache-file", default=None)
    parser.add_argument("--funding-cache-file", default=None)
    parser.add_argument("--output-dir", default="candidate_replay")
    return parser.parse_args(list(arguments))


def candidate_replay_main(arguments: Sequence[str]) -> None:
    args = parse_candidate_replay_args(arguments)
    config = load_config()
    params = candidate_replay_parameters_for_profile(args.profile)
    cache_file = args.cache_file or f"historical_data/{config.symbol}_{params.source_interval}.csv"
    summary = run_candidate_replay(
        config,
        days=args.days or params.default_replay_days,
        end_time=args.end_time,
        cache_file=cache_file,
        output_dir_value=args.output_dir,
        params=params,
        funding_cache_file=args.funding_cache_file,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False))


def parse_candidate_portfolio_replay_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run cross-symbol candidate robustness validation")
    parser.add_argument("--profile", choices=CANDIDATE_REPLAY_PROFILES, default=CANDIDATE_PROFILE_V9)
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--output-dir", default="candidate_portfolio_replay")
    return parser.parse_args(list(arguments))


def candidate_portfolio_replay_main(arguments: Sequence[str]) -> None:
    args = parse_candidate_portfolio_replay_args(arguments)
    config = load_config()
    params = candidate_replay_parameters_for_profile(args.profile)
    symbols_value = args.symbols or ",".join(params.portfolio_symbols)
    symbols = tuple(item for item in str(symbols_value).split(",") if item.strip())
    summary = run_candidate_portfolio_replay(
        config,
        symbols=symbols,
        days=args.days or params.default_replay_days,
        end_time=args.end_time,
        output_dir_value=args.output_dir,
        params=params,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False))


def parse_momentum_rotation_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run frozen V10 cross-sectional momentum validation")
    parser.add_argument("--days", type=int, default=720)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--cache-dir", default="historical_data/v10_momentum")
    parser.add_argument("--output-dir", default="momentum_replay_v10")
    return parser.parse_args(list(arguments))


def momentum_rotation_main(arguments: Sequence[str]) -> None:
    args = parse_momentum_rotation_args(arguments)
    config = load_config()
    summary = run_momentum_rotation_replay(
        config,
        days=args.days,
        end_time=args.end_time,
        cache_dir_value=args.cache_dir,
        output_dir_value=args.output_dir,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False))


def parse_feasibility_stop_scenarios(value: str) -> Tuple[float, ...]:
    try:
        values = tuple(float(item.strip()) for item in str(value).split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Stop scenarios must be comma-separated decimal values") from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("Stop scenarios must contain positive values")
    return values


def parse_feasibility_capital_scenarios(value: str) -> Tuple[float, ...]:
    try:
        values = tuple(float(item.strip()) for item in str(value).split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Capital scenarios must be comma-separated decimal values") from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("Capital scenarios must contain positive values")
    return values


def parse_feasibility_scan_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan the public USD-M perpetual universe for risk feasibility")
    parser.add_argument("--balance", type=float, default=5.0)
    parser.add_argument("--max-net-loss", type=float, default=0.05)
    parser.add_argument("--max-risk-pct", type=float, default=0.01)
    parser.add_argument("--primary-stop-pct", type=float, default=0.01)
    parser.add_argument(
        "--stop-scenarios",
        type=parse_feasibility_stop_scenarios,
        default=(0.006, 0.01, 0.015),
    )
    parser.add_argument(
        "--capital-scenarios",
        type=parse_feasibility_capital_scenarios,
        default=(5.0, 7.5, 10.0, 15.0, 20.0, 25.0, 50.0, 100.0),
    )
    parser.add_argument("--minimum-quote-volume", type=float, default=10_000_000.0)
    parser.add_argument("--maximum-book-spread-pct", type=float, default=0.001)
    parser.add_argument("--recommended-symbol-limit", type=int, default=25)
    parser.add_argument("--output-dir", default="feasibility_scan_v10")
    return parser.parse_args(list(arguments))


def feasibility_scan_main(arguments: Sequence[str]) -> None:
    args = parse_feasibility_scan_args(arguments)
    config = load_config()
    params = FeasibilityScanParameters(
        balance_usdt=args.balance,
        max_net_loss_usdt=args.max_net_loss,
        max_risk_pct=args.max_risk_pct,
        primary_stop_loss_pct=args.primary_stop_pct,
        stop_loss_scenarios=args.stop_scenarios,
        capital_scenarios_usdt=args.capital_scenarios,
        minimum_quote_volume_usdt=args.minimum_quote_volume,
        maximum_book_spread_pct=args.maximum_book_spread_pct,
        recommended_symbol_limit=args.recommended_symbol_limit,
    )
    report = run_feasibility_scan(config, params, args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))


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
    figure = Figure(figsize=(8, 4), dpi=120, facecolor="#ffffff")
    axis = figure.subplots()
    axis.set_facecolor("#ffffff")
    if data:
        x_values = list(range(len(data)))
        y_values = [float(item["value"]) for item in data]
        labels = [str(item.get("time", "")) for item in data]
        line_color = "#087f6a" if plot_type == "zscore" else "#d7603f"
        axis.plot(x_values, y_values, color=line_color, linewidth=1.7)
        step = max(1, math.ceil(len(x_values) / 6))
        tick_indexes = x_values[::step]
        if x_values[-1] not in tick_indexes:
            tick_indexes.append(x_values[-1])
        axis.set_xticks(tick_indexes)
        axis.set_xticklabels([labels[index] for index in tick_indexes], rotation=30, ha="right", fontsize=8)
        if plot_type == "zscore":
            axis.axhline(2.30, color="#d7603f", linestyle="--", linewidth=0.9)
            axis.axhline(-2.30, color="#087f6a", linestyle="--", linewidth=0.9)
            axis.axhline(-2.60, color="#087f6a", linestyle=":", linewidth=0.9)
            axis.axhline(0.0, color="#89948e", linestyle=":", linewidth=0.8)
    else:
        axis.text(0.5, 0.5, "No data yet", ha="center", va="center", transform=axis.transAxes)
    axis.set_title(title, loc="left", fontsize=10, fontweight="bold", color="#26332d", pad=10)
    axis.set_xlabel("Time", fontsize=8, color="#68736d")
    axis.set_ylabel(ylabel, fontsize=8, color="#68736d")
    axis.grid(True, color="#dce1dc", alpha=0.65, linewidth=0.7)
    axis.tick_params(axis="both", colors="#68736d", labelsize=8, length=0)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.spines["left"].set_color("#bdc7bf")
    axis.spines["bottom"].set_color("#bdc7bf")
    figure.tight_layout()
    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", facecolor="#ffffff")
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
      --bg: #f1f3f0;
      --surface: #ffffff;
      --surface-soft: #f7f8f5;
      --line: #dce1dc;
      --line-strong: #bdc7bf;
      --ink: #18211d;
      --muted: #68736d;
      --muted-strong: #3e4a44;
      --nav: #19211e;
      --nav-soft: #26312c;
      --accent: #087f6a;
      --accent-soft: #e3f2ed;
      --signal: #d7603f;
      --signal-soft: #faebe6;
      --good: #087f5b;
      --good-soft: #e1f3eb;
      --warn: #986500;
      --warn-soft: #fff1c7;
      --bad: #bd3d30;
      --bad-soft: #fbe8e4;
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
      grid-template-columns: 220px minmax(0, 1fr);
      min-height: 100vh;
    }
    aside {
      position: sticky;
      top: 0;
      height: 100vh;
      background: var(--nav);
      color: #eef5fb;
      padding: 24px 18px;
      border-right: 1px solid rgba(255,255,255,0.06);
    }
    .brand-mark {
      display: grid;
      grid-template-columns: 36px 1fr;
      gap: 10px;
      align-items: center;
      margin-bottom: 28px;
    }
    .brand-square {
      width: 36px;
      height: 36px;
      border: 1px solid rgba(255,255,255,0.22);
      background: #25322d;
      display: grid;
      place-items: center;
      font-family: var(--mono);
      font-weight: 800;
      letter-spacing: 0.04em;
      border-radius: 8px;
    }
    aside h1 {
      font-size: 16px;
      line-height: 1.35;
      margin: 0;
      text-wrap: balance;
    }
    .side-caption {
      margin: 4px 0 0;
      color: #9eb0a7;
      font-size: 12px;
      letter-spacing: 0.02em;
    }
    nav { display: grid; gap: 4px; }
    aside a {
      display: block;
      color: #c8d3cd;
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
      left: 18px;
      right: 18px;
      bottom: 22px;
      padding-top: 14px;
      border-top: 1px solid rgba(255,255,255,0.12);
      color: #9eb0a7;
      font-size: 12px;
      line-height: 1.7;
    }
    main {
      min-width: 0;
      max-width: 100%;
      overflow-x: hidden;
      padding: 24px 30px 42px;
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

    /* Monitor v3: information-dense operational workspace. */
    main {
      display: flex;
      flex-direction: column;
    }
    .topbar { order: 1; }
    .safety-strip { order: 2; }
    #status { order: 3; }
    #plots { order: 4; }
    .workspace { order: 5; }
    .topbar {
      align-items: center;
      margin-bottom: 12px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--line);
    }
    .page-title {
      display: flex;
      align-items: baseline;
      gap: 14px;
      min-width: 0;
    }
    .page-title h2 {
      font-size: 22px;
      white-space: nowrap;
    }
    .page-title p {
      margin: 0;
      padding-left: 14px;
      border-left: 1px solid var(--line-strong);
    }
    .mode-badge,
    .status-pill {
      border-radius: 4px;
      min-height: 28px;
      padding: 4px 9px;
      font-family: var(--mono);
      letter-spacing: 0;
    }
    .safety-strip {
      background: transparent;
      border: 0;
      border-left: 3px solid var(--warn);
      border-radius: 0;
      padding: 8px 0 8px 13px;
      margin-bottom: 14px;
    }
    .safety-strip strong { font-size: 13px; }
    .safety-strip span { color: var(--muted); }
    .actions { flex-wrap: nowrap; }
    button {
      min-height: 34px;
      border-radius: 4px;
      padding: 7px 12px;
      background: var(--accent);
      border-color: #066856;
      font-size: 13px;
    }
    button.secondary {
      color: var(--muted-strong);
      border-color: var(--line-strong);
    }
    button:focus-visible,
    a:focus-visible {
      outline: 2px solid var(--signal);
      outline-offset: 2px;
    }
    section {
      background: transparent;
      border: 0;
      border-top: 1px solid var(--line);
      border-radius: 0;
      padding: 20px 0 0;
    }
    section + section { margin-top: 20px; }
    #status {
      border: 1px solid var(--line);
      background: var(--surface);
      padding: 0;
      margin-bottom: 18px;
    }
    #status .section-head {
      margin: 0;
      padding: 11px 14px;
      border-bottom: 1px solid var(--line);
      background: var(--surface-soft);
    }
    #status .section-head h3 {
      font-size: 13px;
      text-transform: uppercase;
      color: var(--muted-strong);
    }
    .kpi-strip {
      grid-template-columns: repeat(8, minmax(112px, 1fr));
      gap: 0;
      margin: 0;
      overflow-x: auto;
      scrollbar-width: none;
    }
    .kpi-strip::-webkit-scrollbar { display: none; }
    .kpi-strip .metric {
      min-width: 112px;
      border: 0;
      border-right: 1px solid var(--line);
      border-radius: 0;
      padding: 13px 14px 12px;
      background: var(--surface);
    }
    .kpi-strip .metric:last-child { border-right: 0; }
    .kpi-strip .metric strong {
      min-height: 24px;
      margin-top: 5px;
      font-size: 17px;
    }
    .kpi-strip .metric small {
      margin-top: 4px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    #plots {
      padding-top: 0;
      border-top: 0;
      margin-bottom: 22px;
    }
    #plots .section-head {
      align-items: center;
      margin-bottom: 10px;
    }
    #plots .section-head::after {
      content: "";
      height: 1px;
      flex: 1 1 80px;
      background: var(--line);
      order: 2;
    }
    #plots .section-note { order: 3; }
    h3 { font-size: 16px; }
    .plots { gap: 10px; }
    .plot-frame {
      border-radius: 4px;
      background: var(--surface);
      border-color: var(--line-strong);
      box-shadow: 0 1px 0 rgba(24, 33, 29, 0.04);
    }
    .plot-title {
      padding: 9px 11px;
      background: var(--surface-soft);
      color: var(--muted-strong);
    }
    .plots img {
      aspect-ratio: 2 / 1;
      min-height: 0;
      object-fit: contain;
    }
    .workspace {
      grid-template-columns: minmax(320px, 0.82fr) minmax(520px, 1.35fr);
      gap: 28px;
    }
    .metric-grid,
    .strategy-grid,
    .signal-summary {
      gap: 0;
      border: 1px solid var(--line);
      background: var(--surface);
    }
    .metric-grid .metric,
    .strategy-grid .metric,
    .signal-summary .metric {
      border: 0;
      border-right: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
      border-radius: 0;
      background: var(--surface);
      padding: 11px 12px;
    }
    .metric-grid .metric:nth-child(2n),
    .strategy-grid .metric:nth-child(4n),
    .signal-summary .metric:nth-child(4n) {
      border-right: 0;
    }
    .metric-grid .metric:nth-last-child(-n+2),
    .strategy-grid .metric:nth-last-child(-n+4),
    .signal-summary .metric:nth-last-child(-n+4) {
      border-bottom: 0;
    }
    .metric strong {
      min-height: 22px;
      margin-top: 4px;
      font-size: 16px;
    }
    .metric strong[data-tone="positive"] { color: var(--good); }
    .metric strong[data-tone="negative"] { color: var(--bad); }
    .metric strong[data-tone="warning"] { color: var(--warn); }
    .section-head {
      margin-bottom: 10px;
      align-items: center;
    }
    .section-note {
      font-family: var(--mono);
      font-size: 11px;
    }
    .table-wrap {
      border-radius: 4px;
      background: var(--surface);
      border-color: var(--line);
      scrollbar-width: thin;
    }
    .table-wrap + .table-wrap { margin-top: 10px; }
    th, td {
      padding: 8px 9px;
      line-height: 1.45;
    }
    th {
      background: var(--surface-soft);
      color: var(--muted-strong);
    }
    tbody tr:hover td { background: #f5faf7; }
    .table-wrap thead th {
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .side-note strong { color: #e7efeb; }
    .side-runtime {
      display: grid;
      gap: 5px;
      margin-top: 8px;
      font-family: var(--mono);
      color: #c4d0ca;
    }
    .side-runtime span {
      overflow-wrap: anywhere;
    }
    .refresh-state {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .refresh-state::before {
      content: "";
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--good);
      box-shadow: 0 0 0 3px var(--good-soft);
    }
    .refresh-state.offline::before {
      background: var(--bad);
      box-shadow: 0 0 0 3px var(--bad-soft);
    }
    .empty-row td {
      color: var(--muted);
      text-align: center;
      font-family: inherit;
      padding: 18px;
    }
    @media (max-width: 1280px) {
      .kpi-strip { grid-template-columns: repeat(4, minmax(130px, 1fr)); }
      .workspace { grid-template-columns: 1fr; gap: 0; }
      .plots { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 860px) {
      aside {
        position: sticky;
        top: 0;
        z-index: 10;
        padding: 10px 14px 0;
        border-right: 0;
        border-bottom: 1px solid rgba(255,255,255,0.12);
      }
      .brand-mark {
        margin-bottom: 8px;
        grid-template-columns: 30px minmax(0, 1fr);
      }
      .brand-square { width: 30px; height: 30px; font-size: 11px; }
      aside h1 { font-size: 14px; }
      .side-caption,
      .side-note { display: none; }
      nav {
        display: flex;
        gap: 0;
        overflow-x: auto;
        scrollbar-width: none;
      }
      nav::-webkit-scrollbar { display: none; }
      aside a {
        flex: 0 0 auto;
        padding: 8px 11px 10px;
        border-radius: 0;
        font-size: 12px;
      }
      main { padding: 16px 14px 32px; }
      .topbar {
        grid-template-columns: 1fr;
        align-items: start;
      }
      .page-title {
        display: block;
      }
      .page-title h2 { white-space: normal; }
      .page-title p {
        border-left: 0;
        padding-left: 0;
        margin-top: 4px;
      }
      .mode-stack { justify-content: flex-start; }
      .mode-badge {
        width: auto;
        white-space: nowrap;
      }
      .safety-strip {
        grid-template-columns: minmax(0, 1fr) auto;
      }
      .plots { grid-template-columns: 1fr; }
      .kpi-strip { grid-template-columns: repeat(4, minmax(132px, 1fr)); }
    }
    @media (max-width: 560px) {
      .topbar { padding-bottom: 12px; }
      .page-title h2 { font-size: 20px; }
      .page-title p { font-size: 12px; }
      .mode-stack {
        display: flex;
        overflow: visible;
        flex-wrap: wrap;
        justify-content: flex-start;
        gap: 5px;
        padding-bottom: 0;
      }
      .mode-badge {
        width: auto;
        flex: 0 0 auto;
        padding-inline: 6px;
        font-size: 10px;
      }
      .safety-strip {
        grid-template-columns: 1fr;
        gap: 8px;
      }
      .safety-strip strong,
      .safety-strip span { word-break: normal; }
      .actions { justify-content: flex-start; }
      #status { margin-inline: -14px; border-inline: 0; }
      #status .section-head { padding-inline: 14px; }
      .kpi-strip {
        display: flex;
        scroll-snap-type: x proximity;
      }
      .kpi-strip .metric {
        flex: 0 0 148px;
        scroll-snap-align: start;
      }
      .metric-grid,
      .strategy-grid,
      .signal-summary {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .strategy-grid .metric:nth-child(4n),
      .signal-summary .metric:nth-child(4n) { border-right: 1px solid var(--line); }
      .strategy-grid .metric:nth-child(2n),
      .signal-summary .metric:nth-child(2n) { border-right: 0; }
      .strategy-grid .metric:nth-last-child(-n+4),
      .signal-summary .metric:nth-last-child(-n+4) { border-bottom: 1px solid var(--line); }
      .strategy-grid .metric:nth-last-child(-n+2),
      .signal-summary .metric:nth-last-child(-n+2) { border-bottom: 0; }
      .signal-summary .metric strong[data-testid="rejection-reason"] {
        max-height: 48px;
      }
      .plots img { aspect-ratio: 1.7 / 1; }
      .section-head { align-items: flex-start; }
      th, td { white-space: nowrap; }
      #latest-signal-body th,
      #latest-signal-body td { white-space: normal; min-width: 116px; }
    }
  </style>
</head>
<body data-ui-version="monitor-v3">
  <div class="layout">
    <aside>
      <div class="brand-mark">
        <div class="brand-square">ZS</div>
        <div>
          <h1>量化策略监控</h1>
          <p class="side-caption">SOLUSDT / SHADOW</p>
        </div>
      </div>
      <nav>
        <a href="#status">总览</a>
        <a href="#plots">图表</a>
        <a href="#signal">信号</a>
        <a href="#strategy">策略</a>
        <a href="#market-gate">市场门控</a>
        <a href="#trades">交易记录</a>
      </nav>
      <div class="side-note">
        <strong>安全边界</strong><br>
        仅监控与影子验证，不执行真实订单。
        <div class="side-runtime">
          <span class="refresh-state" data-testid="refresh-state">实时连接</span>
          <span>更新 <span data-testid="last-update">{{ snapshot.last_update }}</span></span>
          <span>启动 <span data-testid="started-at">{{ snapshot.started_at }}</span></span>
        </div>
      </div>
    </aside>
    <main>
      <div class="topbar">
        <div class="page-title">
          <h2><span data-testid="header-symbol">{{ snapshot.config.symbol }}</span> 策略监控</h2>
          <p>已收盘 1 分钟 K 线 · 4 秒局部刷新 · UTC 时间</p>
        </div>
        <div class="mode-stack">
          <span class="mode-badge safe">SHADOW MODE</span>
          <span class="mode-badge safe">DRY_RUN = <span data-testid="dry-run">{{ snapshot.config.dry_run }}</span></span>
          <span class="mode-badge locked">LIVE_TRADING = <span data-testid="live-trading">{{ snapshot.config.live_trading }}</span></span>
        </div>
      </div>

      <div class="safety-strip" data-testid="safety-strip">
        <div>
          <strong>影子验证模式：真实下单通道已锁定</strong>
          <span>这里只记录信号与虚拟成交，不会改变 Binance 账户状态。</span>
        </div>
        <div class="actions">
          <form class="command-form" method="post" action="/pause"><button class="secondary" type="submit" title="暂停影子策略循环">Ⅱ&nbsp; 暂停</button></form>
          <form class="command-form" method="post" action="/resume"><button type="submit" title="继续影子策略循环">▶&nbsp; 继续</button></form>
        </div>
      </div>

      <section id="status">
        <div class="section-head">
          <h3>运行仪表</h3>
          <span class="section-note"><span class="refresh-state">LIVE</span> / 4 SEC</span>
        </div>
        <div class="kpi-strip">
          <div class="metric compact"><span>运行状态</span><strong><span class="status-pill {{ snapshot.status }}" data-testid="status">{{ snapshot.status }}</span></strong><small>暂停后仍可查看已有数据</small></div>
          <div class="metric compact"><span>交易对</span><strong data-testid="symbol">{{ snapshot.config.symbol }}</strong><small>当前监控标的</small></div>
          <div class="metric compact"><span>USDT 余额</span><strong data-testid="balance" data-format="money">{{ "%.6f"|format(snapshot.balance) if snapshot.balance is number else snapshot.balance }}</strong><small>只读余额展示</small></div>
          <div class="metric compact"><span>最新 Z-score</span><strong data-testid="latest-zscore" data-format="score">{{ "%.8f"|format(snapshot.latest_zscore) if snapshot.latest_zscore is number else snapshot.latest_zscore }}</strong><small>基于已收盘 1m K 线</small></div>
          <div class="metric compact"><span>虚拟持仓</span><strong data-testid="elite-open-position">{{ snapshot.shadow.open_position_count }}</strong><small>router shadow position</small></div>
          <div class="metric compact"><span>执行控制器</span><strong data-testid="active-execution-strategy" title="{{ snapshot.shadow.active_execution_strategy }}">{{ snapshot.shadow.active_execution_strategy }}</strong><small>唯一成交入口</small></div>
          <div class="metric compact"><span>市场状态</span><strong data-testid="current-regime">{{ snapshot.shadow.current_regime }}</strong><small>regime classifier</small></div>
          <div class="metric compact"><span>路由策略</span><strong data-testid="router-active-strategy" title="{{ snapshot.shadow.router_active_strategy }}">{{ snapshot.shadow.router_active_strategy }}</strong><small>每根 K 线最多一个</small></div>
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
              <div class="metric"><span>是否暂停</span><strong data-testid="paused" data-format="bool">{{ snapshot.paused }}</strong></div>
              <div class="metric"><span>影子模式</span><strong data-testid="shadow-mode" data-format="bool">{{ snapshot.config.shadow_mode }}</strong></div>
              <div class="metric"><span>杠杆</span><strong data-testid="leverage">{{ snapshot.leverage }}x</strong></div>
              <div class="metric"><span>保证金模式</span><strong data-testid="margin-type">{{ snapshot.margin_type }}</strong></div>
              <div class="metric"><span>最新收盘价</span><strong data-testid="latest-close" data-format="price">{{ "%.4f"|format(snapshot.latest_close) if snapshot.latest_close is number else snapshot.latest_close }}</strong></div>
              <div class="metric"><span>候选信号数</span><strong data-testid="candidate-signals">{{ snapshot.shadow.candidate_signals }}</strong></div>
              <div class="metric"><span>ATR 桶</span><strong data-testid="atr-bucket">{{ snapshot.atr_bucket }}</strong><small data-testid="atr-pct" data-format="percent">{{ "%.8f"|format(snapshot.atr_pct) if snapshot.atr_pct is number else snapshot.atr_pct }}</small></div>
              <div class="metric"><span>趋势桶</span><strong data-testid="trend-bucket">{{ snapshot.trend_bucket }}</strong><small data-testid="trend-slope" data-format="percent">{{ "%.8f"|format(snapshot.trend_slope_pct) if snapshot.trend_slope_pct is number else snapshot.trend_slope_pct }}</small></div>
            </div>
          </section>

          <section id="strategy">
            <div class="section-head">
              <h3>策略验证</h3>
              <span class="section-note">REGIME STRATEGY ROUTER</span>
            </div>
            <div class="strategy-grid">
              <div class="metric"><span>净收益</span><strong data-testid="elite-net-pnl" data-format="money" data-tone-source="signed">{{ snapshot.shadow.net_pnl_usdt }}</strong></div>
              <div class="metric"><span>胜率</span><strong data-testid="elite-win-rate" data-format="rate">{{ snapshot.shadow.win_rate }}</strong></div>
              <div class="metric"><span>单笔期望</span><strong data-testid="elite-expectancy" data-format="money" data-tone-source="signed">{{ snapshot.shadow.expectancy_usdt }}</strong></div>
              <div class="metric"><span>利润因子</span><strong data-testid="elite-profit-factor" data-format="ratio">{{ snapshot.shadow.profit_factor }}</strong></div>
              <div class="metric"><span>最大回撤</span><strong data-testid="elite-drawdown" data-format="money">{{ snapshot.shadow.max_drawdown_usdt }}</strong></div>
              <div class="metric"><span>完成交易</span><strong data-testid="elite-total-trades">{{ snapshot.shadow.completed_trades }}</strong></div>
              <div class="metric"><span>可行性</span><strong data-testid="elite-viability">{{ snapshot.shadow.viability_status }}</strong></div>
              <div class="metric"><span>最近错误</span><strong data-testid="last-error">{{ snapshot.last_error }}</strong></div>
              <div class="metric"><span>数据集有效</span><strong data-testid="dataset-valid" data-format="bool">{{ snapshot.shadow.dataset_valid }}</strong><small>污染记录 <span data-testid="contaminated-count">{{ snapshot.shadow.contaminated_trade_count }}</span></small></div>
              <div class="metric"><span>最佳组合</span><strong data-testid="best-regime-strategy-pair">{{ snapshot.shadow.best_regime_strategy_pair }}</strong></div>
              <div class="metric"><span>最差组合</span><strong data-testid="worst-regime-strategy-pair">{{ snapshot.shadow.worst_regime_strategy_pair }}</strong></div>
            </div>
          </section>

          <section id="market-gate">
            <div class="section-head">
              <h3>市场状态与时间窗口</h3>
              <span class="section-note">OBSERVATION / MARKET GATE</span>
            </div>
            <div class="strategy-grid">
              <div class="metric"><span>稳定状态</span><strong data-testid="current-stability-state">{{ snapshot.shadow.current_stability_state }}</strong></div>
              <div class="metric"><span>稳定评分</span><strong data-testid="current-regime-stability-score" data-format="score">{{ snapshot.shadow.current_regime_stability_score }}</strong></div>
              <div class="metric"><span>UTC 窗口</span><strong data-testid="current-time-window-id">{{ snapshot.shadow.current_time_window_id }}</strong></div>
              <div class="metric"><span>门控允许</span><strong data-testid="current-gate-allowed" data-format="bool">{{ snapshot.shadow.current_trade_allowed_by_market_gate }}</strong></div>
              <div class="metric"><span>门控生效</span><strong data-testid="market-gate-enforced" data-format="bool">{{ snapshot.shadow.market_gate_enforced }}</strong></div>
              <div class="metric"><span>门控原因</span><strong data-testid="current-market-gate-rejection">{{ snapshot.shadow.current_market_gate_rejection_reason }}</strong></div>
              <div class="metric"><span>窗口交易</span><strong data-testid="current-window-trades">{{ snapshot.shadow.latest_signal.get("window_trade_count", "") }}</strong></div>
              <div class="metric"><span>窗口期望</span><strong data-testid="current-window-expectancy" data-format="money">{{ snapshot.shadow.latest_signal.get("window_expectancy", "") }}</strong></div>
              <div class="metric"><span>窗口利润因子</span><strong data-testid="current-window-profit-factor" data-format="ratio">{{ snapshot.shadow.latest_signal.get("window_profit_factor", "") }}</strong></div>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr><th>可盈利窗口</th><th>交易</th><th>期望</th><th>利润因子</th><th>净收益</th><th>胜率</th></tr></thead>
                <tbody id="profitable-windows-body">
                {% for row in snapshot.shadow.profitable_windows %}
                  <tr><td>{{ row.time_window_id }}</td><td>{{ row.trades }}</td><td>{{ row.expectancy }}</td><td>{{ row.profit_factor }}</td><td>{{ row.net_pnl }}</td><td>{{ row.win_rate }}</td></tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr><th>较差窗口</th><th>交易</th><th>期望</th><th>利润因子</th><th>净收益</th><th>胜率</th></tr></thead>
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
    function formatNumber(value, digits) {
      if (!Number.isFinite(value)) return String(value);
      return value.toFixed(digits).replace(/0+$/, "").replace(/\\.$/, "");
    }
    function fmt(value, format) {
      if (value === null || value === undefined || value === "") return "—";
      if (format === "bool") {
        if (value === true || String(value).toLowerCase() === "true") return "是";
        if (value === false || String(value).toLowerCase() === "false") return "否";
      }
      if (typeof value === "number") {
        if (format === "money") return formatNumber(value, 6) + " USDT";
        if (format === "price") return formatNumber(value, 4);
        if (format === "score") return formatNumber(value, 3);
        if (format === "ratio") return formatNumber(value, 3);
        if (format === "percent") return formatNumber(value * 100, 3) + "%";
        if (format === "rate") return formatNumber(value * 100, 2) + "%";
        return formatNumber(value, 6);
      }
      return String(value);
    }
    function applyTone(node, value) {
      if (!node || node.dataset.toneSource !== "signed") return;
      const number = Number(value);
      node.dataset.tone = !Number.isFinite(number) || number === 0
        ? ""
        : (number > 0 ? "positive" : "negative");
    }
    function setText(selector, value) {
      const node = document.querySelector(selector);
      if (!node) return;
      node.textContent = fmt(value, node.dataset.format || "");
      applyTone(node, value);
    }
    function setStatusClass(value) {
      const node = document.querySelector('[data-testid="status"]');
      if (!node) return;
      node.classList.remove("running", "paused", "error");
      if (value) node.classList.add(String(value));
    }
    function escapeHtml(value) {
      return fmt(value, "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;"
      }[char]));
    }
    async function refreshDashboard() {
      const refreshState = document.querySelector('[data-testid="refresh-state"]');
      try {
      const response = await fetch("/api/status", {cache: "no-store"});
      if (!response.ok) throw new Error("Status request failed");
      const snapshot = await response.json();
      const shadow = snapshot.shadow || {};
      const config = snapshot.config || {};
      if (refreshState) {
        refreshState.textContent = "实时连接";
        refreshState.classList.remove("offline");
      }
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
      setText('[data-testid="header-symbol"]', config.symbol);
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
      } catch (error) {
        if (refreshState) {
          refreshState.textContent = "连接异常";
          refreshState.classList.add("offline");
        }
      }
    }
    async function submitCommand(form) {
      const button = form.querySelector("button");
      if (button) button.disabled = true;
      try {
        await fetch(form.action, {method: "POST", cache: "no-store"});
        await refreshDashboard();
      } finally {
        if (button) button.disabled = false;
      }
    }
    window.addEventListener("load", () => {
      document.querySelectorAll(".command-form").forEach((form) => {
        form.addEventListener("submit", (event) => {
          event.preventDefault();
          submitCommand(form).catch(() => {});
        });
      });
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
    LOGGER.info(
        "Forward shadow execution enabled=%s",
        config.forward_shadow_execution_enabled,
    )
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
        if len(sys.argv) > 1 and sys.argv[1] == "candidate-portfolio-replay":
            candidate_portfolio_replay_main(sys.argv[2:])
            return
        if len(sys.argv) > 1 and sys.argv[1] == "v10-replay":
            momentum_rotation_main(sys.argv[2:])
            return
        if len(sys.argv) > 1 and sys.argv[1] == "feasibility-scan":
            feasibility_scan_main(sys.argv[2:])
            return
        config = load_config()
        state = RuntimeState(state_file=config.bot_state_file)
        state.update_config(config)
        recompute_shadow_metrics(state.shadow, config)
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
