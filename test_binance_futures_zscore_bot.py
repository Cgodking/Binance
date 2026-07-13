import csv
import importlib
import json
import os
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


import binance_futures_zscore_bot as bot


def reload_bot():
    module = importlib.reload(bot)
    module.load_dotenv = lambda *args, **kwargs: None
    return module


class EnvTestCase(unittest.TestCase):
    def setUp(self):
        self.old_env = os.environ.copy()
        for key in list(os.environ):
            if key.startswith("BINANCE_") or key in {
                "DRY_RUN",
                "TESTNET",
                "LIVE_TRADING",
                "SHADOW_MODE",
                "SYMBOL",
                "INTERVAL",
                "LOOKBACK",
                "UPPER_Z",
                "LOWER_Z",
                "EXIT_Z",
                "STOP_LOSS_PCT",
                "TAKE_PROFIT_PCT",
                "LEVERAGE",
                "MAX_MARGIN_USDT",
                "MAX_NOTIONAL_USDT",
                "LOOP_SLEEP_SECONDS",
                "SHADOW_STATE_FILE",
                "SHADOW_SIGNAL_LOG_FILE",
                "SHADOW_TRADE_LOG_FILE",
                "EXCHANGE_FILTERS_CACHE_FILE",
                "EXCHANGE_FILTERS_CACHE_TTL_SECONDS",
                "DECISION_REPORT_EVERY_COMPLETED_TRADES",
                "MIN_ENTRY_ABS_Z",
                "MAX_ENTRY_ABS_Z",
                "MIN_Z_REVERSION_DELTA",
                "ELITE_ALLOWED_ATR_BUCKETS",
                "ELITE_ALLOWED_TREND_BUCKETS",
                "REGIME_FAST_EMA_PERIOD",
                "REGIME_SLOW_EMA_PERIOD",
                "REGIME_SLOPE_LOOKBACK",
                "REGIME_HIGH_ATR_PERCENTILE",
                "REGIME_LOW_ATR_PERCENTILE",
                "REGIME_MEAN_REVERSION_MAX_SLOPE_PCT",
                "REGIME_MEAN_REVERSION_MAX_DISTANCE_PCT",
                "REGIME_OSCILLATION_LOOKBACK",
                "REGIME_MIN_EMA_CROSSES",
                "BREAKOUT_LOOKBACK",
                "LOW_ATR_PERCENTILE",
                "TREND_SLOPE_THRESHOLD_PCT",
                "SHADOW_PROBE_ENABLED",
                "SHADOW_PROBE_MIN_ABS_Z",
                "SHADOW_PROBE_MAX_ABS_Z",
                "SHADOW_PROBE_ALLOW_SHORT",
                "SHADOW_PROBE_MAX_HOLDING_MINUTES",
                "MARKET_GATE_ENFORCED",
                "REGIME_STABILITY_WINDOW",
                "MIN_REGIME_PERSISTENCE_CANDLES",
                "MAX_REGIME_SWITCH_RATE",
                "MAX_ATR_COEFFICIENT_OF_VARIATION",
                "MIN_EMA_SLOPE_CONSISTENCY",
                "TIME_WINDOW_HOURS",
                "MIN_TRADES_PER_WINDOW",
                "MIN_PROFIT_WINDOW_PROFIT_FACTOR",
                "WEB_AUTH_ENABLED",
            }:
                os.environ.pop(key, None)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.old_env)
        reload_bot()


class EliteStrategyConfigTests(EnvTestCase):
    def test_load_config_enforces_shadow_only_safety(self):
        module = reload_bot()
        config = module.load_config()

        self.assertTrue(config.dry_run)
        self.assertTrue(config.shadow_mode)
        self.assertFalse(config.live_trading)
        self.assertEqual(
            config.active_strategies,
            (
                "MEAN_REVERSION_STRATEGY",
                "TREND_FOLLOW_LONG_STRATEGY",
                "TREND_FOLLOW_SHORT_STRATEGY",
                "BASELINE_LOGGING_ONLY",
            ),
        )
        self.assertTrue(config.shadow_probe_enabled)
        self.assertEqual(config.shadow_probe_min_abs_z, 0.40)
        self.assertEqual(config.shadow_probe_max_abs_z, 4.00)
        self.assertTrue(config.shadow_probe_allow_short)
        self.assertEqual(config.min_entry_abs_z, 2.05)
        self.assertEqual(config.max_entry_abs_z, 2.60)
        self.assertEqual(config.exit_z, 0.10)
        self.assertEqual(config.elite_allowed_atr_buckets, ("medium",))
        self.assertEqual(config.elite_allowed_trend_buckets, ("medium",))
        self.assertEqual(config.decision_report_every_completed_trades, 50)
        self.assertEqual(config.regime_fast_ema_period, 20)
        self.assertEqual(config.regime_slow_ema_period, 50)
        self.assertEqual(config.breakout_lookback, 20)
        self.assertEqual(config.exchange_filters_cache_file, "exchange_filters_cache.json")
        self.assertFalse(config.market_gate_enforced)
        self.assertEqual(config.regime_stability_window, 20)
        self.assertEqual(config.min_regime_persistence_candles, 10)
        self.assertEqual(config.time_window_hours, 2)
        self.assertEqual(config.min_trades_per_window, 20)
        self.assertEqual(config.min_profit_window_profit_factor, 1.05)

        os.environ["LIVE_TRADING"] = "true"
        with self.assertRaises(module.ConfigError):
            module.load_config()

        os.environ["LIVE_TRADING"] = "false"
        os.environ["DRY_RUN"] = "false"
        with self.assertRaises(module.ConfigError):
            module.load_config()

        os.environ["DRY_RUN"] = "true"
        os.environ["SHADOW_MODE"] = "false"
        with self.assertRaises(module.ConfigError):
            module.load_config()

    def test_dashboard_keeps_chinese_monitor_layout_and_safe_flask_settings(self):
        text = Path("binance_futures_zscore_bot.py").read_text(encoding="utf-8")

        self.assertIn("币安U本位合约监控", text)
        self.assertIn("交易策略影子监控台", text)
        self.assertIn("安全状态", text)
        self.assertIn("策略验证", text)
        self.assertIn("信号筛选", text)
        self.assertIn("实时图表", text)
        self.assertIn("kpi-strip", text)
        self.assertIn("mode-badge", text)
        self.assertIn("data-testid=\"candidate-signals\"", text)
        self.assertIn("SHADOW MODE", text)
        self.assertNotIn("Elite Shadow Strategy Monitor", text)
        self.assertNotIn("seaborn", text.lower())
        self.assertIn("debug=False", text)
        self.assertIn("use_reloader=False", text)


class EliteSignalDecisionTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.module = reload_bot()
        self.config = self.module.load_config()

    def test_elite_strategy_accepts_only_confirmed_long_signal(self):
        decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.20,
            previous_close=68.00,
            zscore=-2.40,
            previous_zscore=-2.62,
            atr_pct=0.0005,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )

        self.assertEqual(decision.trade_decision, "ENTER")
        self.assertEqual(decision.direction, "LONG")
        self.assertEqual(decision.rejection_reason, "")
        self.assertEqual(decision.failed_filter, "")
        self.assertTrue(decision.reversion_confirmed)

    def test_elite_strategy_accepts_configured_medium_atr_and_medium_trend(self):
        decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.20,
            previous_close=68.00,
            zscore=-2.12,
            previous_zscore=-2.34,
            atr_pct=0.0010,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.001,
        )

        self.assertEqual(decision.trade_decision, "ENTER")
        self.assertEqual(decision.direction, "LONG")
        self.assertEqual(decision.rejection_reason, "")
        self.assertEqual(decision.failed_filter, "")
        self.assertTrue(decision.reversion_confirmed)

    def test_elite_strategy_rejects_short_signals(self):
        decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.00,
            previous_close=68.20,
            zscore=2.40,
            previous_zscore=2.62,
            atr_pct=0.0005,
            atr_bucket="low",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )

        self.assertEqual(decision.trade_decision, "REJECT")
        self.assertEqual(decision.failed_filter, "direction")
        self.assertIn("short", decision.rejection_reason.lower())

    def test_elite_strategy_rejects_failed_filters_with_reason(self):
        cases = [
            {"zscore": -2.75, "previous_zscore": -2.95, "atr_bucket": "medium", "trend_bucket": "medium", "close_price": 68.2, "previous_close": 68.0, "failed": "zscore"},
            {"zscore": -2.40, "previous_zscore": -2.62, "atr_bucket": "low", "trend_bucket": "medium", "close_price": 68.2, "previous_close": 68.0, "failed": "atr"},
            {"zscore": -2.40, "previous_zscore": -2.62, "atr_bucket": "high", "trend_bucket": "medium", "close_price": 68.2, "previous_close": 68.0, "failed": "atr"},
            {"zscore": -2.40, "previous_zscore": -2.62, "atr_bucket": "medium", "trend_bucket": "weak", "close_price": 68.2, "previous_close": 68.0, "failed": "trend"},
            {"zscore": -2.40, "previous_zscore": -2.62, "atr_bucket": "medium", "trend_bucket": "strong", "close_price": 68.2, "previous_close": 68.0, "failed": "trend"},
            {"zscore": -2.40, "previous_zscore": -2.46, "atr_bucket": "medium", "trend_bucket": "medium", "close_price": 67.9, "previous_close": 68.0, "failed": "reversion"},
        ]

        for case in cases:
            with self.subTest(case=case["failed"]):
                decision = self.module.evaluate_elite_signal(
                    self.config,
                    timestamp="2026-06-14 00:00 UTC",
                    close_price=case["close_price"],
                    previous_close=case["previous_close"],
                    zscore=case["zscore"],
                    previous_zscore=case["previous_zscore"],
                    atr_pct=0.0005,
                    atr_bucket=case["atr_bucket"],
                    trend_bucket=case["trend_bucket"],
                    trend_slope_pct=0.003,
                )
                self.assertEqual(decision.trade_decision, "REJECT")
                self.assertEqual(decision.failed_filter, case["failed"])
                self.assertTrue(decision.rejection_reason)


class RegimeRouterTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.module = reload_bot()
        self.config = self.module.load_config()

    def _frame(self, closes):
        rows = []
        for idx, close in enumerate(closes):
            rows.append(
                {
                    "open_time": idx * 60000,
                    "open": close,
                    "high": close * 1.001,
                    "low": close * 0.999,
                    "close": close,
                    "volume": 100 + idx,
                    "close_time": idx * 60000 + 59999,
                }
            )
        return self.module.pd.DataFrame(rows)

    def test_regime_detector_prioritizes_trend_up_before_high_volatility(self):
        closes = [50 + i * 0.08 for i in range(90)]
        klines = self._frame(closes)

        regime = self.module.detect_market_regime(klines, self.config)

        self.assertEqual(regime.regime, "TREND_UP")
        self.assertEqual(regime.router_strategy, "TREND_FOLLOW_LONG_STRATEGY")
        self.assertGreater(regime.ema20, regime.ema50)
        self.assertGreater(regime.ema_slope_pct, 0)

    def test_router_routes_mean_reversion_to_one_shadow_strategy(self):
        regime = self.module.MarketRegimeSnapshot(
            timestamp="2026-06-14 00:00 UTC",
            regime="MEAN_REVERTING",
            router_strategy="MEAN_REVERSION_STRATEGY",
            no_trade_reason="",
            ema20=68.0,
            ema50=68.02,
            ema_slope_pct=0.0001,
            atr_pct=0.0006,
            atr_percentile=0.20,
            atr_bucket="low",
            price_distance_ema_pct=-0.002,
            price_distance_vwap_pct=-0.002,
            ema_state="EMA20_BELOW_EMA50",
        )

        decision = self.module.route_strategy(
            self.config,
            regime,
            timestamp="2026-06-14 00:00 UTC",
            close_price=67.80,
            previous_close=67.70,
            zscore=-2.40,
            previous_zscore=-2.62,
            recent_high=68.20,
            recent_low=67.50,
        )

        self.assertEqual(decision.trade_decision, "ENTER")
        self.assertEqual(decision.strategy, "MEAN_REVERSION_STRATEGY")
        self.assertEqual(decision.direction, "LONG")
        self.assertEqual(decision.regime, "MEAN_REVERTING")
        self.assertTrue(decision.reversion_confirmed)

    def test_router_blocks_no_trade_regime_with_logged_reason(self):
        regime = self.module.MarketRegimeSnapshot(
            timestamp="2026-06-14 00:00 UTC",
            regime="NO_TRADE",
            router_strategy="NO_TRADE",
            no_trade_reason="conflicting regime signals",
            ema20=68.0,
            ema50=68.0,
            ema_slope_pct=0.0,
            atr_pct=0.001,
            atr_percentile=0.50,
            atr_bucket="medium",
            price_distance_ema_pct=0.0,
            price_distance_vwap_pct=0.0,
            ema_state="EMA20_EQUALS_EMA50",
        )

        decision = self.module.route_strategy(
            self.config,
            regime,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.0,
            previous_close=67.9,
            zscore=2.40,
            previous_zscore=2.62,
            recent_high=68.2,
            recent_low=67.5,
        )

        self.assertEqual(decision.trade_decision, "REJECT")
        self.assertEqual(decision.strategy, "NO_TRADE")
        self.assertEqual(decision.failed_filter, "regime")

    def test_regime_stability_marks_persistent_uptrend_as_stable_but_not_tradable_without_window_data(self):
        closes = [50 + i * 0.12 for i in range(140)]
        klines = self._frame(closes)
        regime = self.module.detect_market_regime(klines, self.config)

        gate = self.module.evaluate_market_gate(
            klines,
            self.config,
            regime,
            "2026-07-02 10:34 UTC",
            {"trades": []},
        )

        self.assertEqual(gate["stability_state"], "STABLE_TREND")
        self.assertGreaterEqual(gate["regime_stability_score"], 0.70)
        self.assertEqual(gate["time_window_id"], "10:00-12:00 UTC")
        self.assertFalse(gate["window_is_profitable"])
        self.assertFalse(gate["trade_allowed_by_market_gate"])
        self.assertEqual(gate["market_gate_rejection_reason"], "UNPROFITABLE_TIME_WINDOW")

    def test_time_window_analytics_identifies_profitable_and_worst_windows(self):
        trades = []
        for index in range(20):
            trades.append(
                {
                    "entry_time": f"2026-07-02 10:{index:02d} UTC",
                    "regime_at_entry": "TREND_UP",
                    "net_pnl": 0.02,
                    "gross_pnl": 0.03,
                    "fees": 0.004,
                    "slippage": 0.001,
                    "result": "WIN",
                }
            )
            trades.append(
                {
                    "entry_time": f"2026-07-02 12:{index:02d} UTC",
                    "regime_at_entry": "MEAN_REVERTING",
                    "net_pnl": -0.02,
                    "gross_pnl": -0.01,
                    "fees": 0.004,
                    "slippage": 0.001,
                    "result": "LOSS",
                }
            )

        analytics = self.module.build_time_window_analytics(trades, self.config)

        self.assertEqual(analytics["windows"]["10:00-12:00 UTC"]["trades"], 20)
        self.assertTrue(analytics["windows"]["10:00-12:00 UTC"]["is_profitable"])
        self.assertEqual(analytics["profitable_windows"][0]["time_window_id"], "10:00-12:00 UTC")
        self.assertEqual(analytics["worst_windows"][0]["time_window_id"], "12:00-14:00 UTC")


class ExchangeFilterCacheTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        os.environ["EXCHANGE_FILTERS_CACHE_FILE"] = str(Path(self.temp_dir.name) / "filters.json")
        self.module = reload_bot()
        self.config = self.module.load_config()

    def _exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "SOLUSDT",
                    "quantityPrecision": 2,
                    "pricePrecision": 4,
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01", "maxQty": "7000"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    def test_get_exchange_filters_writes_cache_after_successful_api_fetch(self):
        class FakeClient:
            def futures_exchange_info(inner_self):
                return self._exchange_info()

        filters = self.module.get_exchange_filters(FakeClient(), "SOLUSDT", self.config)
        cache_path = Path(self.config.exchange_filters_cache_file)

        self.assertTrue(cache_path.exists())
        self.assertEqual(filters.min_notional, 5.0)
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        self.assertIn("SOLUSDT", payload)
        self.assertEqual(payload["SOLUSDT"]["step_size"], 0.01)

    def test_get_exchange_filters_falls_back_to_cache_when_api_fails(self):
        cache_path = Path(self.config.exchange_filters_cache_file)
        cache_path.write_text(
            json.dumps(
                {
                    "SOLUSDT": {
                        "step_size": 0.01,
                        "tick_size": 0.01,
                        "min_qty": 0.01,
                        "max_qty": 7000.0,
                        "min_notional": 5.0,
                        "quantity_precision": 2,
                        "price_precision": 4,
                    }
                }
            ),
            encoding="utf-8",
        )

        class BrokenClient:
            def futures_exchange_info(inner_self):
                raise RuntimeError("network down")

        filters = self.module.get_exchange_filters(BrokenClient(), "SOLUSDT", self.config)

        self.assertEqual(filters.step_size, 0.01)
        self.assertEqual(filters.min_notional, 5.0)
        self.assertEqual(filters.quantity_precision, 2)

    def test_load_config_reads_positive_exchange_filter_cache_ttl(self):
        os.environ["EXCHANGE_FILTERS_CACHE_TTL_SECONDS"] = "120"
        module = reload_bot()

        config = module.load_config()

        self.assertEqual(config.exchange_filters_cache_ttl_seconds, 120.0)

    def test_load_config_rejects_nonpositive_exchange_filter_cache_ttl(self):
        os.environ["EXCHANGE_FILTERS_CACHE_TTL_SECONDS"] = "0"
        module = reload_bot()

        with self.assertRaises(module.ConfigError):
            module.load_config()

    def test_get_exchange_filters_uses_fresh_cache_without_api_call(self):
        cache_path = Path(self.config.exchange_filters_cache_file)
        cache_path.write_text(
            json.dumps(
                {
                    "SOLUSDT": {
                        "step_size": 0.01,
                        "tick_size": 0.01,
                        "min_qty": 0.01,
                        "max_qty": 7000.0,
                        "min_notional": 5.0,
                        "quantity_precision": 2,
                        "price_precision": 4,
                    }
                }
            ),
            encoding="utf-8",
        )
        calls = 0

        class CountingClient:
            def futures_exchange_info(inner_self):
                nonlocal calls
                calls += 1
                return self._exchange_info()

        filters = self.module.get_exchange_filters(CountingClient(), "SOLUSDT", self.config)

        self.assertEqual(calls, 0)
        self.assertEqual(filters.min_notional, 5.0)

    def test_get_exchange_filters_refreshes_stale_cache(self):
        cache_path = Path(self.config.exchange_filters_cache_file)
        cache_path.write_text(
            json.dumps(
                {
                    "SOLUSDT": {
                        "step_size": 0.01,
                        "tick_size": 0.01,
                        "min_qty": 0.01,
                        "max_qty": 7000.0,
                        "min_notional": 4.0,
                        "quantity_precision": 2,
                        "price_precision": 4,
                    }
                }
            ),
            encoding="utf-8",
        )
        os.utime(cache_path, (time.time() - 3600, time.time() - 3600))
        calls = 0

        class CountingClient:
            def futures_exchange_info(inner_self):
                nonlocal calls
                calls += 1
                return self._exchange_info()

        filters = self.module.get_exchange_filters(CountingClient(), "SOLUSDT", self.config)

        self.assertEqual(calls, 1)
        self.assertEqual(filters.min_notional, 5.0)


class ClosedCandleSchedulingTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.module = reload_bot()
        self.config = self.module.load_config()

    def test_successful_one_minute_cycle_waits_until_next_closed_candle(self):
        delay = self.module.next_bot_loop_delay(
            self.config,
            cycle_failed=False,
            paused=False,
            now_epoch=125.0,
        )

        self.assertAlmostEqual(delay, 57.0)

    def test_failed_cycle_uses_configured_retry_delay(self):
        delay = self.module.next_bot_loop_delay(
            self.config,
            cycle_failed=True,
            paused=False,
            now_epoch=125.0,
        )

        self.assertEqual(delay, self.config.loop_sleep_seconds)

    def test_paused_cycle_uses_configured_check_delay(self):
        delay = self.module.next_bot_loop_delay(
            self.config,
            cycle_failed=False,
            paused=True,
            now_epoch=125.0,
        )

        self.assertEqual(delay, self.config.loop_sleep_seconds)


class EliteShadowRuntimeTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        os.environ["BOT_STATE_FILE"] = str(Path(self.temp_dir.name) / "bot_state.json")
        os.environ["SHADOW_STATE_FILE"] = str(Path(self.temp_dir.name) / "shadow_state.json")
        os.environ["SHADOW_SIGNAL_LOG_FILE"] = str(Path(self.temp_dir.name) / "shadow_signals.jsonl")
        os.environ["SHADOW_TRADE_LOG_FILE"] = str(Path(self.temp_dir.name) / "shadow_trades.jsonl")
        os.environ["DECISION_REPORT_EVERY_COMPLETED_TRADES"] = "1"
        self.module = reload_bot()
        self.config = self.module.load_config()
        self.filters = self.module.ExchangeFilters(
            step_size=0.01,
            tick_size=0.01,
            min_qty=0.01,
            max_qty=7000.0,
            min_notional=5.0,
            quantity_precision=2,
            price_precision=4,
        )

    def test_signal_logging_writes_jsonl_and_csv_for_every_evaluation(self):
        state = self.module.RuntimeState()
        decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.2,
            previous_close=68.0,
            zscore=-2.4,
            previous_zscore=-2.62,
            atr_pct=0.0005,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )

        state.process_elite_signal(self.config, decision, self.filters)

        jsonl_path = Path(self.temp_dir.name) / "shadow_signals.jsonl"
        csv_path = Path(self.temp_dir.name) / "shadow_signals.csv"
        state_path = Path(self.temp_dir.name) / "shadow_state.json"
        self.assertTrue(jsonl_path.exists())
        self.assertTrue(csv_path.exists())
        self.assertTrue(state_path.exists())

        row = json.loads(jsonl_path.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(row["trade_decision"], "ENTER")
        self.assertEqual(row["strategy"], "MEAN_REVERSION_STRATEGY")
        self.assertEqual(row["regime"], "MEAN_REVERTING")
        self.assertIn("reversion_status", row)

        with csv_path.open(newline="", encoding="utf-8") as handle:
            csv_row = next(csv.DictReader(handle))
        self.assertEqual(csv_row["trade_decision"], "ENTER")
        self.assertEqual(csv_row["failed_filter"], "")

    def test_market_gate_annotations_are_logged_without_blocking_when_not_enforced(self):
        state = self.module.RuntimeState()
        regime = self.module.MarketRegimeSnapshot(
            timestamp="2026-07-02 10:34 UTC",
            regime="MEAN_REVERTING",
            router_strategy="MEAN_REVERSION_STRATEGY",
            no_trade_reason="",
            ema20=68.0,
            ema50=68.02,
            ema_slope_pct=0.0001,
            atr_pct=0.0006,
            atr_percentile=0.20,
            atr_bucket="low",
            price_distance_ema_pct=-0.002,
            price_distance_vwap_pct=-0.002,
            ema_state="EMA20_BELOW_EMA50",
        )
        decision = self.module.route_strategy(
            self.config,
            regime,
            timestamp="2026-07-02 10:34 UTC",
            close_price=67.80,
            previous_close=67.70,
            zscore=-2.40,
            previous_zscore=-2.62,
            recent_high=68.20,
            recent_low=67.50,
        )
        gate = {
            "regime_stability_score": 0.20,
            "stability_state": "UNSTABLE",
            "time_window_id": "10:00-12:00 UTC",
            "window_is_profitable": False,
            "trade_allowed_by_market_gate": False,
            "market_gate_enforced": False,
            "market_gate_rejection_reason": "UNSTABLE_REGIME",
        }

        state.process_router_signal(self.config, self.module.attach_market_gate_to_decision(decision, gate), self.filters)
        snapshot = state.snapshot()

        self.assertEqual(snapshot["shadow"]["open_position_count"], 1)
        self.assertEqual(snapshot["shadow"]["latest_signal"]["trade_decision"], "ENTER")
        self.assertEqual(snapshot["shadow"]["latest_signal"]["stability_state"], "UNSTABLE")
        self.assertFalse(snapshot["shadow"]["latest_signal"]["trade_allowed_by_market_gate"])

    def test_duplicate_closed_candle_signal_is_not_counted_or_logged_twice(self):
        state = self.module.RuntimeState()
        decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.2,
            previous_close=68.0,
            zscore=-1.4,
            previous_zscore=-1.6,
            atr_pct=0.0005,
            atr_bucket="low",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )

        state.process_elite_signal(self.config, decision, self.filters)
        state.process_elite_signal(self.config, decision, self.filters)

        snapshot = state.snapshot()
        jsonl_path = Path(self.temp_dir.name) / "shadow_signals.jsonl"
        csv_path = Path(self.temp_dir.name) / "shadow_signals.csv"

        self.assertEqual(snapshot["shadow"]["candidate_signals"], 1)
        self.assertEqual(snapshot["shadow"]["last_signal_timestamp"], "2026-06-14 00:00 UTC")
        self.assertEqual(len(jsonl_path.read_text(encoding="utf-8").splitlines()), 1)
        with csv_path.open(newline="", encoding="utf-8") as handle:
            self.assertEqual(len(list(csv.DictReader(handle))), 1)

    def test_successful_run_once_clears_previous_error(self):
        state = self.module.RuntimeState()
        state.set_error("old transient network error")
        z_result = self.module.ZScoreResult(
            latest_close=68.2,
            previous_close=68.0,
            mean=68.1,
            std=0.1,
            zscore=-1.0,
            previous_zscore=-1.2,
            latest_closed_time="2026-06-14 00:00 UTC",
        )

        originals = {
            "get_exchange_filters": self.module.get_exchange_filters,
            "fetch_klines": self.module.fetch_klines,
            "compute_zscore": self.module.compute_zscore,
            "compute_atr_bucket": self.module.compute_atr_bucket,
            "compute_trend_bucket": self.module.compute_trend_bucket,
            "detect_market_regime": self.module.detect_market_regime,
            "recent_breakout_bounds": self.module.recent_breakout_bounds,
            "fetch_balance": self.module.fetch_balance,
        }
        try:
            self.module.get_exchange_filters = lambda *_args, **_kwargs: self.filters
            self.module.fetch_klines = lambda *_args, **_kwargs: object()
            self.module.compute_zscore = lambda *_args, **_kwargs: z_result
            self.module.compute_atr_bucket = lambda *_args, **_kwargs: (0.0005, "low")
            self.module.compute_trend_bucket = lambda *_args, **_kwargs: (0.003, "medium")
            self.module.detect_market_regime = lambda *_args, **_kwargs: self.module.MarketRegimeSnapshot(
                timestamp="2026-06-14 00:00 UTC",
                regime="NO_TRADE",
                router_strategy="NO_TRADE",
                no_trade_reason="test no trade",
                ema20=68.0,
                ema50=68.0,
                ema_slope_pct=0.0,
                atr_pct=0.0005,
                atr_percentile=0.5,
                atr_bucket="medium",
                price_distance_ema_pct=0.0,
                price_distance_vwap_pct=0.0,
                ema_state="EMA20_EQUALS_EMA50",
                trend_bucket="weak",
            )
            self.module.recent_breakout_bounds = lambda *_args, **_kwargs: (68.5, 67.5)
            self.module.fetch_balance = lambda *_args, **_kwargs: 1.58

            self.module.run_once(object(), self.config, state)
        finally:
            for name, func in originals.items():
                setattr(self.module, name, func)

        snapshot = state.snapshot()
        self.assertEqual(snapshot["status"], "running")
        self.assertEqual(snapshot["last_error"], "")

    def test_single_virtual_position_and_no_short_positions(self):
        os.environ["SHADOW_PROBE_ENABLED"] = "false"
        self.module = reload_bot()
        self.config = self.module.load_config()
        state = self.module.RuntimeState()
        short_decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.0,
            previous_close=68.2,
            zscore=2.4,
            previous_zscore=2.62,
            atr_pct=0.0005,
            atr_bucket="low",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )
        state.process_elite_signal(self.config, short_decision, self.filters)
        self.assertIsNone(state.snapshot()["shadow"]["elite_long_position"])
        self.assertIsNone(state.snapshot()["shadow"]["router_position"])

        long_decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:01 UTC",
            close_price=68.2,
            previous_close=68.0,
            zscore=-2.4,
            previous_zscore=-2.62,
            atr_pct=0.0005,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )
        state.process_elite_signal(self.config, long_decision, self.filters)
        snapshot = state.snapshot()

        self.assertEqual(snapshot["shadow"]["open_position_count"], 1)
        self.assertEqual(snapshot["shadow"]["router_position"]["side"], "LONG")
        self.assertEqual(snapshot["shadow"]["router_position"]["strategy"], "MEAN_REVERSION_STRATEGY")

        state.process_elite_signal(self.config, long_decision, self.filters)
        self.assertEqual(state.snapshot()["shadow"]["open_position_count"], 1)

    def test_shadow_probe_is_logging_only_and_never_executes_trades(self):
        state = self.module.RuntimeState()
        short_probe = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.00,
            previous_close=67.90,
            zscore=1.80,
            previous_zscore=1.60,
            atr_pct=0.0010,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )

        state.process_elite_signal(self.config, short_probe, self.filters)
        snapshot = state.snapshot()

        self.assertIsNone(snapshot["shadow"]["elite_long_position"])
        self.assertIsNone(snapshot["shadow"]["router_position"])
        self.assertIsNone(snapshot["shadow"]["shadow_probe_position"])
        self.assertEqual(snapshot["shadow"]["open_position_count"], 0)
        self.assertEqual(snapshot["positions"], [])
        self.assertEqual(snapshot["shadow"]["latest_signal"]["trade_decision"], "REJECT")
        self.assertEqual(snapshot["shadow"]["latest_signal"]["probe_trade_decision"], "LOG_ONLY")
        self.assertIn("logging-only", snapshot["shadow"]["latest_signal"]["probe_rejection_reason"])

        mean_reversion = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:05 UTC",
            close_price=67.70,
            previous_close=67.80,
            zscore=0.20,
            previous_zscore=0.50,
            atr_pct=0.0010,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )
        state.process_elite_signal(self.config, mean_reversion, self.filters)
        snapshot = state.snapshot()

        self.assertIsNone(snapshot["shadow"]["shadow_probe_position"])
        self.assertEqual(snapshot["shadow"]["completed_trades"], 0)
        self.assertEqual(snapshot["shadow"]["recent_trades"], [])

    def test_breakout_strategy_is_logging_only_and_never_opens_router_position(self):
        state = self.module.RuntimeState()
        decision = self.module.StrategySignalDecision(
            timestamp="2026-06-14 00:00 UTC",
            strategy="BREAKOUT_STRATEGY",
            regime="HIGH_VOLATILITY",
            router_strategy="BREAKOUT_STRATEGY",
            no_trade_reason="",
            close_price=68.50,
            previous_close=68.10,
            zscore=2.20,
            previous_zscore=1.20,
            atr_pct=0.0020,
            atr_percentile=0.90,
            atr_bucket="high",
            trend_bucket="weak",
            trend_slope_pct=0.001,
            ema20=68.0,
            ema50=67.8,
            ema_slope_pct=0.001,
            ema_state="EMA20_ABOVE_EMA50",
            price_distance_ema_pct=0.004,
            price_distance_vwap_pct=0.004,
            reversion_confirmed=False,
            direction="LONG",
            trade_decision="ENTER",
            entry_reason="high-volatility breakout above recent high",
            rejection_reason="",
            failed_filter="",
        )

        state.process_router_signal(self.config, decision, self.filters)
        snapshot = state.snapshot()

        self.assertIsNone(snapshot["shadow"]["router_position"])
        self.assertEqual(snapshot["shadow"]["open_position_count"], 0)
        self.assertEqual(snapshot["positions"], [])
        self.assertEqual(snapshot["shadow"]["completed_trades"], 0)
        self.assertEqual(snapshot["shadow"]["latest_signal"]["trade_decision"], "REJECT")
        self.assertEqual(snapshot["shadow"]["latest_signal"]["failed_filter"], "execution")
        self.assertIn("logging-only", snapshot["shadow"]["latest_signal"]["rejection_reason"])
        self.assertEqual(snapshot["shadow"]["execution_rejections"][0]["rejected_strategy"], "BREAKOUT_STRATEGY")
        self.assertEqual(snapshot["shadow"]["execution_rejections"][0]["reason"], "NON_ACTIVE_STRATEGY_BLOCKED")

    def test_existing_shadow_probe_position_is_cleared_without_trade_record(self):
        state = self.module.RuntimeState()
        state.shadow["shadow_probe_position"] = {
            "strategy": "SHADOW_PROBE_STRATEGY",
            "symbol": "SOLUSDT",
            "side": "SHORT",
            "entry_time": "2026-06-14 00:00 UTC",
            "entry_price": 68.0,
            "entry_zscore": 1.8,
            "entry_atr_pct": 0.001,
            "entry_atr_bucket": "medium",
            "entry_trend_bucket": "medium",
            "entry_reversion_confirmed": False,
            "quantity": 0.14,
            "entry_notional": 9.52,
        }

        decision = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:05 UTC",
            close_price=67.70,
            previous_close=67.80,
            zscore=0.20,
            previous_zscore=0.50,
            atr_pct=0.0010,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )
        state.process_elite_signal(self.config, decision, self.filters)
        snapshot = state.snapshot()

        self.assertIsNone(snapshot["shadow"]["shadow_probe_position"])
        self.assertEqual(snapshot["shadow"]["completed_trades"], 0)
        self.assertEqual(snapshot["shadow"]["recent_trades"], [])
        self.assertEqual(snapshot["shadow"]["execution_rejections"][0]["rejected_strategy"], "SHADOW_PROBE_STRATEGY")
        self.assertEqual(snapshot["shadow"]["execution_rejections"][0]["reason"], "NON_ACTIVE_STRATEGY_BLOCKED")

    def test_non_active_shadow_trade_is_blocked_at_recording_gate(self):
        state = self.module.RuntimeState()
        trade = {
            "strategy": "SHADOW_PROBE_STRATEGY",
            "symbol": "SOLUSDT",
            "entry_time": "2026-06-14 00:00 UTC",
            "entry_price": 68.0,
            "exit_time": "2026-06-14 00:05 UTC",
            "exit_price": 67.7,
            "side": "SHORT",
            "quantity": 0.14,
            "gross_pnl": 0.042,
            "fees": 0.01,
            "slippage": 0.002,
            "net_pnl": 0.03,
            "exit_reason": "mean reversion exit",
            "result": "WIN",
        }

        recorded = state._record_trade_locked(self.config, trade)
        snapshot = state.snapshot()

        self.assertFalse(recorded)
        self.assertEqual(snapshot["shadow"]["completed_trades"], 0)
        self.assertEqual(snapshot["shadow"]["recent_trades"], [])
        self.assertEqual(snapshot["shadow"]["execution_rejections"][0]["rejected_strategy"], "SHADOW_PROBE_STRATEGY")
        self.assertEqual(snapshot["shadow"]["execution_rejections"][0]["reason"], "NON_ACTIVE_STRATEGY_BLOCKED")

    def test_historical_non_active_trade_marks_dataset_invalid(self):
        shadow = self.module.default_shadow_state()
        shadow["trades"] = [
            {
                "strategy": "SHADOW_PROBE_STRATEGY",
                "net_pnl": 0.03,
                "gross_pnl": 0.04,
                "fees": 0.01,
                "slippage": 0.0,
            }
        ]

        self.module.recompute_shadow_metrics(shadow)

        self.assertFalse(shadow["dataset_valid"])
        self.assertTrue(shadow["execution_contamination_detected"])
        self.assertEqual(shadow["contaminated_trade_count"], 1)
        self.assertEqual(shadow["viability_status"], "INVALID_CONTAMINATED_DATASET")

    def test_closing_trade_updates_metrics_and_writes_latest_report(self):
        os.environ["SHADOW_PROBE_ENABLED"] = "false"
        self.module = reload_bot()
        self.config = self.module.load_config()
        state = self.module.RuntimeState()
        enter = self.module.evaluate_elite_signal(
            self.config,
            timestamp="2026-06-14 00:00 UTC",
            close_price=68.0,
            previous_close=67.8,
            zscore=-2.4,
            previous_zscore=-2.62,
            atr_pct=0.0005,
            atr_bucket="medium",
            trend_bucket="medium",
            trend_slope_pct=0.003,
        )
        state.process_elite_signal(self.config, enter, self.filters)

        exit_decision = self.module.EliteSignalDecision(
            timestamp="2026-06-14 00:10 UTC",
            close_price=68.5,
            previous_close=68.4,
            zscore=-0.20,
            previous_zscore=-0.50,
            atr_pct=0.0004,
            atr_bucket="low",
            trend_bucket="medium",
            trend_slope_pct=0.003,
            direction="LONG",
            trade_decision="REJECT",
            rejection_reason="entry not evaluated while position is open",
            failed_filter="position",
            reversion_confirmed=True,
        )
        state.process_elite_signal(self.config, exit_decision, self.filters)
        snapshot = state.snapshot()

        self.assertEqual(snapshot["shadow"]["completed_trades"], 1)
        self.assertIsNone(snapshot["shadow"]["router_position"])
        self.assertGreater(snapshot["shadow"]["net_pnl_usdt"], 0)
        latest_report = Path(self.temp_dir.name) / "reports" / "latest.json"
        history_reports = list((Path(self.temp_dir.name) / "reports").glob("history_*.json"))
        self.assertTrue(latest_report.exists())
        self.assertEqual(len(history_reports), 1)
        report = json.loads(latest_report.read_text(encoding="utf-8"))
        self.assertIn(report["viability_status"], {"INSUFFICIENT_DATA", "CANDIDATE_FOR_MICRO_TEST"})

    def test_build_decision_report_classifies_viability(self):
        trades = []
        for i in range(100):
            trades.append({"net_pnl": 0.003, "gross_pnl": 0.005, "fees": 0.001, "slippage": 0.001, "result": "WIN"})
        report = self.module.build_decision_report(trades, max_drawdown=0.05)
        self.assertEqual(report["viability_status"], "CANDIDATE_FOR_MICRO_TEST")

        losing = [{"net_pnl": -0.01, "gross_pnl": -0.005, "fees": 0.003, "slippage": 0.002, "result": "LOSS"}]
        report = self.module.build_decision_report(losing, max_drawdown=0.01)
        self.assertEqual(report["viability_status"], "NOT_VIABLE")


class HistoricalReplayTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.module = reload_bot()
        self.config = self.module.load_config()
        self.filters = self.module.ExchangeFilters(
            step_size=0.01,
            tick_size=0.01,
            min_qty=0.01,
            max_qty=1000.0,
            min_notional=5.0,
            quantity_precision=2,
            price_precision=2,
        )

    def _raw_kline(self, open_time, close, close_time=None):
        return [
            open_time,
            str(close),
            str(close + 0.1),
            str(close - 0.1),
            str(close),
            "10",
            close_time if close_time is not None else open_time + 59999,
            "0",
            0,
            "0",
            "0",
            "0",
        ]

    def _frame(self, count):
        rows = []
        for index in range(count):
            close = 70.0 + index * 0.001
            rows.append(
                {
                    "open_time": index * 60000,
                    "open": close,
                    "high": close + 0.1,
                    "low": close - 0.1,
                    "close": close,
                    "volume": 10.0,
                    "close_time": index * 60000 + 59999,
                }
            )
        return self.module.pd.DataFrame(rows)

    def _reject_decision(self, timestamp):
        return self.module.StrategySignalDecision(
            timestamp=timestamp,
            strategy="NO_TRADE",
            regime="NO_TRADE",
            router_strategy="NO_TRADE",
            close_price=70.0,
            previous_close=70.0,
            zscore=0.0,
            previous_zscore=0.0,
            atr_pct=0.001,
            atr_percentile=0.5,
            atr_bucket="medium",
            trend_bucket="weak",
            trend_slope_pct=0.0,
            ema20=70.0,
            ema50=70.0,
            ema_slope_pct=0.0,
            ema_state="EMA20_EQUALS_EMA50",
            price_distance_ema_pct=0.0,
            price_distance_vwap_pct=0.0,
            direction="",
            trade_decision="REJECT",
            entry_reason="",
            rejection_reason="test",
            failed_filter="regime",
            reversion_confirmed=False,
        )

    def test_klines_to_dataframe_drops_forming_candle(self):
        raw = [self._raw_kline(0, 70.0), self._raw_kline(60000, 71.0)]

        frame = self.module.klines_to_dataframe(raw, now_ms=90000)

        self.assertEqual(len(frame), 1)
        self.assertEqual(int(frame.iloc[0]["open_time"]), 0)
        self.assertEqual(list(frame.columns), self.module.HISTORICAL_KLINE_COLUMNS)

    def test_resample_closed_klines_drops_partial_target_candle(self):
        frame = self._frame(31)

        result = self.module.resample_closed_klines(frame, "1m", "15m")

        self.assertEqual(len(result), 2)
        self.assertEqual(int(result.iloc[0]["open_time"]), 0)
        self.assertEqual(int(result.iloc[-1]["close_time"]), 30 * 60000 - 1)

    def test_replay_period_boundaries_are_ordered_and_non_overlapping(self):
        day_ms = 24 * 60 * 60 * 1000
        periods = self.module.replay_period_boundaries(8, 20 * day_ms)

        self.assertEqual(periods["train"], (12 * day_ms, 16 * day_ms))
        self.assertEqual(periods["validation"], (16 * day_ms, 18 * day_ms))
        self.assertEqual(periods["test"], (18 * day_ms, 20 * day_ms))

    def test_candidate_selection_uses_fee_adjusted_net_profit_factor(self):
        trades = []
        for index in range(20):
            net_pnl = 0.02 if index < 12 else -0.015
            trades.append(
                {
                    "strategy": "TREND_FOLLOW_SHORT_STRATEGY",
                    "regime_at_entry": "TREND_DOWN",
                    "net_pnl": net_pnl,
                    "gross_pnl": net_pnl + 0.005,
                    "fees": 0.004,
                    "slippage": 0.001,
                }
            )

        candidates = self.module.select_candidate_pairs(trades, minimum_trades=20)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["pair"], "TREND_DOWN|TREND_FOLLOW_SHORT_STRATEGY")
        self.assertGreater(candidates[0]["net_profit_factor"], 1.1)

    def test_time_window_profitability_uses_net_not_gross_profit_factor(self):
        trades = [
            {
                "entry_time": f"2026-07-02 00:{index:02d} UTC",
                "regime_at_entry": "TREND_DOWN",
                "strategy": "TREND_FOLLOW_SHORT_STRATEGY",
                "gross_pnl": 0.01,
                "fees": 0.015,
                "slippage": 0.005,
                "net_pnl": -0.01,
            }
            for index in range(20)
        ]

        analytics = self.module.build_time_window_analytics(trades, self.config)
        window = analytics["windows"]["00:00-02:00 UTC"]

        self.assertGreater(window["profit_factor"], 1.05)
        self.assertEqual(window["net_profit_factor"], 0.0)
        self.assertFalse(window["is_profitable"])

    def test_skipped_replay_segment_records_why_future_data_was_not_evaluated(self):
        result = self.module.skipped_replay_segment(
            self.config,
            0,
            24 * 60 * 60 * 1000,
            allowed_pairs=set(),
            reason="NO_TRAIN_CANDIDATE",
        )

        self.assertEqual(result["trades"], [])
        self.assertEqual(result["report"]["evaluated_candles"], 0)
        self.assertEqual(result["report"]["skipped_reason"], "NO_TRAIN_CANDIDATE")

    def test_replay_runtime_does_not_persist_forward_artifacts(self):
        state_file = Path(self.temp_dir.name) / "runtime.json"
        signal_file = Path(self.temp_dir.name) / "signals.jsonl"
        trade_file = Path(self.temp_dir.name) / "trades.jsonl"
        config = self.module.replace(
            self.config,
            shadow_signal_log_file=str(signal_file),
            shadow_trade_log_file=str(trade_file),
            shadow_state_file=str(Path(self.temp_dir.name) / "shadow.json"),
        )
        state = self.module.RuntimeState(state_file=str(state_file), persist_events=False)

        state.process_router_signal(config, self._reject_decision("2026-07-13 00:00 UTC"), self.filters)

        self.assertEqual(state.shadow["candidate_signals"], 1)
        self.assertFalse(state_file.exists())
        self.assertFalse(signal_file.exists())
        self.assertFalse(trade_file.exists())

    def test_replay_segment_never_passes_future_candles_to_shared_engine(self):
        frame = self._frame(1300)
        segment_start = 1280 * 60000
        segment_end = 1290 * 60000
        seen = []

        def fake_evaluate(config, state, filters, primary, trend, balance, allowed_pairs, include_market_gate_analytics):
            del config, state, filters, balance, allowed_pairs
            self.assertFalse(include_market_gate_analytics)
            seen.append((int(primary["close_time"].iloc[-1]), int(trend["close_time"].iloc[-1])))
            return self._reject_decision(self.module.candle_time_text(int(primary["close_time"].iloc[-1])))

        with mock.patch.object(self.module, "evaluate_router_candle", side_effect=fake_evaluate):
            result = self.module.replay_segment(
                self.config,
                self.filters,
                frame,
                segment_start,
                segment_end,
                allowed_pairs=None,
            )

        self.assertTrue(seen)
        self.assertEqual(result["report"]["evaluated_candles"], len(seen))
        for primary_close, trend_close in seen:
            self.assertLessEqual(trend_close, primary_close)

    def test_missing_completed_trade_report_is_recreated(self):
        config = self.module.replace(
            self.config,
            shadow_state_file=str(Path(self.temp_dir.name) / "shadow_state.json"),
            shadow_signal_log_file=str(Path(self.temp_dir.name) / "shadow_signals.jsonl"),
            shadow_trade_log_file=str(Path(self.temp_dir.name) / "shadow_trades.jsonl"),
            decision_report_every_completed_trades=50,
        )
        shadow = self.module.default_shadow_state()
        shadow["trades"] = [
            {
                "strategy": "TREND_FOLLOW_SHORT_STRATEGY",
                "strategy_id": "TREND_FOLLOW_SHORT_STRATEGY",
                "execution_source": "regime_strategy_router",
                "regime_at_entry": "TREND_DOWN",
                "net_pnl": 0.01,
                "gross_pnl": 0.02,
                "fees": 0.005,
                "slippage": 0.005,
            }
            for _ in range(50)
        ]
        shadow["completed_trades"] = 50
        shadow["last_decision_report_completed_trades"] = 50

        self.module.maybe_write_decision_report(shadow, config)

        latest = Path(self.temp_dir.name) / "reports" / "latest.json"
        self.assertTrue(latest.exists())
        self.assertEqual(json.loads(latest.read_text(encoding="utf-8"))["total_trades"], 50)

    def test_public_replay_client_never_receives_api_credentials(self):
        calls = []

        class FakeClient:
            def __init__(self, *args, **kwargs):
                calls.append((args, kwargs))

        with mock.patch.object(self.module, "Client", FakeClient):
            self.module.build_public_replay_client(self.config)

        self.assertEqual(calls[0][0][:2], (None, None))
        self.assertFalse(calls[0][1]["testnet"])


class CostAwareCandidateReplayTests(EnvTestCase):
    def setUp(self):
        super().setUp()
        self.module = reload_bot()
        self.config = self.module.load_config()
        self.params = self.module.CandidateReplayParameters()
        self.filters = self.module.ExchangeFilters(
            step_size=0.001,
            tick_size=0.01,
            min_qty=0.001,
            max_qty=1000.0,
            min_notional=5.0,
            quantity_precision=3,
            price_precision=2,
        )

    def _signal_rows(self, atr_15m):
        previous = {
            "open_time": 0,
            "close_time": 59999,
            "open": 101.0,
            "high": 101.0,
            "low": 105.0,
            "close": 100.8,
            "trend_feature_close_time": 59999,
            "trend_ema20": 100.0,
            "trend_ema50": 99.0,
            "trend_atr_15m": atr_15m,
        }
        current = {
            "open_time": 60000,
            "close_time": 119999,
            "open": 100.7,
            "high": 102.0,
            "low": 100.6,
            "close": 101.5,
            "trend_feature_close_time": 119999,
            "trend_ema20": 100.0,
            "trend_ema50": 99.0,
            "trend_atr_15m": atr_15m,
            "trend_ema_spread_pct": 0.01,
            "trend_side": 1,
            "trend_persistence": 5,
            "long_slope_consistency": 1.0,
            "short_slope_consistency": 0.0,
            "atr_1m_pct": 0.002,
            "atr_1m_percentile": 0.5,
        }
        return current, previous

    def test_pullback_distance_scales_with_atr_instead_of_fixed_percentage(self):
        current, previous = self._signal_rows(2.0)
        accepted, reason, distance = self.module.evaluate_candidate_signal(
            current, previous, "LONG", self.params
        )

        self.assertTrue(accepted)
        self.assertEqual(reason, "")
        self.assertAlmostEqual(distance, 0.3)

        current, previous = self._signal_rows(1.0)
        accepted, reason, distance = self.module.evaluate_candidate_signal(
            current, previous, "LONG", self.params
        )

        self.assertFalse(accepted)
        self.assertEqual(reason, "PULLBACK_OUTSIDE_ATR_BAND")
        self.assertAlmostEqual(distance, 0.6)

    def test_long_and_short_excursions_use_independent_direction_formulas(self):
        long_mfe, long_mae = self.module.candidate_excursion_pct("LONG", 100.0, 105.0, 98.0)
        short_mfe, short_mae = self.module.candidate_excursion_pct("SHORT", 100.0, 105.0, 98.0)

        self.assertAlmostEqual(long_mfe, 0.05)
        self.assertAlmostEqual(long_mae, 0.02)
        self.assertAlmostEqual(short_mfe, 0.02)
        self.assertAlmostEqual(short_mae, 0.05)

    def test_candidate_signal_fills_at_next_candle_open_and_records_mfe(self):
        current, previous = self._signal_rows(2.0)
        entry = dict(current)
        entry.update(
            {
                "open_time": 120000,
                "close_time": 179999,
                "open": 102.0,
                "high": 104.0,
                "low": 101.8,
                "close": 103.5,
                "trend_feature_close_time": 179999,
            }
        )
        final = dict(entry)
        final.update(
            {
                "open_time": 180000,
                "close_time": 239999,
                "open": 103.5,
                "high": 103.8,
                "low": 103.0,
                "close": 103.6,
                "trend_feature_close_time": 239999,
            }
        )
        frame = self.module.pd.DataFrame([previous, current, entry, final])

        result = self.module.candidate_replay_direction(
            self.config,
            self.filters,
            frame,
            0,
            240000,
            "LONG",
            self.params,
        )

        self.assertEqual(len(result["trades"]), 1)
        trade = result["trades"][0]
        self.assertEqual(trade["entry_price"], 102.0)
        self.assertEqual(trade["exit_reason"], "TAKE_PROFIT")
        self.assertAlmostEqual(trade["mfe_pct"], self.params.take_profit_pct)
        self.assertEqual(trade["mae_pct"], 0.0)
        self.assertGreater(trade["fees"], 0.0)
        self.assertGreater(trade["slippage"], 0.0)

    def test_direction_gates_do_not_allow_long_results_to_mask_short_losses(self):
        long_trades = [
            {
                "gross_pnl": 0.02,
                "fees": 0.001,
                "slippage": 0.001,
                "net_pnl": 0.018,
                "mfe_pct": 0.012,
                "mae_pct": 0.002,
                "mfe_usdt": 0.12,
                "mae_usdt": 0.02,
                "holding_seconds": 600.0,
                "exit_reason": "TAKE_PROFIT",
            }
            for _ in range(100)
        ]
        short_trades = [
            {
                "gross_pnl": -0.01,
                "fees": 0.001,
                "slippage": 0.001,
                "net_pnl": -0.012,
                "mfe_pct": 0.002,
                "mae_pct": 0.006,
                "mfe_usdt": 0.02,
                "mae_usdt": 0.06,
                "holding_seconds": 300.0,
                "exit_reason": "STOP_LOSS",
            }
            for _ in range(100)
        ]

        long_report = self.module.build_candidate_direction_report(long_trades, 100, self.params)
        short_report = self.module.build_candidate_direction_report(short_trades, 100, self.params)

        self.assertTrue(long_report["gate"]["eligible"])
        self.assertFalse(short_report["gate"]["eligible"])
        self.assertIn("NON_POSITIVE_EXPECTANCY", short_report["gate"]["failure_reasons"])

    def test_candidate_features_never_attach_an_unclosed_future_trend_candle(self):
        rows = []
        for index in range(1200):
            close = 100.0 + index * 0.001
            rows.append(
                {
                    "open_time": index * 60000,
                    "open": close,
                    "high": close + 0.1,
                    "low": close - 0.1,
                    "close": close,
                    "volume": 10.0,
                    "close_time": index * 60000 + 59999,
                }
            )
        frame = self.module.pd.DataFrame(rows)

        features = self.module.build_candidate_feature_frame(frame, self.params)
        available = features.dropna(subset=["trend_feature_close_time"])

        self.assertTrue((available["trend_feature_close_time"] <= available["close_time"]).all())

    def test_candidate_replay_refuses_any_non_shadow_safety_configuration(self):
        unsafe = self.module.replace(self.config, live_trading=True)

        with self.assertRaises(self.module.ConfigError):
            self.module.run_candidate_replay(
                unsafe,
                days=360,
                end_time=None,
                cache_file="unused.csv",
                output_dir_value="unused",
            )

    def test_candidate_cost_gate_matches_fee_and_slippage_assumptions(self):
        ratio = self.module.candidate_estimated_tp_cost_ratio(self.config, self.params)

        self.assertAlmostEqual(ratio, 0.14)
        self.assertLess(ratio, self.params.maximum_tp_cost_ratio)


class LiveSafetyTests(EnvTestCase):
    def test_place_market_order_never_calls_client_in_shadow_only_engine(self):
        module = reload_bot()
        config = module.load_config()

        class FakeClient:
            def __init__(self):
                self.calls = 0

            def futures_create_order(self, **_kwargs):
                self.calls += 1
                raise AssertionError("real order path must not be called")

        fake_client = FakeClient()
        filters = module.ExchangeFilters(
            step_size=0.01,
            tick_size=0.01,
            min_qty=0.01,
            max_qty=7000.0,
            min_notional=5.0,
            quantity_precision=2,
            price_precision=4,
        )
        response = module.place_market_order(fake_client, config, "BUY", 0.14, filters, False, None, "test")

        self.assertEqual(fake_client.calls, 0)
        self.assertEqual(response["status"], "SIMULATED")
        self.assertTrue(response["simulated"])


if __name__ == "__main__":
    unittest.main()
