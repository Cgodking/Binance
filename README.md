# Binance 合约影子交易研究平台

这是一个单文件 Python 应用，用于 Binance USD-M 合约行情监控、确定性影子交易、历史回放和滚动样本外策略验证。

## 安全状态

- 必须保持 `DRY_RUN=true`。
- 必须保持 `SHADOW_MODE=true`。
- 必须保持 `LIVE_TRADING=false`。
- 当前引擎会拒绝任何尝试启用真实交易的配置。
- 运行状态、API 凭据、日志、历史行情和研究报告均不会提交到 Git。

当前候选策略尚未证明具有可交易的样本外净正期望。本仓库不能被视为策略已经盈利的证据，也不能作为恢复真实交易的依据。

## 运行环境

- Python 3.10 或更高版本
- 服务器部署推荐使用 Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
```

在对外开放 Flask 监控页面前，必须配置高强度的 `WEB_PASSWORD` 或 `WEB_AUTH_TOKEN`。禁止将 Binance API 凭据提交到 Git。

## 启动程序

```bash
python3 binance_futures_zscore_bot.py
```

监控页面默认监听 `5055` 端口。

## 运行测试

```bash
python3 -m unittest -v test_binance_futures_zscore_bot.py
```

## 历史研究

运行市场状态和策略路由历史回放：

```bash
python3 binance_futures_zscore_bot.py replay --days 180
```

运行冻结参数的候选策略滚动验证：

```bash
python3 binance_futures_zscore_bot.py candidate-replay --profile v4_15m_1h
python3 binance_futures_zscore_bot.py candidate-replay --profile v5_30m_2h
python3 binance_futures_zscore_bot.py candidate-replay --profile v6_long_cost_gate_30m_2h
python3 binance_futures_zscore_bot.py candidate-replay --profile v7_cost_aware_breakout_15m_1h_4h
python3 binance_futures_zscore_bot.py candidate-replay --profile v8_cost_aware_pullback_15m_1h_4h --output-dir candidate_replay_v8
python3 binance_futures_zscore_bot.py candidate-portfolio-replay --profile v6_long_cost_gate_30m_2h
```

V3、V4、V5、V6、V7、V8 profile 均保留冻结参数，历史报告可以按版本复现。V5 默认使用 900 天数据，并执行以下验证：

- 使用闭合 2 小时 K 线确认趋势，使用闭合 30 分钟 K 线确认 ATR 自适应回调，并在下一根 30 分钟 K 线开盘模拟成交。
- 270 天训练、90 天验证、90 天测试，每次向前滚动 90 天。
- 每个训练/验证边界使用 960 分钟隔离区，900 天数据可形成 6 个测试窗口。
- 每个策略只连续执行一次完整历史，再按窗口切片统计，避免重复计算重叠训练区间。
- `TREND_PULLBACK_LONG` 和 `TREND_PULLBACK_SHORT` 使用互不混合的独立账本；旧均值回归候选只保留为冻结基线，不参与候选选择。
- 固定 10 USDT 名义金额用于判断策略边际；独立的 5 USDT 微额风险账本用于判断账户和交易所过滤器下是否可执行，不修改服务器实盘风控。
- 止盈、止损使用执行周期 ATR 自适应距离，并设置上下界，避免低波动时执行成本吞噬目标收益。
- 成本包含双边手续费、滑点、点差、资金费率和跳空穿越止损，并输出 1 倍、1.5 倍、2 倍成本压力测试。
- 样本外结果输出块自助法期望置信区间、滚动窗口一致性、最大回撤和每个独立策略的准入失败原因。
- 历史门槛通过最多只能标记为未来影子验证候选；历史回放不能直接产生微额实盘资格。

V6 不再同时探索多空方向，只验证 Long，并增加严格的训练成本门控：

- 仅使用每个 walk-forward 窗口的训练交易选择 ATR 百分位、EMA spread、回调 ATR 距离和斜率一致性桶。
- 每个训练桶至少需要 30 笔交易，并且必须通过 1.5 倍成本、净利润因子和块自助置信区间检查。
- 训练选出的桶必须先通过独立验证区间；验证失败时，该窗口测试交易不会参与策略准入统计。
- 测试数据不会参与桶选择，报告同时保留 raw 和 gated 交易文件用于审计。
- V6 直接缓存闭合 30 分钟公共行情，旧 profile 继续使用原始 1 分钟缓存，避免改变历史结果。
- V6 使用 100 USDT 固定名义金额归一化不同合约的研究收益，避免 BTC 最小数量步进让研究账本失真；独立的 5 USDT 风险账本仍按真实过滤器拒绝不可执行订单。
- 跨品种验证固定使用 SOLUSDT、BTCUSDT、ETHUSDT、BNBUSDT；至少 3 个品种通过且 5 USDT 风险账本覆盖率不低于 50%，才能进入未来影子验证。
- 跨品种历史通过仍不会自动启用 `FORWARD_SHADOW_EXECUTION_ENABLED`，更不会启用真实交易。

V7 是成本感知的低频趋势突破实验，不复用旧 Z-score 或趋势回调入场：

- 使用闭合 4 小时 K 线的 EMA50/EMA200 与 EMA50 斜率确定大级别方向，使用闭合 1 小时 EMA50/EMA200 确认趋势结构。
- 15 分钟收盘价必须突破此前 20 根已闭合 K 线的 Donchian 通道，当前成交量必须大于此前 20 根平均成交量的 1.3 倍。
- 信号仅在 15 分钟 K 线闭合后成立，并在下一根 15 分钟 K 线开盘模拟成交；高周期特征不会读取尚未闭合或未来的 K 线。
- Long 与 Short 使用 `COST_AWARE_TREND_BREAKOUT_V7_LONG` 和 `COST_AWARE_TREND_BREAKOUT_V7_SHORT` 两个独立账本，不合并判断准入。
- 初始止损为 1 ATR，目标为 3 ATR，最长持仓 72 小时；趋势结构失效、止损、止盈或超时均可触发退出。
- 入场前要求毛目标收益至少为预计手续费、滑点、点差和资金费缓冲的 5 倍，并要求扣除预计成本后的净盈亏比不低于 2。
- 使用 180 天训练、90 天验证、90 天测试，每次滚动 90 天；720 天数据形成 5 个窗口，边界隔离时间为 72 小时。
- 5 USDT 风险账本按单笔不超过余额 1% 且不超过 0.05 USDT 计算数量。最小有效订单超过风险预算时记录 `TRADE_SKIPPED_MIN_NOTIONAL`，不会提高仓位。

截至 2026-07-17 的 720 天 SOLUSDT 冻结回放中，V7 未通过历史门槛：Long 测试 156 笔、净期望 -0.1807、净利润因子 0.7234；Short 测试 215 笔、净期望 -0.1842、净利润因子 0.7352。5 USDT 风险账本中 Long 全部订单因最小名义金额超过风险预算而跳过，Short 仅有 2 笔可执行且净亏损。结论保持 `DO_NOT_TRADE_LIVE`。

V8 是与 V7 隔离的低频趋势回调实验，不会修改旧实时路由或自动开启前向影子执行：

- 使用闭合 4 小时 EMA50/EMA200 与 EMA50 斜率确定大级别方向，使用闭合 1 小时 EMA20/EMA50、趋势持续时间和 EMA 斜率一致性确认中级趋势。
- 15 分钟 K 线必须先在 1 小时 EMA20 附近形成 ATR 自适应回调，再以收盘价重新站上 EMA20 和前一根 15 分钟高点；Short 使用完全独立的镜像条件。
- 高周期方向、1 小时趋势和 15 分钟恢复必须一致。任何尚未闭合或来自未来的高周期特征都会被拒绝。
- 信号在 15 分钟 K 线闭合后确认，只能在下一根 15 分钟 K 线开盘模拟成交。Long 与 Short 使用独立账本和独立准入结论。
- 初始止损为 1.25 ATR，目标为止损距离的 2.5 倍，并分别限制在 0.6%-2.5% 和 1.5%-6.25%；最长持仓及窗口隔离时间均为 48 小时。
- 毛目标收益必须至少为预计双边手续费、滑点、点差和资金费缓冲的 5 倍，扣除预计成本后的净盈亏比不得低于 1.5。
- 使用 720 天数据、180 天训练、90 天验证、90 天测试并每次滚动 90 天。训练、验证和测试利润因子门槛分别为 1.20、1.15 和 1.10。
- 固定 100 USDT 账本只用于归一化判断信号边际；5 USDT 风险账本仍按余额 1% 和 0.05 USDT 上限检查交易所最小订单可执行性。
- 历史验证通过最多只能产生 forward shadow 候选，不会启用真实交易，也不会自动修改 `FORWARD_SHADOW_EXECUTION_ENABLED`。

历史回放仅使用公开市场数据，不使用 API 密钥。行情缓存、资金费率缓存、交易记录和研究报告会写入已被 `.gitignore` 排除的本地目录。

`FORWARD_SHADOW_EXECUTION_ENABLED=false` 是默认设置。历史候选未通过样本外门槛前，实时 1 分钟旧路由只记录信号，不会继续产生新的影子仓位。

## 风险仓位

影子运行和风险覆盖回放使用“最大可接受净亏损”决定仓位：

```text
允许名义金额 = 风险预算 / (止损比例 + 跳空缓冲 + 双边手续费 + 双边滑点 + 双边点差)
```

风险预算取 `MAX_NET_LOSS_PER_TRADE_USDT` 与 `账户余额 × MAX_RISK_PER_TRADE_PCT` 中较小值，同时受 `MAX_MARGIN_USDT`、`MAX_ACCOUNT_MARGIN_FRACTION` 和 `MAX_NOTIONAL_USDT` 限制。若交易所允许的最小订单仍会超过风险预算，系统会跳过信号，不会为了满足最小名义金额而主动加仓。

## 服务器辅助命令

`install_binancep_commands.sh` 用于安装服务器上的交互式配置和进程管理命令。在其他主机执行或加载该脚本前，应先检查脚本内容并确认路径和代理设置适用于目标环境。

## 重要说明

- 项目目前仅用于影子交易和策略研究。
- 历史盈利不代表未来收益。
- 回放结果必须计入手续费、滑点、点差、资金费率和跳空风险。
- 策略必须在多数滚动测试窗口为正，并通过 1.5 倍成本、块自助置信区间、回撤和风险仓位可执行性检查，才可被标记为微额测试候选。
- 未通过样本外验证前，不应启用真实交易。
