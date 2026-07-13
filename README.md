# Binance 合约影子交易研究平台

这是一个单文件 Python 应用，用于 Binance USD-M 合约行情监控、确定性影子交易、历史回放和滚动样本外策略验证。

## 安全状态

- 必须保持 `DRY_RUN=true`。
- 必须保持 `SHADOW_MODE=true`。
- 必须保持 `LIVE_TRADING=false`。
- 当前引擎会拒绝任何尝试启用真实交易的配置。
- 运行状态、API 凭据、日志、历史行情和研究报告均不会提交到 Git。

当前成本感知趋势回调候选策略没有通过训练阶段准入门槛。本仓库不能被视为策略已经盈利的证据，也不能作为恢复真实交易的依据。

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

运行冻结参数的成本感知趋势回调候选策略，并分别验证 LONG 和 SHORT：

```bash
python3 binance_futures_zscore_bot.py candidate-replay --days 360
```

历史回放仅使用公开市场数据。生成的行情缓存、交易记录和研究报告会写入已被 `.gitignore` 排除的本地目录。

## 服务器辅助命令

`install_binancep_commands.sh` 用于安装服务器上的交互式配置和进程管理命令。在其他主机执行或加载该脚本前，应先检查脚本内容并确认路径和代理设置适用于目标环境。

## 重要说明

- 项目目前仅用于影子交易和策略研究。
- 历史盈利不代表未来收益。
- 回放结果必须同时计入手续费和滑点。
- 任何策略都必须通过训练集、验证集和测试集后，才可以进入实时影子观察阶段。
- 未通过样本外验证前，不应启用真实交易。
