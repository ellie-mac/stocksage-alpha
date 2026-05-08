# stocksage-alpha

A-share 量化选股与自动化推送系统，运行在 Azure VM，每日定时扫描并通过微信/飞书推送信号。

## 策略

### 筹码策略（chip）
基于同花顺筹码分布，扫描获利盘比例达标的股票，叠加 BOLL/MACD 过滤后分 T1–T3 档推送。每晚 21:00 自动跑三档（CAH/CADM/CAD），取三筛俱过者为最高置信。

```bash
python -X utf8 scripts/chip_strategy.py --cad --mods bekh bekhm   # 完整扫描+推送
python -X utf8 scripts/chip_strategy.py --cad --dry-run            # 仅打印
```

### 热榜策略（hot_scan）
拉取东财人气榜实时快照，用动量分（MA趋势/净涨/量能/连热）过滤出高热度+技术面强势的标的。每日 19:00 推送。

```bash
python -X utf8 scripts/hot_scan.py --push
```

### 机构策略（institution_scan）
监测 53 只主动/量化基金的季度持仓，找到多家基金同一季度同时新增同一只股票的情况。每日 8:30 运行，有变化才推送。

```bash
python -X utf8 scripts/institution_scan.py --push             # 强制推送
python -X utf8 scripts/institution_scan.py --push-if-changed  # 有变化才推（定时任务用）
```

基金列表：`data/fund_watchlist.json`（53 只，含量化多因子 + 明星主动基金）

### 主策略（monitor）
50 因子多空评分，盘中实时扫描持仓和自选，触发买卖信号推送。每日 7:10 盘前运行。

```bash
python -X utf8 scripts/monitor.py --always-send --buy-only
```

## 基础设施

- **运行环境**：Azure VM（East US），Windows Server，Python 3.13
- **定时任务**：Windows Task Scheduler，bat 文件在 `tasks/`
- **通知渠道**：微信（ServerChan/PushPlus）+ 飞书卡片
- **配置文件**：`alert_config.json`（API keys，不入 git）

| 任务 | 时间 | 说明 |
|------|------|------|
| chip_Night | 18:00 | 预取筹码缓存（不推送） |
| chip_CadScan | 21:00 | 筹码三档扫描+推送 |
| hot_Scan | 19:00 | 热榜策略扫描+推送 |
| institution_Scan | 8:30 | 机构策略扫描，有变化推送 |
| main_Morning | 7:10 | 主策略盘前扫描+推送 |
| main_Night | 22:30 | 预热财务缓存 |

## 关键脚本

```
scripts/
├── chip_strategy.py      # 筹码策略核心（CAH/CAD/CADM）
├── hot_scan.py           # 热榜策略
├── institution_scan.py   # 机构策略
├── monitor.py            # 主策略实时监控
├── run_cad_pipeline.py   # 筹码流水线（自愈缓存+扫描）
├── factors.py            # 因子定义（1–10）
├── factors_extended.py   # 因子定义（11–50）
├── factor_analysis.py    # IC 回测引擎
├── backtest.py           # 组合回测
└── common.py             # 微信/飞书推送工具
data/
└── fund_watchlist.json   # 机构策略基金列表（唯一入 git 的数据文件）
```

## 依赖

```bash
pip install -r requirements.txt
```

主要依赖：`akshare`, `tushare`, `pandas`, `numpy`, `ta`, `tqdm`
