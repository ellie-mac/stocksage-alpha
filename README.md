# stocksage-alpha

A-share 量化选股与自动化推送系统，运行在 Azure VM，每日定时扫描并通过微信/飞书推送信号。

## 策略

### 筹码策略（chip）
基于同花顺筹码分布，扫描获利盘比例达标的股票，叠加 BOLL/MACD 过滤后分 T1–T3 档推送。每晚 21:00 自动跑三档（CAH/CADM/CAD），取三筛俱过者为最高置信。

```bash
python -X utf8 src/chip/strategy.py --cad --mods bekh    # 完整扫描+推送
python -X utf8 src/chip/strategy.py --cad --dry-run      # 仅打印
```

### 热榜策略（hot_scan）
拉取东财人气榜实时快照，用动量分（MA趋势/净涨/量能/连热）过滤出高热度+技术面强势的标的。每日 19:00 推送。

```bash
python -X utf8 src/strategies/hot_scan.py --push
```

### 机构策略（institution_scan）
监测 53 只主动/量化基金的季度持仓，找到多家基金同一季度同时新增同一只股票的情况。每日 8:30 运行，有变化才推送。

```bash
python -X utf8 src/strategies/institution_scan.py --push-if-changed
```

基金列表：`data/fund_watchlist.json`（53 只，含量化多因子 + 明星主动基金）

### 主策略（nightly_scan）
50 因子多空评分，每晚夜间扫描全市场，选出综合得分最高的买入候选。

```bash
python -X utf8 src/jobs/nightly_scan.py
```

## 基础设施

- **运行环境**：Azure VM（Singapore），Windows Server，Python 3.13
- **定时任务**：Windows Task Scheduler，bat 文件在 `tasks/`
- **通知渠道**：微信（PushPlus）+ 飞书卡片
- **配置文件**：`alert_config.json`（API keys，不入 git）

| 任务 | 时间 | 说明 |
|------|------|------|
| chip_Night | 18:00 | 预取筹码缓存（不推送） |
| chip_CadScan | 21:00 | 筹码三档扫描+推送 |
| gc_Scan | 19:30 | 金叉共振扫描+推送 |
| institution_Scan | 8:30 | 机构策略扫描，有变化推送 |
| nightly_Scan | 22:10 | 主策略+小盘+ETF 夜间选股 |
| main_Night | 22:30 | 预热财务缓存 |

## 项目结构

```
src/
├── factors/              # 因子包
│   ├── scoring.py        # 11个核心因子 + compute_total_score
│   ├── config.py         # 五档权重配置
│   └── analysis.py       # IC回测引擎
├── chip/                 # 筹码策略
│   ├── strategy.py       # 核心算法（winner_rate/cost分层）
│   ├── daily_scan.py     # 全市场扫描入口
│   └── pipeline.py       # CAD精筛流水线
├── strategies/           # 选股策略
│   ├── main_strategy.py
│   ├── golden_cross_scan.py
│   ├── hot_scan.py
│   └── institution_scan.py
├── jobs/                 # 定时任务脚本
│   ├── nightly_scan.py
│   ├── closing_batch.py
│   ├── prefetch.py
│   └── auto_tune.py
├── backtest/             # 回测框架
│   ├── main.py           # 组合回测
│   └── etf.py            # ETF回测
├── report/               # 小红书文案
│   └── reporter.py
├── notify/               # 飞书通知
├── tools/                # 工具脚本
├── fetcher.py            # 数据拉取层
├── factors_extended.py   # 56个扩展因子
└── common.py             # 推送工具

stock-bot/
├── lark_bot.py           # 飞书机器人传输层
└── bot_common.py         # 命令逻辑（t/z/p/ca/cad/gc/ic/fx/sc）

data/
└── fund_watchlist.json   # 机构策略基金列表（唯一入 git 的数据文件）
```

## 依赖

```bash
pip install -r requirements.txt
```

主要依赖：`akshare`, `tushare`, `pandas`, `numpy`, `ta`, `tqdm`
