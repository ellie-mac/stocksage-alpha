# Memory

## [2026-04-28] 系统架构总览

**仓库路径**：C:/Users/jiapeichen/repos/stocksage-alpha

**三大策略**：
1. **多因子主策略**（scripts/screener.py + monitor.py）：47个因子加权打分，total_score ≥ 65 买入，sell_score ≥ 60 卖出，止损 -8%
2. **筹码分布策略**（scripts/chip_strategy.py + daily_chip_scan.py）：winner_rate 分 T1-T5 五档，T1 ≥ 95%，T5 ≥ 65%
3. **金叉策略**（scripts/golden_cross_scan.py）：8项技术指标共振，G1 ≥ 7个信号，G5 ≥ 3个

**核心文件**：
- `scripts/factors.py` — 核心4因子（value/growth/momentum/quality）
- `scripts/factors_extended.py` — 扩展因子11-50+
- `scripts/factor_config.py` — 五档市场制度权重配置
- `scripts/factor_analysis.py` — IC分析引擎
- `scripts/backtest.py` — 组合回测（top 20%，16期）
- `scripts/monitor.py` — 持仓监控+买卖信号推送（--loop 模式）
- `scripts/chip_cad.py` — 数据驱动筹码全档（CAD/CADM）
- `alert_config.json` — 买卖阈值配置
- `holdings.json` — 当前持仓

---

## [2026-04-28] 市场制度与因子权重

**制度切换标准**：CSI300 20日收益

| 制度 | 触发条件 | 目标敞口 | 关键特征 |
|------|---------|---------|---------|
| EXTREME_BULL | > +6% | 70% | momentum 2.5，激励连板 limit_hits +1.0 |
| BULL | +2.5%~+6% | 80% | momentum 2.0，停止惩罚涨停 limit_hits 0 |
| NORMAL | -3%~+2.5% | 85% | 均衡，overhead_resistance 1.5 高权重 |
| CAUTION | -6%~-3% | 70% | 防守，overhead_resistance 2.5，limit_hits -2.0 |
| CRISIS | < -6% | 40% | 极端防守，仅保留防守因子 |

**当前敞口**：NORMAL 制度下已从 100% 降至 85%（2026-04-15 起）

**高权重因子（NORMAL）**：
- overhead_resistance（头部阻力）1.5 — 反映筹码套牢压力
- return_skewness（收益偏度）1.5 — 反映尾部风险
- growth 1.0，momentum 0.8，hammer_bottom 0.8

**反向因子（NORMAL）**：
- limit_hits -1.2（涨停股均值回归）
- chip_distribution -1.0
- price_inertia -0.8

---

## [2026-04-28] 关键参数与阈值

**买卖阈值**（alert_config.json）：
- 买入：total_score ≥ 65
- 卖出：sell_score ≥ 60
- 止损：跌幅 ≤ -8%
- 盘中跌幅告警：≤ -5%，涨幅告警：≥ +7%
- 告警冷却：快速 30min，紧急 15min，卖出 90min

**回测参数**：
- periods: 16，forward_days: 10，step_days: 20
- top_pct: 0.2（top 20%），txn_cost: 0.1%，行业中性

**IC分析参数**：
- periods: 6，forward_days: 20，step_days: 20
- IC质量：strong ICIR > 0.5，moderate 0.3~0.5，weak < 0.3

---

## [2026-04-28] 筹码策略详情

**T档划分**（winner_rate = 获利盘比例）：
- T1: ≥ 95%（深度获利，筹码高度集中于成本以下）
- T2: 90~95%
- T3: 85~90%
- T4: 75~85%
- T5: 65~75%

**CAD 推荐排序**：T4 → T1 → T2 → T3 → T5（ML优化）

**常用修饰符**：
- `b` BOLL中轨±8%内，`e` 股价≤50，`k` 排科创板，`h` 排高位，`m` MACD绿柱，`z` MACD近零
- 推荐组合：`cad`（bekh）、`cadm`（bekhm）

**数据源**：Tushare Pro `cyq_perf` 优先，无额度时自动降级 akshare 自算（约5-10分钟）

---

## [2026-04-28] 金叉策略详情

**8项信号**：MACD金叉、KDJ金叉、RSI金叉、MA5/10金叉、MA10/20金叉、量能金叉、OBV金叉、布林中轨金叉

**G档划分**：G1 ≥ 7信号，G2 = 6，G3 = 5，G4 = 4，G5 = 3

---

## [2026-04-28] 定时任务流水线

| 时间 | 任务 | 说明 |
|------|------|------|
| 07:00 | chip_Premarket | 筹码盘前兜底 |
| 07:10 | main_Morning | 主策略盘前兜底 |
| 09:25 | xhs_Morning | 小红书盘前推送 |
| 11:35 | xhs_Midday | 午间推送 |
| 15:30 | xhs_Evening | 收盘推送 |
| 15:35 | market_Warm | 预热市场数据 |
| 15:45 | price_Prefetch | 预热全量价格历史（~1.5h）|
| 16:00 | daily_PerfLog | 三合一胜率记录 |
| 18:00 | chip_Night | 收盘后筹码缓存预取 |
| 18:30 | main_Scan | 主策略扫盘，更新 latest_picks.json |
| 19:30 | gc_Scan | 金叉扫描 |
| 20:30 | chip_CadScan | CAD/CADM推送 |
| 22:30 | main_Night | 预热财务缓存 |

---

## [2026-04-28] Bot 命令速查

**筹码**：`ca`全档，`cad`数据驱动全档（推荐），`cadm`+MACD绿柱，`c1-c5`按档查询
**主策略**：`p`今日推荐，`fx 代码`单股分析，`s`扫盘推送，`hh`持仓列表
**回测**：`bt`启动回测，`br`结果摘要，`bs`回测进度，`kb`终止回测
**因子**：`ic`IC摘要，`ich`全因子列表，`icf 因子名`因子说明
**系统**：`z`系统状态，`t`定时任务，`r`重启monitor，`l`最近日志

---

## [2026-04-28] 常见陷阱与注意事项

**前瞻偏差**：factor_analysis 用 --group A（仅价格因子），基本面数据需注意T-1修正

**市场制度切换**：limit_hits 从 -1.2 跳到 0 会剧烈改变排名，避免频繁切换；需连续观察确认

**筹码信号失效情形**：股权变动/定增后筹码分布打乱，财报扭亏期短期波动大

**科创板**（688xxx）：默认排除，制度差异+流动性差+因子特性不同

**A股涨停**：NORMAL 制度惩罚涨停股（均值回归），BULL 制度停止惩罚（连板是主角）

**止损 -8%** 对 A股波动略严，区分"技术性调整"和"基本面恶化"再决定

---

## [2026-04-28] 待加入因子（按优先级）

以下7个因子已规划，待逐一接入后做散度回测验证：
1. 大单净流入
2. 股东人数变化
3. 解禁压力
4. 行业相对强弱
5. 布林带位置
6. ROE趋势
7. 现金流质量
