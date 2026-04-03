---
name: stocksage
description: A股智研助手 — 个股深度研究、多因子评分与智能选股。支持沪深A股分析，基于47因子模型输出评分及综合研究报告。
metadata: {"openclaw":{"emoji":"📊","requires":{"bins":["python3","python"],"anyBins":["python3","python"]},"install":[{"kind":"uv","package":"akshare"},{"kind":"uv","package":"pandas"},{"kind":"uv","package":"numpy"},{"kind":"uv","package":"ta"}]}}
---

# StockSage — A股智研助手

## 功能

1. **个股研究**：给定股票代码或名称，生成完整的基本面、技术面、多因子评分报告（47因子模型）
2. **因子选股**：通过编号菜单或自然语言描述条件，筛选符合条件的A股
3. **权重定制**：自然语言指定因子偏好（如"重视成长"/"focus on growth"）

## 何时使用 / When to activate

Activate this skill when the user expresses any of the following intents (Chinese or English):
- Research a specific stock: "帮我研究一下茅台" / "analyze 600519" / "research BYD"
- Screen for stocks: "帮我筛选股票" / "find stocks" / "screen for low PE high growth"
- Score a stock: "给茅台打个分" / "score 600519"

## 选股工作流

### Step 1 — 如果用户未指定条件，展示因子菜单

当用户说"帮我选股"、"筛选一些股票"、"find stocks"等而没有具体条件时，
**先向用户展示以下菜单**，让他们选择编号：

```
📊 请选择筛选因子（输入编号，多个用逗号分隔，如 "1,3,9"）：

【估值类】
  1. 低估值        Low Valuation          — PE/PB行业内分位偏低
  2. 高股息        High Dividend Yield    — 股息率TTM ≥ 3%

【成长类】
  3. 高成长        High Growth            — 营收/利润增速 > 20%
  4. 稳健成长      Steady Growth          — 营收/利润增速 > 10%

【技术/动量类】
  5. 价格强势      Price Momentum         — 近60日涨幅居前（强动量）
  6. 均线多头      MA Bullish Alignment   — 5/10/20/60日均线多头排列
  7. 量能放大      Volume Breakout        — 今日成交量 > 20日均量1.5倍
  8. 高量比        High Volume Ratio      — 量比 > 2（短期活跃放量）

【质量类】
  9. 高ROE         High ROE               — ROE ≥ 15%（盈利能力强）
 10. 低负债        Low Debt               — 资产负债率 < 40%（财务稳健）
 11. 高毛利率      High Gross Margin      — 毛利率 > 30%（护城河宽）
 12. 低波动        Low Volatility         — 股价波动率低、走势稳健

【资金类】
 13. 主力资金流入  Institutional Flow     — 近5日大单净流入持续为正

【规模类】
 14. 小市值        Small Cap              — 总市值 < 100亿（高弹性）
 15. 大盘蓝筹      Large Cap Blue Chip    — 总市值 > 500亿（低风险）
```

### Step 2 — 运行选股脚本

用户选择编号后（如 "1,3,9"），运行菜单模式：
```
python {baseDir}/scripts/screener.py --menu "1,3,9"
```

如果用户直接输入自然语言条件，运行查询模式：
```
python {baseDir}/scripts/screener.py "低估值高成长"
python {baseDir}/scripts/screener.py "high quality low debt focus on growth"
```

打印因子菜单（无参数）：
```
python {baseDir}/scripts/screener.py --list
```

### Step 3 — 展示结果

展示 Top 10 候选股，包含代码、名称、主要指标及综合评分。
如结果包含 `unapplied_conditions`，告知用户需先运行 `batch_financials.py` 预热财务数据。

## 个股研究工作流

1. 识别股票代码（6位数字，如 600519）或名称（如 贵州茅台）
2. 运行研究脚本（支持可选权重参数）：
   ```
   python {baseDir}/scripts/research.py <股票代码>
   python {baseDir}/scripts/research.py <股票代码> --weights "focus on growth"
   python {baseDir}/scripts/research.py <股票代码> --weights "重视质量 低波动"
   ```
3. 基于 JSON 输出，用中文撰写研究报告：

---

### 📊 [股票名称]（[代码]）研究报告

**实时行情**
- 当前价格、涨跌幅、量比、换手率

**估值分析**
- PE/PB 当前值、3年历史分位、行业内相对分位（如可用）

**基本面**
- 营收/利润增速、ROE、毛利率趋势解读

**技术面**
- 均线排列（多头/混合/空头）、MACD信号、RSI

**资金面**
- 大单净流入趋势、融资余额变化

**多因子评分（47因子）** [总分 XX/100]

核心因子（各25分）：
- 🟢价值 XX/25 | 🟢成长 XX/25 | 🟡动量 XX/25 | 🟢质量 XX/25

扩展因子 A 组（来自基础数据）：
- 北向资金 XX/10 | 量能突破 XX/10 | 52周位置 X/5
- 股息率 X/10 | 量比 X/10 | 均线排列 XX/15 | 低波动 X/10

扩展因子（短期反转/资金流/筹码/技术/行为等共36因子）：
- 综合扩展得分 XX/100（详见 `research.py` 输出 `extended_factors`）

**综合结论**
- 2-3句话总结，给出明确观点（如：基本面优秀但当前估值偏高，适合回调后布局）

---

## 因子体系说明（47因子）

系统共 47 个因子，分四组。完整文档见 [FACTORS.md](FACTORS.md)。

### 核心因子（4个，各25分，`factors.py`）

| # | 因子 | 满分 | 数据来源 | 说明 |
|---|------|------|----------|------|
| 1 | 价值 (value) | 25 | 实时行情 | PE/PB行业内分位 → 历史分位 → 绝对阈值（三级回退） |
| 2 | 成长 (growth) | 25 | 财报API | 营收增速×10 + 利润增速×10 + ROE趋势×5 |
| 3 | 动量 (momentum) | 25 | 价格历史 | 1月/3月/6月收益率加权 |
| 4 | 质量 (quality) | 25 | 财报API | ROE均值×10 + 毛利率×10 + 低负债×5 |

### 扩展因子 A 组（7个，来自基础数据，`factors_extended.py`）

| # | 因子 | 满分 | 说明 |
|---|------|------|------|
| 5 | 北向/主力资金 | 10 | 近5日大单净流入天数+强度 |
| 6 | 量能突破 (volume) | 10 | 当日成交量 / 20日均量比 |
| 7 | 52周位置 | 5 | 当前价格在52周高低区间中的位置 |
| 8 | 股息率 (div_yield) | 10 | 股息率TTM：≥5%→满分 |
| 9 | 量比 (volume_ratio) | 10 | 今日量/5日均量：2.5–4倍→满分 |
| 10 | 均线排列 (ma_alignment) | 15 | MA5>MA10>MA20>MA60多头排列程度 |
| 11 | 低波动 (low_volatility) | 10 | 年化波动率：≤15%→满分，≥60%→0分 |

### 扩展因子 A 续 + B + C 组（22个，`factors_extended.py`）

| 因子 | 类别 | 说明 |
|------|------|------|
| reversal（12）| 短期反转 | 1月涨跌幅，散户过度反应反转 |
| accruals（13）| 盈利质量 | 应计项目，现金含量检验 |
| asset_growth（14）| 资产扩张 | 总资产增速，过度扩张警示 |
| piotroski（15）| 基本面 | 9项二值指标，F分综合 |
| short_interest（16）| 做空压力 | 融券比例，轧空vs正常做空 |
| rsi_signal（17）| 技术 | 14日RSI超买超卖 |
| macd_signal（18）| 技术 | MACD柱方向与强度 |
| turnover_percentile（19）| 换手率 | 近5日换手率 vs 90日均值 |
| chip_distribution（20）| 筹码 | 52周位置×大小单资金流向 |
| shareholder_change（21）| 股东结构 | 季度股东人数变化（筹码集中度） |
| lhb（22）| 龙虎榜 | 近90日机构净买入 |
| social_heat（23）| 舆情 | 东方财富热榜热度 |
| concept_momentum（24）| 概念动量 | 所属概念板块近1月涨跌幅 |
| institutional_visits（25）| 机构调研 | 近90日调研次数 |
| industry_momentum（26）| 行业动量 | 行业vs沪深300超额收益 |
| northbound_actual（27）| 北向持仓 | 沪深港通实际持仓变化 |
| earnings_revision（28）| 盈利预测 | 分析师净上调/下调次数 |
| limit_hits（29）| 涨跌停 | 近20日净涨停次数 |
| price_inertia（30）| 价格惯性 | 近20日价格涨幅 |
| bb_position（31）| 布林带 | 价格在布林带中的位置 |
| divergence（32）| 量价背离 | OBV与价格方向背离 |
| roe_trend（33）| ROE趋势 | ROE季度环比方向 |

### 扩展因子 A2 组（14个，IC验证批次，`factors_extended.py`）

| 因子 | IC | 说明 |
|------|----|------|
| main_inflow（34）| +0.048 | 近5日主力净流入金额 |
| cash_flow_quality（35）| +0.063 | 经营现金流/净利润 |
| momentum_concavity（36）| +0.058 | 动量加速度（近30日-中30日） |
| low_vol_12m（37）| +0.201 | 12个月年化波动率（低波动异象） |
| idiosyncratic_vol（38）| +0.249 | 特质波动率（去除市场beta后） |
| return_skewness（39）/ skewness | +0.105 | 收益率偏度（彩票效应） |
| atr_normalized（44）| +0.249 | 归一化ATR，捕捉缺口风险 |
| ma60_deviation（45）| +0.098 | 距MA60偏离度（均值回归） |
| max_return（46）| +0.216 | 近20日最大单日涨幅（彩票效应） |
| medium_term_momentum（42）| −0.108 | 40日收益率（A股反转） |
| obv_trend（43）| −0.115 | OBV斜率（A股散户追涨信号） |
| amihud_illiquidity（41）| −0.062 | 非流动性比率（流动性溢价） |
| intraday_vs_overnight（48）| −0.103 | 日内vs隔夜收益分拆 |
| bb_squeeze（40）| — | 布林带收窄，波动率压缩信号 |

## 权重定制语法

在查询或 `--weights` 参数中嵌入以下关键词即可调整权重：

| 关键词（中/英均可） | 效果 |
|---------------------|------|
| `重视成长` / `focus on growth` | 成长权重 ×3 |
| `重视估值` / `focus on value` | 价值权重 ×3 |
| `重视质量` / `focus on quality` | 质量权重 ×3 |
| `重视动量` / `focus on momentum` | 动量权重 ×3 |
| `高股息` / `dividend` | 股息率权重 ×3 |
| `均线` / `trend following` | 均线排列权重 ×2 |
| `低波动` / `defensive` | 低波动权重 ×3，动量权重 ×0.3 |
| `量比` / `active` | 量比权重 ×2 |
| `北向资金` / `smart money` | 主力资金权重 ×2 |
| `均衡` / `balanced` | 恢复默认权重 |

## 批量预计算（推荐设置）

因子 2、4 和部分菜单选项（3、4、9、10、11）需要预先计算财务数据：

```bash
# 全量运行（~5000只，约1小时，支持断点续跑）
python {baseDir}/scripts/batch_financials.py

# 测试运行
python {baseDir}/scripts/batch_financials.py --max 200
```

**推荐设置每日 02:00 定时执行**（cron 或 Windows Task Scheduler）。

## 行业地图缓存

首次使用前，在 Python 中运行一次可预热行业内估值对比（~90次API，缓存7天）：
```python
from scripts.industry import build_industry_map
build_industry_map()
```

## 重要说明

- 实时行情延迟约10秒，本地缓存30秒；价格历史缓存1小时；财报缓存7天
- 本工具仅供辅助研究，不构成投资建议
- 脚本运行通常需要10-30秒，请告知用户耐心等待
- 因子6（均线多头）、12（低波动）、13（主力资金）在选股模式中仅影响排名权重，不作为硬过滤条件（需逐股调取价格历史，批量场景下成本过高）

## 错误处理与常见问题

| 现象 | 原因 | 解决 |
|------|------|------|
| `unapplied_conditions` 出现在结果中 | 财务类筛选条件（ROE、负债率、毛利率等）未预热 | 运行 `python scripts/batch_financials.py`，支持断点续跑 |
| 股票代码无结果 | 代码输入错误、退市或停牌 | 核对6位代码，沪市以6开头，深市以0/3开头，北交所以8开头 |
| 网络超时 / BaoStock 无响应 | API 限速或服务器抖动 | 稍后重试；若持续失败检查网络，BaoStock 有每日调用限额 |
| 52周位置显示为空 | 股票上市不足252个交易日（约1年） | 正常，新股/次新股无法计算52周指标 |
| 因子评分偏低但股票看起来不错 | 默认权重为均衡模式；特定策略可能需要调权 | 使用 `--weights "重视成长"` 等参数按需调整 |
| 数据明显滞后 | 价格缓存TTL为1小时，财报缓存7天 | 清除 `.cache/` 目录后重新运行 |
