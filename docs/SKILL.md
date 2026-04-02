---
name: stocksage
description: A股智研助手 — 个股深度研究、多因子评分与智能选股。支持沪深A股分析，输出11维因子评分及综合研究报告。
metadata: {"openclaw":{"emoji":"📊","requires":{"bins":["python3","python"],"anyBins":["python3","python"]},"install":[{"kind":"uv","package":"akshare"},{"kind":"uv","package":"pandas"},{"kind":"uv","package":"numpy"},{"kind":"uv","package":"ta"}]}}
---

# StockSage — A股智研助手

## 功能

1. **个股研究**：给定股票代码或名称，生成完整的基本面、技术面、11维因子评分报告
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

**11维因子评分** [总分 XX/100]
- 🟢价值 XX/25 | 🟢成长 XX/25 | 🟡动量 XX/25 | 🟢质量 XX/25
- 北向资金 XX/10 | 量能突破 XX/10 | 52周位置 X/5
- 股息率 X/10 | 量比 X/10 | 均线排列 XX/15 | 低波动 X/10

**综合结论**
- 2-3句话总结，给出明确观点（如：基本面优秀但当前估值偏高，适合回调后布局）

---

## 11维因子说明

| # | 因子 | 满分 | 数据来源 | 说明 |
|---|------|------|----------|------|
| 1 | 价值 (value) | 25 | 实时行情 | PE/PB行业内分位 → 历史分位 → 绝对阈值（三级回退） |
| 2 | 成长 (growth) | 25 | 财报API | 营收增速×10 + 利润增速×10 + ROE趋势×5 |
| 3 | 动量 (momentum) | 25 | 价格历史 | 1月/3月/6月收益率加权 |
| 4 | 质量 (quality) | 25 | 财报API | ROE均值×10 + 毛利率×10 + 低负债×5 |
| 5 | 北向/主力资金 | 10 | 资金流向 | 近5日大单净流入天数+强度 |
| 6 | 量能突破 (volume) | 10 | 价格历史 | 当日成交量 / 20日均量比 |
| 7 | 52周位置 | 5 | 价格历史 | 当前价格在52周高低区间中的位置 |
| 8 | 股息率 (div_yield) | 10 | 实时行情 | 股息率TTM：≥5%→满分 |
| 9 | 量比 (volume_ratio) | 10 | 实时行情 | 今日量/5日均量：2.5–4倍→满分，>5倍→注意顶部 |
| 10 | 均线排列 (ma_alignment) | 15 | 价格历史 | MA5>MA10>MA20>MA60多头排列程度 |
| 11 | 低波动 (low_volatility) | 10 | 价格历史 | 年化波动率：≤15%→满分，≥60%→0分 |

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

## 错误处理

- 股票不存在：提示确认代码或名称
- `unapplied_conditions`：告知用户运行 `batch_financials.py` 预热
- 网络超时：建议稍后重试
- 数据不完整：告知缺失指标，基于可用数据给出部分分析
