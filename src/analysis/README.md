# src/analysis - 个股/板块分析工具

## 模块

- `stock_fetcher.py` - 通用数据获取层（行情、财务、资金、新闻、概念标签）
- `portfolio_analyzer.py` - 持仓深度分析（综合多维度，按亏损排序）

## 用法

```bash
# 运行持仓分析（修改 portfolio_analyzer.py 底部的持仓列表）
python src/analysis/portfolio_analyzer.py

# 作为模块导入
from src.analysis.stock_fetcher import fetch_realtime_batch, fetch_finance, fetch_main_flow
```

## 数据源

- 腾讯行情 (qt.gtimg.cn) - 实时价格，24h可用
- 东方财富 (push2.eastmoney.com) - 财务指标、资金流向、板块信息，0:00-6:00不可用
- 东方财富搜索 (search-api-web.eastmoney.com) - 新闻资讯
