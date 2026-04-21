# stocksage-alpha

**A-share multi-factor alpha engine** — a 50-factor scoring system and chip-distribution strategy for systematic stock selection in Chinese equity markets.

## Overview

stocksage-alpha scores individual A-share stocks across 47 factors spanning fundamentals, technicals, sentiment, and market structure. Each factor produces independent **buy** and **sell** scores, enabling nuanced signal separation rather than a single net score.

The system also includes a **chip distribution strategy** (筹码峰策略): daily scans identify stocks where the majority of floating chips are profitable, using turnover-rate survival models to match 同花顺 methodology.

## Factor Groups

| Group | Factors | Description |
|-------|---------|-------------|
| **Core** | 1–10 | Value, growth, quality, momentum, technicals |
| **Extended A** | 11–20 | Fund flow, chip distribution, shareholder structure |
| **Extended B** | 21–28 | Institutional behavior, analyst revisions, northbound |
| **Extended C** | 29–33 | Behavioral & market-context factors |
| **Extended A2** | 34–47 | IC-validated additions: low-vol family, cash flow quality, momentum concavity, mean-reversion signals, divergence, BB squeeze, ROE trend, main inflow, Amihud illiquidity |
| **Extended A3** | 48–50 | Batch 8: intraday vs overnight return split, market relative strength, price efficiency |

See [FACTORS.md](FACTORS.md) for full documentation.

## Key Features

- **Dual scoring**: every factor has independent `buy_score` and `sell_score`
- **Cross-rules (交叉规则)**: factor signals modulated by context (price position, volume, ROE, market regime)
- **Market regime weighting**: NORMAL / BULL / EXTREME_BULL / CAUTION / CRISIS weights
- **IC-based auto-tuning**: `auto_tune.py --ic` maps ICIR tiers to weights and updates `factor_config.py`
- **Chip distribution strategy**: daily scan with winner-rate tiers (T1–T5), BOLL/MACD filters, WeChat push
- **A-share specific signals**: limit-hit patterns, LHB flow, northbound contra-sector detection

## Structure

```
stocksage-alpha/
├── data/                        # All JSON data (universes, backtest results, IC results)
├── scripts/
│   ├── factors.py               # Core factors (1–10)
│   ├── factors_extended.py      # Extended factors (11–33)
│   ├── factor_config.py         # Factor weights (NORMAL/BULL/BEAR regimes)
│   ├── research.py              # Single-stock research report
│   ├── screener.py              # Multi-stock screener
│   ├── factor_analysis.py       # IC backtest engine
│   ├── auto_tune.py             # Weight auto-tuning (signal hit rate or --ic mode)
│   ├── backtest.py              # Portfolio backtest
│   ├── chip_strategy.py         # Chip distribution fetch + screen
│   ├── daily_chip_scan.py       # Daily chip scan runner (--ak/--boll/--no-push etc.)
│   ├── chip_backtest.py         # Chip strategy backtest
│   ├── run_all_backtests.py     # Runs IC + portfolio backtests in parallel
│   ├── fetcher.py               # Data fetching layer (Tushare/akshare/BaoStock)
│   ├── cache.py                 # File-based TTL cache (organised into subdirs)
│   ├── cache/                   # Cache files (chip/, price/, financial/, market/ …)
│   ├── logs/                    # All log files
│   └── monitor.py               # Real-time monitor + multi-factor screener
├── stock-bot/
│   └── discord_bot.py           # Discord remote control bot
└── xhs/
    ├── chip_writer.py           # XiaoHongShu posts (morning/midday/evening)
    └── setup_scheduler.py       # Register Windows scheduled tasks
```

## Scheduled Tasks (Windows Task Scheduler)

Register with: `python xhs/setup_scheduler.py` (admin required)

| Task | Time | Action |
|------|------|--------|
| StockSage_ChipNight | 23:00 | Pre-fetch chip data silently (cache warm-up) |
| StockSage_ChipPremarket | 09:00 | Fallback scan if night task missed |
| StockSage_ChipMorning | 09:25 | Post morning chip report to XiaoHongShu + WeChat |
| StockSage_ChipMidday | 11:35 | Post midday snapshot |
| StockSage_ChipEvening | 15:10 | Post closing summary |

## Usage

```bash
# Single stock research
python scripts/research.py 600519

# Daily chip scan (akshare, all filters)
python scripts/daily_chip_scan.py --ak --boll --max-price 50 --no-kcb --high-filter

# IC backtest + weight auto-tune
python scripts/run_all_backtests.py --ic-only
python scripts/auto_tune.py --ic --apply

# Full backtest suite
python scripts/run_all_backtests.py
```

## Remote Control (Discord Bot)

`stock-bot/discord_bot.py` provides remote control over Discord from any network.

- `ca` / `cabekh` — chip scan (b=BOLL, e=≤¥50, k=排科创, h=排高位)
- `bt [N期] [main|smallcap]` — run portfolio backtest
- `bte [main|ic]` — IC backtest
- `研究 600519` — single-stock research report
- `br` — latest backtest result summary

**Setup:**
```bash
pip install -r stock-bot/requirements.txt
cp stock-bot/config.json.example stock-bot/config.json
python -X utf8 stock-bot/discord_bot.py
```

## Requirements

**Python 3.10+**

```bash
pip install -r requirements.txt
```

Dependencies: `akshare>=1.14.0`, `tushare`, `pandas>=2.0.0`, `numpy>=1.24.0`, `ta>=0.11.0`

### First-run setup

```bash
# 1. Pre-warm financial data cache (~5000 stocks, ~1h, resumable)
python scripts/batch_financials.py

# 2. Build screener universe
python scripts/build_universe.py

# 3. Register scheduled tasks (admin)
python xhs/setup_scheduler.py
```

## Factor Documentation

Full factor specs: [FACTORS.md](FACTORS.md)
