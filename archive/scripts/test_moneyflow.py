#!/usr/bin/env python3
"""快速测试 tushare moneyflow_ths 是否支持按 trade_date 批量拉全市场。"""
import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import tushare as ts

cfg_path = ROOT / "alert_config.json"
cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
token = cfg.get("tushare", {}).get("token", "")
pro = ts.pro_api(token)

for trade_date in ["20260507", "20260506"]:
    print(f"\n[test] moneyflow_ths(trade_date={trade_date}) ...")
    try:
        df = pro.moneyflow_ths(trade_date=trade_date)
        print(f"  rows={len(df)}  cols={list(df.columns)}")
        if not df.empty:
            print(df[["ts_code", "net_amount", "buy_lg_amount", "sell_lg_amount"]].head(5).to_string())
    except Exception as e:
        print(f"  [error] {e}")
