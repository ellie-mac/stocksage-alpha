import sys, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import tushare as ts

cfg = os.path.join(os.path.dirname(__file__), "..", "..", "alert_config.json")
with open(cfg, encoding="utf-8") as f:
    token = json.load(f).get("tushare", {}).get("token", "")
ts.set_token(token)
pro = ts.pro_api()

for api in ["income", "balancesheet", "cashflow", "express", "forecast", "fina_indicator"]:
    try:
        df = getattr(pro, api)(ts_code="000001.SZ", start_date="20230101")
        cols = list(df.columns[:5]) if df is not None and not df.empty else []
        print(f"{api}: ok  rows={len(df) if df is not None else 0}  cols={cols}")
    except Exception as e:
        print(f"{api}: ERROR {e}")
