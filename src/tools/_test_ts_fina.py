import sys, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import tushare as ts

cfg = os.path.join(os.path.dirname(__file__), "..", "..", "alert_config.json")
with open(cfg, encoding="utf-8") as f:
    token = json.load(f).get("tushare", {}).get("token", "")
print("token found:", bool(token))
ts.set_token(token)
pro = ts.pro_api()

try:
    df = pro.fina_indicator(ts_code="000001.SZ", start_date="20220101",
                            fields="ts_code,ann_date,roe,grossprofit_margin")
    print("fina_indicator ok, rows:", len(df) if df is not None else "None")
    if df is not None and not df.empty:
        print(df.head(2).to_string())
except Exception as e:
    print(f"error: {type(e).__name__}: {e}")
