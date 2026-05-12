import akshare as ak
from datetime import date

today = date.today().strftime("%Y%m%d")
print("today:", today)

try:
    zt = ak.stock_zt_pool_em(date=today)
    print("zt ok:", len(zt))
except Exception as e:
    print("zt error:", e)

try:
    dt = ak.stock_dt_pool_em(date=today)
    print("dt ok:", len(dt))
except Exception as e:
    print("dt error:", e)

try:
    df = ak.stock_board_industry_name_em()
    print("sector ok, cols:", list(df.columns[:6]), "rows:", len(df))
    print(df[["板块名称", "涨跌幅"]].sort_values("涨跌幅", ascending=False).head(3).to_string())
except Exception as e:
    print("sector error:", e)
