"""测一只票拉数据 + 验证 hammer 检测，并打印每根 K 线"""
import akshare as ak
START, END = "20260101", "20260520"
CODE = "600519"

df = ak.stock_zh_a_hist(symbol=CODE, period="daily",
                         start_date=START, end_date=END, adjust="qfq")
print(f"{CODE} shape={df.shape if df is not None else None}")
if df is None or df.empty:
    print("EMPTY")
    raise SystemExit(0)

df = df.rename(columns={"日期":"date","开盘":"open","收盘":"close",
                        "最高":"high","最低":"low","成交量":"vol","成交额":"amt"})
print(df.dtypes)
print()
print("first 3 bars:")
print(df.head(3).to_string())
print()
print("last 3 bars:")
print(df.tail(3).to_string())
