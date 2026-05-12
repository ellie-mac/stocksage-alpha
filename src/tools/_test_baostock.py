import baostock as bs
import pandas as pd

lg = bs.login()
print("login:", lg.error_code, lg.error_msg)

# test profit data (ROE, gross_margin)
rs = bs.query_profit_data(code="sh.600001", year=2023, quarter=4)
print("profit_data fields:", rs.fields)
data = []
while (rs.error_code == "0") and rs.next():
    data.append(rs.get_row_data())
if data:
    df = pd.DataFrame(data, columns=rs.fields)
    print(df.head(2).to_string())
else:
    print("profit_data: no rows, error:", rs.error_code, rs.error_msg)

# test growth data (revenue_growth, profit_growth)
rs2 = bs.query_growth_data(code="sh.600001", year=2023, quarter=4)
print("growth_data fields:", rs2.fields)
data2 = []
while (rs2.error_code == "0") and rs2.next():
    data2.append(rs2.get_row_data())
if data2:
    df2 = pd.DataFrame(data2, columns=rs2.fields)
    print(df2.head(2).to_string())

# test balance data (debt ratio)
rs3 = bs.query_balance_data(code="sh.600001", year=2023, quarter=4)
print("balance_data fields:", rs3.fields)
data3 = []
while (rs3.error_code == "0") and rs3.next():
    data3.append(rs3.get_row_data())
if data3:
    df3 = pd.DataFrame(data3, columns=rs3.fields)
    print(df3.head(2).to_string())

bs.logout()
