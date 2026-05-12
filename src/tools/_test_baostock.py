import baostock as bs
import pandas as pd

lg = bs.login()
print("login:", lg.error_code)

for api_name, kwargs in [
    ("query_profit_data",    {"code": "sh.600001", "year": 2023, "quarter": 4}),
    ("query_growth_data",    {"code": "sh.600001", "year": 2023, "quarter": 4}),
    ("query_balance_data",   {"code": "sh.600001", "year": 2023, "quarter": 4}),
    ("query_operation_data", {"code": "sh.600001", "year": 2023, "quarter": 4}),
    ("query_cash_flow_data", {"code": "sh.600001", "year": 2023, "quarter": 4}),
    ("query_dupont_data",    {"code": "sh.600001", "year": 2023, "quarter": 4}),
]:
    try:
        rs = getattr(bs, api_name)(**kwargs)
        data = []
        while rs.error_code == "0" and rs.next():
            data.append(rs.get_row_data())
        if data:
            df = pd.DataFrame(data, columns=rs.fields)
            print(f"\n{api_name}: {rs.fields}")
            print(df.iloc[0].to_dict())
        else:
            print(f"\n{api_name}: no rows (err={rs.error_code} {rs.error_msg})")
    except Exception as e:
        print(f"\n{api_name}: EXCEPTION {e}")

bs.logout()
