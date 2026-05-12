import baostock as bs
import pandas as pd

lg = bs.login()
print("login:", lg.error_code, lg.error_msg)

# Try multiple stocks and years to confirm data availability
for code, year, quarter in [
    ("sh.600000", 2023, 4),
    ("sh.600000", 2023, 3),
    ("sh.600036", 2023, 4),
    ("sz.000001", 2023, 4),
]:
    rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
    data = []
    while rs.error_code == "0" and rs.next():
        data.append(rs.get_row_data())
    if data:
        df = pd.DataFrame(data, columns=rs.fields)
        print(f"profit_data {code} {year}Q{quarter}: roeAvg={df.iloc[0]['roeAvg']}  gpMargin={df.iloc[0]['gpMargin']}")
    else:
        print(f"profit_data {code} {year}Q{quarter}: no rows (err={rs.error_code})")

    rs2 = bs.query_growth_data(code=code, year=year, quarter=quarter)
    data2 = []
    while rs2.error_code == "0" and rs2.next():
        data2.append(rs2.get_row_data())
    if data2:
        df2 = pd.DataFrame(data2, columns=rs2.fields)
        print(f"growth_data {code} {year}Q{quarter}: YOYNI={df2.iloc[0]['YOYNI']}  YOYPNI={df2.iloc[0]['YOYPNI']}")
    else:
        print(f"growth_data {code} {year}Q{quarter}: no rows")

    rs3 = bs.query_balance_data(code=code, year=year, quarter=quarter)
    data3 = []
    while rs3.error_code == "0" and rs3.next():
        data3.append(rs3.get_row_data())
    if data3:
        df3 = pd.DataFrame(data3, columns=rs3.fields)
        print(f"balance_data {code} {year}Q{quarter}: liabilityToAsset={df3.iloc[0]['liabilityToAsset']}")
    else:
        print(f"balance_data {code} {year}Q{quarter}: no rows")
    print()

bs.logout()
