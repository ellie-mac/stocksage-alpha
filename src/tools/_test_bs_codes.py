import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import baostock as bs
from common import get_spot_em

lg = bs.login()
print("login:", lg.error_code)

spot = get_spot_em()
codes = spot["代码"].astype(str).str.zfill(6).tolist()
print("first 10 raw codes:", codes[:10])

def _code_to_bs(code):
    return ("sh." if code.startswith("6") else "sz.") + code

bs_codes = [_code_to_bs(c) for c in codes[:10]]
print("bs_codes:", bs_codes)
print("lengths:", [len(c) for c in bs_codes])

# test one
for c in bs_codes[:3]:
    rs = bs.query_profit_data(code=c, year=2026, quarter=1)
    data = []
    while rs.error_code == "0" and rs.next():
        data.append(rs.get_row_data())
    print(f"{c}: rows={len(data)}, err={rs.error_code}, msg={rs.error_msg}")

bs.logout()
