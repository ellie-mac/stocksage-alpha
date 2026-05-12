import akshare as ak
import urllib.request, json
from datetime import date

today = date.today().strftime("%Y%m%d")
print("today:", today)

# Test dtgc (limit-down pool)
try:
    dt = ak.stock_zt_pool_dtgc_em(date=today)
    print(f"dtgc ok: {len(dt)} stocks")
except Exception as e:
    print("dtgc error:", e)

# Try alternative EM endpoints for sector data (not push2)
urls = [
    ("EM板块 datacenter", "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_INDUSTRY_INDEX_PRICE&columns=BOARD_NAME%2CCHANGE_RATE&pageNumber=1&pageSize=20&sortTypes=-1&sortColumns=CHANGE_RATE&source=WEB"),
    ("EM板块 push2his", "https://push2his.eastmoney.com/api/qt/clist/get?pn=1&pz=30&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f3,f12,f14"),
    ("EM板块 push2", "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=30&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f3,f12,f14"),
]
for label, url in urls:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com"})
        with urllib.request.urlopen(req, timeout=6) as r:
            raw = r.read().decode("utf-8", errors="replace")
            print(f"{label}: ok (len={len(raw)}) first200: {raw[:200]}")
    except Exception as e:
        print(f"{label}: error {e}")
