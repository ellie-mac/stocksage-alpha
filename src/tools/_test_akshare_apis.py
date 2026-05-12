import akshare as ak
import urllib.request, json

# Find limit-down related functions
dt_fns = [x for x in dir(ak) if "stock" in x and any(k in x for k in ["_dt_", "dtgc", "跌停"])]
print("dt functions:", dt_fns[:15])

zt_fns = [x for x in dir(ak) if "stock" in x and "_zt_" in x]
print("zt functions:", zt_fns[:15])

# Test Sina sector API directly
print("\nTesting Sina sector API...")
try:
    url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple?num=80&sort=changepercent&asc=0&node=hy_001"
    req = urllib.request.Request(url, headers={"Referer": "https://finance.sina.com.cn"})
    with urllib.request.urlopen(req, timeout=8) as r:
        raw = r.read().decode("gbk", errors="replace")
        data = json.loads(raw)
        print(f"Sina sector ok: {len(data)} sectors")
        if data:
            print("sample keys:", list(data[0].keys()))
            print("top 3:", [(d.get("name","?"), d.get("changepercent","?")) for d in data[:3]])
except Exception as e:
    print("Sina sector error:", e)
