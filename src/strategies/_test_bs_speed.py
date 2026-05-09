import baostock as bs, pandas as pd, time, akshare as ak, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

print("Getting Sina spot data...", flush=True)
spot = ak.stock_zh_a_spot()
print(f"Sina spot: {spot.shape}", flush=True)

# Filter candidates (same as marketcap_strategy)
df = spot.copy()
df = df[~df["名称"].str.contains("ST|退", na=False)]
df = df[~df["代码"].str.startswith("688")]
df = df[~(df["代码"].str.startswith("8") | df["代码"].str.startswith("43"))]
price_col = pd.to_numeric(df["最新价"], errors="coerce")
df = df[price_col > 2.0].copy()
print(f"Candidates after filter: {len(df)}", flush=True)

# Last trading day (skip weekends)
today = datetime.now()
d = today - timedelta(days=1)
while d.weekday() >= 5:  # Saturday=5, Sunday=6
    d -= timedelta(days=1)
last_td = d.strftime("%Y-%m-%d")
print(f"Querying BaoStock for date: {last_td}", flush=True)

# Map code to baostock format
def to_bs_code(code6):
    if code6.startswith("6"):
        return f"sh.{code6}"
    elif code6.startswith("0") or code6.startswith("3"):
        return f"sz.{code6}"
    return None

# Build list of (code6, bs_code, name, price, change_pct)
candidates = []
for _, row in df.iterrows():
    c = row["代码"]
    bs_c = to_bs_code(c)
    if bs_c:
        candidates.append({
            "code": c,
            "bs_code": bs_c,
            "name": row["名称"],
            "price": float(pd.to_numeric(row["最新价"], errors="coerce") or 0),
            "change_pct": float(pd.to_numeric(row["涨跌幅"], errors="coerce") or 0),
        })
print(f"First 5 raw codes: {list(df['代码'].head())}", flush=True)
print(f"Mapped candidates: {len(candidates)}, using first 300", flush=True)
candidates = candidates[:300]

# Batch query via BaoStock (single connection, sequential)
bs.login()

def fetch_mv(cand):
    try:
        rs = bs.query_history_k_data_plus(
            cand["bs_code"], "close,turn,amount",
            start_date=last_td, end_date=last_td,
            frequency="d", adjustflag="3"
        )
        data = rs.data
        if not data:
            return None
        row = data[0]
        fields = rs.fields
        d = dict(zip(fields, row))
        t = float(d.get("turn", 0) or 0)
        a = float(d.get("amount", 0) or 0)
        if t <= 0 or a <= 0:
            return None
        mv_yi = a / (t / 100) / 1e8
        return {**cand, "marketcap_yi": round(mv_yi, 2)}
    except Exception:
        return None

t0 = time.time()
results = []
done = 0
for cand in candidates:
    r = fetch_mv(cand)
    if r:
        results.append(r)
    done += 1
    if done % 100 == 0:
        print(f"  {done}/{len(candidates)} done, {len(results)} valid  ({time.time()-t0:.0f}s)", flush=True)

elapsed = time.time() - t0
bs.logout()
print(f"\nQueried {len(candidates)} stocks, got {len(results)} valid in {elapsed:.1f}s", flush=True)

results.sort(key=lambda x: x["marketcap_yi"])
print(f"\nTop 20 by 流通市值 ({last_td}):")
for i, r in enumerate(results[:20], 1):
    print(f"{i:2d}. {r['code']} {r['name']:8s}  {r['marketcap_yi']:.1f}亿  ¥{r['price']:.2f}")
