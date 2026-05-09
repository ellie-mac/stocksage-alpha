import baostock as bs, pandas as pd, time, akshare as ak
from datetime import datetime, timedelta

print("Getting Sina spot data...", flush=True)
spot = ak.stock_zh_a_spot()
print(f"Sina spot: {spot.shape}", flush=True)
print(f"First 5 raw codes: {list(spot['代码'].head())}", flush=True)

df = spot.copy()
# Normalize codes: Sina returns sh600000/sz000001/bj920000; strip 2-char prefix
df["代码"] = df["代码"].astype(str).apply(
    lambda c: c[2:] if len(c) > 6 and c[:2].isalpha() else c
)
print(f"First 5 normalized codes: {list(df['代码'].head())}", flush=True)

# Filter (same logic as marketcap_strategy)
df = df[~df["名称"].str.contains("ST|退", na=False)]
df = df[~df["代码"].str.startswith("688")]
df = df[~(df["代码"].str.startswith("8") | df["代码"].str.startswith("43") | df["代码"].str.startswith("9"))]
price_col = pd.to_numeric(df["最新价"], errors="coerce")
df = df[price_col > 2.0].copy()
print(f"Candidates after filter: {len(df)}", flush=True)

# Last trading day (skip weekends)
today = datetime.now()
d = today - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
last_td = d.strftime("%Y-%m-%d")
print(f"Querying BaoStock for date: {last_td}", flush=True)

def to_bs_code(code6):
    if code6.startswith("6") and not code6.startswith("688"):
        return f"sh.{code6}"
    elif code6.startswith("0") or code6.startswith("3"):
        return f"sz.{code6}"
    return None

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

print(f"Mapped candidates: {len(candidates)}, using first 300", flush=True)
candidates = candidates[:300]

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
        d = dict(zip(rs.fields, row))
        t = float(d.get("turn") or 0)
        a = float(d.get("amount") or 0)
        if t <= 0 or a <= 0:
            return None
        mv_yi = a / (t / 100) / 1e8
        return {**cand, "marketcap_yi": round(mv_yi, 2)}
    except Exception:
        return None

t0 = time.time()
results = []
for i, cand in enumerate(candidates, 1):
    r = fetch_mv(cand)
    if r:
        results.append(r)
    if i % 100 == 0:
        print(f"  {i}/{len(candidates)} done, {len(results)} valid ({time.time()-t0:.0f}s)", flush=True)

elapsed = time.time() - t0
bs.logout()
print(f"\nQueried {len(candidates)} stocks, got {len(results)} valid in {elapsed:.1f}s", flush=True)

results.sort(key=lambda x: x["marketcap_yi"])
print(f"\nTop 20 by 流通市值 ({last_td}):")
for i, r in enumerate(results[:20], 1):
    print(f"{i:2d}. {r['code']} {r['name']:8s}  {r['marketcap_yi']:.1f}亿  ¥{r['price']:.2f}  {r['change_pct']:+.2f}%")
