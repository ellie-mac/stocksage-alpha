"""
双锤子线策略回测 v2（与 double_hammer_scan v2 参数对齐）

放宽点：
  · 下影 2.0 → 1.5× 实体
  · 量能 1.0 → 0.8× MA5
  · 成交额 0.5 → 0.3 亿
  · 严格连两天 → 近 3 天 ≥2 个锤子
"""
import os, sys, time, random, warnings, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import pandas as pd
import numpy as np
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings("ignore")

random.seed(42)
SAMPLE_N = 200
START = "20260101"
END   = "20260520"
HORIZONS = [1, 3, 5, 10]
OUT = r"C:/Users/jiapeichen/repos/me/notes/output/double_hammer_v2_backtest.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# ---------- v2 参数（与 scan 一致）----------
LOWER_SHADOW_MIN = 1.5
UPPER_SHADOW_MAX = 1.0
BODY_PCT_MIN     = 0.3
HAMMER_WINDOW    = 3
HAMMER_MIN_COUNT = 2
VOL_RATIO_MIN    = 0.8
AMOUNT_MIN_YI    = 0.3


def is_hammer(o, c, h, l):
    if o <= 0:
        return False
    body = abs(c - o)
    body_pct = body / o * 100
    if body_pct < BODY_PCT_MIN:
        return False
    lower = min(o, c) - l
    upper = h - max(o, c)
    if lower < body * LOWER_SHADOW_MIN:
        return False
    if upper > body * UPPER_SHADOW_MAX:
        return False
    return True


def fetch_hist(code, retries=3):
    for i in range(retries):
        try:
            return ak.stock_zh_a_hist(symbol=code, period="daily",
                                      start_date=START, end_date=END, adjust="qfq")
        except Exception:
            time.sleep(1 + i)
    return None


def scan_history(code):
    df = fetch_hist(code)
    if df is None or len(df) < 30:
        return []
    df = df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close",
                            "最高": "high", "最低": "low", "成交量": "vol", "成交额": "amt"})
    df = df.sort_values("date").reset_index(drop=True)
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma5v"] = df["vol"].rolling(5).mean()
    n = len(df)
    triggers = []
    for i in range(25, n - max(HORIZONS) - 1):
        # 今天必须是锤子
        d2 = df.iloc[i]
        if not is_hammer(d2["open"], d2["close"], d2["high"], d2["low"]):
            continue
        # 近 HAMMER_WINDOW 天找锤子
        hammers = []
        for offset in range(HAMMER_WINDOW - 1, -1, -1):
            row = df.iloc[i - offset]
            if is_hammer(row["open"], row["close"], row["high"], row["low"]):
                hammers.append(i - offset)
        if len(hammers) < HAMMER_MIN_COUNT:
            continue
        # 不破窗口内**第一个**锤子的 low（0.5% 容差）
        d1 = df.iloc[hammers[0]]
        if d2["low"] < d1["low"] * 0.995:
            continue
        # MA20 above + 5d 上行
        if pd.isna(d2["ma20"]) or d2["close"] < d2["ma20"]:
            continue
        if pd.isna(df["ma20"].iloc[i - 5]):
            continue
        if d2["ma20"] <= df["ma20"].iloc[i - 5]:
            continue
        # 量能 + 金额
        if pd.isna(d2["ma5v"]) or d2["vol"] < d2["ma5v"] * VOL_RATIO_MIN:
            continue
        if (d2["amt"] / 1e8) < AMOUNT_MIN_YI:
            continue
        if i + 1 >= n:
            continue
        buy = df.iloc[i + 1]["open"]
        if buy <= 0:
            continue
        rec = {"code": code, "trigger_date": str(d2["date"])[:10], "buy": buy,
               "hammers_in_window": len(hammers)}
        for h in HORIZONS:
            if i + h < n:
                rec[f"r{h}"] = (df.iloc[i + h]["close"] / buy - 1) * 100
            else:
                rec[f"r{h}"] = np.nan
        triggers.append(rec)
    return triggers


def main():
    print("[1/3] 加载本地股票池...", flush=True)
    raw = json.load(open('data/universe_main.json', encoding='utf-8'))
    codes = []
    for x in raw:
        c = x[2:] if x[:2] in ('sh', 'sz', 'bj') else x
        if c.startswith(('8', '4', '9')):
            continue
        codes.append(c)
    print(f"  全A: {len(codes)}", flush=True)
    random.shuffle(codes)
    codes = codes[:SAMPLE_N]
    print(f"  样本: {len(codes)}", flush=True)
    print(f"  v2 参数: lower≥{LOWER_SHADOW_MIN}*body / vol≥{VOL_RATIO_MIN}*MA5 / "
          f"amt≥{AMOUNT_MIN_YI}亿 / {HAMMER_WINDOW}天≥{HAMMER_MIN_COUNT}锤", flush=True)

    print("[2/3] 回测中(并发4)...", flush=True)
    all_recs = []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(scan_history, c): c for c in codes}
        for f in as_completed(futs):
            done += 1
            try:
                rs = f.result()
                if rs:
                    all_recs.extend(rs)
            except Exception:
                pass
            if done % 20 == 0:
                print(f"  {done}/{len(codes)}  触发={len(all_recs)}  {int(time.time() - t0)}s", flush=True)
    print(f"  完成 {done}/{len(codes)}  总触发: {len(all_recs)}  耗时 {int(time.time() - t0)}s", flush=True)

    if not all_recs:
        print("无任何触发样本，v2 仍过严 → 可考虑 lower→1.2 或 window→5")
        return
    df = pd.DataFrame(all_recs)
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("\n[3/3] 统计结果", flush=True)
    print(f"{'horizon':<10}{'样本':<8}{'胜率%':<10}{'平均%':<10}{'中位%':<10}{'最大%':<10}{'最小%':<10}")
    for h in HORIZONS:
        s = df[f"r{h}"].dropna()
        if len(s) == 0:
            continue
        win = (s > 0).mean() * 100
        print(f"T+{h:<8}{len(s):<8}{win:<10.2f}{s.mean():<10.2f}{s.median():<10.2f}{s.max():<10.2f}{s.min():<10.2f}")
    print(f"\n明细: {OUT}")


if __name__ == "__main__":
    main()
