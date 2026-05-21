"""
双锤子线扫盘 v2（放宽版）— 基于实战调阈：
  · 下影 2.0 → 1.5× 实体（仍是 hammer 形态）
  · 量能 1.0 → 0.8× MA5（允许略缩但形态完整）
  · 成交额 0.5亿 → 0.3亿（小盘友好）
  · 严格连两天 → 近 3 天有 ≥2 个锤子，且最后一根是今天

基准日：TARGET_DATE
"""
import os
import time
import pandas as pd
import numpy as np
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

OUT_DIR = r"C:/Users/jiapeichen/repos/me/notes/output"
os.makedirs(OUT_DIR, exist_ok=True)

TARGET_DATE = "2026-05-20"
START_DATE = "20260101"
END_DATE = "20260520"

# ---------- v4 range-based hammer 判定 ----------
# 之前按 body 比例判定上下影，小实体时 upper ≤ body 极严（body=0.01 时 upper 必须 ≤1%）。
# 改成按 range = high-low 的比例，更符合经典教科书定义，对小实体宽容。
RANGE_PCT_MIN    = 0.5     # K 线 range ≥ 0.5% open（防平盘）
BODY_OVER_RANGE_MAX = 0.4  # 实体 ≤ range 的 40%（hammer 特征：小实体）
LOWER_OVER_RANGE_MIN = 0.55  # 下影 ≥ range 的 55%（hammer 特征：长下影）
UPPER_OVER_RANGE_MAX = 0.15  # 上影 ≤ range 的 15%（hammer 特征：短上影）

HAMMER_WINDOW    = 5       # 5 天窗口找锤子
HAMMER_MIN_COUNT = 2       # 至少 2 个锤子
VOL_RATIO_MIN    = 0.8     # 量能下限
AMOUNT_MIN_YI    = 0.3     # 成交额下限·亿


# ---------- 形态判定 ----------
def is_hammer(row):
    """返回 (是否锤子, 下影占 range 的百分比)。

    经典 hammer 定义（range-based）：
      · range ≥ 0.5% open（有波动）
      · 实体 ≤ 40% range（小实体）
      · 下影 ≥ 55% range（长下影）
      · 上影 ≤ 15% range（短上影）
    """
    o, c, h, l = row["open"], row["close"], row["high"], row["low"]
    if o <= 0 or h <= l:
        return False, 0.0
    rng = h - l
    if rng / o * 100 < RANGE_PCT_MIN:
        return False, 0.0
    body = abs(c - o)
    if body / rng > BODY_OVER_RANGE_MAX:
        return False, 0.0
    lower = min(o, c) - l
    upper = h - max(o, c)
    if lower / rng < LOWER_OVER_RANGE_MIN:
        return False, 0.0
    if upper / rng > UPPER_OVER_RANGE_MAX:
        return False, 0.0
    lower_pct = lower / rng * 100   # 下影占 range %
    return True, lower_pct


def scan_one(code, name, board):
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=START_DATE, end_date=END_DATE, adjust="qfq"
        )
    except Exception:
        return None
    if df is None or len(df) < 30:
        return None

    df = df.rename(columns={
        "日期": "date", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount"
    })
    df["date"] = df["date"].astype(str)
    if df["date"].iloc[-1] != TARGET_DATE:
        return None  # 当日停牌或数据未更新

    df["ma20"] = df["close"].rolling(20).mean()
    df["ma5_vol"] = df["volume"].rolling(5).mean()
    if len(df) < 26:
        return None

    d2 = df.iloc[-1]  # 今天，必须是锤子
    ok2, ls2 = is_hammer(d2)
    if not ok2:
        return None

    # 近 HAMMER_WINDOW 天里找锤子（含今天），需要 ≥ HAMMER_MIN_COUNT 个
    hammers = []  # [(row_index, lower_shadow_pct)]
    for offset in range(HAMMER_WINDOW, 0, -1):
        if len(df) < offset:
            continue
        row = df.iloc[-offset]
        ok, ls = is_hammer(row)
        if ok:
            hammers.append((len(df) - offset, ls))
    if len(hammers) < HAMMER_MIN_COUNT:
        return None

    # 取窗口内**第一个**锤子作为 d1（参考锤）；今天 low 不破它的 low（0.5% 容差）
    d1_idx = hammers[0][0]
    d1 = df.iloc[d1_idx]
    if d2["low"] < d1["low"] * 0.995:
        return None

    # MA20 上方且 5 日内上行
    if pd.isna(d2["ma20"]) or d2["close"] < d2["ma20"]:
        return None
    ma20_5d_ago = df["ma20"].iloc[-6]
    if pd.isna(ma20_5d_ago) or d2["ma20"] <= ma20_5d_ago:
        return None

    # 量能 + 成交额
    if pd.isna(d2["ma5_vol"]) or d2["volume"] < d2["ma5_vol"] * VOL_RATIO_MIN:
        return None
    amount_yi = d2["amount"] / 1e8
    if amount_yi < AMOUNT_MIN_YI:
        return None

    return {
        "code": code,
        "name": name,
        "board": board,
        "date": TARGET_DATE,
        "close": round(d2["close"], 2),
        "ma20": round(d2["ma20"], 2),
        "ma20_dev_pct": round((d2["close"] / d2["ma20"] - 1) * 100, 2),
        "hammer_count_in_window": len(hammers),
        "d1_date": str(d1["date"])[:10],
        "d1_lower_shadow_pct": round(hammers[0][1], 2),
        "d2_lower_shadow_pct": round(ls2, 2),
        "vol_vs_ma5": round(d2["volume"] / d2["ma5_vol"], 2),
        "amount_yi": round(amount_yi, 2),
    }


def board_of(code):
    if code.startswith("60"): return "sh_main"
    if code.startswith("00"): return "sz_main"
    if code.startswith("30"): return "cyb"
    if code.startswith("68"): return "kcb"
    return "other"


def main():
    print("[1/3] 拉取全 A 现货清单...")
    spot = ak.stock_zh_a_spot_em()
    spot = spot.rename(columns={"代码": "code", "名称": "name"})
    spot["code"] = spot["code"].astype(str).str.zfill(6)

    spot = spot[~spot["name"].str.contains("ST|退", regex=True, na=False)]
    spot = spot[~spot["code"].str.startswith(("8", "4", "9"))]
    spot["board"] = spot["code"].apply(board_of)
    spot = spot[spot["board"] != "other"]

    tasks = list(zip(spot["code"], spot["name"], spot["board"]))
    total = len(tasks)
    print(f"  待扫描：{total}")
    print(f"  v4 参数 (range-based): body≤{BODY_OVER_RANGE_MAX*100:.0f}% / lower≥{LOWER_OVER_RANGE_MIN*100:.0f}% / "
          f"upper≤{UPPER_OVER_RANGE_MAX*100:.0f}% of range / vol≥{VOL_RATIO_MIN}*MA5 / amt≥{AMOUNT_MIN_YI}亿 / "
          f"{HAMMER_WINDOW}天≥{HAMMER_MIN_COUNT}锤")

    print("[2/3] 并发扫描...")
    results = []
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(scan_one, c, n, b): c for c, n, b in tasks}
        for f in as_completed(futs):
            done += 1
            try:
                r = f.result()
                if r:
                    results.append(r)
            except Exception:
                pass
            if done % 500 == 0:
                el = time.time() - t0
                print(f"  进度 {done}/{total}  命中 {len(results)}  用时 {el:.0f}s")

    print(f"[3/3] 完成。命中 {len(results)} 只。")

    if not results:
        print("无候选（v2 仍无信号 → 可再放宽 lower→1.2 或 window→5）。")
        return

    df_out = pd.DataFrame(results).sort_values("ma20_dev_pct").reset_index(drop=True)
    out_path = os.path.join(OUT_DIR, f"double_hammer_v2_候选_{TARGET_DATE}.csv")
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"输出：{out_path}")

    print("\nTop 20:")
    print(df_out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
