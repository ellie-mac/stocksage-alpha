#!/usr/bin/env python3
"""
市值策略 — 全市场最低市值 20 只（排除 ST/科创/北证/股价≤2元）

用法：
    python -X utf8 src/strategies/marketcap_strategy.py
    python -X utf8 src/strategies/marketcap_strategy.py --dry-run
    python -X utf8 src/strategies/marketcap_strategy.py --push

市值数据优先级：
  1. 当日 EM spot 数据（含 总市值 列，盘中可用）
  2. 磁盘缓存 data/marketcap_cache.json（每次 EM 成功时刷新）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import fetcher
from common import configure_pushplus, send_wechat

_OUT_LATEST     = ROOT / "data" / "marketcap_latest.json"
_MARKETCAP_CACHE = ROOT / "data" / "marketcap_cache.json"
_CACHE_MAX_DAYS = 7   # 超过 7 天的缓存视为过期

TOP_N      = 20
MIN_PRICE  = 2.0      # 股价门槛（严格大于）


# ── 市值数据获取 ───────────────────────────────────────────────────────────────

def _load_cached_marketcap() -> dict[str, float]:
    """从磁盘缓存读取市值，返回 {6位代码: 总市值(元)}。"""
    if not _MARKETCAP_CACHE.exists():
        return {}
    try:
        raw = json.loads(_MARKETCAP_CACHE.read_text(encoding="utf-8"))
        from datetime import date, timedelta
        cache_date = raw.get("date", "")
        if cache_date:
            cutoff = (date.today() - timedelta(days=_CACHE_MAX_DAYS)).strftime("%Y%m%d")
            if cache_date < cutoff:
                print(f"[marketcap] 市值缓存过期({cache_date})，跳过")
                return {}
        return raw.get("data", {})
    except Exception as e:
        print(f"[marketcap] 读取市值缓存失败: {e}")
        return {}


def _save_marketcap_cache(code_mv: dict[str, float]) -> None:
    """将市值数据写入磁盘缓存。"""
    payload = {
        "date":      datetime.now().strftime("%Y%m%d"),
        "timestamp": datetime.now().isoformat(),
        "data":      code_mv,
    }
    tmp = str(_MARKETCAP_CACHE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, _MARKETCAP_CACHE)
        print(f"[marketcap] 市值缓存已更新 ({len(code_mv)} 只)")
    except Exception as e:
        print(f"[marketcap] 保存市值缓存失败: {e}")


def get_spot_with_marketcap() -> tuple[pd.DataFrame, bool]:
    """
    返回 (spot_df, has_fresh_marketcap)。
    has_fresh_marketcap=True 时 spot_df 含 总市值 列。
    """
    spot = fetcher._get_spot_df()
    if spot is not None and not spot.empty and "总市值" in spot.columns:
        # 顺便更新磁盘缓存
        mv_map = (
            spot[["代码", "总市值"]]
            .dropna(subset=["总市值"])
            .set_index("代码")["总市值"]
            .astype(float)
            .to_dict()
        )
        _save_marketcap_cache(mv_map)
        return spot, True

    # EM 不可用，走磁盘缓存
    cached = _load_cached_marketcap()
    if cached and spot is not None and not spot.empty:
        spot = spot.copy()
        spot["总市值"] = spot["代码"].map(cached)
        print(f"[marketcap] 使用磁盘缓存市值 ({len(cached)} 只)")
        return spot, False

    if spot is None or spot.empty:
        print("[marketcap] 行情数据不可用")
        return pd.DataFrame(), False

    print("[marketcap] 无市值数据（既无 EM 实时，也无磁盘缓存）")
    return spot, False


# ── 核心筛选 ──────────────────────────────────────────────────────────────────

def scan() -> list[dict]:
    """返回全市场市值最低 TOP_N 只（过滤后），按市值升序。"""
    spot, _ = get_spot_with_marketcap()
    if spot.empty or "总市值" not in spot.columns:
        print("[marketcap] 无法执行筛选，缺少必要数据")
        return []

    df = spot.copy()
    # 规一化代码：Sina 数据带 sh/sz/bj 前缀，EM 数据是纯 6 位
    df["代码"] = df["代码"].astype(str).apply(
        lambda c: c[2:] if len(c) > 6 and c[:2].isalpha() else c
    )

    # 过滤 ST / 退市
    df = df[~df["名称"].str.contains("ST|退", na=False)]
    # 过滤科创板 (688xxx)
    df = df[~df["代码"].str.startswith("688")]
    # 过滤北证 (8xxxxx / 43xxxx / 9xxxxx for 920xxx)
    df = df[~(df["代码"].str.startswith("8") | df["代码"].str.startswith("43") | df["代码"].str.startswith("9"))]
    # 过滤股价 ≤ 2 元
    price_col = pd.to_numeric(df["最新价"], errors="coerce")
    df = df[price_col > MIN_PRICE]
    # 过滤市值为空或零
    mv_col = pd.to_numeric(df["总市值"], errors="coerce")
    df = df[mv_col > 0].copy()
    df["_mv"] = pd.to_numeric(df["总市值"], errors="coerce")

    df = df.nsmallest(TOP_N, "_mv")

    results = []
    for _, row in df.iterrows():
        mv_yi = row["_mv"] / 1e8 if row["_mv"] > 0 else 0
        results.append({
            "code":      row["代码"],
            "name":      row["名称"],
            "price":     float(pd.to_numeric(row["最新价"], errors="coerce") or 0),
            "change_pct": float(pd.to_numeric(row.get("涨跌幅", 0), errors="coerce") or 0),
            "marketcap_yi": round(mv_yi, 2),
        })

    print(f"[marketcap] 筛选完成，共 {len(results)} 只")
    return results


# ── 持久化 ────────────────────────────────────────────────────────────────────

def save_results(picks: list[dict]) -> None:
    payload = {
        "date":      datetime.now().strftime("%Y%m%d"),
        "timestamp": datetime.now().isoformat(),
        "picks":     picks,
    }
    tmp = str(_OUT_LATEST) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, _OUT_LATEST)
        print(f"[marketcap] 已保存 {len(picks)} 只 → marketcap_latest.json")
    except Exception as e:
        print(f"[marketcap] 保存失败: {e}")


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--push",    action="store_true")
    args = parser.parse_args()

    config_path = ROOT / "alert_config.json"
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    configure_pushplus(config.get("pushplus", {}).get("token", ""))
    sendkey = config.get("serverchan", {}).get("sendkey", "")

    picks = scan()
    if not picks:
        print("[marketcap] 无结果，退出")
        return

    if not args.dry_run:
        save_results(picks)

    rows = [f"*{datetime.now():%Y-%m-%d %H:%M}*<br>**市值最低 {TOP_N} 只**（排除ST/科创/北证/≤{MIN_PRICE}元）"]
    for i, p in enumerate(picks, 1):
        pct_s = f" {p['change_pct']:+.2f}%" if p['change_pct'] else ""
        rows.append(f"{i}. **{p['code']} {p['name']}**  ¥{p['price']:.2f}  {p['marketcap_yi']:.1f}亿{pct_s}")
    rows.append("<br>> 仅供参考，不构成投资建议")
    desp  = "<br>".join(rows)
    title = f"市值策略 | 最低{TOP_N}只"

    if args.dry_run:
        print(f"\n[dry-run]\n{title}\n{desp}")
        return

    if args.push:
        try:
            send_wechat(title, desp, sendkey, dry_run=False)
            print("[marketcap] 微信推送完成")
        except Exception as e:
            print(f"[marketcap] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
