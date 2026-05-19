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
from common import send_wechat, setup_push

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
        data = raw.get("data", {})
        if cache_date:
            cutoff = (date.today() - timedelta(days=_CACHE_MAX_DAYS)).strftime("%Y%m%d")
            if cache_date < cutoff:
                if data:
                    print(f"[marketcap] 市值缓存已过期({cache_date})，使用旧数据({len(data)}只)继续")
                    return data
                print(f"[marketcap] 市值缓存过期({cache_date})且为空，跳过")
                return {}
        return data
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
        # 缓存 key 是 6 位纯数字；Sina spot 代码带 sh/sz 前缀，需先规一化
        norm_codes = spot["代码"].astype(str).apply(
            lambda c: c[2:] if len(c) > 6 and c[:2].isalpha() else c
        )
        spot["总市值"] = norm_codes.map(cached)
        print(f"[marketcap] 使用磁盘缓存市值 ({len(cached)} 只)")
        return spot, False

    if spot is None or spot.empty:
        print("[marketcap] 行情数据不可用")
        return pd.DataFrame(), False

    # EM 和磁盘缓存都没有，尝试 BaoStock 计算流通市值作为代理
    print("[marketcap] 尝试 BaoStock 获取流通市值（非交易时段备用，约需 1-2 分钟）")
    bs_mv = _get_marketcap_from_baostock(spot)
    if bs_mv:
        spot = spot.copy()
        # 规一化代码后再 map
        norm_codes = spot["代码"].astype(str).apply(
            lambda c: c[2:] if len(c) > 6 and c[:2].isalpha() else c
        )
        spot["总市值"] = norm_codes.map(bs_mv)
        _save_marketcap_cache(bs_mv)
        print(f"[marketcap] BaoStock 流通市值已获取 ({len(bs_mv)} 只)")
        return spot, False
    print("[marketcap] 无市值数据（EM/磁盘缓存/BaoStock 均不可用）")
    return spot, False


def _get_marketcap_from_baostock(spot: pd.DataFrame) -> dict[str, float]:
    """
    用 BaoStock 查上一交易日流通市值，返回 {6位代码: 市值(元)}。
    限制查询数量以控制运行时间（约 2-3 分钟）。
    """
    try:
        import baostock as bs
        from datetime import timedelta
    except ImportError:
        print("[marketcap] baostock 未安装，跳过")
        return {}

    # 筛选候选（和 scan() 相同过滤）
    df = spot.copy()
    df["_code6"] = df["代码"].astype(str).apply(
        lambda c: c[2:] if len(c) > 6 and c[:2].isalpha() else c
    )
    df = df[~df["名称"].str.contains("ST|退", na=False)]
    df = df[~df["_code6"].str.startswith("688")]
    df = df[~(df["_code6"].str.startswith("8") | df["_code6"].str.startswith("43") | df["_code6"].str.startswith("9"))]
    price_col = pd.to_numeric(df["最新价"], errors="coerce")
    df = df[price_col > MIN_PRICE]

    # 构建 BaoStock 格式代码列表（按价格升序，低价≈小市值，最多 1500 只）
    def _to_bs(c6):
        if c6.startswith("6") and not c6.startswith("688"):
            return f"sh.{c6}"
        if c6.startswith("0") or c6.startswith("3"):
            return f"sz.{c6}"
        return None

    df_sorted = df.copy()
    df_sorted["_price"] = pd.to_numeric(df_sorted["最新价"], errors="coerce")
    df_sorted = df_sorted.sort_values("_price", ascending=True)
    cands = [(row["_code6"], _to_bs(row["_code6"])) for _, row in df_sorted.iterrows() if _to_bs(row["_code6"])]
    cands = cands[:1500]
    if not cands:
        return {}

    # 最近交易日
    from datetime import datetime, timedelta as td
    d = datetime.now() - td(days=1)
    while d.weekday() >= 5:
        d -= td(days=1)
    last_td = d.strftime("%Y-%m-%d")
    print(f"[marketcap] BaoStock 查询 {len(cands)} 只，日期 {last_td}...")

    result: dict[str, float] = {}
    try:
        login_resp = bs.login()
        if login_resp.error_code != "0":
            print(f"[marketcap] BaoStock 登录失败: {login_resp.error_msg}")
            return {}
        for i, (code6, bs_code) in enumerate(cands, 1):
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code, "turn,amount",
                    start_date=last_td, end_date=last_td,
                    frequency="d", adjustflag="3",
                )
                if rs.data:
                    d_row = dict(zip(rs.fields, rs.data[0]))
                    t = float(d_row.get("turn") or 0)
                    a = float(d_row.get("amount") or 0)
                    if t > 0 and a > 0:
                        result[code6] = a / (t / 100)
            except Exception:
                pass
            if i % 100 == 0:
                print(f"[marketcap] BaoStock 进度 {i}/{len(cands)}")
    finally:
        bs.logout()
    return result


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

    # 多取一些候选给质量门槛筛（部分会被流动性/量比剔除）
    df = df.nsmallest(TOP_N * 3, "_mv")

    # 流动性 / 量能 enrichment + 行业黑名单
    import fetcher as _f
    from strategies._quality import compute_metrics, passes_quality, is_blacklisted
    from concurrent.futures import ThreadPoolExecutor as _TPE

    # 黑名单需要 industry 映射 — 从 stock_names.json 读
    _stock_names_path = ROOT / "data" / "stock_names.json"
    _ind_map: dict[str, str] = {}
    try:
        _raw = json.loads(_stock_names_path.read_text(encoding="utf-8"))
        _ind_map = {ts.split(".")[0]: (info.get("industry", "") if isinstance(info, dict) else "")
                    for ts, info in _raw.items()}
    except Exception:
        pass

    def _enrich(row) -> dict | None:
        code = str(row["代码"])
        code6 = code[-6:]
        if is_blacklisted(_ind_map.get(code6, "")):
            return None
        try:
            ddf = _f.get_price_history(code6, days=65)
        except Exception:
            return None
        m = compute_metrics(ddf)
        if not passes_quality(m):
            return None
        mv_yi = row["_mv"] / 1e8 if row["_mv"] > 0 else 0
        return {
            "code":         code6,
            "name":         row["名称"],
            "price":        float(pd.to_numeric(row["最新价"], errors="coerce") or 0),
            "change_pct":   float(pd.to_numeric(row.get("涨跌幅", 0), errors="coerce") or 0),
            "marketcap_yi": round(mv_yi, 2),
            "amt_5d_yi":    m["amt_5d_yi"],
            "vol_ratio":    m["vol_ratio"],
        }

    rows = list(df.to_dict("records"))
    with _TPE(max_workers=10) as ex:
        enriched = list(ex.map(_enrich, rows))
    results = [r for r in enriched if r is not None][:TOP_N]

    print(f"[marketcap] 筛选完成，共 {len(results)} 只（候选 {len(rows)}，过质量门槛后取 top {TOP_N}）")
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
    sendkey = setup_push(config)

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
