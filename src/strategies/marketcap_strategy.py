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

TOP_N      = 20
MIN_PRICE  = 2.0      # 股价门槛（严格大于）


# ── 市值数据获取 ───────────────────────────────────────────────────────────────


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
    from strategies._quality import load_marketcap_cache, inject_marketcap
    cached = load_marketcap_cache()
    if cached and spot is not None and not spot.empty:
        return inject_marketcap(spot, cached), False

    if spot is None or spot.empty:
        print("[marketcap] 行情数据不可用")
        return pd.DataFrame(), False

    # EM 和磁盘缓存都没有，尝试 BaoStock 计算流通市值作为代理
    print("[marketcap] 尝试 BaoStock 获取流通市值（非交易时段备用，约需 1-2 分钟）")
    bs_mv = _get_marketcap_from_baostock(spot)
    if bs_mv:
        spot = inject_marketcap(spot, bs_mv, verbose=False)
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
    df["_code6"] = df["代码"].astype(str).apply(fetcher.normalize_code)
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

def _build_candidates_from_cache() -> pd.DataFrame:
    """spot 不可用时（akshare EM 端 RemoteDisconnected 等），用 marketcap_cache +
    name_industry_map 构造候选清单。只在 as_of_date 回填模式下用——live 仍走
    spot 才能拿当前价/涨跌幅。

    构造的 df 没有 "最新价" / "涨跌幅" 列，scan() 必须从 history 拉。
    """
    from strategies._quality import load_marketcap_cache, load_name_industry_map
    cached = load_marketcap_cache()
    if not cached:
        return pd.DataFrame()
    name_map, _ = load_name_industry_map()
    rows = []
    for code6, mv in cached.items():
        rows.append({
            "代码": code6,
            "名称": name_map.get(code6, code6),
            "总市值": float(mv),
        })
    df = pd.DataFrame(rows)
    print(f"[marketcap] spot 不可用，回填走 marketcap_cache fallback ({len(df)} 候选)")
    return df


def scan(as_of_date: str = "") -> list[dict]:
    """返回市值最低 TOP_N 只。

    as_of_date='YYYYMMDD' 时回填：用 close_at_D × 隐含股本 重建当日市值，
    隐含股本 = 当前总市值 / 当前价（短期内股本相对稳定，分红/送转/增发误差可控）。
    ST/科创/北证过滤用当前名单当代理（历史 ST 状态难恢复，影响样本极少）。
    """
    spot, _ = get_spot_with_marketcap()
    spot_ok = (not spot.empty) and "总市值" in spot.columns
    if not spot_ok:
        if as_of_date:
            spot = _build_candidates_from_cache()
            spot_ok = not spot.empty
        if not spot_ok:
            print("[marketcap] 无法执行筛选，缺少必要数据")
            return []

    df = spot.copy()
    # 规一化代码：Sina 数据带 sh/sz/bj 前缀，EM 数据是纯 6 位
    df["代码"] = df["代码"].astype(str).apply(fetcher.normalize_code)

    # 过滤 ST / 退市
    df = df[~df["名称"].str.contains("ST|退", na=False)]
    # 过滤科创板 (688xxx)
    df = df[~df["代码"].str.startswith("688")]
    # 过滤北证 (8xxxxx / 43xxxx / 9xxxxx for 920xxx)
    df = df[~(df["代码"].str.startswith("8") | df["代码"].str.startswith("43") | df["代码"].str.startswith("9"))]
    # 过滤市值异常
    mv_col = pd.to_numeric(df["总市值"], errors="coerce")
    df = df[mv_col > 0].copy()
    df["_curr_mv"] = pd.to_numeric(df["总市值"], errors="coerce")

    has_live_price = "最新价" in df.columns
    if has_live_price:
        df["_curr_price"] = pd.to_numeric(df["最新价"], errors="coerce")
        df = df[df["_curr_price"] > 0].copy()
    else:
        df["_curr_price"] = float("nan")  # 由 _enrich 用 history 推算

    if as_of_date:
        # 回填模式：先按当前市值预筛 top 500 候选（小盘股池短期内不会剧烈漂移），
        # 再逐个拉历史算 as-of 市值排序
        df = df.nsmallest(500, "_curr_mv")
    else:
        # live：必须有最新价做价格过滤
        if not has_live_price:
            print("[marketcap] live 模式无最新价，无法过滤")
            return []
        df = df[df["_curr_price"] > MIN_PRICE]
        df = df.nsmallest(TOP_N * 3, "_curr_mv")

    # 流动性 / 量能 enrichment + 行业黑名单 + as-of 价格/市值
    import fetcher as _f
    from strategies._quality import compute_metrics, passes_quality, is_blacklisted, load_name_industry_map
    from concurrent.futures import ThreadPoolExecutor as _TPE

    _, _ind_map = load_name_industry_map()

    def _enrich(row) -> dict | None:
        code = str(row["代码"])
        code6 = code[-6:]
        if is_blacklisted(_ind_map.get(code6, "")):
            return None
        try:
            # 回填模式拉 120 天给 slice 留 buffer；live 只拉 65 天足够 quality
            fetch_days = 120 if as_of_date else 65
            ddf = _f.get_price_history(code6, days=fetch_days)
            if ddf is None or ddf.empty:
                return None
        except Exception:
            return None

        if as_of_date:
            cutoff_ts = pd.to_datetime(as_of_date, format="%Y%m%d")
            ddf_full = ddf  # 完整 history 用来推 implied shares（最新 close）
            ddf = ddf[ddf["date"] <= cutoff_ts]
            if ddf.empty:
                return None
            last_close = float(ddf["close"].iloc[-1])
            last_pct = float(ddf["pct_chg"].iloc[-1]) if "pct_chg" in ddf.columns else 0.0
            curr_mv = float(row["_curr_mv"])
            # 推算 implied shares：优先用 row["_curr_price"]（spot 路径），否则用
            # history 最末日（fallback 路径）— 都代表"当前/最近"价
            curr_price = float(row.get("_curr_price") or 0)
            if not (curr_price > 0):
                curr_price = float(ddf_full["close"].iloc[-1])
            # as-of 市值 = 当前市值 × (当日 close / 当前价) — 短期内股本近似不变
            mv_at_d = curr_mv * (last_close / curr_price) if curr_price > 0 else 0
            if last_close <= MIN_PRICE or mv_at_d <= 0:
                return None
            price_use = last_close
            change_use = last_pct
            mv_yi = mv_at_d / 1e8
        else:
            price_use = float(row["_curr_price"])
            change_use = float(pd.to_numeric(row.get("涨跌幅", 0), errors="coerce") or 0)
            mv_yi = float(row["_curr_mv"]) / 1e8

        m = compute_metrics(ddf, code6)
        if not passes_quality(m):
            return None
        return {
            "code":         code6,
            "name":         row["名称"],
            "industry":     _ind_map.get(code6, ""),
            "price":        round(price_use, 2),
            "change_pct":   round(change_use, 2),
            "marketcap_yi": round(mv_yi, 2),
            "amt_5d_yi":    m["amt_5d_yi"],
            "vol_ratio":    m["vol_ratio"],
        }

    rows = list(df.to_dict("records"))
    with _TPE(max_workers=10) as ex:
        enriched = list(ex.map(_enrich, rows))
    candidates = [r for r in enriched if r is not None]
    # 回填模式按 as-of 市值升序再取 TOP_N（_enrich 出来还是预筛序）
    if as_of_date:
        candidates.sort(key=lambda r: r["marketcap_yi"])
    results = candidates[:TOP_N]

    print(f"[marketcap] 筛选完成，共 {len(results)} 只（候选 {len(rows)}，过质量门槛 {len(candidates)} 只）")
    return results


# ── 持久化 ────────────────────────────────────────────────────────────────────

def save_results(picks: list[dict], as_of_date: str = "") -> None:
    date_str = as_of_date or datetime.now().strftime("%Y%m%d")
    payload = {
        "date":      date_str,
        "timestamp": datetime.now().isoformat(),
        "picks":     picks,
    }

    # dated 归档（始终写，供 strategy_replay）
    dated_path = ROOT / "data" / f"marketcap_{date_str}.json"
    try:
        tmp = str(dated_path) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, dated_path)
    except Exception as e:
        print(f"[marketcap] dated 归档失败: {e}")

    # latest 只在 live 模式写，避免回填覆盖当日
    if as_of_date:
        print(f"[marketcap] 已保存 {len(picks)} 只 → marketcap_{date_str}.json")
        return
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
    parser.add_argument("--date",     type=str, default="", help="回填模式 YYYYMMDD（按该日 as-of 扫描，存 marketcap_<date>.json，不覆盖 latest）")
    parser.add_argument("--backfill", type=int, default=0,  help="回填最近 N 个交易日，跑完退出（不推送）")
    args = parser.parse_args()

    config_path = ROOT / "alert_config.json"
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    sendkey = setup_push(config)

    if args.backfill > 0:
        from datetime import date as _d, timedelta as _td
        dates = []
        cur = _d.today()
        while len(dates) < args.backfill:
            cur -= _td(days=1)
            if cur.weekday() < 5:
                dates.append(cur.strftime("%Y%m%d"))
        for ds in dates:
            print(f"\n=== backfill {ds} ===")
            picks = scan(as_of_date=ds)
            if picks:
                save_results(picks, as_of_date=ds)
            else:
                # 写空 picks dated 文件，标记当日扫了无结果
                save_results([], as_of_date=ds)
        return

    picks = scan(as_of_date=args.date)
    if not picks and not args.date:
        print("[marketcap] 无结果，退出")
        return

    if not args.dry_run:
        save_results(picks, as_of_date=args.date)

    if args.date:
        # 回填单日模式不推送
        print(f"[marketcap] {args.date} 完成，{len(picks)} 只")
        return

    rows = [f"*{datetime.now():%Y-%m-%d %H:%M}*<br>**市值最低 {TOP_N} 只**（排除ST/科创/北证/≤{MIN_PRICE}元）"]
    for i, p in enumerate(picks, 1):
        pct_s = f" {p['change_pct']:+.2f}%" if p['change_pct'] else ""
        ind_s = f" ({p['industry']})" if p.get("industry") else ""
        rows.append(f"{i}. **{p['code']} {p['name']}**{ind_s}  ¥{p['price']:.2f}  {p['marketcap_yi']:.1f}亿{pct_s}")
    rows.append("<br>> 仅供参考，不构成投资建议")
    desp  = "<br>".join(rows)
    title = f"[市值] 最低{TOP_N}只"

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
