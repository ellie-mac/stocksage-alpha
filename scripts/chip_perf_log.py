#!/usr/bin/env python3
"""
筹码每日胜率记录器
每日 17:15 运行，读取前一日 cah/cadm/cad 扫描结果，统计今日涨幅表现。

分组（T1-T4）：
  三者共有 — cadm T1-T4（三筛俱过，最高置信）
  cah 独有 — cah T1-T4 中不在 cad 里的（宽筛，未过 BOLL/价格/科创）
  cad 独有 — cad T1-T4 中不在 cadm 里的（BOLL 等通过，MACD 未收敛）

用法：
    python -X utf8 scripts/chip_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
PERF_PATH = ROOT / "data" / "chip_daily_perf.json"
DATA_DIR  = ROOT / "data"

TOP_TIERS = ["T1", "T2", "T3", "T4"]


def _fetch_prices(codes: list[str]) -> dict[str, float]:
    import sys
    import pandas as pd
    sys.path.insert(0, str(Path(__file__).parent))
    from common import get_spot_em
    df = get_spot_em()
    if df.empty:
        return {}
    df["_code"] = df["代码"].astype(str).str.zfill(6)
    df = df[df["_code"].isin(codes)].copy()
    df["_pct"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
    df = df.dropna(subset=["_pct"])
    return dict(zip(df["_code"], df["_pct"]))


def _tier_stats(picks: list[dict], prices: dict[str, float]) -> dict:
    rets = [prices[p["code"]] for p in picks if p["code"] in prices]
    if not rets:
        return {"n": 0, "win_rate": None, "avg_ret": None, "top3": []}
    n_win    = sum(1 for r in rets if r > 0)
    win_rate = round(n_win / len(rets) * 100, 1)
    avg_ret  = round(sum(rets) / len(rets), 2)
    top3 = sorted(
        [{"code": p["code"], "name": p.get("name", ""), "pct": prices[p["code"]]}
         for p in picks if p["code"] in prices],
        key=lambda x: x["pct"], reverse=True,
    )[:3]
    return {"n": len(rets), "win_rate": win_rate, "avg_ret": avg_ret, "top3": top3}


def _compute_group(picks_by_tier: dict[str, list[dict]], prices: dict[str, float]) -> dict:
    tiers_out: dict[str, dict] = {}
    total_rets: list[float] = []
    for tier in TOP_TIERS:
        picks = picks_by_tier.get(tier, [])
        stats = _tier_stats(picks, prices)
        tiers_out[tier] = stats
        if stats["avg_ret"] is not None:
            total_rets.extend(prices[p["code"]] for p in picks if p["code"] in prices)
    result: dict = {"tiers": tiers_out}
    if total_rets:
        result["total_n"]        = len(total_rets)
        result["total_win_rate"] = round(sum(1 for r in total_rets if r > 0) / len(total_rets) * 100, 1)
        result["total_avg_ret"]  = round(sum(total_rets) / len(total_rets), 2)
    return result


def _find_prev(glob_pat: str, today: str) -> dict | None:
    candidates = sorted(
        (p for p in DATA_DIR.glob(glob_pat) if p.stem[-8:] < today),
        key=lambda p: p.stem[-8:], reverse=True,
    )
    if not candidates:
        return None
    try:
        return json.loads(candidates[0].read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[perf] 读取 {candidates[0].name} 失败: {e}")
        return None


def _summary_row(label: str, block: dict) -> str:
    n  = block.get("total_n", 0)
    wr = block.get("total_win_rate")
    ar = block.get("total_avg_ret")
    wr_s = f"{wr}%" if wr is not None else "-"
    ar_s = f"{ar:+.2f}%" if ar is not None else "-"
    return f"| {label} | {n} | {wr_s} | {ar_s} |"


def _detail_block(label: str, block: dict) -> str:
    total = block.get("total_n", 0)
    lines = [f"**{label}**  共{total}只\n"]
    for tier in TOP_TIERS:
        s = block["tiers"].get(tier, {})
        if not s or s.get("win_rate") is None:
            continue
        emoji = "🟢" if s["win_rate"] >= 50 else "🔴"
        lines.append(f"{emoji} {tier} ({s['n']}只)  胜率 **{s['win_rate']}%**  均 {s['avg_ret']:+.2f}%")
        top = "  ".join(f"{t['name']}{t['pct']:+.1f}%" for t in s["top3"])
        if top:
            lines.append(f"  ↑ {top}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true", help="跳过时间/去重检查")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force:
        hm = now.hour * 60 + now.minute
        if hm < 15 * 60 + 10:
            print(f"[perf] 当前 {now:%H:%M}，需 15:10 后运行，跳过")
            return

    today = now.strftime("%Y%m%d")

    # Load previous-day scan files
    cah_scan  = _find_prev("chip_cah_????????.json",  today)
    cadm_scan = _find_prev("chip_cadm_????????.json", today)
    cad_scan  = _find_prev("chip_cad_????????.json",  today)

    for name, scan in [("cah", cah_scan), ("cadm", cadm_scan), ("cad", cad_scan)]:
        if scan:
            print(f"[perf] {name}: 前日={scan.get('date')}  mods={scan.get('mods') or '全档'}")
        else:
            print(f"[perf] {name}: 无前日文件")

    if not cadm_scan and not cad_scan:
        print("[perf] cadm/cad 均无前日数据，退出")
        return

    # Build T1-T4 code sets for deduplication
    def _codes(scan: dict | None) -> set[str]:
        if not scan:
            return set()
        return {p["code"] for t in TOP_TIERS for p in scan.get("tiers", {}).get(t, [])}

    cad_codes  = _codes(cad_scan)
    cadm_codes = _codes(cadm_scan)

    def _tier_picks(scan: dict | None) -> dict[str, list[dict]]:
        if not scan:
            return {t: [] for t in TOP_TIERS}
        return {t: scan.get("tiers", {}).get(t, []) for t in TOP_TIERS}

    # Group 1: 三者共有 = cadm T1-T4
    g_cadm = _tier_picks(cadm_scan)

    # Group 2: cah独有 = cah T1-T4 不在 cad 里
    g_cah_only = {
        t: [p for p in _tier_picks(cah_scan).get(t, []) if p["code"] not in cad_codes]
        for t in TOP_TIERS
    }

    # Group 3: cad独有 = cad T1-T4 不在 cadm 里
    g_cad_only = {
        t: [p for p in _tier_picks(cad_scan).get(t, []) if p["code"] not in cadm_codes]
        for t in TOP_TIERS
    }

    groups = [
        ("三者共有", g_cadm),
        ("cah独有",  g_cah_only),
        ("cad独有",  g_cad_only),
    ]

    # Fetch prices once for all codes
    all_codes: set[str] = set()
    for _, picks_by_tier in groups:
        for picks in picks_by_tier.values():
            all_codes.update(p["code"] for p in picks)
    print(f"[perf] 获取 {len(all_codes)} 只股票行情 ...")
    prices = _fetch_prices(list(all_codes))
    print(f"[perf] 获取到 {len(prices)} 只")

    # Compute blocks
    blocks: list[tuple[str, dict]] = []
    for label, picks_by_tier in groups:
        block = _compute_group(picks_by_tier, prices)
        blocks.append((label, block))
        n  = block.get("total_n", 0)
        wr = block.get("total_win_rate", "-")
        ar = block.get("total_avg_ret")
        ar_s = f"{ar:+.2f}%" if ar is not None else "-"
        print(f"  [{label}] {n}只  胜率{wr}%  均涨{ar_s}")

    # Build record
    record: dict = {"date": today, "logged": now.isoformat(timespec="seconds")}
    for label, block in blocks:
        record[label] = block

    # Build push
    date_fmt = f"{today[4:6]}/{today[6:]}"
    lines = []

    lines.append("| 分组 | 只数 | 胜率 | 均涨 |")
    lines.append("|------|-----:|-----:|-----:|")
    for label, block in blocks:
        lines.append(_summary_row(label, block))

    for label, block in blocks:
        lines.append(f"\n\n---\n\n{_detail_block(label, block)}")

    lines.append("\n\n⚠️ 仅供参考，不构成投资建议")
    push_body = "\n".join(lines)
    print(f"\n{push_body}\n")

    if args.dry_run:
        print("[perf] dry-run，不写入")
        return

    # Dedup
    existing: list[dict] = []
    if PERF_PATH.exists():
        existing = json.loads(PERF_PATH.read_text(encoding="utf-8"))
    if any(r["date"] == today for r in existing):
        if args.force:
            existing = [r for r in existing if r["date"] != today]
        else:
            print(f"[perf] {today} 已记录，跳过（--force 覆盖）")
            return

    existing.append(record)
    PERF_PATH.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[perf] 已写入（共 {len(existing)} 条）")

    # WeChat push
    try:
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from common import send_wechat, configure_pushplus
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        parts = [
            f"{lbl}{b.get('total_win_rate','-')}%"
            for lbl, b in blocks if b.get("total_win_rate") is not None
        ]
        title = f"筹码胜率 T1-T4 {date_fmt} | {' / '.join(parts)}"
        send_wechat(title, push_body, sendkey)
        print("[perf] 微信推送成功")
    except Exception as e:
        print(f"[perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
