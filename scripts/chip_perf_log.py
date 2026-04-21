#!/usr/bin/env python3
"""
筹码每日胜率记录器
每日收盘后（17:15）运行，统计当日 cad/cadm 筛选结果的各档表现，
追加写入 data/chip_daily_perf.json。

用法：
    python -X utf8 scripts/chip_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
PERF_PATH = ROOT / "data" / "chip_daily_perf.json"

CAD_GLOB  = "chip_cad_????????.json"    # cad  (bekh)  dated files
CADM_GLOB = "chip_cadm_????????.json"  # cadm (bekhm) dated files

TIER_ORDER = ["T4", "T1", "T2", "T3", "T5"]   # display order matches cad


def _fetch_prices(codes: list[str], retries: int = 3) -> dict[str, float]:
    import akshare as ak
    for attempt in range(1, retries + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            df = df[df["代码"].isin(codes)].copy()
            result: dict[str, float] = {}
            for _, row in df.iterrows():
                code = str(row["代码"]).zfill(6)
                try:
                    pct = float(row["涨跌幅"])
                    if not math.isnan(pct):
                        result[code] = pct
                except Exception:
                    pass
            return result
        except Exception as e:
            print(f"[perf] 行情获取失败（第{attempt}次）: {e}")
            if attempt < retries:
                time.sleep(5)
    return {}


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
        key=lambda x: x["pct"], reverse=True
    )[:3]
    return {"n": len(rets), "win_rate": win_rate, "avg_ret": avg_ret, "top3": top3}


def _compute_block(scan: dict, prices: dict[str, float]) -> dict:
    tiers_data = scan.get("tiers", {})
    tiers_out: dict[str, dict] = {}
    total_rets: list[float] = []
    for tier in TIER_ORDER:
        picks = tiers_data.get(tier, [])
        stats = _tier_stats(picks, prices)
        tiers_out[tier] = stats
        if stats["avg_ret"] is not None:
            total_rets.extend(prices[p["code"]] for p in picks if p["code"] in prices)
    result = {"mods": scan.get("mods", ""), "tiers": tiers_out}
    if total_rets:
        result["total_n"]        = len(total_rets)
        result["total_win_rate"] = round(sum(1 for r in total_rets if r > 0) / len(total_rets) * 100, 1)
        result["total_avg_ret"]  = round(sum(total_rets) / len(total_rets), 2)
    return result


def _format_block(label: str, block: dict) -> str:
    mods  = block.get("mods", "")
    total = block.get("total_n", 0)
    lines = [f"**{label}** ({mods})  共{total}只\n"]
    for tier in TIER_ORDER:
        s = block["tiers"].get(tier, {})
        if not s or s.get("win_rate") is None:
            lines.append(f"{tier}: 无数据")
            continue
        wr_emoji = "🟢" if s["win_rate"] >= 50 else "🔴"
        ar_s     = f"{s['avg_ret']:+.2f}%"
        lines.append(f"{wr_emoji} {tier} ({s['n']}只)  胜率 **{s['win_rate']}%**  均 {ar_s}")
        top = "  ".join(f"{t['name']}{t['pct']:+.1f}%" for t in s["top3"])
        if top:
            lines.append(f"  ↑ {top}")
    if "total_win_rate" in block:
        wr   = block["total_win_rate"]
        ar   = block["total_avg_ret"]
        emoji = "🟢" if wr >= 50 else "🔴"
        lines.append(f"\n{emoji} 全档 胜率 **{wr}%**  均 {ar:+.2f}%")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true", help="跳过时间窗口检查并覆盖今日已有记录")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force:
        hm = now.hour * 60 + now.minute
        if hm < 15 * 60 + 10:
            print(f"[perf] 当前 {now:%H:%M}，需 15:10 后运行，跳过")
            return

    today = now.strftime("%Y%m%d")

    def _find_prev(glob_pat: str) -> dict | None:
        """Return the most recent dated file with date strictly before today."""
        candidates = sorted(
            (p for p in (ROOT / "data").glob(glob_pat) if p.stem[-8:] < today),
            key=lambda p: p.stem[-8:], reverse=True,
        )
        if not candidates:
            return None
        try:
            return json.loads(candidates[0].read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[perf] 读取 {candidates[0].name} 失败: {e}")
            return None

    # Load available scan files (previous trading day's picks)
    scans: list[tuple[str, dict]] = []
    for label, glob_pat in [("cad", CAD_GLOB), ("cadm", CADM_GLOB)]:
        s = _find_prev(glob_pat)
        if s:
            scans.append((label, s))
            print(f"[perf] 读取 {label}: 日期={s.get('date')}  mods={s.get('mods','?')}")
        else:
            print(f"[perf] {label}: 无前日文件，跳过")

    if not scans:
        print("[perf] 今日 cad/cadm 均未运行，无数据可记录")
        return

    # Collect all codes
    all_codes: set[str] = set()
    for _, s in scans:
        for tier_picks in s.get("tiers", {}).values():
            all_codes.update(p["code"] for p in tier_picks)

    print(f"[perf] 获取 {len(all_codes)} 只股票行情 ...")
    prices = _fetch_prices(list(all_codes))
    print(f"[perf] 获取到 {len(prices)} 只")

    record: dict = {
        "date":   today,
        "logged": now.isoformat(timespec="seconds"),
    }

    push_blocks: list[str] = []
    for label, scan in scans:
        block = _compute_block(scan, prices)
        record[label] = block
        push_blocks.append(_format_block(label, block))
        # Console summary
        for tier in TIER_ORDER:
            s  = block["tiers"].get(tier, {})
            n  = s.get("n", 0)
            wr = f"{s['win_rate']}%" if s.get("win_rate") is not None else "-"
            ar = f"{s['avg_ret']:+.2f}%" if s.get("avg_ret") is not None else "-"
            print(f"  [{label}] {tier}: {n}只  胜率{wr}  均涨{ar}")
        if "total_win_rate" in block:
            print(f"  [{label}] 全档: {block['total_n']}只  胜率{block['total_win_rate']}%  均涨{block['total_avg_ret']:+.2f}%")

    date_fmt  = f"{today[4:6]}/{today[6:]}"
    push_body = f"## 📊 筹码胜率 {date_fmt}\n\n" + "\n\n---\n\n".join(push_blocks) + "\n\n⚠️ 仅供参考，不构成投资建议"
    print(f"\n{push_body}\n")

    if args.dry_run:
        print("[perf] dry-run，不写入文件")
        return

    # Dedup: remove existing record for today if --force, else skip
    existing: list[dict] = []
    if PERF_PATH.exists():
        existing = json.loads(PERF_PATH.read_text(encoding="utf-8"))
    if any(r["date"] == today for r in existing):
        if args.force:
            existing = [r for r in existing if r["date"] != today]
            print(f"[perf] --force：覆盖 {today} 已有记录")
        else:
            print(f"[perf] {today} 已记录，跳过（使用 --force 覆盖）")
            return

    existing.append(record)
    PERF_PATH.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[perf] 已写入 {PERF_PATH.name}（共 {len(existing)} 条记录）")

    # WeChat push
    try:
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from common import send_wechat, configure_pushplus
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

        # Build short title
        parts = []
        for label, _ in scans:
            b  = record.get(label, {})
            wr = b.get("total_win_rate", "-")
            ar = b.get("total_avg_ret")
            ar_s = f"{ar:+.2f}%" if ar is not None else "-"
            parts.append(f"{label}胜率{wr}% 均{ar_s}")
        title = f"筹码胜率 {date_fmt} | {' / '.join(parts)}"
        send_wechat(title, push_body, sendkey)
        print("[perf] 微信推送成功")
    except Exception as e:
        print(f"[perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
