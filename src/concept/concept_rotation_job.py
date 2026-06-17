#!/usr/bin/env python3
"""
concept_rotation_job.py — 概念轮动定时任务
功能：
  1. 跑concept_rotation打分
  2. 结果推送飞书（卡片格式）
  3. 保存历史JSON到 data/concept_history/ 方便回溯

定时计划：
  08:50 盘前 (evening模式，用昨天数据预判今天)
  10:30 盘中第一次
  12:20 午间
  14:20 盘中第二次
  16:00 收盘复盘

用法：
  python -X utf8 src/concept/concept_rotation_job.py [--mode evening|intraday]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "concept"))

from concept_rotation import concept_rotation, WEIGHTS_EVENING, WEIGHTS_INTRADAY, _format_inflow, save_daily_change
from notify.notify import push_feishu_card


HISTORY_DIR = ROOT / "data" / "concept_history"


def _auto_mode() -> str:
    """根据当前时间自动选择模式"""
    now = datetime.now()
    hour = now.hour
    # 9:15前或15:30后用evening模式（盘后/盘前）
    if hour < 9 or (hour == 9 and now.minute < 15) or hour >= 16:
        return "evening"
    return "intraday"


def _save_history(result: dict, mode: str) -> Path:
    """保存到 data/concept_history/YYYYMMDD/HHMM_{mode}.json"""
    now = datetime.now()
    day_dir = HISTORY_DIR / now.strftime("%Y%m%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{now.strftime('%H%M')}_{mode}.json"
    out_path = day_dir / filename
    out_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8"
    )
    return out_path


def _build_feishu_lines(result: dict) -> list[str]:
    """构建飞书推送的文本行"""
    lines = []
    ts = result["timestamp"]
    mode_cn = result["mode_cn"]
    total = result["total_concepts"]

    lines.append(f"📊 概念轮动 [{mode_cn}] {ts}")
    lines.append(f"共{total}个概念板块")
    lines.append("")

    # 强势轮入 Top5
    if result.get("strong_entry"):
        lines.append("🔥 强势轮入:")
        for c in result["strong_entry"][:5]:
            inflow = _format_inflow(c['net_inflow'])
            lines.append(
                f"  {c['name']} {c['score']:.0f}分 "
                f"{c['pct_chg']:+.1f}% 3日{c['trend_3d']:+.1f}% "
                f"量比{c['volume_ratio']:.1f} 主力{inflow} "
                f"龙头:{c['leader_name']}"
            )
        lines.append("")

    # 蓄势待发
    if result.get("preparing"):
        lines.append("🌱 蓄势待发:")
        for c in result["preparing"][:5]:
            inflow = _format_inflow(c['net_inflow'])
            lines.append(
                f"  {c['name']} {c['score']:.0f}分 "
                f"{c['pct_chg']:+.1f}% 量比{c['volume_ratio']:.1f} "
                f"主力{inflow} 龙头:{c['leader_name']}"
            )
        lines.append("")

    # 获利了结警告
    if result.get("take_profit"):
        lines.append("⚠️ 过热/获利了结:")
        for c in result["take_profit"][:5]:
            inflow = _format_inflow(c['net_inflow'])
            lines.append(
                f"  {c['name']} 3日{c['trend_3d']:+.1f}% "
                f"今日{c['pct_chg']:+.1f}% 主力{inflow}"
            )
        lines.append("")

    # Top10排名
    lines.append("📈 综合Top10:")
    for i, c in enumerate(result.get("full_ranking", [])[:10], 1):
        inflow = _format_inflow(c['net_inflow'])
        lines.append(
            f"  {i:>2}. {c['name']} {c['score']:.0f}分 "
            f"{c['pct_chg']:+.1f}% 3日{c['trend_3d']:+.1f}% "
            f"量比{c['volume_ratio']:.1f} 广度{c['breadth']*100:.0f}% "
            f"主力{inflow} 龙头:{c['leader_name']}"
        )

    return lines


def main():
    parser = argparse.ArgumentParser(description="概念轮动定时任务")
    parser.add_argument("--mode", choices=["evening", "intraday", "auto"], default="auto")
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--no-push", action="store_true", help="不推送飞书")
    parser.add_argument("--no-save", action="store_true", help="不保存历史")
    args = parser.parse_args()

    mode = args.mode if args.mode != "auto" else _auto_mode()
    print(f"[concept_job] 开始运行 mode={mode} time={datetime.now().strftime('%H:%M:%S')}")

    # 跑评分
    result = concept_rotation(mode=mode, top_n=args.top, use_proxy=True)
    if not result:
        # API不通，降级用缓存recalc
        print("[concept_job] ⚠️ API不通，尝试从缓存recalc...")
        from concept_rotation import recalc_from_cache
        result = recalc_from_cache(mode=mode, top_n=args.top)
        if not result:
            print("[concept_job] ❌ recalc也失败，无缓存可用")
            sys.exit(1)
        result["mode_cn"] = result.get("mode_cn", "") + " (缓存降级)"

    # 保存历史
    if not args.no_save:
        hist_path = _save_history(result, mode)
        print(f"[concept_job] 历史已保存: {hist_path}")

    # 也保存一份到 data/ 作为最新快照（供其他模块读取）
    suffix = "evening" if mode == "evening" else "intraday"
    snap_path = ROOT / "data" / f"concept_rotation_{suffix}.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8"
    )

    # 推送飞书
    if not args.no_push:
        lines = _build_feishu_lines(result)
        title = f"概念轮动 [{result['mode_cn']}] {datetime.now().strftime('%H:%M')}"
        push_feishu_card(title, lines)
        print(f"[concept_job] 飞书推送完成")

    # 打印摘要
    print(f"[concept_job] ✅ 完成 | Top3: ", end="")
    for c in result["full_ranking"][:3]:
        print(f"{c['name']}({c['score']:.0f}) ", end="")
    print()

    # 16:00收盘后，保存当日涨幅供3日趋势自算
    now = datetime.now()
    if now.hour >= 15 and mode == "evening":
        full_ranking = result.get("full_ranking", [])
        if full_ranking and full_ranking[0].get("pct_chg") is not None:
            save_daily_change(full_ranking, now.strftime("%Y%m%d"))

    # 如果有获利了结信号，额外打印
    if result.get("take_profit"):
        print(f"[concept_job] ⚠️ 获利了结信号: ", end="")
        for c in result["take_profit"][:3]:
            print(f"{c['name']}(3日{c['trend_3d']:+.1f}%) ", end="")
        print()


if __name__ == "__main__":
    main()
