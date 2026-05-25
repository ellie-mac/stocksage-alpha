#!/usr/bin/env python3
"""
概念轮动回测追踪器 — 每日记录实验组+对照组，自动追踪T+1/T+3/T+5收益。

设计:
  实验组: AI精选5只/概念
  对照A: 概念内等权全买
  对照B: 只买东财龙头(f128)
  对照C: 概念内随机5只(跑10次取均值)

  同时记录两种成本:
    - 尾盘成本: 当日收盘价 (cost_close)
    - 开盘成本: 次日开盘价 (cost_open_t1)

用法:
  记录:  python -X utf8 src/concept/concept_backtest.py record [--date 2026-05-26]
  追踪:  python -X utf8 src/concept/concept_backtest.py track
  报告:  python -X utf8 src/concept/concept_backtest.py report [--days 20]

数据存储: data/concept_backtest.json
"""
from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"
BACKTEST_FILE = DATA / "concept_backtest.json"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
STOCK_FIELDS = "f2,f3,f6,f8,f12,f14,f17,f20"


def _get_session(use_proxy: bool = True) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    if use_proxy:
        proxy = "http://127.0.0.1:7890"
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _load_backtest() -> dict:
    if BACKTEST_FILE.exists():
        try:
            return json.loads(BACKTEST_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"records": [], "version": 1}


def _save_backtest(data: dict):
    DATA.mkdir(parents=True, exist_ok=True)
    BACKTEST_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _fetch_all_stocks_in_concept(session: requests.Session,
                                  block_code: str) -> list[dict]:
    """获取概念内所有个股(最多100只)"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1",
        "pz": "100", "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": f"b:{block_code}",
        "fields": STOCK_FIELDS,
    }
    try:
        r = session.get(url, params=params, timeout=15)
        items = r.json().get("data", {}).get("diff", [])
        stocks = []
        for s in items:
            price = s.get("f2", 0) or 0
            if price <= 0:
                continue
            stocks.append({
                "code": s.get("f12", ""),
                "name": s.get("f14", ""),
                "close": price,
                "open": s.get("f17", 0) or 0,
                "pct_chg": s.get("f3", 0) or 0,
                "amount": s.get("f6", 0) or 0,
                "turnover": s.get("f8", 0) or 0,
                "market_cap": s.get("f20", 0) or 0,
            })
        return stocks
    except Exception:
        return []


def _fetch_stock_price(session: requests.Session, code: str) -> dict | None:
    """获取单只股票当前价格"""
    # Determine market prefix
    if code.startswith("6"):
        secid = f"1.{code}"
    else:
        secid = f"0.{code}"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {"secid": secid, "fields": "f43,f44,f45,f46,f47,f48,f170"}
    try:
        r = session.get(url, params=params, timeout=8)
        data = r.json().get("data", {})
        return {
            "close": (data.get("f43", 0) or 0) / 100,  # 收盘价(分→元)
            "open": (data.get("f46", 0) or 0) / 100,
            "pct_chg": data.get("f170", 0) or 0,
        }
    except Exception:
        return None


def _fetch_stock_kline_prices(session: requests.Session, code: str,
                               days: int = 10) -> dict:
    """获取个股近N日K线 → 返回 {date: {close, open}}"""
    if code.startswith("6"):
        secid = f"1.{code}"
    elif code.startswith("0") or code.startswith("3"):
        secid = f"0.{code}"
    else:
        secid = f"0.{code}"
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", "fqt": "1",
        "beg": (datetime.now() - timedelta(days=days + 5)).strftime("%Y%m%d"),
        "end": datetime.now().strftime("%Y%m%d"),
        "lmt": str(days + 5),
    }
    try:
        r = session.get(url, params=params, timeout=10)
        klines = r.json().get("data", {}).get("klines", [])
        result = {}
        for k in klines:
            p = k.split(",")
            if len(p) >= 6:
                result[p[0]] = {
                    "open": float(p[1]),
                    "close": float(p[2]),
                }
        return result
    except Exception:
        return {}


# ── 记录命令 ─────────────────────────────────────────────────────────────────

def cmd_record(args):
    """记录当天的实验组和对照组"""
    # Load today's picks
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    date_file = date_str.replace("-", "")
    picks_file = DATA / f"concept_picks_{date_file}.json"

    if not picks_file.exists():
        print(f"[backtest] ❌ 找不到 {picks_file}")
        print(f"[backtest] 请先运行 concept_picker.py 生成选股结果")
        return

    picks_data = json.loads(picks_file.read_text(encoding="utf-8"))
    session = _get_session(use_proxy=not args.no_proxy)

    backtest = _load_backtest()
    # Check if already recorded
    if any(r["date"] == date_str for r in backtest["records"]):
        print(f"[backtest] ⚠️ {date_str} 已有记录, 跳过")
        return

    record = {
        "date": date_str,
        "mode": picks_data.get("mode", "evening"),
        "concepts": [],
    }

    for concept_data in picks_data.get("concepts", []):
        concept_name = concept_data["concept_name"]
        concept_code = concept_data["concept_code"]
        print(f"[backtest] 记录: {concept_name}")

        # Get all stocks for control groups
        all_stocks = _fetch_all_stocks_in_concept(session, concept_code)
        time.sleep(0.3)

        if not all_stocks:
            print(f"  跳过 {concept_name}: 无法获取成分股")
            continue

        # Experiment group: AI picks
        ai_picks = [
            {"code": p["code"], "name": p["name"], "cost_close": p["price"]}
            for p in concept_data.get("picks", [])
        ]

        # Control A: all stocks equally weighted (record avg close)
        control_a = [
            {"code": s["code"], "name": s["name"], "cost_close": s["close"]}
            for s in all_stocks
        ]

        # Control B: eastmoney leader (from rotation data)
        leader_code = concept_data.get("picks", [{}])[0].get("code", "") if concept_data.get("picks") else ""
        # Actually get leader from concept_rotation output
        # For now use the first stock (highest gain) as proxy, or find from rotation
        control_b = []
        # Find the leader from all_stocks by matching concept leader
        for s in all_stocks:
            if s["code"] == concept_data.get("concept_code", ""):
                control_b = [{"code": s["code"], "name": s["name"], "cost_close": s["close"]}]
                break
        # If leader not found, skip control B for this concept
        # We'll fix this by reading from rotation output

        # Control C: random 5 stocks (10 trials)
        random_trials = []
        tradeable = [s for s in all_stocks if s["amount"] and s["amount"] > 5e7]
        for _ in range(10):
            sample = random.sample(tradeable, min(5, len(tradeable)))
            random_trials.append([
                {"code": s["code"], "name": s["name"], "cost_close": s["close"]}
                for s in sample
            ])

        concept_record = {
            "concept_name": concept_name,
            "concept_code": concept_code,
            "concept_score": concept_data.get("concept_score", 0),
            "experiment": ai_picks,
            "control_a_count": len(control_a),
            "control_a_avg_close": (
                sum(s["cost_close"] for s in control_a) / len(control_a)
                if control_a else 0
            ),
            "control_b_leader": control_b,
            "control_c_random_trials": random_trials,
            # These will be filled by `track` command later
            "returns": {},
        }
        record["concepts"].append(concept_record)

    backtest["records"].append(record)
    _save_backtest(backtest)
    print(f"\n[backtest] ✅ {date_str} 已记录 {len(record['concepts'])} 个概念")


# ── 追踪命令 ─────────────────────────────────────────────────────────────────

def cmd_track(args):
    """追踪已记录的选股，填充T+1/T+3/T+5收益"""
    backtest = _load_backtest()
    session = _get_session(use_proxy=not args.no_proxy)

    today = datetime.now().strftime("%Y-%m-%d")
    updated = 0

    for record in backtest["records"]:
        record_date = record["date"]
        # Calculate trading days since record
        rd = datetime.strptime(record_date, "%Y-%m-%d")
        delta_days = (datetime.now() - rd).days

        if delta_days < 1:
            continue  # 至少需要1天后才能追踪

        for concept in record["concepts"]:
            returns = concept.get("returns", {})

            # Check which periods still need tracking
            periods_needed = []
            if delta_days >= 1 and "t1" not in returns:
                periods_needed.append(("t1", 1))
            if delta_days >= 3 and "t3" not in returns:
                periods_needed.append(("t3", 3))
            if delta_days >= 5 and "t5" not in returns:
                periods_needed.append(("t5", 5))

            if not periods_needed:
                continue

            print(f"[track] {record_date} | {concept['concept_name']} | "
                  f"追踪 {[p[0] for p in periods_needed]}")

            # Fetch price data for experiment picks
            experiment_returns = {}
            for period_name, period_days in periods_needed:
                exp_returns = []
                for pick in concept["experiment"]:
                    klines = _fetch_stock_kline_prices(
                        session, pick["code"], days=period_days + 3
                    )
                    if not klines:
                        continue
                    dates = sorted(klines.keys())
                    # Find record_date index
                    try:
                        idx = dates.index(record_date)
                    except ValueError:
                        # Find nearest date after record_date
                        idx = -1
                        for i, d in enumerate(dates):
                            if d >= record_date:
                                idx = i
                                break
                        if idx < 0:
                            continue

                    cost_close = klines[dates[idx]]["close"]
                    # T+N close
                    target_idx = idx + period_days
                    if target_idx < len(dates):
                        target_close = klines[dates[target_idx]]["close"]
                        # Also get T+1 open for open cost
                        open_t1 = klines[dates[idx + 1]]["open"] if idx + 1 < len(dates) else cost_close
                        ret_from_close = round((target_close / cost_close - 1) * 100, 2)
                        ret_from_open = round((target_close / open_t1 - 1) * 100, 2)
                        exp_returns.append({
                            "code": pick["code"],
                            "cost_close": cost_close,
                            "cost_open_t1": open_t1,
                            "exit_close": target_close,
                            "return_from_close": ret_from_close,
                            "return_from_open": ret_from_open,
                        })
                    time.sleep(0.1)

                if exp_returns:
                    avg_ret_close = round(
                        sum(r["return_from_close"] for r in exp_returns) / len(exp_returns), 2
                    )
                    avg_ret_open = round(
                        sum(r["return_from_open"] for r in exp_returns) / len(exp_returns), 2
                    )
                    win_rate = round(
                        sum(1 for r in exp_returns if r["return_from_close"] > 0) / len(exp_returns) * 100, 1
                    )
                    experiment_returns[period_name] = {
                        "avg_return_close": avg_ret_close,
                        "avg_return_open": avg_ret_open,
                        "win_rate": win_rate,
                        "details": exp_returns,
                    }

            if experiment_returns:
                concept["returns"].update(experiment_returns)
                updated += 1

    if updated:
        _save_backtest(backtest)
        print(f"\n[track] ✅ 更新了 {updated} 条追踪记录")
    else:
        print("[track] 没有需要更新的记录")


# ── 报告命令 ─────────────────────────────────────────────────────────────────

def cmd_report(args):
    """生成回测报告"""
    backtest = _load_backtest()
    records = backtest.get("records", [])

    if not records:
        print("[report] 无记录")
        return

    # Filter by days
    cutoff = datetime.now() - timedelta(days=args.days)
    recent = [r for r in records
              if datetime.strptime(r["date"], "%Y-%m-%d") >= cutoff]

    if not recent:
        print(f"[report] 最近{args.days}天无记录")
        return

    print(f"\n{'='*90}")
    print(f"  概念轮动回测报告  最近{args.days}天 ({len(recent)}条记录)")
    print(f"{'='*90}")

    # Aggregate stats
    all_t1_close = []
    all_t1_open = []
    all_t3_close = []
    all_t5_close = []

    for record in recent:
        for concept in record.get("concepts", []):
            returns = concept.get("returns", {})
            if "t1" in returns:
                all_t1_close.append(returns["t1"]["avg_return_close"])
                all_t1_open.append(returns["t1"]["avg_return_open"])
            if "t3" in returns:
                all_t3_close.append(returns["t3"]["avg_return_close"])
            if "t5" in returns:
                all_t5_close.append(returns["t5"]["avg_return_close"])

    def _stats(data: list[float], label: str):
        if not data:
            print(f"  {label}: 无数据")
            return
        avg = sum(data) / len(data)
        win = sum(1 for x in data if x > 0) / len(data) * 100
        max_gain = max(data)
        max_loss = min(data)
        print(f"  {label}: 均值{avg:+.2f}% | 胜率{win:.0f}% | "
              f"最佳{max_gain:+.2f}% | 最差{max_loss:+.2f}% | 样本{len(data)}")

    print(f"\n📊 实验组(AI选股) 汇总:")
    print(f"{'─'*90}")
    _stats(all_t1_close, "T+1(尾盘买)")
    _stats(all_t1_open, "T+1(开盘买)")
    _stats(all_t3_close, "T+3(尾盘买)")
    _stats(all_t5_close, "T+5(尾盘买)")

    # Per-record detail
    print(f"\n📋 每日明细:")
    print(f"{'─'*90}")
    print(f"  {'日期':<12} {'概念':<12} {'T+1(尾盘)':>10} {'T+1(开盘)':>10} "
          f"{'T+3':>8} {'T+5':>8} {'胜率':>6}")
    print(f"{'─'*90}")
    for record in recent:
        for concept in record.get("concepts", []):
            returns = concept.get("returns", {})
            t1c = returns.get("t1", {}).get("avg_return_close", "-")
            t1o = returns.get("t1", {}).get("avg_return_open", "-")
            t3 = returns.get("t3", {}).get("avg_return_close", "-")
            t5 = returns.get("t5", {}).get("avg_return_close", "-")
            wr = returns.get("t1", {}).get("win_rate", "-")
            t1c_s = f"{t1c:+.2f}%" if isinstance(t1c, (int, float)) else t1c
            t1o_s = f"{t1o:+.2f}%" if isinstance(t1o, (int, float)) else t1o
            t3_s = f"{t3:+.2f}%" if isinstance(t3, (int, float)) else t3
            t5_s = f"{t5:+.2f}%" if isinstance(t5, (int, float)) else t5
            wr_s = f"{wr:.0f}%" if isinstance(wr, (int, float)) else wr
            print(f"  {record['date']:<12} {concept['concept_name']:<12} "
                  f"{t1c_s:>10} {t1o_s:>10} {t3_s:>8} {t5_s:>8} {wr_s:>6}")

    print(f"{'='*90}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="概念轮动回测追踪器")
    sub = parser.add_subparsers(dest="command")

    # record
    p_rec = sub.add_parser("record", help="记录当天选股")
    p_rec.add_argument("--date", help="指定日期 YYYY-MM-DD (默认今天)")
    p_rec.add_argument("--no-proxy", action="store_true")

    # track
    p_trk = sub.add_parser("track", help="追踪已有记录的收益")
    p_trk.add_argument("--no-proxy", action="store_true")

    # report
    p_rpt = sub.add_parser("report", help="生成回测报告")
    p_rpt.add_argument("--days", type=int, default=30, help="报告最近N天")

    args = parser.parse_args()

    if args.command == "record":
        cmd_record(args)
    elif args.command == "track":
        cmd_track(args)
    elif args.command == "report":
        cmd_report(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
