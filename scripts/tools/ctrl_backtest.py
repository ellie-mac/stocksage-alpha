#!/usr/bin/env python3
"""
控制变量回测：三刀对比
  刀1  Baseline   — 无 Winsorize，regime 用阶梯函数权重
  刀2  +Winsorize — Winsorize+行业中位数填 NaN，regime 仍阶梯
  刀3  +λ连续化   — Winsorize + λ 插值平滑权重（当前全量版本）

只看：mean_IC / ICIR / 换手率 / regime_ic_breakdown 平滑性
输出：控制台表格 + data/ctrl_backtest_result.json

用法：
    python -X utf8 scripts/tools/ctrl_backtest.py [--n 200] [--fwd 20] [--periods 6]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent.parent
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))


def _load_universe(n: int) -> list[str]:
    p = ROOT / "data" / "universe_main.json"
    raw = json.loads(p.read_text(encoding="utf-8"))
    codes = raw if isinstance(raw, list) else list(raw.keys())
    return codes[:n]


def _run_ic_variant(codes: list[str], fwd: int, periods: int, step: int,
                    winsorize: bool, label: str) -> dict:
    from factor_analysis import run_analysis
    print(f"\n{'='*55}")
    print(f"  {label}  (winsorize={winsorize})")
    print(f"{'='*55}")
    return run_analysis(codes=codes, forward_days=fwd, group="A",
                        rolling=periods, step=step, winsorize=winsorize)


def _run_bt_variant(codes: list[str], fwd: int, periods: int, step: int,
                    use_lambda: bool, label: str) -> dict:
    from backtest import run_backtest
    print(f"\n{'='*55}")
    print(f"  {label}  (use_lambda={use_lambda})")
    print(f"{'='*55}")
    return run_backtest(codes=codes, forward_days=fwd, n_periods=periods, step=step,
                        group="A", use_regime=True, sector_neutral=True,
                        use_lambda=use_lambda)


def _ic_summary(res: dict) -> dict:
    """Extract mean_IC / ICIR per factor from rolling result."""
    periods = res.get("periods", [])
    if not periods:
        return {}
    factor_names: list[str] = []
    for p in periods:
        factor_names = list(p.get("ic", {}).keys())
        if factor_names:
            break
    ic_by_factor: dict[str, list[float]] = {f: [] for f in factor_names}
    for p in periods:
        for f, v in p.get("ic", {}).items():
            if v is not None and abs(v) < 1.0:
                ic_by_factor[f].append(v)
    import statistics
    summary = {}
    for f, vals in ic_by_factor.items():
        if len(vals) < 2:
            continue
        mean_ic = statistics.mean(vals)
        stdev   = statistics.stdev(vals)
        icir    = mean_ic / stdev if stdev > 0 else 0.0
        summary[f] = {"mean_ic": round(mean_ic, 4), "icir": round(icir, 3), "n": len(vals)}
    return summary


def _bt_summary(res: dict) -> dict:
    """Extract turnover + regime_lambda smoothness."""
    periods = res.get("period_results", [])
    lambdas = [p.get("regime_lambda") for p in periods if p.get("regime_lambda") is not None]
    turnovers = [p.get("turnover_pct") for p in periods if p.get("turnover_pct") is not None]
    if not lambdas:
        return {"lambda_std": None, "mean_turnover": None}
    import statistics
    lam_std = round(statistics.stdev(lambdas), 4) if len(lambdas) >= 2 else None
    lam_range = round(max(lambdas) - min(lambdas), 4)
    mean_to = round(statistics.mean(turnovers), 2) if turnovers else None
    return {"lambda_std": lam_std, "lambda_range": lam_range,
            "mean_turnover": mean_to, "lambdas": [round(x, 3) for x in lambdas]}


def _print_ic_table(results: dict[str, dict]) -> None:
    all_factors = set()
    for v in results.values():
        all_factors.update(v.keys())
    factors = sorted(all_factors)
    labels  = list(results.keys())

    col_w = 14
    hdr   = f"{'Factor':<22}" + "".join(f"{l:>{col_w}}" for l in labels)
    print("\n── Rank IC 对比 ──────────────────────────────────────────────")
    print(hdr)
    print("-" * (22 + col_w * len(labels)))
    for f in factors:
        row = f"{f:<22}"
        for lbl in labels:
            s = results[lbl].get(f)
            if s is None:
                row += f"{'N/A':>{col_w}}"
            else:
                row += f"  IC={s['mean_ic']:+.4f} IR={s['icir']:+.3f}"
        print(row)


def _print_bt_table(results: dict[str, dict]) -> None:
    print("\n── 换手率 / λ 平滑性 对比 ──────────────────────────────────")
    print(f"{'Variant':<30}  {'mean_turnover':>14}  {'lambda_std':>12}  {'lambda_range':>13}")
    print("-" * 74)
    for lbl, s in results.items():
        to  = f"{s['mean_turnover']:.2f}%" if s['mean_turnover'] is not None else "N/A"
        std = f"{s['lambda_std']:.4f}"     if s['lambda_std']    is not None else "N/A"
        rng = f"{s['lambda_range']:.4f}"   if s.get('lambda_range') is not None else "N/A"
        print(f"{lbl:<30}  {to:>14}  {std:>12}  {rng:>13}")
    for lbl, s in results.items():
        lams = s.get("lambdas")
        if lams:
            print(f"  {lbl}: λ序列 = {lams}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n",       type=int, default=200, help="Universe size")
    ap.add_argument("--fwd",     type=int, default=20,  help="Forward return days")
    ap.add_argument("--periods", type=int, default=6,   help="Rolling periods")
    ap.add_argument("--step",    type=int, default=20,  help="Days between periods")
    ap.add_argument("--ic-only", action="store_true",   help="Only run IC (skip backtest)")
    ap.add_argument("--bt-only", action="store_true",   help="Only run backtest (skip IC)")
    args = ap.parse_args()

    codes = _load_universe(args.n)
    print(f"[ctrl_backtest] Universe: {len(codes)} stocks | fwd={args.fwd}d | "
          f"periods={args.periods} × {args.step}d")

    ic_results: dict[str, dict]  = {}
    bt_results: dict[str, dict]  = {}

    if not args.bt_only:
        r0 = _run_ic_variant(codes, args.fwd, args.periods, args.step,
                             winsorize=False, label="刀1  Baseline")
        r1 = _run_ic_variant(codes, args.fwd, args.periods, args.step,
                             winsorize=True,  label="刀2  +Winsorize")
        ic_results["刀1 Baseline"]   = _ic_summary(r0)
        ic_results["刀2 +Winsorize"] = _ic_summary(r1)
        _print_ic_table(ic_results)

    if not args.ic_only:
        b0 = _run_bt_variant(codes, args.fwd, args.periods, args.step,
                             use_lambda=False, label="刀1  阶梯权重")
        b1 = _run_bt_variant(codes, args.fwd, args.periods, args.step,
                             use_lambda=True,  label="刀3  +λ连续化")
        bt_results["刀1 阶梯权重"] = _bt_summary(b0)
        bt_results["刀3 +λ连续化"] = _bt_summary(b1)
        _print_bt_table(bt_results)

    out = ROOT / "data" / "ctrl_backtest_result.json"
    out.write_text(
        json.dumps({"ic": ic_results, "backtest": bt_results}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    print(f"\n[ctrl_backtest] 结果已保存 → {out.name}")


if __name__ == "__main__":
    main()
