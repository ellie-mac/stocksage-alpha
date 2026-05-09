# auto_tune.py — 根据信号绩效自动调整 factor_config.py 中的因子权重
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# ── 路径定义 ────────────────────────────────────────────────────────────────────
_ROOT            = Path(__file__).parent.parent.parent
PERF_LOG         = _ROOT / "data" / "signal_performance.json"
WEIGHT_HISTORY   = _ROOT / "data" / "weight_history.json"
WEIGHT_BASE_FILE = _ROOT / "data" / "weight_base.json"        # IC-derived base (frozen)
MULTIPLIER_FILE  = _ROOT / "data" / "weight_multipliers.json" # online overlay
FACTOR_CONFIG    = _ROOT / "src" / "factors" / "config.py"

# ── 调参超参数 ──────────────────────────────────────────────────────────────────
MIN_FACTOR_SAMPLES   = 50      # 每个因子至少需要的已完成信号数（提高到50，减少噪声）
MIN_TOTAL_SIGNALS    = 15      # 全局最低：20d 已完成买入信号数量
SCALE                = 0.6     # delta_m = (adjusted_hit_rate - 0.50) * SCALE
MAX_DELTA_PER_RUN    = 0.3     # 单次运行最大调整幅度（绝对值）
MAX_CHANGE_RATE      = 0.10    # 单次权重变化率上限（相对于当前权重），防止线上权重震荡
WEIGHT_MIN_POS       = 0.1     # 正权重下界
WEIGHT_MAX           = 3.0     # 权重上界
MULT_MIN             = 0.7     # 乘子下界：不允许 live weight < 70% of base
MULT_MAX             = 1.3     # 乘子上界：不允许 live weight > 130% of base
DECAY_RATE           = 0.05    # 每次运行乘子向 1.0 衰减的速率（防止长期漂移）
LOWER_QUANTILE       = 0.10    # Beta 后验下分位数（10th percentile lower bound）
# 注：负权重因子保持负，不翻转符号

# ── 辅助函数 ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict | list:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _beta_lower_bound(n_wins: int, n_total: int,
                      q: float = LOWER_QUANTILE) -> float:
    """
    Beta-Binomial posterior lower bound on hit rate (uniform prior Beta(1,1)).
    Returns the q-th quantile of Beta(1+n_wins, 1+n_total-n_wins).
    Shrinks raw hit_rate toward 0.5 under small samples.
    """
    try:
        from scipy.stats import beta as _beta
        return float(_beta.ppf(q, 1 + n_wins, 1 + n_total - n_wins))
    except ImportError:
        # Wilson one-sided lower bound (no external dependency)
        n = n_total
        p_hat = n_wins / n if n > 0 else 0.5
        z = 1.282  # z_{0.10}
        denom = 1 + z * z / n
        centre = p_hat + z * z / (2 * n)
        spread = z * (p_hat * (1 - p_hat) / n + z * z / (4 * n * n)) ** 0.5
        return max(0.0, (centre - spread) / denom)


def _load_multipliers() -> dict[str, float]:
    """Load overlay multipliers; default all 1.0 (no adjustment)."""
    raw = _load_json(MULTIPLIER_FILE)
    return {k: float(v) for k, v in raw.items()} if isinstance(raw, dict) else {}


def _save_multipliers(multipliers: dict[str, float]) -> None:
    _save_json(MULTIPLIER_FILE, {k: round(v, 4) for k, v in multipliers.items()})


def _load_or_init_base_weights(current_weights: dict[str, float]) -> dict[str, float]:
    """
    Load IC-derived base weights from WEIGHT_BASE_FILE.
    On first run, snapshots current_weights as the base (带时间戳).
    Metadata keys prefixed with __ are stripped before returning.
    """
    raw = _load_json(WEIGHT_BASE_FILE)
    if isinstance(raw, dict) and raw:
        base = {k: float(v) for k, v in raw.items() if not k.startswith("__")}
        if base:
            created = raw.get("__created_at__", "未知")
            print(f"  [base] IC基础权重快照（{created}）。如需重置请删除 {WEIGHT_BASE_FILE.name}。")
            return base
    snapshot: dict = {k: round(v, 4) for k, v in current_weights.items()}
    snapshot["__created_at__"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    _save_json(WEIGHT_BASE_FILE, snapshot)
    return dict(current_weights)


def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)  # atomic rename — prevents partial-write corruption


def _load_factor_weights() -> dict[str, float]:
    """
    从 factor_config.py 解析 FACTOR_WEIGHTS 字典（只解析 NORMAL 权重块，
    即 FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS 这行之前的内容）。
    """
    if not FACTOR_CONFIG.exists():
        raise FileNotFoundError(f"找不到 {FACTOR_CONFIG}")

    source = FACTOR_CONFIG.read_text(encoding="utf-8")
    # 截取到 FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS 之前
    norm_marker = "FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS"
    end_idx = source.find(norm_marker)
    if end_idx == -1:
        raise ValueError("factor_config.py 中找不到 FACTOR_WEIGHTS_NORMAL 标记")
    block = source[:end_idx]

    # 匹配 "factor_name": weight_value（支持负数、小数）
    pattern = re.compile(r'"(\w+)"\s*:\s*([-\d.]+)')
    weights: dict[str, float] = {}
    for m in pattern.finditer(block):
        weights[m.group(1)] = float(m.group(2))
    return weights


def _apply_weights_to_file(new_weights: dict[str, float]) -> None:
    """
    用正则将 FACTOR_WEIGHTS 块（FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS 之前）中
    的权重数值替换为新值。只替换有变化的条目。
    """
    source = FACTOR_CONFIG.read_text(encoding="utf-8")
    norm_marker = "FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS"
    end_idx = source.find(norm_marker)
    if end_idx == -1:
        raise ValueError("找不到 FACTOR_WEIGHTS_NORMAL 标记")

    header = source[:end_idx]
    tail   = source[end_idx:]

    for factor, new_val in new_weights.items():
        # 匹配形如  "factor_name"   :   1.5  （值后面跟逗号/空白/注释）
        pat = re.compile(
            r'("' + re.escape(factor) + r'"\s*:\s*)([-\d.]+)',
        )
        header = pat.sub(lambda m: m.group(1) + str(new_val), header, count=1)

    FACTOR_CONFIG.write_text(header + tail, encoding="utf-8")


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# ── 核心逻辑 ────────────────────────────────────────────────────────────────────

def compute_suggestions(
    factor_hit_rates: dict,
    current_weights: dict[str, float],
) -> tuple[list[dict], dict[str, float]]:
    """
    Base/Overlay架构：在IC-calibrated基础权重上，用Beta-Binomial收缩后的胜率
    更新一个乘子（clip到[MULT_MIN, MULT_MAX]），live_weight = base × multiplier。
    返回 (suggestions, updated_multipliers)。
    """
    base_weights = _load_or_init_base_weights(current_weights)
    multipliers  = _load_multipliers()
    suggestions: list[dict] = []

    for factor, stats in factor_hit_rates.items():
        n        = stats.get("total", 0)
        hit_rate = stats.get("hit_rate")

        if n < MIN_FACTOR_SAMPLES or hit_rate is None:
            continue
        if factor not in base_weights:
            continue  # 因子不在IC-base中，跳过

        n_wins  = min(n, max(0, int(round(n * hit_rate))))
        adj_hit = _beta_lower_bound(n_wins, n)

        # 先向1.0衰减，再根据调整后胜率更新乘子
        m       = multipliers.get(factor, 1.0)
        m       = (1 - DECAY_RATE) * m + DECAY_RATE * 1.0
        delta_m = (adj_hit - 0.5) * SCALE
        m       = _clamp(m + delta_m, MULT_MIN, MULT_MAX)

        # Shrinkage toward equal-weight (multiplier=1.0) when sample count is low.
        # α=1 at n=0 (no data → keep equal weight), α=0 at n=MIN_FACTOR_SAMPLES (full trust).
        alpha = max(0.0, 1.0 - n / MIN_FACTOR_SAMPLES)
        m     = alpha * 1.0 + (1 - alpha) * m
        m     = _clamp(m, MULT_MIN, MULT_MAX)
        multipliers[factor] = round(m, 4)

        base_w  = base_weights[factor]
        live_w  = round(base_w * m, 2)

        # 保持符号方向，限制量级
        if base_w > 0:
            live_w = _clamp(live_w, WEIGHT_MIN_POS, WEIGHT_MAX)
        elif base_w < 0:
            live_w = _clamp(live_w, -WEIGHT_MAX, -WEIGHT_MIN_POS)
        # base_w == 0 保持0，不进入suggestions

        current_w = current_weights.get(factor, base_w)

        # Rate-of-change guard: cap single-run adjustment at MAX_CHANGE_RATE × |current|
        max_delta = max(abs(current_w), 0.01) * MAX_CHANGE_RATE
        if abs(live_w - current_w) > max_delta:
            live_w = current_w + max_delta * (1 if live_w > current_w else -1)
            live_w = round(live_w, 4)

        if live_w == current_w:
            continue

        suggestions.append({
            "factor":     factor,
            "current":    current_w,
            "suggested":  live_w,
            "delta":      round(live_w - current_w, 4),
            "hit_rate":   hit_rate,
            "adj_hit":    round(adj_hit, 3),
            "multiplier": multipliers[factor],
            "shrinkage_alpha": round(alpha, 3),
            "n":          n,
        })

    suggestions.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return suggestions, multipliers


def print_preview(suggestions: list[dict], total_signals: int) -> None:
    """打印建议摘要（Base × 乘子架构）。"""
    print("\n" + "=" * 76)
    print("  auto_tune — 因子权重调整预览（Base × Multiplier架构）")
    print(f"  已完成 20d 买入信号总数: {total_signals}")
    print("=" * 76)

    if not suggestions:
        print("  当前没有满足条件的调整建议。")
        print("=" * 76)
        return

    print(f"  {'因子':<28} {'当前':>6} {'建议':>6}  {'变化':>7}  {'胜率':>6}  {'调整后':>6}  {'乘子':>7}  N")
    print("  " + "-" * 72)
    for s in suggestions:
        arrow   = "↑" if s["delta"] > 0 else "↓"
        adj     = s.get("adj_hit", s["hit_rate"])
        mult    = s.get("multiplier", 1.0)
        print(f"  {s['factor']:<28} {s['current']:>6.2f} {s['suggested']:>6.2f}"
              f"  {arrow}{abs(s['delta']):>6.3f}  {s['hit_rate']:.1%}  {adj:.1%}  {mult:>7.4f}  {s['n']}")
    print("=" * 76)


def apply_suggestions(
    suggestions: list[dict],
    current_weights: dict[str, float],
    multipliers: dict[str, float] | None = None,
    dry_run: bool = False,
) -> None:
    """将 live weights 写入 factor_config.py，并持久化乘子到 MULTIPLIER_FILE。"""
    if not suggestions:
        print("  没有需要应用的调整。")
        return

    new_weights = dict(current_weights)
    for s in suggestions:
        new_weights[s["factor"]] = s["suggested"]

    if not dry_run:
        _apply_weights_to_file(new_weights)
        print(f"  已更新 factor_config.py（共 {len(suggestions)} 个因子）。")
        if multipliers:
            _save_multipliers(multipliers)
            print(f"  乘子已保存到 {MULTIPLIER_FILE}。")
    else:
        print(f"  [dry-run] 跳过写入 factor_config.py。")
        if multipliers:
            print(f"  [dry-run] 跳过写入 {MULTIPLIER_FILE}。")

    # 记录历史
    history: list = _load_json(WEIGHT_HISTORY)  # type: ignore[assignment]
    if not isinstance(history, list):
        history = []

    history.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "dry_run":   dry_run,
        "changes":   suggestions,
    })

    if not dry_run:
        _save_json(WEIGHT_HISTORY, history)
        print(f"  权重历史已写入 {WEIGHT_HISTORY}。")
    else:
        print("  [dry-run] 跳过写入 weight_history.json。")


# ── IC-based tuning ─────────────────────────────────────────────────────────────

IC_FILE_DEFAULT = Path(__file__).parent.parent.parent / "data" / "factor_ic_main.json"

# ICIR magnitude → weight magnitude tiers
_ICIR_TIERS = [(1.0, 1.5), (0.5, 1.0), (0.3, 0.5)]


def _icir_to_target(icir: float) -> float:
    """Map ICIR to a target weight magnitude * sign."""
    sign = 1.0 if icir >= 0 else -1.0
    abs_icir = abs(icir)
    for threshold, magnitude in _ICIR_TIERS:
        if abs_icir >= threshold:
            return sign * magnitude
    return 0.0


def compute_ic_suggestions(
    ic_table: dict,
    current_weights: dict[str, float],
) -> tuple[list[dict], list[str]]:
    """
    Compute weight adjustment suggestions from IC backtest results.

    Returns (suggestions, warnings) where warnings lists factors with
    IC=0.000 that were skipped (likely data-gap artifacts).
    """
    suggestions: list[dict] = []
    warnings: list[str] = []

    for factor, current_w in current_weights.items():
        entry = ic_table.get(factor)
        if entry is None:
            continue  # factor not in IC results

        icir    = entry.get("icir") or 0.0
        mean_ic = entry.get("mean_ic") or 0.0

        # ICIR=0 and IC=0 almost certainly means a data gap — skip
        if icir == 0.0 and mean_ic == 0.0:
            warnings.append(f"  ⚠ {factor}: ICIR=0 IC=0, 疑似数据缺口，跳过")
            continue

        target = _icir_to_target(icir)

        # Gradually move toward target, capped at MAX_DELTA_PER_RUN
        raw_delta = target - current_w
        delta = _clamp(raw_delta, -MAX_DELTA_PER_RUN, MAX_DELTA_PER_RUN)
        new_w = round(current_w + delta, 2)

        # Clamp: preserve sign direction from IC
        if target >= 0:
            new_w = _clamp(new_w, 0.0, WEIGHT_MAX)
        else:
            new_w = _clamp(new_w, -WEIGHT_MAX, 0.0)

        if new_w == current_w:
            continue

        suggestions.append({
            "factor":    factor,
            "current":   current_w,
            "suggested": new_w,
            "delta":     round(new_w - current_w, 4),
            "icir":      round(icir, 3),
            "mean_ic":   round(mean_ic, 4),
        })

    suggestions.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return suggestions, warnings


def print_ic_preview(suggestions: list[dict], warnings: list[str], ic_meta: dict) -> None:
    print("\n" + "=" * 70)
    print("  auto_tune --ic — IC回测权重调整预览")
    print(f"  IC回测: {ic_meta.get('n_stocks',0)}只股票, "
          f"{ic_meta.get('n_periods',0)}期, step={ic_meta.get('step_days',0)}d")
    print("=" * 70)

    if warnings:
        for w in warnings:
            print(w)
        print()

    if not suggestions:
        print("  没有需要调整的权重（当前权重已与IC结果一致）。")
        print("=" * 70)
        return

    print(f"  {'因子':<30} {'当前':>6} {'建议':>6}  {'变化':>7}  ICIR")
    print("  " + "-" * 60)
    for s in suggestions:
        arrow = "↑" if s["delta"] > 0 else "↓"
        print(f"  {s['factor']:<30} {s['current']:>6.2f} {s['suggested']:>6.2f}"
              f"  {arrow}{abs(s['delta']):>6.3f}  {s['icir']:>+.3f}")
    print("=" * 70)


# ── CLI 入口 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="根据信号绩效或IC结果自动调整因子权重")
    parser.add_argument("--preview", action="store_true",
                        help="只预览建议（默认行为）")
    parser.add_argument("--apply", action="store_true",
                        help="将建议应用到 factor_config.py")
    parser.add_argument("--ic", action="store_true",
                        help="使用IC回测结果（而非信号胜率）调整权重")
    parser.add_argument("--ic-file", type=str, default=str(IC_FILE_DEFAULT),
                        help=f"IC结果文件路径（默认: {IC_FILE_DEFAULT}）")
    args = parser.parse_args()

    do_apply = args.apply
    if not do_apply:
        print("运行模式: --preview（使用 --apply 来实际写入权重）")

    # ── IC模式 ───────────────────────────────────────────────────────────────────
    if args.ic:
        ic_path = Path(args.ic_file)
        if not ic_path.exists():
            print(f"ERROR: IC文件不存在: {ic_path}")
            return
        data = _load_json(ic_path)
        ic_table = data.get("ic_table", {})
        ic_meta  = data.get("meta", {})
        if not ic_table:
            print("ERROR: ic_table 为空，请先运行 factor_analysis.py")
            return

        try:
            current_weights = _load_factor_weights()
            print(f"从 factor_config.py 读取到 {len(current_weights)} 个 NORMAL 权重。")
        except Exception as e:
            print(f"ERROR 读取权重: {e}")
            return

        suggestions, warnings = compute_ic_suggestions(ic_table, current_weights)
        print_ic_preview(suggestions, warnings, ic_meta)

        if do_apply:
            print("\n应用IC权重调整...")
            apply_suggestions(suggestions, current_weights, dry_run=False)
        elif suggestions:
            print("\n  提示: 使用 --apply 参数将上述调整写入 factor_config.py")
        return

    # ── 信号胜率模式（原逻辑）────────────────────────────────────────────────────
    perf = _load_json(PERF_LOG)
    if not perf:
        print(f"WARN: {PERF_LOG} 不存在或为空。请先运行 signal_tracker.py。")
        return

    fwd20_stats = perf.get("window_stats", {}).get("fwd_20d", {})
    total_signals = fwd20_stats.get("n", 0)
    print(f"已完成 20d 买入信号总数: {total_signals}（最低要求: {MIN_TOTAL_SIGNALS}）")

    if total_signals < MIN_TOTAL_SIGNALS:
        print(f"  样本不足，暂不调整权重。")
        return

    try:
        current_weights = _load_factor_weights()
        print(f"  从 factor_config.py 读取到 {len(current_weights)} 个 NORMAL 权重。")
    except Exception as e:
        print(f"  ERROR 读取权重: {e}")
        return

    factor_hit_rates = perf.get("factor_hit_rates", {})
    suggestions, multipliers = compute_suggestions(factor_hit_rates, current_weights)
    print_preview(suggestions, total_signals)

    if do_apply:
        print("\n应用权重调整...")
        apply_suggestions(suggestions, current_weights, multipliers=multipliers, dry_run=False)
    elif suggestions:
        print("\n  提示: 使用 --apply 参数将上述调整写入 factor_config.py")


if __name__ == "__main__":
    main()
