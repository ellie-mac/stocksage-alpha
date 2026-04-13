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
_ROOT          = Path(__file__).parent.parent
PERF_LOG       = _ROOT / "data" / "signal_performance.json"
WEIGHT_HISTORY = _ROOT / "data" / "weight_history.json"
FACTOR_CONFIG  = _ROOT / "scripts" / "factor_config.py"

# ── 调参超参数 ──────────────────────────────────────────────────────────────────
MIN_FACTOR_SAMPLES   = 5       # 每个因子至少需要的已完成信号数
MIN_TOTAL_SIGNALS    = 15      # 全局最低：20d 已完成买入信号数量
SCALE                = 0.6     # delta = (hit_rate - 0.50) * SCALE
MAX_DELTA_PER_RUN    = 0.3     # 单次运行最大调整幅度（绝对值）
WEIGHT_MIN_POS       = 0.1     # 正权重下界
WEIGHT_MAX           = 3.0     # 权重上界
# 注：负权重因子保持负，不翻转符号

# ── 辅助函数 ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict | list:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


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
) -> list[dict]:
    """
    根据因子胜率计算权重调整建议。
    返回 list of {factor, current, suggested, delta, hit_rate, n}。
    """
    suggestions = []

    for factor, stats in factor_hit_rates.items():
        n = stats.get("total", 0)
        hit_rate = stats.get("hit_rate")

        if n < MIN_FACTOR_SAMPLES or hit_rate is None:
            continue  # 样本不足，跳过

        if factor not in current_weights:
            continue  # 因子不在 FACTOR_WEIGHTS 中（可能在其他 regime）

        current_w = current_weights[factor]

        # delta = (hit_rate - 0.50) * 0.6，限制单次幅度
        raw_delta = (hit_rate - 0.50) * SCALE
        delta = _clamp(raw_delta, -MAX_DELTA_PER_RUN, MAX_DELTA_PER_RUN)

        new_w = current_w + delta

        # 正权重因子：夹到 [WEIGHT_MIN_POS, WEIGHT_MAX]
        if current_w >= 0:
            new_w = _clamp(new_w, WEIGHT_MIN_POS, WEIGHT_MAX)
        else:
            # 负权重因子：保持负，上界 -WEIGHT_MIN_POS，下界 -WEIGHT_MAX
            new_w = _clamp(new_w, -WEIGHT_MAX, -WEIGHT_MIN_POS)

        # 格式化为最多 2 位小数
        new_w = round(new_w, 2)

        if new_w == current_w:
            continue  # 没有实质变化，跳过

        suggestions.append({
            "factor":    factor,
            "current":   current_w,
            "suggested": new_w,
            "delta":     round(new_w - current_w, 4),
            "hit_rate":  hit_rate,
            "n":         n,
        })

    # 按 |delta| 降序排列
    suggestions.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return suggestions


def print_preview(suggestions: list[dict], total_signals: int) -> None:
    """打印建议摘要。"""
    print("\n" + "=" * 62)
    print("  auto_tune — 因子权重调整预览")
    print(f"  已完成 20d 买入信号总数: {total_signals}")
    print("=" * 62)

    if not suggestions:
        print("  当前没有满足条件的调整建议。")
        print("=" * 62)
        return

    print(f"  {'因子':<28} {'当前':>6} {'建议':>6}  {'变化':>7}  {'胜率':>6}  N")
    print("  " + "-" * 58)
    for s in suggestions:
        arrow = "↑" if s["delta"] > 0 else "↓"
        print(f"  {s['factor']:<28} {s['current']:>6.2f} {s['suggested']:>6.2f}"
              f"  {arrow}{abs(s['delta']):>6.3f}  {s['hit_rate']:.1%}  {s['n']}")
    print("=" * 62)


def apply_suggestions(
    suggestions: list[dict],
    current_weights: dict[str, float],
    dry_run: bool = False,
) -> None:
    """将建议写入 factor_config.py 并记录到 weight_history.json。"""
    if not suggestions:
        print("  没有需要应用的调整。")
        return

    new_weights = dict(current_weights)
    for s in suggestions:
        new_weights[s["factor"]] = s["suggested"]

    if not dry_run:
        _apply_weights_to_file(new_weights)
        print(f"  已更新 factor_config.py（共 {len(suggestions)} 个因子）。")
    else:
        print(f"  [dry-run] 跳过写入 factor_config.py。")

    # 记录历史
    history: list = _load_json(WEIGHT_HISTORY)  # type: ignore[assignment]
    if not isinstance(history, list):
        history = []

    history.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "dry_run": dry_run,
        "changes": suggestions,
    })

    if not dry_run:
        _save_json(WEIGHT_HISTORY, history)
        print(f"  权重历史已写入 {WEIGHT_HISTORY}。")
    else:
        print("  [dry-run] 跳过写入 weight_history.json。")


# ── CLI 入口 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="根据信号绩效自动调整因子权重")
    parser.add_argument("--preview", action="store_true",
                        help="只预览建议（默认行为）")
    parser.add_argument("--apply", action="store_true",
                        help="将建议应用到 factor_config.py")
    args = parser.parse_args()

    # 默认行为：preview
    do_apply = args.apply
    if not do_apply:
        print("运行模式: --preview（使用 --apply 来实际写入权重）")

    # 1. 加载绩效数据
    perf = _load_json(PERF_LOG)
    if not perf:
        print(f"WARN: {PERF_LOG} 不存在或为空。请先运行 signal_tracker.py。")
        return

    # 2. 检查全局样本量（20d 完成信号总数）
    fwd20_stats = perf.get("window_stats", {}).get("fwd_20d", {})
    total_signals = fwd20_stats.get("n", 0)
    print(f"已完成 20d 买入信号总数: {total_signals}（最低要求: {MIN_TOTAL_SIGNALS}）")

    if total_signals < MIN_TOTAL_SIGNALS:
        print(f"  样本不足，暂不调整权重。")
        return

    # 3. 加载当前权重
    try:
        current_weights = _load_factor_weights()
        print(f"  从 factor_config.py 读取到 {len(current_weights)} 个 NORMAL 权重。")
    except Exception as e:
        print(f"  ERROR 读取权重: {e}")
        return

    # 4. 计算建议
    factor_hit_rates = perf.get("factor_hit_rates", {})
    suggestions = compute_suggestions(factor_hit_rates, current_weights)

    # 5. 打印预览
    print_preview(suggestions, total_signals)

    # 6. 若 --apply，写入文件
    if do_apply:
        print("\n应用权重调整...")
        apply_suggestions(suggestions, current_weights, dry_run=False)
    else:
        # preview 模式也可以展示 dry-run 日志
        if suggestions:
            print("\n  提示: 使用 --apply 参数将上述调整写入 factor_config.py")


if __name__ == "__main__":
    main()
