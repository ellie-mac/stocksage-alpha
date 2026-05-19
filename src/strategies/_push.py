"""Push 推送公共片段 — 供 main/small/etf 等策略复用。

只抽出真正重复的部分：
  - regime_header_line: "*{run_time}*<br>市场 {emoji} {score}/10 {label}"
  - DISCLAIMER:         尾部的"仅供参考"声明
  - wechat_send_with_log: setup_push 之后那段 9 行的 try/except + dry-run 分支

不试图统一各策略的 body 格式 —— tier 扫描器（gc/hot/sideways/institution）
渲染语义差异太大，强行模板化反而冗余。
"""
from __future__ import annotations

from common import send_wechat, regime_emoji


DISCLAIMER = "<br><br>> 仅供参考，不构成投资建议"


def regime_header_line(run_time: str, regime_score: float, regime_label: str) -> str:
    """统一格式："*{run_time}*<br>市场 {emoji} {score:.0f}/10 {label}" """
    return (f"*{run_time}*<br>市场 {regime_emoji(regime_score)} "
            f"{regime_score:.0f}/10 {regime_label}")


def wechat_send_with_log(
    title: str,
    body: str,
    sendkey: str,
    log_prefix: str,
    dry_run: bool = False,
) -> None:
    """send_wechat + 成功/失败日志 + dry-run 打印。
    失败时 raise，给上层 scheduler 看到错误。
    """
    if dry_run:
        print(f"[{log_prefix}] dry-run:\n{title}\n{body}")
        return
    try:
        send_wechat(title, body, sendkey, dry_run=False)
        print(f"[{log_prefix}] 微信推送完成")
    except Exception as e:
        print(f"[{log_prefix}] 微信推送失败: {e}")
        raise
