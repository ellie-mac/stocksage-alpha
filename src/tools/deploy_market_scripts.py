#!/usr/bin/env python3
"""
Deploy updated daily_market.ps1 + new market_supplement.py to both bot folders.
Run once on the VM: python src/tools/deploy_market_scripts.py
"""
from pathlib import Path
import shutil

TARGETS = [
    Path("C:/Users/jiapeichen/repos/bro/claude1"),
    Path("C:/Users/jiapeichen/repos/me/life"),
]

# ─────────────────────────────────────────────
# market_supplement.py  (identical for both bots)
# ─────────────────────────────────────────────
SUPPLEMENT_PY = '''\
#!/usr/bin/env python3
"""
Supplement data for daily_market.ps1.
Outputs one JSON line: northbound fallback, limit-up/down counts, sector movers.
All failures silently return null so PS script degrades gracefully.
"""
import json
import sys
from datetime import date

out = {
    "nb_sh": None, "nb_sz": None, "nb_total": None,
    "limit_up": None, "limit_down": None,
    "sectors_top": [], "sectors_bot": []
}

today = date.today().strftime("%Y%m%d")

# --- limit up / down ---
try:
    import akshare as ak
    zt = ak.stock_zt_pool_em(date=today)
    dt = ak.stock_zt_pool_dtgc_em(date=today)
    if zt is not None:
        out["limit_up"] = int(len(zt))
    if dt is not None:
        out["limit_down"] = int(len(dt))
except Exception:
    pass

# --- sector movers (THS, accessible from overseas) ---
try:
    import akshare as ak
    import pandas as pd
    df = ak.stock_board_industry_summary_ths()
    if df is not None and not df.empty:
        df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
        df = df.dropna(subset=["涨跌幅"]).sort_values("涨跌幅", ascending=False)
        def _row(r):
            return {"name": r["板块"], "pct": round(float(r["涨跌幅"]), 2)}
        out["sectors_top"] = [_row(r) for _, r in df.head(3).iterrows()]
        out["sectors_bot"] = [_row(r) for _, r in df.tail(3).iloc[::-1].iterrows()]
except Exception:
    pass

# --- northbound flow (Python fallback, same endpoint as PS but with retry headers) ---
try:
    import urllib.request, json as _json
    url = "https://push2.eastmoney.com/api/qt/kamt/get?fields=f1,f2,f3,f4,f5,f6"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.eastmoney.com"
    })
    with urllib.request.urlopen(req, timeout=5) as r:
        data = _json.loads(r.read().decode()).get("data") or {}
        if data.get("f1") is not None:
            out["nb_sh"]    = round(data["f1"] / 1e8, 2)
            out["nb_sz"]    = round(data["f3"] / 1e8, 2)
            out["nb_total"] = round((data["f1"] + data["f3"]) / 1e8, 2)
except Exception:
    pass

print(json.dumps(out, ensure_ascii=False))
'''

# ─────────────────────────────────────────────
# daily_market.ps1  template  ({BOT_ROOT} replaced per target)
# ─────────────────────────────────────────────
MARKET_PS1_TEMPLATE = r'''# 大盘日报：指数 + 成交额 + 北向 + 港股 + 涨跌停 + 板块
# 每日 16:35 触发，写入 daily_market.md

$botRoot    = "{BOT_ROOT}"
$outputFile = "$botRoot\daily_market.md"
$logFile    = "$botRoot\scripts\run_log.txt"
$python     = "C:\Program Files\Python313\python.exe"
$pyHelper   = "$botRoot\scripts\market_supplement.py"

$date    = Get-Date -Format "yyyy-MM-dd"
$headers = @{"Referer" = "https://finance.sina.com.cn"}
$lines   = @()
$lines  += "# 大盘日报 - $date"
$lines  += ""

# ── 调 Python 获取涨跌停 + 板块 + 北向备用 ──────────────────────
$supp = $null
try {
    $suppJson = & $python -X utf8 $pyHelper 2>$null
    if ($suppJson) { $supp = $suppJson | ConvertFrom-Json }
} catch {}

# ── 大盘指数（新浪实时）────────────────────────────────────────
$idxRaw  = Invoke-WebRequest "https://hq.sinajs.cn/list=sh000001,sz399001,sh000300,sz399006" `
             -UseBasicParsing -Headers $headers
$idxText = [System.Text.Encoding]::GetEncoding("GBK").GetString($idxRaw.RawContentStream.ToArray())

$indices = @(
    @{code="sh000001"; name="上证指数"},
    @{code="sz399001"; name="深证成指"},
    @{code="sh000300"; name="沪深300"},
    @{code="sz399006"; name="创业板指"}
)

$shAmt = 0; $szAmt = 0
$lines += "## 大盘指数"
$lines += ""
$lines += "| 指数 | 收盘 | 涨跌幅 |"
$lines += "|------|------|--------|"

foreach ($idx in $indices) {
    if ($idxText -match "hq_str_$($idx.code)=""([^""]+)""") {
        $f   = $Matches[1] -split ","
        $pre = [double]$f[1]; $cur = [double]$f[3]; $amt = [double]$f[9]
        $pct = [math]::Round(($cur - $pre) / $pre * 100, 2)
        $pctStr = if ($pct -ge 0) { "+$pct%" } else { "$pct%" }
        $lines += "| $($idx.name) | $cur | $pctStr |"
        if ($idx.code -eq "sh000001") { $shAmt = $amt }
        if ($idx.code -eq "sz399001") { $szAmt = $amt }
    }
}
$lines += ""

# ── 成交额 ─────────────────────────────────────────────────────
$totalAmt = [math]::Round(($shAmt + $szAmt) / 1e12, 2)
$amtSignal = if ($totalAmt -ge 2) { "🔥 活跃" } elseif ($totalAmt -ge 1.5) { "📊 一般" } else { "🧊 低迷" }
$lines += "## 成交额"
$lines += ""
$lines += "- 合计:**${totalAmt}万亿** $amtSignal（沪 $([math]::Round($shAmt/1e12,2))万亿 + 深 $([math]::Round($szAmt/1e12,2))万亿）"
$lines += "- 参考:2万亿+活跃 / 1.5万亿以上一般 / <1.5万亿低迷"
$lines += ""

# ── 涨跌停统计 ─────────────────────────────────────────────────
$lines += "## 涨跌停"
$lines += ""
if ($supp -and $supp.limit_up -ne $null) {
    $upN  = $supp.limit_up
    $dnN  = $supp.limit_down
    $sentiment = if ($upN -gt $dnN * 2) { "多头占优" } elseif ($dnN -gt $upN * 2) { "空头占优" } else { "多空均衡" }
    $lines += "- 涨停:**${upN}只** | 跌停:**${dnN}只** | $sentiment"
} else {
    $lines += "- 数据获取失败"
}
$lines += ""

# ── 北向资金 ───────────────────────────────────────────────────
$lines += "## 北向资金"
$lines += ""
$nbDone = $false
try {
    $nbRaw = Invoke-RestMethod "https://push2.eastmoney.com/api/qt/kamt/get?fields=f1,f2,f3,f4,f5,f6" -TimeoutSec 5
    if ($nbRaw.data -ne $null) {
        $nb    = $nbRaw.data
        $sh    = [math]::Round($nb.f1 / 1e8, 2)
        $sz    = [math]::Round($nb.f3 / 1e8, 2)
        $total = [math]::Round(($nb.f1 + $nb.f3) / 1e8, 2)
        $sig   = if ($total -gt 0) { "↑ 净流入" } else { "↓ 净流出" }
        $lines += "- 沪股通:${sh}亿 | 深股通:${sz}亿 | **合计:${total}亿** $sig"
        $nbDone = $true
    }
} catch {}

if (-not $nbDone) {
    if ($supp -and $supp.nb_total -ne $null) {
        $sh    = $supp.nb_sh; $sz = $supp.nb_sz; $total = $supp.nb_total
        $sig   = if ($total -gt 0) { "↑ 净流入" } else { "↓ 净流出" }
        $lines += "- 沪股通:${sh}亿 | 深股通:${sz}亿 | **合计:${total}亿** $sig"
    } else {
        $lines += "- 数据不可用（VM网络受限）"
    }
}
$lines += ""

# ── 板块轮动 ───────────────────────────────────────────────────
$lines += "## 板块"
$lines += ""
if ($supp -and $supp.sectors_top.Count -gt 0) {
    $topStr = ($supp.sectors_top | ForEach-Object {
        $p = $_.pct; $s = if ($p -ge 0) { "+${p}%" } else { "${p}%" }; "$($_.name) $s"
    }) -join " / "
    $botStr = ($supp.sectors_bot | ForEach-Object {
        $p = $_.pct; $s = if ($p -ge 0) { "+${p}%" } else { "${p}%" }; "$($_.name) $s"
    }) -join " / "
    $lines += "**领涨:** $topStr"
    $lines += "**领跌:** $botStr"
} else {
    $lines += "- 数据获取失败"
}
$lines += ""

# ── 港股指数（新浪实时）────────────────────────────────────────
$hkRaw  = Invoke-WebRequest "https://hq.sinajs.cn/list=hkHSI,hkHSTECH" -UseBasicParsing -Headers $headers
$hkText = [System.Text.Encoding]::GetEncoding("GBK").GetString($hkRaw.RawContentStream.ToArray())

$lines += "## 港股"
$lines += ""
$lines += "| 指数 | 收盘 | 涨跌幅 |"
$lines += "|------|------|--------|"

foreach ($pair in @(@("hkHSI","恒生指数"), @("hkHSTECH","恒生科技"))) {
    $code = $pair[0]; $name = $pair[1]
    if ($hkText -match "hq_str_${code}=""([^""]+)""") {
        $f = $Matches[1] -split ","
        if ($f.Count -ge 9 -and $f[6] -ne "") {
            $cur = [double]$f[6]
            $pct = [math]::Round([double]$f[8], 2)
            $pctStr = if ($pct -ge 0) { "+$pct%" } else { "$pct%" }
            $lines += "| $name | $cur | $pctStr |"
        }
    }
}
$lines += ""
$lines += "---"
$lines += "_由 daily_market.ps1 自动生成_"

# ── 写文件 ─────────────────────────────────────────────────────
$lines | Out-File $outputFile -Encoding UTF8
Write-Host "已写入  $outputFile"

"[$(Get-Date -Format 'yyyy-MM-dd HH:mm')] 大盘日报完成  成交额${totalAmt}万亿  $outputFile" |
    Add-Content $logFile -Encoding UTF8
'''

# ─────────────────────────────────────────────
# deploy
# ─────────────────────────────────────────────
for target in TARGETS:
    scripts_dir = target / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    # 1. market_supplement.py
    sup_path = scripts_dir / "market_supplement.py"
    sup_path.write_text(SUPPLEMENT_PY, encoding="utf-8")
    print(f"wrote {sup_path}")

    # 2. daily_market.ps1  (backup old first)
    ps1_path = scripts_dir / "daily_market.ps1"
    if ps1_path.exists():
        bak = scripts_dir / f"daily_market.ps1.bak_before_optimize"
        shutil.copy2(ps1_path, bak)
        print(f"backup  {bak}")

    bot_root = str(target).replace("/", "\\")
    ps1_content = MARKET_PS1_TEMPLATE.replace("{BOT_ROOT}", bot_root)
    ps1_path.write_text(ps1_content, encoding="utf-8")
    print(f"wrote {ps1_path}")

print("\nDone. Test with:")
print("  python C:/Users/jiapeichen/repos/bro/claude1/scripts/market_supplement.py")
print("  pwsh C:/Users/jiapeichen/repos/bro/claude1/scripts/daily_market.ps1")
