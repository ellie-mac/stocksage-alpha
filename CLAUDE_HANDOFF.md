# Claude → Copilot CLI 交接手册

写于 2026-05-19。组织六月底（约 5 周后）停用 Claude，本文把 Claude 累积的关于这个项目和用户的"非显性"知识全部留下来，用于交给 Copilot CLI 无缝接替。

**适用范围**：本文是"交接 brief"，不是"项目文档"。代码、架构、定时任务这些在仓库里已经有专门文档，本文只补充那些只在我（Claude）脑子里、不在代码里的东西。

---

## 1. 项目文档地图（先读这些）

| 文件 | 内容 | 备注 |
|---|---|---|
| `README.md` | 项目简介、入口命令 | 仓库一级文档 |
| `AGENTS.md` | Agent persona（"太子"）+ 行为规则 + memory.md 协议 + 跨 bot relay | **必读** |
| `memory.md` | 项目长期记忆：三大策略、市场制度、因子权重、定时任务、Bot 命令、注意事项 | **必读**（gitignored，仅本地）|
| `ARCHITECTURE.md` | 系统架构详述 | gitignored，仅本地 |
| `SCHEDULED_TASKS.md` | VM 上 Windows Task Scheduler 任务清单 | gitignored，VM 特有 |
| `docs/` | 其他主题文档 | 按需 |

`memory.md` 末尾还有 `[2026-04-28] 待加入因子` 清单等 TODO，定期检查。

---

## 2. 用户档案与协作偏好

### 身份
- **姓名**：Jiapei Chen（jiapeichen@microsoft.com，Microsoft 员工）
- **时区**：北京时间（UTC+8） —— 任何"今晚"、"现在几点"、"还有多久到 X 点"必须按 Asia/Shanghai。**不要用本机 system time 推断 A 股交易时段或 VM scheduler 时间**。
- **语言**：中英文都可，跟随用户最近一条消息的语言；中文回复更自然
- **角色**：项目所有者，量化交易兴趣，对 A 股市场、因子模型熟悉

### 回复风格（严格）
- **简短直接**。不要冗余总结、不要重复用户已知信息。1-2 句能说清就别写一段。
- **结论先行**。不要"分析过程→最后结论"，要"结论 + 1 句依据"。
- **行动优先**。能直接干的就直接干，**不要问"要我做 X 吗"**这类无脑确认（详见下方"自主执行"）。
- **代码引用用 `file:line`** 让用户能跳转。

### 自主执行原则（重要）
- **非破坏性动作直接执行，不要问确认**。包括：编辑文件、跑命令、装包、重启服务、push 到 origin。
- **仅在以下动作前停下确认**：`rm -rf`、`DROP TABLE`、`git reset --hard`、`git push --force`、删持仓数据、改 `.ssh/*`、改系统环境变量、装/卸系统级软件。
- 用户讨厌重复的"要不要继续"提示，会打断 flow。

### 推荐票展示格式（硬性要求）
任何展示主策略 / 筹码 / 金叉选股结果都必须带当天**收盘价**和**涨跌幅**，让用户能核对推荐是否基于当日真实数据：

```
代码 名称  收盘=XXX  涨跌=+X.XX%  score=XX
```

适用：preview_picks、chip_cad、chip_scan、morning_push 等所有选股推荐场景。

### 日志组织
脚本日志写到 `logs/YYYY-MM-DD/<name>.log`，**不要**散在 `scripts/` 根目录或乱命名。logger 用 dated subfolder 方便清理。

### Python 调用简写
命令里写 `python -X utf8 scripts/...`，不要写完整路径 `"C:\Program Files\Python313\python.exe" -X utf8 ...`。`python` 在 PATH 里能解析。

### 禁止动作（AGENTS.md 已写但强调）
- 不改 SSH 配置（`authorized_keys`、`sshd_config`、`.ssh/*`）
- 不装/卸系统级软件
- 不改系统环境变量
- 不删非本项目文件

---

## 3. VM 运维参考

### 登录
```bash
ssh stocksage-vm   # ~/.ssh/config 已配置 → 172.203.226.33 / jiapeichen / ~/.ssh/stocksage_vm
```

RDP（管理任务必需，比如改 Task Scheduler）：
- IP: 172.203.226.33
- User: jiapeichen
- 已配 auto-logon（密码存 Winlogon registry）

VM 启动后**无需手动启服务**：auto-logon + Task Scheduler 全自动接管（feishu_bot、discord_bot 用 AtLogon trigger；15 个数据任务都在 Task Scheduler 里）。

管理类命令必须 RDP + admin PowerShell：
```powershell
cd C:\Users\jiapeichen\repos\stocksage-alpha
& "C:\Program Files\Python313\python.exe" -X utf8 src\setup_scheduler.py
```

### 双向代码同步（git，不要 SCP）

**本地 → VM**：本地 edit → `git push` → `ssh stocksage-vm "cd C:/Users/jiapeichen/repos/stocksage-alpha && git pull"`

- 改了 scheduler：拼接 `&& python -X utf8 src/setup_scheduler.py` 一并注册
- 改了 stock-bot 代码（lark_bot.py 等）：pull 后 kill 进程，bat loop 10s 内自动重启

**VM → 本地**（VM 上跑出的数据文件）：
```bash
ssh stocksage-vm "gh auth setup-git && cd C:/Users/jiapeichen/repos/stocksage-alpha && git push"
# 然后本地 git pull
```
VM 上 git push 必须先 `gh auth setup-git`（用已设置的 `GH_TOKEN` 环境变量）。

**SSH + PowerShell 转义陷阱**：`$_` piping、`WINDOWTITLE` 等通过 SSH 几乎必坏。处理办法：
1. 用 `powershell -EncodedCommand` 传 UTF-16LE base64（最稳）
2. 或把 ps1 写到 `C:/tmp/` 再 `powershell -File` 执行
3. 验进程状态用 `tasklist /FI` 或 `schtasks /query`，不用 `Get-Process | Where-Object MainWindowTitle`（不可靠）

### lark_agent 重启（重要）
**不要从 SSH 直接 `Start-Process cmd /c start.bat`**——SSH 会话断开时新启的 wrapper 会被一起清理掉。

正确方式：用 watchdog scheduled task，或直接 kill 等 watchdog 自愈。

```bash
# 强制立即触发（kill 后这条让 watchdog 立马跑一次）
ssh stocksage-vm 'schtasks /run /tn StockSage_LarkAgent_Watchdog'

# 验证
ssh stocksage-vm 'netstat -ano | findstr :9820'   # 应该 LISTENING
```

watchdog 逻辑见 `C:\Users\jiapeichen\repos\lark-agent\watchdog.ps1`：检测 python.exe 命令行含 `lark_agent`，没有就 `Start-Process cmd /c start.bat`。任务自动每 1-2 分钟跑一次，kill 后等就行。

### 直接调 lark bot 的 HTTP API（不经过飞书）

lark_agent 在 VM 上 127.0.0.1:9820 暴露 Management API，**只在 VM 本地能访问**（需 SSH 进去 curl/python）。

两个 endpoint：

**`POST /push`** — 让某 bot 主动发一条消息到指定 chat（outbound only，不触发 agent 处理）：
```json
{"bot": "首辅", "chat_id": "oc_xxx", "text": "消息内容"}
```

**`POST /chat`** — 模拟 inbound message：让某 bot 收到一条 user 内容、跑完 LLM 处理、**同步返回 reply 文本**。可在外部脚本里直接调任何 bot 拿到答复，不需要走 webhook：
```json
{"bot": "户部尚书", "user_id": "external", "chat_id": "oc_xxx", "text": "评审请求..."}
→ {"ok": true, "reply": "GPT 的回复..."}
```

⚠️ `/chat` 是**同步阻塞**——agent 跑 multi-turn tool calls 可能 30s-10min，HTTP client 要设大 timeout（建议 900s）。同时 reply 也会发到 `chat_id` 那个 lark chat（user 看得到）。

### Bot 模型 + chat_id 表

底层 LLM 配置在 `~/repos/lark-agent/config.toml`：

| Bot | 模型 | 主私聊 chat_id | 角色定位 |
|---|---|---|---|
| 太子 | claude-opus-4.7 | （需从 log 抓）| 量化系统主助手（stocksage-alpha） |
| 户部尚书 | **gpt-5.4** | `oc_42a6b63b6418c03bec927fb43fb5d60d` | "生活笔记"但与太子知识同步，用于双模型交叉验证 |
| 工部尚书 | claude-opus-4.7 | — | 工作笔记 |
| 首辅 | claude-opus-4.7 | `oc_26d013bc7976148ea877c5f856bed1e7` | 综合协调 |
| 西蒙斯 | claude-opus-4.7 | — | bro/simons work_dir |
| 尚书令 | gpt-5.4 | — | — |
| 总舵舵主 | gpt-5.4 | — | — |
| 朋友-GPT-2 | gpt-5.4 | — | — |

`relay_bindings.json` 里的 chat_id 是 user 在群里调过 `relay_to` 工具时的缓存——**不一定是 bot 实际所在的 chat**。各 bot 的私聊 chat_id 没持久化到 session 文件，要从 `lark-agent.log` 里 grep（搜 bot 名 + `oc_` pattern，取出现频次最高的）。

### 使用示例：让户部尚书 (GPT-5.4) 做策略评审

```python
# /tmp/ask_hubu.py
import json, urllib.request
text = open('/tmp/review_request.txt', encoding='utf-8').read()
body = json.dumps({
    'bot': '户部尚书',
    'user_id': 'external',
    'chat_id': 'oc_42a6b63b6418c03bec927fb43fb5d60d',  # 户部尚书私聊
    'text': text,
}, ensure_ascii=False).encode('utf-8')
req = urllib.request.Request(
    'http://127.0.0.1:9820/chat',
    data=body,
    headers={'Content-Type': 'application/json; charset=utf-8'},
)
r = urllib.request.urlopen(req, timeout=900).read().decode('utf-8')
print(json.loads(r)['reply'])
```

```bash
scp /tmp/ask_hubu.py stocksage-vm:/tmp/
ssh stocksage-vm "python -X utf8 /tmp/ask_hubu.py"
```

用途：
- **双模型交叉验证**：太子 (Claude) 写完代码后让户部尚书 (GPT) 评审，看两个模型分歧/共识
- **跨域 ask**：让首辅做综合判断、让工部尚书评工程方案
- **autonomous loop**：脚本自动调多个 bot 协作（不需要人 lark 上 @）

### 相关仓库

**lark-agent**（`C:\Users\jiapeichen\repos\lark-agent`） —— 飞书 bot 8 个 agent 的 Copilot Chat API 后端
- master 分支
- 入口 `lark_agent.py`、wrapper `start.bat`、watchdog `watchdog.ps1`
- 日志 `lark-agent.log`
- 端口 9810（card callback）、9820（management API）
- 配合 GH CLI 拿 Copilot token（每 50 分钟 refresh）
- 与 stocksage-alpha **不在同一仓库**，用户 cwd 经常切

**stocksage-alpha**（本仓库） —— 主项目，VM 上 git pull 后所有 scheduled task 自动跑

### Scheduled Tasks 速查（VM）
完整清单在 `SCHEDULED_TASKS.md`（gitignored），关键的：

| Task | 时间 | 干嘛 |
|---|---|---|
| `StockSage_LarkAgent_Watchdog` | 每 1-2 min | 自动救活 lark_agent |
| `StockSage_LarkBot` | AtLogon + 常驻 | stocksage 飞书 bot |
| `marketcap_Scan` | 15:45 | 收盘后市值扫盘（拆开 15:35 多任务） |
| `hot_Scan` | 16:35 | 热榜（2026-05 从 19:00 提前，不依赖 price_Prefetch）|
| `chip_Night` | 18:00 | 收盘后筹码缓存预取 |
| `main_Scan` | 18:30 | 主策略扫盘，更新 `latest_picks.json` |
| `quality_Prefetch` | 19:00 | 全市场 amt_5d_yi/vol_ratio 预热 → `data/quality_metrics_latest.json`（5 路 scanner 早退用）|
| `golden_Scan` | 19:30 | 金叉扫描 |
| `sideways_Scan` | 20:00 | 横盘策略（8 档 HX/HS，2026-05 新增） |
| `chip_CadScan` | 21:00 | CAD/CADM 推送 |
| `evening_Strategy` | **22:00**（原 `morning_Push`，2026-05-19 改名）| 七路汇总推送（流动性 ≥0.5亿 + 行业黑名单过滤；`--tech-only` opt-in，默认全行业）|
| `sync_Knowledge` | 02:00 | "户部尚书" ↔ "西蒙斯" AI agent 知识同步，bat 不在 git 里 |

---

## 4. 当前 open TODOs / 已知问题

### 4.1 active

**筹码回测 vs 主策略对比（持续追踪）**
- 2026-04-23 结果（step20，7期）：T2 胜率 57% / 均涨 +1.14%，主策略 16期 胜率 60% / +1.19% —— 主策略略胜
- chip_backtest.py 已固定用 CAH T1-T4（去掉 T5 和 MODS）
- 下次重跑条件：缓存再积累 3-6 个月，用 `--step 10` 跑更多期
- 命令：`python -X utf8 scripts/chip_backtest.py [--push-only]`

**watchlist_Updater notify 失败**
- 2026-05-12 20:00 任务 0xC000013A（STATUS_CONTROL_C_EXIT）
- `src/notify/notify.py` 的 `_save_failures()` 无 try/except，并发写 `task_failures.json` 会炸
- 复现策略：下次 watchlist_Updater 非零退出码时，先看 `notify_discord.log` 有没有 wl_Update 条目；若没有就给 `_save_failures()` 加 try/except

**fetcher.py 交易日历缓存合并（暂缓）**
- `fetcher.py` 约 line 1638 用独立缓存 key 调 `ak.tool_trade_date_hist_sina()`
- 与 `common._load_trade_dates()` 的 `trade_dates_sina` key 重复
- 风险：fetcher.py 线程逻辑复杂，动它易破坏 IC/backtest
- 触发条件：下次因别的原因改 `fetcher.py:_get_trade_dates_window()` 时顺手合并

**6 个因子 IC 缺失**
- `value`、`short_interest`、`accruals`、`volume_ratio`、`rsi_signal`、`macd_signal` 仍 `n_periods: 0`
- 不是数据缺口，是数据源未接入。`amihud_illiquidity` 已修，IC=-0.0048（噪声水平）
- 接入数据源后重跑：`python -X utf8 scripts/factor_analysis.py --rolling 6 --step 20 --group AB --out data/factor_ic.json`

**lark_agent 日志 3 小时空白（小谜题，不阻塞）**
- 2026-05-19 17:18→20:32 lark_agent 活着但没写日志一条
- 可能原因：watchdog.ps1 的 `Out-File -Append` 偶尔与 start.bat 的 `>> lark-agent.log` 抢句柄
- 修法：watchdog 改 `Add-Content` 或独立 watchdog.log

---

**P2 / P3 重构待办（来自 2026-05-19 全项目 review — Claude + GPT-5.4 双评审）**

已完成 P0（commit `2111db9`）+ P1（`f01bc92`/`b35addc`/`b2d6c0b`），剩下两个层级仍开。

**P2 — 中等改动，0.5-1 天/项，需先观察新数据再决策**
- ❑ snapshot_store 推广到全部 scanner（替代 70% `*_latest.json` 中间产物）。当前 SQLite snapshots 表只 nightly_scan 用，evening_strategy/reporter/watchlist 都自己 json.loads。改完后 data/ 大幅简化、统一查询入口。**风险**：reporter / watchlist_updater 等下游 reader 全要改。
- ❑ `factors/technical.py`（34 函数）vs `factors/technical/` 包（5 子模块）同名共存 → 重命名其一。**风险**：47 因子引用都要改。
- ❑ 两套权重表 `dict[REGIME_WEIGHTS]`（factors/config.py）vs `dataclass FactorWeights`（factors/scoring.py） → 删一个。靠 `weights_from_config_dict()` 桥接，任一字段名漂移就静默失效。
- ❑ `wait_for_fresh_prices` 从 `jobs/prefetch.py` 抽到 `src/data_freshness.py` 纯库 — 打破 strategies ↔ jobs 双向依赖（gc/sideways/chip 都反向 import jobs.prefetch）。
- ❑ `main_universe.json`（CSI300+500 backtest 用 ~500票）vs `universe_main.json`（全A生产用 ~5500票）名字差异化，比如 `cs300_500_universe.json`。当前命名 smell 但功能不冲突，留观察。
- ❑ `factor_ic*.json` 6 份权重源头追溯混乱（factor_ic.json/factor_ic_main.json/factor_ic_etf.json/factor_ic_fresh.json/factor_ic_rerun.json/factor_ic_smallcap.json）—— 整理一次溯源文档，删过期的。
- ❑ `os._exit(0)` (nightly_scan.py:133) 是治标，治本是找 ThreadPoolExecutor 在 strategy code 里 hang 的真正原因（怀疑 BaoStock socket 或 fetcher fork 子进程继承坏全局）。Work 时不要乱动。

**P3 — 大重构，多天，需 user 决策**
- ❑ **monitor.py 上帝模块**（1895 行，反向 import jobs/strategy_tracker + jobs/prefetch）→ 拆 `monitor/holdings/`, `monitor/midday/`, `monitor/scheduler_tasks/`
- ❑ **fetcher.py 拆数据服务**（2153 行 + 25+ 模块全局 _src_fail/_v8_lock/_bs_module/_lhb_cache/...）。multiprocessing fork 子进程继承父进程坏状态是隐性 bug，独立成 HTTP/gRPC service 一劳永逸。
- ❑ **notify/ → notification gateway**（飞书+微信+Discord 散落 24+ 文件 inline 调）→ 一个 HTTP svc，所有 scanner POST，token 单源
- ❑ **reporter.py 拆三层**（1701 行）→ templates / data aggregation / publish

P0 / P1 完成情况（参考）：commit `2111db9` (P0: read_json/normalize_code/fetcher 锁/prefetch workers)、`f01bc92` (P1.1 alert_config 收口 + lru_cache)、`b35addc` (P1.2 task_failures via common + P1.3 save_picks file_lock)、`b2d6c0b` (P1.4 prefetch_quality Tushare batch fast path)

### 4.2 closed（保留参考，遇类似症状用得上）

**chip_Night 数据停滞 3 天（2026-05-18 21:14 已解决）**
- 根因：5-15 18:00 那次 chip_Night 的 python 进程（PID 6296）扫描完没退出，3 天持有 `src/logs/chip_scan_night.log` 句柄
- 后续 chip_Night 调度 → bat `>>` 拿不到文件 → cmd 静默失败 → errorlevel=0（Task Scheduler 谎报成功）→ chip_CadScan 自愈逻辑 PermissionError
- 修复：`Stop-Process -Id 6296 -Force` + `run_chip_night.bat` / `run_cad_scan.bat` 加 `task_probe.log` 探针
- **教训**：看到 "Task Scheduler 报成功但 output mtime 不变" 立刻查孤儿进程：`Get-CimInstance Win32_Process | Where-Object CmdLine -like "*<script>*"`。long-running python + `>>` append 是隐形地雷。
- **未做的预防项**（下次顺手）：daily_scan.py 加 atexit / sys.exit；找挂死位置（ThreadPoolExecutor 或 baostock socket）；bat 检测 redirect 失败时 exit /b N；logger 改每日独立文件

---

## 5. 近期方向（看 git log 也能看出，强调要点）

最近 commits：
- `e44a03c` unified quality gate（amt_5d_yi + vol_ratio）across 4 scanners
- `0d78cd4` morning_push 应用 TMT-only industry filter 到所有 7 路策略源
- `7487bc6` sideways 默认 TMT（科技）行业过滤
- `a6b8fce` morning_Push 移到 22:00（在所有 scan 完成之后）

**当前重点**：
1. **TMT-only 行业过滤**成为多策略推荐的默认（sideways 已默认开，morning_push 7 路全应用）
2. **统一质量门**（amt_5d_yi + vol_ratio）覆盖 4 个 scanner —— 流动性 + 量能联合门槛
3. **morning_push 时间窗调整**到 22:00 之后，确保所有 scan 数据齐全

---

## 6. 我的记忆原始档（参考用）

我（Claude）的逐项原始记忆在 `C:/Users/jiapeichen/.claude/projects/C--Users-jiapeichen-repos-stocksage-alpha/memory/`，共 14 个文件：

```
MEMORY.md                              （索引）
feedback_autonomous_execution.md      （不要问确认）
feedback_lark_agent_restart.md        （用 schtasks/watchdog，不要 SSH+Start-Process）
feedback_log_organization.md          （logs/YYYY-MM-DD/）
feedback_picks_with_price.md          （选股带收盘价+涨跌幅）
feedback_python_invocation.md         （python -X utf8 简写）
feedback_timezone.md                  （北京时间 UTC+8）
feedback_vm_sync.md                   （git push/pull 双向同步）
project_chip_night_stale.md           （孤儿进程锁日志案例）
project_todo_rebacktest.md            （6 个因子待重跑 IC）
project_watchlist_updater_notify.md   （notify 失败待查）
reference_sync_knowledge.md           （户部尚书↔西蒙斯 sync 任务）
reference_vm_login.md                 （SSH/RDP 信息）
todo_chip_vs_main_backtest.md         （筹码 vs 主策略对比）
todo_trade_date_cache.md              （fetcher.py 缓存合并暂缓）
```

本文已经把这些内容整合进来。Copilot CLI 直接读本文即可，不必再读原始记忆。

---

## 7. 给接替者（Copilot CLI）的几句话

1. **memory.md 在每个 session 开头读一次**（AGENTS.md 第 41 行规定），不要中途反复读。
2. **批量写 memory.md**：一个 session 攒到最后一次写，不要每个回合都写。
3. **遵循 AGENTS.md 的 "太子" persona**：直接给判断、引用具体数值（score / IC / winner_rate）、区分系统信号与主观判断。
4. **跨 bot 协作**用 `relay_to(bot="工部尚书/户部尚书/首辅", message=...)` —— 不要让用户自己去问其他 bot。
5. **回复风格**：短、直接、行动优先、收盘价必带、北京时间为准、非破坏性动作直接执行。
6. **遇到"Task Scheduler 报成功但 output mtime 不变"** —— 查孤儿进程，是隐形地雷。
7. **VM 上执行命令** 优先用 `powershell -EncodedCommand`（UTF-16LE base64），SSH 转义会咬你。

祝接班顺利。

— Claude Opus 4.7, 2026-05-19 20:35 北京时间
