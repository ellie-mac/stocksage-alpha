#!/usr/bin/env python3
"""
合并 户部尚书 的 s1 + s2 session，清除卡住状态，统一映射到 s1。
"""
import glob, json, sys
from pathlib import Path
from datetime import datetime

SESSIONS_DIR = Path(r"C:\Users\jiapeichen\repos\lark-agent\data\sessions")

# 找到户部尚书的 session 文件（hash = f83279cb）
matches = list(SESSIONS_DIR.glob("*f83279cb.json"))
if not matches:
    print("找不到 *f83279cb.json，退出")
    sys.exit(1)

path = matches[0]
print(f"找到文件: {path.name}")

data = json.loads(path.read_text(encoding="utf-8"))
sessions = data.get("sessions", {})

s1 = sessions.get("s1", {})
s2 = sessions.get("s2", {})

h1 = s1.get("history", [])
h2 = s2.get("history", [])

print(f"s1 消息数: {len(h1)}，s2 消息数: {len(h2)}")

# 合并并按时间排序
def ts(msg):
    t = msg.get("timestamp", "")
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00"))
    except Exception:
        return datetime.min

merged = sorted(h1 + h2, key=ts)

# 如果末尾是 user 消息（无对应 assistant 回复），加一条占位回复
if merged and merged[-1].get("role") == "user":
    last_ts = merged[-1].get("timestamp", "")
    merged.append({
        "role": "assistant",
        "content": "[上一条消息因系统故障未能回复，请重新提问]",
        "timestamp": last_ts,
    })
    print("末尾挂起的 user 消息已插入占位回复")

print(f"合并后消息数: {len(merged)}")

# 用 s2 的 agent_type，清空 agent_session_id 让 lark-agent 重建连接
s1["history"] = merged
s1["agent_session_id"] = ""
s1["past_agent_session_ids"] = []

# 把所有聊天的 active_session 指向 s1
active = data.get("active_session", {})
for k in list(active.keys()):
    if not k.startswith("relay:"):   # relay session 保持原样
        active[k] = "s1"

user_sessions = data.get("user_sessions", {})
for k in list(user_sessions.keys()):
    if not k.startswith("relay:"):
        user_sessions[k] = ["s1"]

# 删除 s2（s3 relay session 保留）
sessions.pop("s2", None)
sessions["s1"] = s1

data["sessions"] = sessions
data["active_session"] = active
data["user_sessions"] = user_sessions

# 写回
path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"完成，已写回 {path.name}")
print(f"active_session: {json.dumps(active, ensure_ascii=False)}")
