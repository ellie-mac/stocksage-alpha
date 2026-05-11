"""一次性修复 watchlist_dynamic.json 中名字被存成 sz/sh+代码 的错误条目。"""
import json, os, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

WL_PATH    = ROOT / "data" / "watchlist_dynamic.json"
NAMES_PATH = ROOT / "data" / "stock_names.json"

# 加载名字映射（stock_names.json 的 key 格式是 000001.SZ / 600000.SH）
raw_names = json.loads(NAMES_PATH.read_text(encoding="utf-8"))
name_map: dict[str, str] = {}
for key, val in raw_names.items():
    code6 = key.split(".")[0]
    name_map[code6] = val["name"] if isinstance(val, dict) else val

def _is_bad_name(name: str, code: str) -> bool:
    """名字是 sz/sh + 代码格式，或者名字就等于代码本身。"""
    return bool(re.fullmatch(r"(sh|sz|bj)\d{6}", name)) or name == code

wl = json.loads(WL_PATH.read_text(encoding="utf-8"))
fixed = 0
for entry in wl:
    code  = entry.get("code", "")
    name  = entry.get("name", "")
    if _is_bad_name(name, code):
        correct = name_map.get(code)
        if correct:
            print(f"  {code}: '{name}' → '{correct}'")
            entry["name"] = correct
            fixed += 1
        else:
            print(f"  {code}: '{name}' → (未找到，保留)")

tmp = str(WL_PATH) + ".tmp"
with open(tmp, "w", encoding="utf-8") as f:
    json.dump(wl, f, ensure_ascii=False, indent=2)
os.replace(tmp, WL_PATH)
print(f"\n完成，共修复 {fixed} 条")
