"""
统一监控脚本 v2 - 全功能合并版
功能:
1. ETF系统性下跌检测
2. QDII溢价监控
3. 观察池到价提醒
4. MA5/MA20金叉死叉 (量价确认+MACD辅助+周线共振+连续破位)
5. 持仓做T信号 (超跌反弹/冲高减仓/MA20支撑)
6. 收盘日报汇总 (15:05后运行时自动触发)
7. 策略共振检测 (筹码C0/C1 + 扶梯E0 交叉验证, 收盘后运行)
8. 止盈/止损信号 (盈利回撤/亏损止损/趋势反转)

改进规则:
- 金叉确认: MA5上穿MA20 + 当日放量(>5日均量)
- MACD辅助: DIF>0(零轴上) → 强信号; DIF<0 → 弱信号
- 周线共振: 周收盘>周MA20 → 中期趋势向上,信号更可靠
- 破位确认: 连续2天收盘 < MA20 才算有效
- 去重通知: 同一信号同一天只推一次飞书
- 做T辅助: 连跌3天+偏离MA20>-8% → 低吸; 连涨3天+偏离MA5>+5% → 高抛
- 策略共振: 读取筹码/扶梯扫描结果，多策略命中的票优先推荐
"""
import sys
import os
import urllib.request
import json
import re
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('http_proxy', 'http://127.0.0.1:7890')
os.environ.setdefault('https_proxy', 'http://127.0.0.1:7890')

# ========== 命令行参数 ==========
NO_PUSH = '--no-push' in sys.argv  # 不推送飞书，仅输出到控制台供审核
PUSH_SAVED = '--push-saved' in sys.argv  # 推送上次保存的信号（审核通过后用）
RESONANCE_ONLY = '--resonance' in sys.argv  # 只运行策略共振检测
ALERTS_FILE = Path(os.path.dirname(os.path.abspath(__file__))) / '.pending_alerts.json'

# ========== 推送已保存信号（审核通过后） ==========
if PUSH_SAVED:
    if ALERTS_FILE.exists():
        saved = json.loads(ALERTS_FILE.read_text(encoding='utf-8'))
        title, lines = saved['title'], saved['lines']
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from notify.notify import push_feishu_card
            push_feishu_card(title, lines)
            print(f'[OK] 已推送: {title}')
            ALERTS_FILE.unlink()
        except Exception as e:
            print(f'[WARN] 推送失败: {e}')
    else:
        print('[INFO] 无待推送信号')
    sys.exit(0)

# ========== 交易时间检查 ==========
now = datetime.now()
if __name__ == '__main__':
    if RESONANCE_ONLY:
        # --resonance 模式: 跳过时段检查，直接运行共振检测
        pass
    elif now.weekday() >= 5 or now.hour < 9 or (now.hour == 9 and now.minute < 15):
        # 周末跳过; 9:15前跳过
        print('非交易时段，跳过')
        sys.exit(0)
    elif 16 <= now.hour < 21:
        # 16:00-20:59: 非活跃时段，跳过（除了15:05-15:59的收盘总结）
        if not (now.hour == 15 and now.minute >= 5):
            print('非交易时段，跳过')
            sys.exit(0)

IS_CLOSING_SUMMARY = (now.hour >= 15) or RESONANCE_ONLY  # 15点后或--resonance = 收盘模式

# ========== 配置 ==========
MA_SHORT = 5
MA_LONG = 20
KLINE_DAYS = 35  # 多取几天保证MACD计算
WEEKLY_KLINE_WEEKS = 30  # 30周数据

# 通知去重缓存
NOTIFY_CACHE_FILE = Path(os.path.dirname(os.path.abspath(__file__))).parent / 'copilot' / 'data' / '.notify_cache.json'

# ---------- ETF列表 (名称从API动态获取，这里只是注释备忘) ----------
ETF_LIST = [
    ('sh', '588060'),   # 科创50ETF
    ('sh', '588200'),   # 科创芯片ETF
    ('sz', '159516'),   # 半导体设备ETF
    ('sh', '515050'),   # 通信ETF
    ('sz', '159949'),   # 创业板50ETF
    ('sh', '515220'),   # 煤炭ETF
    ('sz', '159667'),   # 工业母机ETF
    ('sh', '588780'),   # 科创芯片设计ETF
    ('sh', '513880'),   # 日经225ETF
    ('sh', '513100'),   # 纳指ETF
    ('sh', '513310'),   # 中韩半导体ETF
]

# (Watchlist已合并到OBSERVATION_POOL_ACTIVE，不再单独维护)

# ---------- 持仓列表 (做T监控) ----------
PORTFOLIO = [
    # A号 - 坚定持有
    ('sz', '002468', '申通快递', 18.06),
    ('sh', '000792', '盐湖股份', 33.712),
    ('sh', '603993', '洛阳钼业', 23.105),
    ('sz', '300953', '震裕科技', 158.511),
    ('sh', '688525', '佰维存储', 260.615),
    ('sz', '300475', '香农芯创', 158.955),
    ('sh', '688059', '华锐精密', 122.116),
    ('sh', '688257', '新锐股份', 75.091),
    ('sh', '688549', '中巨芯', 30.157),
    # A号 - 持有
    ('sh', '603588', '高能环境', 10.236),
    ('sh', '688401', '路维光电', 78.998),
    ('sh', '688530', '欧莱新材', 59.308),
    ('sh', '601869', '长飞光纤', 458.0),
    # A号 - 观察
    ('sz', '301219', '腾远钴业', 83.507),
    # A号 - 待清仓
    ('sh', '688515', '裕太微', 229.047),
    # B号
    ('sh', '603444', '吉比特', 370.133),
    ('sh', '603893', '瑞芯微', 194.53),
    ('sh', '603629', '利通电子', 195),
    ('sz', '300102', '乾照光电', 33.476),
    ('sh', '600378', '昊华科技', 58.733),
    ('sz', '002156', '通富微电', 63.005),
    ('sz', '300236', '上海新阳', 100.008),
    ('sz', '300346', '南大光电', 63.905),
    ('sh', '600667', '太极实业', 17.512),
    ('sz', '001267', '汇绿生态', 50.444),
    ('sh', '605589', '圣泉集团', 59.005),
    ('sz', '001270', '铖昌科技', 141.881),
    ('sz', '002273', '水晶光电', 37.503),
    # 股池号
    ('sz', '301358', '湖南裕能', 97.579),
    ('sz', '002810', '山东赫达', 26.48),
    ('sz', '002170', '芭田股份', 13.161),
    ('sh', '600801', '华新建材', 22.75),
    ('sh', '601069', '西部黄金', 33.213),
    ('sz', '002033', '丽江股份', 9.884),
    ('sz', '000933', '神火股份', 32.016),
    ('sh', '605228', '神通科技', 14.736),
    ('sh', '600711', '盛屯矿业', 14.184),
]

# ---------- 择机卖出清单(接近成本价推送提醒) ----------
SELL_ON_REBOUND = {
    '300102',  # 乾照光电 33.5
    '301358',  # 湖南裕能 97.6
    '002810',  # 山东赫达 26.5
    '600801',  # 华新建材 22.8
    '605228',  # 神通科技 14.7
    '002033',  # 丽江股份 9.9
    '000933',  # 神火股份 32.0
    '601069',  # 西部黄金 33.2
    '002468',  # 申通快递 18.0
    '603629',  # 利通电子 212.9
    '688515',  # 裕太微 229.0
    '300953',  # 震裕科技 158.5
    '301219',  # 腾远钴业 63.5
    '603893',  # 瑞芯微 194.5
    '001270',  # 铖昌科技 141.9
    '603444',  # 吉比特 370.1
    '002170',  # 芭田股份 13.2
    '600711',  # 盛屯矿业 14.2
}

# ---------- 观察池-主动(到价提醒) ----------
OBSERVATION_POOL_ACTIVE = [
    # 格式: (market, code, name, category) — 买入信号由MA动态判断
    # --- CPO/光模块 ---
    ('sz', '300308', '中际旭创', 'CPO'),
    ('sz', '300394', '天孚通信', 'CPO'),
    ('sz', '300620', '光库科技', 'CPO'),
    ('sz', '002281', '光迅科技', 'CPO'),
    ('sh', '688313', '仕佳光子', 'CPO'),
    ('sh', '688498', '源杰科技', 'CPO光芯片'),
    ('sz', '301205', '联特科技', 'CPO光交换'),
    ('sh', '688195', '腾景科技', 'CPO光学元件'),
    ('sz', '300502', '新易盛', '光模块'),
    ('sh', '688205', '德科立', '光模块'),
    ('sh', '688807', '优迅股份', '光芯片'),
    ('sz', '000988', '华工科技', '激光/光模块'),
    ('sz', '002384', '东山精密', 'PCB/光模块'),
    # --- 光纤光缆/MPO ---
    ('sh', '600522', '中天科技', '光纤光缆'),
    ('sh', '600487', '亨通光电', '光纤光缆'),
    ('sh', '601869', '长飞光纤', '光纤光缆'),
    ('sh', '603618', '杭电股份', '光纤光缆'),
    ('sh', '600105', '永鼎股份', '光纤光缆'),
    ('sz', '300570', '太辰光', 'MPO连接器龙头'),
    # --- 存储 ---
    ('sh', '688008', '澜起科技', '存储'),
    ('sh', '688123', '聚辰股份', '存储芯片'),
    ('sh', '603986', '兆易创新', '存储芯片'),
    ('sz', '301308', '江波龙', '存储模组'),
    ('sz', '001309', '德明利', '存储芯片'),
    ('sz', '000021', '深科技', '封测/存储'),
    # --- 封装 ---
    ('sh', '600584', '长电科技', '封装'),
    ('sz', '002156', '通富微电', '封装'),
    # --- MLCC ---
    ('sz', '300408', '三环集团', 'MLCC'),
    ('sz', '000636', '风华高科', 'MLCC'),
    ('sz', '300285', '国瓷材料', 'MLCC'),
    ('sh', '605376', '博迁新材', 'MLCC'),
    ('sh', '603678', '火炬电子', 'MLCC'),
    # --- PCB/载板 ---
    ('sz', '002463', '沪电股份', 'PCB'),
    ('sz', '002916', '深南电路', 'PCB载板'),
    ('sz', '002436', '兴森科技', 'PCB载板'),
    ('sz', '002938', '鹏鼎控股', 'PCB'),
    ('sz', '300476', '胜宏科技', 'PCB'),
    ('sh', '600183', '生益科技', 'PCB材料'),
    ('sh', '601208', '东材科技', 'PCB材料'),
    ('sh', '603228', '景旺电子', 'PCB'),
    # --- 铜箔/铜连接 ---
    ('sh', '688388', '嘉元科技', '铜箔'),
    ('sz', '301217', '铜冠铜箔', '铜箔'),
    ('sz', '301511', '德福科技', '铜箔'),
    ('sh', '600237', '铜峰电子', 'PET铜箔/颠覆性'),
    ('sz', '301486', '致尚科技', '铜连接'),
    ('sz', '002897', '意华股份', '铜连接'),
    # --- 液冷 ---
    ('sz', '300499', '高澜股份', '液冷'),
    ('sz', '300990', '同飞股份', '液冷'),
    ('sz', '301018', '申菱环境', '液冷'),
    # --- 芯片/设备 ---
    ('sz', '002371', '北方华创', '设备'),
    ('sh', '688041', '海光信息', 'GPU'),
    ('sh', '688981', '中芯国际', '晶圆代工'),
    ('sh', '688256', '寒武纪', 'AI芯片'),
    # --- 网络 ---
    ('sh', '688702', '盛科通信', '网络芯片'),
    ('sh', '688515', '裕太微', '网络芯片'),
    ('sz', '000063', '中兴通讯', '网络设备'),
    # --- AI电源 ---
    ('sz', '002364', '中恒电气', 'AI电源'),
    ('sz', '002851', '麦格米特', '电源'),
    # --- AI服务器 ---
    ('sh', '601138', '工业富联', 'AI服务器'),
    # --- 其他 ---
    ('sz', '002428', '云南锗业', '光材料'),
    ('sz', '301183', '东田微', '光学薄膜'),
    ('sh', '688025', '杰普特', '激光设备'),
    ('sz', '300666', '江丰电子', '靶材'),
    # --- 电子特气 ---
    ('sz', '002549', '中船特气', '电子特气'),
    ('sh', '688268', '华特气体', '电子特气'),
    ('sh', '600378', '昊华科技', '电子特气/WF6'),
    # --- 涨价链观察（等回调买入）---
    # ⭐⭐⭐⭐⭐ 断供级
    ('sz', '300346', '南大光电', 'NF3三氟化氮'),
    ('sh', '688146', '中船特气', 'NF3(情绪温度计,不买)'),
    ('sh', '600206', '有研新材', '溅射靶材'),
    ('sz', '300666', '江丰电子', '溅射靶材'),
    ('sz', '300706', '阿石创', '溅射靶材/钼靶全球第一'),
    ('sh', '688530', '欧莱新材', '面板靶材龙头'),
    # ⭐⭐⭐⭐ 逻辑硬
    ('sz', '002409', '雅克科技', '半导体前驱体'),
    ('sh', '688596', '正帆科技', '前驱体/拐点年'),
    ('sz', '300395', '菲利华', '石英Q布/下一代电子布'),
    ('sz', '002080', '中材科技', '电子布/泰山玻纤'),
    ('sh', '600176', '中国巨石', '电子布'),
    ('sh', '605589', '圣泉集团', '电子级树脂/EMC'),
    ('sh', '603256', '宏和科技', '电子布(泡沫,不买)'),
    ('sh', '601208', '东材科技', '电子布/树脂'),
    ('sh', '600641', '先导基电', '磷化铟/离子注入'),
    ('sz', '002428', '云南锗业', '磷化铟跟风'),
    ('sh', '688019', '安集科技', 'CMP抛光液'),
    ('sz', '300054', '鼎龙股份', 'CMP抛光垫'),
    ('sh', '688138', '清溢光电', '光掩模版'),
    ('sh', '688401', '路维光电', '光掩模版'),
    ('sh', '688721', '龙图光罩', '光掩模版/纯半导体'),
    ('sh', '688300', '联瑞新材', '球形硅微粉'),
    ('sh', '688535', '华海诚科', 'EMC塑封料'),
    # ⭐⭐⭐ 逻辑对但有瑕疵
    ('sh', '600183', '生益科技', '覆铜板'),
    ('sz', '002636', '金安国纪', '覆铜板'),
    ('sh', '688519', '南亚新材', '覆铜板/AI'),
    ('sh', '603186', '华正新材', '覆铜板'),
    ('sz', '002916', '深南电路', '封装基板/ABF'),
    ('sz', '002436', '兴森科技', '封装基板/ABF'),
    ('sh', '688545', '兴福电子', '湿电子化学品'),
    ('sz', '300236', '上海新阳', '光刻胶/铜电镀'),
    ('sh', '603650', '彤程新材', '光刻胶'),
    ('sz', '002463', '沪电股份', 'PCB'),
    ('sz', '002938', '鹏鼎控股', 'PCB'),
    ('sz', '300476', '胜宏科技', 'PCB'),
    ('sz', '002407', '多氟多', '无水氢氟酸/六氟磷酸锂'),
    ('sh', '688268', '华特气体', 'NF3/WF6特气'),
    ('sz', '301526', '国际复材', '电子布全产业链'),
    ('sh', '688126', '沪硅产业', '硅片'),
    ('sh', '605358', '立昂微', '硅片'),
    # --- 新增持仓 ---
    ('sz', '001267', '汇绿生态', '光模块/转型'),
    ('sh', '600667', '太极实业', 'HBM封测/SK海力士'),
    ('sh', '603938', '三孚股份', '电子特气/硅烷'),
]

# QDII溢价阈值
QDII_THRESHOLDS = {
    '513310': ('中韩半导体', 12, 10),
    '513100': ('纳指', 8, 5),
    '513880': ('日经225', 3, 2),
}

# ---------- 观察池-策略 (从策略共振自动生成, 信号弱化时清理) ----------
# 格式同主动观察池, 但由策略共振模块动态管理
# 文件存储: data/strategy_pool.json
STRATEGY_POOL_FILE = Path(os.path.dirname(os.path.abspath(__file__))).parent / 'data' / 'strategy_pool.json'


def load_strategy_pool():
    """加载策略观察池"""
    if STRATEGY_POOL_FILE.exists():
        try:
            return json.loads(STRATEGY_POOL_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass
    return []


def save_strategy_pool(pool):
    """保存策略观察池"""
    STRATEGY_POOL_FILE.write_text(json.dumps(pool, ensure_ascii=False, indent=2), encoding='utf-8')


def load_strategy_results():
    """读取各策略最新扫描结果，返回 {code: {strategies: [...], tier: best_tier, info: {...}}}"""
    data_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent / 'data'
    results = {}  # code -> {strategies: set, best_tier, info}

    # 1. 筹码策略 chip_cad_latest.json
    chip_file = data_dir / 'chip_cad_latest.json'
    if chip_file.exists():
        try:
            chip = json.loads(chip_file.read_text(encoding='utf-8'))
            tiers = chip.get('tiers', {})
            for tier in ['C0', 'C1', 'C2']:  # 只取强档
                for pick in tiers.get(tier, []):
                    code = pick.get('code', '')
                    if code not in results:
                        results[code] = {'strategies': [], 'tiers': [], 'name': pick.get('name', ''), 'info': {}}
                    results[code]['strategies'].append('chip')
                    results[code]['tiers'].append(tier)
                    results[code]['info']['winner_rate'] = pick.get('winner_rate')
                    results[code]['info']['industry'] = pick.get('industry', '')
        except Exception as e:
            print(f'[策略共振] 读取筹码数据失败: {e}')

    # 2. 扶梯策略 escalator_latest.json
    esc_file = data_dir / 'escalator_latest.json'
    if esc_file.exists():
        try:
            esc = json.loads(esc_file.read_text(encoding='utf-8'))
            tiers = esc.get('tiers', {})
            for tier in ['E0']:  # 只取E0
                for pick in tiers.get(tier, []):
                    code = pick.get('code', '')
                    if code not in results:
                        results[code] = {'strategies': [], 'tiers': [], 'name': pick.get('name', ''), 'info': {}}
                    results[code]['strategies'].append('escalator')
                    results[code]['tiers'].append(tier)
                    results[code]['info']['r2'] = pick.get('r2')
                    results[code]['info']['slope_pct'] = pick.get('slope_pct')
                    results[code]['info']['industry'] = pick.get('industry', results[code]['info'].get('industry', ''))
        except Exception as e:
            print(f'[策略共振] 读取扶梯数据失败: {e}')

    # 3. 金叉共振 (只取G0, 用于加分不单独推)
    gc_dir = sorted(data_dir.glob('golden_cross_*.json'))
    if gc_dir:
        gc_file = gc_dir[-1]  # 最新的
        try:
            gc = json.loads(gc_file.read_text(encoding='utf-8'))
            g0_picks = gc.get('G0', []) if isinstance(gc, dict) else []
            # 如果是 tiers 结构
            if not g0_picks and isinstance(gc, dict) and 'tiers' in gc:
                g0_picks = gc['tiers'].get('G0', [])
            for pick in g0_picks:
                code = pick.get('code', '')
                if code in results:  # 只作为已有信号的加分
                    results[code]['strategies'].append('golden_cross')
                    results[code]['tiers'].append('G0')
        except Exception:
            pass

    return results


def filter_resonance_picks(results):
    """过滤策略结果: 排除ST/北证, 要求多策略命中或高档位"""
    filtered = []
    portfolio_codes = {code for _, code, _, _ in PORTFOLIO}
    
    for code, data in results.items():
        name = data.get('name', '')
        # 排除ST
        if 'ST' in name or 'st' in name:
            continue
        # 排除北证 (8开头/4开头/9开头)
        if code.startswith('8') or code.startswith('4') or code.startswith('9'):
            continue
        # 已持仓标记（不排除，共振强信号可加仓）
        in_portfolio = code in portfolio_codes

        n_strategies = len(set(data['strategies']))
        best_chip = None
        for t in data['tiers']:
            if t in ('C0', 'C1', 'C2'):
                if best_chip is None or t < best_chip:
                    best_chip = t
        has_escalator = 'escalator' in data['strategies']
        has_gc = 'golden_cross' in data['strategies']

        # 共振评分
        score = 0
        if best_chip == 'C0': score += 3
        elif best_chip == 'C1': score += 2
        elif best_chip == 'C2': score += 1
        if has_escalator: score += 3
        if has_gc: score += 1

        # 至少2分才入选 (C1单独=2分OK, C2单独=1分不够, E0单独=3分OK)
        if score >= 2:
            filtered.append({
                'code': code,
                'name': name,
                'score': score,
                'strategies': list(set(data['strategies'])),
                'tiers': list(set(data['tiers'])),
                'n_strategies': n_strategies,
                'info': data['info'],
                'in_portfolio': in_portfolio,
            })

    # 按分数降序
    filtered.sort(key=lambda x: -x['score'])
    return filtered


def format_resonance_alert(picks):
    """格式化策略共振结果为推送文本"""
    if not picks:
        return []

    lines = ['', '🧩 策略共振发现', '━' * 20]
    
    # 分级展示
    top_picks = [p for p in picks if p['score'] >= 4]  # 双重共振
    good_picks = [p for p in picks if 2 <= p['score'] < 4]

    if top_picks:
        lines.append('⭐ 双重/三重共振:')
        for p in top_picks[:10]:
            tier_str = '+'.join(p['tiers'])
            info = p['info']
            detail = []
            if p.get('in_portfolio'):
                detail.append('💰已持仓-可加仓')
            if info.get('winner_rate'):
                detail.append(f'获利盘{info["winner_rate"]:.0f}%')
            if info.get('r2'):
                detail.append(f'R²={info["r2"]:.2f}')
            if info.get('industry'):
                detail.append(info['industry'])
            lines.append(f'  {p["name"]}({p["code"]}) [{tier_str}] {" ".join(detail)}')

    if good_picks:
        lines.append('📋 单策略强信号:')
        for p in good_picks[:15]:
            tier_str = '+'.join(p['tiers'])
            info = p['info']
            detail = []
            if p.get('in_portfolio'):
                detail.append('💰已持仓-可加仓')
            if info.get('winner_rate'):
                detail.append(f'获利盘{info["winner_rate"]:.0f}%')
            if info.get('r2'):
                detail.append(f'R²={info["r2"]:.2f}')
            if info.get('industry'):
                detail.append(info['industry'])
            lines.append(f'  {p["name"]}({p["code"]}) [{tier_str}] {" ".join(detail)}')

    lines.append(f'共{len(picks)}只，显示前{min(25, len(picks))}只')
    return lines


# ========== 工具函数 ==========
def get_realtime_quotes(codes_str):
    """获取实时行情"""
    url = f'http://qt.gtimg.cn/q={codes_str}'
    resp = urllib.request.urlopen(urllib.request.Request(url)).read().decode('gbk')
    results = {}
    for line in resp.strip().split(';'):
        if '~' not in line:
            continue
        p = line.split('~')
        if len(p) < 33:
            continue
        results[p[2]] = {
            'name': p[1],
            'price': float(p[3]) if p[3] else 0,
            'chg': float(p[32]) if p[32] else 0
        }
    return results


def get_kline(market, code, days=None):
    """获取日K线(前复权)"""
    d = days or KLINE_DAYS
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}{code},day,,,{d},qfq'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=10).read().decode('utf-8')
        data = json.loads(resp)
        stock_key = f'{market}{code}'
        stock_data = data.get('data', {}).get(stock_key, {})
        klines = stock_data.get('qfqday') or stock_data.get('day') or []
        closes = [float(k[2]) for k in klines]
        volumes = [float(k[5]) for k in klines] if all(len(k) > 5 for k in klines) else []
        return closes, volumes
    except Exception as e:
        return [], []


def get_weekly_kline(market, code):
    """获取周K线"""
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}{code},week,,,{WEEKLY_KLINE_WEEKS},qfq'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=10).read().decode('utf-8')
        data = json.loads(resp)
        stock_key = f'{market}{code}'
        stock_data = data.get('data', {}).get(stock_key, {})
        klines = stock_data.get('qfqweek') or stock_data.get('week') or []
        closes = [float(k[2]) for k in klines]
        return closes
    except Exception:
        return []


def calc_ma(values, period):
    """计算MA(返回今天和昨天)"""
    if len(values) < period + 1:
        return None, None
    ma_today = sum(values[-period:]) / period
    ma_yesterday = sum(values[-period - 1:-1]) / period
    return ma_today, ma_yesterday


def calc_ema(values, period):
    """计算EMA序列"""
    if len(values) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append((v - ema[-1]) * multiplier + ema[-1])
    return ema


def calc_macd(closes):
    """计算MACD(12,26,9), 返回(DIF, DEA, MACD柱)"""
    if len(closes) < 35:
        return None, None, None
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    # DIF = EMA12 - EMA26 (对齐长度)
    offset = 26 - 12
    dif_series = [ema12[i + offset] - ema26[i] for i in range(len(ema26))]
    if len(dif_series) < 9:
        return None, None, None
    dea_series = calc_ema(dif_series, 9)
    if not dea_series:
        return None, None, None
    return dif_series[-1], dea_series[-1], 2 * (dif_series[-1] - dea_series[-1])


def check_weekly_above_ma20(market, code):
    """检查周线是否在MA20上方"""
    w_closes = get_weekly_kline(market, code)
    if len(w_closes) < 20:
        return None  # 数据不足
    w_ma20 = sum(w_closes[-20:]) / 20
    return w_closes[-1] > w_ma20


# ---------- 通知去重 ----------
def load_notify_cache():
    """加载今日已通知缓存"""
    try:
        if NOTIFY_CACHE_FILE.exists():
            data = json.loads(NOTIFY_CACHE_FILE.read_text(encoding='utf-8'))
            if data.get('date') == str(date.today()):
                return set(data.get('notified', [])), data
    except Exception:
        pass
    return set(), {'date': str(date.today()), 'notified': []}


def save_notify_cache(notified_set):
    """保存通知缓存"""
    try:
        data = {'date': str(date.today()), 'notified': list(notified_set)}
        NOTIFY_CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass


def is_new_alert(alert_key, notified_set):
    """检查是否是新信号(未通知过)"""
    return alert_key not in notified_set


# ========== 主逻辑 ==========
notified_today, _ = load_notify_cache()
print(f'{"📊 收盘日报" if IS_CLOSING_SUMMARY else "📊 盘中监控"} ({now.strftime("%Y-%m-%d %H:%M")})')
print('=' * 65)

# ========== Part 1: ETF + 系统性下跌 ==========
etf_codes_str = 'sh000001,' + ','.join(f'{m}{c}' for m, c in ETF_LIST)
etf_results = get_realtime_quotes(etf_codes_str)

shanghai = etf_results.get('000001', {})
sh_chg = shanghai.get('chg', 0)

alerts_etf = []
etf_drop_count = 0

print(f'\n📈 上证: {shanghai.get("price", 0)} ({sh_chg:+.2f}%)')
print('  ETF:')
for _, code in ETF_LIST:
    info = etf_results.get(code)
    if not info or info['price'] == 0:
        continue
    name = info['name']
    if info['chg'] <= -2:
        etf_drop_count += 1
    if info['chg'] <= -3:
        key = f'etf_drop_{code}'
        if is_new_alert(key, notified_today):
            alerts_etf.append(f'🔴 {info["name"]} 跌{info["chg"]}% 现价{info["price"]}')
            notified_today.add(key)
    print(f'    {info["name"]:12s} {info["price"]:>8.3f} ({info["chg"]:+.2f}%)')

if etf_drop_count >= 3:
    key = 'etf_systemic'
    if is_new_alert(key, notified_today):
        alerts_etf.append(f'⚠️ 系统性下跌！{etf_drop_count}只ETF同时跌超2%')
        notified_today.add(key)

if sh_chg <= -2 and etf_drop_count >= 1:
    key = 'sh_crash'
    if is_new_alert(key, notified_today):
        alerts_etf.append(f'💥 大盘暴跌{sh_chg}%+ETF联动')
        notified_today.add(key)

# ========== Part 2: QDII溢价 ==========
alerts_qdii = []
print('\n  QDII:')
for code, (hint, t_watch, t_buy) in QDII_THRESHOLDS.items():
    try:
        fu_url = f'http://hq.sinajs.cn/list=fu_{code}'
        req = urllib.request.Request(fu_url, headers={
            'Referer': 'http://finance.sina.com.cn', 'User-Agent': 'Mozilla/5.0'
        })
        fu_resp = urllib.request.urlopen(req, timeout=5).read().decode('gbk')
        match = re.search(r'"(.+)"', fu_resp)
        if match:
            parts = match.group(1).split(',')
            # parts[2]=Sina的ETF市价(可能延迟), parts[3]=IOPV净值, parts[6]=Sina算的溢价(延迟!)
            iopv = float(parts[3]) if len(parts) > 3 and parts[3] else 0
            # 用腾讯的实时市价来计算溢价(更准确)
            price = etf_results.get(code, {}).get('price', 0)
            if iopv > 0 and price > 0:
                premium = (price - iopv) / iopv * 100
                sina_premium = float(parts[6]) if len(parts) > 6 and parts[6] else 0
                print(f'    {hint}: 溢价{premium:.2f}% (实时算) IOPV={iopv:.4f} 市价={price:.4f} [Sina参考:{sina_premium:.1f}%] (关注<{t_watch}%/买入<{t_buy}%)')
                key = f'qdii_{code}'
                if premium < t_buy and is_new_alert(key, notified_today):
                    alerts_qdii.append(f'🟢 {hint} 溢价{premium:.1f}%(<{t_buy}%)！可买入')
                    notified_today.add(key)
                elif premium < t_watch and is_new_alert(key, notified_today):
                    alerts_qdii.append(f'🟡 {hint} 溢价{premium:.1f}%(<{t_watch}%)，关注')
                    notified_today.add(key)
            elif iopv == 0:
                print(f'    {hint}: IOPV未获取到')
    except Exception as e:
        print(f'    {hint}: 获取失败 {e}')

# ========== Part 3: 观察池MA买入信号 ==========
alerts_price = []
# 合并主动观察池 + 策略观察池
_strategy_pool_data = load_strategy_pool()
OBSERVATION_POOL_STRATEGY = [
    (sp['market'], sp['code'], sp['name'], f"策略:{'+'.join(sp.get('tiers', []))}")
    for sp in _strategy_pool_data
]
OBSERVATION_POOL = OBSERVATION_POOL_ACTIVE + OBSERVATION_POOL_STRATEGY
obs_codes = ','.join(f'{m}{c}' for m, c, *_ in OBSERVATION_POOL)
obs_results = get_realtime_quotes(obs_codes)

print(f'\n📋 观察池({len(OBSERVATION_POOL)}只):')
for prefix, code, name, cat in OBSERVATION_POOL:
    info = obs_results.get(code)
    if not info or info['price'] == 0:
        continue
    print(f'    [{cat:6s}]{name:6s} {info["price"]:>8.2f} ({info["chg"]:+.2f}%)')

# ========== Part 4: MA信号 (ETF + Watchlist + 持仓 + 观察池，去重) ==========
_ma_seen = set()
ma_targets = []
# 记录每只票的优先级：0=ETF, 1=重仓持仓, 2=待清仓, 3=观察池
_ma_priority = {}
_sell_on_rebound_codes = SELL_ON_REBOUND
for source_idx, source in enumerate([
    [(m, c, etf_results.get(c, {}).get('name', c)) for m, c in ETF_LIST],
    [(m, c, n, _cost) for m, c, n, _cost in PORTFOLIO],
    [(m, c, n) for m, c, n, *_ in OBSERVATION_POOL],
]):
    for item in source:
        m, c, n = item[0], item[1], item[2]
        if c not in _ma_seen:
            _ma_seen.add(c)
            ma_targets.append((m, c, n))
            # 持仓中待清仓的优先级为2，其余持仓为1
            if source_idx == 1:
                _ma_priority[c] = 2 if c in _sell_on_rebound_codes else 1
            elif source_idx == 0:
                _ma_priority[c] = 0  # ETF
            else:
                _ma_priority[c] = 3  # 观察池

alerts_ma = []
break_list = []  # 收集破位票名，最后合并成一条

# 收盘日报额外收集状态
daily_bullish = []  # 多头排列
daily_bearish = []  # 空头排列

# 获取所有MA标的的实时行情(用于盘中价格判断)
ma_realtime_codes = ','.join(f'{m}{c}' for m, c, _ in ma_targets)
ma_realtime = get_realtime_quotes(ma_realtime_codes)

print(f'\n📉 MA{MA_SHORT}/{MA_LONG}信号 ({len(ma_targets)}只):')
print(f'   金叉(放量+MACD零上+周线共振=强) | 盘中实时确认')
print('-' * 65)

for market, code, name in ma_targets:
    closes, volumes = get_kline(market, code)
    if len(closes) < MA_LONG + 2:
        continue

    ma5_today, ma5_yesterday = calc_ma(closes, MA_SHORT)
    ma20_today, ma20_yesterday = calc_ma(closes, MA_LONG)
    if ma5_today is None or ma20_today is None:
        continue

    current_price = closes[-1]  # 日K最新收盘(昨天)
    yesterday_price = closes[-2]

    # 盘中实时价格(如果有的话用实时,没有就用日K最新)
    rt_info = ma_realtime.get(code, {})
    live_price = rt_info.get('price', 0) or current_price
    live_chg = rt_info.get('chg', 0)

    # MACD
    dif, dea, macd_bar = calc_macd(closes)
    macd_above_zero = (dif is not None and dif > 0)

    # 周线共振
    weekly_bullish = check_weekly_above_ma20(market, code)

    # 量价确认
    vol_confirmed = False
    vol_data_available = len(volumes) >= MA_SHORT + 1
    if vol_data_available:
        vol_today = volumes[-1]
        vol_ma5 = sum(volumes[-MA_SHORT - 1:-1]) / MA_SHORT
        vol_confirmed = vol_today > vol_ma5 * 1.2

    # 金叉判断: 最近3天内MA5从下方穿越MA20 (不只看今天,避免错过)
    golden_cross = False
    golden_cross_days_ago = 0
    if len(closes) >= MA_LONG + 4:
        for lookback in range(3):  # 检查今天、昨天、前天
            idx = len(closes) - 1 - lookback
            if idx < MA_LONG + 1:
                break
            ma5_at = sum(closes[idx - MA_SHORT + 1:idx + 1]) / MA_SHORT
            ma20_at = sum(closes[idx - MA_LONG + 1:idx + 1]) / MA_LONG
            ma5_prev = sum(closes[idx - MA_SHORT:idx]) / MA_SHORT
            ma20_prev = sum(closes[idx - MA_LONG:idx]) / MA_LONG
            if ma5_prev < ma20_prev and ma5_at >= ma20_at:
                golden_cross = True
                golden_cross_days_ago = lookback
                break
    # 金叉后MA5仍在MA20上方才有效(如果已经跌回去了就取消)
    if golden_cross and ma5_today < ma20_today:
        golden_cross = False

    # 破位判断(连续2天) — 注意用日K收盘价判断
    today_below = current_price < ma20_today
    yesterday_below = yesterday_price < ma20_yesterday if ma20_yesterday else False
    break_confirmed = today_below and yesterday_below

    # 首次破位
    first_break = today_below and not yesterday_below

    # 盘中修正: 如果日线已破位但实时价站回MA20, 可能是假破位
    # 如果日线未破位但实时价跌破MA20, 是盘中预警

    # 信号强度
    strength_tags = []
    if golden_cross:
        if vol_confirmed:
            strength_tags.append('放量')
        if macd_above_zero:
            strength_tags.append('MACD零上')
        if weekly_bullish:
            strength_tags.append('周线共振')

    # 综合评级
    if golden_cross:
        score = sum([vol_confirmed, macd_above_zero, weekly_bullish is True])
        if score >= 2:
            strength = '强'
        elif score == 1:
            strength = '中'
        else:
            strength = '弱'
    else:
        strength = ''

    # 状态 & 通知
    status = ''
    intraday_note = ''  # 盘中辅助判断

    # 盘中实时位置判断
    if not IS_CLOSING_SUMMARY and live_price > 0:
        if live_price >= ma5_today:
            intraday_note = '盘中站稳MA5'
        elif live_price >= ma20_today:
            intraday_note = '盘中MA5下MA20上'
        else:
            intraday_note = '盘中跌破MA20'

    if golden_cross:
        tag_str = '+'.join(strength_tags) if strength_tags else '无确认'
        day_hint = '' if golden_cross_days_ago == 0 else f'(第{golden_cross_days_ago+1}天)'

        # 盘中对金叉的确认/否定
        if not IS_CLOSING_SUMMARY and live_price > 0:
            if live_price < ma20_today:
                # 金叉后盘中跌破MA20 = 假信号
                status = f'❌ 金叉失败！盘中破MA20 实时{live_price:.2f}<MA20={ma20_today:.2f}'
                key = f'ma_golden_fail_{code}'
                if is_new_alert(key, notified_today):
                    alerts_ma.append(f'❌ {name}({code}) 金叉失败！盘中跌破MA20 实时{live_price:.2f}')
                    notified_today.add(key)
            elif live_price < ma5_today:
                status = f'🟡 金叉[{strength}]{day_hint} 盘中弱(破MA5) 实时{live_price:.2f}'
                key = f'ma_golden_{code}'
                if is_new_alert(key, notified_today):
                    alerts_ma.append(f'🟡 {name}({code}) 金叉[{strength}]{day_hint} {tag_str} 但盘中破MA5 实时{live_price:.2f} 先别急')
                    notified_today.add(key)
            else:
                # 追高判断: 当日涨幅过大时提示风险
                chase_warn = ''
                if live_chg >= 5:
                    chase_warn = f' ⚠️今日已涨{live_chg:.1f}%勿追'
                elif live_chg >= 3:
                    chase_warn = f' (今日+{live_chg:.1f}%，注意追高)'
                status = f'🟢 金叉[{strength}]{day_hint} 盘中强势(站稳MA5) 实时{live_price:.2f}{chase_warn}'
                key = f'ma_golden_{code}'
                if is_new_alert(key, notified_today):
                    if live_chg >= 5:
                        alerts_ma.append(f'🟢 {name}({code}) 金叉[{strength}]{day_hint} {tag_str} 实时{live_price:.2f} ⚠️今日已涨{live_chg:.1f}%，信号确认但别追，等回调')
                    elif live_chg >= 3:
                        alerts_ma.append(f'🟢 {name}({code}) 金叉[{strength}]{day_hint} {tag_str} 实时{live_price:.2f} (今日+{live_chg:.1f}%注意追高)')
                    else:
                        alerts_ma.append(f'🟢 {name}({code}) 金叉[{strength}]{day_hint} {tag_str}+盘中站稳MA5 实时{live_price:.2f} 可入场')
                    notified_today.add(key)
        else:
            # 收盘模式或无实时价,正常输出
            status = f'🟢 金叉[{strength}]{day_hint} ({tag_str})'
            key = f'ma_golden_{code}'
            if is_new_alert(key, notified_today):
                alerts_ma.append(f'🟢 {name}({code}) 金叉[{strength}]{day_hint} {tag_str} 现价{current_price:.2f}')
                notified_today.add(key)
    elif break_confirmed:
        # 盘中对破位的修正
        if not IS_CLOSING_SUMMARY and live_price > 0 and live_price >= ma20_today:
            status = f'🟡 尝试修复！盘中收回MA20 实时{live_price:.2f}>MA20={ma20_today:.2f}'
            key = f'ma_repair_{code}'
            if is_new_alert(key, notified_today):
                alerts_ma.append(f'🟡 {name}({code}) 破位后盘中反弹站回MA20！实时{live_price:.2f} 观察收盘能否确认')
                notified_today.add(key)
        else:
            status = '🔴 连续破MA20！清仓'
            break_list.append(name)
    elif first_break:
        if not IS_CLOSING_SUMMARY and live_price > 0 and live_price < ma20_today:
            status = f'⚠️ 首破MA20 盘中仍在下方 实时{live_price:.2f}'
        else:
            status = '⚠️ 首破MA20'
    elif ma5_today > ma20_today:
        # 多头但盘中跌破MA20 = 盘中预警
        if not IS_CLOSING_SUMMARY and live_price > 0 and live_price < ma20_today:
            status = f'⚠️ 多头但盘中破MA20！实时{live_price:.2f}<MA20={ma20_today:.2f}'
            key = f'ma_intraday_break_{code}'
            if is_new_alert(key, notified_today):
                alerts_ma.append(f'⚠️ {name}({code}) 日线多头但盘中跌破MA20！实时{live_price:.2f} 关注收盘')
                notified_today.add(key)
        else:
            # 观察池标的: 检测回踩MA支撑买入信号
            _obs_codes = {c for _, c, *_ in OBSERVATION_POOL}
            if code in _obs_codes and len(closes) >= 11:
                ma10_today = sum(closes[-10:]) / 10
                # 回踩MA5: 昨日收盘贴近MA5(差距<1.5%), 今日止跌
                dist_to_ma5 = abs(current_price - ma5_today) / ma5_today
                dist_to_ma10 = abs(current_price - ma10_today) / ma10_today
                if dist_to_ma5 < 0.015 and current_price >= ma5_today and yesterday_price > ma5_yesterday:
                    # 价格刚好在MA5附近且站稳 = 回踩MA5成功
                    key_bounce = f'ma_bounce5_{code}'
                    if is_new_alert(key_bounce, notified_today):
                        alerts_price.append(f'📗 {name}({code}) 回踩MA5支撑 多头+站稳 轻仓信号 实时{live_price:.2f}')
                        notified_today.add(key_bounce)
                elif dist_to_ma10 < 0.015 and current_price >= ma10_today:
                    # 回踩MA10支撑
                    key_bounce = f'ma_bounce10_{code}'
                    if is_new_alert(key_bounce, notified_today):
                        alerts_price.append(f'📘 {name}({code}) 回踩MA10支撑 多头排列 标准买入信号 实时{live_price:.2f}')
                        notified_today.add(key_bounce)
            status = '✅ 多头'
            daily_bullish.append(f'{name}({live_price:.2f})')
    elif current_price < ma20_today:
        status = '— 空头'
        daily_bearish.append(f'{name}({live_price:.2f})')
    else:
        # 价格在MA5和MA20之间 — 观察池标的检测MA20支撑
        _obs_codes2 = {c for _, c, *_ in OBSERVATION_POOL}
        if code in _obs_codes2:
            dist_to_ma20 = abs(current_price - ma20_today) / ma20_today
            if dist_to_ma20 < 0.02 and current_price >= ma20_today:
                key_bounce = f'ma_bounce20_{code}'
                if is_new_alert(key_bounce, notified_today):
                    alerts_price.append(f'📙 {name}({code}) 回踩MA20支撑 重仓信号 实时{live_price:.2f} MA20={ma20_today:.2f}')
                    notified_today.add(key_bounce)
        status = '— 等待'

    # MACD/周线附加信息
    extra = ''
    if dif is not None:
        extra += f' DIF={dif:.2f}'
    if weekly_bullish is not None:
        extra += ' W✅' if weekly_bullish else ' W❌'

    print(f'  {name:12s} {current_price:>8.2f} | MA5={ma5_today:.2f} MA20={ma20_today:.2f}{extra} | {status}')

# 破位信号合并成一条推送
if break_list:
    key = 'ma_break_summary'
    if is_new_alert(key, notified_today):
        prefix = '🔴收盘确认' if IS_CLOSING_SUMMARY else '🔴'
        alerts_ma.append(f'{prefix} {len(break_list)}只连续破MA20: {", ".join(break_list[:15])}{"..." if len(break_list) > 15 else ""}')
        notified_today.add(key)

# 收盘日报模式下按优先级排序：重仓(1) > 待清仓(2) > ETF(0) > 观察池(3)
if IS_CLOSING_SUMMARY and alerts_ma:
    def _alert_priority(alert_str):
        """从alert字符串中提取code并返回排序优先级"""
        import re
        m = re.search(r'[(\(](\d{6})[)\)]', alert_str)
        if m:
            code = m.group(1)
            p = _ma_priority.get(code, 9)
            # 排序：重仓持仓=1, 待清仓=2, ETF=0→映射为2.5, 观察池=3
            sort_map = {1: 0, 2: 1, 0: 2, 3: 3}
            return sort_map.get(p, 9)
        return 9  # 无法识别的放最后（如破位汇总）
    alerts_ma.sort(key=_alert_priority)

print('-' * 65)

# ========== Part 5: 持仓做T信号 ==========
alerts_t = []
# 复用ma_realtime已有的行情数据，避免重复请求
port_results = ma_realtime

# 日报用
daily_port_gainers = []
daily_port_losers = []

# 止盈/止损数据收集
tp_portfolio_data = []

print(f'\n💼 持仓做T监控({len(PORTFOLIO)}只):')
for market, code, name, cost in PORTFOLIO:
    closes, volumes = get_kline(market, code)
    info = port_results.get(code, {})
    price = info.get('price', 0)
    chg = info.get('chg', 0)

    if not closes or len(closes) < MA_LONG + 1 or price == 0:
        continue

    ma5_t, _ = calc_ma(closes, MA_SHORT)
    ma20_t, ma20_y = calc_ma(closes, MA_LONG)
    if ma5_t is None or ma20_t is None:
        continue

    # 偏离度
    dev_ma20 = (price - ma20_t) / ma20_t * 100
    dev_ma5 = (price - ma5_t) / ma5_t * 100

    # 连涨/连跌天数(包含今天实时涨跌)
    streak = 0
    # 先看今天: 实时价 vs 昨收(日K最后一根)
    today_up = price > closes[-1] if closes else False
    today_down = price < closes[-1] if closes else False

    if today_up:
        streak = 1
        for i in range(1, min(6, len(closes))):
            if closes[-i] > closes[-i - 1]:
                streak += 1
            else:
                break
    elif today_down:
        streak = -1
        for i in range(1, min(6, len(closes))):
            if closes[-i] < closes[-i - 1]:
                streak -= 1
            else:
                break
    else:
        # 今天平,看昨天方向
        for i in range(1, min(6, len(closes))):
            if closes[-i] > closes[-i - 1]:
                if streak >= 0:
                    streak += 1
                else:
                    break
            elif closes[-i] < closes[-i - 1]:
                if streak <= 0:
                    streak -= 1
                else:
                    break
            else:
                break

    # 盈亏
    pnl = (price - cost) / cost * 100

    signal = ''
    key_prefix = f't_{code}'

    # 趋势判断: MA5>MA20=上升趋势, MA5<MA20=下降趋势
    is_uptrend = ma5_t > ma20_t

    # 做T买入信号: 连跌3天 + 偏离MA20 > -8%
    if streak <= -3 and dev_ma20 < -8:
        if is_uptrend:
            # 上升趋势中超跌 = 安全低吸
            signal = '📉 超跌!低吸做T'
            key = f'{key_prefix}_oversold'
            if is_new_alert(key, notified_today):
                alerts_t.append(f'📉 {name} 连跌{-streak}天 偏离MA20={dev_ma20:.1f}% 现价{price} 趋势仍多头，适合低吸做T')
                notified_today.add(key)
        else:
            # 下降趋势中超跌 = 接飞刀，风险提示
            signal = '📉 超跌但趋势空头⚠️'
            key = f'{key_prefix}_oversold_risky'
            if is_new_alert(key, notified_today):
                alerts_t.append(f'⚠️ {name} 连跌{-streak}天 偏离MA20={dev_ma20:.1f}% 现价{price} 但MA5<MA20趋势向下，低吸风险大')
                notified_today.add(key)
    # 连跌3天但偏离不够深
    elif streak <= -3 and dev_ma20 < -5:
        signal = '📉 连跌关注'

    # 做T卖出信号: 连涨3天 + 偏离MA5 > +5%
    if streak >= 3 and dev_ma5 > 5:
        signal = '📈 冲高!高抛做T'
        key = f'{key_prefix}_overbought'
        if is_new_alert(key, notified_today):
            alerts_t.append(f'📈 {name} 连涨{streak}天 偏离MA5={dev_ma5:.1f}% 现价{price} 适合高抛做T')
            notified_today.add(key)
    elif streak >= 3 and dev_ma5 > 3:
        if not signal:
            signal = '📈 连涨关注'

    # MA20支撑做T: 回踩MA20不破(今天最低触及但收盘在上方)
    if len(closes) >= 2:
        # 近似: 昨天在MA20上方, 今天收盘仍在MA20附近(偏离<1%)且仍在上方
        if 0 <= dev_ma20 <= 1 and closes[-1] > ma20_t:
            if not signal:
                chase_note = f' ⚠️今日已涨{chg:.1f}%勿追' if chg >= 5 else (f' (今日+{chg:.1f}%)' if chg >= 3 else '')
                signal = f'🎯 MA20支撑{chase_note}'
                key = f'{key_prefix}_support'
                if is_new_alert(key, notified_today):
                    if chg >= 5:
                        alerts_t.append(f'🎯 {name} 回踩MA20支撑 现价{price}≈MA20={ma20_t:.2f} 但今日已涨{chg:.1f}%，等回落再T')
                    else:
                        alerts_t.append(f'🎯 {name} 回踩MA20支撑 现价{price}≈MA20={ma20_t:.2f} 可做T买入')
                    notified_today.add(key)

    # 接近成本价卖出提醒（择机卖出清单，上午/下午各推一次）
    if code in SELL_ON_REBOUND and pnl >= -2:
        ampm = 'pm' if now.hour >= 13 else 'am'
        key = f'{key_prefix}_sell_rebound_{ampm}'
        if is_new_alert(key, notified_today):
            if pnl >= 0:
                alerts_t.append(f'🚨 {name} 已回本! 现价{price} 盈亏{pnl:+.1f}% 成本{cost} → 建议卖出换龙头')
            else:
                alerts_t.append(f'⚡ {name} 接近成本! 现价{price} 盈亏{pnl:+.1f}% 成本{cost} → 关注反弹卖出')
            notified_today.add(key)

    # 日报统计
    if chg >= 3:
        daily_port_gainers.append(f'{name}+{chg:.1f}%')
    elif chg <= -3:
        daily_port_losers.append(f'{name}{chg:.1f}%')

    # 收集止盈/止损数据
    dif_t, dea_t, _ = calc_macd(closes + [price])  # 加上今天实时价格
    tp_portfolio_data.append({
        'name': name, 'code': code, 'cost': cost,
        'price': price, 'chg': chg,
        'ma5': ma5_t, 'ma20': ma20_t,
        'dif': dif_t, 'dea': dea_t,
        'weekly_ok': True,  # 简化: 后续可接入周线数据
        'consecutive_down': -streak if streak < 0 else 0,
    })

    if signal:
        print(f'  {name:8s} {price:>8.2f}({chg:+.1f}%) 成本{cost} 盈亏{pnl:+.1f}% | {signal}')
    elif IS_CLOSING_SUMMARY:
        streak_desc = f'连涨{streak}天' if streak > 0 else (f'连跌{-streak}天' if streak < 0 else '平盘')
        print(f'  {name:8s} {price:>8.2f}({chg:+.1f}%) 偏MA20={dev_ma20:+.1f}% {streak_desc}')

print('-' * 65)

# ========== Part 5.5: 止盈/止损信号 ==========
alerts_tp = []
if IS_CLOSING_SUMMARY and tp_portfolio_data:
    from take_profit import check_take_profit_signals
    alerts_tp = check_take_profit_signals(tp_portfolio_data)
    if alerts_tp:
        print('\n'.join(alerts_tp))

# ========== Part 6: 收盘日报 (仅收盘模式) ==========
if IS_CLOSING_SUMMARY:
    print('\n' + '=' * 65)
    print(f'📊 {now.strftime("%m/%d")} 收盘日报')
    print(f'  上证 {shanghai.get("price",0)} ({sh_chg:+.2f}%)')
    if daily_port_gainers:
        print(f'  🚀 持仓大涨: {", ".join(daily_port_gainers[:8])}')
    if daily_port_losers:
        print(f'  💔 持仓大跌: {", ".join(daily_port_losers[:8])}')
    if daily_bullish:
        print(f'  ✅ Watchlist多头({len(daily_bullish)}): {", ".join(daily_bullish[:6])}')
    if daily_bearish:
        print(f'  ❌ Watchlist空头({len(daily_bearish)}): {", ".join(daily_bearish[:6])}')
    print('=' * 65)

# ========== Part 7: 策略共振检测 (收盘后运行，读取策略扫描结果) ==========
alerts_resonance = []
if IS_CLOSING_SUMMARY:
    print('\n🧩 策略共振检测...')
    strat_results = load_strategy_results()
    if strat_results:
        resonance_picks = filter_resonance_picks(strat_results)
        if resonance_picks:
            print(f'  共{len(resonance_picks)}只通过筛选')
            resonance_lines = format_resonance_alert(resonance_picks)
            alerts_resonance = resonance_lines

            # 更新策略观察池: 保留score>=3的(双重共振)
            new_pool = []
            for p in resonance_picks:
                if p['score'] >= 3:
                    # 确定market前缀
                    c = p['code']
                    if c.startswith('6'):
                        mkt = 'sh'
                    elif c.startswith('0') or c.startswith('3'):
                        mkt = 'sz'
                    else:
                        continue
                    new_pool.append({
                        'market': mkt,
                        'code': c,
                        'name': p['name'],
                        'tiers': p['tiers'],
                        'score': p['score'],
                        'strategies': p['strategies'],
                        'info': p['info'],
                        'added_date': now.strftime('%Y-%m-%d'),
                    })
            save_strategy_pool(new_pool)
            print(f'  策略观察池已更新: {len(new_pool)}只(score>=3)')

            for p in resonance_picks[:10]:
                tier_str = '+'.join(p['tiers'])
                print(f'    {"⭐" if p["score"]>=4 else "📋"} {p["name"]}({p["code"]}) [{tier_str}] score={p["score"]}')
            
            # 记录picks到跟踪日志
            try:
                from strategy_tracker import log_picks
                log_picks(resonance_picks, now.strftime('%Y-%m-%d'))
            except Exception as e:
                print(f'  [WARN] 跟踪记录失败: {e}')
        else:
            print('  无共振信号')
    else:
        print('  未找到策略扫描数据')

# ========== 汇总推送 ==========
all_alerts = alerts_etf + alerts_qdii + alerts_price + alerts_ma + alerts_t + alerts_tp + alerts_resonance

if all_alerts:
    print(f'\n🚨 共触发{len(all_alerts)}个新信号!')
    for a in all_alerts:
        print(f'  {a}')

    if NO_PUSH:
        # 保存信号到文件，等审核后用 --push-saved 推送
        parts = []
        if alerts_ma: parts.append('MA信号')
        if alerts_t: parts.append('做T提醒')
        if alerts_etf: parts.append('ETF预警')
        if alerts_price: parts.append('到价提醒')
        if alerts_qdii: parts.append('QDII溢价')
        if IS_CLOSING_SUMMARY:
            # 收盘日报不推单策略强信号，排除 alerts_resonance
            closing_alerts = alerts_etf + alerts_qdii + alerts_price + alerts_ma + alerts_t + alerts_tp
            title = f'📊 {now.strftime("%m/%d")}收盘日报'
            lines = [f'上证 {shanghai.get("price",0)} ({sh_chg:+.2f}%)']
            if daily_port_gainers: lines.append(f'🚀 大涨: {", ".join(daily_port_gainers[:5])}')
            if daily_port_losers: lines.append(f'💔 大跌: {", ".join(daily_port_losers[:5])}')
            lines.append('')
            lines.extend(closing_alerts)
            if daily_bullish: lines.append(f'✅ 多头({len(daily_bullish)}): {", ".join(daily_bullish[:5])}')
            if daily_bearish: lines.append(f'❌ 空头({len(daily_bearish)}): {", ".join(daily_bearish[:5])}')
        else:
            title = f'📊 {"+".join(parts)}'
            lines = all_alerts + ['', '检查后决定是否操作！']
        ALERTS_FILE.write_text(json.dumps({'title': title, 'lines': lines}, ensure_ascii=False), encoding='utf-8')
        print(f'[审核模式] {len(all_alerts)}条信号已保存，审核通过后运行 --push-saved 推送')
    else:
        try:
            from notify.notify import push_feishu_card
            parts = []
            if alerts_ma:
                parts.append('MA信号')
            if alerts_t:
                parts.append('做T提醒')
            if alerts_etf:
                parts.append('ETF预警')
            if alerts_price:
                parts.append('到价提醒')
            if alerts_qdii:
                parts.append('QDII溢价')

            if IS_CLOSING_SUMMARY:
                title = f'📊 {now.strftime("%m/%d")}收盘日报'
                # 日报内容更丰富
                summary_lines = [f'上证 {shanghai.get("price",0)} ({sh_chg:+.2f}%)']
                if daily_port_gainers:
                    summary_lines.append(f'🚀 大涨: {", ".join(daily_port_gainers[:5])}')
                if daily_port_losers:
                    summary_lines.append(f'💔 大跌: {", ".join(daily_port_losers[:5])}')
                summary_lines.append('')
                summary_lines.extend(all_alerts)
                summary_lines.append('')
                if daily_bullish:
                    summary_lines.append(f'✅ 多头({len(daily_bullish)}): {", ".join(daily_bullish[:5])}')
                if daily_bearish:
                    summary_lines.append(f'❌ 空头({len(daily_bearish)}): {", ".join(daily_bearish[:5])}')
                push_feishu_card(title, summary_lines)
            else:
                title = f'📊 {"+".join(parts)}'
                push_feishu_card(title, all_alerts + ['', '检查后决定是否操作！'])
            print('[OK] 飞书已通知')
        except Exception as e:
            print(f'[WARN] 飞书通知失败: {e}')
elif IS_CLOSING_SUMMARY:
    # 收盘日报即使无信号也推
    if NO_PUSH:
        title = f'📊 {now.strftime("%m/%d")}收盘日报'
        lines = [f'上证 {shanghai.get("price",0)} ({sh_chg:+.2f}%)', '']
        if daily_port_gainers: lines.append(f'🚀 大涨: {", ".join(daily_port_gainers[:5])}')
        if daily_port_losers: lines.append(f'💔 大跌: {", ".join(daily_port_losers[:5])}')
        lines.append('')
        lines.append('✅ 无交易信号触发，继续持有')
        if daily_bullish: lines.append(f'多头({len(daily_bullish)}): {", ".join(daily_bullish[:5])}')
        if daily_bearish: lines.append(f'空头({len(daily_bearish)}): {", ".join(daily_bearish[:5])}')
        ALERTS_FILE.write_text(json.dumps({'title': title, 'lines': lines}, ensure_ascii=False), encoding='utf-8')
        print('[审核模式] 收盘日报已保存，审核通过后运行 --push-saved 推送')
    else:
        try:
            from notify.notify import push_feishu_card
            title = f'📊 {now.strftime("%m/%d")}收盘日报'
            lines = [f'上证 {shanghai.get("price",0)} ({sh_chg:+.2f}%)', '']
            if daily_port_gainers:
                lines.append(f'🚀 大涨: {", ".join(daily_port_gainers[:5])}')
            if daily_port_losers:
                lines.append(f'💔 大跌: {", ".join(daily_port_losers[:5])}')
            lines.append('')
            lines.append('✅ 无交易信号触发，继续持有')
            if daily_bullish:
                lines.append(f'多头({len(daily_bullish)}): {", ".join(daily_bullish[:5])}')
            if daily_bearish:
                lines.append(f'空头({len(daily_bearish)}): {", ".join(daily_bearish[:5])}')
            push_feishu_card(title, lines)
            print('[OK] 飞书日报已推送')
        except Exception as e:
            print(f'[WARN] 飞书通知失败: {e}')
else:
    print('\n✅ 无新信号，盘面正常')

# 保存通知缓存
save_notify_cache(notified_today)
