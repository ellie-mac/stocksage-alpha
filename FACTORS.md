# 因子说明文档 · Factor Reference

---

## 中文版

### 评分体系说明

每个因子独立输出两个评分，均为 0–10 分（核心因子为 0–25 分）：

- **买入分（buy_score）**：分越高代表买入信号越强，即该因子维度越有利于持有/买入
- **卖出分（sell_score）**：分越高代表卖出信号越强，即该因子维度出现明显负面信号

**关键原则：买入分低 ≠ 卖出信号。** 例如动量平淡（买入分低）不代表应该卖出，只有出现实质性负面信号时卖出分才会升高。

综合评分（0–100）：
- `total_score`：所有因子买入分的加权平均，衡量综合做多吸引力
- `total_sell_score`：所有因子卖出分的加权平均，衡量综合卖出压力

---

### 核心因子（满分各 25 分）

---

#### 1. 价值因子（value）
**衡量内容**：股票相对估值水平，基于 PE（市盈率）和 PB（市净率）；叠加近3个月价格动量识别"价值陷阱"

**买入分高的条件**（估值低廉）：
- PE/PB 处于行业内历史低位（行业相对百分位 < 20%）
- 或 PE/PB 低于自身3年历史中位数
- 绝对值参考：PE < 15 倍、PB < 1 倍视为明显低估
- **深度低估（PE/PB 百分位 ≤ 20%）+ 3个月涨幅 > 5%（价值催化剂已启动，买入分 +2）**：便宜已经开始被市场认识，是布局窗口

**卖出分高的条件**（估值泡沫）：
- PE/PB 处于行业内或历史 90% 分位以上（极度高估）
- 绝对值参考：PE > 60 倍、PB > 5 倍触发强卖出信号

**买入分降低/卖出分升高的条件**（价值陷阱）：
- **深度低估 + 3个月跌幅 > 10%（价值陷阱风险，买入分 -2，卖出分 +1）**：估值低但价格持续下跌——市场在定价某种买方不知道的坏消息

**盈利修正交叉（双击/双杀信号）**：
- **深度低估（PE/PB 百分位 ≤ 25%）+ 分析师净调升 ≥ 2（买入分 +3，双击信号）**：便宜 + 分析师开始积极评级——机构建仓前最经典的触发信号，两个独立信号源同向时置信度最高
- **高估（PE/PB 百分位 ≥ 80%）+ 分析师净调降 ≤ -2（卖出分 +3，双杀信号）**：贵 + 基本面还在恶化，量化上最明确的做空/减仓信号

**行业相对PEG交叉（行业内最便宜/最贵成长股识别）**：
- **PE行业百分位 ≤ 30% + 利润增速 ≥ 20%（行业内最便宜的成长股，买入分 +2）**：行业内估值最低的高速成长企业——PEG < 1 的最直接表达，是行业配置时性价比最高的标的；便宜 + 高成长 = 估值扩张空间最大
- **PE行业百分位 ≥ 80% + 利润增速 ≤ 0（行业内最贵的衰退股，卖出分 +2）**：以行业内最高估值定价一家利润正在萎缩的企业，是行业配置中最危险的组合——高溢价 + 基本面恶化 = 双重杀估值风险

**关键原则**：便宜≠买入信号，便宜 + 价格企稳或上涨才是真正的价值机会。"cheap but falling"是 A 股最常见的价值陷阱。便宜 + 分析师升级是双重确认——市场定价机制和分析师预测同时给出同向信号，是最高置信度的价值买入时机。行业相对PEG交叉进一步升级这个问题：从"这家公司便宜吗"升级到"这是行业内最有性价比的成长机会吗"——价值投资与成长投资的最直接交叉点。

**注意**：A 股市场情绪驱动明显，高估值可持续较长时间，卖出分作为预警而非一票否决。

---

#### 2. 成长因子（growth）
**衡量内容**：营收增长、利润增长、净资产收益率（ROE）趋势；叠加成长加速度 + ROE 质量 + PEG 估值三重交叉分析

**买入分高的条件**：
- 营收同比增长 > 20%
- 净利润同比增长 > 20%
- ROE > 15%
- **净利润增速加速（当期 > 上期，差值 ≥ 5pp，买入分 +1.5 或 +3）**：成长故事从"稳定"升级为"加速"，通常是机构加仓窗口
- **净利润增速 ≥ 30% + ROE ≥ 15%（高质量复利成长，买入分 +2）**：高速增长同时保持高资本回报率，是 A 股最稀缺的"复利机器"信号
- **净利润增速 ≥ 30% + PE 百分位 ≤ 40%（成长被低估，PEG < 1，买入分 +2）**：高速增长还没有被市场充分定价，PE 历史上属于便宜区间——这正是 PEG < 1 的结构性买入机会，成长故事还有充足的估值扩张空间

**卖出分高的条件**：
- 营收同比增长为负（营收萎缩）
- 净利润同比下滑 > 20%
- ROE < 3%（资本回报极低）
- **成长急剧减速（增速环比下降 ≥ 20pp 且当期增速 < 20%，卖出分 +4）**：高成长故事崩塌的经典信号
- **净利润增速 ≥ 20% 但 ROE < 5%（空心成长，卖出分 +2）**：高增长但资产拼命扩张、回报率极低，属于"烧钱型"成长，不可持续
- **净利润增速 ≥ 20% + PE 百分位 ≥ 80%（成长已完全定价，PEG 高，卖出分 +1.5）**：分析师和机构都已经持有，上行空间需要完美执行才能实现，容错率极低
- **净利润增速 ≤ 5% + PE 百分位 ≥ 70%（停滞成长 + 高溢价，最危险的组合，卖出分 +2）**：以成长股估值定价一家停滞甚至倒退的企业——预期差最大，杀估值风险最高

**关键原则**：成长 × ROE 捕捉"成长含金量"；成长 × 估值（PEG）捕捉"你是否为这个成长支付了合理价格"。低估值的高成长（PEG < 1）是成长投资的圣杯；高估值的停滞成长（"高 PE + 低增速"）是最危险的价值毁灭陷阱。

**注意**：A 股对成长股定价较快，成长因子 IC 约 0.027，属于弱信号，需与其他因子结合。

---

#### 3. 动量因子（momentum）
**衡量内容**：1个月、3个月、6个月价格涨跌幅；叠加量价背离（近20日量 vs 近60日量）+ 基本面质量（ROE）判断趋势健康度和可持续性

**买入分高的条件**：
- 3个月涨幅 > 10%（趋势确立）
- 6个月涨幅 > 15%（持续动量）
- **强趋势（3月涨幅 ≥ 20% 或 6月涨幅 ≥ 25%）+ ROE ≥ 15%（高质量动量，买入分 +2）**：基本面支撑的趋势，企业真的在赚钱，动量具有持续性

**卖出分高的条件**：
- **重要**：1个月大跌不算卖出信号（属于反转买入逻辑）
- 3个月跌幅 > 5% 开始计入卖出分
- 3个月跌幅 > 20% 或 6个月跌幅 > 30% 为强卖出信号
- 例外：1个月跌幅 > 30%（崩盘式下跌）才计入少量卖出分
- **强趋势 + ROE < 5%（低质量动量，卖出分 +2）**：价格飞涨但企业几乎不赚钱——典型的主题炒作泡沫，没有基本面支撑

**量价背离（卖出分调整）**：
- **3月涨幅 ≥ 15% + 近20日量 < 近60日量的 75%（价升量缩）→ 卖出分 +4**：趋势"吃老本"，上涨动力不足
- **3月涨幅 ≥ 15% + 近20日量 < 近60日量的 85%（轻度量缩）→ 卖出分 +2**：轻微警告
- **3月跌幅 ≥ 15% + 近20日量 < 近60日量的 70%（跌势缩量）→ 卖出分 -3**：卖压枯竭，抛盘减少，减轻卖出压力

**市场环境交叉（Daniel & Moskowitz 动量崩溃）**：
- **强动量（3月涨幅 ≥ 15% 或 6月涨幅 ≥ 20%）+ 熊市环境（市场环境分 ≤ 3）→ 买入分 -3，卖出分 +2**：动量因子在熊市中系统性失效是实证最充分的发现之一——熊市中追涨往往是在追下跌途中的反弹，而非真正的趋势延续
- **强动量 + 牛市环境（市场环境分 ≥ 7）→ 买入分 +1.5**：牛市趋势延续概率显著更高，机构在牛市中更愿意追动量标的
- **强跌势（3月跌幅 ≥ 15% 或 6月跌幅 ≥ 20%）+ 牛市环境 → 卖出分 -1.5**：牛市中个股逆势大跌往往是均值回归机会，而非系统性熊市，降低卖出紧迫性

**关键原则**：价格趋势是方向，成交量是燃料，ROE 是引擎，市场环境是背景底色。动量因子的最大陷阱是在熊市中失效——牛市中的强动量股是趋势延续，熊市中的强动量股常常是下跌途中的反弹，两者外表相似、本质不同，市场环境交叉是区分二者的关键。

---

#### 4. 质量因子（quality）
**衡量内容**：ROE 水平、毛利率、资产负债率，衡量企业盈利质量；叠加52周价格位置区分"优质低估"和"劣质高估"

**买入分高的条件**：
- ROE > 20%（高回报）
- 毛利率 > 40%（强护城河）
- 资产负债率 < 30%（低杠杆）
- **高质量（ROE ≥ 15%且毛利率 ≥ 30%）+ 52周位置 < 30%（质量折价，买入分 +3）**：真正优质的生意以低廉的价格买到，是 A 股最具性价比的建仓机会
- **高质量 + 估值偏低（PE 百分位 ≤ 30% 或 PB 百分位 ≤ 30%）（GARP 信号，买入分 +2）**：高质量 + 历史低估，是基本面维度最高确信度的买入组合
- **低质量 + 估值偏低（PE/PB 百分位 ≤ 30%）（买入分 -2）**：表面便宜，实为"劣质低价"——企业本身弱，而非被错误定价

**卖出分高的条件**：
- 资产负债率 > 70%（高危杠杆）
- ROE < 3%（资本回报近乎为零）
- 毛利率 < 5%（几乎无利润空间）
- **低质量（ROE < 5% 或毛利率 < 10%）+ 52周位置 > 70%（劣质高估，卖出分 +3）**：资质平庸甚至差劲的企业却在历史高位交易——要么是故事炒作、要么是趋势反转在即

**卖出分降低的条件**：
- **高质量 + 估值极贵（PE/PB 百分位 ≥ 85%）（卖出分 -1.5）**：高质量企业本应享受估值溢价，估值高不等于应该卖出

**关键原则**：质量因子 × 价格位置复现了"好股票 + 好价格"的经典选股逻辑；新增的 GARP 交叉（质量 × 估值百分位）进一步量化了"物美价廉"的程度。与 Piotroski 因子（衡量基本面改善方向）不同，质量因子衡量绝对水平——一家始终保持 20% ROE 的企业在历史低估区是真正的宝藏。

---

### 扩展因子 A 组（满分 5–15 分，来自基础数据）

---

#### 5. 北向/大单资金（northbound）
**衡量内容**：近5日大单净流入天数及总量，作为机构资金代理指标

**买入分高的条件**：
- 5日内大单净流入天数 ≥ 4 天
- 净流入总量为正且较大

**卖出分高的条件**：
- 5日内大单净流出天数 = 5 天（持续流出）
- 净流出量大（5亿以上触及满分）

---

#### 6. 量能突破（volume）
**衡量内容**：当日成交量 vs 20日均量，结合价格方向判断量价配合；叠加 MA5/MA20 趋势方向确认放量含义

**买入分高的条件**：
- 放量上涨（量比 ≥ 1.5 倍 + 涨幅 ≥ 1%）→ 量价齐升，确认突破，强买入
- 放量下跌但有长下影线（量比 ≥ 1.5 倍 + 跌幅 ≥ 2% + 下影线长）→ 低位有承接，可能探底
- 缩量下跌（量比 < 0.5 倍 + 价格下跌）→ 卖盘枯竭，轻微买入信号
- **放量（≥ 1.5 倍）+ MA5 > MA20（上升趋势）→ 买入分 +1.5**：上涨趋势中的放量是主力建仓确认，不是出货

**卖出分高的条件**：
- 放量大阴线（量比 ≥ 1.5 倍 + 跌幅 ≥ 2% + 无明显下影线）→ 主力出货，强卖出信号（7–9 分）
- 缩量上涨（量比 < 0.5 倍 + 涨幅 ≥ 1%）→ 上涨不可持续（5 分）
- 极端放量（量比 > 5 倍）→ 顶部高位出货风险（6 分）
- **放量（≥ 1.5 倍）+ MA5 < MA20（下降趋势）→ 卖出分 +1.5**：下跌趋势中的放量是出货 / 恐慌抛售加速

**52周价格位置交叉（量能事件的背景诊断）**：
- **量比 ≥ 1.5 倍 + 涨幅 ≥ 1% + 52周位置 < 30%（底部放量突破，买入分 +2）**：低位放量上涨 = 经典的机构建仓启动信号，底部吸筹完成、主力开始拉升，是 A 股量价配合最可信的布局形态
- **量比 ≥ 1.5 倍 + 涨幅 ≥ 1% + 52周位置 > 70%（高位放量，卖出分 +1.5）**：高位放量上涨通常是主力在人气最旺时借势派发，与低位放量外表相同、本质相反
- **量比 ≥ 1.5 倍 + 跌幅 ≥ 2% + 52周位置 < 30%（低位恐慌性抛售，卖出分 -1.5）**：底部区域的放量大跌通常是极度恐慌的情绪性抛售而非主力出货——相同的放量大跌在高位是出货信号，在底部是情绪顶点，降低卖出紧迫性

**关键原则**：放量 × 当日涨跌 是基础信号；放量 × 趋势方向（MA）是确认信号；放量 × 52周价格位置是背景诊断层。三层叠加才能完整回答"这次放量意味着什么"——同样的放量大阴线，在年高附近是机构出货，在年低附近是散户恐慌，两者外表一致而意义截然不同。

---

#### 7. 52周位置（position_52w）
**衡量内容**：当前价格在52周高低点区间中的位置，0%=年低，100%=年高

**买入分高的条件**：
- 位置 > 80%（强势创新高趋势）

**卖出分高的条件**（弱信号，满分仅 3 分）：
- 位置 > 95%（紧逼52周高点，短期轻微警惕）

**注意**：此因子主要作为趋势确认，不宜单独作为卖出依据。

---

#### 8. 股息率（div_yield）
**衡量内容**：TTM 股息率，衡量现金回报吸引力；叠加财务质量（ROE + 资产负债率）+ 盈利增速趋势，双重防范"股息陷阱"

**买入分高的条件**：
- 股息率 ≥ 5%（高收益）
- 股息率 ≥ 3%（中等吸引力）
- **股息率 ≥ 2% + ROE ≥ 12% + 负债率 ≤ 60%（可持续高股息，买入分+1.5）**：盈利能力强、资产负债健康的企业，股息有充分的利润支撑，是真正的"现金奶牛"
- **股息率 ≥ 4% + 利润增速 > 5%（成长中的股息，买入分 +1.5）**：不仅有高股息，盈利还在增长，意味着股息金额本身会继续增加——是最优质的股息股形态
- **零股息 + ROE ≥ 20%（利润留存复利，买入分 +1，移除无股息惩罚）**：不分红不等于差——ROE 极高的企业把利润留在内部复利往往比派息更能创造价值（巴菲特效应）

**卖出分高的条件**：
- 零股息或停止分红（轻微负面，2 分）（高 ROE 例外，见上）
- **股息率 ≥ 2% + ROE < 5% 或负债率 > 70%（股息陷阱风险，卖出分+3）**：利润极低或负债高企的企业高分红，往往是分红政策不可持续的信号——要么即将减派、要么在蚕食净资产
- **股息率 ≥ 4% + 利润增速 < -20%（股息行将被削减，卖出分 +2）**：盈利崩溃式下滑而股息率还高，是最典型的"股息陷阱"——未来分红很可能被大幅减少甚至取消，当前高股息率是虚假诱饵

**关键原则**：股息 = 盈利 × 分红率。财务健康度（ROE + 负债）是静态可持续性检验；盈利趋势是动态可持续性检验。两者共同构成完整的股息质量评估：表面高股息 + 盈利正在崩溃 = 最危险的价值陷阱。

**注意**：A 股分红文化较弱，此因子在高股息策略中权重较高，普通策略权重较低。

---

#### 9. 量比（volume_ratio）
**衡量内容**：当日量 vs 5日均量（量比），结合价格方向判断量价配合

**买入分高的条件**：
- 量比 2.5–4 倍 + 价格上涨（放量上涨，买入分提升 20%）
- 量比 < 0.8 倍 + 价格下跌（缩量下跌，卖盘枯竭，小幅加分）

**买入分降低的条件**：
- 量比 ≥ 1.5 倍 + 跌幅 ≥ 2%（放量下跌，买入分降至 30%）

**卖出分高的条件**：
- 量比 ≥ 1.5 倍 + 跌幅 ≥ 2%（放量下跌，出货信号，7 分）
- 量比 < 0.8 倍 + 涨幅 ≥ 1%（缩量上涨，不可持续，5 分）
- 量比 > 8 倍（极端放量，恐慌出货，7 分）
- 量比 > 5 倍（轻微顶部预警，4 分）

**关键原则**：放量本身不是信号，放量的方向才是。

---

#### 10. 均线多头排列（ma_alignment）
**衡量内容**：价格是否在 MA5 上方、MA5>MA10、MA10>MA20、MA20>MA60 四个条件；叠加近5日 vs 近20日成交量趋势；叠加分析师预期修正方向双重确认

**买入分高的条件**：
- 四个条件全部满足（完美多头排列，最高 15 分）
- 三个条件满足（基本多头，11 分）
- **完美多头排列 + 近5日量 > 近20日量的 115%（量价齐升，趋势确认，买入分 +2）**：成交量放大说明真实买盘进场，不是无量虚涨
- **完美多头排列 + 分析师净上调 ≥ 2（技术 + 基本面双重确认，买入分 +2）**：均线多头说明价格在涨，分析师上调说明业绩在改善，两个完全独立的信号同向

**卖出分高的条件**：
- 四个条件全部反转（完全空头排列，13 分）
- 仅 1 个条件满足（基本空头，9 分）
- 价格跌破 MA5 且整体偏弱（5 分）
- **完美多头排列 + 近5日量 < 近20日量的 75%（价升量缩，趋势吃老本，卖出分 +3）**：上涨但成交量持续萎缩，买盘在消失，随时可能反转
- **完全空头排列 + 近5日量 > 近20日量的 115%（放量下跌，出货加速，卖出分 +2）**
- **完全空头排列 + 分析师净下调 ≤ -2（技术 + 基本面双重确认下跌，卖出分 +2）**：均线空头说明趋势已破，分析师下调说明业绩恶化中，双重确认

**卖出分降低的条件**：
- **完全空头排列 + 近5日量 < 近20日量的 75%（缩量下跌，卖盘枯竭，卖出分 -2）**：跌势中成交量萎缩是底部临近的信号

**关键原则**：均线排列 = 技术方向，成交量 = 技术力度，分析师修正 = 基本面方向。三个维度分别来自价格、资金、卖方研究，信息完全不重叠。全部一致时是系统中最高置信度的信号。

---

#### 11. 低波动（low_volatility）
**衡量内容**：过去60交易日年化波动率，越低越稳健；叠加 MA5/MA20 趋势方向 + 市场环境（regime）两重交叉

**买入分高的条件**：
- 年化波动率 ≤ 15%（极稳定）
- 年化波动率 ≤ 25%（低波动）
- **低波动（≤ 25%）+ MA5 > MA20（上升趋势中的安静走强，买入分 +2）**：价格缓步上涨但没有散户追捧，往往是机构慢慢建仓的形态，是最可持续的涨势
- **低波动（≤ 25%）+ 熊市（regime ≤ 3，买入分 +2）**：熊市中低波动股票提供资本保护；Ang et al. 学术研究证实，熊市中低波动因子显著跑赢指数（防御性溢价最高的市场环境）

**卖出分高的条件**：
- 年化波动率 > 80%（极端波动，高风险）
- 年化波动率 > 60%（高波动预警）
- **低波动（≤ 25%）+ MA5 < MA20（下降趋势中的安静走弱，卖出分 +2）**：没有恐慌抛售但也没人接盘，价格在无声无息中慢慢溜走
- **高波动（> 45%）+ 熊市（regime ≤ 3，卖出分 +1.5）**：高 beta 在下行市中放大亏损，是最不利的组合

**买入分降低的条件**：
- **低波动（≤ 25%）+ 牛市（regime ≥ 7，买入分 -1.5，卖出分 +1）**：牛市由动量和成长股主导，持有低波动股票是显著的机会成本，等价于放弃市场 beta

**关键原则**：低波动 × 市场环境 揭示了这个因子最重要的时机性质——同样的"低波动"在熊市是最有价值的防御资产，在牛市是拖累组合的低效持仓。Ang 等人的经典研究表明：低波动因子的 alpha 绝大部分来自熊市阶段。

---

### 扩展因子 A 组续（来自 factors_extended.py）

---

#### 12. 短期反转（reversal）
**衡量内容**：近1个月涨跌幅，基于 A 股散户过度反应的反转效应；叠加52周价格位置 + 近期成交量 + 基本面质量三重交叉

**买入分高的条件**（超跌反弹）：
- 近1月跌幅 ≥ 15%（强力超跌，10 分）
- 近1月跌幅 5–15%（一般超跌，线性得分）
- **近1月跌幅 ≥ 10% + 52周位置 < 30%（底部恐慌，买入分+2）**：散户在低位割肉，主力趁机吸筹，是最典型的反转信号
- **近1月跌幅 ≥ 10% + 近5日量 < 前10日量的 80%（缩量下跌，买入分+2）**：卖盘正在枯竭，经典底部形成信号
- **近1月跌幅 ≥ 10% + ROE ≥ 12% + 负债率 ≤ 60%（基本面扎实的超跌，买入分+1.5）**：好公司因情绪压制而超跌——市场在过度惩罚一个财务健康的企业，真正的反转概率更高

**卖出分高的条件**（超涨回调风险）：
- 近1月涨幅 ≥ 20%（强力超涨，回调风险高，9 分）
- 近1月涨幅 10–20%（线性 5–9 分）
- 近1月涨幅 5–10%（轻微警惕，2 分）
- **近1月跌幅 ≥ 10% + 52周位置 > 70%（高位下跌，卖出分+3）**：这不是超跌，是趋势转折的开始，切忌抄底
- **近1月跌幅 ≥ 10% + ROE < 5% 或负债率 > 70%（弱基本面超跌，卖出分+2）**：企业本身质地差，跌不是市场情绪错误，而是对基本面的正确定价，继续下跌风险高

**买入分降低**：
- **近1月跌幅 ≥ 10% + 52周位置 > 70%（高位下跌）→ 买入分 -4**：不要把高位的跌当成超跌
- **近1月跌幅 ≥ 10% + 近5日量 > 前10日量的 120%（放量下跌）→ 买入分 -2**：量能仍在扩大说明恐慌还未结束
- **近1月跌幅 ≥ 10% + ROE < 5% 或高负债（弱基本面下跌）→ 买入分 -2.5**：这可能是"刀尖"，而非"反弹机会"

**卖出分降低**：
- **近1月涨幅 ≥ 10% + 52周位置 < 30%（底部反弹）→ 卖出分 -2**：可能是底部突破，而非超涨，不急于做卖

**盈利修正交叉（真反转 vs 死猫弹）**：
- **超跌（1月跌幅 ≥ 10%）+ 分析师净调升 ≥ 2（买入分 +2，真反转信号）**：价格超跌 + 分析师同时开始升级 = 基本面改善在支撑反弹，大概率是真反转而非反弹陷阱
- **超跌 + 分析师净调降 ≤ -2（买入分 -1.5，卖出分 +1.5，死猫弹风险）**：价格虽然超跌，但基本面还在恶化——典型的"刀尖下落"模式，抄底风险极高
- **超涨（1月涨幅 ≥ 10%）+ 分析师净调升 ≥ 2（卖出分 -1.5）**：涨势有基本面支撑，降低超涨卖出压力
- **超涨 + 分析师净调降 ≤ -2（卖出分 +1.5）**：涨势无基本面支撑，反弹是没有锚的，放大卖出信号

**极端跌幅交叉（均值回归概率随跌幅非线性上升）**：
- **近1月跌幅 > 25%（极端踩踏，买入分 +1.5，卖出分 -2）**：单月跌幅超过 25% 在统计上极为罕见，往往是散户极度恐慌导致的过度超跌，均值回归概率在此区间显著提升；同时大幅削减卖出分——不应追空一个已经跌了 25% 的股票
- **近1月跌幅 > 20%（深度超跌，买入分 +0.5，卖出分 -1）**：跌幅虽未达极端，但已深度超跌，均值回归倾向增强

**关键原则**：反转因子 IC 最高（0.070），但其弱点在于会把"下跌中的差公司"也打分很高。基本面质量交叉 + 盈利修正交叉共同解决这一核心缺陷。极端跌幅交叉进一步精化：超跌越深，均值回归概率越高，但同时说明近期已无卖出价值——应减轻卖出冲动，而非加大。

**背景**：A 股反转因子 IC 约 0.070，是所有因子中信号最强的之一，默认权重 2.0。

---

#### 13. 应计项目（accruals）
**衡量内容**：应计利润占总资产比例，判断盈利质量（现金含量）；叠加净利润增速区分"高增长靠真现金"还是"高增长靠应计虚增"

公式：应计比例 = (净利润 - 经营现金流) / 总资产

**买入分高的条件**（盈利质量高）：
- 应计比例 ≤ -5%（净利润大幅低于现金流，现金收益充足）
- **应计比例 ≤ -5% + 净利润增速 ≥ 20%（高增长 + 现金充足，成长质量最佳，买入分+2）**

**卖出分高的条件**（盈利质量差）：
- 应计比例 ≥ 10%（利润大幅超出现金流，存在虚增嫌疑）
- 应计比例 0–10% 线性计分
- **应计比例 ≥ 5% + 净利润增速 ≥ 20%（高增长但现金不支撑，成长质量失配，卖出分+2）**
- **应计比例 ≥ 5% + 净利润增速 < 0%（利润下滑还靠应计撑，双重质量预警，卖出分+1）**

**关键原则**：应计项目 × 盈利增速的交叉规则识别"成长故事的真伪"——高速增长若伴随高应计，说明利润被人为前置确认，大概率不可持续；反之，高增长 + 负应计是真实现金驱动的优质成长。

---

#### 14. 资产扩张（asset_growth）
**衡量内容**：总资产同比增长率，过度扩张警示；叠加 ROE 水平判断扩张是"价值创造"还是"帝国主义"

**买入分高的条件**：
- 资产增速 ≤ 5%（经营克制）

**卖出分高的条件**：
- 资产增速 ≥ 50%（极端扩张，8 分）
- 资产增速 30–50%（激进扩张，5 分）
- **资产增速 ≥ 20% + ROE < 5%（低回报扩张，卖出分+2）**：在资本回报极低的情况下大举扩张，是典型的帝国主义式经营——管理层追求规模却以牺牲股东回报为代价

**卖出分降低**：
- **资产增速 ≥ 20% + ROE ≥ 15%（高回报扩张，卖出分-2）**：在高回报背景下快速扩张属于价值创造型增长——比如茅台大规模扩产，每增加一单位资产创造大量收益，不应被同等对待

**注意**：A 股投资者偏好成长故此因子买入权重默认为 0（已归零），但卖出信号（极端扩张）仍有效。ROE 交叉让卖出信号更精准，避免惩罚真正的优质扩张。

---

#### 15. Piotroski F 分（piotroski）
**衡量内容**：9 项基本面二值信号综合评分，涵盖盈利能力、杠杆、经营效率；叠加52周价格位置判断"质量与估值"匹配度

**买入分高的条件**：
- F 分 ≥ 7（强基本面，满分 9）
- **F 分 ≥ 7 + 52周位置 < 30%（强基本面 + 低价位 = "质量+价值"双重机会，买入分 +2）**：这是量化策略中最经典的买入组合——公司基本面扎实，但股价处于低迷区，市场给了折价买入优质公司的机会
- **F 分 ≥ 7 + 估值偏低（PE 百分位 ≤ 30% 或 PB 百分位 ≤ 30%）（数据验证的 GARP，买入分 +1.5）**：基本面多项指标同步改善（F ≥ 7）且估值便宜，属于最有据可查的买入配置

**卖出分高的条件**：
- F 分 ≤ 2（基本面极弱，8 分）
- F 分 3–4（偏弱，4 分）
- **F 分 ≤ 2 + 52周位置 > 70%（弱基本面 + 高价位 = 价值陷阱风险，卖出分 +2）**：基本面差却在高位，完全依赖情绪支撑，任何基本面恶化都可能引发快速下跌
- **F 分 ≤ 3 + 估值偏贵（PE/PB 百分位 ≥ 80%）（基本面恶化 + 估值高企，卖出分 +1.5）**：基本面多项指标同时弱化，却仍以溢价定价——"完美定价"下出现裂缝，最容易引发估值杀

**9项信号**：ROA>0、经营现金流>0、ROA改善、现金流>净利润（4项盈利）；负债率下降、流动比率改善、无摊薄（3项杠杆）；毛利率改善、资产周转率改善（2项效率）

---

#### 16. 融券做空比例（short_interest）
**衡量内容**：融券余额 / 流通市值，衡量做空力量；叠加52周价格位置 + 分析师预期修正方向进行双重交叉分析

**买入分高的条件**：
- 融券比例 ≤ 0.5%（几乎无做空压力）
- **融券比例 ≥ 3% + 52周位置 < 30%（高空仓叠加低位 = 轧空潜力，买入分+3）**：空头在高位做空，但股价已在低位，一旦反转空头被迫回补
- **融券比例 ≥ 3% + 分析师净上调 ≥ 2（逼空催化剂，买入分 +2）**：空头重仓 + 分析师集体上调预期，双重压力下空头覆盖将加速

**卖出分高的条件**：
- 融券比例 ≥ 5%（重度做空，9 分）
- 融券比例 3–5%（线性 5–9 分）
- **融券比例 ≥ 3% + 52周位置 > 70%（高位重度做空 = 空头位置合理，卖出分+2）**：在高位做空是理性判断，增强卖出信号
- **融券比例 ≥ 3% + 分析师净下调 ≤ -2（空头被基本面验证，卖出分 +2）**：机构和分析师同向看空，空头立场更有底气

**关键原则**：高融券比例在底部是"轧空机会"，在高位是"理性看空"，两者信号相反。当分析师预期同步支持时，信号强度翻倍。

---

#### 17. RSI 信号（rsi_signal）
**衡量内容**：14日 RSI，超买超卖判断；叠加 MA5/MA20 趋势方向 + 近期量能变化双重交叉

**买入分高的条件**：
- RSI ≤ 30（超卖区，10 分）
- RSI 30–50（回暖区，线性 5–10 分）
- **RSI ≤ 30 + MA5 > MA20（趋势向上中的回调，理想低吸点）**
- **RSI ≤ 30 + 近5日量/前10日量 < 0.80（超卖 + 缩量，卖盘枯竭，反转确认，买入分 +2）**

**卖出分高的条件**：
- RSI ≥ 80（极度超买，9 分）
- RSI 70–80（线性 5–9 分）
- RSI 60–70（轻微警惕，2 分）
- **RSI ≥ 70 + MA5 < MA20（趋势向下中的反弹，死猫跳，卖出分+2）**
- **RSI ≥ 70 + 近5日量/前10日量 < 0.80（超买 + 量能萎缩，买盘在消退，卖出分 +2）**

**卖出分降低的条件**：
- **RSI ≥ 70 + MA5 > MA20（确认上升趋势，可能仍有上涨空间，卖出分 -2）**
- **RSI ≥ 70 + 近5日量/前10日量 > 1.30（超买但量能仍在扩大，动量仍强，卖出分 -1）**

**买入分降低的条件**：
- **RSI ≤ 30 + MA5 < MA20（趋势向下超卖，"刀口舔血"，买入分 -3）**

**关键原则**：MA 交叉判断趋势方向，量能交叉判断极端信号的可持续性。两个维度共同确认才是最强信号。

---

#### 18. MACD 信号（macd_signal）
**衡量内容**：MACD 柱状图方向和强度；叠加52周价格位置，区分底部突破和高位末端行情

**买入分高的条件**：
- MACD 柱为正且持续扩大（牛市加速，8–10 分）
- MACD 柱为负但正在收窄（底部转势，4 分）
- **MACD 金叉（柱转正+扩大）+ 52周位置 < 30%（底部突破，买入分+2）**：低位突破往往是最有效的趋势启动信号

**卖出分高的条件**：
- MACD 柱为负且持续扩大（空头加速，7–9 分）
- MACD 柱为正但快速缩小（顶部减速，3–5 分）
- **MACD 死叉（柱转负+扩大）+ 52周位置 > 70%（高位顶部确认，卖出分+2）**：高位死叉是最典型的见顶信号
- **MACD 金叉 + 52周位置 > 70%（高位末段上涨，卖出分+2）**：高位出现金叉可能是最后一波拉升，需警惕

**卖出分降低**：
- **MACD 死叉 + 52周位置 < 30%（低位测底，卖出分-2）**：低位死叉卖压相对有限，底部可能正在形成

**成交量确认交叉**（20日/60日成交量比值）：
- **MACD 金叉（柱转正+扩大）+ 量比 > 1.3（放量确认，买入分 +1.5）**：金叉叠加量能同步放大，是最可信的趋势启动组合——方向与流动性双重验证
- **MACD 金叉 + 量比 < 0.8（缩量金叉，买入分 -1）**：技术指标转多但成交量萎缩，信号可信度存疑，可能是假突破
- **MACD 死叉（柱转负+扩大）+ 量比 > 1.3（放量跌破，卖出分 +1.5）**：空头趋势叠加量能扩张，是加速下跌的确认信号

**关键原则**：MACD 本质是趋势惯性的量化，但惯性需要"燃料"（成交量）来维持。金叉+放量 = 方向与流动性双确认，是最高置信度的趋势启动；金叉+缩量 = 信号存疑；死叉+放量 = 跌势加速，而非超卖反弹。

---

#### 19. 换手率百分位（turnover_percentile）
**衡量内容**：近5日平均换手率 vs 90日均值，结合价格方向判断量价配合

**买入分高的条件**：
- 比值 ≥ 3 倍 + 涨幅 ≥ 1%（强势吸筹确认，10 分）
- 比值 1.5–3 倍 + 价格上涨（温和放量，主动积累区，8–10 分）
- 比值 0.8–1 倍 + 价格下跌（缩量下跌，卖盘枯竭，5.5 分）

**买入分降低的条件**：
- 比值 ≥ 4 倍 + 跌幅 ≥ 2%（高换手暴跌，出货信号，3 分）
- 比值 ≥ 3 倍 + 跌幅 ≥ 2%（较高换手下跌，4 分）
- 比值 < 0.8 倍 + 涨幅 ≥ 0.5%（缩量上涨，1.5 分）

**卖出分高的条件**：
- 比值 ≥ 3 倍 + 跌幅 ≥ 2%（高换手暴跌，出货信号，9 分）
- 比值 ≥ 1.5 倍 + 跌幅 ≥ 2%（活跃量下跌，6 分）
- 比值 < 0.8 倍 + 涨幅 ≥ 1%（缩量上涨，不可持续，5 分）
- 比值 ≥ 4 倍（极端放量，无价格方向加成，7 分）
- 比值 ≥ 3 倍（较高换手，4 分）

**近1月价格方向交叉**（叠加近20日收益率）：
- **比值 < 0.7 倍 + 近1月涨幅 +3% 至 +15%（静悄悄的积累，买入分 +1.5）**：缩量慢涨是主力无声建仓的最佳形态，价格稳步上升但成交极度萎缩，说明卖盘枯竭、买盘在慢慢消化
- **比值 ≥ 2.0 倍 + 近1月跌幅 ≥ 5%（持续出货，卖出分 +1.5）**：放量下跌持续一个月，远不只是单日波动，是机构或大户系统性减仓的证据

**关键原则**：放量本身不是信号，放量的方向才是；叠加1月收益率把单日噪音过滤掉，看趋势维度的量价背离。

---

#### 20. 筹码分布（chip_distribution）
**衡量内容**：52周价格位置 × 大单/小单资金流向的交叉分析

**买入分高的条件**：
- **底部恐慌**：52周位置 < 30% 且小单净卖出远大于大单净卖出（散户割肉，机构坚守）→ 9–10 分
- **底部积累**：52周位置 < 30% 且两方均净流入 → 8 分

**卖出分高的条件**：
- **高位机构派发**：52周位置 > 70% 且大单净卖出远大于小单净卖出（机构出货）→ 7–10 分
- **底部机构逃跑**：52周位置 < 30% 且大单净卖出远大于小单（机构在低位都在卖）→ 7–10 分

**中性场景**：
- 高位散户跑路+大单净买入（可能对倒，信号不明）→ 5 分 / 4 分
- 中间区间 → 5 分 / 2 分

---

### 扩展因子 B 组（满分 10–15 分，需额外 API）

---

#### 21. 股东人数变化（shareholder_change）
**衡量内容**：季度股东人数环比变化，衡量筹码集中程度；叠加52周价格位置判断集中/分散的背景含义

**买入分高的条件**：
- 股东人数减少 ≥ 10%（筹码高度集中，15 分）
- 股东人数小幅减少（线性 8–15 分）
- **股东人数减少 ≥ 5% + 52周位置 < 30%（低位筹码集中 = 主力在低价区悄然吸筹，买入分 +3）**：这是 A 股最经典的主力建仓模式——价格低迷、股东减少，代表主力在锁仓，反转行情临近

**卖出分高的条件**：
- 股东人数增加 ≥ 20%（筹码严重分散，大规模派发，12 分）
- 股东人数增加 10–20%（线性 7–12 分）
- **股东人数增加 ≥ 10% + 52周位置 > 70%（高位筹码分散 = 机构出货给散户，卖出分 +2）**：股价在高位、散户接盘，是最典型的顶部派发信号

**卖出分降低的条件**：
- **股东人数增加 ≥ 10% + 52周位置 < 30%（低位分散 = 可能是新买入者在低位进场，卖出分 -2）**：低价吸引更多新投资者进入，不一定是机构出逃

**盈利修正交叉（双机构确认）**：
- **股东人数减少 ≥ 5% + 分析师净调升 ≥ 2（买入分 +2）**：筹码集中（主力锁仓）+ 分析师上调预期（卖方看多）= 两个完全独立的机构视角同时指向同一方向，是比单一信号高一个量级的确认信号
- **股东人数增加 ≥ 10% + 分析师净调降 ≤ -2（卖出分 +2）**：筹码分散（主力出货）+ 分析师下调（卖方认错）= 双重机构撤退，是系统中最强的卖出/减仓组合之一

**背景**：A 股筹码集中因子 IC 约 0.065，强信号，默认权重 2.0。

---

#### 22. 龙虎榜（lhb）
**衡量内容**：近90日龙虎榜机构席位净买入金额；叠加52周价格位置，区分"底部发现"与"高位派发"

**买入分高的条件**：
- 机构净买入 ≥ 5000 万（10 分）
- **机构净买入 ≥ 1000 万 + 52周位置 < 30%（底部发现，买入分 +2，卖出分 -1）**：机构在历史低位借龙虎榜大单建仓，是最具可信度的底部识别信号之一——机构承担了买在低位的信息不对称成本

**卖出分高的条件**：
- 机构净卖出 ≥ 5000 万（9 分）
- 净卖出 1000–5000 万（线性 5–9 分）
- **机构净卖出 ≥ 1000 万 + 52周位置 > 70%（高位派发确认，卖出分 +2）**：机构在历史高位通过龙虎榜大单减仓，是最直接的分布形态证据——以高价卖给涌入的散户
- **机构净买入 ≥ 1000 万 + 52周位置 > 70%（高位"买入"存疑，卖出分 +1）**：高位出现机构买单需要警惕——可能是拉高出货的前奏（对倒或配合主力），而非真实的看多建仓

**注意**：A 股研究显示龙虎榜标记的股票短期可能反向（标记往往是短期顶部），故买入权重默认为 0，但叠加价格位置后信号区分度显著改善。

---

#### 23. 解禁压力（lockup_pressure）
**衡量内容**：未来90天解禁市值 / 流通市值，衡量供给冲击；叠加52周价格位置 + 盈利增速判断解禁冲击能否被市场吸收

**买入分**：无解禁 → 2 分（中性，不作为买入理由）

**卖出分高的条件**（主要是卖出因子）：
- 解禁比例 ≥ 20%（9 分，重大供给冲击）
- 解禁比例 5–20%（线性 5–9 分）
- 解禁比例 1–5%（线性 1–5 分）
- **解禁比例 ≥ 5% + 52周位置 > 70%（高位解禁，股东有厚利可图，减持意愿强，卖出分+2）**
- **解禁比例 ≥ 5% + 利润增速 < 0（基本面恶化 + 大规模供给 = 无买盘接筹，卖出分+2）**：下行业绩无法吸引新多头，解禁冲击更难被市场消化

**卖出分降低的条件**：
- **解禁比例 ≥ 5% + 52周位置 < 30%（低位解禁，股东本身亏损，减持动力不足，卖出分-3）**
- **解禁比例 ≥ 5% + 利润增速 ≥ 20%（高速增长的企业吸引买盘，解禁冲击被基本面买方吸收，卖出分-2）**

**社交热度交叉（解禁遭遇散户FOMO）**：
- **解禁比例 ≥ 5% + 社交热度前 10%（极端热度助力解禁套现，卖出分 +2）**：大规模解禁恰逢媒体极度关注 + 散户FOMO情绪 = 机构减持的最佳窗口，潜在供给叠加散户接盘热情被放大，是 A 股"解禁 + 热搜"套现模式的经典组合
- **解禁比例 ≥ 5% + 社交热度后 50%（冷清环境下解禁，卖出分 -1）**：无散户接盘热情意味着潜在买方减少，出货更困难——降低（而非消除）解禁的卖出压力评估

**关键原则**：解禁压力 = 潜在供给 ÷ 潜在需求。高增长企业的解禁有买盘对冲；衰退企业的解禁供给只能打压股价。社交热度交叉升级了对"需求侧"的判断：极端热度时散户接盘意愿最强，是机构完成解禁套现的最优窗口；冷清时无散户接盘，解禁压力更难消化。

---

#### 24. 大股东增减持（insider）
**衡量内容**：近6个月重要股东增减持净数量，衡量内部人信心；叠加52周价格位置判断增减持的真实含义

**买入分高的条件**：
- 净买入比例 > 50%（大股东大幅增持，8–10 分）
- **净买入比例 > 30% + 52周位置 < 30%（低位增持 = 大股东在低价买入自家股票，最高置信度买入信号，买入分 +2）**：大股东用真金白银在股价低迷时增持，代表最强的内部人信心

**卖出分高的条件**：
- 净卖出比例 > 50%（大股东大幅减持，8 分）
- 净卖出比例 0–50%（线性 0–3 分）
- **净卖出比例 > 30% + 52周位置 < 30%（低位减持 = 红色警报——在亏损时仍然减持，说明大股东预见结构性问题，卖出分 +3）**：这是整个系统中最危险的信号之一，大股东宁愿亏损也要出逃
- **净卖出比例 > 30% + 52周位置 > 70%（高位减持 = 理性获利了结，卖出分 +2）**：高位减持是正常的，但仍是负面信号

**盈利修正交叉（大股东 + 分析师双向共识）**：
- **净买入比例 > 30% + 分析师净调升 ≥ 2（买入分 +2，双重确信信号）**：大股东用真金白银增持，同时独立的卖方分析师也在上调评级——两个完全独立的信息优势群体同时看多，内部人信息 + 外部分析共识双向验证，是系统中最强的多头信号之一
- **净卖出比例 > 30% + 分析师净调降 ≤ -2（卖出分 +2，双重退出信号）**：大股东和分析师同时看空，两个独立群体给出同向信号，是高确信度的卖出/减仓确认
- **净买入比例 > 30% + 分析师净调降 ≤ -2（卖出分 -1，大股东逆势增持）**：大股东在分析师集体唱空时仍然增持——内部人掌握分析师不知道的信息，管理层的真金白银行动比卖方报告更具说服力，适当降低卖出分

**关键原则**：增减持的含义完全依赖于价格水平。低位增持 = 大股东"押注"；低位减持 = 大股东"逃跑"，是性质截然不同的信号。盈利修正交叉进一步区分：大股东增持 + 分析师上调是双重确信（内部人 + 外部人同向）；大股东增持 + 分析师下调是管理层逆势信心（内部人信息 > 外部人分析），降低卖出冲动。

---

#### 25. 机构调研（institutional_visits）
**衡量内容**：近90日机构调研次数，衡量机构关注度；叠加盈利预测修正方向，区分"公开上调前的提前埋伏"与"共识正在形成"

**买入分高的条件**：
- 调研次数 ≥ 10 次（高关注，10 分）
- 调研次数 5–10 次（中等关注）
- **调研次数 ≥ 5 + 净修正次数 = 0（上调前的提前信号，买入分 +1）**：机构频繁调研但分析师尚未公开上调——这是机构在公众信息传播前安静建仓的典型形态
- **调研次数 ≥ 5 + 净上调 ≥ 2（机构共识正在形成，买入分 +1）**：高频调研叠加已有上调，说明机构实地尽调验证了投资逻辑，共识在加速凝聚

**卖出分高的条件**（弱信号）：
- 零调研次数 → 2 分（机构失去兴趣，轻微负面）

**关键原则**：机构调研是分析师上调的领先指标。大量调研但尚未上调（净修正为0）往往意味着分析师仍在核实，上调报告还在路上；等到上调出来，股价可能已经提前反应了。

---

#### 26. 行业动量（industry_momentum）
**衡量内容**：所属行业近1月涨幅 vs 沪深300，判断板块轮动；叠加个股52周价格位置区分"板块轮动尚未传导的迟动股"和"高位追入"

**买入分高的条件**：
- 行业超额收益 ≥ 5%（板块强势，10 分）
- 行业超额收益 0–5%（线性 5–10 分）
- **行业超额收益 ≥ 2% + 个股52周位置 < 30%（板块在涨但个股还在低位，板块轮动尚未传导，迟动机会，买入分+2）**

**卖出分高的条件**：
- 行业超额收益 ≤ -5%（板块严重弱于大盘，9 分）
- 行业超额收益 -5–0%（线性 3–9 分）
- **行业超额收益 ≤ -2% + 个股52周位置 > 70%（板块弱但个股仍高位，向下均值回归风险，卖出分+2）**
- **行业超额收益 ≥ 2% + 个股52周位置 > 70%（板块涨但个股已在高位，追高进入尾段风险，卖出分+1）**

**卖出分降低的条件**：
- **行业超额收益 ≤ -2% + 个股52周位置 < 30%（板块弱且个股已在低位，损伤已充分消化，卖出分-1）**

**关键原则**：板块动量 × 个股位置捕捉板块轮动的"时间差"机会——行业强势但个股还滞涨（低位），意味着资金尚未传导到该股，是板块内最后的低位布局窗口；反之行业弱势而个股还在高位是典型的补跌风险。

---

#### 27. 北向持股变化（northbound_actual）
**衡量内容**：沪深港通实际持股数量近5期变化，区别于大单流向代理；叠加价格位置区分主动卖出与被动减仓；叠加近1个月动量判断外资是否在逆势布局

**买入分高的条件**：
- 持股量增加 ≥ 5%（强流入，10 分）
- **持股量增加 ≥ 2% + 1月跌幅 ≥ 10%（外资在股价大跌时逆势加仓，高置信度抄底信号，买入分+2）**：外资在多数散户恐慌时买入，是系统中最有信息含量的反向信号之一

**卖出分高的条件**：
- 持股量减少 ≥ 5%（明显流出，9 分）
- 持股量减少 2–5%（线性 5–9 分）
- 持股量小幅减少 0–2%（2 分）
- **持股量减少 > 2% + 52周位置 > 70%（高位主动减仓，有利可图，卖出分+2）**
- **持股量减少 > 2% + 1月跌幅 ≥ 10%（外资在下跌中同步撤出，基本面确信看空，卖出分+1.5）**：外资出逃 + 股价持续下行，双重验证的卖出信号

**卖出分降低的条件**：
- **持股量减少 > 2% + 52周位置 < 30%（低位减仓，很可能是 ETF 被动赎回 / 权重调整，而非主动看空，卖出分-2.5）**

**盈利修正交叉（双机构确认）**：
- **持股增加 ≥ 2% + 分析师净调升 ≥ 2（买入分 +2，外资+国内分析师双向共识）**：外资实际增仓 + 国内卖方分析师同时上调，是两个完全独立的机构群体给出相同方向信号——最高置信度的机构共识信号，大概率正在形成机构配置合力
- **持股减少 ≤ -2% + 分析师净调降 ≤ -2（卖出分 +2，双机构撤退）**：外资撤出 + 国内分析师下调，两类机构同时放弃，是系统中最强的卖出/减仓信号之一

**板块动量交叉（逆板块北向 = 最高确信度信号）**：
- **持股增加 ≥ 2% + 行业超额收益 ≤ -2%（逆板块净流入，买入分 +2）**：外资在整个板块被抛售时单独买入该股，是系统中最具信息含量的主动配置信号——背景噪音为负而外资仍在增仓，说明有明确的个股逻辑
- **持股增加 ≥ 2% + 行业超额收益 ≥ +5%（顺势热门板块买入，买入分 -1）**：外资跟着热板块流入，可能是被动权重调整或趋势追踪，而非独立看多信号
- **持股减少 ≥ 2% + 行业超额收益 ≥ +5%（在强势板块中主动减仓，卖出分 +1.5）**：板块热、外资却在减持，是外资已经看到估值顶部或基本面隐忧的强烈暗示

**关键原则**：北向的真正价值在于判断外资是"主动配置"还是"被动调整"——逆势（下跌中买入）和顺势（高位出逃）都是主动信号；低位减仓和高位买入则更可能是被动行为。板块动量交叉把"个股北向"与"板块背景"对比，逆板块的净流入是最高置信度的机构独立判断。

---

#### 28. 盈利预测修正（earnings_revision）
**衡量内容**：分析师评级上调/下调数量净值，衡量卖方预期变化；叠加52周价格位置区分"逆势发现"和"追涨上调"；叠加历史实际利润增速判断上调是否有基本面支撑

**买入分高的条件**：
- 净上调次数 ≥ 3（强势上调，10 分）
- 净上调次数 1–3（线性 7–10 分）
- **净上调 ≥ 2 + 52周位置 < 30%（低位上调，分析师逆势发现，买入分 +2）**：分析师在市场冷落时主动上调是真正的 alpha 信号，低位 + 分析师上调 = 双重催化剂
- **净上调 ≥ 2 + 历史利润增速 ≥ 20%（有实际业绩背书的上调，买入分 +1.5）**：分析师看好，同时真实盈利也在高速增长——前瞻预测与当期结果相互验证，是最高确信度的上调信号

**卖出分高的条件**：
- 净下调次数 ≥ 3（强势下调，9 分）
- 净下调次数 1–3（线性 5–9 分）
- **净下调 ≥ 2 + 52周位置 > 70%（高位下调，确认顶部，卖出分 +2）**：分析师终于承认估值偏高时，通常意味着聪明钱已经开始撤退
- **净上调 ≥ 1 + 52周位置 > 70%（追涨上调，轻微卖出警示，卖出分 +1）**：分析师跟涨上调往往落后市场，股价已在高位意味着上调预期大部分已 Price in
- **净上调 ≥ 2 + 历史利润增速 < 0%（空洞的上调，卖出分 +1.5）**：分析师在上调预期，但实际盈利正在下滑——往往是关系驱动的报告或市值管理配合，而非真实信号

**机构调研交叉（预测修正 + 实地尽调 = 两层验证）**：
- **净上调 ≥ 2 + 近90日调研次数 ≥ 5（买入分 +1.5，卖方+买方双重共识）**：分析师公开上调报告 + 基金经理/买方机构实地到访——卖方预测和买方现场验证同时给出同向信号，是当前系统中最高置信度的卖方与买方共识配置信号
- **净下调 ≤ -2 + 近90日调研次数 = 0（卖出分 +1.5，彻底被机构抛弃）**：分析师在下调，而且没有任何机构前来调研——卖方唱空 + 买方缺席，是完全的机构放弃状态，最彻底的悲观信号
- **净上调 ≥ 2 + 近90日调研次数 = 0（卖出分 +1，无实地支撑的纸面上调）**：分析师上调报告缺乏买方实地验证，更可能来自模型更新或市值管理目的，而非基于扎实调研的真实看多——上调的信号质量存疑

**关键原则**：分析师是跟随者而非领先者。低位逆势上调是稀有的高价值信号；高位上调往往是羊群效应。叠加历史增速交叉可以区分"真上调"（有业绩根基）和"假上调"（与实际趋势背离），显著提升信号纯度。机构调研交叉进一步增加一层买方现场验证：卖方上调 × 买方调研 = 两类机构通过完全不同渠道得出同一结论，是预测修正信号的最高置信度形态。

---

### 扩展因子 C 组（满分 10 分，行为与市场环境因子）

---

#### 29. 涨跌停板（limit_hits）
**衡量内容**：近20个交易日涨停/跌停次数净值（涨停数 - 跌停数）；叠加52周价格位置 + 基本面质量（ROE）双重交叉

**买入分高的条件**：
- 净涨停数 ≥ 3（频繁涨停，强动量，9 分）
- 净涨停数 ≥ 1（净涨停，7 分）
- 无涨跌停事件（中性，5 分）
- **净涨停数 ≥ 2 + 52周位置 < 30%（低位涨停突破 = 底部放量启动，买入分 +1）**：从底部区域涨停是最可信的趋势启动信号
- **净跌停数 ≥ 2 + 52周位置 < 30%（低位恐慌性跌停 = 反转信号，买入分 +3）**：散户在底部的极端恐慌往往是主力抄底的最佳时机
- **净涨停数 ≥ 2 + ROE ≥ 12%（业绩支撑的动量加速，买入分 +1.5）**：连续涨停有真实盈利能力做背书，是可持续的强势，而非纯游资炒作

**卖出分高的条件**：
- 净跌停数 ≥ 3（频繁跌停，9 分）
- 净跌停数 ≥ 1（净跌停，6 分）
- **净涨停数 ≥ 2 + 52周位置 > 70%（高位涨停狂欢 = 零售热潮见顶，卖出分 +3）**：高位出现频繁涨停往往是极度过热的信号，反转临近
- **净跌停数 ≥ 2 + 52周位置 > 70%（高位派发跌停 = 机构出货，卖出分 +2）**
- **净涨停数 ≥ 2 + ROE < 5%（纯游资热钱，无基本面支撑，卖出分 +2）**：没有利润支撑的连续涨停是 A 股最典型的热钱炒作模式，随时可能崩盘

**社交热度交叉（涨停 × 情绪 = 炒作识别三件套）**：
- **净涨停数 ≥ 2 + 社交热度前 5%（极端热度，卖出分 +2）**：连续涨停 + 媒体极度关注是 A 股最经典的"炒作顶部三件套"——涨停、热搜、散户跟风，智慧资金通常在此阶段完成出货
- **净涨停数 ≥ 2 + 社交热度后 50%（低热度涨停，买入分 +1.5）**：连续涨停但媒体几乎无人关注，说明机构在悄然主导拉升，尚未被散户发现，是比高热度涨停更可持续的信号
- **净跌停数 ≥ 2 + 社交热度前 10% + 52周位置 < 30%（底部极度关注中跌停，卖出分 -1）**：低位 + 极端散户恐慌（跌停又热搜）= 短期恐慌可能触底，削减卖出冲动

**关键原则**：涨跌停 × 价格位置区分"底部启动"和"顶部泡沫"；涨跌停 × ROE 区分"基本面驱动"和"游资驱动"；涨跌停 × 社交热度区分"机构主导"和"散户炒作"。A 股连板股中，低热度连板股（机构主导）后续涨幅远超高热度连板股（散户炒作）。

---

#### 30. 涨跌惯性（price_inertia）
**衡量内容**：近期连续上涨/下跌天数；叠加成交量趋势判断趋势可持续性

**买入分高的条件**：
- 连续上涨 ≥ 4 天（强势上涨惯性，8 分）
- 连续上涨 ≥ 3 天（上涨惯性，7 分）
- 连续上涨 ≥ 2 天（小幅上涨，6 分）
- **连续上涨 ≥ 3 天 + 成交量放大（量价齐升，确认延续，买入分 +2，上限 10 分）**
- **连续下跌 ≥ 3 天 + 成交量收缩（跌势缩量，卖盘枯竭，潜在反弹，买入分 +2）**

**卖出分高的条件**：
- 连续下跌 ≥ 4 天（强势下跌惯性，7 分）
- 连续下跌 ≥ 3 天（下跌惯性，5 分）
- 连续下跌 ≥ 2 天（小幅下跌，3 分）
- **连续上涨 ≥ 3 天 + 成交量收缩（价升量缩，动力不足，卖出分 +3）**
- **连续下跌 ≥ 3 天 + 成交量放大（放量加速下跌，卖出分 +2）**

**年化波动率交叉**（60日收益率标准差 × √252）：
- **连续上涨 ≥ 3 天 + 年化波动率 ≤ 25%（低波动惯性，买入分 +2）**：稳定上涨 + 极低波动 = 机构资金在主导的可持续趋势（Ang et al. 研究证实低波动因子有显著 alpha，尤其在稳定上涨时）；A 股散户驱动的上涨通常伴随高波动，低波动惯性因此更具含金量
- **连续上涨 ≥ 3 天 + 年化波动率 > 50%（高波动惯性，卖出分 +1.5）**：上涨伴随剧烈波动，历史上这类走势均值回归概率高，不宜追高
- **连续下跌 ≥ 3 天 + 年化波动率 ≤ 25%（低波动跌势，卖出分 +1）**：安静且持续的下跌——没有恐慌，没有放量，往往是资金在有序撤离，比暴跌更难逆转

**关键原则**：趋势是方向，成交量是燃料，波动率是背景噪音。低波动惯性 = 机构推动的可持续趋势；高波动惯性 = 散户/游资驱动的过热，均值回归风险高；低波动下跌 = 无声的结构性撤退，不可小视。

---

#### 31. 社交热度（social_heat）
**衡量内容**：东方财富热股榜排名百分位，作为散户情绪代理；**反向指标**——极度关注 = 卖出信号；叠加52周价格位置区分"高位狂热"和"低位散户抢筹"

**买入分高的条件**（中等关注度）：
- 热股榜排名前 20%（7 分）：正处于媒体和机构的关注视野，适度热度代表资金聚焦
- 热股榜排名前 50%（5 分）：有一定关注度，中性偏好
- 热股榜排名后 50%（3 分）：关注度低，可能被忽视的价值股

**卖出分高的条件**（极端关注度）：
- 热股榜排名前 1%（8 分）：**极端零售热潮 = 强卖出信号**——"人人都知道的好股票"不是好股票，极度关注往往伴随估值泡沫和随后的快速下跌
- 热股榜排名前 5%（5 分）：关注度过高，需警惕
- **极高热度（前5%）+ 52周位置 > 70%（高位狂热，卖出分 +2）**：股价在历史高位叠加极端散户热潮 = "最后一棒"经典形态，主力出货完毕的信号

**卖出分降低的条件**：
- **极高热度（前5%）+ 52周位置 < 30%（低位抢筹，卖出分 -2）**：散户在历史低位疯狂买入，A 股低位空头挤压是真实现象（散户情绪可以自我实现），此时反向做空不如适度softening

**ROE 质量交叉（热度 × 基本面 = 情绪类型识别）**：
- **热度前 5% + ROE ≥ 15%（高热度+高质量，卖出分 -1.5，买入分 +1）**：极高关注度叠加真实盈利能力，说明市场是在"发现"优质公司而非盲目炒作；这是稀有的高热度但不看空的组合，削减卖出信号，同时温和加持买入
- **热度前 5% + ROE < 5%（高热度+劣质，卖出分 +2）**：极度关注 + 几乎没有真实盈利 = 最典型的投机旋风——散户用想象力代替利润在定价，极高崩盘风险
- **热度前 20% + ROE ≥ 15%（中等热度+高质量，买入分 +1）**：品质公司开始进入关注视野，尚未达到顶部泡沫状态，是参与窗口而非离场信号

**关键原则**：社交热度本质是散户情绪的强度指标；ROE 交叉揭示"热度的成色"。高热度+高ROE = 机构发现优质标的，情绪与基本面共振；高热度+低ROE = 纯粹投机旋风，是最危险的组合。两个变量合并判断把"是否关注"升级为"为什么关注"。

---

#### 32. 市场环境（market_regime）
**衡量内容**：沪深300指数 MA5/MA20/MA60 多头/空头排列，衡量系统性市场风险

**买入分高的条件**（牛市环境）：
- MA5 > MA20 > MA60（完全多头排列，9 分）：牛市最强信号，所有标的的买入信号有效性增强
- MA5 > MA20 且 MA20 ≈ MA60（复苏中，8 分）：趋势向上，但整体多头排列仍在形成中
- 当前价格 > MA20（6 分）：短中期偏多，市场中性偏好

**卖出分高的条件**（熊市环境）：
- 当前价格 < MA60（熊市，9 分）：**系统性熊市**——即使个股基本面优秀，在系统性下跌中也难逃下行拖累；所有买入信号需打折
- 当前价格接近但仍在 MA60 上方（7 分）：熊市风险，注意止损
- 当前价格 > MA60 但 < MA20（5 分）：市场处于调整，需谨慎

**关键原则**：市场环境是所有个股因子的"底色"。牛市中弱股也能涨，熊市中强股也难涨。这是唯一一个纯粹衡量系统性风险（而非个股信号）的因子。

**市场环境权重调节机制**：市场环境因子不仅作为独立因子参与评分，还动态调整其他因子的权重：
- **牛市（买入分 ≥ 7）**：动量因子权重 ×1.3、均线排列 ×1.2、反转因子 ×0.6（牛市中逆势反转容易亏损）；卖出侧压低仓位和估值的卖出敏感度
- **熊市（买入分 ≤ 3）**：动量因子权重 ×0.4（熊市突破大多是假信号）、反转因子 ×1.6（超跌反弹频繁）、价值因子 ×1.3（低估提供下跌缓冲）；卖出侧放大动量和仓位的卖出信号

---

#### 33. 概念板块动量（concept_momentum）
**衡量内容**：个股所属概念板块（A 股特有的题材/主题板块）的近 1 个月涨跌幅，捕捉热点概念驱动的板块联动效应；叠加个股相对概念板块的涨跌幅差异识别补涨机会和龙头过热风险

**买入分高的条件**（热点概念驱动）：
- 所属最强概念 1 月涨幅 ≥ +15%（9 分）：概念高度活跃，题材驱动力强
- 所属最强概念 1 月涨幅 +8% ~ +15%（7 分）：概念走强，板块共振
- 所属最强概念 1 月涨幅 +3% ~ +8%（5.5 分）：概念温和上涨
- **概念最强涨幅 ≥ +8% + 个股涨幅落后概念 ≥ 15%（买入 +2，补涨候选）**：热点板块已有其他成员率先启动，本股尚未跟上——典型的 A 股轮动补涨机会

**卖出分高的条件**（概念崩塌或个股过热）：
- 所属最差概念 1 月跌幅 ≤ -15%（8 分）：概念崩塌，板块估值重构压力大
- 所属最差概念 1 月跌幅 -8% ~ -15%（5 分）：概念走弱，题材退潮
- **概念最强涨幅 ≥ +8% + 个股领涨幅超过概念 ≥ 20%（卖出 +2，龙头见顶风险）**：个股已经远超板块平均涨幅——"龙头"效应消退后通常面临回调补跌，追高风险极高

**市场环境交叉（概念行情的持续性取决于市场背景）**：
- **热点概念（最强涨幅 ≥ +10%）+ 熊市（市场环境分 ≤ 3）→ 买入分 -2，卖出分 +1.5**：熊市概念炒作几乎完全由游资主导，3–5 天窗口后迅速撤退，持续性极差
- **热点概念 + 牛市（市场环境分 ≥ 7）→ 买入分 +1**：牛市板块共振有机构参与，延续性更强

**ROE 质量交叉（主题行情的含金量）**：
- **热点概念（最强涨幅 ≥ +8%）+ ROE ≥ 15%（高质量公司在热板块，买入分 +1.5）**：基本面优秀的公司被概念驱动资金发现，是有业绩锚定的主题行情，不只是讲故事
- **热点概念 + ROE < 5%（无利润支撑的概念炒作，卖出分 +2）**：连盈利都很少的公司却在热板块中狂涨——纯粹的题材泡沫，随时崩盘

**关键原则**：A 股的概念板块是资金轮动的核心逻辑之一。概念起势时补涨股是性价比最高的标的；概念退潮时即使个股基本面优秀也难逃下跌。市场环境交叉决定"要不要跟"；ROE 交叉决定"值不值得跟"——两者合并把"有没有概念"升级为"什么质地的概念"。

**注意**：概念板块成员数据首次构建约需 30 秒（之后缓存 6 小时）。概念板块与行业板块相互补充，行业反映业务属性，概念反映市场叙事和资金热点。

---

### 综合评分解读

**买入综合分（total_score）**：

| 分数 | 解读 |
|---|---|
| ≥ 80 | 优秀——各维度均强，高优先级关注 |
| 65–79 | 良好——整体稳健，值得跟踪 |
| 50–64 | 一般——有亮点但有明显短板 |
| 35–49 | 较弱——多维度偏弱，谨慎 |
| < 35 | 差——存在重大基本面或估值问题 |

**卖出综合分（total_sell_score）**：

| 分数 | 解读 |
|---|---|
| ≥ 70 | 强卖出信号——多项重要负面信号同时触发 |
| 50–69 | 中等卖出压力——考虑减仓 |
| 35–49 | 轻度警惕——密切监控 |
| 20–34 | 低卖出压力——持有，等待更明确信号 |
| < 20 | 无明显卖出信号 |

---
---

## English Version

### Scoring System Overview

Each factor independently outputs two scores, both on a 0–10 scale (core factors 0–25):

- **buy_score**: Higher = stronger buy signal; the factor dimension favors holding/buying
- **sell_score**: Higher = stronger sell signal; the factor dimension shows a meaningful negative signal

**Key principle: Low buy_score ≠ sell signal.** For example, flat momentum (low buy_score) does not imply selling. sell_score only rises when a genuine negative signal is present.

Composite scores (0–100):
- `total_score`: Weighted average of all buy_scores — measures overall bullish attractiveness
- `total_sell_score`: Weighted average of all sell_scores — measures overall selling pressure

---

### Core Factors (max 25 pts each)

---

#### 1. Value (value)
**Measures**: Relative valuation via PE (P/E ratio) and PB (P/B ratio); crossed with 3-month price momentum to filter value traps

**High buy_score** (cheap valuation):
- PE/PB in the bottom 20% of industry peers or own 3-year history
- Absolute fallback: PE < 15×, PB < 1× considered clearly undervalued
- **Deep value (PE/PB percentile ≤ 20%) + 3m return > +5% (value catalyst firing, buy +2)**: cheap and already moving — the market is beginning to recognize the discount

**High sell_score** (valuation bubble):
- PE/PB at or above 90th percentile vs industry or own history
- Absolute fallback: PE > 60×, PB > 5× triggers strong sell signal

**buy_score reduced / sell_score raised** (value trap):
- **Deep value + 3m return < −10% (value trap risk, buy −2, sell +1)**: cheap but still falling — the market is pricing in something the model doesn't know about

**Earnings revision cross (double-bottom / double-kill)**:
- **Deep value (PE/PB percentile ≤ 25%) + net analyst upgrades ≥ 2 (buy +3, double-bottom signal)**: cheap AND analysts awakening = the classic institutional accumulation trigger; two independent signal sources converging produces the highest-confidence value buy
- **High valuation (PE/PB percentile ≥ 80%) + net downgrades ≤ −2 (sell +3, double-kill)**: expensive AND fundamentals deteriorating — the most unambiguous quantitative short/reduce signal

**Sector-relative PEG cross (cheapest / most expensive growth stock in peer group)**:
- **PE percentile ≤ 30% + profit growth ≥ 20% (sector-cheapest growth stock, buy +2)**: the lowest-valued high-growth company in its peer group — the most direct expression of PEG < 1 in a sector-relative context; the highest-value-for-money entry within a sector allocation; cheap valuation + high growth = maximum room for valuation re-rating
- **PE percentile ≥ 80% + profit growth ≤ 0% (sector-most-expensive declining stock, sell +2)**: paying the sector's highest valuation premium for a company with contracting earnings — the most dangerous sector allocation combination; high premium + deteriorating fundamentals = dual valuation compression risk

**Key principle**: Cheap ≠ buy signal. Cheap + price stabilizing or recovering = true value opportunity. "Cheap but falling" is the most common value trap pattern in A-shares. Cheap + analyst upgrades is the strongest value confirmation: market pricing and sell-side consensus both giving the same signal simultaneously is the highest-conviction value entry window. The sector-relative PEG cross upgrades the question from "is this stock cheap?" to "is this the best-value growth opportunity within its sector?" — the natural intersection of value and growth investing.

**Note**: A-share markets can sustain stretched valuations for extended periods; treat sell_score as a warning, not a veto.

---

#### 2. Growth (growth)
**Measures**: Revenue growth, profit growth, and ROE trend; crossed with growth acceleration, ROE quality, AND valuation (PEG logic) for three-layer conviction assessment

**High buy_score**:
- Revenue YoY growth > 20%
- Net profit YoY growth > 20%
- ROE > 15%
- **Profit growth accelerating (current − prior ≥ 5 pp while positive, buy +1.5 or +3)**: upgrading from "stable" to "accelerating" is the typical institutional accumulation trigger
- **Profit growth ≥ 30% + ROE ≥ 15% (high-quality compounder, buy +2)**: rapid growth with excellent capital returns — the rarest and highest-quality growth archetype in A-shares
- **Profit growth ≥ 30% + PE percentile ≤ 40% (undervalued growth, PEG < 1 territory, buy +2)**: the market has not yet priced in the growth rate — historically cheap PE despite high growth = structural PEG opportunity; the highest-conviction combination in growth investing

**High sell_score**:
- Revenue declining (negative YoY growth)
- Net profit down > 20% YoY
- ROE < 3% (near-zero return on capital)
- **Profit growth decelerating sharply (−20 pp while growth < 20%, sell +4)**: high-growth narrative collapse
- **Profit growth ≥ 20% but ROE < 5% (hollow growth, sell +2)**: expanding revenues/profits with terrible capital efficiency — asset-heavy or unsustainable story
- **Profit growth ≥ 20% + PE percentile ≥ 80% (growth fully priced in, sell +1.5)**: every consensus buyer already owns it; upside requires flawless execution with zero error tolerance
- **Profit growth ≤ 5% + PE percentile ≥ 70% (expensive stagnant growth = PEG trap, sell +2)**: paying a growth premium for a stagnant or declining business — the maximum valuation gap, highest re-rating risk

**Key principle**: Growth × ROE identifies the quality of growth (efficiency-driven vs asset-heavy). Growth × PE percentile identifies whether you're paying a fair price for that growth (PEG logic). Undervalued high growth (PEG < 1) is the holy grail of growth investing; expensive stagnant growth is the most destructive value trap.

**Note**: Growth is priced in quickly in A-shares; factor IC ~0.027 (weak signal). Best used in combination.

---

#### 3. Momentum (momentum)
**Measures**: 1-month, 3-month, 6-month price returns, crossed with volume trend (vol_20d/vol_60d ratio) AND fundamental quality (ROE) to assess trend sustainability

**High buy_score**:
- 3-month return > 10% (trend established)
- 6-month return > 15% (sustained momentum)
- **Strong uptrend (3m ≥ +20% or 6m ≥ +25%) + ROE ≥ 15% (quality momentum, buy +2)**: price trend backed by real business returns — the most durable form of momentum

**High sell_score**:
- **Important**: 1-month decline is a reversal BUY signal, not sell
- 3-month decline > 5% starts generating sell_score
- 3-month decline > 20% or 6-month decline > 30% = strong sell
- Exception: 1-month crash > 30% adds a small sell component
- **Strong uptrend + ROE < 5% (low-quality momentum, sell +2)**: price surging but the business barely earns returns — classic speculative theme play with no fundamental anchor

**Volume divergence (sell_score adjustment)**:
- **3m gain ≥ 15% + vol_20d/vol_60d < 0.75 (price up, volume shrinking fast) → sell +4**: trend running on fumes — price rising with no participation
- **3m gain ≥ 15% + vol_20d/vol_60d < 0.85 (mild contraction) → sell +2**: mild warning
- **3m decline ≥ 15% + vol_20d/vol_60d < 0.70 (price down, volume drying up) → sell −3**: selling exhausted, fewer sellers left — reduce sell urgency

**Market regime cross (Daniel & Moskowitz momentum crash)**:
- **Strong uptrend (3m ≥ +15% or 6m ≥ +20%) + bear market (regime ≤ 3) → buy −3, sell +2**: momentum systematically fails in bear markets — one of the most empirically robust findings in factor investing; chasing uptrends in a bear market typically means buying into bear-market rallies, not genuine trend continuation
- **Strong uptrend + bull market (regime ≥ 7) → buy +1.5**: trend continuation probability is materially higher in bull markets; institutional capital is more willing to chase momentum when the macro tide is rising
- **Strong downtrend (3m ≤ −15% or 6m ≤ −20%) + bull market → sell −1.5**: an individual stock falling sharply in a bull market is more likely a mean-reversion opportunity than a structural decline — reduce sell urgency

**Key principle**: Price is direction, volume is fuel, ROE is the engine, market regime is the background context. The biggest trap in momentum investing is bear-market application — bull-market momentum stocks represent trend continuation; bear-market momentum stocks are often bear rallies in disguise. The regime cross is the critical lens for distinguishing the two.

---

#### 4. Quality (quality)
**Measures**: ROE level, gross margin, debt ratio — earnings quality indicators; crossed with 52-week price position to identify "quality at value" and "expensive junk" setups

**High buy_score**:
- ROE > 20% (high return on equity)
- Gross margin > 40% (strong moat)
- Debt ratio < 30% (low leverage)
- **High quality (ROE ≥ 15% + gross margin ≥ 30%) + 52w position < 30% (quality at value, buy +3)**: a genuinely excellent business trading cheaply — the canonical GARP accumulation opportunity
- **High quality + cheap valuation (PE percentile ≤ 30% or PB percentile ≤ 30%) (GARP signal, buy +2)**: quality business at a historically cheap valuation — the strongest fundamental buy signal; combines level quality with quantitative cheapness
- **Low quality + cheap valuation (PE/PB percentile ≤ 30%) (buy −2)**: superficially cheap but low-quality; the stock is not mispriced — the business is genuinely weak ("cheap for a reason")

**High sell_score**:
- Debt ratio > 70% (dangerous leverage)
- ROE < 3% (near-zero capital returns)
- Gross margin < 5% (almost no profit margin)
- **Low quality (ROE < 5% or gross margin < 10%) + 52w position > 70% (expensive junk, sell +3)**: a mediocre or poor-quality business priced near its annual high — typical of theme/narrative-driven rallies approaching their end

**sell_score reduced**:
- **High quality + extreme valuation (PE/PB percentile ≥ 85%) (sell −1.5)**: high-quality businesses deserve a valuation premium; expensive ≠ should sell

**Key principle**: Quality × price position recreates the classic "good business at a good price" screen. The GARP cross (quality × valuation percentile) adds a second layer: it quantifies how cheap "cheap" actually is using historical context rather than absolute PE thresholds. Unlike Piotroski (directional improvement), Quality measures absolute level — a 20%+ ROE business in its historical valuation trough is a structural opportunity.

---

### Extended Factors — Group A (max 5–15 pts, from base data)

---

#### 5. Northbound / Large-Order Flow (northbound)
**Measures**: Net large-order inflow over last 5 days; used as institutional money proxy

**High buy_score**:
- Large-order net inflow on 4 or 5 of the last 5 days
- Net total inflow is positive and significant

**High sell_score**:
- Large-order net outflow on all 5 days (persistent selling)
- Net outflow exceeds 500M CNY (approaching max score)

---

#### 6. Volume Breakout (volume)
**Measures**: Today's volume vs 20-day average, crossed with price direction AND MA trend direction to confirm the nature of the volume event

**High buy_score**:
- Volume ratio ≥ 1.5× + price up ≥ 1% (放量上涨 — confirmed breakout/accumulation, strong buy)
- Volume ratio ≥ 1.5× + price drop ≥ 2% + long lower shadow (high-vol drop with absorption — possible bottom)
- Volume ratio < 0.5× + price down (缩量下跌 — selling exhausted, mild buy signal)
- **Volume ratio ≥ 1.5× + MA5 > MA20 (uptrend context, buy +1.5)**: volume surge in an uptrend = institutional accumulation confirmed, not distribution

**High sell_score**:
- Volume ratio ≥ 1.5× + price drop ≥ 2% + no significant lower shadow (放量大阴线 — distribution, 7–9 pts)
- Volume ratio < 0.5× + price up ≥ 1% (缩量上涨 — unsustainable rally, 5 pts)
- Volume ratio > 5× (extreme climax — possible blow-off top, 6 pts)
- **Volume ratio ≥ 1.5× + MA5 < MA20 (downtrend context, sell +1.5)**: volume surge in a downtrend = distribution/panic selling amplified

**52-week price position cross (contextual diagnosis of the volume event)**:
- **Volume ratio ≥ 1.5× + price up ≥ 1% + 52w position < 30% (base breakout, buy +2)**: high-volume surge at annual lows = classic institutional accumulation launch signal; distribution phase completed, mark-up phase beginning — the most credible volume-price configuration in A-share bottoming analysis; likely institutional entry, not retail FOMO
- **Volume ratio ≥ 1.5× + price up ≥ 1% + 52w position > 70% (possible distribution top, sell +1.5)**: high-volume surge at annual highs is often institutional distribution into retail buying — identical surface pattern to the base breakout but opposite in nature; smart money exiting on elevated price and elevated volume
- **Volume ratio ≥ 1.5× + price down ≥ 2% + 52w position < 30% (panic capitulation, sell urgency reduced, sell −1.5)**: high-volume selloff at annual lows is typically emotional retail panic rather than institutional distribution — the same pattern that would be a strong sell signal at highs is instead a sentiment exhaustion signal at lows; reduce sell urgency

**Key principle**: Volume × price direction is the base signal. Volume × trend direction (MA) is the confirmation layer. Volume × 52-week price position is the contextual diagnosis layer. All three combined answer the full question: "what does this volume event actually mean?" — the same high-volume selloff means active distribution near annual highs and panic capitulation near annual lows; surface appearance identical, interpretation opposite.

---

#### 7. 52-Week Position (position_52w)
**Measures**: Current price position within 52-week high/low range (0% = annual low, 100% = annual high)

**High buy_score**:
- Position > 80% (strong uptrend, making new highs)

**High sell_score** (weak signal, max 3 pts):
- Position > 95% (within 5% of 52-week high — mild caution only)

**Note**: Primarily a trend confirmation factor; not recommended as a standalone sell trigger.

---

#### 8. Dividend Yield (div_yield)
**Measures**: TTM dividend yield; crossed with financial sustainability (ROE + debt) AND earnings trend direction to provide two-layer yield trap detection

**High buy_score**:
- Yield ≥ 5% (high income)
- Yield ≥ 3% (moderate attractiveness)
- **Yield ≥ 2% + ROE ≥ 12% + debt ≤ 60% (sustainable yield, buy +1.5)**: strong earnings + manageable leverage = dividend is well-supported; genuine cash cow
- **Yield ≥ 4% + profit growth > 5% (growing dividend, buy +1.5)**: not only high yield but growing earnings mean the absolute dividend payment will increase over time — the best possible dividend stock configuration
- **Zero dividend + ROE ≥ 20% (high-ROE compounder, buy +1, removes no-dividend penalty)**: choosing not to pay dividends while compounding capital at 20%+ ROE is value-creating, not a red flag (Buffett effect)

**High sell_score**:
- Zero dividend / dividend cut (mildly negative, 2 pts; waived for high-ROE compounders)
- **Yield ≥ 2% + ROE < 5% or debt > 70% (dividend trap risk, sell +3)**: high yield from a low-return or over-leveraged business is unsustainable — the dividend will either be cut or erode equity
- **Yield ≥ 4% + profit growth < −20% (yield about to be cut, sell +2)**: collapsing earnings while yield looks attractive = the textbook yield trap; the current high yield is a false signal that will soon reverse when the dividend is reduced or cancelled

**Key principle**: Dividend = earnings × payout ratio. Financial health (ROE + debt) is the static sustainability check; earnings trend is the dynamic check. A high yield paired with collapsing earnings is the most dangerous value trap — the yield percentage rises precisely because the stock is falling, and will then disappear when the dividend is cut.

**Note**: Dividend culture is weaker in A-shares; this factor carries higher weight in high-dividend strategies.

---

#### 9. Volume Ratio / 量比 (volume_ratio)
**Measures**: Today's volume vs 5-day average, crossed with price direction

**High buy_score**:
- Volume ratio 2.5–4× + price up (放量上涨 — buy score boosted 20%)
- Volume ratio < 0.8× + price down (缩量下跌 — selling exhausted, mild buy boost)

**Buy score reduced**:
- Volume ratio ≥ 1.5× + price drop ≥ 2% (放量下跌 — buy score reduced to 30%)

**High sell_score**:
- Volume ratio ≥ 1.5× + price drop ≥ 2% (放量下跌 — distribution signal, 7 pts)
- Volume ratio < 0.8× + price up ≥ 1% (缩量上涨 — unsustainable rally, 5 pts)
- Volume ratio > 8× (panic/climax distribution, 7 pts)
- Volume ratio > 5× (mild climax warning, 4 pts)

**Key principle**: Volume alone is not a signal — direction of volume is what matters.

---

#### 10. Moving Average Alignment (ma_alignment)
**Measures**: Whether price > MA5, MA5 > MA10, MA10 > MA20, MA20 > MA60 (bull alignment), crossed with 5-day vs 20-day volume ratio AND analyst earnings revision direction

**High buy_score**:
- All 4 conditions met (perfect bull alignment, 15 pts)
- 3 of 4 conditions met (mostly bullish, 11 pts)
- **Perfect bull alignment + vol_5d/vol_20d > 1.15 (price trend confirmed by real buying, buy +2)**: volume expansion means genuine participation — not a hollow rally
- **Perfect bull alignment + net analyst upgrades ≥ 2 (technical + fundamental double confirmation, buy +2)**: MA bull means prices are rising; analyst upgrades mean earnings are improving — two completely independent signals, both bullish

**High sell_score**:
- All 4 conditions inverted (full bearish alignment, 13 pts)
- Only 1 of 4 conditions met (mostly bearish, 9 pts)
- 2 of 4 met with price below MA5 (mixed-bearish, 5 pts)
- **Perfect bull alignment + vol_5d/vol_20d < 0.75 (price up but volume shrinking = trend on empty, sell +3)**: rising price with no volume is the most classic warning sign before a reversal
- **Full bearish alignment + vol_5d/vol_20d > 1.15 (distribution accelerating, sell +2)**
- **Full bearish alignment + net analyst downgrades ≤ −2 (technical + fundamental double confirmation of deterioration, sell +2)**: both the price trend and the sell-side are pointing down

**sell_score reduced**:
- **Full bearish alignment + vol_5d/vol_20d < 0.75 (sellers running out, possible bottom, sell −2)**

**Key principle**: MA alignment = technical direction. Volume = technical momentum. Analyst revisions = fundamental direction. Three non-overlapping information sources — when all three align, it's the highest-confidence signal in the entire system.

---

#### 11. Low Volatility (low_volatility)
**Measures**: Annualized return volatility over last 60 trading days, crossed with MA5/MA20 trend direction AND market regime to reveal regime-dependent defensive alpha

**High buy_score**:
- Annualized vol ≤ 15% (very stable)
- Annualized vol ≤ 25% (low volatility)
- **Low vol (≤ 25%) + MA5 > MA20 (uptrend) → buy +2 ("quiet strength")**: price grinding up without retail frenzy — the most sustainable rally pattern, typically reflects institutional accumulation
- **Low vol (≤ 25%) + bear market (regime ≤ 3) → buy +2 (defensive premium)**: Ang et al. academic research confirms low-vol stocks significantly outperform in bear markets; the defensive premium is at maximum precisely when market risk is highest

**High sell_score**:
- Annualized vol > 80% (extreme volatility spike, 8 pts)
- Annualized vol > 60% (high volatility warning, 5 pts)
- **Low vol (≤ 25%) + MA5 < MA20 (downtrend) → sell +2 ("quiet decay")**: no panic but nobody buying either — price drifting lower with no visible alarm bells
- **High vol (> 45%) + bear market (regime ≤ 3) → sell +1.5**: high-beta in a down market amplifies losses; worst combination for capital preservation

**buy_score reduced**:
- **Low vol (≤ 25%) + bull market (regime ≥ 7) → buy −1.5, sell +1**: in risk-on bull markets, momentum and high-beta dominate; holding low-vol stocks is an explicit opportunity cost equivalent to foregoing market beta

**Key principle**: Low vol × trend direction reveals signal quality within a single stock; low vol × market regime reveals the *timing* of when this factor's alpha is highest. Per Ang et al., the vast majority of low-vol factor alpha is generated in bear markets. Owning low-vol in a bull market is expensive insurance; owning it in a bear market is exactly what the factor was built for.

---

### Extended Factors — Group A continued (factors_extended.py)

---

#### 12. Short-Term Reversal (reversal)
**Measures**: 1-month price return exploiting retail overreaction, crossed with 52-week price position AND recent volume trend (two-layer context cross)

**High buy_score** (oversold bounce candidates):
- 1-month decline ≥ 15% (strong oversold, 10 pts)
- 1-month decline 0–15% (linear 5–10 pts)
- **1-month decline ≥ 10% + 52w position < 30% (bottom panic, buy +2)**: retail capitulating at lows while smart money absorbs — the textbook reversal setup
- **1-month decline ≥ 10% + vol_5d/vol_10d < 0.80 (volume drying up on the way down, buy +2)**: sellers running out of shares to sell — the classic "底部量缩" bottom formation pattern

**High sell_score** (overbought reversal risk / trend-start warning):
- 1-month gain ≥ 20% (strong overbought, 9 pts)
- 1-month gain 10–20% (linear 5–9 pts)
- 1-month gain 5–10% (mild caution, 2 pts)
- **1-month decline ≥ 10% + 52w position > 70% (high-position drop, sell +3)**: this is a trend reversal beginning, NOT a dip to buy — the first month of institutional distribution

**buy_score reduced**:
- **1-month decline ≥ 10% + 52w position > 70% → buy −4**: a 10% pullback from a 52w high is NOT oversold — do not treat it as a reversal opportunity
- **1-month decline ≥ 10% + vol_5d/vol_10d > 1.20 (volume still elevated, buy −2)**: panic is still ongoing — the bottom has not formed yet, buying here is catching a falling knife

**sell_score reduced**:
- **1-month gain ≥ 10% + 52w position < 30% → sell −2**: possible base breakout — rally from lows has different implications than overbought at highs

**Earnings revision cross (genuine reversal vs. dead-cat bounce)**:
- **Oversold (1m ≤ −10%) + net upgrades ≥ 2 (buy +2, genuine reversal)**: price oversold AND analysts improving their outlook simultaneously = the rebound has fundamental backing, not just technical exhaustion
- **Oversold + net downgrades ≤ −2 (buy −1.5, sell +1.5, dead-cat risk)**: price down but fundamentals still deteriorating — the classic "falling knife" pattern; avoid
- **Overbought (1m ≥ +10%) + net upgrades ≥ 2 (sell −1.5)**: rally justified by improving fundamentals — ease overbought sell signal
- **Overbought + net downgrades ≤ −2 (sell +1.5)**: rally has no fundamental backing — amplify the reversal sell signal

**Decline magnitude cross (mean-reversion probability rises nonlinearly with depth of decline)**:
- **1-month decline > 25% (extreme capitulation, buy +1.5, sell −2)**: a 25%+ single-month decline is statistically rare and typically reflects extreme retail panic overshooting fundamental value; mean-reversion probability at this depth is significantly elevated; simultaneously, selling something that has already dropped 25% is near-zero-value — sharply reduce sell urgency
- **1-month decline > 20% (deep oversold, buy +0.5, sell −1)**: below the extreme threshold but still deeply oversold; mean-reversion tendency increases and short-side value diminishes

**Key principle**: Reversal tells you "how much has it fallen"; volume cross tells you "has the falling stopped"; earnings revision cross tells you "are the fundamentals turning too?" The magnitude cross adds a fourth layer: "is the fall so deep that mean-reversion probability is statistically elevated?" All four combined form the most complete reversal signal in the system. The reversal factor's main weakness is treating fundamentally broken companies the same as genuinely oversold quality stocks — the quality cross and earnings revision cross address that flaw; the magnitude cross addresses the complementary question of statistical overshooting.

**Background**: Reversal IC ~0.070 in A-shares — one of the strongest signals. Default weight 2.0.

---

#### 13. Accruals (accruals)
**Measures**: Accrual ratio = (net income − operating cash flow) / total assets, crossed with profit growth rate to distinguish "genuine cash-backed growth" from "accounting-inflated growth stories"

**High buy_score** (high earnings quality):
- Accrual ratio ≤ −5% (earnings well-backed by cash)
- **Accrual ratio ≤ −5% + profit growth ≥ 20% (high growth fully backed by cash — best-quality growth, buy +2)**

**High sell_score** (low earnings quality):
- Accrual ratio ≥ 10% (profit far exceeds cash flow — possible earnings inflation)
- Ratio 0–10%: linear 2–9 pts
- **Accrual ratio ≥ 5% + profit growth ≥ 20% (high growth not backed by cash — earnings quality mismatch, sell +2)**
- **Accrual ratio ≥ 5% + profit growth < 0% (declining profit propped up by accruals — double quality warning, sell +1)**

**Key principle**: The accruals × profit growth cross identifies whether a growth story is real — high growth alongside high accruals suggests earnings are being front-loaded or inflated and are unlikely to persist; conversely, high growth with negative accruals (cash exceeds reported income) is genuine cash-driven quality growth.

---

#### 14. Asset Growth (asset_growth)
**Measures**: YoY total asset growth rate; over-expansion penalty

**High buy_score**:
- Asset growth ≤ 5% (disciplined capital allocation)

**High sell_score**:
- Asset growth ≥ 50% (extreme over-expansion, 8 pts)
- Asset growth 30–50% (aggressive expansion, 5 pts)

**Note**: A-share investors reward growth so the buy weight is set to 0 by default, but the sell signal for extreme expansion remains active.

---

#### 15. Piotroski F-Score (piotroski)
**Measures**: 9 binary fundamental signals covering profitability, leverage, and efficiency, crossed with 52-week price position

**High buy_score**:
- F-score ≥ 7 (strong fundamentals, max 9)
- **F-score ≥ 7 + 52w position < 30% (strong fundamentals at beaten-down price = quality-at-value, buy +2)**: the textbook long-term accumulation setup — solid company trading at a discount because the market has been unkind to the sector or short-term results
- **F-score ≥ 7 + cheap valuation (PE percentile ≤ 30% or PB percentile ≤ 30%) (data-driven GARP, buy +1.5)**: multiple fundamental metrics improving simultaneously AND historically cheap — the most empirically grounded fundamental buy configuration

**High sell_score**:
- F-score ≤ 2 (very weak fundamentals, 8 pts)
- F-score 3–4 (below-average, 4 pts)
- **F-score ≤ 2 + 52w position > 70% (weak fundamentals near highs = value trap risk, sell +2)**: a story priced to perfection with no fundamental substance to support it — any deterioration can trigger a sharp re-rating
- **F-score ≤ 3 + high valuation (PE/PB percentile ≥ 80%) (deteriorating financials at premium, sell +1.5)**: multiple fundamental signals deteriorating simultaneously while the stock remains expensively priced — "priced to perfection with cracks showing", the most dangerous sell configuration

**9 signals**: ROA > 0, CFO > 0, improving ROA, CFO > net income (4 profitability); declining debt, improving current ratio, no dilution (3 leverage); improving gross margin, improving asset turnover (2 efficiency)

---

#### 16. Short Interest (short_interest)
**Measures**: Short balance / circulating market cap, crossed with 52-week price position AND analyst earnings revision direction

**High buy_score**:
- Short ratio ≤ 0.5% (minimal short pressure)
- **Short ratio ≥ 3% + 52w position < 30% (high short + beaten-down stock = short squeeze potential, buy +3)**: Shorts built at high levels are now underwater; any reversal forces covering
- **Short ratio ≥ 3% + net analyst upgrades ≥ 2 (squeeze catalyst, buy +2)**: Heavy short position + analyst upgrades = squeeze accelerant; shorts face both price and fundamental pressure

**High sell_score**:
- Short ratio ≥ 5% (heavily shorted, 9 pts)
- Short ratio 3–5% (linear 5–9 pts)
- **Short ratio ≥ 3% + 52w position > 70% (heavy short at high price = shorts likely correct, sell +2)**
- **Short ratio ≥ 3% + net analyst downgrades ≤ −2 (shorts confirmed by fundamentals, sell +2)**: Both shorts and analysts are bearish — high-conviction sell setup

**Key principle**: High short interest at the bottom is a squeeze opportunity; at the top, it confirms bearish consensus. Analyst revision direction provides a fundamental catalyst layer that determines whether the squeeze fires or the shorts are validated.

---

#### 17. RSI Signal (rsi_signal)
**Measures**: 14-day RSI overbought/oversold zones, crossed with MA5/MA20 trend direction AND recent volume trend (two-layer cross)

**High buy_score**:
- RSI ≤ 30 (oversold, 10 pts)
- RSI 30–50 (recovering, linear 5–10 pts)
- **RSI ≤ 30 + MA5 > MA20 (oversold dip in uptrend — ideal buy opportunity)**
- **RSI ≤ 30 + vol_5d/vol_10d < 0.80 (oversold + volume drying up = selling exhausted, confirmed reversal setup, buy +2)**

**High sell_score**:
- RSI ≥ 80 (extreme overbought, 9 pts)
- RSI 70–80 (linear 5–9 pts)
- RSI 60–70 (mild caution, 2 pts)
- **RSI ≥ 70 + MA5 < MA20 (overbought dead-cat bounce in downtrend, sell +2)**
- **RSI ≥ 70 + vol_5d/vol_10d < 0.80 (overbought + buyers leaving = momentum fading, sell +2)**

**sell_score reduced**:
- **RSI ≥ 70 + MA5 > MA20 (overbought but in confirmed uptrend — may continue, sell −2)**
- **RSI ≥ 70 + vol_5d/vol_10d > 1.30 (overbought but volume still expanding = momentum intact, sell −1)**

**buy_score reduced**:
- **RSI ≤ 30 + MA5 < MA20 (oversold in downtrend — falling knife risk, buy −3)**

**Key principle**: MA cross tells you the trend direction; volume cross tells you whether the extreme RSI reading is confirmed or fading. Both dimensions together produce the highest-conviction signals.

---

#### 18. MACD Signal (macd_signal)
**Measures**: MACD histogram direction and momentum, crossed with 52-week price position to differentiate base breakouts from late-stage rallies

**High buy_score**:
- Histogram positive and expanding (bullish acceleration, 8–10 pts)
- Histogram negative but narrowing (base-forming recovery, 4 pts)
- **Histogram turning positive + expanding (golden cross) + 52w position < 30% (base breakout, buy +2)**: the most reliable trend-start signal in technical analysis

**High sell_score**:
- Histogram negative and expanding (bearish deepening, 7–9 pts)
- Histogram positive but shrinking rapidly (top deceleration, 3–5 pts)
- **Bearish deepening + 52w position > 70% (confirmed distribution top, sell +2)**: high-position dead cross = classic topping signal
- **Golden cross + 52w position > 70% (late-stage rally, sell +2)**: a golden cross near 52w highs may signal a final push before reversal

**sell_score reduced**:
- **Bearish deepening + 52w position < 30% (bottom testing, sell −2)**: sell pressure at lows is limited; reversal may be close

**Volume confirmation cross** (20-day / 60-day volume ratio):
- **Golden cross (histogram turning positive + expanding) + vol ratio > 1.3× (volume confirmed, buy +1.5)**: bullish signal validated by expanding participation — both direction and liquidity align; the most reliable trend-start configuration
- **Golden cross + vol ratio < 0.8× (low-volume golden cross, buy −1)**: technical indicator turns bullish but volume is contracting — signal is questionable, possible false breakout
- **Dead cross (histogram turning negative + expanding) + vol ratio > 1.3× (breakdown confirmed, sell +1.5)**: bearish signal with expanding volume participation — confirms acceleration rather than an oversold bounce

**Key principle**: MACD quantifies trend inertia, but inertia requires fuel (volume) to sustain. Golden cross + expanding volume = direction and liquidity both confirmed, the highest-conviction trend-start configuration. Golden cross + contracting volume = signal quality uncertain. Dead cross + expanding volume = accelerating breakdown, not an oversold setup.

---

#### 19. Turnover Percentile (turnover_percentile)
**Measures**: 5-day avg turnover rate vs 90-day average, crossed with price direction

**High buy_score**:
- Ratio ≥ 3× + price up ≥ 1% (strong accumulation confirmed, 10 pts)
- Ratio 1.5–3× + price up ≥ 0.5% (active accumulation zone, 8–10 pts)
- Ratio 0.8–1× + price down (缩量下跌 — selling exhausted, 5.5 pts)

**Buy score reduced**:
- Ratio ≥ 4× + price drop ≥ 2% (climax selloff — distribution, 3 pts)
- Ratio ≥ 3× + price drop ≥ 2% (elevated turnover + decline, 4 pts)
- Ratio < 0.8× + price up ≥ 0.5% (缩量上涨 — unsustainable, 1.5 pts)

**High sell_score**:
- Ratio ≥ 3× + price drop ≥ 2% (high turnover + big drop — distribution, 9 pts)
- Ratio ≥ 1.5× + price drop ≥ 2% (active turnover declining, 6 pts)
- Ratio < 0.8× + price up ≥ 1% (缩量上涨 — unsustainable rally, 5 pts)
- Ratio ≥ 4× (climax without price context, 7 pts)
- Ratio ≥ 3× (elevated turnover, 4 pts)

**1-month return cross** (adds 20-day trend context to filter single-day noise):
- **Ratio < 0.7× + 1m return +3% to +15% (quiet accumulation, buy +1.5)**: price rising steadily while volume shrinks — retail selling has been absorbed, slow institutional accumulation underway; the combination of rising price on declining volume is the cleanest structural accumulation pattern
- **Ratio ≥ 2.0× + 1m return ≤ −5% (active distribution, sell +1.5)**: high volume sustained alongside a month-long decline — this is not a single-day selloff but systematic position reduction by large holders

**Key principle**: Volume alone is not a signal — direction of volume is what matters. The 1-month return cross extends this logic from a single-day lens to a trend dimension, separating noise from structural accumulation/distribution.

---

#### 20. Chip Distribution / 筹码分布 (chip_distribution)
**Measures**: Cross-interaction of 52-week price position × large-order vs small-order flow composition

**High buy_score**:
- **Retail panic bottom**: Position < 30%, small-order outflow >> large-order outflow (retail capitulating, smart money holding) → 9–10 pts
- **Bottom accumulation**: Position < 30%, both sides net buying → 8 pts

**High sell_score**:
- **Institutional top distribution**: Position > 70%, large-order outflow >> small-order outflow (institutions exiting) → 7–10 pts
- **Institutional bottom exit**: Position < 30%, large-order outflow >> small-order (institutions selling even at lows — dangerous) → 7–10 pts

**Neutral scenarios**:
- High position + retail selling + large-order buying (ambiguous — possible wash trading / 对倒) → 5 buy / 4 sell
- Mid-range position → 5 buy / 2 sell

---

### Extended Factors — Group B (max 10–15 pts, require additional API calls)

---

#### 21. Shareholder Count Change (shareholder_change)
**Measures**: Quarterly change in shareholder count; measures chip concentration, crossed with 52-week price position

**High buy_score**:
- Shareholder count down ≥ 10% (strong concentration, 15 pts)
- Count declining (linear 8–15 pts)
- **Count down ≥ 5% + 52w position < 30% (concentration at low price = smart money quietly accumulating, buy +3)**: the classic A-share accumulation pattern — low price, fewer shareholders, institutions locking in position before reversal

**High sell_score**:
- Shareholder count up ≥ 20% (severe dispersion — large-scale distribution, 12 pts)
- Count up 10–20% (linear 7–12 pts)
- **Count up ≥ 10% + 52w position > 70% (dispersion at high price = institutions distributing to retail, sell +2)**: the classic top distribution pattern

**sell_score reduced**:
- **Count up ≥ 10% + 52w position < 30% (dispersion at low price = new buyers entering at depressed prices, sell −2)**: low-price dispersion is often bottom-fishing by new investors, not institutional exit

**Earnings revision cross (dual institutional confirmation)**:
- **Count down ≥ 5% + net analyst upgrades ≥ 2 (buy +2)**: chip concentration (smart money locking in) + analyst upgrades (sell-side turning bullish) = two completely independent institutional perspectives aligning on the same direction; the probability of a false signal drops substantially when two non-overlapping information sets agree
- **Count up ≥ 10% + net analyst downgrades ≤ −2 (sell +2)**: dispersion (smart money exiting) + analyst cuts (sell-side recognizing deterioration) = dual institutional retreat; one of the strongest combined sell signals in the system

**Background**: Chip concentration IC ~0.065 in A-shares — strong signal. Default weight 2.0.

---

#### 22. Dragon-Tiger List / 龙虎榜 (lhb)
**Measures**: Net institutional buy amount on the Dragon-Tiger list over last 90 days; crossed with 52-week price position to distinguish genuine bottom discovery from high-price distribution

**High buy_score**:
- Net institutional buy ≥ 50M CNY (10 pts)
- **Net buy ≥ 10M CNY + 52w position < 30% (institutional bottom discovery, buy +2, sell −1)**: institutions buying large blocks via LHB at 52-week lows is the most credible bottom signal — they are bearing the full information asymmetry cost of buying into weakness

**High sell_score**:
- Net institutional sell ≥ 50M CNY (9 pts)
- Net sell 10–50M CNY (linear 5–9 pts)
- **Net sell ≥ 10M CNY + 52w position > 70% (confirmed distribution at highs, sell +2)**: institutions selling large blocks via LHB near 52-week highs is direct evidence of systematic distribution — selling to retail FOMO buyers at peak prices
- **Net buy ≥ 10M CNY + 52w position > 70% (suspicious buy at highs, sell +1)**: institutional buying at highs via LHB is not reliably bullish — may be markup before exit (wash trading or coordinated distribution camouflaged as institutional interest)

**Note**: A-share research shows Dragon-Tiger appearances often mark short-term tops. Buy weight is 0 by default; the 52w position cross significantly improves signal discrimination — bottom-discovery buys are materially different from high-price buys.

---

#### 23. Lock-up Expiry Pressure (lockup_pressure)
**Measures**: Upcoming 90-day unlock value / circulating market cap, crossed with 52w price position AND earnings growth to assess both selling motivation and buying absorption capacity

**buy_score**: 2 pts when no lockup (neutral, not a genuine buy reason)

**High sell_score** (primarily a sell factor):
- Unlock ratio ≥ 20% (major supply shock, 9 pts)
- Unlock ratio 5–20% (linear 5–9 pts)
- Unlock ratio 1–5% (linear 1–5 pts)
- **Unlock ratio ≥ 5% + 52w position > 70% (insiders in profit — strong incentive to sell, sell +2)**
- **Unlock ratio ≥ 5% + profit growth < 0% (declining business, no fundamental buyers to absorb supply, sell +2)**: deteriorating earnings attract no new longs; the unlock becomes a one-sided supply shock

**sell_score reduced**:
- **Unlock ratio ≥ 5% + 52w position < 30% (insiders underwater — limited selling motivation, sell −3)**
- **Unlock ratio ≥ 5% + profit growth ≥ 20% (fast-growing business attracts buyers who absorb the supply, sell −2)**

**Social heat cross (lockup expiry meets retail FOMO)**:
- **Unlock ratio ≥ 5% + social heat top 10% (extreme retail attention amplifies the unlock, sell +2)**: large-scale unlock coinciding with peak media attention and retail FOMO = the optimal exit window for institutional holders; supply pressure amplified by a willing retail bid — the classic A-share "unlock + hot search" distribution pattern
- **Unlock ratio ≥ 5% + social heat bottom 50% (unlocking into a cold market, sell −1)**: no retail buying enthusiasm means fewer natural buyers absorbing the supply, making distribution harder — reduces (but does not eliminate) the sell urgency; no retail bubble to exploit

**Key principle**: Unlock pressure = potential supply ÷ potential demand. High-growth companies attract buyers who offset the supply; declining companies face supply with no offsetting demand. The social heat cross upgrades the demand-side assessment: extreme heat means peak retail absorption capacity — the optimal institutional exit window; cold market means no retail bid — the supply is harder to place.

---

#### 24. Insider Transactions (insider)
**Measures**: Net major shareholder buy/sell over past 6 months; measures insider conviction, crossed with 52-week price position

**High buy_score**:
- Net buy ratio > 50% (significant insider accumulation, 8–10 pts)
- **Net buy ratio > 30% + 52w position < 30% (insider buying at depressed prices = highest-conviction signal, buy +2)**: insiders committing capital at low prices is the clearest expression of insider confidence

**High sell_score**:
- Net sell ratio > 50% (significant insider distribution, 8 pts)
- Net sell ratio 0–50% (linear 0–3 pts)
- **Net sell ratio > 30% + 52w position < 30% (insider selling while underwater = RED FLAG, sell +3)**: insiders willing to take a loss to exit implies they see structural or undisclosed problems — one of the most dangerous signals in the entire system
- **Net sell ratio > 30% + 52w position > 70% (insider selling near highs = rational profit-taking confirmed, sell +2)**

**Earnings revision cross (insider + analyst dual conviction)**:
- **Net buy ratio > 30% + net analyst upgrades ≥ 2 (buy +2, dual conviction signal)**: insiders committing capital AND independent sell-side analysts simultaneously raising ratings — two completely independent information-advantaged groups both bullish; insider information + external consensus analysis converging is one of the strongest long signals in the entire system
- **Net sell ratio > 30% + net analyst downgrades ≤ −2 (sell +2, dual exit signal)**: both insiders and analysts bearish simultaneously — two independent groups giving the same negative signal produces high-conviction sell/reduce confirmation
- **Net buy ratio > 30% + net analyst downgrades ≤ −2 (sell −1, insider conviction overrides sell-side)**: insiders accumulating despite widespread analyst cuts — management knows things analysts do not; the real-money action of insiders carries more informational weight than sell-side notes; reduce sell urgency

**Key principle**: The same transaction means completely different things depending on price. Insider buying at lows = conviction bet. Insider selling at lows = structural alarm. These are opposite signals requiring position context to interpret correctly. The earnings revision cross adds a second dimension: insider + analyst convergence produces the system's strongest buy/sell signals; insider vs analyst divergence (buying against cuts) indicates management confidence that overrides public-information consensus.

---

#### 25. Institutional Visits (institutional_visits)
**Measures**: Number of institutional research visits in past 90 days; crossed with earnings revision direction to distinguish "pre-upgrade accumulation" (visits without upgrades yet) from "consensus crystallising" (visits + upgrades together)

**High buy_score**:
- ≥ 10 visits (high institutional attention, 10 pts)
- 5–10 visits (moderate attention)
- **≥ 5 visits + net revisions = 0 (pre-upgrade accumulation signal, buy +1)**: institutions actively surveying but no analyst has yet published an upgrade — this is the typical pattern of institutions building positions before public consensus forms; the upgrade note is likely still being drafted
- **≥ 5 visits + net upgrades ≥ 2 (institutional consensus crystallising, buy +1)**: high visit frequency AND published upgrades together mean analyst conviction has been field-verified and is now being formalised; the smart money and sell-side are aligned

**High sell_score** (weak signal):
- 0 visits → 2 pts (analysts losing interest, mildly negative)

**Key principle**: Institutional visits are a leading indicator for analyst upgrades. High visits with no upgrades (net = 0) is often a more actionable signal than visits after upgrades have already been published — by the time the upgrade note is out, the institutional positioning is largely complete.

---

#### 26. Industry Momentum (industry_momentum)
**Measures**: Industry 1-month return vs CSI 300; sector rotation signal, crossed with individual stock 52-week price position to distinguish "late-mover laggards within a hot sector" from "late-cycle high entries"

**High buy_score**:
- Industry excess return ≥ 5% (sector outperforming, 10 pts)
- Excess return 0–5% (linear 5–10 pts)
- **Excess return ≥ 2% + stock 52w position < 30% (sector rotating up but this stock still lagging — late-mover opportunity, buy +2)**

**High sell_score**:
- Industry excess return ≤ −5% (sector badly underperforming market, 9 pts)
- Excess return −5–0% (linear 3–9 pts)
- **Excess return ≤ −2% + stock 52w position > 70% (sector weakening but stock still at highs — mean-reversion catch-down risk, sell +2)**
- **Excess return ≥ 2% + stock 52w position > 70% (sector strong but stock already at highs — late entry into a crowded trade, sell +1)**

**sell_score reduced**:
- **Excess return ≤ −2% + stock 52w position < 30% (sector weak but stock already low — damage largely absorbed, sell −1)**

**Key principle**: Industry momentum × stock position captures the "time-lag" of sector rotation — a strong sector with a lagging stock (low position) means capital flow has not yet reached this name and represents a final low-cost entry window within the rotation. Conversely, a weak sector paired with a stock still at highs is a classic catch-down setup.

---

#### 27. Northbound Actual Holdings (northbound_actual)
**Measures**: Actual 沪深港通 (Stock Connect) per-stock holding change over last 5 periods, crossed with price position to distinguish active exits from passive redemptions, and with 1-month momentum to detect contrarian accumulation

**High buy_score**:
- Holdings up ≥ 5% (strong inflow, 10 pts)
- **Holdings up ≥ 2% + 1m return ≤ −10% (foreign capital buying into a falling stock — high-conviction contrarian bottom, buy +2)**: one of the highest-information signals in the system — foreign institutions accumulating while retail panics

**High sell_score**:
- Holdings down ≥ 5% (significant outflow, 9 pts)
- Holdings down 2–5% (linear 5–9 pts)
- Holdings slightly down 0–2% (2 pts)
- **Holdings down > 2% + 52w position > 70% (active profit-taking exit at high price, sell +2)**
- **Holdings down > 2% + 1m return ≤ −10% (foreign capital exiting a declining stock — conviction-based fundamental sell, sell +1.5)**: smart money also leaving while the stock is falling confirms bear thesis

**sell_score reduced**:
- **Holdings down > 2% + 52w position < 30% (likely passive ETF redemption at low price, not a genuine bearish signal, sell −2.5)**

**Earnings revision cross (dual institutional confirmation)**:
- **Holdings up ≥ 2% + net upgrades ≥ 2 (buy +2, NB × analyst consensus)**: two completely independent institutional groups both positive simultaneously — the highest-conviction convergent institutional signal in the system
- **Holdings down ≤ −2% + net downgrades ≤ −2 (sell +2, dual institutional exit)**: foreign capital reducing and domestic analysts cutting — both institutional groups abandoning the stock is one of the strongest sell signals in the system

**Industry momentum cross (contra-sector NB = highest conviction)**:
- **Holdings up ≥ 2% + industry excess return ≤ −2% (contra-sector net inflow, buy +2)**: foreign capital buying a single stock while the entire sector is being sold — the highest-information active positioning signal in the system; negative macro backdrop while NB still accumulates implies a specific, high-conviction stock-level thesis
- **Holdings up ≥ 2% + industry excess return ≥ +5% (riding a hot sector, buy −1)**: NB flowing into a sector that is already running — likely passive weight adjustment or trend-following, not independent bullish conviction; discount the signal
- **Holdings down ≥ 2% + industry excess return ≥ +5% (active exit from hot sector, sell +1.5)**: sector strength while NB is reducing holdings is a strong signal that foreign investors see valuation or fundamental risk that the market is ignoring

**Key principle**: The true value of northbound data is distinguishing "active positioning" from "passive mechanics". The industry momentum cross adds the critical dimension: contra-sector NB inflow is the system's highest-conviction independent institutional signal. Sector-following NB inflow is largely noise.

---

#### 28. Earnings Revision (earnings_revision)
**Measures**: Net analyst rating upgrades minus downgrades; sell-side expectation momentum; crossed with 52-week price position to distinguish "contra-consensus discovery" from "price-chasing upgrades"; crossed with trailing actual profit growth to test whether upgrades are grounded in real results

**High buy_score**:
- Net upgrades ≥ 3 (strong upward revision, 10 pts)
- Net upgrades 1–3 (linear 7–10 pts)
- **Net upgrades ≥ 2 + 52w position < 30% (contra-consensus discovery, buy +2)**: analysts upgrading a beaten-down stock against the prevailing sentiment is a rare, high-alpha signal — the stock is being re-rated from underappreciated to discovered
- **Net upgrades ≥ 2 + trailing profit growth ≥ 20% (grounded upgrade, buy +1.5)**: forward-looking optimism validated by actual recent growth — forward estimate upgrades + backward-looking confirmation is the highest-conviction upgrade configuration; both the analyst and the financial statements agree

**High sell_score**:
- Net downgrades ≥ 3 (strong downward revision, 9 pts)
- Net downgrades 1–3 (linear 5–9 pts)
- **Net downgrades ≥ 2 + 52w position > 70% (confirmed top, sell +2)**: analysts cutting targets on an expensive stock confirms that the market's repricing thesis is gaining consensus
- **Net upgrades ≥ 1 + 52w position > 70% (price-chasing upgrades, sell +1)**: upgrades after a large price run are usually analysts following the move rather than leading it; the upside is already priced in
- **Net upgrades ≥ 2 + trailing profit growth < 0% (hollow upgrade, sell +1.5)**: analysts raising estimates while actual profits are declining — a characteristic pattern of relationship-driven coverage, IR-orchestrated market cap management, or systematic over-optimism; the upgrade is not supported by fundamental evidence

**Institutional visits cross (earnings revision × field research = two-layer validation)**:
- **Net upgrades ≥ 2 + institutional visits ≥ 5 in past 90 days (buy +1.5, sell-side + buy-side dual consensus)**: published analyst upgrade notes AND fund manager/buy-side field research visits — sell-side forward-looking conviction and buy-side on-the-ground verification arriving simultaneously; two completely independent institutional channels converging on the same conclusion is the highest-conviction upgrade configuration in the system
- **Net downgrades ≤ −2 + institutional visits = 0 (sell +1.5, fully abandoned by institutions)**: analysts cutting while no institutions bother to visit — sell-side negative AND buy-side absent; total institutional abandonment is the most comprehensive bearish institutional signal
- **Net upgrades ≥ 2 + institutional visits = 0 (sell +1, upgrades without field validation)**: analyst upgrade notes lacking buy-side field verification are more likely to reflect model updates, IR-driven coverage, or market-cap management than genuine fundamental conviction; signal quality is questionable without on-the-ground corroboration

**Key principle**: Analysts are followers, not leaders. Upgrades are most valuable when they arrive before the price move (stock at lows); upgrades after a large run indicate consensus formation and typically generate little further alpha. The trailing growth cross adds a quality filter: upgrades aligned with actual results carry signal; upgrades against declining earnings are noise or worse. The institutional visits cross adds a field-verification layer: sell-side upgrades × buy-side research visits = two institutional groups converging through independent channels, the strongest possible upgrade validation.

---

### Extended Factors — Group C (max 10 pts, behavioral & market-context factors)

---

#### 29. Limit Hits / 涨跌停板 (limit_hits)
**Measures**: Net limit-up count minus limit-down count over last 20 trading days; crossed with 52-week price position AND fundamental quality (ROE)

**High buy_score**:
- Net limit-ups ≥ 3 (frequent limit-ups, strong momentum, 9 pts)
- Net limit-ups ≥ 1 (net positive, 7 pts)
- No limit events (neutral, 5 pts)
- **Net limit-ups ≥ 2 + 52w position < 30% (limit-up breakout from base = genuine momentum, buy +1)**: limit-ups from a depressed base are the most reliable trend-start signal
- **Net limit-downs ≥ 2 + 52w position < 30% (panic capitulation at lows = reversal signal, buy +3)**: extreme retail panic at bottoms is often the ideal institutional accumulation window
- **Net limit-ups ≥ 2 + ROE ≥ 12% (earnings-backed acceleration, buy +1.5)**: consecutive limit-ups supported by real profitability = sustainable momentum, not just hot money

**High sell_score**:
- Net limit-downs ≥ 3 (frequent limit-downs, 9 pts)
- Net limit-downs ≥ 1 (net negative, 6 pts)
- **Net limit-ups ≥ 2 + 52w position > 70% (limit-up frenzy at highs = retail euphoria top, sell +3)**: frequent limit-ups near highs signal extreme overheating and imminent reversal
- **Net limit-downs ≥ 2 + 52w position > 70% (distribution selloff from highs, sell +2)**
- **Net limit-ups ≥ 2 + ROE < 5% (pure hot money speculation, sell +2)**: consecutive limit-ups with near-zero earnings = classic A-share hot-money pump — high collapse risk

**Social heat cross (limit-ups × sentiment = pump detection)**:
- **Net limit-ups ≥ 2 + social heat top 5% (extreme heat, sell +2)**: consecutive limit-ups combined with peak retail media attention is the classic A-share "pump top trio" — limit-ups, trending on social media, retail FOMO all aligned; smart money is usually distributing at exactly this point
- **Net limit-ups ≥ 2 + social heat below top 50% (low-heat limit-ups, buy +1.5)**: consecutive limit-ups with near-zero public attention signals institutional-driven accumulation not yet discovered by retail; a more sustainable momentum pattern than high-heat limit-ups
- **Net limit-downs ≥ 2 + social heat top 10% + 52w position < 30% (panic at lows with extreme attention, sell −1)**: low base + extreme retail panic (both limit-downs and trending) = short-term capitulation may be near; soften the contrarian sell impulse

**Key principle**: Limit hits × position distinguishes "base breakout" from "euphoric top". Limit hits × ROE distinguishes "earnings-driven" from "hot-money speculation". Limit hits × social heat distinguishes "institutional-driven" (sustainable) from "retail frenzy-driven" (collapse risk). The three-way cross is the most complete pump-detection filter in the system.

---

#### 30. Price Inertia / 涨跌惯性 (price_inertia)
**Measures**: Consecutive up/down day streak; crossed with volume trend to confirm continuation vs. exhaustion

**High buy_score**:
- Consecutive up days ≥ 4 (strong up streak, 8 pts)
- Consecutive up days ≥ 3 (up streak, 7 pts)
- Consecutive up days ≥ 2 (mild momentum, 6 pts)
- **Up streak ≥ 3 days + volume expanding (price-volume confirmed continuation, buy +2, cap 10 pts)**
- **Down streak ≥ 3 days + volume contracting (selling exhausted, potential bounce, buy +2)**

**High sell_score**:
- Consecutive down days ≥ 4 (strong down streak, 7 pts)
- Consecutive down days ≥ 3 (down streak, 5 pts)
- Consecutive down days ≥ 2 (mild weakness, 3 pts)
- **Up streak ≥ 3 days + volume contracting (rising on no participation — unsustainable, sell +3)**
- **Down streak ≥ 3 days + volume expanding (accelerating sell with expanding participation, sell +2)**

**Annualized volatility cross** (60-day daily-return std × √252):
- **Up streak ≥ 3 days + annualized vol ≤ 25% (low-vol inertia, buy +2)**: steady appreciation with very low realized volatility = institutional capital driving a quiet, persistent trend (confirmed by Ang et al. — low-volatility alpha is real, especially in trending environments); A-share retail-driven moves typically come with high volatility, so low-vol inertia is a higher-quality signal
- **Up streak ≥ 3 days + annualized vol > 50% (high-vol inertia, sell +1.5)**: upward momentum with violent daily swings has high mean-reversion probability historically; chasing this setup is risky
- **Down streak ≥ 3 days + annualized vol ≤ 25% (quiet structural decline, sell +1)**: slow, low-noise deterioration — no panic, no volume spike, just orderly exit; this type of decline is structurally more durable than sharp drops and harder to call a bottom on

**Key principle**: Direction is the trend; volume is the fuel; volatility is the quality of the trend. Low-vol inertia = institutional-driven persistent trend. High-vol inertia = retail/hot-money overheating with mean-reversion risk. Low-vol decline = silent structural exit, do not dismiss as temporary noise.

---

#### 31. Social Heat / 社交热度 (social_heat)
**Measures**: East Money hot stock ranking percentile as a retail sentiment proxy. **Contrarian indicator** — extreme attention = sell signal; crossed with 52-week price position to distinguish "peak euphoria at highs" from "retail FOMO at lows"

**High buy_score** (moderate attention):
- Top 20% of hot list (7 pts): stock is in the spotlight of media and institutional attention — healthy visibility
- Top 50% of hot list (5 pts): moderate attention, neutral-to-positive
- Below top 50% (3 pts): low attention — potential overlooked value

**High sell_score** (extreme attention):
- Top 1% of hot list (8 pts): **extreme retail heat = strong sell signal** — when everyone is talking about a stock, institutions are often quietly distributing
- Top 5% of hot list (5 pts): very high attention, exercise caution
- **Extreme heat (top 5%) + 52w position > 70% (peak frenzy at highs, sell +2)**: stock already at a high price AND generating maximum retail attention = classic "last buyer" setup — smart money has already distributed

**sell_score reduced**:
- **Extreme heat (top 5%) + 52w position < 30% (retail FOMO from low base, sell −2)**: retail piling into a beaten-down stock; A-share short squeezes driven by retail sentiment from low positions are a real phenomenon — the pure contrarian short is riskier here

**ROE quality cross (heat × fundamentals = sentiment-type classification)**:
- **Heat top 5% + ROE ≥ 15% (high heat + high quality, sell −1.5, buy +1)**: extreme attention combined with genuine profitability suggests the market is "discovering" a quality company rather than blindly speculating; a rare configuration where high heat does not warrant the usual contrarian sell — reduce sell signal and mildly reinforce buy
- **Heat top 5% + ROE < 5% (high heat + poor quality, sell +2)**: extreme attention with near-zero earnings = the purest form of speculative frenzy — retail is pricing imagination rather than profits; maximum collapse risk
- **Heat top 20% + ROE ≥ 15% (moderate heat + high quality, buy +1)**: a quality compounder beginning to attract attention but not yet at bubble levels; a participation window rather than an exit signal

**Key principle**: Social heat measures the intensity of retail sentiment. The ROE cross reveals the quality of the heat. High heat + high ROE = institutional discovery of a quality company, sentiment and fundamentals in resonance. High heat + low ROE = pure speculative mania, the most dangerous combination. The cross upgrades the question from "how much attention?" to "why is the attention there?" — a fundamentally more informative question.

---

#### 32. Market Regime / 市场环境 (market_regime)
**Measures**: CSI 300 index MA5/MA20/MA60 bull/bear alignment; captures systematic market risk

**High buy_score** (bull market context):
- MA5 > MA20 > MA60 (full bull alignment, 9 pts): strongest bull signal — all individual stock buy signals are more reliable
- MA5 > MA20 and MA20 ≈ MA60 (recovering, 8 pts): trend turning up, broader bull alignment forming
- Current price > MA20 (6 pts): neutral-to-positive short/medium-term market

**High sell_score** (bear market context):
- Current price < MA60 (bear market, 9 pts): **systemic bear market** — even strong fundamentals are overwhelmed by macro selling; all buy signals should be discounted
- Price near but above MA60 (7 pts): bear risk elevated, tighten stops
- Price > MA60 but < MA20 (5 pts): market in correction, proceed carefully

**Key principle**: Market regime is the "background color" for all individual stock signals. In bull markets even weak stocks rise; in bear markets even strong stocks struggle. This is the only factor measuring purely systematic risk rather than stock-specific signals.

**Market regime weight modulation**: Beyond contributing as a standalone factor, market_regime dynamically adjusts the weights of other factors:
- **Bull market (buy_score ≥ 7)**: momentum ×1.3, MA-alignment ×1.2, reversal ×0.6 (fighting the bull trend is costly); sell-side eases position and valuation sensitivity
- **Bear market (buy_score ≤ 3)**: momentum ×0.4 (most breakouts fail in bear markets), reversal ×1.6 (oversold bounces are frequent), value ×1.3 (cheap stocks provide downside buffer); sell-side amplifies momentum and position sell signals

---

#### 33. Concept Momentum (concept_momentum)
**Measures**: 1-month return of A-share concept/theme boards that the stock belongs to, capturing theme-driven sector co-movement; crossed with the stock's own return vs. its hottest concept to detect catch-up opportunities and dragon-head overheating

**High buy_score** (hot concept driving):
- Best concept 1m return ≥ +15% (9 pts): concept highly active, strong theme momentum
- Best concept 1m return +8%–+15% (7 pts): concept strengthening, sector co-movement
- Best concept 1m return +3%–+8% (5.5 pts): concept mildly rising
- **Best concept ≥ +8% + stock lags concept by ≥ 15% (buy +2, catch-up candidate)**: other members in the hot sector already moved; this stock hasn't caught up yet — the classic A-share rotational catch-up opportunity

**High sell_score** (concept collapse or stock overheated):
- Worst concept 1m return ≤ −15% (8 pts): concept collapse, sector re-rating pressure
- Worst concept 1m return −8% to −15% (5 pts): concept weakening, theme fading
- **Best concept ≥ +8% + stock leads concept by ≥ 20% (sell +2, dragon-head fade risk)**: the stock has massively outrun its sector average — after the "dragon-head" premium fades, catch-down selling is typical

**Market regime cross (concept rally sustainability)**:
- **Hot concept (best return ≥ +10%) + bear market (regime ≤ 3) → buy −2, sell +1.5**: bear market concept pumps are almost exclusively driven by short-term traders with a 3–5 day window; holding for trend continuation in a bear market is a high-risk mistake
- **Hot concept + bull market (regime ≥ 7) → buy +1**: institutional participation in bull-market concept rallies produces genuine follow-through; the signal is more reliable

**ROE quality cross (what kind of concept rally is this?)**:
- **Hot concept (best return ≥ +8%) + ROE ≥ 15% (quality company in hot sector, buy +1.5)**: a fundamentally strong business being lifted by thematic flows — the rally has a fundamental anchor, not just narrative; the most sustainable form of concept-driven appreciation
- **Hot concept + ROE < 5% (speculative play, sell +2)**: a near-zero-earnings company surging inside a hot concept sector = pure narrative without profit foundation; the highest collapse risk profile in theme investing

**Key principle**: A-share concept boards are a core driver of capital rotation. When a concept ignites, laggard stocks within the same concept offer the best risk-adjusted entry. When a concept fades, even fundamentally strong stocks in the group suffer. The regime cross determines "should I participate?"; the ROE cross determines "is this worth participating in?" — together they upgrade the question from "is the concept hot?" to "is this the right stock in the right concept at the right time?"

**Note**: The concept membership reverse-lookup map is built once at cold start (~30s) and cached for 6 hours. Concept boards complement industry boards: industry reflects business characteristics; concept reflects market narratives and capital flows.

---

### Extended Factors — Group A2 (IC-validated additions, factors_extended.py)

> All factors below were added after rolling 6-period IC analysis (20d forward, Group A, 50 stocks, 2026-03-31). IC and ICIR values are from that run.

#### 34. Divergence / 多指标共振 (divergence)
**Measures**: Confluence of multiple technical indicators (MACD histogram, RSI, volume trend, BB position) to detect signals confirmed across methodologies rather than from a single indicator.
IC=+0.130, ICIR=0.810 — one of the most stable technical signals in our universe.

**High buy_score**: MACD, RSI, volume, and BB all aligned bullishly; more confirming indicators = higher score.
**High sell_score**: Multiple indicators aligned bearishly simultaneously.

**Key principle**: A-share retail-driven markets frequently generate false signals from individual indicators. Multi-indicator confluence filters out noise and identifies cleaner trend continuations. Weight=2.0 in NORMAL/CAUTION regimes.

---

#### 35. Idiosyncratic Volatility / 个股特质波动率 (idiosyncratic_vol)
**Measures**: Residual volatility after removing market beta — the stock-specific risk component not explained by CSI 300 index movement. Computed via 60-day OLS regression.
IC=+0.229, ICIR=0.578 — **inverted**: lower idiosyncratic vol → higher score.

**High buy_score**: Low residual vol after market-beta removal (stock moves with market, not as a standalone lottery ticket).
**High sell_score**: High residual vol — the stock moves unpredictably relative to the market, a sign of speculative activity.

**Key principle**: A-share "lottery effect" — high-idiosyncratic-vol stocks attract retail speculators seeking asymmetric payoffs, causing overpricing and subsequent underperformance. This is the opposite of US markets where idiosyncratic vol is often rewarded. Weight=2.0 in NORMAL/CAUTION/CRISIS regimes, reduced to 0.3 in BULL (speculative names lead rallies).

---

#### 36. Momentum Concavity / 动量加速度 (momentum_concavity)
**Measures**: Acceleration of momentum — recent 10-day return minus prior 10-day return. Positive concavity means momentum is accelerating (improving), not just continuing.
IC=+0.135, ICIR=0.566.

**High buy_score**: Recent momentum stronger than prior momentum (acceleration).
**High sell_score**: Momentum decelerating (recent period weaker than prior period).

**Key principle**: Trend continuation vs. trend fading is more predictive than the trend level alone. A stock that was +5% last month and +8% this month is a better hold than one that was +8% then +5%. Weight=2.0 in NORMAL/BULL; dropped in CAUTION (unreliable when trend is breaking).

---

#### 37. Bollinger Band Squeeze / 布林带收缩 (bb_squeeze)
**Measures**: Ratio of current Bollinger Band width to its 60-day average. A very narrow band (squeeze) means volatility has compressed — often precedes a breakout.
IC=+0.064, ICIR=0.399.

**High buy_score**: Band width significantly below its 60-day average (compressed) AND price above MA20 (squeeze + uptrend).
**High sell_score**: Band width above average (expanding volatility) in a downtrend.

**Key principle**: Volatility compression is a universal breakout precursor. The squeeze alone is direction-neutral; the MA20 condition filters for upward resolutions. Weight=0.5 in NORMAL; 1.0 in BULL (breakout setups thrive in bull momentum).

---

#### 38. Cash Flow Quality / 现金流质量 (cash_flow_quality)
**Measures**: Ratio of operating cash flow to net profit (经营现金净流量与净利润的比率). High ratio means earnings are backed by actual cash, not accounting accruals.
IC=+0.164, ICIR=0.894 — **second-strongest fundamental factor after low_volatility**.

**High buy_score**: Operating cash flow ≥ 100% of net profit (cash > earnings — genuine cash generation).
**High sell_score**: Operating cash flow far below net profit (earnings not converting to cash — potential accruals or channel stuffing).

**Key principle**: A-share accounting manipulation is widespread. Companies with high accrual-to-earnings ratios often suffer future earnings disappointments as accounting reverses to cash reality. Weight=2.0 across all regimes including CRISIS.

---

#### 39. Main Force Inflow / 大单净流入 (main_inflow)
**Measures**: Net inflow percentage from large orders (主力净流入占比) over 5 days — a proxy for institutional accumulation vs. distribution.
IC=+0.060, ICIR=0.239 — meaningful but low ICIR (noisy).

**High buy_score**: Large-order net inflow positive (institutions accumulating).
**High sell_score**: Large-order net outflow (institutions distributing).

**Key principle**: Large order flow separates "smart money" from retail activity in A-shares. However, signal is noisy (low ICIR) because East Money's "主力" classification includes both institutions and large retail operators. Weight=0.5 in NORMAL; 1.0 in BULL (institutional flow more relevant in rallies).

---

#### 40. ROE Trend / ROE趋势 (roe_trend)
**Measures**: Direction of change in Return on Equity between the most recent and prior period.
IC=+0.053, ICIR=0.362 — weak but stable positive signal.

**High buy_score**: ROE increasing period-over-period (business profitability improving).
**High sell_score**: ROE declining (deteriorating profitability).

**Key principle**: Level of ROE matters less than direction — a company improving from 5% to 8% ROE often outperforms one declining from 15% to 12%. Complements the Piotroski score (which measures 9-dimension financial health improvement). Weight=0.5 in NORMAL; 1.0 in CAUTION (resilient businesses hold ROE in corrections).

---

#### 41. Amihud Illiquidity / 非流动性 (amihud_illiquidity)
**Measures**: Mean(|daily return| / daily amount) over 60 days — the Amihud (2002) illiquidity ratio. Higher = less liquid.
IC=−0.062, ICIR=−0.275 — **inverted**: lower illiquidity (more liquid) → higher score.

**High buy_score**: Highly liquid stock (low Amihud ratio) — easy to trade in and out without price impact.
**High sell_score**: Illiquid stock — large bid-ask spreads, price impact on entry/exit.

**Key principle**: Academic literature finds an illiquidity premium (illiquid stocks should earn more). In A-share short-horizon cross-sections, we observe the opposite: illiquid micro-caps underperform due to reversal risk, forced selling, and index exclusion effects. Weight=−0.5 (inverted) in NORMAL.

---

#### 42. Medium-Term Momentum / 中期动量 (medium_term_momentum)
**Measures**: 40-day return (skipping the most recent 20 days to avoid overlap with price_inertia). Captures the 1–3 month price trend.
IC=−0.108, ICIR=−0.352 — **inverted**: prior 40-day winners underperform.

**High buy_score** (inverted): Recent 40-day losers (contrarian entry point).
**High sell_score** (inverted): Recent 40-day winners (mean-reversion sell signal).

**Key principle**: In global markets, medium-term momentum (6–12 months) is a robust factor. In A-shares, it **reverses**: the 1–3 month horizon is dominated by retail mean-reversion behavior. Stocks that have run up 40 days ago are crowded by retail, become overbought, and subsequently underperform. This is one of the clearest regime differences between A-shares and global markets. Weight=−1.0 in NORMAL; −1.5 in CAUTION (mean-reversion stronger in corrections).

---

#### 43. OBV Trend / OBV趋势 (obv_trend)
**Measures**: Slope of On-Balance Volume (OBV) over the past 20 days, normalized by average volume. Positive slope = accumulation (volume on up-days > down-days).
IC=−0.115, ICIR=−0.479 — **inverted**: OBV accumulation predicts underperformance.

**High buy_score** (inverted): OBV declining (distribution pattern) — retail has exited or is distributing.
**High sell_score** (inverted): OBV strongly positive (accumulation pattern) — retail retail-chasing signal.

**Key principle**: In A-shares, OBV accumulation is a **reversal signal**, not a continuation signal. OBV rising means retail investors are chasing a move (buying on up-days), creating overbuying that subsequently reverses. This is the inverse of the US market intuition. Weight=−1.0 in NORMAL/CAUTION.

---

#### 44. ATR Normalized / 归一化ATR (atr_normalized)
**Measures**: Average True Range over 14 days, divided by closing price to normalize. ATR captures realised price range (including gap risk), unlike close-to-close volatility.
IC=+0.249, ICIR=0.802 — **strongest new factor; tied with low_volatility for top signal**.

**High buy_score**: Low normalised ATR — stock has tight daily ranges relative to its price level, indicating low realised risk and stable price behavior.
**High sell_score**: High normalised ATR — wide daily swings, gap risk, harder to hold without being stopped out.

**Key principle**: ATR captures gap risk that close-to-close volatility misses. A stock that opens -4% once a week looks "stable" on daily close vol but has catastrophic ATR. This makes atr_normalized a complementary (not redundant) signal to low_volatility. The extremely high ICIR (0.802) reflects that the signal is stable across all 6 rolling periods. Weight=2.0 in NORMAL; upweighted to 2.5 in CAUTION/CRISIS (capital preservation regimes).

---

#### 45. MA60 Deviation / 60日均线偏离 (ma60_deviation)
**Measures**: (Close − MA60) / MA60 × 100%. Positive = stock above its 60-day MA; negative = below.
IC=+0.098, ICIR=0.668 — **inverted in score**: stocks below MA60 get higher scores (contrarian).

**High buy_score**: Stock trading near or below its 60-day moving average — historically cheap relative to its recent trend, mean-reversion opportunity.
**High sell_score**: Stock trading significantly above MA60 — extended, prone to mean-reversion.

**Key principle**: MA60 deviation is a medium-term mean-reversion signal in A-shares. Unlike US markets where extended moves can continue (momentum), A-share retail investors and institutional profit-taking create systematic reversion at the 60-day horizon. Stocks 20%+ above MA60 face selling pressure; stocks near or below MA60 find natural support. Weight=1.0 in NORMAL; 1.5 in CAUTION/CRISIS (extended stocks fall hardest in corrections).

---

#### 46. MAX Effect / 最大单日涨幅 (max_return)
**Measures**: Maximum single-day return over the past 20 trading days.
IC=+0.216, ICIR=0.947 — **one of the strongest and most stable signals in the entire system**.
Score is *inverted*: high MAX → low score (lottery overpricing → expect underperformance).

**High buy_score** (inverted): No extreme single-day spikes in past 20 days — stock moves in a controlled, steady manner consistent with fundamental-driven buying.
**High sell_score** (inverted): Large spike day in recent history (≥5%) — characteristic of lottery stocks targeted by retail momentum-chasers, subsequently reverting.

**Key principle**: Bali, Cakici & Whitelaw (2011) document that stocks with extreme recent positive returns are systematically overpriced by investors seeking lottery-like payoffs. The MAX effect is especially powerful in A-shares given the dominant retail investor base and the limit-up/limit-down system that concentrates attention on stocks hitting daily limits. A stock's maximum single day captures this lottery appeal better than average volatility. The extremely high ICIR (0.947) means this factor fires consistently across all market conditions. Weight=2.0 in NORMAL; downweighted to 0.3 in BULL (lottery stocks are bid up in rallies); upweighted to 2.5/3.0 in CAUTION/CRISIS (lottery stocks implode fastest in sell-offs).

---

#### 47. Return Skewness / 收益率偏度 (return_skewness)
**Measures**: Skewness of the daily return distribution over the past 60 days. Positive skewness = right-tail asymmetry (occasional large gains, frequent small losses — the "lottery" pattern).
IC=+0.105, ICIR=0.872 — strong and consistent signal.
Score is *inverted*: positive skewness → low score.

#### 48. Intraday vs Overnight Return Split / 日内vs隔夜收益分拆 (intraday_vs_overnight)
**Measures**: Decomposes daily returns into intraday ((close-open)/open) and overnight ((open-prev_close)/prev_close) components over 20 days. Net signal = avg_intraday − avg_overnight.
IC=−0.103, ICIR=−0.461 — moderate inverted signal. Weight=−0.5 in NORMAL/CAUTION regimes.
Score is *inverted*: high intraday vs overnight → low score.

**A-share interpretation**: Stocks with strong intraday returns relative to overnight gaps tend to underperform — in A-shares, intraday price strength without overnight gaps signals retail day-trader activity (pump-and-dump dynamics) rather than genuine institutional accumulation. Stocks that gap overnight have real news catalysts or genuine institutional pre-positioning. High intraday/overnight ratio = potential exhaustion signal.

**Excluded (noise):**

#### 49. Market Relative Strength / 个股相对指数强弱 (market_relative_strength)
IC=+0.0006, ICIR=0.003 — pure noise. The 20-day excess return of a stock vs CSI300 adds no signal beyond price_inertia after controlling for market moves. Excluded.

#### 50. Price Efficiency (Kaufman ER) / 价格效率比率 (price_efficiency)
IC=+0.034, ICIR=0.249 — weak signal below the noise threshold. Kaufman Efficiency Ratio (|net_price_change| / Σ|daily_changes|) over 20 days measures trend linearity, but this signal does not survive in A-shares where price efficiency may be regime-dependent. Excluded.

**High buy_score** (inverted): Negative or near-zero skewness — returns are symmetric or slightly left-skewed, indicating no lottery-premium overpricing.
**High sell_score** (inverted): Strongly positive skewness — stock exhibits lottery-like return patterns, attracting overpricing by retail investors seeking asymmetric payoffs.

**Key principle**: Harvey & Siddique (2000) show that investors accept lower expected returns for positively skewed assets (they pay a premium for right-tail exposure). This creates a systematic overpricing of lottery-like stocks. Skewness is related to but distinct from the MAX effect: MAX captures the single largest spike, while skewness measures the overall distributional shape across 60 days. A stock can have moderate MAX but persistent positive skewness (many small positive outliers), or vice versa. Together they provide complementary lottery-risk coverage. Weight=1.5 in NORMAL; 2.0/2.5 in CAUTION/CRISIS.

---

### Score Interpretation

**Total Buy Score (total_score)**:

| Score | Interpretation |
|---|---|
| ≥ 80 | Excellent — strong across all dimensions, high-priority watchlist |
| 65–79 | Good — solid overall, worth tracking |
| 50–64 | Average — some strengths but notable weaknesses |
| 35–49 | Weak — underperforms on multiple dimensions, proceed with caution |
| < 35 | Poor — significant fundamental or valuation concerns |

**Total Sell Score (total_sell_score)**:

| Score | Interpretation |
|---|---|
| ≥ 70 | Strong sell — multiple significant bearish signals firing simultaneously |
| 50–69 | Moderate sell pressure — consider reducing position |
| 35–49 | Mild caution — monitor closely |
| 20–34 | Low sell pressure — hold, wait for clearer signal |
| < 20 | No significant sell signal |
