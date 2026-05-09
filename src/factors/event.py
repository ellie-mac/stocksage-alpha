from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from ._utils import _extract, _extract_two, _neutral, _get_price_position


def score_shareholder_change(
    shareholder_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    industry_excess: Optional[float] = None,
    market_regime_score: Optional[float] = None,
) -> dict:
    """
    Shareholder count quarterly change (max 15).
    Decreasing count = share concentration = bullish signal (A-share specific).
      change <= -10%  -> 15 pts
      change 0 to -10% -> linear 8-15 pts
      change 0 to +10% -> linear 3-8 pts
      change >= +20%  ->  0 pts

    Context cross with 52w price position:
      Concentration (change <= -5%) + low position (< 0.3)  -> smart money accumulating at lows -> buy +3
      Dispersion (change >= +10%) + high position (> 0.7)   -> top distribution confirmed -> sell +2
      Dispersion (change >= +10%) + low position (< 0.3)    -> retail bottom-fishing (less bearish) -> sell -2

    Earnings revision cross (dual institutional confirmation):
      Concentration (change <= -5%) + net analyst upgrades >= 2
        -> two independent signals (chip concentration + sell-side upgrade) pointing the same way -> buy +2
      Dispersion (change >= +10%) + net analyst downgrades <= -2
        -> smart money exiting + analysts cutting simultaneously -> sell +2 (dual exit signal)

    Industry momentum cross (requires industry_excess):
      Concentration (change <= -5%) + industry outperforming (excess >= +3%) -> buy +1.5 (轮动窗口加速建仓)
      Concentration + industry underperforming (excess <= -3%)               -> buy -1 (可能是套牢盘集中)
      Dispersion (change >= +10%) + industry underperforming (excess <= -3%) -> sell +1 (行业下行加速出逃)

    Market regime cross (requires market_regime_score):
      Concentration (change <= -5%) + bear market (regime <= 3) -> smart money bottom-fishing in bear, buy +2
      Concentration + bull market (regime >= 7)                 -> normal accumulation, slightly less informative, buy -0.5
      Dispersion (change >= +10%) + bear market                 -> retail fleeing a falling market, sell +1.5
    """
    if shareholder_df is None or shareholder_df.empty:
        return _neutral(15)

    holder_cols = [c for c in shareholder_df.columns
                   if any(k in c for k in ["股东人数", "股东总人数", "持股人数"])]
    if not holder_cols:
        return _neutral(15)

    series = pd.to_numeric(shareholder_df[holder_cols[0]], errors="coerce").dropna()
    if len(series) < 2:
        return _neutral(15)

    current = float(series.iloc[0])
    prev    = float(series.iloc[1])
    if prev <= 0:
        return _neutral(15)

    change_pct = (current - prev) / prev * 100

    if change_pct <= -10:
        score = 15.0
    elif change_pct <= 0:
        score = 8.0 + (-change_pct / 10) * 7.0
    elif change_pct <= 10:
        score = 8.0 - (change_pct / 10) * 5.0
    elif change_pct <= 20:
        score = 3.0 - (change_pct - 10) / 10 * 3.0
    else:
        score = 0.0

    signal = ("strong concentration" if change_pct <= -10 else
              "concentrating" if change_pct <= 0 else
              "dispersing" if change_pct <= 10 else "heavy distribution")

    # --- Sell score: shareholder count increasing (dispersion/distribution) ---
    if change_pct >= 20:
        sell_score = 12.0
    elif change_pct >= 10:
        # linear: 10% -> 7pts, 20% -> 12pts
        sell_score = 7.0 + (change_pct - 10) / 10 * 5.0
    elif change_pct >= 0:
        # linear: 0% -> 2pts, 10% -> 7pts
        sell_score = 2.0 + (change_pct / 10) * 5.0
    else:
        sell_score = 0.0

    sell_score = round(min(15.0, sell_score), 1)

    # --- Context cross: price position × shareholder change direction ---
    position = _get_price_position(price_df)
    if position is not None:
        if change_pct <= -5 and position < 0.3:
            # Concentration at low price: smart money picking up shares while stock is beaten down
            score = min(15.0, score + 3.0)
            signal = "strong concentration at low price (smart money accumulating)"
        elif change_pct >= 10 and position > 0.7:
            # Dispersion at high price: classic top — retail buying as institutions exit
            sell_score = min(15.0, sell_score + 2.0)
            signal = "dispersion at high price (top distribution confirmed)"
        elif change_pct >= 10 and position < 0.3:
            # Dispersion at low price: retail bottom-fishing (many new buyers entering)
            # Less bearish — could be driven by new long-term investors, not distribution
            sell_score = max(0.0, sell_score - 2.0)
            signal = "dispersion at low price (bottom-fishing, less bearish)"

    # --- Earnings revision cross: dual institutional confirmation ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if change_pct <= -5 and net_rev >= 2:
                # Chip concentration + analyst upgrades: two independent institutions pointing the same way
                score = min(15.0, score + 2.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — dual confirmation"
            elif change_pct >= 10 and net_rev <= -2:
                # Dispersion + analyst cuts: smart money exits + sell-side consensus deteriorates
                sell_score = min(15.0, sell_score + 2.0)
                signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dual exit signal"

    # --- Industry momentum cross: sector context changes chip signal interpretation ---
    if industry_excess is not None:
        if change_pct <= -5 and industry_excess >= 3:
            # Chip concentration while sector is rising: institutions accelerating accumulation in rotation window
            score = min(15.0, score + 1.5)
            signal = signal + f" + industry outperforming ({industry_excess:+.1f}%) — 轮动窗口加速建仓"
        elif change_pct <= -5 and industry_excess <= -3:
            # Concentration in a falling sector: could be trapped longs, not conviction buying
            score = max(0.0, score - 1.0)
            signal = signal + f" + industry weak ({industry_excess:+.1f}%) — 可能是套牢盘集中，打折"
        elif change_pct >= 10 and industry_excess <= -3:
            # Dispersion while sector falls: holders fleeing a weak sector
            sell_score = min(15.0, sell_score + 1.0)
            signal = signal + f" + industry weak ({industry_excess:+.1f}%) — 行业下行加速出逃"

    # --- Market regime cross: concentration signal reliability is highest in bear markets ---
    if market_regime_score is not None:
        if change_pct <= -5:
            if market_regime_score <= 3:
                # Bear market concentration: informed buyers picking up shares against the trend — highest conviction
                score = min(15.0, score + 2.0)
                signal = signal + " (bear market — 熊市集中是高置信度逆势建仓)"
            elif market_regime_score >= 7:
                # Bull market concentration: normal in rising markets, lower informational edge
                score = max(0.0, score - 0.5)
                signal = signal + " (bull market — 牛市集中信号平凡化，略打折)"
        elif change_pct >= 10 and market_regime_score <= 3:
            # Dispersion in a bear market: holders fleeing a weak environment — amplify sell
            sell_score = min(15.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市分散加速出逃)"

    sell_score = round(sell_score, 1)

    return {
        "score": round(score, 1),
        "sell_score": sell_score,
        "max": 15,
        "details": {
            "current_holders":  int(current),
            "prev_holders":     int(prev),
            "change_pct":       round(change_pct, 2),
            "position_52w":     round(position, 3) if position is not None else None,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "market_regime_score": market_regime_score,
            "signal":           signal,
            "sell_score":       sell_score,
        },
    }




def score_lhb(
    lhb_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


def score_lockup_pressure(
    lockup_df: Optional[pd.DataFrame],
    circulating_cap: float = 0,
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    social_dict: Optional[dict] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Upcoming lock-up expiry supply pressure (max 10, inverse signal).
    Large upcoming unlock → supply overhang → lower score.
      ratio <= 1%    -> 10 pts
      ratio 1-5%     -> linear 10-5 pts
      ratio 5-20%    -> linear 5-2 pts
      ratio >= 20%   ->  0 pts

    Context cross with 52w price position:
      Large unlock (>= 5%) + low position (< 0.3)  -> insiders underwater, unlikely to sell -> reduce sell (-3)
      Large unlock (>= 5%) + high position (> 0.7) -> insiders sitting on profits, likely to sell -> amplify sell (+2)

    Earnings growth cross: can buyers absorb the unlock supply? (requires financial_df)
      Large unlock (>= 5%) + profit growth >= 20% -> growing business attracts buyers, sell -2
      Large unlock (>= 5%) + profit growth < 0    -> declining earnings, no buyers to absorb supply, sell +2

    Social heat cross: unlock into retail frenzy = A-share "lockup dump" pattern (requires social_dict)
      Large unlock (>= 5%) + social heat top 10%  -> PE holders dump into retail FOMO = amplified sell +2
      Large unlock (>= 5%) + social heat > 50%    -> no retail bid to absorb, insiders can't easily exit = sell -1

    Volume distribution cross (requires price_df):
      Large unlock (>= 5%) + price up > 5% in 1m + volume contracting (v10/v30 < 0.75)
        -> classic pre-unlock distribution: insiders drip-selling into strength -> sell +2

    Market regime cross (requires market_regime_score):
      Large unlock (>= 5%) + bear market (regime <= 3) -> no buyers to absorb, amplify sell +1.5
      Large unlock (>= 5%) + bull market (regime >= 7) -> rising tide provides buyer depth, reduce sell -1
    """
    if lockup_df is None or lockup_df.empty or circulating_cap <= 0:
        # No lockup data: neutral buy score, minimal sell score
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    # Look for unlock amount column
    amount_cols = [c for c in lockup_df.columns
                   if any(k in c for k in ["解禁数量", "解禁金额", "解禁市值", "解禁股数"])]
    if not amount_cols:
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    try:
        amounts = pd.to_numeric(lockup_df[amount_cols[0]], errors="coerce").dropna()
        if amounts.empty:
            return {"score": 2.0, "sell_score": 2.0, "max": 10,
                    "details": {"signal": "no data, neutral", "sell_score": 2.0}}

        # Sum unlocks within next 90 days if date column available
        date_cols = [c for c in lockup_df.columns if any(k in c for k in ["日期", "解禁日"])]
        if date_cols:
            lockup_df = lockup_df.copy()
            lockup_df["_date"] = pd.to_datetime(lockup_df[date_cols[0]], errors="coerce")
            today = pd.Timestamp.now()
            near_term = lockup_df[
                (lockup_df["_date"] >= today) &
                (lockup_df["_date"] <= today + pd.Timedelta(days=90))
            ]
            unlock_val = float(pd.to_numeric(near_term[amount_cols[0]], errors="coerce").sum())
        else:
            unlock_val = float(amounts.iloc[0])
    except Exception:
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"signal": "no data, neutral", "sell_score": 2.0}}

    # If unlock value is in share count (not CNY), we can't compute ratio without price
    # Use simple ratio if within same unit as circulating_cap (CNY)
    ratio = unlock_val / circulating_cap * 100

    # buy_score: 2 for no/minimal upcoming lockup (neutral, not a real buy signal)
    if ratio <= 1:
        buy_score = 2.0
    elif ratio <= 5:
        buy_score = 2.0 - (ratio - 1) / 4 * 1.0   # slight negative
    else:
        buy_score = 0.0

    # sell_score: pressure-based — this IS the sell signal for this factor
    if ratio >= 20:
        sell_score = 9.0
    elif ratio >= 5:
        # linear: 5% -> 5pts, 20% -> 9pts
        sell_score = 5.0 + (ratio - 5) / 15 * 4.0
    elif ratio >= 1:
        # linear: 1% -> 1pt, 5% -> 5pts
        sell_score = 1.0 + (ratio - 1) / 4 * 4.0
    else:
        sell_score = 0.0

    sell_score = round(min(10.0, sell_score), 1)
    signal = ("clean" if ratio <= 1 else
              "moderate overhang" if ratio <= 5 else "heavy overhang")

    # --- Context cross: price position × unlock pressure ---
    position = _get_price_position(price_df)
    if position is not None and ratio >= 5:
        if position < 0.3:
            # Low price position: insiders are likely underwater → less incentive to sell
            sell_score = max(0.0, sell_score - 3.0)
            signal = f"{signal} (mitigated — low price, insiders likely underwater)"
        elif position > 0.7:
            # High price position: insiders sitting on large profits → strong incentive to sell
            sell_score = min(10.0, sell_score + 2.0)
            signal = f"{signal} (amplified — high price, insiders motivated to sell)"

    sell_score = round(sell_score, 1)

    # --- Earnings growth cross: does the business have enough buyers to absorb supply? ---
    if financial_df is not None and ratio >= 5:
        profit_growth = _extract(financial_df, [
            "净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"])
        if profit_growth is not None:
            if profit_growth >= 20:
                # Growing fast: fundamental buyers entering, absorbing unlock supply
                sell_score = max(0.0, sell_score - 2.0)
                signal = signal + " (growth attracts buyers — supply absorbed)"
            elif profit_growth < 0:
                # Declining earnings: no fundamental buyers; unlock supply hits a weak bid
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " (declining earnings — no buyers, amplified pressure)"

    # --- Social heat cross: unlocking into retail frenzy = A-share lockup dump pattern ---
    if social_dict is not None and ratio >= 5:
        rank_pct = social_dict.get("rank_pct")
        if rank_pct is not None:
            rank_pct_f = float(rank_pct)
            if rank_pct_f <= 10:
                # Extreme retail attention: PE/founder holders see the perfect exit window
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " + extreme social heat (unlock into retail FOMO — amplified)"
            elif rank_pct_f > 50:
                # Low retail interest: insiders have no easy buyer pool to exit into
                sell_score = max(0.0, sell_score - 1.0)
                signal = signal + " + low social heat (no retail bid — exit harder, mitigated)"

    # --- Volume distribution cross: pre-unlock distribution pattern ---
    if price_df is not None and ratio >= 5 and "volume" in price_df.columns and len(price_df) >= 40:
        try:
            closes = pd.to_numeric(price_df["close"], errors="coerce").dropna()
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(closes) >= 21 and len(vol) >= 40:
                ret_1m_lk = float((closes.iloc[-1] - closes.iloc[-21]) / closes.iloc[-21] * 100)
                v10 = float(vol.tail(10).mean())
                v30 = float(vol.tail(40).head(30).mean())
                if v30 > 0 and ret_1m_lk > 5 and v10 / v30 < 0.75:
                    # Price has risen while volume contracted + big unlock coming
                    # = classic pre-unlock distribution: insiders drip-selling into strength
                    sell_score = min(10.0, sell_score + 2.0)
                    signal = signal + f" + price up {ret_1m_lk:.0f}% on contracting volume (pre-unlock distribution)"
        except Exception:
            pass

    # --- Market regime cross: unlock absorption capacity is regime-dependent ---
    if market_regime_score is not None and ratio >= 5:
        if market_regime_score <= 3:
            # Bear market: incremental sellers from unlock have no buyers → supply overhang worsens
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市无人接盘，解禁压力放大)"
        elif market_regime_score >= 7:
            # Bull market: rising prices draw in buyers who can absorb unlock supply
            sell_score = max(0.0, sell_score - 1.0)
            signal = signal + " (bull market — 牛市有买盘消化解禁压力)"

    # --- Industry excess cross: sector direction shifts unlock holder motivation ---
    industry_signal_lk = None
    if industry_ret_1m is not None and market_ret_1m is not None and ratio >= 5:
        excess_lk = industry_ret_1m - market_ret_1m
        if excess_lk <= -3.0:
            # Weak sector: holders see deteriorating fundamentals, rush to exit during unlock window
            sell_score = min(10.0, sell_score + 1.5)
            industry_signal_lk = f"行业弱(超额{excess_lk:+.1f}%) — 股东借解禁窗口加速撤退"

    # --- Earnings revision cross: analyst view on company health during unlock window ---
    revision_signal_lk = None
    if revision_df is not None and not revision_df.empty and ratio >= 5:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_lk   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_lk = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_lk  = up_lk - down_lk
                if net_lk <= -2:
                    # Large unlock + analyst downgrades: supply shock + demand collapse double kill
                    sell_score = min(10.0, sell_score + 1.5)
                    revision_signal_lk = f"大解禁+分析师下调({net_lk:+d}家) — 供给冲击+需求萎缩双杀"
                elif net_lk >= 2:
                    # Large unlock + analyst upgrades: company in good shape, unlock may not trigger sell-off
                    sell_score = max(0.0, sell_score - 1.0)
                    revision_signal_lk = f"大解禁+分析师上调({net_lk:+d}家) — 基本面向好，解禁冲击有限"
        except Exception:
            pass

    sell_score = round(sell_score, 1)
    return {
        "score": round(max(0.0, buy_score), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "unlock_amount_billion": round(unlock_val / 1e8, 2),
            "ratio_pct": round(ratio, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "market_regime_score": market_regime_score,
            "industry_signal": industry_signal_lk,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "revision_signal": revision_signal_lk,
            "signal": signal,
            "sell_score": sell_score,
        },
    }




def score_insider(
    insider_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    industry_excess: Optional[float] = None,
    market_regime_score: Optional[float] = None,
) -> dict:
    """
    Major shareholder net buy/sell in past 6 months (max 10).
    Net buying = positive alignment; selling = negative signal.
      Net positive   -> 5-10 pts (proportional to buy/sell ratio)
      No activity    ->  5 pts (neutral)
      Net negative   -> 0-4 pts

    Context cross with 52w price position:
      Net buy + low position (< 0.3)   -> highest conviction buy (insiders invest at lows) -> buy +2
      Net sell + low position (< 0.3)  -> RED FLAG (selling underwater = structural problem) -> sell +3
      Net sell + high position (> 0.7) -> rational profit-taking confirmed -> sell +2

    Earnings revision cross: insider vs. analyst information signal
      Net buy + net upgrades >= 2  -> insider AND analyst both bullish = dual conviction signal -> buy +2
      Net sell + net downgrades <= -2 -> insider AND analyst both bearish = dual exit signal -> sell +2
      Net buy + net downgrades <= -2 -> management buying despite analyst cuts = insider conviction overrides -> sell -1

    Industry momentum cross (requires industry_excess):
      Net buy + industry underperforming (excess <= -3%) -> buy +2 (逆势增持，熊途最高置信度)
      Net sell + industry outperforming (excess >= +3%)  -> sell +2 (趁行业好出货，信息优势明显)
      Net sell + industry also falling (excess <= -3%)   -> sell +1.5 (随行业下行出逃确认)

    Market regime cross (requires market_regime_score):
      Net buy (ratio > 0.3) + bear market (regime <= 3) -> buy +2 (熊市逆势增持=最高置信度的内部人信号)
      Net sell (ratio < -0.3) + bull market (regime >= 7) -> sell -1 (牛市减持可能只是正常套现，降低惩罚)
    """
    if insider_df is None or insider_df.empty:
        return _neutral(10)

    buy_cols  = [c for c in insider_df.columns if any(k in c for k in ["增持", "买入数量", "增持数量"])]
    sell_cols = [c for c in insider_df.columns if any(k in c for k in ["减持", "卖出数量", "减持数量"])]

    try:
        buy_total  = float(pd.to_numeric(insider_df[buy_cols[0]], errors="coerce").sum()) if buy_cols else 0.0
        sell_total = float(pd.to_numeric(insider_df[sell_cols[0]], errors="coerce").sum()) if sell_cols else 0.0
    except Exception:
        return _neutral(10)

    net = buy_total - sell_total
    total = buy_total + sell_total

    buy_events  = len(insider_df[insider_df[buy_cols[0]].notna()]) if buy_cols else 0
    sell_events = len(insider_df[insider_df[sell_cols[0]].notna()]) if sell_cols else 0

    if total == 0:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"net_shares": 0, "buy_events": 0, "sell_events": 0, "signal": "no activity", "sell_score": 0.0}}

    # Score based on net buy ratio
    net_ratio = net / total  # -1 to +1
    score = 5.0 + net_ratio * 5.0

    signal = ("strong buy" if net_ratio > 0.5 else
              "net buy" if net_ratio > 0 else
              "net sell" if net_ratio > -0.5 else "strong sell")

    # --- Sell score: insider net selling ---
    if net_ratio < -0.5:
        # strong sell: net_ratio from -0.5 to -1.0 -> 3 to 8pts
        sell_score = 3.0 + (-net_ratio - 0.5) / 0.5 * 5.0  # 3-8pts
    elif net_ratio <= 0:
        # net sell: 0 to -0.5 -> 0 to 3pts
        sell_score = (-net_ratio) / 0.5 * 3.0
    else:
        sell_score = 0.0

    sell_score = round(min(8.0, sell_score), 1)

    # --- Context cross: price position × insider transaction direction ---
    position = _get_price_position(price_df)
    if position is not None:
        if net_ratio > 0.3 and position < 0.3:
            # Insider buying at depressed prices: maximum conviction — they're putting money in at lows
            score = min(10.0, score + 2.0)
            signal = signal + " (at low price — highest conviction)"
        elif net_ratio < -0.3 and position < 0.3:
            # Insider selling even when underwater: RED FLAG — they see structural issues ahead
            sell_score = min(8.0, sell_score + 3.0)
            signal = signal + " (at low price — RED FLAG: selling underwater)"
        elif net_ratio < -0.3 and position > 0.7:
            # Insider selling near 52w high: rational profit-taking → amplify sell signal
            sell_score = min(8.0, sell_score + 2.0)
            signal = signal + " (at high price — profit-taking confirmed)"

    # --- Earnings revision cross: insider intent vs. analyst consensus ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up_r   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down_r = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up_r - down_r
            if net_ratio > 0.3 and net_rev >= 2:
                # Insider buying + analyst upgrades: both information sources bullish
                score = min(10.0, score + 2.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — dual conviction"
            elif net_ratio < -0.3 and net_rev <= -2:
                # Insider selling + analyst downgrades: dual institutional exit
                sell_score = min(8.0, sell_score + 2.0)
                signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dual exit signal"
            elif net_ratio > 0.3 and net_rev <= -2:
                # Insiders buying but analysts cutting: management has conviction analysts don't
                sell_score = max(0.0, sell_score - 1.0)
                signal = signal + f" (buying despite analyst cuts — management conviction overrides)"

    # --- Industry momentum cross: sector context amplifies insider signal ---
    if industry_excess is not None:
        if net_ratio > 0.3 and industry_excess <= -3:
            # Insider buying while sector is falling: maximum conviction (against the tide)
            score = min(10.0, score + 2.0)
            signal = signal + f" (逆势增持 — industry {industry_excess:+.1f}%, 熊途最高置信度)"
        elif net_ratio < -0.3 and industry_excess >= 3:
            # Insider selling while sector is hot: information advantage, dumping into sector rally
            sell_score = min(8.0, sell_score + 2.0)
            signal = signal + f" (趁好出货 — industry {industry_excess:+.1f}%, 信息优势明显)"
        elif net_ratio < -0.3 and industry_excess <= -3:
            # Insider also selling in a weak sector: confirms structural deterioration
            sell_score = min(8.0, sell_score + 1.5)
            signal = signal + f" (随行业下行出逃 — industry {industry_excess:+.1f}%)"

    # --- Market regime cross: insider buy/sell conviction varies with market environment ---
    regime_signal = None
    if market_regime_score is not None:
        if net_ratio > 0.3 and market_regime_score <= 3:
            # Insider buying in a bear market: putting capital in against the tide = highest conviction
            score = min(10.0, score + 2.0)
            regime_signal = f"逆熊增持(regime={market_regime_score:.1f}) — 最高置信度的内部人信号"
        elif net_ratio < -0.3 and market_regime_score >= 7:
            # Insider selling in a bull market: likely routine profit-taking, less alarming
            sell_score = max(0.0, sell_score - 1.0)
            regime_signal = f"牛市减持(regime={market_regime_score:.1f}) — 可能是正常套现，降低惩罚"

    sell_score = round(sell_score, 1)

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "net_shares_million": round(net / 1e6, 1),
            "buy_events":         buy_events,
            "sell_events":        sell_events,
            "position_52w":       round(position, 3) if position is not None else None,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "market_regime_score": market_regime_score,
            "regime_signal":       regime_signal,
            "signal":             signal,
            "sell_score":         sell_score,
        },
    }




def score_institutional_visits(
    visits_df: Optional[pd.DataFrame],
    revision_df: Optional[pd.DataFrame] = None,
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
) -> dict:
    """
    Institutional research visit frequency in past 90 days (max 10).
    More visits = analyst/fund attention = rising conviction.
      visits >= 10  -> 10 pts
      visits  5-10  -> linear 7-10 pts
      visits  1-5   -> linear 3-7 pts
      visits == 0   ->  2 pts

    Earnings revision cross: early information signal (requires revision_df)
      Visits >= 5 (past 90d) + net revisions == 0 -> buy +1
        (institutions surveying ahead of consensus — the "pre-upgrade accumulation" pattern)
      Visits >= 5 + net upgrades >= 2             -> buy +1
        (visits AND upgrades together = institutional consensus crystallising)

    52w price position cross (requires price_df):
      High visits (>= 5) + low position (< 0.3) -> 机构在低位调研=抄底发现被低估标的 -> buy +2
      High visits (>= 5) + high position (> 0.7) -> 机构高位调研可能是卖前尽调 -> sell +1

    Market regime cross (requires market_regime_score):
      High visits (>= 5) + bear market (regime <= 3) -> buy +1.5 (熊市主动调研=内部人发现被低估的逆向信号)

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      High visits (>= 5) + weak industry (excess <= -3%) -> buy +1 (弱行业中仍在调研=对个股alpha有信心)
      High visits (>= 5) + hot industry (excess >= +3%)  -> buy -0.5 (热行业调研可能是被动跟随而非主动发现)
    """
    if visits_df is None or visits_df.empty:
        return {"score": 2.0, "sell_score": 2.0, "max": 10,
                "details": {"visit_count_90d": 0, "signal": "no visits recorded", "sell_score": 2.0}}

    # Filter to past 90 days if date column available
    date_cols = [c for c in visits_df.columns if any(k in c for k in ["日期", "调研日期", "接待日期"])]
    count = len(visits_df)
    if date_cols:
        try:
            visits_df = visits_df.copy()
            visits_df["_date"] = pd.to_datetime(visits_df[date_cols[0]], errors="coerce")
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
            count = int((visits_df["_date"] >= cutoff).sum())
        except Exception:
            pass

    if count >= 10:
        score = 10.0
    elif count >= 5:
        score = 7.0 + (count - 5) / 5 * 3.0
    elif count >= 1:
        score = 3.0 + (count - 1) / 4 * 4.0
    else:
        score = 2.0

    signal = ("high attention" if count >= 10 else
              "moderate" if count >= 5 else
              "low" if count >= 1 else "none")

    # --- Sell score: declining visits (not a strong sell signal) ---
    # 0 visits in 90 days = mild, analysts losing interest
    sell_score = 2.0 if count == 0 else 0.0

    # --- Earnings revision cross: early positioning vs. confirmed consensus ---
    if revision_df is not None and not revision_df.empty and count >= 5:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if net_rev == 0:
                # Many visits but no public upgrade yet: institutions building positions quietly
                score = min(10.0, score + 1.0)
                signal = signal + " (pre-upgrade: institutions active, no consensus yet)"
            elif net_rev >= 2:
                # Both visits and upgrades: consensus is crystallising
                score = min(10.0, score + 1.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — institutional consensus forming"

    # --- 52w price position cross: visit intent changes completely with price level ---
    position_iv = _get_price_position(price_df)
    if position_iv is not None and count >= 5:
        if position_iv < 0.3:
            # Institutions visiting a beaten-down stock: bottom-fishing, genuine discovery
            score = min(10.0, score + 2.0)
            signal = signal + " (at low price — 低位调研=抄底发现低估标的)"
        elif position_iv > 0.7:
            # Institutions visiting a high-priced stock: possibly due-diligence before exit
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (at high price — 高位调研可能是卖前尽调)"

    # --- Market regime cross: bear market visits signal genuine contrarian discovery ---
    regime_signal = None
    if market_regime_score is not None and count >= 5:
        if market_regime_score <= 3:
            # Institutions actively visiting in bear market: going against the grain
            # = internal discovery of undervalued stock, highly contrarian signal
            score = min(10.0, score + 1.5)
            regime_signal = f"熊市主动调研({count}次) — 逆向发现被低估标的，内部人信号"

    # --- Industry excess return cross: context determines if visit is discovery or momentum-chasing ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None and count >= 5:
        excess = industry_ret_1m - market_ret_1m
        if excess <= -3.0:
            # Visiting in a weak sector: analysts going against the grain, high discovery value
            score = min(10.0, score + 1.0)
            industry_signal = f"弱行业调研(超额{excess:.1f}%) — 逆行业个股alpha，高发现价值"
        elif excess >= 3.0:
            # Hot sector: institutions may be following the crowd rather than discovering value
            score = max(0.0, score - 0.5)
            industry_signal = f"热行业调研(超额{excess:.1f}%) — 可能是被动跟随热点，发现价值较低"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "visit_count_90d": count,
            "position_52w": round(position_iv, 3) if position_iv is not None else None,
            "market_regime_score": market_regime_score,
            "regime_signal": regime_signal,
            "industry_signal": industry_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }




def score_industry_momentum(
    industry_ret_1m: Optional[float],
    market_ret_1m: Optional[float],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_stats: Optional[dict] = None,
    best_concept_ret: Optional[float] = None,
    social_dict: Optional[dict] = None,
) -> dict:
    """
    Industry 1-month excess return vs broad market (max 10).
    Rewards stocks in industries with positive relative momentum.
      excess >= +5%   -> 10 pts
      excess  0-+5%   -> linear 5-10 pts
      excess -5-0%    -> linear 2-5 pts
      excess <= -5%   ->  0 pts

    Context cross with individual stock 52w position:
      Industry outperforming (excess >= 2%) + stock low position (< 0.3)
        -> sector hot but this stock hasn't moved yet = late-mover opportunity -> buy +2
      Industry underperforming (excess <= -2%) + stock high position (> 0.7)
        -> sector falling AND stock at highs = double negative -> sell +2
      Industry outperforming + stock high position (> 0.7)
        -> stock already rode the sector wave, late entry risk -> sell +1
      Industry underperforming + stock low position (< 0.3)
        -> sector falling but stock already beaten down, most damage done -> sell -1

    Market regime cross (requires market_regime_score):
      Hot sector (excess >= 3%) + bear market (regime <= 3) -> sector momentum unreliable -> buy -1.5, sell +1
      Hot sector + bull market (regime >= 7) -> sector rotation has follow-through -> buy +1

    Industry valuation cross (requires industry_stats):
      Cheap industry (median PE <= 20) + positive momentum -> early rotation setup -> buy +1.5
      Expensive industry (median PE >= 40) + outperforming -> late-stage stretched rally -> sell +1

    Concept momentum cross (requires best_concept_ret):
      Industry outperforming (excess >= +3%) + hot concept (>= +8%) -> buy +1.5 (行业+概念双重催化=最强的散户共振信号)
      Industry underperforming + hot concept (>= +8%) -> sell -0.5 (热概念可能即将轮动到该行业，略减弱卖出)
    """
    if industry_ret_1m is None:
        return _neutral(10)

    market = market_ret_1m if market_ret_1m is not None else 0.0
    excess = industry_ret_1m - market

    if excess >= 5:
        score = 10.0
    elif excess >= 0:
        score = 5.0 + excess / 5 * 5.0
    elif excess >= -5:
        score = 2.0 + (excess + 5) / 5 * 3.0
    else:
        score = 0.0

    signal = ("outperforming" if excess >= 2 else
              "in-line" if excess >= -2 else "underperforming")

    # --- Sell score: industry underperforming market ---
    if excess <= -5:
        sell_score = 9.0
    elif excess <= 0:
        # linear: 0% -> 3pts, -5% -> 9pts
        sell_score = 3.0 + (-excess / 5) * 6.0
    else:
        sell_score = 0.0

    sell_score = round(min(9.0, sell_score), 1)

    # --- Context cross: industry momentum × individual stock price position ---
    position = _get_price_position(price_df)
    if position is not None:
        if excess >= 2 and position < 0.3:
            # Hot sector, stock hasn't moved: late-mover setup
            score = min(10.0, score + 2.0)
            signal = "outperforming sector + stock lagging (late-mover opportunity)"
        elif excess <= -2 and position > 0.7:
            # Weak sector, stock still high: catch-down risk
            sell_score = min(9.0, sell_score + 2.0)
            signal = "underperforming sector + stock at highs (catch-down risk)"
        elif excess >= 2 and position > 0.7:
            # Stock already moved with sector: late entry
            sell_score = min(9.0, sell_score + 1.0)
            signal = "outperforming sector + stock at highs (late entry risk)"
        elif excess <= -2 and position < 0.3:
            # Sector falling but stock already beaten down: most damage done
            sell_score = max(0.0, sell_score - 1.0)
            signal = "underperforming sector + stock already low (damage absorbed)"

    # --- Market regime cross: sector momentum reliability in bull vs bear ---
    if market_regime_score is not None:
        if excess >= 3 and market_regime_score <= 3:
            # Hot sector in bear market: sector pumps are short-lived (游资 dominates)
            score = max(0.0, score - 1.5)
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (bear market — sector momentum unreliable)"
        elif excess >= 3 and market_regime_score >= 7:
            # Hot sector in bull market: sector rotation has institutional follow-through
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — sector momentum more reliable)"

    # --- Industry valuation cross: early rotation vs late-stage rally ---
    if industry_stats is not None and excess >= 2:
        pe_vals = industry_stats.get("pe")
        if pe_vals and len(pe_vals) >= 5:
            try:
                median_pe = float(pd.Series(pe_vals).median())
                if 0 < median_pe <= 20:
                    # Cheap sector starting to move: classic early rotation setup
                    score = min(10.0, score + 1.5)
                    signal = signal + f" (cheap sector PE~{median_pe:.0f}x — early rotation)"
                elif median_pe >= 40:
                    # Expensive sector still rallying: late-stage, stretched valuation
                    sell_score = min(9.0, sell_score + 1.0)
                    signal = signal + f" (expensive sector PE~{median_pe:.0f}x — late-stage rally)"
            except Exception:
                pass

    # --- Concept momentum cross: concept board as sector amplifier or rotation signal ---
    concept_signal = None
    if best_concept_ret is not None:
        if excess >= 3.0 and best_concept_ret >= 8.0:
            # Hot sector + hot concept: retail capital is converging on dual catalysts
            score = min(10.0, score + 1.5)
            concept_signal = f"行业强+热概念(+{best_concept_ret:.1f}%) — 双重催化，散户资金共振"
        elif excess <= -2.0 and best_concept_ret >= 8.0:
            # Weak sector but hot concept board: rotation may be imminent, soften sell signal
            sell_score = max(0.0, sell_score - 0.5)
            concept_signal = f"行业弱+热概念(+{best_concept_ret:.1f}%) — 概念可能轮动至此行业"

    # --- Social heat cross: retail lag vs institutional sector rotation ---
    social_signal_im = None
    if social_dict is not None and excess >= 3.0:
        rank_pct_im = social_dict.get("rank_pct")
        if rank_pct_im is not None:
            rank_pct_im = float(rank_pct_im)
            if rank_pct_im > 50:
                # Strong sector + low social heat: institutional rotation in progress, retail hasn't noticed
                score = min(10.0, score + 1.5)
                social_signal_im = f"行业强+社交低热(rank={rank_pct_im:.0f}%) — 机构已在推板块散户未感知，轮动早期"
            elif rank_pct_im <= 5:
                # Strong sector + extreme social heat: retail FOMO at sector peak, rotation likely ending
                sell_score = min(10.0, sell_score + 1.5)
                social_signal_im = f"行业强+社交极热(rank={rank_pct_im:.0f}%) — 散户FOMO接盘，板块轮动尾声"

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "industry_ret_1m_pct": round(industry_ret_1m, 2),
            "market_ret_1m_pct": round(market, 2),
            "excess_pct": round(excess, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "market_regime_score": market_regime_score,
            "best_concept_ret": round(best_concept_ret, 2) if best_concept_ret is not None else None,
            "concept_signal": concept_signal,
            "social_signal": social_signal_im,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }




def score_northbound_actual(
    northbound_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    revision_df: Optional[pd.DataFrame] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    social_dict: Optional[dict] = None,
) -> dict:
    """
    Real 沪深港通 per-stock holding change (max 10).
    Distinct from score_northbound (which uses per-stock order flow).
    Uses actual share-count change over last 5 data points.
      change >= +5%   -> 10 pts
      change  0-+5%   -> linear 5-10 pts
      change -2-0%    -> linear 3-5 pts
      change <= -5%   ->  0 pts

    Context cross with 52w price position:
      NB reducing (< -2%) + low position (< 0.3)  -> likely passive redemption/ETF rebalancing -> reduce sell (-2.5)
      NB reducing (< -2%) + high position (> 0.7) -> active profit-taking exit -> amplify sell (+2)

    Momentum direction cross:
      NB buying (>= +2%) + 1m return <= -10%  -> smart money buying the dip, high conviction -> buy +2
      NB reducing (< -2%) + 1m return <= -10% -> foreign capital exiting declining stock -> sell +1.5

    Earnings revision cross: dual institutional confirmation (requires revision_df)
      NB increasing (>= +2%) + net upgrades >= 2   -> foreign + domestic institutions aligned -> buy +2
      NB reducing (<= -2%) + net downgrades <= -2  -> both institutional groups exiting       -> sell +2

    Industry momentum cross: contra-sector NB flow carries far more information
      NB buying (>= +2%) + industry excess <= -2%  -> single-stock pick against weak sector, buy +2
      NB buying (>= +2%) + industry excess >= +5%  -> trend-following, weaker signal, buy -1
      NB reducing (<= -2%) + industry excess >= +5% -> exiting a hot sector, sell +1.5

    Market regime cross (requires market_regime_score):
      NB increasing (>= +2%) + bull market (regime >= 7) -> foreign capital riding bull, amplify buy +1
      NB reducing (<= -2%) + bear market (regime <= 3)   -> systematic foreign exit in downturn, sell +1.5
    """
    if northbound_df is None or northbound_df.empty:
        return _neutral(10)

    hold_cols = [c for c in northbound_df.columns
                 if any(k in c for k in ["持股数量", "持仓量", "持股比例", "持有股数"])]
    if not hold_cols:
        return _neutral(10)

    series = pd.to_numeric(northbound_df[hold_cols[0]], errors="coerce").dropna()
    if len(series) < 2:
        return _neutral(10)

    current = float(series.iloc[-1])
    past    = float(series.iloc[max(0, len(series) - 5)])

    if past <= 0:
        return _neutral(10)

    change_pct = (current - past) / past * 100

    if change_pct >= 5:
        score = 10.0
    elif change_pct >= 0:
        score = 5.0 + change_pct / 5 * 5.0
    elif change_pct >= -2:
        score = 3.0 + (change_pct + 2) / 2 * 2.0
    elif change_pct >= -5:
        score = 3.0 * (change_pct + 5) / 3
    else:
        score = 0.0

    signal = ("strong inflow" if change_pct >= 3 else
              "inflow" if change_pct >= 0 else
              "slight outflow" if change_pct >= -2 else "outflow")

    # --- Sell score: NB holdings declining ---
    if change_pct <= -5:
        sell_score = 9.0
    elif change_pct <= -2:
        # linear: -2% -> 5pts, -5% -> 9pts
        sell_score = 5.0 + (-change_pct - 2) / 3 * 4.0
    elif change_pct <= 0:
        sell_score = 2.0
    else:
        sell_score = 0.0

    sell_score = round(min(9.0, sell_score), 1)

    # --- Context cross: price position × NB flow direction ---
    position = _get_price_position(price_df)
    if position is not None and change_pct < -2:
        if position < 0.3:
            # NB reducing at low price = likely passive redemption (ETF weight rebalancing)
            # Not a genuine conviction exit → weaken sell signal
            sell_score = max(0.0, sell_score - 2.5)
            signal = f"{signal} (at low price — passive redemption likely)"
        elif position > 0.7:
            # NB reducing at high price = active profit-taking → stronger sell signal
            sell_score = min(10.0, sell_score + 2.0)
            signal = f"{signal} (at high price — active exit, stronger sell)"

    sell_score = round(sell_score, 1)

    # --- Momentum direction cross: is NB flow contrarian or confirmatory? ---
    ret_1m = None
    try:
        if price_df is not None and len(price_df) >= 21 and "close" in price_df.columns:
            close = price_df["close"]
            cur_p  = float(close.iloc[-1])
            past_p = float(close.iloc[-21])
            if past_p > 0:
                ret_1m = (cur_p - past_p) / past_p * 100
    except Exception:
        pass

    if ret_1m is not None:
        if change_pct >= 2 and ret_1m <= -10:
            # Foreign money buying into a falling stock: high-conviction contrarian bottom signal
            score = min(10.0, score + 2.0)
            signal = signal + " (buying the dip — smart money contrarian)"
        elif change_pct <= -2 and ret_1m <= -10:
            # Foreign money also exiting a falling stock: fundamental sell conviction
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (selling into decline — fundamental exit)"

    # --- Earnings revision cross: dual institutional confirmation ---
    if revision_df is not None and not revision_df.empty:
        rating_cols = [c for c in revision_df.columns
                       if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
        if rating_cols:
            col_str = revision_df[rating_cols[0]].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
            net_rev = up - down
            if change_pct >= 2 and net_rev >= 2:
                # Foreign money buying + domestic analysts upgrading: highest-conviction buy
                score = min(10.0, score + 2.0)
                signal = signal + f" + analyst upgrades (net {net_rev:+d}) — NB × analyst consensus"
            elif change_pct <= -2 and net_rev <= -2:
                # Foreign money exiting + analysts cutting: dual institutional exit
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + f" + analyst downgrades (net {net_rev:+d}) — dual institutional exit"

    # --- Industry momentum cross: contra-sector NB flow = highest conviction ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess = industry_ret_1m - market_ret_1m
        if change_pct >= 2:
            if excess <= -2:
                # NB buying while sector is underperforming market: stock-specific high-conviction pick
                score = min(10.0, score + 2.0)
                signal = signal + f" (contra-sector: NB buying while industry excess {excess:+.1f}%)"
            elif excess >= 5:
                # NB buying into sector momentum: trend-following, information value lower
                score = max(0.0, score - 1.0)
                signal = signal + f" (sector-following: NB buying with hot sector {excess:+.1f}%)"
        elif change_pct <= -2 and excess >= 5:
            # NB reducing while sector is hot: foreign capital exiting a popular trade
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + f" (smart exit: NB reducing in hot sector {excess:+.1f}%)"

    # --- Market regime cross: NB flow conviction is amplified by market direction ---
    if market_regime_score is not None:
        if change_pct >= 2 and market_regime_score >= 7:
            # NB adding in a bull market: trend-aligned capital with real follow-through
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — 北向增仓顺势而为，信号增强)"
        elif change_pct <= -2 and market_regime_score <= 3:
            # NB reducing in a bear market: systematic risk-off exit, structural sell pressure
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 北向熊市减仓，系统性出逃)"

    # --- Social heat cross: A-share divergence between foreign and retail money ---
    social_signal_nb = None
    if social_dict is not None:
        rank_pct_nb = social_dict.get("rank_pct")
        if rank_pct_nb is not None:
            rank_pct_nb = float(rank_pct_nb)
            if change_pct <= -2 and rank_pct_nb <= 20:
                # NB reducing + high social heat: foreign money exits while domestic retail holds
                sell_score = min(10.0, sell_score + 2.0)
                social_signal_nb = f"北向减仓+社交高热(rank={rank_pct_nb:.0f}%) — A股散户接盘陷阱，外资出货"
            elif change_pct >= 2 and rank_pct_nb > 50:
                # NB increasing + low social heat: foreign money quietly accumulating before retail catches on
                score = min(10.0, score + 1.5)
                social_signal_nb = f"北向增仓+社交低热(rank={rank_pct_nb:.0f}%) — 外资悄悄买入散户未感知，早期机会"

    sell_score = round(sell_score, 1)
    return {
        "score": round(max(0.0, score), 1),
        "sell_score": sell_score,
        "max": 10,
        "details": {
            "latest_holding": round(current / 1e6, 1),
            "change_pct": round(change_pct, 2),
            "position_52w": round(position, 3) if position is not None else None,
            "ret_1m_pct": round(ret_1m, 2) if ret_1m is not None else None,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "market_regime_score": market_regime_score,
            "social_signal": social_signal_nb,
            "signal": signal,
            "sell_score": sell_score,
        },
    }




def score_earnings_revision(
    revision_df: Optional[pd.DataFrame],
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    visits_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    best_concept_ret: Optional[float] = None,
    social_dict: Optional[dict] = None,
) -> dict:
    """
    Analyst EPS forecast revision direction (max 10).
    Upward revisions → subsequent outperformance (strong academic evidence).
      net_up >= 3    -> 10 pts
      net_up  1-3    -> linear 7-10 pts
      net_up == 0    ->  5 pts (no coverage / neutral)
      net_down       -> linear 0-4 pts

    Context cross with 52w price position:
      Upgrades (net >= 2) + low position (< 0.3)   -> analysts discovered an underpriced stock -> buy +2
      Downgrades (net <= -2) + high position (> 0.7) -> analysts finally cutting an expensive stock -> sell +2
      Upgrades (net >= 1) + high position (> 0.7)   -> price-chasing upgrades, stock already moved -> sell +1

    Trailing growth cross (requires financial_df): are upgrades grounded in real results?
      Upgrades (net >= 2) + trailing profit growth >= 20% -> buy +1.5
        (analyst optimism validated by actual results — highest-conviction upgrade)
      Upgrades (net >= 2) + trailing profit growth < 0%   -> sell +1.5
        (analysts upgrading despite declining earnings — likely relationship-driven, not signal)

    Institutional visits cross: sell-side × buy-side dual confirmation (requires visits_df)
      Upgrades (net >= 2) + visit_count >= 5  -> buy-side AND sell-side both bullish -> buy +1.5
      Downgrades (net <= -2) + visit_count == 0 -> no buy-side interest + sell-side cutting -> sell +1.5
      Upgrades (net >= 2) + visit_count == 0   -> analysts upgrading but buy-side absent -> sell +1
        (possible relationship/IR-driven upgrade without real institutional conviction)

    Market regime cross (requires market_regime_score):
      Upgrades (net >= 2) + bull market (regime >= 7) -> buy +1 (牛市双击：EPS↑ × 估值扩张)
      Upgrades (net >= 2) + bear market (regime <= 3) -> buy -1 (上修也对抗不了整体去估值)
      Downgrades (net <= -2) + bear market            -> sell +1 (熊市下修雪上加霜)

    Industry background cross (requires industry_ret_1m, market_ret_1m):
      Upgrades (net >= 2) + industry underperforming (excess <= -2%) -> buy +2 (异类上修，区分度最高)
      Upgrades (net >= 2) + industry outperforming (excess >= +5%)   -> buy -1 (随波逐流，打折处理)
      Downgrades (net <= -2) + industry weak (excess <= -3%)         -> sell +1 (行业顺风下调确认)
    """
    if revision_df is None or revision_df.empty:
        return {"score": 5.0, "sell_score": 0.0, "max": 10,
                "details": {"up": 0, "down": 0, "net": 0, "signal": "no coverage", "sell_score": 0.0}}

    # Look for rating change direction columns
    rating_cols = [c for c in revision_df.columns
                   if any(k in c for k in ["评级变动", "方向", "上调", "下调", "rating"])]
    up_down_col = rating_cols[0] if rating_cols else None

    try:
        if up_down_col:
            col_str = revision_df[up_down_col].astype(str).str.lower()
            up   = int(col_str.str.contains("上调|upgrade|buy|strong").sum())
            down = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
        else:
            # Fallback: treat all rows as coverage with unknown direction
            return {"score": 5.0, "sell_score": 0.0, "max": 10,
                    "details": {"up": 0, "down": 0, "net": 0, "signal": "no direction data", "sell_score": 0.0}}
    except Exception:
        return _neutral(10)

    net = up - down

    if net >= 3:
        score = 10.0
    elif net >= 1:
        score = 7.0 + (net - 1) / 2 * 3.0
    elif net == 0:
        score = 5.0
    elif net >= -2:
        score = 5.0 + net / 2 * 5.0
    else:
        score = 0.0

    signal = ("strong upgrade" if net >= 3 else
              "upgraded" if net > 0 else
              "neutral" if net == 0 else
              "downgraded" if net >= -2 else "strong downgrade")

    # --- Sell score: analyst downgrades ---
    if net <= -3:
        sell_score = 9.0
    elif net <= -1:
        # linear: -1 -> 5pts, -3 -> 9pts
        sell_score = 5.0 + (-net - 1) / 2 * 4.0
    elif net < 0:
        sell_score = 2.0
    else:
        sell_score = 0.0

    sell_score = round(min(9.0, sell_score), 1)

    # --- Context cross: revision direction × 52w price position ---
    position = _get_price_position(price_df)
    if position is not None:
        if net >= 2 and position < 0.3:
            # Analysts upgrading a beaten-down stock: contra-consensus discovery → high conviction
            score = min(10.0, score + 2.0)
            signal = "analyst upgrades at low price (contra-consensus discovery)"
        elif net <= -2 and position > 0.7:
            # Analysts cutting targets on a high-priced stock: confirmed top
            sell_score = min(9.0, sell_score + 2.0)
            signal = "analyst downgrades at high price (confirmed top)"
        elif net >= 1 and position > 0.7:
            # Upgrades after the stock already ran: price-chasing, late to the party
            sell_score = min(9.0, sell_score + 1.0)
            signal = "analyst upgrades at high price (may be price-chasing)"

    # --- Trailing growth cross: are upgrades grounded in actual results? ---
    trailing_growth = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净利润增长率(%)", "净利润同比增长率(%)", "归母净利润增长率(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    trailing_growth = float(vals.iloc[0])
                break
        if trailing_growth is not None and net >= 2:
            if trailing_growth >= 20:
                # Forward upgrades validated by actual growth: highest-conviction upgrade
                score = min(10.0, score + 1.5)
                signal = signal + f" + trailing growth {trailing_growth:.1f}% (upgrade grounded in results)"
            elif trailing_growth < 0:
                # Analysts upgrading while actual earnings are declining: hollow signal
                sell_score = min(9.0, sell_score + 1.5)
                signal = signal + f" + trailing decline {trailing_growth:.1f}% (upgrade not backed by results)"

    # --- Institutional visits cross: sell-side × buy-side dual confirmation ---
    visit_count = None
    if visits_df is not None and not visits_df.empty:
        date_cols_v = [c for c in visits_df.columns if any(k in c for k in ["日期", "调研日期", "接待日期"])]
        visit_count = len(visits_df)
        if date_cols_v:
            try:
                _vc = visits_df.copy()
                _vc["_date"] = pd.to_datetime(_vc[date_cols_v[0]], errors="coerce")
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
                visit_count = int((_vc["_date"] >= cutoff).sum())
            except Exception:
                pass
        if net >= 2 and visit_count >= 5:
            # Sell-side upgrading + buy-side actively visiting: both institutional groups bullish
            score = min(10.0, score + 1.5)
            signal = signal + f" + {visit_count} institutional visits (sell-side + buy-side consensus)"
        elif net <= -2 and visit_count == 0:
            # Analysts cutting + zero buy-side interest: stock abandoned by all institutional players
            sell_score = min(9.0, sell_score + 1.5)
            signal = signal + " (downgrades + no institutional visits — fully abandoned)"
        elif net >= 2 and visit_count == 0:
            # Analysts upgrading but buy-side not visiting: possible IR-driven or relationship upgrade
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (upgrades without institutional visits — conviction questionable)"

    # --- Market regime cross: revision reliability differs in bull vs bear ---
    if market_regime_score is not None:
        if net >= 2 and market_regime_score >= 7:
            # Bull market: upgrades trigger multiple expansion on top of EPS growth (双击效应)
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — 牛市双击效应放大)"
        elif net >= 2 and market_regime_score <= 3:
            # Bear market: upgrades fighting against systematic de-rating
            score = max(0.0, score - 1.0)
            signal = signal + " (bear market — 上修对抗不了整体去估值)"
        elif net <= -2 and market_regime_score <= 3:
            # Downgrades in bear market: macro headwind amplifies fundamental deterioration
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (bear market — 熊市下修雪上加霜)"

    # --- Industry background cross: is this upgrade an anomaly or just sector tailwind? ---
    if industry_ret_1m is not None and market_ret_1m is not None:
        ind_excess = industry_ret_1m - market_ret_1m
        if net >= 2 and ind_excess <= -2:
            # Individual upgrade while sector is weak: analyst independently discovered value
            score = min(10.0, score + 2.0)
            signal = signal + f" (异类上修 — sector excess {ind_excess:+.1f}%, 区分度最高)"
        elif net >= 2 and ind_excess >= 5:
            # Individual upgrade when sector is already hot: likely just riding sector tailwind
            score = max(0.0, score - 1.0)
            signal = signal + f" (随波逐流 — sector excess {ind_excess:+.1f}%, 打折处理)"
        elif net <= -2 and ind_excess <= -3:
            # Downgrades in a weak sector: sector headwind confirms the cut
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + f" (行业顺风下调 — sector excess {ind_excess:+.1f}%)"

    # --- Concept cross: fundamental revision + theme catalyst = A-share dual catalyst ---
    if best_concept_ret is not None:
        if net >= 2 and best_concept_ret >= 8.0:
            # Analyst upgrades + hot concept board: fundamental and theme catalysts converge
            score = min(10.0, score + 2.0)
            signal = signal + f" + hot concept {best_concept_ret:+.1f}% — 基本面+题材双重催化，A股最强买入信号"
        elif net <= -2 and best_concept_ret >= 8.0:
            # Analyst cuts + hot concept: theme is hiding fundamental deterioration
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + f" + hot concept {best_concept_ret:+.1f}% — 题材掩盖业绩恶化，散户被热度迷惑"

    # --- Social heat cross: A-share early-discovery vs peak-consensus signal ---
    social_signal_er = None
    if social_dict is not None and net >= 2:
        rank_pct_er = social_dict.get("rank_pct")
        if rank_pct_er is not None:
            rank_pct_er = float(rank_pct_er)
            if rank_pct_er > 50:
                # Analyst upgrades + low social heat: institutional discovery before retail notices
                score = min(10.0, score + 2.0)
                social_signal_er = f"分析师上调+社交低热(rank={rank_pct_er:.0f}%) — 机构悄悄发现散户未感知，A股最佳早期买点"
            elif rank_pct_er <= 5:
                # Analyst upgrades + extreme social heat: too much consensus, likely at peak
                sell_score = min(9.0, sell_score + 1.0)
                social_signal_er = f"分析师上调+社交极热(rank={rank_pct_er:.0f}%) — 过度共识可能是顶部，反向警示"

    return {
        "score": round(max(0.0, min(10.0, score)), 1),
        "sell_score": round(min(9.0, sell_score), 1),
        "max": 10,
        "details": {
            "up_revisions":      up,
            "down_revisions":    down,
            "net_revisions":     net,
            "trailing_growth":   round(trailing_growth, 1) if trailing_growth is not None else None,
            "visit_count_90d":   visit_count,
            "position_52w":      round(position, 3) if position is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 2) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "social_signal":     social_signal_er,
            "signal":            signal,
            "sell_score":        round(min(9.0, sell_score), 1),
        },
    }


# ===========================================================================
# GROUP C — New behavioral / market-context factors
# ===========================================================================



def score_main_inflow(
    fund_flow_df: Optional[pd.DataFrame],
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


def score_market_relative_strength(
    price_df: Optional[pd.DataFrame],
    market_price_df: Optional[pd.DataFrame] = None,
) -> dict:
    """相对强弱因子 — stock 20d return minus CSI300 20d return.

    Stocks that outperform the market on a rolling 20d basis tend to
    continue outperforming (relative momentum). Distinct from absolute
    momentum (price_inertia) — controls for market-wide moves.

    Score: linear map excess_return in [-15%, +15%] -> [0, 10].
    Center at 0% excess = score 5.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(close) < 22:
            return _neutral(MAX)

        stock_ret = float(close.iloc[-1] / close.iloc[-21] - 1) * 100  # pct

        mkt_ret = 0.0
        if market_price_df is not None and "close" in market_price_df.columns:
            mkt_close = pd.to_numeric(market_price_df["close"], errors="coerce").dropna()
            if len(mkt_close) >= 21:
                mkt_ret = float(mkt_close.iloc[-1] / mkt_close.iloc[-21] - 1) * 100

        excess = stock_ret - mkt_ret

        # Map excess in [-15%, +15%] -> [0, 10]; center 0% -> 5
        score      = float(np.clip((excess + 15.0) / 30.0 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if excess >= 10:
            signal = f"strong market leader (+{excess:.1f}%)"
        elif excess >= 3:
            signal = f"market outperformer (+{excess:.1f}%)"
        elif excess >= -3:
            signal = f"in line with market ({excess:+.1f}%)"
        elif excess >= -10:
            signal = f"market underperformer ({excess:.1f}%)"
        else:
            signal = f"sharp underperformer ({excess:.1f}%) — potential laggard"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "stock_ret":  round(stock_ret, 2),
                "mkt_ret":    round(mkt_ret, 2),
                "excess_ret": round(excess, 2),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)




def score_sector_sympathy(
    code: str,
    industry: str,
    spot_df,            # full-market spot DataFrame from fetcher._get_spot_df()
    price_df=None,      # optional, reserved for future board-type detection
) -> dict:
    """
    Sector sympathy play score.

    Logic:
    1. Filter spot_df for stocks in the same industry, excluding self.
    2. Count stocks up > 5% (strong movers) and > 3% (moderate movers).
    3. Base score = f(count of movers, avg gain of movers).
    4. Board multiplier: 创业板 (300x) and 科创板 (688x) get 1.3x —
       higher retail beta, stronger sympathy effect.
    5. Sell score: if sector is down heavily (multiple -5%), flag reversal risk.

    Returns score 0-10 and sell_score 0-10.
    """
    MAX = 10

    # Guard: missing inputs → neutral
    if spot_df is None or (hasattr(spot_df, "empty") and spot_df.empty):
        return {"score": 5.0, "sell_score": 5.0, "max": MAX,
                "details": {"signal": "no spot data, neutral"}}
    if not industry or industry in ("", "未知", "其他"):
        return {"score": 5.0, "sell_score": 5.0, "max": MAX,
                "details": {"signal": "unknown industry, neutral"}}

    try:
        # Identify relevant columns
        industry_col = None
        for c in spot_df.columns:
            if "行业" in c or "industry" in c.lower():
                industry_col = c
                break
        code_col = None
        for c in spot_df.columns:
            if c in ("代码", "股票代码"):
                code_col = c
                break
        change_col = None
        for c in spot_df.columns:
            if "涨跌幅" in c or "change_pct" in c.lower() or "涨跌" in c:
                change_col = c
                break

        if industry_col is None or code_col is None or change_col is None:
            return {"score": 5.0, "sell_score": 5.0, "max": MAX,
                    "details": {"signal": "required columns missing, neutral"}}

        # Filter peers: same industry, exclude self
        peers = spot_df[
            (spot_df[industry_col].astype(str) == str(industry)) &
            (spot_df[code_col].astype(str).str.zfill(6) != code.zfill(6))
        ].copy()

        if peers.empty:
            return {"score": 5.0, "sell_score": 5.0, "max": MAX,
                    "details": {"signal": "no peers found, neutral"}}

        pct = pd.to_numeric(peers[change_col], errors="coerce").dropna()
        if pct.empty:
            return {"score": 5.0, "sell_score": 5.0, "max": MAX,
                    "details": {"signal": "no valid change_pct data, neutral"}}

        # --- Buy score ---
        strong_movers   = pct[pct > 5.0]
        moderate_movers = pct[pct > 3.0]
        strong_count    = len(strong_movers)
        moderate_count  = len(moderate_movers)

        if moderate_count > 0:
            avg_gain = float(min(float(moderate_movers.mean()), 20.0))
        else:
            avg_gain = 0.0

        raw_score = min(10.0, strong_count * 2.5 + moderate_count * 1.0 + avg_gain * 0.3)

        # Board multiplier
        if code.startswith("300") or code.startswith("301"):
            board, multiplier = "GEM", 1.3
        elif code.startswith("688") or code.startswith("689"):
            board, multiplier = "STAR", 1.3
        else:
            board, multiplier = "MAIN", 1.0

        score = round(min(10.0, raw_score * multiplier), 1)

        # --- Sell score (sector-wide weakness) ---
        weak_movers       = pct[pct < -5.0]
        moderate_weak     = pct[pct < -3.0]
        weak_count        = len(weak_movers)
        moderate_weak_cnt = len(moderate_weak)

        if moderate_weak_cnt > 0:
            avg_loss = float(min(abs(float(moderate_weak.mean())), 20.0))
        else:
            avg_loss = 0.0

        raw_sell   = min(10.0, weak_count * 2.5 + moderate_weak_cnt * 1.0 + avg_loss * 0.3)
        sell_score = round(min(10.0, raw_sell * multiplier), 1)

        # Signal label
        if strong_count >= 3:
            signal = f"strong sympathy play: {strong_count} stocks >5%, avg_gain={avg_gain:.1f}%"
        elif moderate_count >= 2:
            signal = f"moderate sympathy: {moderate_count} stocks >3%, avg_gain={avg_gain:.1f}%"
        elif weak_count >= 3:
            signal = f"sector weakness: {weak_count} stocks <-5%, sell pressure"
        else:
            signal = f"no clear sector trend (strong={strong_count}, weak={weak_count})"

        return {
            "score":      score,
            "sell_score": sell_score,
            "max":        MAX,
            "details": {
                "signal":          signal,
                "industry":        industry,
                "board":           board,
                "multiplier":      multiplier,
                "strong_count":    strong_count,
                "moderate_count":  moderate_count,
                "avg_gain":        round(avg_gain, 2),
                "weak_count":      weak_count,
                "sell_score":      sell_score,
            },
        }

    except Exception:
        return {"score": 5.0, "sell_score": 5.0, "max": MAX,
                "details": {"signal": "error, neutral"}}


# ===========================================================================
# score_overhead_resistance — 套牢盘压力 (chip distribution overhead resistance)
# ===========================================================================

