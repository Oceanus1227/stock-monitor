"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制
"""

import time
import pandas as pd
import numpy as np


# ==================== 数据获取 ====================

def get_stock_data(code, days=60):
    """
    从akshare获取A股历史数据（带重试机制）

    Args:
        code: str, 股票代码
        days: int, 获取历史天数

    Returns:
        DataFrame with OHLCV data
    """
    import akshare as ak
    from datetime import datetime, timedelta

    end_date   = datetime.now()
    start_date = end_date - timedelta(days=days + 30)

    start_str = start_date.strftime('%Y%m%d')
    end_str   = end_date.strftime('%Y%m%d')

    # ── 外层重试（替换 tenacity，行为更可控）────────────────
    MAX_RETRIES  = 3
    RETRY_DELAYS = [3, 8, 15]   # 递增等待，给服务器喘息时间
    last_error   = None

    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust="qfq"
            )
            break   # 成功则跳出

        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"  ⚠️  {code} 第{attempt + 1}次请求失败，{wait}s 后重试... ({e})")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"获取 {code} 数据失败，已重试 {MAX_RETRIES} 次: {last_error}"
                )

    # 空数据校验
    if df is None or df.empty:
        raise ValueError(f"无数据返回: {code}")

    if len(df) < 20:
        raise ValueError(f"数据严重不足 {code}: 仅 {len(df)} 条，无法计算指标")
    if len(df) < 60:
        print(f"  ⚠️  {code} 数据较少({len(df)}条)，MA60/长周期指标精度下降")

    # 重命名列
    df = df.rename(columns={
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume'
    })

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.set_index('date', inplace=True)

    available = [c for c in ['open', 'high', 'low', 'close', 'volume'] if c in df.columns]
    df = df[available].astype(float)

    return df


# ==================== 技术指标计算 ====================

def calculate_ma(data, periods=None):
    """
    计算移动平均线
    periods 默认 None，避免可变默认参数陷阱
    """
    if periods is None:
        periods = [5, 10, 20, 60]
    for period in periods:
        data[f'MA{period}'] = data['close'].rolling(window=period, min_periods=1).mean()
    return data


def calculate_macd(data, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow, adjust=False).mean()

    data['DIF']  = ema_fast - ema_slow
    data['DEA']  = data['DIF'].ewm(span=signal, adjust=False).mean()
    data['MACD'] = 2 * (data['DIF'] - data['DEA'])
    return data


def calculate_rsi(data, period=14):
    """
    计算RSI相对强弱指数
    使用标准 Wilder EWM（com=period-1），比 rolling mean 更准确
    """
    delta = data['close'].diff()
    gain  = delta.where(delta > 0, 0.0)
    loss  = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()

    rs           = avg_gain / avg_loss.replace(0, np.nan)
    data['RSI']  = 100 - (100 / (1 + rs))
    return data


def calculate_bollinger(data, period=20, std_dev=2):
    """计算布林带（含宽度和 %B）"""
    data['BOLL_MID']   = data['close'].rolling(window=period, min_periods=1).mean()
    data['BOLL_STD']   = data['close'].rolling(window=period, min_periods=1).std()
    data['BOLL_UPPER'] = data['BOLL_MID'] + (data['BOLL_STD'] * std_dev)
    data['BOLL_LOWER'] = data['BOLL_MID'] - (data['BOLL_STD'] * std_dev)

    data['BOLL_WIDTH'] = (
        (data['BOLL_UPPER'] - data['BOLL_LOWER']) / data['BOLL_MID']
    )
    data['BOLL_PCT_B'] = (
        (data['close'] - data['BOLL_LOWER']) /
        (data['BOLL_UPPER'] - data['BOLL_LOWER']).replace(0, np.nan)
    )
    return data


def calculate_kdj(data, n=9, m1=3, m2=3):
    """计算KDJ指标"""
    low_list  = data['low'].rolling(window=n, min_periods=1).min()
    high_list = data['high'].rolling(window=n, min_periods=1).max()

    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    rsv = rsv.replace([np.inf, -np.inf], 50).fillna(50)

    data['K'] = rsv.ewm(com=m1 - 1, adjust=False).mean()
    data['D'] = data['K'].ewm(com=m2 - 1, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    return data


# ==================== 辅助函数 ====================

def volume_confirm(df, n=20, ratio=1.5):
    """
    检查是否放量

    Args:
        df:    DataFrame with 'volume' column
        n:     int, 移动平均周期
        ratio: float, 放量倍数

    Returns:
        bool: 是否放量
    """
    if len(df) < n:
        return False

    vol_ma      = df["volume"].rolling(n).mean()
    current_vol = df["volume"].iloc[-1]
    avg_vol     = vol_ma.iloc[-1]

    if pd.isna(avg_vol) or avg_vol == 0:
        return False

    return bool(current_vol > ratio * avg_vol)


def _safe_round(val, digits=2):
    """
    统一处理 NaN/None 的安全取整，
    避免返回值里散落大量 if not pd.isna(...) 判断
    """
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return round(float(val), digits)
    except Exception:
        return None


# ==================== 信号检测 ====================

def check_signals(data, code, name, config=None):
    """
    检查交易信号

    Args:
        data:   DataFrame with technical indicators
        code:   str, 股票代码
        name:   str, 股票名称
        config: dict, 信号阈值配置（来自 config.yaml 的 signals 节）

    Returns:
        dict with signal information，无信号时返回 None
    """
    cfg       = config or {}
    rsi_ob    = cfg.get("rsi",    {}).get("overbought", 70)
    rsi_os    = cfg.get("rsi",    {}).get("oversold",   30)
    vol_ratio = cfg.get("volume", {}).get("ratio",      1.5)
    kdj_ob    = cfg.get("kdj",    {}).get("overbought", 80)
    kdj_os    = cfg.get("kdj",    {}).get("oversold",   20)

    if len(data) < 2:
        return None

    latest = data.iloc[-1]
    prev   = data.iloc[-2]

    signals = []
    score   = 0

    # ── 1. 均线突破 ──────────────────────────────────────────
    if prev['close'] < prev['MA5'] and latest['close'] > latest['MA5']:
        vol_ok = volume_confirm(data, ratio=vol_ratio)
        signals.append({
            'type':      '突破',
            'indicator': 'MA5',
            'desc':      '股价放量突破5日均线' if vol_ok else '股价突破5日均线',
            'strength':  '强' if vol_ok else '中等',
            'action':    '关注'
        })
        score += 2 if vol_ok else 1

    # 均线多头/空头排列
    if latest['MA5'] > latest['MA10'] > latest['MA20']:
        signals.append({
            'type':      '多头排列',
            'indicator': 'MA',
            'desc':      'MA5>MA10>MA20，均线多头排列',
            'strength':  '中等',
            'action':    '趋势向上'
        })
        score += 1
    elif latest['MA5'] < latest['MA10'] < latest['MA20']:
        signals.append({
            'type':      '空头排列',
            'indicator': 'MA',
            'desc':      'MA5<MA10<MA20，均线空头排列',
            'strength':  '中等',
            'action':    '趋势向下'
        })
        score -= 1

    # ── 2. MACD 金叉/死叉 ────────────────────────────────────
    if not pd.isna(prev['DIF']) and not pd.isna(prev['DEA']):
        if prev['DIF'] < prev['DEA'] and latest['DIF'] > latest['DEA']:
            above_zero = latest['DIF'] > 0
            signals.append({
                'type':      '金叉',
                'indicator': 'MACD',
                'desc':      f'MACD金叉（{"零轴上方" if above_zero else "零轴下方"}），动能转强',
                'strength':  '强' if above_zero else '中等',
                'action':    '买入信号'
            })
            score += 2 if above_zero else 1

        elif prev['DIF'] > prev['DEA'] and latest['DIF'] < latest['DEA']:
            below_zero = latest['DIF'] < 0
            signals.append({
                'type':      '死叉',
                'indicator': 'MACD',
                'desc':      f'MACD死叉（{"零轴下方" if below_zero else "零轴上方"}），动能转弱',
                'strength':  '强' if below_zero else '中等',
                'action':    '卖出信号'
            })
            score -= 2 if below_zero else 1

    # ── 3. RSI 超买/超卖 ─────────────────────────────────────
    if not pd.isna(latest['RSI']):
        if latest['RSI'] < rsi_os:
            signals.append({
                'type':      '超卖',
                'indicator': 'RSI',
                'desc':      f'RSI={latest["RSI"]:.1f}，进入超卖区间',
                'strength':  '中等',
                'action':    '关注反弹'
            })
            score += 1
        elif latest['RSI'] > rsi_ob:
            signals.append({
                'type':      '超买',
                'indicator': 'RSI',
                'desc':      f'RSI={latest["RSI"]:.1f}，进入超买区间',
                'strength':  '中等',
                'action':    '注意回调'
            })
            score -= 1

    # ── 4. 布林带突破 ────────────────────────────────────────
    if not pd.isna(latest['BOLL_UPPER']):
        vol_ok = volume_confirm(data, ratio=vol_ratio)
        if latest['close'] > latest['BOLL_UPPER']:
            signals.append({
                'type':      '放量突破上轨' if vol_ok else '突破上轨',
                'indicator': 'BOLL',
                'desc':      '股价放量突破布林带上轨，强势' if vol_ok else '股价突破布林带上轨',
                'strength':  '强' if vol_ok else '中等',
                'action':    '持有'
            })
            score += 2 if vol_ok else 1
        elif latest['close'] < latest['BOLL_LOWER']:
            signals.append({
                'type':      '跌破下轨',
                'indicator': 'BOLL',
                'desc':      '股价跌破布林带下轨，超跌',
                'strength':  '中等',
                'action':    '关注反弹'
            })
            score -= 1

    # ── 5. KDJ 金叉/死叉 ─────────────────────────────────────
    if not pd.isna(prev['K']) and not pd.isna(prev['D']):
        k_cross_up   = prev['K'] < prev['D'] and latest['K'] > latest['D']
        k_cross_down = prev['K'] > prev['D'] and latest['K'] < latest['D']

        if k_cross_up and latest['K'] < kdj_os:
            signals.append({
                'type':      '金叉',
                'indicator': 'KDJ',
                'desc':      f'KDJ低位金叉(K={latest["K"]:.1f})，短线反弹信号',
                'strength':  '强',
                'action':    '短线关注'
            })
            score += 2
        elif k_cross_up:
            signals.append({
                'type':      '金叉',
                'indicator': 'KDJ',
                'desc':      f'KDJ金叉(K={latest["K"]:.1f})',
                'strength':  '中等',
                'action':    '关注'
            })
            score += 1

        if k_cross_down and latest['K'] > kdj_ob:
            signals.append({
                'type':      '死叉',
                'indicator': 'KDJ',
                'desc':      f'KDJ高位死叉(K={latest["K"]:.1f})，短线回调风险',
                'strength':  '强',
                'action':    '注意风险'
            })
            score -= 2
        elif k_cross_down:
            signals.append({
                'type':      '死叉',
                'indicator': 'KDJ',
                'desc':      f'KDJ死叉(K={latest["K"]:.1f})',
                'strength':  '中等',
                'action':    '观望'
            })
            score -= 1

    # ── 无信号直接返回 ────────────────────────────────────────
    if not signals:
        return None

    # 趋势判定（五档）
    if score >= 4:
        trend = '强势'
    elif score >= 2:
        trend = '偏强'
    elif score <= -4:
        trend = '弱势'
    elif score <= -2:
        trend = '偏弱'
    else:
        trend = '震荡'

    return {
        'code':             code,
        'name':             name,
        'date':             str(latest.name),
        'price':            _safe_round(latest['close'], 2),
        'change_pct':       _safe_round(
                                (latest['close'] - prev['close']) / prev['close'] * 100
                                if prev['close'] != 0 else 0,
                                2
                            ),
        'volume':           int(latest['volume']) if not pd.isna(latest['volume']) else 0,
        'ma5':              _safe_round(latest['MA5'],        2),
        'ma10':             _safe_round(latest['MA10'],       2),
        'ma20':             _safe_round(latest['MA20'],       2),
        'rsi':              _safe_round(latest['RSI'],        2),
        'macd':             _safe_round(latest['MACD'],       4),
        'kdj_k':            _safe_round(latest['K'],          2),
        'kdj_d':            _safe_round(latest['D'],          2),
        'kdj_j':            _safe_round(latest['J'],          2),
        'boll_upper':       _safe_round(latest['BOLL_UPPER'], 2),
        'boll_lower':       _safe_round(latest['BOLL_LOWER'], 2),
        'boll_width':       _safe_round(latest.get('BOLL_WIDTH'), 4),
        'signals':          signals,
        'score':            score,
        'trend':            trend,
        'volume_confirmed': volume_confirm(data, ratio=vol_ratio),
    }
