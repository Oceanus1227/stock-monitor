"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制
"""

import pandas as pd
import numpy as np
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
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
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days + 20)
    
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date.strftime('%Y%m%d'),
        end_date=end_date.strftime('%Y%m%d'),
        adjust="qfq"
    )
    
    # 数据校验
    if df is None or df.empty:
        raise ValueError(f"No data returned for {code}")
    
    if len(df) < 60:
        raise ValueError(f"Insufficient data for {code}: only {len(df)} rows")
    
    # 重命名列
    df = df.rename(columns={
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume'
    })
    
    # 转换日期并排序
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.set_index('date', inplace=True)
    
    # 选择需要的列并转换类型
    df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    
    return df


def calculate_ma(data, periods=[5, 10, 20, 60]):
    """计算移动平均线"""
    for period in periods:
        data[f'MA{period}'] = data['close'].rolling(window=period, min_periods=1).mean()
    return data


def calculate_macd(data, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow, adjust=False).mean()
    
    data['DIF'] = ema_fast - ema_slow
    data['DEA'] = data['DIF'].ewm(span=signal, adjust=False).mean()
    data['MACD'] = 2 * (data['DIF'] - data['DEA'])
    return data


def calculate_rsi(data, period=14):
    """计算RSI相对强弱指数"""
    delta = data['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    
    rs = avg_gain / avg_loss.replace(0, np.nan)
    data['RSI'] = 100 - (100 / (1 + rs))
    return data


def calculate_bollinger(data, period=20, std_dev=2):
    """计算布林带"""
    data['BOLL_MID'] = data['close'].rolling(window=period, min_periods=1).mean()
    data['BOLL_STD'] = data['close'].rolling(window=period, min_periods=1).std()
    
    data['BOLL_UPPER'] = data['BOLL_MID'] + (data['BOLL_STD'] * std_dev)
    data['BOLL_LOWER'] = data['BOLL_MID'] - (data['BOLL_STD'] * std_dev)
    return data


def calculate_kdj(data, n=9, m1=3, m2=3):
    """计算KDJ指标"""
    low_list = data['low'].rolling(window=n, min_periods=1).min()
    high_list = data['high'].rolling(window=n, min_periods=1).max()
    
    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    rsv = rsv.replace([np.inf, -np.inf], 50).fillna(50)
    
    data['K'] = rsv.ewm(com=m1-1, adjust=False).mean()
    data['D'] = data['K'].ewm(com=m2-1, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    return data


def volume_confirm(df, n=20, ratio=1.5):
    """
    检查是否放量
    
    Args:
        df: DataFrame with 'volume' column
        n: int, 移动平均周期
        ratio: float, 放量倍数
    
    Returns:
        bool: 是否放量
    """
    if len(df) < n:
        return False
    
    vol_ma = df["volume"].rolling(n).mean()
    current_vol = df["volume"].iloc[-1]
    avg_vol = vol_ma.iloc[-1]
    
    if pd.isna(avg_vol) or avg_vol == 0:
        return False
    
    return bool(current_vol > ratio * avg_vol)


def check_signals(data, code, name, config=None):
    """
    检查交易信号
    
    Args:
        data: DataFrame with technical indicators
        code: str, 股票代码
        name: str, 股票名称
        config: dict, 信号阈值配置
    
    Returns:
        dict with signal information
    """
    if config is None:
        config = {
            'rsi': {'overbought': 70, 'oversold': 30},
            'volume': {'ratio': 1.5}
        }
    
    if len(data) < 2:
        return None
    
    latest = data.iloc[-1]
    prev = data.iloc[-2]
    
    signals = []
    score = 0
    
    # 1. 均线突破/回调
    if prev['close'] < prev['MA5'] and latest['close'] > latest['MA5']:
        if volume_confirm(data):
            signals.append({
                'type': '突破',
                'indicator': 'MA5',
                'desc': '股价放量突破5日均线',
                'strength': '强',
                'action': '关注'
            })
            score += 2
        else:
            signals.append({
                'type': '突破',
                'indicator': 'MA5',
                'desc': '股价突破5日均线',
                'strength': '中等',
                'action': '关注'
            })
            score += 1
    
    # 2. MACD金叉/死叉
    if not pd.isna(prev['DIF']) and not pd.isna(prev['DEA']):
        if prev['DIF'] < prev['DEA'] and latest['DIF'] > latest['DEA']:
            signals.append({
                'type': '金叉',
                'indicator': 'MACD',
                'desc': 'MACD金叉，动能转强',
                'strength': '强',
                'action': '买入信号'
            })
            score += 2
        elif prev['DIF'] > prev['DEA'] and latest['DIF'] < latest['DEA']:
            signals.append({
                'type': '死叉',
                'indicator': 'MACD',
                'desc': 'MACD死叉，动能转弱',
                'strength': '强',
                'action': '卖出信号'
            })
            score -= 2
    
    # 3. RSI超买/超卖
    rsi_config = config.get('rsi', {})
    if not pd.isna(latest['RSI']):
        if latest['RSI'] < rsi_config.get('oversold', 30):
            signals.append({
                'type': '超卖',
                'indicator': 'RSI',
                'desc': f'RSI={latest["RSI"]:.1f}，进入超卖区间',
                'strength': '中等',
                'action': '关注反弹'
            })
            score += 1
        elif latest['RSI'] > rsi_config.get('overbought', 70):
            signals.append({
                'type': '超买',
                'indicator': 'RSI',
                'desc': f'RSI={latest["RSI"]:.1f}，进入超买区间',
                'strength': '中等',
                'action': '注意回调'
            })
            score -= 1
    
    # 4. 布林带突破（带放量确认）
    if not pd.isna(latest['BOLL_UPPER']):
        if latest['close'] > latest['BOLL_UPPER']:
            if volume_confirm(data):
                signals.append({
                    'type': '放量突破上轨',
                    'indicator': 'BOLL',
                    'desc': '股价放量突破布林带上轨，强势',
                    'strength': '强',
                    'action': '持有'
                })
                score += 2
            else:
                signals.append({
                    'type': '突破上轨',
                    'indicator': 'BOLL',
                    'desc': '股价突破布林带上轨',
                    'strength': '中等',
                    'action': '持有'
                })
                score += 1
        elif latest['close'] < latest['BOLL_LOWER']:
            signals.append({
                'type': '跌破下轨',
                'indicator': 'BOLL',
                'desc': '股价跌破布林带下轨，超跌',
                'strength': '中等',
                'action': '关注反弹'
            })
            score -= 1
    
    if not signals:
        return None
    
    trend = '强势' if score >= 2 else ('弱势' if score <= -2 else '震荡')
    
    return {
        'code': code,
        'name': name,
        'date': str(latest.name) if hasattr(latest, 'name') else str(latest.get('date', '')),
        'price': round(latest['close'], 2),
        'change_pct': round((latest['close'] - prev['close']) / prev['close'] * 100, 2) if prev['close'] != 0 else 0,
        'volume': int(latest.get('volume', 0)),
        'ma5': round(latest['MA5'], 2),
        'ma10': round(latest['MA10'], 2),
        'ma20': round(latest['MA20'], 2),
        'rsi': round(latest['RSI'], 2) if not pd.isna(latest['RSI']) else None,
        'macd': round(latest['MACD'], 4) if not pd.isna(latest['MACD']) else None,
        'boll_upper': round(latest['BOLL_UPPER'], 2) if not pd.isna(latest.get('BOLL_UPPER')) else None,
        'boll_lower': round(latest['BOLL_LOWER'], 2) if not pd.isna(latest.get('BOLL_LOWER')) else None,
        'signals': signals,
        'score': score,
        'trend': trend,
        'volume_confirmed': volume_confirm(data)
    }
