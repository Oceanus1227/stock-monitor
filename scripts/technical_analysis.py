"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制
"""

import time
import pandas as pd
import numpy as np


# ==================== 数据获取 ====================

def get_stock_data(code, period="daily", count=120):
    """
    从 akshare 获取 A 股历史数据（带重试机制）

    修复：
    - 原版签名 get_stock_data(code, days=60)，
      build_data.py 调用时传的是 period= 和 count=，
      导致参数完全对不上，period 固定为 "daily"、count 固定为 60。
    - 新版签名改为 (code, period="daily", count=120)，
      与 build_data.py 的调用保持一致。
    - 同时把 period 透传给 akshare，支持 weekly / monthly。

    Args:
        code:   str,  股票代码
        period: str,  周期，"daily" / "weekly" / "monthly"
        count:  int,  需要的 K 线条数

    Returns:
        DataFrame with OHLCV data，index 为 date
    """
    import akshare as ak
    from datetime import datetime, timedelta

    # 多取 30 条作为缓冲，保证指标计算有足够数据
    fetch_count = count + 30
    end_date    = datetime.now()
    # 按交易日估算：1 自然日 ≈ 0.7 交易日，再加 60 天余量
    start_date  = end_date - timedelta(days=int(fetch_count / 0.7) + 60)

    start_str = start_date.strftime('%Y%m%d')
    end_str   = end_date.strftime('%Y%m%d')

    MAX_RETRIES  = 3
    RETRY_DELAYS = [3, 8, 15]
    last_error   = None

    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,          # 修复：原版硬编码 "daily"，现在透传参数
                start_date=start_str,
                end_date=end_str,
                adjust="qfq"
            )
            break

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

    if df is None or df.empty:
        raise ValueError(f"无数据返回: {code}")

    if len(df) < 20:
        raise ValueError(f"数据严重不足 {code}: 仅 {len(df)} 条，无法计算指标")
    if len(df) < 60:
        print(f"  ⚠️  {code} 数据较少({len(df)}条)，MA60/长周期指标精度下降")

    df = df.rename(columns={
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume'
    })

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    df.set_index('date', inplace=True)

    available = [c for c in ['open', 'high', 'low', 'close', 'volume'] if c in df.columns]
    df = df[available].astype(float)

    # 只返回最新 count 条，缓冲数据不暴露给调用方
    return df.tail(count)


# ==================== 技术指标计算 ====================

def calculate_ma(data, windows=None, periods=None):
    """
    计算移动平均线

    修复：
    - 原版参数名是 periods，build_data.py 传的是 windows=，静默使用默认值。
    - 新版主参数改为 windows，同时保留 periods 作为兼容别名。
    """
    # 兼容旧调用方式：calculate_ma(data, periods=[...])
    effective = windows if windows is not None else periods
    if effective is None:
        effective = [5, 10, 20, 60]

    for w in effective:
        data[f'MA{w}'] = data['close'].rolling(window=w, min_periods=1).mean()
    return data


def calculate_macd(data, fast=12, slow=26, signal=9):
    """计算 MACD 指标（DIF / DEA / MACD 柱）"""
    ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow, adjust=False).mean()

    data['DIF']  = ema_fast - ema_slow
    data['DEA']  = data['DIF'].ewm(span=signal, adjust=False).mean()
    data['MACD'] = 2 * (data['DIF'] - data['DEA'])
    return data


def calculate_rsi(data, period=14):
    """
    计算 RSI 相对强弱指数
    使用标准 Wilder EWM（com=period-1），比 rolling mean 更准确
    """
    delta    = data['close'].diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()

    rs          = avg_gain / avg_loss.replace(0, np.nan)
    data['RSI'] = 100 - (100 / (1 + rs))
    return data


def calculate_bollinger(data, window=20, num_std=2.0, period=None, std_dev=None):
    """
    计算布林带（含宽度和 %B）

    修复：
    - 原版参数名是 (period, std_dev)，build_data.py 传的是 (window=, num_std=)，
      静默使用默认值，配置文件里的自定义参数完全失效。
    - 新版主参数改为 window / num_std，同时保留旧名作为兼容别名。
    """
    # 兼容旧调用方式
    effective_window  = window  if window  is not None else (period  or 20)
    effective_std     = num_std if num_std is not None else (std_dev or 2.0)

    mid = data['close'].rolling(window=effective_window, min_periods=1).mean()
    std = data['close'].rolling(window=effective_window, min_periods=1).std()

    data['BOLL_MID']   = mid
    data['BOLL_STD']   = std
    data['BOLL_UPPER'] = mid + std * effective_std
    data['BOLL_LOWER'] = mid - std * effective_std

    band_width = (data['BOLL_UPPER'] - data['BOLL_LOWER']).replace(0, np.nan)
    data['BOLL_WIDTH'] = band_width / data['BOLL_MID']
    data['BOLL_PCT_B'] = (data['close'] - data['BOLL_LOWER']) / band_width
    return data


def calculate_kdj(data, fastk_period=9, signal_period=3, n=None, m1=None, m2=None):
    """
    计算 KDJ 指标

    修复：
    - 原版参数名是 (n, m1, m2)，build_data.py 传的是 (fastk_period=, signal_period=)，
      静默使用默认值，配置文件里的自定义参数完全失效。
    - 新版主参数改为 fastk_period / signal_period，同时保留旧名作为兼容别名。
    """
    # 兼容旧调用方式
    effective_n  = fastk_period  if fastk_period  is not None else (n  or 9)
    effective_m  = signal_period if signal_period is not None else (m1 or 3)

    low_list  = data['low'].rolling(window=effective_n, min_periods=1).min()
    high_list = data['high'].rolling(window=effective_n, min_periods=1).max()

    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    rsv = rsv.replace([np.inf, -np.inf], 50).fillna(50)

    data['K'] = rsv.ewm(com=effective_m - 1, adjust=False).mean()
    data['D'] = data['K'].ewm(com=effective_m - 1, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    return data


# ==================== 辅助函数 ====================

def volume_confirm(df, n=20, ratio=1.5):
    """
    检查最新一根 K 线是否放量

    修复：
    - 原版用 rolling(n).mean() 计算均量，会把当日成交量本身纳入计算，
      导致放量判断偏低。
    - 新版改为 iloc[-n-1:-1]，只取当日之前的 n 根，避免自我参照。
    """
    if 'volume' not in df.columns or len(df) < 2:
        return False

    avg_vol = df['volume'].iloc[-n - 1:-1].mean()
    if avg_vol <= 0 or np.isnan(avg_vol):
        return False

    return bool(df['volume'].iloc[-1] > avg_vol * ratio)


def _safe_round(val, digits=2):
    """
    统一处理 NaN / None 的安全取整，
    避免返回值里散落大量 if not pd.isna(...) 判断
    """
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return round(float(val), digits)
    except Exception:
        return None


# ==================== 信号检测 ====================

def check_signals(data, cfg_or_code=None, name=None, config=None):
    """
    检查交易信号

    修复（核心 Bug）：
    - 原版签名 check_signals(data, code, name, config=None)，
      build_data.py 调用时只传两个参数：check_signals(df, cfg)，
      导致 cfg 被错误地赋给 code，name 缺失直接 TypeError 崩溃。
    - 新版用 cfg_or_code 做兼容参数，自动区分两种调用方式：
        · check_signals(df, cfg)          ← build_data.py 的调用方式
        · check_signals(df, code, name)   ← 旧版调用方式（向后兼容）

    Args:
        data:        DataFrame，已计算好所有指标
        cfg_or_code: dict（来自 build_data.py）或 str 股票代码（旧调用）
        name:        str，股票名称（旧调用时使用）
        config:      dict，旧调用时的配置参数

    Returns:
        dict with signal information，无信号时返回 None
    """
    # ── 兼容两种调用方式 ─────────────────────────────────────
    if isinstance(cfg_or_code, dict):
        # 新调用：check_signals(df, cfg)
        cfg  = cfg_or_code
        code = cfg.get("symbol", "")
        name = cfg.get("name",   "")
    else:
        # 旧调用：check_signals(df, code, name, config)
        code = cfg_or_code or ""
        cfg  = config or {}

    # ── 读取阈值（兼容两种 config 结构）─────────────────────
    # 新结构（build_data.py）：cfg["rsi_overbought"]
    # 旧结构（原版）：         cfg["rsi"]["overbought"]
    def _get(flat_key, nested_section, nested_key, default):
        if flat_key in cfg:
            return cfg[flat_key]
        return cfg.get(nested_section, {}).get(nested_key, default)

    rsi_ob    = _get("rsi_overbought", "rsi",    "overbought", 70)
    rsi_os    = _get("rsi_oversold",   "rsi",    "oversold",   30)
    vol_ratio = _get("volume_ratio",   "volume", "ratio",      1.5)
    kdj_ob    = _get("kdj_overbought", "kdj",    "overbought", 80)
    kdj_os    = _get("kdj_oversold",   "kdj",    "oversold",   20)

    if len(data) < 2:
        return None

    latest = data.iloc[-1]
    prev   = data.iloc[-2]

    signals = []
    score   = 0

    # ── 1. 均线突破 ──────────────────────────────────────────
    if 'MA5' in data.columns:
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

    # 均线多头 / 空头排列
    ma_cols = ['MA5', 'MA10', 'MA20']
    if all(c in data.columns for c in ma_cols):
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

    # ── 2. MACD 金叉 / 死叉 ──────────────────────────────────
    if all(c in data.columns for c in ['DIF', 'DEA', 'MACD']):
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

            # MACD 柱翻红 / 翻绿
            if not pd.isna(prev['MACD']) and not pd.isna(latest['MACD']):
                if prev['MACD'] < 0 and latest['MACD'] >= 0:
                    signals.append({
                        'type':      'MACD柱翻红',
                        'indicator': 'MACD',
                        'desc':      'MACD柱由负转正，动能增强',
                        'strength':  '中等',
                        'action':    '关注'
                    })
                    score += 1
                elif prev['MACD'] > 0 and latest['MACD'] <= 0:
                    signals.append({
                        'type':      'MACD柱翻绿',
                        'indicator': 'MACD',
                        'desc':      'MACD柱由正转负，动能减弱',
                        'strength':  '中等',
                        'action':    '观望'
                    })
                    score -= 1

    # ── 3. RSI 超买 / 超卖 ───────────────────────────────────
    if 'RSI' in data.columns and not pd.isna(latest['RSI']):
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

    # ── 4. 布林带突破 ─────────────────────────────────────────
    if all(c in data.columns for c in ['BOLL_UPPER', 'BOLL_LOWER', 'BOLL_PCT_B']):
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

            # %B 回归（超卖后反弹）
            if not pd.isna(prev['BOLL_PCT_B']) and not pd.isna(latest['BOLL_PCT_B']):
                if prev['BOLL_PCT_B'] < 0.05 and latest['BOLL_PCT_B'] >= 0.05:
                    signals.append({
                        'type':      '%B回归',
                        'indicator': 'BOLL',
                        'desc':      '布林 %B 回归，超卖后反弹',
                        'strength':  '中等',
                        'action':    '关注'
                    })
                    score += 1

    # ── 5. KDJ 金叉 / 死叉 ───────────────────────────────────
    if all(c in data.columns for c in ['K', 'D', 'J']):
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
        'price':            _safe_round(latest['close'],      2),
        'change_pct':       _safe_round(
                                (latest['close'] - prev['close']) / prev['close'] * 100
                                if prev['close'] != 0 else 0,
                                2
                            ),
        'volume':           int(latest['volume']) if not pd.isna(latest['volume']) else 0,
        'ma5':              _safe_round(latest.get('MA5'),        2),
        'ma10':             _safe_round(latest.get('MA10'),       2),
        'ma20':             _safe_round(latest.get('MA20'),       2),
        'rsi':              _safe_round(latest.get('RSI'),        2),
        'macd':             _safe_round(latest.get('MACD'),       4),
        'kdj_k':            _safe_round(latest.get('K'),          2),
        'kdj_d':            _safe_round(latest.get('D'),          2),
        'kdj_j':            _safe_round(latest.get('J'),          2),
        'boll_upper':       _safe_round(latest.get('BOLL_UPPER'), 2),
        'boll_lower':       _safe_round(latest.get('BOLL_LOWER'), 2),
        'boll_width':       _safe_round(latest.get('BOLL_WIDTH'), 4),
        'signals':          signals,
        'score':            score,
        'trend':            trend,
        'volume_confirmed': volume_confirm(data, ratio=vol_ratio),
    }
