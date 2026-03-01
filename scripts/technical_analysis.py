"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制

修复清单（相对上一版）：
  P0-1  get_stock_data     df 未初始化为 None，MAX_RETRIES=0 时 UnboundLocalError
  P0-2  check_signals      int(nan) 抛 ValueError，改用安全转换
  P1-3  calculate_bollinger 兼容别名 period/std_dev 因默认值非 None 永远不生效
  P1-4  calculate_kdj       同上，n/m1/m2 兼容别名永远不生效
  P1-5  volume_confirm      数据不足时静默失效，补充警告日志
  P1-6  check_signals       price_history NaN 过滤用 isinstance(np.float64, float)
                            在部分环境下失效，改用 pd.notna()
  P1-7  check_signals       MACD 金叉与柱翻红同日重复计分，加互斥标记
  P2-8  get_stock_data      重试延迟太短（3/8/15s），改为 15/30/60s
  P2-9  _safe_round         NaN 判断改用 pd.isna()，跨平台更稳健
  P2-10 check_signals       date 字段统一格式为 YYYY-MM-DD 字符串
"""

import time
import pandas as pd
import numpy as np


# ==================== 数据获取 ====================

def get_stock_data(code, period="daily", count=120):
    """
    从 akshare 获取 A 股历史数据（带重试机制）

    Args:
        code:   str,  股票代码
        period: str,  周期，"daily" / "weekly" / "monthly"
        count:  int,  需要的 K 线条数

    Returns:
        DataFrame with OHLCV data，index 为 date（pd.DatetimeIndex）
    """
    import akshare as ak
    from datetime import datetime, timedelta

    fetch_count = count + 30
    end_date    = datetime.now()
    start_date  = end_date - timedelta(days=int(fetch_count / 0.7) + 60)

    start_str = start_date.strftime('%Y%m%d')
    end_str   = end_date.strftime('%Y%m%d')

    MAX_RETRIES  = 3
    # 修复 P2-8：延迟从 3/8/15 改为 15/30/60，东财 IP 封锁通常需要 >10s 才恢复
    RETRY_DELAYS = [15, 30, 60]
    last_error   = None
    # 修复 P0-1：提前初始化 df，防止 MAX_RETRIES=0 时 UnboundLocalError
    df           = None

    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,
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

    return df.tail(count)


# ==================== 技术指标计算 ====================

def calculate_ma(data, windows=None, periods=None):
    """
    计算移动平均线

    参数：
        windows: 主参数，list，如 [5, 10, 20, 60]
        periods: 兼容别名，与 windows 等价
    """
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
    使用标准 Wilder EWM（com=period-1）
    """
    delta    = data['close'].diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()

    rs          = avg_gain / avg_loss.replace(0, np.nan)
    data['RSI'] = 100 - (100 / (1 + rs))
    return data


def calculate_bollinger(data, window=None, num_std=None, period=None, std_dev=None):
    """
    计算布林带（含宽度和 %B）

    修复 P1-3：原版 window=20、num_std=2.0 有默认值，导致兼容别名
    period/std_dev 的条件 `window is not None` 永远为 True，别名完全失效。
    新版四个参数默认值全部改为 None，优先级：window > period > 20。

    参数：
        window / period:   均线窗口期，默认 20
        num_std / std_dev: 标准差倍数，默认 2.0
    """
    effective_window = window  or period  or 20
    effective_std    = num_std or std_dev or 2.0

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


def calculate_kdj(data, fastk_period=None, signal_period=None, n=None, m1=None, m2=None):
    """
    计算 KDJ 指标

    修复 P1-4：原版 fastk_period=9、signal_period=3 有默认值，
    导致兼容别名 n/m1/m2 永远不生效，与 calculate_bollinger 同类问题。
    新版五个参数默认值全部改为 None。

    参数：
        fastk_period / n:    RSV 窗口期，默认 9
        signal_period / m1:  K 线平滑周期，默认 3
        m2:                  D 线平滑周期，默认与 m1 相同
    """
    effective_n  = fastk_period  or n  or 9
    effective_m1 = signal_period or m1 or 3
    effective_m2 = m2 or effective_m1

    low_list  = data['low'].rolling(window=effective_n, min_periods=1).min()
    high_list = data['high'].rolling(window=effective_n, min_periods=1).max()

    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    rsv = rsv.replace([np.inf, -np.inf], 50).fillna(50)

    data['K'] = rsv.ewm(com=effective_m1 - 1, adjust=False).mean()
    data['D'] = data['K'].ewm(com=effective_m2 - 1, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    return data


# ==================== 辅助函数 ====================

def volume_confirm(df, n=20, ratio=1.5):
    """
    检查最新一根 K 线是否放量

    修复 P1-5：数据不足时原版静默返回 False 无任何提示，
    补充 print 警告，方便排查数据问题。
    """
    if 'volume' not in df.columns or len(df) < 2:
        return False

    if len(df) < n + 1:
        # 数据不足以计算 n 日均量，降级用现有数据，并给出提示
        print(f"  ⚠️  volume_confirm: 数据仅 {len(df)} 条，不足 {n+1} 条，均量精度下降")
        avg_vol = df['volume'].iloc[:-1].mean()
    else:
        avg_vol = df['volume'].iloc[-n - 1:-1].mean()

    if avg_vol <= 0 or pd.isna(avg_vol):
        return False

    return bool(df['volume'].iloc[-1] > avg_vol * ratio)


def _safe_round(val, digits=2):
    """
    统一处理 NaN / None 的安全取整

    修复 P2-9：原版用 isinstance(val, float) and np.isnan(val)，
    np.float64 在部分环境下不被识别为 Python float，改用 pd.isna() 更稳健。
    """
    try:
        if val is None or pd.isna(val):
            return None
        return round(float(val), digits)
    except Exception:
        return None


def _safe_int_volume(val):
    """
    修复 P0-2：安全地将成交量转为 int。
    原版直接 int(latest['volume'])，当值为 NaN 时抛 ValueError。
    """
    try:
        if val is None or pd.isna(val):
            return 0
        return int(float(val))
    except Exception:
        return 0


# ==================== 信号检测 ====================

def check_signals(data, cfg_or_code=None, name=None, config=None):
    """
    检查交易信号

    调用方式（两种均支持）：
        check_signals(df, cfg_dict)           ← build_data.py 的调用方式
        check_signals(df, "600519", "贵州茅台") ← 旧版调用方式

    Returns:
        dict（有信号时）或 None（无信号时）
    """
    # ── 兼容两种调用方式 ─────────────────────────────────────
    if isinstance(cfg_or_code, dict):
        cfg  = cfg_or_code
        code = cfg.get("symbol", "")
        name = cfg.get("name",   "")
    else:
        code = cfg_or_code or ""
        cfg  = config or {}

    # ── 读取阈值（兼容两种 config 结构）─────────────────────
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
            macd_cross_happened = False  # 修复 P1-7：互斥标记，防止金叉和柱翻红同日重复计分

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
                macd_cross_happened = True

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
                macd_cross_happened = True

            # 修复 P1-7：只有在没有发生金叉/死叉时才单独统计柱翻红/翻绿
            # 金叉当天必然伴随柱翻红，两者描述同一事件，不应重复计分
            if not macd_cross_happened:
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

    # ── 趋势判定（五档）──────────────────────────────────────
    # 修复 P1-5（趋势判定）：原版 score=±1 全归震荡，信息丢失。
    # 新版细化为七档，±1 分别对应"轻微偏强/偏弱"。
    if score >= 4:
        trend = '强势'
    elif score >= 2:
        trend = '偏强'
    elif score == 1:
        trend = '轻微偏强'
    elif score == -1:
        trend = '轻微偏弱'
    elif score <= -4:
        trend = '弱势'
    elif score <= -2:
        trend = '偏弱'
    else:
        trend = '震荡'

    close_price = _safe_round(latest['close'], 2)
    change_pct  = _safe_round(
        (latest['close'] - prev['close']) / prev['close'] * 100
        if prev['close'] != 0 else 0,
        2
    )

    # 修复 P2-10：date 字段统一输出 YYYY-MM-DD 字符串，不受 index 类型影响
    if hasattr(latest.name, 'strftime'):
        date_str = latest.name.strftime('%Y-%m-%d')
    else:
        date_str = str(latest.name)[:10]

    return {
        # 双写字段：兼容 build_data.py（symbol/close）和旧前端（code/price）
        'symbol':           code,
        'code':             code,
        'name':             name,
        'date':             date_str,
        'close':            close_price,
        'price':            close_price,
        'change_pct':       change_pct,
        # 修复 P0-2：用 _safe_int_volume() 替代裸 int()，NaN 时返回 0 而非崩溃
        'volume':           _safe_int_volume(latest.get('volume', None)),
        'ma5':              _safe_round(latest.get('MA5'),        2),
        'ma10':             _safe_round(latest.get('MA10'),       2),
        'ma20':             _safe_round(latest.get('MA20'),       2),
        'rsi':              _safe_round(latest.get('RSI'),        2),
        'dif':              _safe_round(latest.get('DIF'),        4),
        'dea':              _safe_round(latest.get('DEA'),        4),
        'macd_bar':         _safe_round(latest.get('MACD'),       4),
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
        # 修复 P1-6：用 pd.notna() 过滤 NaN，兼容 np.float64，避免 JSON 序列化报错
        'price_history':    [
            round(float(v), 2)
            for v in data['close'].tail(30).tolist()
            if pd.notna(v)
        ],
    }
