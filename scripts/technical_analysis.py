"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制
数据源：iFinD HTTP API（纯 requests，无需安装 SDK）
"""

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ==================== iFinD HTTP 客户端 ====================

_BASE_URL    = "https://quantapi.51ifind.com/api/v1"
_TOKEN_CACHE = {"access_token": None, "expires_at": 0}


def _get_access_token(force_refresh=False):
    now = time.time()
    if (
        not force_refresh
        and _TOKEN_CACHE["access_token"]
        and now < _TOKEN_CACHE["expires_at"]
    ):
        return _TOKEN_CACHE["access_token"]

    refresh_token = os.environ.get("IFIND_REFRESH_TOKEN", "").strip()
    if not refresh_token:
        raise RuntimeError(
            "❌ 环境变量 IFIND_REFRESH_TOKEN 未配置，"
            "请在 GitHub Secrets 中添加该变量"
        )

    url     = f"{_BASE_URL}/get_access_token"
    headers = {
        "Content-Type":  "application/json",
        "refresh_token": refresh_token,
    }
    try:
        resp = requests.post(url, headers=headers, timeout=15)
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        raise RuntimeError(f"❌ 获取 access_token 网络异常: {e}")

    token = body.get("data", {}).get("access_token")
    if not token:
        raise RuntimeError(f"❌ 获取 access_token 失败，接口返回: {body}")

    _TOKEN_CACHE["access_token"] = token
    _TOKEN_CACHE["expires_at"]   = now + 6 * 24 * 3600
    return token


def _ifind_post(endpoint, payload, access_token):
    url     = f"{_BASE_URL}/{endpoint}"
    headers = {
        "Content-Type":    "application/json",
        "access_token":    access_token,
        "Accept-Encoding": "gzip,deflate",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _fmt_code(code: str) -> str:
    """'600519' → '600519.SH'，'000858' → '000858.SZ'"""
    code = code.strip()
    if "." in code:
        return code
    if code.startswith("6") or code.startswith("5"):
        return f"{code}.SH"
    return f"{code}.SZ"


# ==================== 解析 iFinD 返回结构 ====================

def _parse_history_response(result: dict, fmt_code: str) -> pd.DataFrame:
    """
    解析 cmd_history_quotation 返回的 JSON。

    iFinD 标准结构（手册）：
      {
        "errorcode": 0,
        "tables": [
          {
            "thscode": "600519.SH",
            "table": {
              "time":   [...],
              "open":   [...],
              "high":   [...],
              "low":    [...],
              "close":  [...],
              "volume": [...]
            }
          }
        ]
      }

    兼容变体：
      - tables 是 dict（旧版）
      - table 是行列表而非列字典
      - 字段名大小写不一致
      - 时间字段名不是 time
    """
    errorcode = result.get("errorcode", -1)
    if errorcode != 0:
        raise ValueError(
            f"iFinD 接口错误 (errorcode={errorcode}): "
            f"{result.get('errmsg', '未知错误')}"
        )

    # ── 工具函数 ────────────────────────────────────────────

    def _align(lst, n):
        """截断或补 None，保证长度 == n"""
        lst = list(lst) if lst else []
        if len(lst) >= n:
            return lst[:n]
        return lst + [None] * (n - len(lst))

    def _build_df_from_col_dict(col_dict: dict) -> pd.DataFrame:
        """
        从列字典构建 DataFrame，自动探测时间/收盘/成交量字段名。
        兼容 iFinD 各版本返回的不同字段命名风格。
        """
        # 字段名全部转小写
        col_dict = {k.lower(): v for k, v in col_dict.items()}

        # DEBUG：打印实际字段名及首个值，便于排查（确认正常后可删除）
        print(
            f"  🔍 [DEBUG] table keys & samples: "
            + str({
                k: (v[0] if isinstance(v, list) and v else v)
                for k, v in list(col_dict.items())[:8]
            })
        )

        # ── 探测时间列 ──────────────────────────────────────
        TIME_ALIASES = [
            "time", "date", "datetime", "trading_date",
            "tradedate", "trade_date", "tdate", "tradingday",
            "tradeday", "trade_day", "date_time",
            "日期", "时间", "交易日期", "交易日",
        ]
        time_key = next(
            (a for a in TIME_ALIASES if col_dict.get(a)),
            None
        )

        if time_key is None:
            # 兜底：只取值为字符串列表的字段（时间值是字符串，价格是数字）
            for k, v in col_dict.items():
                if isinstance(v, list) and v and isinstance(v[0], str):
                    time_key = k
                    print(
                        f"  ⚠️  [WARN] 未找到标准时间字段，"
                        f"fallback 使用 '{k}'（值样本: {v[0]}）"
                    )
                    break

        if not time_key:
            raise ValueError(
                f"{fmt_code} 返回时间序列为空，"
                f"实际字段及样本值: "
                + str({
                    k: (v[0] if isinstance(v, list) and v else v)
                    for k, v in list(col_dict.items())[:6]
                })
            )

        time_list = col_dict[time_key]
        n = len(time_list)

        # ── 探测 close 列 ───────────────────────────────────
        CLOSE_ALIASES = [
            "close", "latest", "price", "close_price", "收盘价", "收盘",
        ]
        close_key = next(
            (a for a in CLOSE_ALIASES if col_dict.get(a)),
            "close"
        )

        # ── 探测 volume 列 ──────────────────────────────────
        VOL_ALIASES = [
            "volume", "vol", "turnovervolume", "turnover_volume",
            "成交量", "volume(手)", "volume(股)", "成交量(手)",
        ]
        vol_key  = next((a for a in VOL_ALIASES if col_dict.get(a)), None)
        vol_data = col_dict.get(vol_key, []) if vol_key else []

        return pd.DataFrame({
            "date":   time_list,
            "open":   _align(col_dict.get("open",  []), n),
            "high":   _align(col_dict.get("high",  []), n),
            "low":    _align(col_dict.get("low",   []), n),
            "close":  _align(col_dict.get(close_key, []), n),
            "volume": _align(vol_data, n),
        })

    # ── 定位 tables ─────────────────────────────────────────
    tables_raw = result.get("tables") or result.get("data")

    # ══ 形态 A：tables 是 list（手册标准格式）══════════════
    if isinstance(tables_raw, list):
        # 找到匹配 fmt_code 的条目
        stock_entry = None
        for item in tables_raw:
            if not isinstance(item, dict):
                continue
            if (item.get("thscode") or item.get("code", "")) == fmt_code:
                stock_entry = item
                break

        # 单股查询时列表只有一条，直接取
        if stock_entry is None:
            if len(tables_raw) == 1 and isinstance(tables_raw[0], dict):
                stock_entry = tables_raw[0]
            else:
                codes_found = [
                    i.get("thscode") or i.get("code", "?")
                    for i in tables_raw if isinstance(i, dict)
                ]
                raise ValueError(
                    f"返回列表中找不到 {fmt_code}，实际包含: {codes_found}"
                )

        # 标准结构：行情数据在 stock_entry["table"] 里（列字典）
        raw_table = stock_entry.get("table")

        if isinstance(raw_table, dict):
            return _build_df_from_col_dict(raw_table)

        if isinstance(raw_table, list) and raw_table:
            # 行列表：[{"time": "2024-01-02", "close": 10.5, ...}, ...]
            df = pd.DataFrame(raw_table)
            df.columns = [c.lower() for c in df.columns]
            if "time" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"time": "date"})
            if "vol" in df.columns and "volume" not in df.columns:
                df = df.rename(columns={"vol": "volume"})
            return df

        # 降级：行情列直接挂在 stock_entry 上（非标准）
        entry_lower = {k.lower(): v for k, v in stock_entry.items()}
        if any(a in entry_lower for a in ["time", "date", "datetime", "日期", "时间"]):
            return _build_df_from_col_dict(stock_entry)

        raise ValueError(
            f"{fmt_code} 的 table 字段为空或格式未知，"
            f"stock_entry keys: {list(stock_entry.keys())}"
        )

    # ══ 形态 B：tables 是 dict（旧版格式）══════════════════
    if isinstance(tables_raw, dict):
        inner = tables_raw.get("table", {})
        if fmt_code in inner:
            return _build_df_from_col_dict(inner[fmt_code])

        tables_lower = {k.lower(): v for k, v in tables_raw.items()}
        if any(a in tables_lower for a in ["time", "date", "datetime", "日期", "时间"]):
            return _build_df_from_col_dict(tables_raw)

        raise ValueError(
            f"返回数据中找不到 {fmt_code}，"
            f"实际 keys: {list(inner.keys() if inner else tables_raw.keys())}"
        )

    raise ValueError(
        f"无法识别的 tables 结构: {type(tables_raw)}，"
        f"原始内容前 200 字符: {str(tables_raw)[:200]}"
    )


# ==================== 数据获取 ====================

def get_stock_data(code, period="daily", count=120):
    period_map = {"daily": "D", "weekly": "W", "monthly": "M"}
    interval   = period_map.get(period, "D")

    fetch_count = count + 30
    end_date    = datetime.now()
    # A股每年约250个交易日，2.8倍缓冲确保拉取足够数据
    day_buffer  = {"D": 2.8, "W": 12.0, "M": 45.0}.get(interval, 2.8)
    start_date  = end_date - timedelta(days=int(fetch_count * day_buffer) + 60)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    fmt_code  = _fmt_code(code)

    payload = {
        "codes":      fmt_code,
        "indicators": "open,high,low,close,volume",
        "startdate":  start_str,
        "enddate":    end_str,
        "functionpara": {
            "Interval": interval,
            "CPS":      "2",
            "Fill":     "Previous",
            "Currency": "RMB",
        },
    }

    MAX_RETRIES  = 3
    RETRY_DELAYS = [15, 30, 60]
    last_error   = None
    result       = None

    for attempt in range(MAX_RETRIES):
        try:
            access_token = _get_access_token(force_refresh=(attempt > 0))
            result = _ifind_post("cmd_history_quotation", payload, access_token)
            break
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"  ⚠️  [{code}] 第 {attempt+1} 次请求失败，{wait}s 后重试… ({e})")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"❌ 获取 [{code}] 数据失败，已重试 {MAX_RETRIES} 次。"
                    f"最后错误: {last_error}"
                ) from last_error

    df = _parse_history_response(result, fmt_code)

    df["date"] = pd.to_datetime(df["date"])
    df = (
        df.sort_values("date")
          .drop_duplicates("date")
          .reset_index(drop=True)
          .set_index("date")
    )

    cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df   = df[cols].apply(pd.to_numeric, errors="coerce")
    df   = df.dropna(subset=["close"])

    if df.empty:
        raise ValueError(f"❌ [{code}] 清洗后数据为空，请检查代码或日期范围")
    if len(df) < 20:
        raise ValueError(
            f"❌ [{code}] 数据严重不足（仅 {len(df)} 条），无法计算技术指标"
        )
    if len(df) < 60:
        print(f"  ⚠️  [{code}] 数据较少（{len(df)} 条），MA60 等长周期指标精度下降")

    return df.tail(count)


# ==================== 技术指标计算 ====================

def calculate_ma(data, windows=None, periods=None):
    effective = windows if windows is not None else periods
    if effective is None:
        effective = [5, 10, 20, 60]
    for w in effective:
        data[f'MA{w}'] = data['close'].rolling(window=w, min_periods=1).mean()
    return data


def calculate_macd(data, fast=12, slow=26, signal=9):
    ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow, adjust=False).mean()
    data['DIF']  = ema_fast - ema_slow
    data['DEA']  = data['DIF'].ewm(span=signal, adjust=False).mean()
    data['MACD'] = 2 * (data['DIF'] - data['DEA'])
    return data


def calculate_rsi(data, period=14):
    delta    = data['close'].diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    rs          = avg_gain / avg_loss.replace(0, np.nan)
    data['RSI'] = 100 - (100 / (1 + rs))
    return data


def calculate_bollinger(data, window=None, num_std=None, period=None, std_dev=None):
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
    if 'volume' not in df.columns or len(df) < 2:
        return False
    if len(df) < n + 1:
        print(f"  ⚠️  volume_confirm: 数据仅 {len(df)} 条，不足 {n+1} 条，均量精度下降")
        avg_vol = df['volume'].iloc[:-1].mean()
    else:
        avg_vol = df['volume'].iloc[-n - 1:-1].mean()
    if avg_vol <= 0 or pd.isna(avg_vol):
        return False
    return bool(df['volume'].iloc[-1] > avg_vol * ratio)


def _safe_round(val, digits=2):
    try:
        if val is None or pd.isna(val):
            return None
        return round(float(val), digits)
    except Exception:
        return None


def _safe_int_volume(val):
    try:
        if val is None or pd.isna(val):
            return 0
        return int(float(val))
    except Exception:
        return 0


# ==================== 信号检测 ====================

def check_signals(data, cfg_or_code=None, name=None, config=None):
    if isinstance(cfg_or_code, dict):
        cfg  = cfg_or_code
        code = cfg.get("symbol", "")
        name = cfg.get("name",   "")
    else:
        code = cfg_or_code or ""
        cfg  = config or {}

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
            macd_cross_happened = False

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

    # ── 趋势判定（七档）──────────────────────────────────────
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

    if hasattr(latest.name, 'strftime'):
        date_str = latest.name.strftime('%Y-%m-%d')
    else:
        date_str = str(latest.name)[:10]

    return {
        'symbol':           code,
        'code':             code,
        'name':             name,
        'date':             date_str,
        'close':            close_price,
        'price':            close_price,
        'change_pct':       change_pct,
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
        'price_history':    [
            round(float(v), 2)
            for v in data['close'].tail(30).tolist()
            if pd.notna(v)
        ],
    }
