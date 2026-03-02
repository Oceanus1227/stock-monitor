"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制
数据源：iFinD HTTP API（纯 requests，无需安装 SDK）
"""

import os
import json
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

    iFinD 实际结构（已通过 DEBUG 确认）：
      {
        "tables": [
          {
            "thscode": "688372.SH",
            "time":  ["2025-02-05", ...],   ← 时间序列在顶层
            "table": {                       ← 价格序列在 table 里
              "open":   [...],
              "high":   [...],
              "low":    [...],
              "close":  [...],
              "volume": [...]
            }
          }
        ]
      }
    """
    errorcode = result.get("errorcode", -1)
    if errorcode != 0:
        raise ValueError(
            f"iFinD 接口错误 (errorcode={errorcode}): "
            f"{result.get('errmsg', '未知错误')}"
        )

    def _align(lst, n):
        """截断或补 None，保证长度 == n"""
        lst = list(lst) if lst else []
        if len(lst) >= n:
            return lst[:n]
        return lst + [None] * (n - len(lst))

    def _build_from_entry(entry: dict) -> pd.DataFrame:
        entry_lower = {k.lower(): v for k, v in entry.items()}

        TIME_ALIASES = [
            "time", "date", "datetime", "trading_date",
            "tradedate", "trade_date", "tdate", "tradingday",
            "tradeday", "trade_day", "date_time",
            "日期", "时间", "交易日期", "交易日",
        ]
        time_key  = next((a for a in TIME_ALIASES if entry_lower.get(a)), None)
        time_list = entry_lower.get(time_key, []) if time_key else []

        raw_table = entry_lower.get("table", {})
        if isinstance(raw_table, dict):
            table_lower = {k.lower(): v for k, v in raw_table.items()}
        else:
            table_lower = {}

        def _get_col(name):
            return table_lower.get(name) or entry_lower.get(name) or []

        open_list   = _get_col("open")
        high_list   = _get_col("high")
        low_list    = _get_col("low")
        close_list  = _get_col("close")

        VOL_ALIASES = [
            "volume", "vol", "turnovervolume", "turnover_volume",
            "成交量", "volume(手)", "volume(股)", "成交量(手)",
        ]
        vol_key  = next(
            (a for a in VOL_ALIASES if table_lower.get(a) or entry_lower.get(a)),
            None
        )
        vol_list = (
            table_lower.get(vol_key) or entry_lower.get(vol_key) or []
        ) if vol_key else []

        if not time_list:
            time_key = next(
                (a for a in TIME_ALIASES if table_lower.get(a)), None
            )
            time_list = table_lower.get(time_key, []) if time_key else []

        if not time_list:
            for k, v in entry_lower.items():
                if isinstance(v, list) and v and isinstance(v[0], str):
                    time_list = v
                    print(f"  ⚠️  [WARN] fallback 时间字段: '{k}'（样本: {v[0]}）")
                    break
            if not time_list:
                for k, v in table_lower.items():
                    if isinstance(v, list) and v and isinstance(v[0], str):
                        time_list = v
                        print(f"  ⚠️  [WARN] fallback 时间字段(table): '{k}'（样本: {v[0]}）")
                        break

        if not time_list:
            raise ValueError(
                f"{fmt_code} 无法找到时间序列，"
                f"entry keys={list(entry.keys())}，"
                f"table keys={list(raw_table.keys()) if isinstance(raw_table, dict) else '非dict'}"
            )

        n = len(time_list)
        return pd.DataFrame({
            "date":   time_list,
            "open":   _align(open_list,  n),
            "high":   _align(high_list,  n),
            "low":    _align(low_list,   n),
            "close":  _align(close_list, n),
            "volume": _align(vol_list,   n),
        })

    tables_raw = result.get("tables") or result.get("data")

    if isinstance(tables_raw, list):
        stock_entry = None
        for item in tables_raw:
            if not isinstance(item, dict):
                continue
            if (item.get("thscode") or item.get("code", "")) == fmt_code:
                stock_entry = item
                break

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

        raw_table = stock_entry.get("table")
        if isinstance(raw_table, list) and raw_table and isinstance(raw_table[0], dict):
            df = pd.DataFrame(raw_table)
            df.columns = [c.lower() for c in df.columns]
            if "time" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"time": "date"})
            if "vol" in df.columns and "volume" not in df.columns:
                df = df.rename(columns={"vol": "volume"})
            if "date" in df.columns and len(df) > 1:
                return df

        return _build_from_entry(stock_entry)

    if isinstance(tables_raw, dict):
        inner = tables_raw.get("table", {})
        if fmt_code in inner:
            sub = inner[fmt_code]
            if isinstance(sub, dict):
                return _build_from_entry(sub)
        return _build_from_entry(tables_raw)

    raise ValueError(
        f"无法识别的 tables 结构: {type(tables_raw)}，"
        f"原始内容前 200 字符: {str(tables_raw)[:200]}"
    )


# ==================== 缓存配置（方案一）====================

CACHE_DIR = "cache/stock_data"
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_ttl(period: str) -> timedelta:
    """缓存有效期：收盘后才算当天数据完整"""
    now          = datetime.now()
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if period == "daily":
        # 收盘后：1 小时内有效；收盘前：沿用昨天缓存
        return timedelta(hours=1) if now > market_close else timedelta(hours=20)
    if period == "weekly":
        return timedelta(days=2)
    if period == "monthly":
        return timedelta(days=7)
    return timedelta(hours=1)


def _cache_path(code: str, period: str) -> str:
    return os.path.join(CACHE_DIR, f"{code}_{period}.parquet")


def _meta_path(code: str, period: str) -> str:
    return os.path.join(CACHE_DIR, f"{code}_{period}.meta.json")


def _is_cache_fresh(code: str, period: str) -> bool:
    mp = _meta_path(code, period)
    cp = _cache_path(code, period)
    if not os.path.exists(mp) or not os.path.exists(cp):
        return False
    try:
        with open(mp) as f:
            meta = json.load(f)
        saved_at = datetime.fromisoformat(meta["saved_at"])
        return (datetime.now() - saved_at) < _cache_ttl(period)
    except Exception:
        return False


def _save_cache(df: pd.DataFrame, code: str, period: str):
    try:
        df.to_parquet(_cache_path(code, period))
        with open(_meta_path(code, period), "w") as f:
            json.dump({
                "saved_at": datetime.now().isoformat(),
                "rows":     len(df),
            }, f)
    except Exception as e:
        print(f"  ⚠️  [{code}] 缓存写入失败（不影响运行）: {e}")


def _load_cache(code: str, period: str) -> pd.DataFrame:
    return pd.read_parquet(_cache_path(code, period))


# ==================== 数据获取（方案一 + 三）====================

def _clean_df(df: pd.DataFrame, count: int) -> pd.DataFrame:
    """解析后统一清洗"""
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
    return df.tail(count)


def _build_payload(fmt_codes: str, period: str, count: int) -> dict:
    """构造 iFinD 请求 payload"""
    period_map  = {"daily": "D", "weekly": "W", "monthly": "M"}
    interval    = period_map.get(period, "D")
    fetch_count = count + 30
    end_date    = datetime.now()
    day_buffer  = {"D": 2.8, "W": 12.0, "M": 45.0}.get(interval, 2.8)
    start_date  = end_date - timedelta(days=int(fetch_count * day_buffer) + 60)

    return {
        "codes":      fmt_codes,
        "indicators": "open,high,low,close,volume",
        "startdate":  start_date.strftime("%Y-%m-%d"),
        "enddate":    end_date.strftime("%Y-%m-%d"),
        "functionpara": {
            "Interval": interval,
            "CPS":      "2",
            "Fill":     "Previous",
            "Currency": "RMB",
        },
    }


def get_batch_stock_data(
    codes:     list,
    period:    str  = "daily",
    count:     int  = 120,
    use_cache: bool = True,
) -> dict:
    """
    批量获取多只股票数据，自动利用本地缓存（方案一 + 三）。

    返回: {code_str: pd.DataFrame}，失败的股票不在字典里。

    流程：
      1. 检查每只股票本地缓存是否新鲜
      2. 新鲜的直接读缓存，其余合并成 1 次 API 请求
      3. 解析结果后写入缓存
    """
    result     = {}
    need_fetch = []

    # ── 步骤 1：缓存检查 ────────────────────────────────────
    for code in codes:
        if use_cache and _is_cache_fresh(code, period):
            try:
                df = _load_cache(code, period)
                result[code] = df.tail(count)
                print(f"  📦 [{code}] 命中本地缓存，跳过 API")
            except Exception as e:
                print(f"  ⚠️  [{code}] 缓存读取失败，重新拉取: {e}")
                need_fetch.append(code)
        else:
            need_fetch.append(code)

    if not need_fetch:
        print(f"  ✅ 全部 {len(codes)} 只股票命中缓存，本次 API 调用次数: 0")
        return result

    # ── 步骤 2：一次批量 API 请求（方案三）─────────────────
    fmt_codes_str = ",".join(_fmt_code(c) for c in need_fetch)
    print(f"  🌐 批量请求 {len(need_fetch)} 只股票（1 次 API 调用）: {fmt_codes_str}")

    payload      = _build_payload(fmt_codes_str, period, count)
    MAX_RETRIES  = 3
    RETRY_DELAYS = [15, 30, 60]
    raw_result   = None
    last_error   = None

    for attempt in range(MAX_RETRIES):
        try:
            access_token = _get_access_token(force_refresh=(attempt > 0))
            raw_result   = _ifind_post("cmd_history_quotation", payload, access_token)
            break
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"  ⚠️  批量请求第 {attempt+1} 次失败，{wait}s 后重试… ({e})")
                time.sleep(wait)

    if raw_result is None:
        print(f"  ❌ 批量 API 请求失败，已重试 {MAX_RETRIES} 次。最后错误: {last_error}")
        return result   # 返回已有缓存部分，不抛异常

    # ── 步骤 3：逐只解析 + 写缓存 ──────────────────────────
    for code in need_fetch:
        fmt_code = _fmt_code(code)
        try:
            df = _parse_history_response(raw_result, fmt_code)
            df = _clean_df(df, count)

            if df.empty:
                print(f"  ⚠️  [{code}] 清洗后数据为空，跳过")
                continue
            if len(df) < 20:
                print(f"  ⚠️  [{code}] 数据不足 20 条（{len(df)} 条），跳过")
                continue
            if len(df) < 60:
                print(f"  ⚠️  [{code}] 数据较少（{len(df)} 条），长周期指标精度下降")

            _save_cache(df, code, period)
            result[code] = df
            print(f"  ✅ [{code}] 解析成功，{len(df)} 条，缓存已更新")

        except Exception as e:
            print(f"  ❌ [{code}] 解析失败: {e}")

    return result


def get_stock_data(code: str, period: str = "daily", count: int = 120) -> pd.DataFrame:
    """
    保持向后兼容的单只股票接口。
    内部调用 get_batch_stock_data()，享受缓存 + 批量逻辑。
    """
    batch = get_batch_stock_data([code], period=period, count=count)
    if code not in batch:
        raise ValueError(f"❌ [{code}] 数据获取失败")
    return batch[code]


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
# ==================== 飞书卡片信号判定（追加在文件末尾）====================

def check_buy_signals_card(df):
    """为飞书卡片生成买入信号 dict，列名适配本项目"""
    latest = df.iloc[-1]
    prev   = df.iloc[-2]
    signals = {}

    # MA
    ma_golden_cross = prev['MA5'] <= prev['MA20'] and latest['MA5'] > latest['MA20']
    ma_bullish      = (latest['MA5'] > latest['MA10'] > latest['MA20'])
    bounce_ma20     = prev['close'] <= prev['MA20'] and latest['close'] > latest['MA20']
    ma_hit   = ma_golden_cross or ma_bullish or bounce_ma20
    ma_parts = []
    if ma_golden_cross: ma_parts.append('MA5金叉MA20')
    if ma_bullish:      ma_parts.append('多头排列')
    if bounce_ma20:     ma_parts.append('站上MA20')
    signals['MA'] = {'hit': ma_hit, 'desc': '、'.join(ma_parts) or '均线无信号'}

    # MACD（你的列名是 DIF / DEA）
    macd_golden     = prev['DIF'] <= prev['DEA'] and latest['DIF'] > latest['DEA']
    macd_above_zero = macd_golden and latest['DIF'] > 0
    bottom_div      = (latest['close'] < df['close'].iloc[-10:-1].min() and
                       latest['DIF'] > df['DIF'].iloc[-10:-1].min())
    macd_hit   = macd_golden or bottom_div
    macd_parts = []
    if macd_above_zero:  macd_parts.append('零轴上金叉⭐')
    elif macd_golden:    macd_parts.append('MACD金叉')
    if bottom_div:       macd_parts.append('底背离')
    signals['MACD'] = {'hit': macd_hit, 'desc': '、'.join(macd_parts) or '动能无信号'}

    # RSI
    rsi = latest['RSI']
    rsi_oversold = rsi < 30
    rsi_rebound  = prev['RSI'] < 30 and rsi >= 30
    rsi_hit = rsi_oversold or rsi_rebound
    if rsi < 20:          rsi_desc = f'严重超卖 RSI={rsi:.0f}'
    elif rsi_rebound:     rsi_desc = f'从超卖回升 RSI={rsi:.0f} ⭐'
    elif rsi_oversold:    rsi_desc = f'超卖区间 RSI={rsi:.0f}'
    else:                 rsi_desc = f'RSI={rsi:.0f} 无信号'
    signals['RSI'] = {'hit': rsi_hit, 'desc': rsi_desc}

    # KDJ
    k, d, j = latest['K'], latest['D'], latest['J']
    kdj_low_golden = prev['K'] <= prev['D'] and k > d and k < 30
    j_oversold     = j < 0
    kdj_hit   = kdj_low_golden or j_oversold
    kdj_parts = []
    if kdj_low_golden: kdj_parts.append(f'低位金叉 K={k:.0f}')
    if j_oversold:     kdj_parts.append(f'J值超卖={j:.0f}')
    signals['KDJ'] = {'hit': kdj_hit, 'desc': '、'.join(kdj_parts) or f'K={k:.0f} 无信号'}

    # 布林带（你的列名是 BOLL_UPPER / BOLL_MID / BOLL_LOWER）
    touch_lower  = latest['close'] <= latest['BOLL_LOWER'] * 1.01
    break_up_mid = prev['close'] < prev['BOLL_MID'] and latest['close'] >= latest['BOLL_MID']
    boll_hit   = touch_lower or break_up_mid
    boll_parts = []
    if touch_lower:  boll_parts.append('触碰下轨超卖')
    if break_up_mid: boll_parts.append('突破布林中轨')
    signals['BOLL'] = {'hit': boll_hit, 'desc': '、'.join(boll_parts) or '布林无信号'}

    return signals


def check_sell_signals_card(df):
    """为飞书卡片生成卖出信号 dict，列名适配本项目"""
    latest = df.iloc[-1]
    prev   = df.iloc[-2]
    signals = {}

    # MA
    ma_dead_cross = prev['MA5'] >= prev['MA20'] and latest['MA5'] < latest['MA20']
    ma_bearish    = (latest['MA5'] < latest['MA10'] < latest['MA20'])
    break_ma20    = prev['close'] >= prev['MA20'] and latest['close'] < latest['MA20']
    ma_hit   = ma_dead_cross or ma_bearish or break_ma20
    ma_parts = []
    if ma_dead_cross: ma_parts.append('MA5死叉MA20')
    if ma_bearish:    ma_parts.append('空头排列')
    if break_ma20:    ma_parts.append('跌破MA20支撑')
    signals['MA'] = {'hit': ma_hit, 'desc': '、'.join(ma_parts) or '均线无异常'}

    # MACD（你的列名是 DIF / DEA）
    macd_dead  = prev['DIF'] >= prev['DEA'] and latest['DIF'] < latest['DEA']
    below_zero = macd_dead and latest['DIF'] < 0
    top_div    = (latest['close'] > df['close'].iloc[-10:-1].max() and
                  latest['DIF'] < df['DIF'].iloc[-10:-1].max())
    macd_hit   = macd_dead or top_div
    macd_parts = []
    if below_zero:  macd_parts.append('零轴下死叉⚠️')
    elif macd_dead: macd_parts.append('MACD死叉')
    if top_div:     macd_parts.append('顶背离')
    signals['MACD'] = {'hit': macd_hit, 'desc': '、'.join(macd_parts) or '动能无异常'}

    # RSI
    rsi = latest['RSI']
    rsi_overbought = rsi > 70
    rsi_pullback   = prev['RSI'] > 70 and rsi < 70
    rsi_hit = rsi_overbought or rsi_pullback
    if rsi > 80:          rsi_desc = f'严重超买 RSI={rsi:.0f}'
    elif rsi_pullback:    rsi_desc = f'从超买回落 RSI={rsi:.0f} ⚠️'
    elif rsi_overbought:  rsi_desc = f'超买区间 RSI={rsi:.0f}'
    else:                 rsi_desc = f'RSI={rsi:.0f} 无异常'
    signals['RSI'] = {'hit': rsi_hit, 'desc': rsi_desc}

    # KDJ
    k, d, j = latest['K'], latest['D'], latest['J']
    kdj_high_dead = prev['K'] >= prev['D'] and k < d and k > 70
    j_overbought  = j > 100
    kdj_hit   = kdj_high_dead or j_overbought
    kdj_parts = []
    if kdj_high_dead: kdj_parts.append(f'高位死叉 K={k:.0f}')
    if j_overbought:  kdj_parts.append(f'J值超买={j:.0f}')
    signals['KDJ'] = {'hit': kdj_hit, 'desc': '、'.join(kdj_parts) or f'K={k:.0f} 无异常'}

    # 布林带（你的列名是 BOLL_UPPER / BOLL_MID / BOLL_LOWER）
    touch_upper      = latest['close'] >= latest['BOLL_UPPER'] * 0.99
    break_mid        = prev['close'] >= prev['BOLL_MID'] and latest['close'] < latest['BOLL_MID']
    bw_now           = latest['BOLL_UPPER'] - latest['BOLL_LOWER']
    bw_prev          = prev['BOLL_UPPER']   - prev['BOLL_LOWER']
    boll_expand_down = bw_now > bw_prev * 1.1 and latest['close'] < latest['BOLL_MID']
    boll_hit   = touch_upper or break_mid or boll_expand_down
    boll_parts = []
    if touch_upper:       boll_parts.append('触碰上轨超买')
    if break_mid:         boll_parts.append('跌破布林中轨⚠️')
    if boll_expand_down:  boll_parts.append('带宽扩张向下')
    signals['BOLL'] = {'hit': boll_hit, 'desc': '、'.join(boll_parts) or '布林无异常'}

    return signals


def calc_key_levels_card(df):
    latest = df.iloc[-1]
    return {
        'resistance': round(float(latest['BOLL_UPPER']), 2),
        'support':    round(float(latest['BOLL_LOWER']), 2),
        'stop_loss':  round(float(latest['MA20']) * 0.97, 2),
    }
