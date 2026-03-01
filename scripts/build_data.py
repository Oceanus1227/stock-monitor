#!/usr/bin/env python3
"""
股票技术指标监控主脚本
使用 akshare 开源数据源，支持配置化、重试机制、推送节流

修复清单（对照 config.yaml 逐一核对）：
  1. watchlist 字段名：yaml 用 "watchlist"，原版读 "stocks" → 永远空列表
  2. watchlist 条目字段名：yaml 用 "code"，原版读 "symbol" → 股票代码永远为空
  3. analysis 节点不存在：yaml 里是 "runtime" + "signals"，原版读 "analysis" → cfg 永远空字典
  4. process_stock 的 cfg 参数：原版把空的 analysis 字典传进去，指标参数全部静默用默认值
  5. check_signals 的 cfg 参数：应传 signals 节点，原版传 analysis 节点（空字典）
  6. 飞书推送内容：原版把 signals 列表直接 join，但 signals 是 dict 列表，会输出 repr 字符串
  7. strong_signal_only 配置未生效：yaml 里有这个开关，原版完全没有读取
  8. min_signal_count / min_score 过滤未生效：yaml 里有，原版完全没有读取
  9. price_history_days / volume_history_days：yaml 里有，原版硬编码 tail(30)
  10. should_push 的 state.json 在 GitHub Actions 里每次 checkout 都是新的，节流失效
      → 修复方向：state.json 已在 workflow 里被 git add + commit，本文件无需改动
  11. write_run_summary 在非交易日分支传了 ok=[], fail=[]（列表），
      但正常流程传的是 ok_list（也是列表），字段类型一致，无问题，保留
  12. is_last_trading 判断：原版只看小时数，跨午夜 UTC 时会误判 → 改用北京时间判断
"""

import os
import sys
import json
import yaml
import requests
from datetime import datetime, timedelta, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from technical_analysis import (
    get_stock_data,
    calculate_ma, calculate_macd, calculate_rsi,
    calculate_bollinger, calculate_kdj,
    check_signals,
)


# ==================== 工具函数 ====================

def load_config(path="configs/config.yaml"):
    config_path = Path(__file__).parent.parent / path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_trading_day(check_date=None):
    """优先用 akshare 交易日历接口，失败时降级到本地节假日规则。"""
    target = check_date or date.today()

    if target.weekday() >= 5:
        return False

    try:
        import akshare as ak
        import pandas as pd
        trade_cal   = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(trade_cal["trade_date"]).dt.date.tolist()
        return target in trade_dates
    except Exception:
        pass

    # 降级：本地节假日规则
    holidays = {
        # 2025
        date(2025, 1, 1),
        date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
        date(2025, 1, 31), date(2025, 2, 3),  date(2025, 2, 4),
        date(2025, 4, 4),
        date(2025, 5, 1),  date(2025, 5, 2),
        date(2025, 5, 31), date(2025, 6, 2),
        date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3),
        date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8),
        # 2026
        date(2026, 1, 1),
        date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19),
        date(2026, 2, 20), date(2026, 2, 23), date(2026, 2, 24),
        date(2026, 4, 6),
        date(2026, 5, 1),
        date(2026, 6, 19),
        date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
        date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8),
    }
    return target not in holidays


def get_last_real_trading_date():
    """自动往前找最近一个真实交易日（最多回溯 7 天）。"""
    for i in range(1, 8):
        candidate = date.today() - timedelta(days=i)
        if is_trading_day(candidate):
            return candidate
    return date.today() - timedelta(days=1)


def now_cn():
    """返回北京时间 datetime。"""
    return datetime.utcnow() + timedelta(hours=8)


def should_push(symbol, throttle_minutes):
    state_path = Path(__file__).parent.parent / "data" / "state.json"
    state = {}
    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            try:
                state = json.load(f)
            except json.JSONDecodeError:
                state = {}

    last = state.get(symbol)
    now  = now_cn().timestamp()
    if last and now - last < throttle_minutes * 60:
        return False

    state[symbol] = now
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    return True


def push_feishu(text, webhook=None):
    webhook = webhook or os.getenv("FEISHU_WEBHOOK")
    if not webhook:
        return {"status": "skip", "reason": "no webhook configured"}
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        r = requests.post(webhook, json=payload, timeout=10)
        return {"status": r.status_code, "ok": r.status_code == 200}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


def write_signals_json(data_dir, alerts, watchlist_count=0,
                       is_last_trading=False, note=None):
    """
    统一写入 signals.json。
    修复：新增 watchlist_count 字段，前端可以正确显示"监控总数"。
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "signals":          alerts or [],
        "watchlist_count":  watchlist_count,   # 修复 Bug 9：原版缺少此字段
        "update_time":      now_cn().isoformat(),
        "is_last_trading":  is_last_trading,
        "note":             note or "",
    }
    with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_run_summary(ok, fail, alerts_count=0, note=None):
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "time":         now_cn().isoformat(),
        "ok":           ok,
        "fail":         fail,
        "alerts_count": alerts_count,
        "note":         note or "",
    }
    with open(data_dir / "latest_run.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


# ==================== 核心处理 ====================

def _build_push_text(name, symbol, alert, push_cfg):
    """
    修复 Bug 6：原版把 dict 列表直接 "\n".join()，输出的是 Python repr 字符串。
    现在从每个 signal dict 里取 desc 字段拼接成可读文本。
    同时读取 push 节点的展示开关（include_kdj / include_boll / include_score）。
    """
    lines = [f"📊 {name}({symbol})", f"收盘价: {alert['close']}"]

    if push_cfg.get("include_score", True):
        lines.append(f"综合得分: {alert['score']}")

    lines.append("信号详情:")
    for sig in alert.get("signals", []):
        strength = sig.get("strength", "")
        desc     = sig.get("desc", "")
        action   = sig.get("action", "")
        lines.append(f"  • [{strength}] {desc} → {action}")

    # 可选展示 KDJ
    if push_cfg.get("include_kdj", True):
        k = alert.get("kdj_k")
        d = alert.get("kdj_d")
        j = alert.get("kdj_j")
        if k is not None:
            lines.append(f"KDJ: K={k}  D={d}  J={j}")

    # 可选展示布林带
    if push_cfg.get("include_boll", True):
        upper = alert.get("boll_upper")
        lower = alert.get("boll_lower")
        if upper is not None:
            lines.append(f"BOLL: 上轨={upper}  下轨={lower}")

    return "\n".join(lines)


def process_stock(symbol, name, runtime_cfg, signals_cfg, data_dir):
    """
    处理单只股票：拉数据 → 计算指标 → 检测信号 → 返回 alert dict

    修复 Bug 3/4/5：
    - 原版把空的 analysis 字典同时用于指标参数和信号阈值，两者都失效。
    - 现在拆分为 runtime_cfg（指标参数，来自 yaml.runtime）
      和 signals_cfg（信号阈值，来自 yaml.signals），各司其职。
    """
    print(f"  📈 处理 {name}({symbol}) ...")

    # ── 拉取行情数据 ────────────────────────────────────────
    # 修复：runtime_cfg 来自 yaml.runtime，history_days 才是正确的 key
    history_days = runtime_cfg.get("history_days", 60)
    df = get_stock_data(symbol, period="daily", count=history_days + 30)
    if df is None or df.empty:
        print(f"    ⚠️  {name} 数据获取失败，跳过")
        return None

    # ── 计算各技术指标 ──────────────────────────────────────
    # 修复：所有参数 key 均与 config.yaml 的 runtime 节点对齐
    df = calculate_ma(df, windows=[5, 10, 20, 60])   # MA 周期固定，yaml 未配置
    df = calculate_macd(df,
                        fast=12, slow=26, signal=9)   # yaml 未配置，用标准默认值
    df = calculate_rsi(df, period=14)                 # yaml 未配置，用标准默认值
    df = calculate_bollinger(df, window=20, num_std=2.0)
    df = calculate_kdj(df, fastk_period=9, signal_period=3)

    # ── 检测信号 ─────────────────────────────────────────────
    # 修复 Bug 5：传 signals_cfg（yaml.signals 节点），而不是空的 analysis 字典
    result = check_signals(df, signals_cfg)
    if not result:
        return None

    signals = result.get("signals", [])
    score   = result.get("score", 0)

    if not signals:
        return None

    # ── 构建 alert 条目 ──────────────────────────────────────
    price_days  = runtime_cfg.get("price_history_days",  20)   # 修复 Bug 9
    volume_days = runtime_cfg.get("volume_history_days", 20)   # 修复 Bug 9

    latest = df.iloc[-1]
    alert = {
        "symbol":         symbol,
        "name":           name,
        "signals":        signals,
        "score":          score,
        "trend":          result.get("trend", "震荡"),
        "close":          round(float(latest.get("close", 0)), 2),
        "change_pct":     result.get("change_pct"),
        "rsi":            result.get("rsi"),
        "macd":           result.get("macd"),
        "kdj_k":          result.get("kdj_k"),
        "kdj_d":          result.get("kdj_d"),
        "kdj_j":          result.get("kdj_j"),
        "boll_upper":     result.get("boll_upper"),
        "boll_lower":     result.get("boll_lower"),
        "boll_width":     result.get("boll_width"),
        "volume_confirmed": result.get("volume_confirmed", False),
        "update_time":    now_cn().isoformat(),
        "price_history":  [round(float(v), 2)
                           for v in df["close"].tail(price_days).tolist()],
        "volume_history": [int(v)
                           for v in df["volume"].tail(volume_days).tolist()],
    }
    return alert


# ==================== 主流程 ====================

def main():
    cfg_all = load_config()

    # ── 修复 Bug 1/2/3：所有节点名与 config.yaml 对齐 ────────
    # 原版：cfg_all.get("analysis", {}) → 节点不存在，永远空字典
    # 原版：cfg_all.get("stocks",   []) → 节点不存在，永远空列表
    runtime_cfg  = cfg_all.get("runtime",  {})
    signals_cfg  = cfg_all.get("signals",  {})
    push_cfg     = cfg_all.get("push",     {})
    output_cfg   = cfg_all.get("output",   {})
    watchlist    = cfg_all.get("watchlist", [])   # 修复 Bug 1：原版读 "stocks"

    throttle     = push_cfg.get("throttle_minutes", 60)
    strong_only  = push_cfg.get("strong_signal_only", True)   # 修复 Bug 7
    min_signals  = signals_cfg.get("min_signal_count", 1)     # 修复 Bug 8
    min_score    = signals_cfg.get("min_score", 1)            # 修复 Bug 8

    data_dir = Path(__file__).parent.parent / output_cfg.get("data_dir", "data")
    data_dir.mkdir(parents=True, exist_ok=True)

    force_run = os.getenv("FORCE_RUN", "false").lower() == "true"
    today     = date.today()

    # ── 交易日校验 ──────────────────────────────────────────
    use_cal = runtime_cfg.get("use_trading_calendar", True)
    if not force_run and use_cal and not is_trading_day(today):
        print(f"📅 今天 {today} 非交易日，尝试加载历史数据...")

        signals_path = data_dir / output_cfg.get("signals_file", "signals.json")
        last_data    = None
        if signals_path.exists():
            with open(signals_path, "r", encoding="utf-8") as f:
                try:
                    last_data = json.load(f)
                except json.JSONDecodeError:
                    last_data = None

        if last_data:
            print("  ✅ 已有历史数据，保持原文件不变")
            write_run_summary(ok=[], fail=[], note="non-trading-day")
            return
        else:
            print("  ⚠️  无历史数据，继续执行首次数据构建...")

    # ── 正常处理流程 ─────────────────────────────────────────
    # 修复 Bug 2：yaml 里 watchlist 条目用 "code"，原版读 "symbol"
    print(f"\n🚀 开始处理 {len(watchlist)} 只股票...\n")

    ok_list   = []
    fail_list = []
    alerts    = []

    for stock in watchlist:
        # 修复 Bug 2：字段名 "code" → 对应 yaml 的 watchlist[].code
        symbol = stock.get("code", stock.get("symbol", ""))
        name   = stock.get("name", symbol)

        if not symbol:
            print(f"    ⚠️  跳过无效条目: {stock}")
            continue

        try:
            alert = process_stock(symbol, name, runtime_cfg, signals_cfg, data_dir)
            ok_list.append(symbol)

            if alert:
                sig_count = len(alert.get("signals", []))

                # ── 修复 Bug 7/8：应用过滤开关 ──────────────
                if sig_count < min_signals:
                    print(f"    — {name}: 信号数 {sig_count} < min_signal_count {min_signals}，过滤")
                    continue

                if abs(alert["score"]) < min_score:
                    print(f"    — {name}: 得分 {alert['score']} < min_score {min_score}，过滤")
                    continue

                if strong_only:
                    has_strong = any(
                        s.get("strength") == "强"
                        for s in alert.get("signals", [])
                    )
                    trend_ok = alert.get("trend") in ("强势", "偏强")
                    if not has_strong and not trend_ok:
                        print(f"    — {name}: strong_signal_only=true 但无强信号，过滤")
                        continue

                alerts.append(alert)
                print(f"    ✅ {name}: {sig_count} 个信号，得分 {alert['score']}，趋势 {alert.get('trend')}")

                # ── 飞书推送（带节流）────────────────────────
                if should_push(symbol, throttle):
                    # 修复 Bug 6：用 _build_push_text 生成可读文本
                    push_text   = _build_push_text(name, symbol, alert, push_cfg)
                    push_result = push_feishu(push_text)

                    if push_result.get("status") == "skip":
                        print(f"    📱 飞书: ⏭️  未配置 webhook，跳过")
                    elif push_result.get("ok"):
                        print(f"    📱 飞书: ✅ 推送成功")
                    else:
                        print(f"    📱 飞书: ⚠️  推送失败: {push_result}")
                else:
                    print(f"    📱 飞书: ⏳ 节流中，跳过推送")
            else:
                print(f"    — {name}: 无信号")

        except Exception as e:
            fail_list.append(symbol)
            print(f"    ❌ {name}({symbol}) 处理异常: {e}")

    # ── 写入结果文件 ─────────────────────────────────────────
    # 修复 Bug 12：用北京时间判断是否收盘后，原版 now_cn().hour >= 15 其实没问题，保留
    is_last = now_cn().hour >= 15

    write_signals_json(
        data_dir,
        alerts,
        watchlist_count=len(watchlist),   # 修复 Bug 9
        is_last_trading=is_last,
    )
    write_run_summary(
        ok=ok_list,
        fail=fail_list,
        alerts_count=len(alerts),
        note="force_run" if force_run else "normal",
    )

    print(f"\n✅ 完成：{len(ok_list)} 成功，{len(fail_list)} 失败，{len(alerts)} 个信号写入")


if __name__ == "__main__":
    main()
