#!/usr/bin/env python3
"""
股票技术指标监控主脚本
使用akshare开源数据源，支持配置化、重试机制、推送节流
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
    check_signals
)

# ==================== 工具函数 ====================

def load_config(path="configs/config.yaml"):
    config_path = Path(__file__).parent.parent / path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_trading_day(check_date=None):
    """
    优先用 akshare 交易日历接口，失败时降级到本地节假日规则。
    """
    target = check_date or date.today()

    if target.weekday() >= 5:
        return False

    try:
        import akshare as ak
        import pandas as pd
        # 修复：删除了无用的 year = str(target.year)
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(trade_cal["trade_date"]).dt.date.tolist()
        return target in trade_dates
    except Exception:
        pass

    # 降级：本地节假日规则
    holidays = {
        # 2025
        date(2025, 1, 1),
        date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
        date(2025, 1, 31), date(2025, 2, 3), date(2025, 2, 4),
        date(2025, 4, 4),
        date(2025, 5, 1), date(2025, 5, 2),
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
    """
    自动往前找最近一个真实交易日（最多回溯7天）。
    """
    for i in range(1, 8):
        candidate = date.today() - timedelta(days=i)
        if is_trading_day(candidate):
            return candidate
    return date.today() - timedelta(days=1)


def now_cn():
    return datetime.utcnow() + timedelta(hours=8)


def should_push(symbol, throttle_minutes):
    state_path = Path(__file__).parent.parent / "data" / "state.json"
    state = {}
    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            try:
                state = json.load(f)
            except json.JSONDecodeError:
                # 修复：state.json 损坏时不崩溃，重置为空
                state = {}

    last = state.get(symbol)
    now = now_cn().timestamp()
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


def write_signals_json(data_dir, alerts, is_last_trading=False, note=None):
    """
    统一写入 signals.json 的入口，避免字段遗漏。
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "signals":         alerts or [],
        "update_time":     now_cn().isoformat(),
        "is_last_trading": is_last_trading,
        "note":            note or "",
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

def process_stock(symbol, name, cfg, data_dir):
    """
    处理单只股票：拉数据 → 计算指标 → 检测信号 → 返回 alert dict
    """
    print(f"  📈 处理 {name}({symbol}) ...")

    # 拉取行情数据
    df = get_stock_data(symbol, period=cfg.get("period", "daily"), count=cfg.get("count", 120))
    if df is None or df.empty:
        print(f"    ⚠️  {name} 数据获取失败，跳过")
        return None

    # 计算各技术指标
    df = calculate_ma(df, windows=cfg.get("ma_windows", [5, 10, 20, 60]))
    df = calculate_macd(df,
                        fast=cfg.get("macd_fast", 12),
                        slow=cfg.get("macd_slow", 26),
                        signal=cfg.get("macd_signal", 9))
    df = calculate_rsi(df, period=cfg.get("rsi_period", 14))
    df = calculate_bollinger(df,
                             window=cfg.get("boll_window", 20),
                             num_std=cfg.get("boll_std", 2.0))
    df = calculate_kdj(df,
                       fastk_period=cfg.get("kdj_fastk", 9),
                       signal_period=cfg.get("kdj_signal", 3))

    # 检测信号
    result = check_signals(df, cfg)
    if not result:
        return None

    signals = result.get("signals", [])
    score   = result.get("score", 0)

    if not signals:
        return None

    # 构建 alert 条目
    latest = df.iloc[-1]
    alert = {
        "symbol":         symbol,
        "name":           name,
        "signals":        signals,
        "score":          score,
        "close":          round(float(latest.get("close", 0)), 2),
        "update_time":    now_cn().isoformat(),
        # 近 30 日收盘价，供前端画迷你折线图
        "price_history":  [
            round(float(v), 2)
            for v in df["close"].tail(30).tolist()
        ],
        # 近 30 日成交量
        "volume_history": [
            int(v)
            for v in df["volume"].tail(30).tolist()
        ],
    }
    return alert


# ==================== 主流程 ====================

def main():
    cfg_all  = load_config()
    cfg      = cfg_all.get("analysis", {})
    stocks   = cfg_all.get("stocks", [])
    throttle = cfg_all.get("push", {}).get("throttle_minutes", 60)

    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    force_run = os.getenv("FORCE_RUN", "false").lower() == "true"
    today     = date.today()

    # ── 交易日校验 ──────────────────────────────────────────
    if not force_run and not is_trading_day(today):
        print(f"📅 今天 {today} 非交易日，尝试加载历史数据...")

        # 读取已有 signals.json（如果存在）
        signals_path = data_dir / "signals.json"
        if signals_path.exists():
            with open(signals_path, "r", encoding="utf-8") as f:
                try:
                    last_data = json.load(f)
                except json.JSONDecodeError:
                    last_data = None
        else:
            last_data = None

        if last_data:
            # 修复：非交易日且有历史数据 → 只写一次 run_summary，然后直接返回
            print("  ✅ 已有历史数据，保持原文件不变")
            write_run_summary(ok=[], fail=[], note="non-trading-day")
            return
        else:
            # 没有历史数据，继续往下跑一次，让 signals.json 有初始内容
            print("  ⚠️  无历史数据，继续执行首次数据构建...")
            # 注意：这里不写 run_summary，让下面正常流程统一写

    # ── 正常处理流程 ────────────────────────────────────────
    print(f"\n🚀 开始处理 {len(stocks)} 只股票...\n")

    ok_list    = []
    fail_list  = []
    alerts     = []

    for stock in stocks:
        symbol = stock.get("symbol", "")
        name   = stock.get("name", symbol)

        try:
            alert = process_stock(symbol, name, cfg, data_dir)
            ok_list.append(symbol)

            if alert:
                alerts.append(alert)
                sig_count = len(alert.get("signals", []))
                print(f"    ✅ {name}: {sig_count} 个信号，得分 {alert['score']}")

                # 飞书推送（带节流）
                if should_push(symbol, throttle):
                    sig_text = "\n".join(alert["signals"])
                    push_text = (
                        f"📊 {name}({symbol})\n"
                        f"收盘价: {alert['close']}\n"
                        f"得分: {alert['score']}\n"
                        f"信号:\n{sig_text}"
                    )
                    push_result = push_feishu(push_text)

                    # 修复：区分 skip / 成功 / 失败，不再把 skip 误报为失败
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
    is_last = now_cn().hour >= 15  # 15:00 之后视为收盘后

    write_signals_json(data_dir, alerts, is_last_trading=is_last)
    write_run_summary(
        ok=ok_list,
        fail=fail_list,
        alerts_count=len(alerts),
        note="force_run" if force_run else "normal",
    )

    print(f"\n✅ 完成：{len(ok_list)} 成功，{len(fail_list)} 失败，{len(alerts)} 个信号")


if __name__ == "__main__":
    main()
