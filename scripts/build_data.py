#!/usr/bin/env python3
"""
股票技术指标监控主脚本（重构版）
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
    get_stock_data, calculate_ma, calculate_macd, calculate_rsi,
    calculate_bollinger, calculate_kdj, check_signals
)


# ==================== 工具函数 ====================

def load_config(path="configs/config.yaml"):
    config_path = Path(__file__).parent.parent / path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_trading_day():
    today = date.today()
    if today.weekday() >= 5:
        return False

    holidays = {
        date(2025, 1, 1),
        date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
        date(2025, 1, 31), date(2025, 2, 3), date(2025, 2, 4),
        date(2025, 4, 4),
        date(2025, 5, 1), date(2025, 5, 2),
        date(2025, 5, 31), date(2025, 6, 2),
        date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3),
        date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8),
        date(2026, 1, 1),
        date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19),
        date(2026, 2, 20), date(2026, 2, 23), date(2026, 2, 24),
        date(2026, 4, 6),
        date(2026, 5, 1),
        date(2026, 6, 19),
        date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
        date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8),
    }

    return today not in holidays


def now_cn():
    return datetime.utcnow() + timedelta(hours=8)


def should_push(symbol, throttle_minutes):
    state_path = Path(__file__).parent.parent / "data" / "state.json"
    state = {}

    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

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


def write_run_summary(ok, fail, alerts_count=0, note=None):
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "time": now_cn().isoformat(),
        "ok": ok,
        "fail": fail,
        "alerts_count": alerts_count,
        "note": note
    }

    with open(data_dir / "latest_run.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def ensure_signals_json(data_dir, alerts=None, note=None):
    """文件不存在时写入空占位结构"""
    signals_path = data_dir / "signals.json"
    if not signals_path.exists():
        with open(signals_path, "w", encoding="utf-8") as f:
            json.dump({
                "signals": alerts or [],
                "update_time": now_cn().isoformat(),
                "note": note or ""
            }, f, ensure_ascii=False, indent=2)


def get_last_trading_data(data_dir):
    """读取上一次真实交易日数据"""
    signals_path = data_dir / "signals.json"
    if not signals_path.exists():
        return None
    try:
        with open(signals_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 排除纯占位数据，只保留有真实交易数据的
        if data.get("note") != "non-trading-day" and data.get("signals") is not None:
            return data
    except Exception:
        pass
    return None


# ==================== 主流程 ====================

def main():
    print("=" * 60)
    print("🚀 股票技术指标监控系统（重构版）")
    print("=" * 60)

    try:
        cfg = load_config()
        print("✅ 配置加载成功")
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return

    # 判断是否交易日
    if cfg.get("runtime", {}).get("use_trading_calendar", True):
        if not is_trading_day():
            print("📅 非交易日，尝试保留上一交易日数据")
            data_dir = Path(__file__).parent.parent / "data"
            data_dir.mkdir(parents=True, exist_ok=True)

            last_data = get_last_trading_data(data_dir)
            if last_data:
                last_data["note"] = "non-trading-day"
                last_data["is_last_trading"] = True
                with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
                    json.dump(last_data, f, ensure_ascii=False, indent=2)
                print(f"   ✅ 已保留上一交易日数据，时间: {last_data.get('update_time')}")
            else:
                ensure_signals_json(data_dir, note="non-trading-day")
                print("   ℹ️ 无历史数据，写入空占位文件")

            write_run_summary(ok=[], fail=[], note="non-trading-day")
            return
        else:
            print("📅 交易日，正常执行")

    watchlist = cfg.get("watchlist", [])
    runtime_cfg = cfg.get("runtime", {})
    push_cfg = cfg.get("push", {})
    signals_cfg = cfg.get("signals", {})

    throttle_minutes = push_cfg.get("throttle_minutes", 30)
    strong_only = push_cfg.get("strong_signal_only", True)
    history_days = runtime_cfg.get("history_days", 60)

    print(f"📊 监控股票数: {len(watchlist)}")
    print(f"⏱️ 推送节流: {throttle_minutes}分钟")
    print(f"📈 历史数据: {history_days}天")
    print("-" * 60)

    alerts = []
    ok_list = []
    fail_list = []

    for stock in watchlist:
        code = stock.get("code")
        name = stock.get("name", code)

        if not code:
            continue

        print(f"\n📈 处理 {name}({code})...")

        try:
            df = get_stock_data(code, days=history_days)
            df = calculate_ma(df)
            df = calculate_macd(df)
            df = calculate_rsi(df)
            df = calculate_bollinger(df)
            df = calculate_kdj(df)

            result = check_signals(df, code, name, config=signals_cfg)

            if result:
                alerts.append(result)
                print(f"   ✅ 发现 {len(result['signals'])} 个信号，趋势: {result['trend']}")

                should_send = (
                    result['trend'] == '强势' or
                    any(s['strength'] == '强' for s in result['signals'])
                ) if strong_only else True

                if should_send and should_push(code, throttle_minutes):
                    signal_texts = [f"[{s['indicator']}] {s['type']}: {s['desc']}" for s in result['signals']]
                    msg = f"🚨 股票信号提醒\n\n📈 {name} ({code})\n💰 现价: ¥{result['price']}\n📊 趋势: {result['trend']}\n🔔 信号:\n"
                    for sig_text in signal_texts:
                        msg += f"  • {sig_text}\n"
                    msg += f"\n⏰ {now_cn().strftime('%Y-%m-%d %H:%M')}\n"
                    msg += f"📊 RSI: {result['rsi']:.1f} | MACD: {result['macd']:.4f}"

                    push_result = push_feishu(msg)
                    if push_result.get("ok"):
                        print("   📱 飞书推送成功")
                    else:
                        print(f"   ⚠️ 飞书推送: {push_result.get('status')}")
            else:
                print("   ℹ️ 无显著信号")

            ok_list.append({"code": code, "name": name})

        except Exception as e:
            print(f"   ❌ 处理失败: {e}")
            fail_list.append({"code": code, "name": name, "error": str(e)})

    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    with open(data_dir / "alerts.json", "w", encoding="utf-8") as f:
        json.dump(alerts, f, ensure_ascii=False, indent=2)

    # 交易日写入真实数据，is_last_trading 明确为 False
    with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump({
            "signals": alerts,
            "update_time": now_cn().isoformat(),
            "is_last_trading": False
        }, f, ensure_ascii=False, indent=2)

    write_run_summary(ok=ok_list, fail=fail_list, alerts_count=len(alerts))

    print("\n" + "=" * 60)
    print(f"✅ 完成: 成功 {len(ok_list)}，失败 {len(fail_list)}，信号 {len(alerts)}")
    print(f"📁 数据已保存到 data/")
    print("=" * 60)


if __name__ == "__main__":
    main()
