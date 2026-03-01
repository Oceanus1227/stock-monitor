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

# 添加脚本目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from technical_analysis import (
    get_stock_data, calculate_ma, calculate_macd, calculate_rsi,
    calculate_bollinger, calculate_kdj, check_signals
)


# ==================== 工具函数 ====================

def load_config(path="configs/config.yaml"):
    """加载配置文件"""
    config_path = Path(__file__).parent.parent / path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_trading_day():
    """
    判断今天是否为交易日
    不依赖 chinese_calendar，改用简单的周末判断 + 手动节假日列表
    """
    today = date.today()

    # 周六、周日直接跳过
    if today.weekday() >= 5:
        return False

    # 手动维护节假日（2025-2026年）
    holidays = {
        # 2025年
        date(2025, 1, 1),   # 元旦
        date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
        date(2025, 1, 31), date(2025, 2, 3), date(2025, 2, 4),  # 春节
        date(2025, 4, 4),   # 清明
        date(2025, 5, 1), date(2025, 5, 2),  # 劳动节
        date(2025, 5, 31), date(2025, 6, 2),  # 端午
        date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3),
        date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8),  # 国庆
        # 2026年
        date(2026, 1, 1),   # 元旦
        date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19),
        date(2026, 2, 20), date(2026, 2, 23), date(2026, 2, 24),  # 春节
        date(2026, 4, 6),   # 清明
        date(2026, 5, 1),   # 劳动节
        date(2026, 6, 19),  # 端午
        date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
        date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8),  # 国庆
    }

    return today not in holidays


def now_cn():
    """返回北京时间"""
    return datetime.utcnow() + timedelta(hours=8)


def should_push(symbol, throttle_minutes):
    """
    推送节流：同一股票在指定时间内只推送一次

    Returns:
        bool: 是否应该推送
    """
    state_path = Path(__file__).parent.parent / "data" / "state.json"
    state = {}

    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

    last = state.get(symbol)
    now = now_cn().timestamp()

    if last and now - last < throttle_minutes * 60:
        return False

    # 更新状态
    state[symbol] = now
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

    return True


def push_feishu(text, webhook=None):
    """
    飞书推送

    Returns:
        dict: 推送结果状态
    """
    webhook = webhook or os.getenv("FEISHU_WEBHOOK")
    if not webhook:
        return {"status": "skip", "reason": "no webhook configured"}

    payload = {
        "msg_type": "text",
        "content": {"text": text}
    }

    try:
        r = requests.post(webhook, json=payload, timeout=10)
        return {"status": r.status_code, "ok": r.status_code == 200}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


def write_run_summary(ok, fail, alerts_count=0, note=None):
    """写入运行摘要"""
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
    """
    确保 signals.json 始终存在，防止前端报错
    仅在文件不存在时写入空结构
    """
    signals_path = data_dir / "signals.json"
    if not signals_path.exists():
        with open(signals_path, "w", encoding="utf-8") as f:
            json.dump({
                "signals": alerts or [],
                "update_time": now_cn().isoformat(),
                "note": note or ""
            }, f, ensure_ascii=False, indent=2)


# ==================== 主流程 ====================

def main():
    """主函数"""
    print("=" * 60)
    print("🚀 股票技术指标监控系统（重构版）")
    print("=" * 60)

    # 加载配置
    try:
        cfg = load_config()
        print(f"✅ 配置加载成功")
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return

    # 判断是否交易日
    if cfg.get("runtime", {}).get("use_trading_calendar", True):
        if not is_trading_day():
            print("📅 非交易日，跳过数据抓取")
            data_dir = Path(__file__).parent.parent / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            # ✅ 非交易日也保证 signals.json 存在，前端不报错
            ensure_signals_json(data_dir, note="non-trading-day")
            write_run_summary(ok=[], fail=[], note="non-trading-day")
            return
        else:
            print("📅 交易日，正常执行")

    # 获取配置参数
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

    # 遍历自选股
    for stock in watchlist:
        code = stock.get("code")
        name = stock.get("name", code)

        if not code:
            continue

        print(f"\n📈 处理 {name}({code})...")

        try:
            # 获取数据（带重试）
            df = get_stock_data(code, days=history_days)

            # 计算技术指标
            df = calculate_ma(df)
            df = calculate_macd(df)
            df = calculate_rsi(df)
            df = calculate_bollinger(df)
            df = calculate_kdj(df)

            # 检查信号
            result = check_signals(df, code, name, config=signals_cfg)

            if result:
                alerts.append(result)
                print(f"   ✅ 发现 {len(result['signals'])} 个信号，趋势: {result['trend']}")

                # 推送处理
                should_send = (
                    result['trend'] == '强势' or
                    any(s['strength'] == '强' for s in result['signals'])
                ) if strong_only else True

                if should_send and should_push(code, throttle_minutes):
                    signal_texts = [f"[{s['indicator']}] {s['type']}: {s['desc']}" for s in result['signals']]
                    msg = f"""🚨 股票信号提醒

📈 {name} ({code})
💰 现价: ¥{result['price']}
📊 趋势: {result['trend']}
🔔 信号:
"""
                    for sig_text in signal_texts:
                        msg += f"  • {sig_text}\n"

                    msg += f"\n⏰ {now_cn().strftime('%Y-%m-%d %H:%M')}\n"
                    msg += f"📊 RSI: {result['rsi']:.1f} | MACD: {result['macd']:.4f}"

                    push_result = push_feishu(msg)
                    if push_result.get("ok"):
                        print(f"   📱 飞书推送成功")
                    else:
                        print(f"   ⚠️ 飞书推送: {push_result.get('status')}")
            else:
                print(f"   ℹ️ 无显著信号")

            ok_list.append({"code": code, "name": name})

        except Exception as e:
            print(f"   ❌ 处理失败: {e}")
            fail_list.append({"code": code, "name": name, "error": str(e)})

    # 写入数据文件
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # alerts.json（原有格式）
    with open(data_dir / "alerts.json", "w", encoding="utf-8") as f:
        json.dump(alerts, f, ensure_ascii=False, indent=2)

    # signals.json（给前端用，交易日直接覆盖写入真实数据）
    with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump({
            "signals": alerts,
            "update_time": now_cn().isoformat()
        }, f, ensure_ascii=False, indent=2)

    # 运行摘要
    write_run_summary(ok=ok_list, fail=fail_list, alerts_count=len(alerts))

    print("\n" + "=" * 60)
    print(f"✅ 完成: 成功 {len(ok_list)}，失败 {len(fail_list)}，信号 {len(alerts)}")
    print(f"📁 数据已保存到 data/")
    print("=" * 60)


if __name__ == "__main__":
    main()
