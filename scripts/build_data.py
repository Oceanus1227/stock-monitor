#!/usr/bin/env python3
"""
股票技术指标监控主脚本
使用 akshare 开源数据源，支持配置化、重试机制、推送节流
"""

import os
import sys
import json
import yaml
import requests
import time  
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
    # 👇 修复：统一使用北京时间
    target = check_date or now_cn().date()
    if target.weekday() >= 5:
        return False
    try:
        import akshare as ak
        import pandas as pd
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(trade_cal["trade_date"]).dt.date.tolist()
        return target in trade_dates
    except Exception:
        pass
        
    holidays = {
        # 2025
        date(2025, 1, 1), date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30), date(2025, 1, 31),
        date(2025, 2, 3), date(2025, 2, 4), date(2025, 4, 4), date(2025, 5, 1), date(2025, 5, 2),
        date(2025, 5, 31), date(2025, 6, 2), date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3),
        date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8),
        # 2026
        date(2026, 1, 1), date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19), date(2026, 2, 20),
        date(2026, 2, 23), date(2026, 2, 24), date(2026, 4, 6), date(2026, 5, 1), date(2026, 6, 19),
        date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8),
    }
    return target not in holidays


def get_last_real_trading_date():
    """自动往前找最近一个真实交易日（最多回溯 7 天）。"""
    for i in range(1, 8):
        # 👇 修复：统一使用北京时间
        candidate = now_cn().date() - timedelta(days=i)
        if is_trading_day(candidate):
            return candidate
    return now_cn().date() - timedelta(days=1)


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
                       is_last_trading=False, note=None, pretty=True):
    data_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "signals":         alerts or [],
        "watchlist_count": watchlist_count,
        "update_time":     now_cn().isoformat(),
        "is_last_trading": is_last_trading,
        "note":            note or "",
    }
    indent = 2 if pretty else None
    with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=indent)


def write_run_summary(ok, fail, alerts_count=0, note=None, pretty=True):
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "time":         now_cn().isoformat(),
        "ok":           ok,
        "fail":         fail,
        "alerts_count": alerts_count,
        "note":         note or "",
    }
    indent = 2 if pretty else None
    with open(data_dir / "latest_run.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=indent)


# ==================== 核心处理 ====================

def _build_push_text(name, symbol, alert, push_cfg):
    """生成飞书推送的可读文本。"""
    lines = [f"📊 {name}({symbol})", f"收盘价: {alert['close']}"]

    if push_cfg.get("include_score", True):
        lines.append(f"综合得分: {alert['score']}  趋势: {alert.get('trend', '-')}")

    lines.append("信号详情:")
    for sig in alert.get("signals", []):
        strength = sig.get("strength", "")
        desc     = sig.get("desc", "")
        action   = sig.get("action", "")
        lines.append(f"  • [{strength}] {desc} → {action}")

    if push_cfg.get("include_kdj", True):
        k = alert.get("kdj_k")
        d = alert.get("kdj_d")
        j = alert.get("kdj_j")
        if k is not None:
            lines.append(f"KDJ: K={k}  D={d}  J={j}")

    if push_cfg.get("include_boll", True):
        upper = alert.get("boll_upper")
        lower = alert.get("boll_lower")
        if upper is not None:
            lines.append(f"BOLL: 上轨={upper}  下轨={lower}")

    return "\n".join(lines)


def process_stock(symbol, name, runtime_cfg, signals_cfg):
    """
    处理单只股票：拉数据 → 计算指标 → 检测信号 → 返回 alert dict
    失败时抛出异常，由 main() 统一捕获计入 fail_list。
    """
    print(f"  📈 处理 {name}({symbol}) ...")

    # ── 读取运行时参数（全部来自 yaml.runtime）──────────────
    history_days        = runtime_cfg.get("history_days", 60)
    price_history_days  = runtime_cfg.get("price_history_days", 20)
    volume_history_days = runtime_cfg.get("volume_history_days", 20)

    # ── 拉取行情数据 ─────────────────────────────────────────
    df = get_stock_data(symbol, period="daily", count=history_days + 30)
    if df is None or df.empty:
        raise ValueError(f"{name}({symbol}) 数据获取失败")

    # ── 计算各技术指标 ───────────────────────────────────────
    df = calculate_ma(df, windows=[5, 10, 20, 60])
    df = calculate_macd(df, fast=12, slow=26, signal=9)
    df = calculate_rsi(df, period=14)
    df = calculate_bollinger(df, window=20, num_std=2.0)
    df = calculate_kdj(df, fastk_period=9, signal_period=3)

    # ── 检测信号 ─────────────────────────────────────────────
    # 把 symbol/name 注入 signals_cfg，让 check_signals 能正确填返回值
    cfg_for_check = {
        "symbol": symbol,
        "name":   name,
        **signals_cfg,   # 展开 rsi / kdj / volume / boll 等阈值
    }
    result = check_signals(df, cfg_for_check)
    if not result or not result.get("signals"):
        return None

    # ── 构建 alert 条目 ──────────────────────────────────────
    latest = df.iloc[-1]

    # volume 列不一定存在（部分数据源缺失），做安全处理
    volume_history = []
    if "volume" in df.columns:
        volume_history = [
            int(v) for v in df["volume"].tail(volume_history_days).tolist()
            if v == v  # 过滤 NaN（NaN != NaN）
        ]

    alert = {
        "symbol":           symbol,
        "name":             name,
        "signals":          result.get("signals", []),
        "score":            result.get("score", 0),
        "trend":            result.get("trend", "震荡"),
        "close":            result.get("close") or round(float(latest["close"]), 2),
        "change_pct":       result.get("change_pct"),
        "rsi":              result.get("rsi"),
        "dif":              result.get("dif"),
        "dea":              result.get("dea"),
        "macd_bar":         result.get("macd_bar"),
        "kdj_k":            result.get("kdj_k"),
        "kdj_d":            result.get("kdj_d"),
        "kdj_j":            result.get("kdj_j"),
        "boll_upper":       result.get("boll_upper"),
        "boll_lower":       result.get("boll_lower"),
        "boll_width":       result.get("boll_width"),
        "volume_confirmed": result.get("volume_confirmed", False),
        "update_time":      now_cn().isoformat(),
        # 取最近 N 天收盘价，供前端迷你折线图使用
        "price_history":    result.get("price_history") or [
            round(float(v), 2)
            for v in df["close"].tail(price_history_days).tolist()
            if v == v
        ],
        "volume_history":   volume_history,
    }
    return alert


# ==================== 主流程 ====================

def main():
    cfg_all = load_config()

    # ── 读取各配置节点（全部对齐 config.yaml 的真实 key）────
    runtime_cfg = cfg_all.get("runtime",  {})
    signals_cfg = cfg_all.get("signals",  {})
    push_cfg    = cfg_all.get("push",     {})
    output_cfg  = cfg_all.get("output",   {})
    watchlist   = cfg_all.get("watchlist", [])

    # ── 读取过滤开关 ─────────────────────────────────────────
    throttle    = push_cfg.get("throttle_minutes", 60)
    strong_only = push_cfg.get("strong_signal_only", True)
    min_signals = signals_cfg.get("min_signal_count", 1)
    min_score   = signals_cfg.get("min_score", 1)
    pretty_json = output_cfg.get("pretty_json", True)

    data_dir = Path(__file__).parent.parent / output_cfg.get("data_dir", "data")
    data_dir.mkdir(parents=True, exist_ok=True)

    force_run = os.getenv("FORCE_RUN", "false").lower() == "true"
    today     = date.today()

    # ── 交易日校验 ───────────────────────────────────────────
    use_cal = runtime_cfg.get("use_trading_calendar", True)
    if not force_run and use_cal and not is_trading_day(today):
        print(f"📅 今天 {today} 非交易日，尝试加载历史数据...")

        signals_path = data_dir / output_cfg.get("signals_file", "signals.json")
        if signals_path.exists():
            try:
                with open(signals_path, "r", encoding="utf-8") as f:
                    json.load(f)   # 只做合法性校验
                print("  ✅ 已有历史数据，保持原文件不变")
                write_run_summary(ok=[], fail=[], note="non-trading-day",
                                  pretty=pretty_json)
                return
            except (json.JSONDecodeError, OSError):
                print("  ⚠️  历史数据损坏，继续执行重建...")
        else:
            print("  ⚠️  无历史数据，继续执行首次数据构建...")

    # ── 校验 watchlist 不为空 ────────────────────────────────
    if not watchlist:
        print("❌ watchlist 为空，请检查 configs/config.yaml 的 watchlist 节点")
        write_run_summary(ok=[], fail=[], note="empty-watchlist", pretty=pretty_json)
        return

    print(f"\n🚀 开始处理 {len(watchlist)} 只股票...\n")

    ok_list   = []
    fail_list = []
    alerts    = []

    for stock in watchlist:
        # config.yaml 里 watchlist 条目用 "code"，兼容旧版 "symbol"
        symbol = stock.get("code") or stock.get("symbol", "")
        name   = stock.get("name", symbol)

        if not symbol:
            print(f"    ⚠️  跳过无效条目: {stock}")
            continue

        try:
            alert = process_stock(symbol, name, runtime_cfg, signals_cfg)
            ok_list.append(symbol)

            # 👇 新增 1：成功获取完一只股票的数据后，强制程序休息 3 秒
            time.sleep(3)

            if not alert:
                print(f"    — {name}: 无信号")
                continue

            sig_count = len(alert.get("signals", []))

            # ── 过滤：最低信号数 ─────────────────────────────
            if sig_count < min_signals:
                print(f"    — {name}: 信号数 {sig_count} < {min_signals}，过滤")
                continue

            # ── 过滤：最低得分 ───────────────────────────────
            if abs(alert["score"]) < min_score:
                print(f"    — {name}: 得分 {alert['score']} 绝对值 < {min_score}，过滤")
                continue

            # ── 过滤：strong_signal_only ─────────────────────
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
            print(f"    ✅ {name}: {sig_count} 个信号，"
                  f"得分 {alert['score']}，趋势 {alert.get('trend')}")

            # ── 飞书推送（带节流）───────────────────────────
            if should_push(symbol, throttle):
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

        except Exception as e:
            fail_list.append(symbol)
            print(f"    ❌ {name}({symbol}) 处理异常: {e}")
            import traceback
            traceback.print_exc()
            
            # 👇 新增 2：即使被拦截报错了，也要强制休息 3 秒，避免疯狂重试彻底激怒服务器
            time.sleep(3)

    # ── 写入结果文件 ─────────────────────────────────────────
    is_last = now_cn().hour >= 15

    write_signals_json(
        data_dir,
        alerts,
        watchlist_count=len(watchlist),
        is_last_trading=is_last,
        pretty=pretty_json,
    )
    write_run_summary(
        ok=ok_list,
        fail=fail_list,
        alerts_count=len(alerts),
        note="force_run" if force_run else "normal",
        pretty=pretty_json,
    )

    print(f"\n{'✅' if not fail_list else '⚠️ '} 完成："
          f"{len(ok_list)} 成功，{len(fail_list)} 失败，{len(alerts)} 个信号写入")

    # 👇 优化建议：把下面这两行注释掉或删掉
    # if fail_list:
    #     sys.exit(1)


if __name__ == "__main__":
    main()
