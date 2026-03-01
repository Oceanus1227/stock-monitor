#!/usr/bin/env python3
"""
股票技术指标监控主脚本
支持akshare数据源
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path

# 添加脚本目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from technical_analysis import (
    calculate_ma, calculate_macd, calculate_rsi,
    calculate_bollinger, calculate_kdj, check_signals
)


# ==================== 配置区 ====================

# 监控股票列表（A股代码）
WATCHLIST = [
    {'code': '600519', 'name': '贵州茅台'},  # 白酒龙头
    {'code': '000858', 'name': '五粮液'},    # 白酒
    {'code': '002594', 'name': '比亚迪'},    # 新能源汽车
    {'code': '300750', 'name': '宁德时代'},  # 锂电池
    {'code': '000333', 'name': '美的集团'},  # 家电
    {'code': '600036', 'name': '招商银行'},  # 银行
]

# 飞书Webhook（可选，用于推送提醒）
FEISHU_WEBHOOK = os.getenv('FEISHU_WEBHOOK', '')

# 数据文件路径
DATA_DIR = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)


# ==================== 数据获取 ====================

def get_mock_data(code, days=60):
    """
    模拟数据生成（用于Demo演示）
    实际使用时替换为akshare调用
    """
    import pandas as pd
    import numpy as np
    
    # 生成模拟股价数据
    end_date = datetime.now()
    dates = pd.date_range(end=end_date, periods=days, freq='B')  # 工作日
    
    np.random.seed(int(code))  # 固定种子使结果可复现
    
    # 随机游走生成价格
    returns = np.random.normal(0.001, 0.02, days)  # 日均收益0.1%，波动2%
    price = 100 * np.exp(np.cumsum(returns))
    
    # 生成OHLCV数据
    data = pd.DataFrame({
        'date': dates,
        'open': price * (1 + np.random.normal(0, 0.005, days)),
        'high': price * (1 + abs(np.random.normal(0, 0.015, days))),
        'low': price * (1 - abs(np.random.normal(0, 0.015, days))),
        'close': price,
        'volume': np.random.randint(1000000, 10000000, days)
    })
    
    data.set_index('date', inplace=True)
    return data

def get_akshare_data(code, days=60):
    """
    从akshare获取A股历史数据（真实数据）
    """
    try:
    import akshare as ak
    import pandas as pd
    from datetime import datetime, timedelta

    # 计算日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days + 20)

    # 获取历史行情 - 前复权数据
    df = ak.stock_zh_a_hist(
    symbol=code,
    period="daily",
    start_date=start_date.strftime('%Y%m%d'),
    end_date=end_date.strftime('%Y%m%d'),
    adjust="qfq"
    )

    if df is None or len(df) == 0:
        print(f"⚠️ akshare无数据，使用模拟数据: {code}")
        return get_mock_data(code, days)

    # 重命名列以兼容原有代码
    df = df.rename(columns={
    '日期': 'date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume'
    })

    # 转换日期
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # 确保列名正确
    df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    print(f"✅ akshare数据获取成功: {code} ({len(df)}条)")
    return df

    except Exception as e:
    print(f"⚠️ akshare获取失败({e})，使用模拟数据: {code}")
    return get_mock_data(code, days)


# ==================== 主流程 ====================

def analyze_stock(stock_info):
    """分析单只股票"""
    code = stock_info['code']
    name = stock_info['name']
    
    print(f"\n📊 分析 {code} {name}...")
    
    # 获取数据
    data = get_akshare_data(code, days=60)
    
    if data is None or len(data) < 20:
        print(f"❌ 数据不足: {code}")
        return None
    
    # 计算技术指标
    data = calculate_ma(data)
    data = calculate_macd(data)
    data = calculate_rsi(data)
    data = calculate_bollinger(data)
    data = calculate_kdj(data)
    
    # 检查信号
    result = check_signals(data, code, name)
    
    if result:
        print(f"✅ 发现 {len(result['signals'])} 个信号")
        for sig in result['signals']:
            print(f"   - [{sig['type']}] {sig['desc']} ({sig['strength']})")
    else:
        print(f"ℹ️ 无显著信号")
    
    return result


def send_feishu_alert(alerts):
    """发送飞书提醒"""
    if not FEISHU_WEBHOOK or not alerts:
        return
    
    # 只发送重要信号（强信号或买入信号）
    important_alerts = [
        a for a in alerts 
        if any(s['strength'] == '强' or '买入' in s.get('action', '') for s in a['signals'])
    ]
    
    if not important_alerts:
        return
    
    message_parts = ["🚨 股票监控提醒\n"]
    message_parts.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    message_parts.append("=" * 30 + "\n")
    
    for alert in important_alerts[:5]:  # 最多5条
        message_parts.append(f"\n📈 {alert['name']} ({alert['code']})")
        message_parts.append(f"💰 现价: ¥{alert['price']}")
        message_parts.append(f"📊 趋势: {alert['trend']}")
        
        for sig in alert['signals']:
            if sig['strength'] == '强':
                message_parts.append(f"🔴 [{sig['indicator']}] {sig['desc']}")
    
    message = "\n".join(message_parts)
    
    try:
        response = requests.post(FEISHU_WEBHOOK, json={
            "msg_type": "text",
            "content": {"text": message}
        })
        print(f"✅ 飞书推送成功")
    except Exception as e:
        print(f"❌ 飞书推送失败: {e}")


def generate_html(alerts, all_data):
    """生成HTML报告"""
    html_template = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>股票技术指标监控</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 20px;
        }
        .header h1 { font-size: 28px; margin-bottom: 10px; }
        .header .time { opacity: 0.9; }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            text-align: center;
        }
        .stat-card .number { font-size: 32px; font-weight: bold; color: #667eea; }
        .stat-card .label { color: #666; margin-top: 5px; }
        .alerts-section { margin-bottom: 20px; }
        .section-title {
            font-size: 20px;
            margin-bottom: 15px;
            padding-left: 10px;
            border-left: 4px solid #667eea;
        }
        .stock-card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 15px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        .stock-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        .stock-name { font-size: 18px; font-weight: bold; }
        .stock-code { color: #999; font-size: 14px; }
        .stock-price {
            text-align: right;
        }
        .price { font-size: 24px; font-weight: bold; }
        .change { font-size: 14px; }
        .change.up { color: #e74c3c; }
        .change.down { color: #27ae60; }
        .indicators {
            display: flex;
            gap: 15px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }
        .indicator {
            background: #f8f9fa;
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 13px;
        }
        .indicator span { color: #666; }
        .signals {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .signal {
            display: flex;
            align-items: center;
            padding: 10px 15px;
            border-radius: 8px;
            font-size: 14px;
        }
        .signal.buy { background: #fff3f3; border-left: 3px solid #e74c3c; }
        .signal.sell { background: #f0fff0; border-left: 3px solid #27ae60; }
        .signal.neutral { background: #fffbf0; border-left: 3px solid #f39c12; }
        .signal-icon { margin-right: 10px; font-size: 16px; }
        .signal-content { flex: 1; }
        .signal-type { font-weight: bold; margin-bottom: 2px; }
        .signal-desc { color: #666; font-size: 12px; }
        .signal-strength {
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: bold;
        }
        .signal-strength.strong { background: #e74c3c; color: white; }
        .signal-strength.medium { background: #f39c12; color: white; }
        .trend-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
        }
        .trend-badge.bull { background: #ffebee; color: #c62828; }
        .trend-badge.bear { background: #e8f5e9; color: #2e7d32; }
        .trend-badge.neutral { background: #fff3e0; color: #ef6c00; }
        .no-signals {
            text-align: center;
            padding: 40px;
            color: #999;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>📈 股票技术指标监控</h1>
        <div class="time">更新时间：''' + datetime.now().strftime('%Y-%m-%d %H:%M') + '''</div>
    </div>
    
    <div class="stats">
        <div class="stat-card">
            <div class="number">''' + str(len(WATCHLIST)) + '''</div>
            <div class="label">监控股票</div>
        </div>
        <div class="stat-card">
            <div class="number">''' + str(len(alerts)) + '''</div>
            <div class="label">今日信号</div>
        </div>
        <div class="stat-card">
            <div class="number">''' + str(sum(1 for a in alerts if a['trend'] == '强势')) + '''</div>
            <div class="label">强势股票</div>
        </div>
    </div>
    
    <div class="alerts-section">
        <h2 class="section-title">🔔 技术信号</h2>
        ''' + generate_alerts_html(alerts) + '''
    </div>
</body>
</html>'''
    
    return html_template


def generate_alerts_html(alerts):
    """生成信号列表HTML"""
    if not alerts:
        return '<div class="no-signals">暂无技术信号</div>'
    
    html_parts = []
    
    # 按趋势强度排序
    alerts_sorted = sorted(alerts, key=lambda x: x['score'], reverse=True)
    
    for alert in alerts_sorted:
        trend_class = 'bull' if alert['trend'] == '强势' else ('bear' if alert['trend'] == '弱势' else 'neutral')
        change_class = 'up' if alert['change_pct'] > 0 else 'down'
        change_symbol = '+' if alert['change_pct'] > 0 else ''
        
        html_parts.append(f'''
        <div class="stock-card">
            <div class="stock-header">
                <div>
                    <div class="stock-name">{alert['name']} <span class="stock-code">{alert['code']}</span></div>
                    <span class="trend-badge {trend_class}">{alert['trend']}</span>
                </div>
                <div class="stock-price">
                    <div class="price">¥{alert['price']}</div>
                    <div class="change {change_class}">{change_symbol}{alert['change_pct']}%</div>
                </div>
            </div>
            <div class="indicators">
                <div class="indicator">MA5: <span>{alert['ma5']}</span></div>
                <div class="indicator">MA10: <span>{alert['ma10']}</span></div>
                <div class="indicator">MA20: <span>{alert['ma20']}</span></div>
                <div class="indicator">RSI: <span>{alert['rsi']}</span></div>
                <div class="indicator">MACD: <span>{alert['macd']}</span></div>
            </div>
            <div class="signals">
        ''')
        
        for signal in alert['signals']:
            signal_class = 'buy' if '买入' in signal.get('action', '') or signal['type'] in ['突破', '金叉', '超卖'] else \
                          ('sell' if '卖出' in signal.get('action', '') or signal['type'] in ['回调', '死叉', '超买'] else 'neutral')
            strength_class = 'strong' if signal['strength'] == '强' else 'medium'
            icon = '📈' if signal_class == 'buy' else ('📉' if signal_class == 'sell' else '⚠️')
            
            html_parts.append(f'''
                <div class="signal {signal_class}">
                    <span class="signal-icon">{icon}</span>
                    <div class="signal-content">
                        <div class="signal-type">[{signal['indicator']}] {signal['type']}</div>
                        <div class="signal-desc">{signal['desc']}</div>
                    </div>
                    <span class="signal-strength {strength_class}">{signal['strength']}</span>
                </div>
            ''')
        
        html_parts.append('</div></div>')
    
    return '\n'.join(html_parts)


def main():
    """主函数"""
    print("=" * 50)
    print("🚀 股票技术指标监控系统")
    print("=" * 50)
    
    all_alerts = []
    all_data = {}
    
    # 分析每只股票
    for stock in WATCHLIST:
        result = analyze_stock(stock)
        if result:
            all_alerts.append(result)
            all_data[stock['code']] = result
    
    # 保存JSON数据
    with open(DATA_DIR / 'alerts.json', 'w', encoding='utf-8') as f:
        json.dump(all_alerts, f, ensure_ascii=False, indent=2)
    
    with open(DATA_DIR / 'snapshot.json', 'w', encoding='utf-8') as f:
        json.dump({
            'update_time': datetime.now().isoformat(),
            'total_stocks': len(WATCHLIST),
            'alert_count': len(all_alerts),
            'data': all_data
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ 分析完成: {len(WATCHLIST)}只股票，{len(all_alerts)}个信号")
    
    # 生成HTML报告
    html_content = generate_html(all_alerts, all_data)
    with open(DATA_DIR / '..' / 'index.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("✅ HTML报告已生成")
    
    # 发送飞书提醒
    send_feishu_alert(all_alerts)
    
    return all_alerts


if __name__ == '__main__':
    main()
