# scripts/feishu_card.py
import requests
import json


def send_signal_card(webhook_url, stock_code, stock_name,
                     price, change_pct, signals, key_levels,
                     signal_type: str = 'buy'):
    """
    signal_type: 'buy' | 'sell_profit' | 'sell_loss'
    """
    hit_count = sum(1 for s in signals.values() if s['hit'])
    stars     = '⭐' * hit_count + '☆' * (5 - hit_count)

    config = {
        'buy':         ('🟢 买入信号', 'green',  '📈 查看K线'),
        'sell_profit': ('💰 止盈信号', 'orange', '📊 查看K线'),
        'sell_loss':   ('🚨 止损信号', 'red',    '📉 查看K线'),
    }
    title_text, header_color, btn_text = config[signal_type]
    change_str = f"▲ +{change_pct:.1f}%" if change_pct >= 0 else f"▼ {change_pct:.1f}%"

    indicator_text = '\n'.join([
        f"{'✅' if s['hit'] else '➖'} **{name}**　{s['desc']}"
        for name, s in signals.items()
    ])

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"{title_text}  ·  {stock_name} {stock_code}"
                },
                "template": header_color
            },
            "elements": [
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**现价**\n¥{price:.2f}  {change_str}"
                            }
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**信号强度**\n{stars}  ({hit_count}/5)"
                            }
                        }
                    ]
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**📊 指标共振**\n{indicator_text}"
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**压力位**\n¥{key_levels['resistance']}"
                            }
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**支撑位**\n¥{key_levels['support']}"
                            }
                        }
                    ]
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"🛡️ **止损位**　¥{key_levels['stop_loss']}"
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": btn_text},
                            "type": "primary",
                            "url": f"https://www.futunn.com/quote/stock?market=SZ&code={stock_code}"
                        }
                    ]
                }
            ]
        }
    }

    resp = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(card)
    )
    return resp.json()
