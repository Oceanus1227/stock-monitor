# Stock Monitor Demo

基于iFinD数据源的A股技术指标监控系统

## 功能特性

- 📈 技术指标计算：MA、MACD、RSI、布林带
- 🚨 自动信号检测：金叉/死叉、突破/回调、超买/超卖
- 📱 飞书Webhook推送
- 🌐 GitHub Pages展示
- ⏰ 定时自动更新（GitHub Actions）

## 安装依赖

```bash
pip install -r requirements.txt
```

## 本地运行

```bash
# 设置iFinD账号（需自行申请）
export IFIND_USER="your_username"
export IFIND_PASS="your_password"

# 运行监控脚本
python scripts/build_data.py

# 本地预览
python -m http.server 8000
# 访问 http://localhost:8000
```

## GitHub部署

1. Fork本仓库
2. 设置Secrets：`IFIND_USER` 和 `IFIND_PASS`
3. 启用GitHub Pages
4. 手动运行Actions或等待定时触发

## 监控股票配置

编辑 `scripts/build_data.py` 中的 `WATCHLIST` 变量

## 数据来源

- iFinD金融数据终端（需账号）
- 每日自动更新

## License


