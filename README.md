# 股票技术指标监控系统

基于 **akshare** 开源数据源的A股技术指标监控系统，自动计算MA、MACD、RSI、布林带等指标，实时推送交易信号。

## ✨ 功能特性

| 功能 | 说明 |
|------|------|
| 📈 **技术指标** | MA5/10/20/60、MACD、RSI、布林带、KDJ |
| 🚨 **信号检测** | 金叉/死叉、突破/回调、超买/超卖 |
| 📱 **飞书推送** | 强信号自动推送到飞书群 |
| 🌐 **网页展示** | GitHub Pages托管，随时查看 |
| ⏰ **定时更新** | 交易时段每15分钟自动更新 |

## 🚀 快速开始

### 1. 本地运行

```bash
# 克隆仓库
git clone https://github.com/Oceanus1227/stock-monitor.git
cd stock-monitor

# 安装依赖
pip install -r requirements.txt

# 运行监控脚本
python scripts/build_data.py

# 本地预览
python -m http.server 8000
# 访问 http://localhost:8000
```

### 2. GitHub自动化部署

#### 步骤1：Fork/创建仓库
- 在GitHub创建新仓库
- 上传本仓库所有文件

#### 步骤2：配置Secrets（可选）
在仓库 Settings → Secrets → Actions 中添加：

| Secret | 说明 | 必需 |
|--------|------|------|
| `FEISHU_WEBHOOK` | 飞书机器人Webhook地址 | 可选 |

> 不配置飞书Webhook也能正常运行，只是不会收到推送提醒

#### 步骤3：启用GitHub Pages
1. Settings → Pages
2. Source 选择 **Deploy from a branch**
3. Branch 选择 **main**，文件夹选 **/(root)**
4. 点击 Save

#### 步骤4：运行工作流
1. Actions → Refresh Stock Data
2. 点击 Run workflow
3. 等待完成（绿色✓）

#### 步骤5：访问Dashboard
```
https://Oceanus1227.github.io/stock-monitor/
```

## 📊 监控股票配置

编辑 `scripts/build_data.py`，修改 `WATCHLIST`：

```python
WATCHLIST = [
    {'code': '600519', 'name': '贵州茅台'},
    {'code': '000858', 'name': '五粮液'},
    {'code': '002594', 'name': '比亚迪'},
    {'code': '300750', 'name': '宁德时代'},
    # 添加你自己的股票...
]
```

**股票代码格式**：
- 上交所：6开头（如600519）
- 深交所：0或3开头（如000858、300750）

## 📡 运行频率

**自动运行**：
- 时间：工作日 9:00-15:00（北京时间）
- 频率：每15分钟更新一次

**手动运行**：
- Actions → Refresh Stock Data → Run workflow

## 🔔 飞书推送配置

1. 在飞书创建群聊（可以只有自己一个人）
2. 群设置 → 群机器人 → 添加机器人 → 自定义机器人
3. 复制 Webhook 地址
4. 在GitHub Secrets中添加 `FEISHU_WEBHOOK`
5. 有强信号时自动推送到飞书

## 🛠️ 技术栈

- **数据源**：akshare（免费开源A股数据）
- **技术指标**：pandas, numpy
- **自动化**：GitHub Actions
- **前端**：原生HTML/CSS
- **托管**：GitHub Pages

## 📁 项目结构

```
stock-monitor/
├── .github/workflows/refresh_data.yml  # GitHub Actions配置
├── scripts/
│   ├── technical_analysis.py           # 技术指标计算
│   └── build_data.py                   # 主程序
├── data/                               # 生成的数据文件
├── requirements.txt                    # Python依赖
├── index.html                          # 前端页面（自动生成）
└── README.md                           # 本文件
```

## ⚠️ 免责声明

本项目仅供学习研究使用，不构成投资建议。股市有风险，投资需谨慎。

## 📄 License

MIT License

---

**有问题或建议？欢迎提交Issue！**
