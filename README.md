# 社交媒体数据监控系统

## 项目概述

这是自用的社交媒体数据监控系统，代码和说明文档主要由 AI 完成，本麻瓜 @枝因 辅助。支持 7 个主流平台的粉丝数据获取和小红书创作者中心数据导出，并能自动同步到飞书多维表格。系统具备定时监控功能，支持通过飞书机器人进行交互式操作：

- 关注者数量获取：支持 Bilibili、YouTube、小红书、抖音、微博、微信公众号、知乎。
- 平台数据导出备份并增量更新：支持小红书。

## 核心功能

### 1. 粉丝数据监控
- 自动获取 7 个平台账号的订阅者数量
- 数据保存到本地CSV文件和飞书表格
- 支持多账号批量监控（除公众号，其粉丝数仅自己可见）
- 错误处理和重试机制

### 2. 小红书数据导出
- 自动登录小红书创作者中心
- 导出笔记数据（平台限制半年内记录）
- 增量更新到飞书表格和本地 csv 文件
- 本地Excel文件备份

### 3. 飞书机器人集成
- 支持@机器人触发数据更新
- 实时状态反馈
- 定时任务管理
- 自动Git备份

## 文件结构说明

### 主要脚本文件

- `monitor_bot.py` - 飞书机器人主程序，用于定时、触发数据更新和 git 备份。
- `followers_feishu.py` - 各平台关注者数据获取和飞书同步
- `redbook.py` - 小红书笔记数据处理并同步飞书多维表格

### 平台专用脚本

- `bilibili_followers.py` - B站粉丝数获取
- `youtube_followers.py` - YouTube订阅者数获取
- `redbook_followers.py` - 小红书粉丝数获取
- `douyin_followers.py` - 抖音粉丝数获取
- `weibo_followers.py` - 微博粉丝数获取
- `wechat_followers.py` - 微信公众号粉丝数获取
- `zhihu_followers.py` - 知乎关注者数获取
- `redbook_data.py` - 小红书创作者中心数据导出

### 配置和数据文件

- `*_cookie.json` - 各平台的Cookie配置文件
- `data/` - 数据存储目录
  - `followers.csv` - 粉丝数据CSV文件
  - `redbook_data.csv` - 小红书数据CSV文件
- `downloads/` - 下载文件存储目录
- `logs/` - 日志文件目录

## 配置指南

### 1. 飞书机器人配置

在 `monitor_bot.py`、`redbook.py` 和 `followers_feishu.py` 中配置：

```python
FEISHU_APP_ID = "your_app_id"          # 飞书应用ID
FEISHU_APP_SECRET = "your_app_secret"  # 飞书应用密钥
FEISHU_APP_TOKEN = "your_app_token"    # 飞书应用令牌
FEISHU_TABLE_ID = "your_table_id"      # 飞书多维表格ID
```

### 2. 账号配置

在 `followers_feishu.py` 中配置要监控账号的唯一标识符：

```python
# B站用户ID
BILIBILI_UIDS = ['18175054']  

# YouTube频道链接
YOUTUBE_CHANNELS = ['https://www.youtube.com/@channel/about']

# 小红书用户ID
REDBOOK_USER_IDS = ['609401890000000001009646']

# 抖音用户ID
DOUYIN_USER_IDS = ['superslow']

# 微博用户ID
WEIBO_USER_IDS = ['7737430801']

# 知乎用户slug
ZHIHU_USER_SLUGS = ['zhi-yin-233']
```

### 3. Cookie配置

项目中包含了空的Cookie配置文件模板，使用前请确认文件内容：

- `bilibili_cookie.json` - B站Cookie
- `redbook_cookie.json` - 小红书Cookie
- `douyin_cookie.json` - 抖音Cookie
- `weibo_cookie.json` - 微博Cookie

Cookie文件格式示例：
```json
[
  {
    "name": "SESSDATA",
    "value": "your_cookie_value",
    "domain": ".bilibili.com"
  }
]
```

## 使用方法

### 1. 环境准备

#### Python版本要求
- **最低要求**：Python 3.7+
- **推荐版本**：Python 3.8 或更高

#### 检查Python版本
```bash
python --version  # 应该显示 Python 3.7+
```

如果显示 Python 2.x 或命令不存在，请使用 `python3` 命令。

#### 安装依赖

**方式一：使用 requirements.txt（推荐）**
```bash
# 安装所有依赖
pip install -r requirements.txt

# 安装浏览器（用于微信、知乎、小红书）
playwright install
```

**方式二：手动安装**
```bash
# 核心依赖
pip install playwright pandas requests lark-oapi bilibili-api yt-dlp httpx xhs openpyxl

# 安装浏览器（用于微信、知乎、小红书）
playwright install
```

#### 虚拟环境（推荐）
```bash
# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# macOS/Linux:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
playwright install
```

### 2. 运行方式

#### 启动飞书机器人（推荐）
```bash
python monitor_bot.py
```

#### 单独运行粉丝数据获取
```bash
python followers_feishu.py
```

#### 单独运行小红书数据导出
```bash
python redbook.py
```

### 3. 飞书机器人命令

在飞书中@机器人并发送含有以下关键词的消息，即可立即执行：

- `粉丝/关注者/用户数` - 执行关注者数据同步
- `小红书` - 执行小红书数据同步

## 数据输出

### 1. 粉丝数据格式

CSV文件包含以下字段：
- 日期
- 账号名
- 平台
- 粉丝数

### 2. 小红书数据格式

包含笔记的详细数据：
- 首次发布时间
- 笔记标题
- 笔记类型
- 浏览量
- 点赞数
- 收藏数
- 评论数
- 分享数

## 注意事项

1. **Cookie有效性**：定期更新各平台的Cookie文件，避免登录失效
2. **访问频率**：系统内置了请求间隔，避免触发平台反爬机制
3. **数据权限**：小红书数据导出仅限半年内记录（平台限制）
4. **网络环境**：确保网络稳定，部分平台可能需要特定网络环境
5. **浏览器数据**：微信、知乎、小红书使用Playwright，会在本地创建浏览器数据目录

## 错误处理

系统包含完整的错误处理机制：
- 网络连接错误自动重试
- Cookie失效提醒
- 详细的错误日志记录
- 飞书消息通知异常情况

## 扩展功能

- 自动Git备份
- 增量数据更新
- 多账号批量处理
- 实时状态监控
- 定制化数据分析

## 许可证

本项目仅供学习和研究使用，请遵守各平台的使用条款和相关法律法规。

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

---

通过这个系统，您可以轻松监控多个社交媒体平台的数据变化，并实现自动化的数据收集和分析工作流。