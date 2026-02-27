# WeiboCrawler 微博多模式爬虫

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue" alt="Python Version">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/Build-Stable-brightgreen" alt="Build Status">
</p>

基于 `asyncio` + `aiohttp` + `Playwright` 构建的高性能微博数据采集框架，支持关键词搜索、帖子链接、用户主页等多种抓取模式，具备完善的去重机制和结构化存储能力。

---

## 🌟 核心特性

### 🎯 多模式采集
- **关键词模式** (`keyword`): 按关键词搜索相关微博内容
- **链接模式** (`post_url`): 精准抓取指定微博帖子及评论
- **用户模式** (`user`): 批量采集用户主页信息及发布内容

### 🚀 高性能设计
- **异步并发**: 基于 `asyncio` 实现高并发数据采集
- **智能限流**: 内置请求频率控制，避免触发反爬机制
- **连接池复用**: TCP 连接池管理，提升请求效率
- **队列化写盘**: 后台协程批量刷盘，减少 IO 阻塞

### 🔧 工程化能力
- **增量去重**: 启动时自动加载历史数据索引，避免重复采集
- **断点续采**: 支持中断后继续采集，无需从头开始
- **结构化存储**: 按模式分目录，支持 CSV/JSON 双格式输出
- **媒体下载**: 自动识别并下载图片/视频资源

---

## 📁 项目架构

```
weibo_crawl/
├── weibo_crawler/              # 核心爬虫包
│   ├── scrapers/               # 抓取器模块
│   │   ├── keyword_scraper.py  # 关键词抓取器
│   │   ├── link_scraper.py     # 链接抓取器  
│   │   ├── user_scraper.py     # 用户抓取器
│   │   ├── post_scraper.py     # 帖子处理器
│   │   └── comment_scraper.py  # 评论抓取器
│   ├── models.py               # 数据模型定义
│   ├── storage.py              # 数据存储引擎
│   ├── http_client.py          # 异步HTTP客户端
│   ├── cookie_manager.py       # Cookie管理器
│   ├── media_downloader.py     # 媒体下载器
│   ├── config.py               # 配置管理中心
│   ├── user_config.py          # 用户配置文件
│   ├── utils.py                # 工具函数集合
│   └── logger.py               # 日志系统
├── run.py                      # 程序入口
├── requirements.txt            # 依赖清单
└── README.md                   # 项目文档
```

---

## ⚡ 快速开始

### 1. 环境准备

```bash
# Python 版本要求 ≥ 3.8
python3 --version

# 安装依赖包
pip install -r requirements.txt

# 安装 Playwright 浏览器驱动
playwright install chromium
```

### 2. 基础配置

编辑配置文件 `weibo_crawler/user_config.py`：

```python
# 选择抓取模式
SELECTED_MODES = ["keyword"]  # keyword / post_url / user

# 设置目标关键词
TARGET_KEYWORDS = ["人工智能", "机器学习"]

# 配置输出格式
SAVE_FORMAT = "csv"  # csv / json / both
```

### 3. 启动采集

```bash
python run.py
```

首次运行会自动弹出浏览器进行微博扫码登录，登录状态将被持久化保存。

---

## 🛠️ 详细配置

### 抓取模式配置

```python
# 支持的模式列表
SELECTED_MODES = [
    "keyword",    # 关键词搜索模式
    "post_url",   # 帖子链接模式  
    "user",       # 用户主页模式
]

# 模式别名兼容
# "url" / "link" / "comment" → "post_url"
# "personal" → "user"
```

### 目标配置

```python
# 关键词模式目标
TARGET_KEYWORDS = ["科技", "数码", "创新"]

# 帖子链接模式目标（仅支持微博帖子链接）
TARGET_POST_URLS = [
    "https://weibo.com/123456789/AbCdEfGhI",
    "https://weibo.com/detail/987654321"
]

# 用户模式目标（支持多种格式）
TARGET_USER_TARGETS = [
    "1234567890",                           # UID
    "https://weibo.com/u/1234567890",      # 用户主页链接
    "https://weibo.com/n/用户名",           # 用户名链接
    "@用户名"                               # 用户名
]
```

### 媒体采集配置

```python
# 媒体模式总开关
ENABLE_MEDIA_MODE = True

# 媒体类型过滤
MEDIA_TYPE_MODE = "all"  # all / image / video

# 媒体下载行为
ENABLE_MEDIA_DOWNLOAD = True        # 是否下载媒体文件
OVERWRITE_EXISTING_MEDIA = False    # 是否覆盖已存在文件

# 个人主页媒体（头像/封面）
ENABLE_PROFILE_MEDIA = True
```

### 评论采集配置

```python
# 各模式评论开关
ENABLE_COMMENTS_FOR_KEYWORD = True
ENABLE_COMMENTS_FOR_POST_URL = True  
ENABLE_COMMENTS_FOR_USER = True

# 评论层级控制
FETCH_TOP_LEVEL_COMMENTS = True    # 一级评论
FETCH_SUB_COMMENTS = True          # 楼中楼评论
```

### 数量控制配置

```python
# 帖子数量限制
MAX_POSTS_PER_SEARCH_PAGE = 20    # 每页最大帖子数
MAX_POSTS_PER_KEYWORD = 100       # 每关键词最大帖子数
MAX_POSTS_PER_USER = 50           # 每用户最大帖子数

# 评论数量限制
MAX_COMMENTS_PER_POST = 100       # 每帖子最大评论数
MAX_COMMENTS_PER_KEYWORD = 500    # 每关键词最大评论数

# 用户主页限制
MAX_USER_PAGES = 5                # 用户主页最大翻页数
```

---

## 📊 输出结构

### 文件组织结构

```
output/
├── text/                          # 结构化文本数据
│   ├── keyword/                   # 关键词模式输出
│   │   ├── posts.csv             # 帖子数据
│   │   ├── comments.csv          # 评论数据
│   │   └── media.csv             # 媒体数据
│   ├── post_url/                  # 链接模式输出
│   │   └── ...
│   └── user/                      # 用户模式输出
│       ├── posts.csv
│       ├── comments.csv
│       ├── media.csv
│       └── profiles.csv          # 用户主页信息
└── media/                         # 媒体文件
    ├── keyword/
    │   └── <author>/<post_id>/   # 按作者和帖子ID分类
    ├── post_url/
    │   └── ...
    └── user/
        └── ...
```

### 数据字段说明

#### 帖子数据 (posts.csv)
| 字段名 | 说明 |
|--------|------|
| 来源模式 | keyword / post_url / user |
| 帖子ID | 微博唯一标识符 |
| 帖子链接 | 完整微博链接 |
| 帖子发布者 | 发布用户名 |
| 是否带V | 认证标识 |
| 转发量 | 转发次数 |
| 评论量 | 评论总数 |
| 点赞量 | 点赞数量 |
| 帖子标题 | 内容摘要 |
| 帖子内容 | 完整微博文本 |

#### 评论数据 (comments.csv)
| 字段名 | 说明 |
|--------|------|
| 评论者 | 评论用户名 |
| 评论IP属地 | 地理位置信息 |
| 评论内容 | 评论文本内容 |
| 评论时间 | 发布时间 |
| 评论层级 | 主评论 / 楼中楼 |

#### 媒体数据 (media.csv)
| 字段名 | 说明 |
|--------|------|
| 媒体类型 | image / video |
| 媒体链接 | 原始媒体URL |
| 预览链接 | 缩略图URL |
| 本地文件路径 | 下载后的本地路径 |
| 下载状态 | pending / success / failed |

#### 用户信息 (profiles.csv)
| 字段名 | 说明 |
|--------|------|
| 用户ID | 微博UID |
| 昵称 | 用户显示名称 |
| 性别 | 性别信息 |
| 粉丝数 | 关注者数量 |
| 关注数 | 关注用户数 |
| 微博数 | 发布微博总数 |
| 是否认证 | 认证状态 |
| 简介 | 个人简介 |
| 头像链接 | 头像图片URL |
| 封面链接 | 封面图片URL |

---

## 🔧 高级用法

### 并发控制配置

在 `config.py` 中调整并发参数：

```python
@dataclass
class ConcurrencyConfig:
    keyword_concurrency: int = 2        # 关键词并发数
    post_detail_concurrency: int = 4    # 帖子详情并发数
    comment_concurrency: int = 3        # 评论抓取并发数
    user_concurrency: int = 2           # 用户抓取并发数
    media_download_concurrency: int = 6 # 媒体下载并发数
    global_concurrency: int = 8         # 全局请求并发上限
```

### 延迟策略配置

```python
@dataclass
class DelayConfig:
    request_delay: Tuple[float, float] = (0.3, 0.8)       # 请求间隔
    page_delay: Tuple[float, float] = (0.8, 1.5)          # 翻页延迟
    comment_page_delay: Tuple[float, float] = (0.3, 0.7)  # 评论翻页延迟
    user_page_delay: Tuple[float, float] = (0.6, 1.2)     # 用户页延迟
```

### 自定义请求头

```python
headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
    "referer": "https://weibo.com/",
    "accept": "application/json, text/plain, */*",
    # ... 其他自定义头部
}
```

---

## 📈 最佳实践

### 生产环境部署建议

1. **分批执行**: 大规模采集时建议分批次运行，避免触发风控
2. **监控日志**: 定期检查 `crawler.log` 日志文件
3. **资源控制**: 根据服务器性能调整并发参数
4. **数据备份**: 定期备份 `output/` 目录数据
5. **Cookie维护**: 定期更新登录状态

### 性能优化技巧

```python
# 增加并发数（需谨慎）
global_concurrency = 12  # 默认8

# 调整延迟范围
request_delay = (0.1, 0.5)  # 默认(0.3, 0.8)

# 批量写盘优化
writer_batch_size = 500     # 默认200
writer_flush_interval = 0.1 # 默认0.25
```

### 故障排查

常见问题及解决方案：

1. **验证码拦截**: 降低请求频率，增加随机延迟
2. **登录失效**: 删除 `.weibo_auth` 目录重新登录
3. **数据缺失**: 检查目标是否存在，调整数量限制
4. **内存溢出**: 减少并发数，及时清理中间数据

---

## ⚠️ 合规声明

### 法律风险提示

- 本项目仅供学习研究使用，请严格遵守相关法律法规
- 请尊重微博平台服务条款和用户隐私权
- 禁止用于商业盈利或大规模恶意爬取
- 采集的数据仅限个人学习研究用途

### 使用建议

1. **频率控制**: 合理设置请求间隔，避免给服务器造成压力
2. **数据使用**: 采集的数据不得用于非法用途
3. **版权尊重**: 注意微博内容的知识产权保护
4. **隐私保护**: 不得采集和传播用户敏感信息

---

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

### 联系方式

如有问题或建议，请通过以下方式联系：

- GitHub: [xqoder](https://github.com/xqoder)
- 邮箱: 719047501@qq.com
- 微信: wxazd0

### 开发环境搭建

```bash
# Fork 项目后克隆
git clone https://github.com/xqoder/weibo-crawler.git
cd weibo-crawler

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装开发依赖
pip install -r requirements-dev.txt
```

### 代码规范

- 遵循 PEP 8 代码风格
- 添加必要的类型注解
- 编写清晰的文档字符串
- 包含单元测试

### 提交规范

```bash
git commit -m "feat: 添加新的抓取模式"
git commit -m "fix: 修复评论去重逻辑"
git commit -m "docs: 更新使用文档"
```

---

## 📄 许可证

本项目采用 MIT 许可证，详见 [LICENSE](LICENSE) 文件。

---

## 🙏 致谢

- 感谢开源社区提供的优秀工具和库
- 感谢所有贡献者的支持和反馈
- 感谢微博平台提供的公开数据接口

---

## 📞 联系方式

如有问题或建议，请通过以下方式联系：

- GitHub: [xqoder](https://github.com/xqoder)
- 邮箱: 719047501@qq.com
- 微信: wxazd0

---