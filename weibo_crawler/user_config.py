# -*- coding: utf-8 -*-
"""
用户简化配置（建议只改这里）
=========================
"""
from typing import List

# 抓取模式可选:
# keyword(关键词) / post_url(仅帖子链接) /
# user(批量用户目标)
# 也支持别名: personal -> user, url/link -> post_url
SELECTED_MODES = [
    "keyword",
    # "post_url",  # 仅帖子链接，用户主页链接请放 TARGET_USER_TARGETS
    # "user",      # 可选：用于 TARGET_USER_TARGETS 批量跑用户
]

# 输出格式: csv / json / both
SAVE_FORMAT = "csv"

# 抓取目标（非常重要）
# ======================
# 每个模式只会读取它对应的目标列表：
#
# - keyword 模式  -> TARGET_KEYWORDS
# - post_url 模式 -> TARGET_POST_URLS
# - user 模式     -> TARGET_USER_TARGETS

# keyword 模式目标示例：["Apple", "华为"]
TARGET_KEYWORDS: List[str] = [
    "Apple",
    "华为",
    "小米",
    "OPPO",
    "VIVO",
    "魅族",
    "三星",

]

# post_url 模式目标示例：
# - 帖子链接: https://weibo.com/xxxx/xxxx
TARGET_POST_URLS: List[str] = [
    "https://weibo.com/?layerid=5270588752661663",
    "https://weibo.com/?layerid=5269414140185428",
    "https://weibo.com/?layerid=5270292035279441",
]

# 兼容字段：已不再作为独立模式使用（保留仅为兼容旧配置）
TARGET_MEDIA_URLS: List[str] = []

# user 模式目标（你当前使用这个）
# 支持格式：
# 1) uid: "7040797671"
# 2) 用户主页链接: "https://weibo.com/u/7040797671"
# 3) 用户名主页: "https://weibo.com/n/用户名"（部分账号可能受接口限制）
# 4) 用户名: "@用户名" 或 "用户名"
TARGET_USER_TARGETS: List[str] = [
    "https://weibo.com/u/7040797671",
    "https://weibo.com/u/5157634999",
    "https://weibo.com/u/2615417307",
    "https://weibo.com/u/1796087453",
]

# 输出目录
OUTPUT_DIR = "output"
TEXT_OUTPUT_DIR = "text"
MEDIA_OUTPUT_DIR = "media"

# 媒体模式总开关
# True: 在 keyword/post_url/user 抓帖子时抽取媒体；并按下载策略处理
# False: 全部媒体处理关闭
ENABLE_MEDIA_MODE = True
# 媒体类型过滤:
# - "all"   : 图片+视频都抓
# - "image" : 只抓图片
# - "video" : 只抓视频
MEDIA_TYPE_MODE = "all"

# 媒体下载行为
ENABLE_MEDIA_DOWNLOAD = True   # True: 下载图片/视频到 output/media；False: 只记录媒体链接
OVERWRITE_EXISTING_MEDIA = True  # True: 覆盖本地已存在同名媒体文件

# 个人主页媒体（头像/背景图）
# 仅在 ENABLE_MEDIA_MODE=True 时生效
ENABLE_PROFILE_MEDIA = True

# 是否抓取主页信息（用户昵称、粉丝、关注、头像链接、封面链接等）
# 作用于 user 模式目标
ENABLE_PROFILE_INFO = True

# 评论抓取开关（按来源模式）
ENABLE_COMMENTS_FOR_KEYWORD = True
ENABLE_COMMENTS_FOR_POST_URL = True
ENABLE_COMMENTS_FOR_USER = True

# 评论层级开关
FETCH_TOP_LEVEL_COMMENTS = True  # 一级评论
FETCH_SUB_COMMENTS = True        # 二级评论（楼中楼）

# 抓取数量控制（0 表示不限）
MAX_POSTS_PER_SEARCH_PAGE = 20     # 每个搜索结果页最多处理多少条帖子
MAX_COMMENTS_PER_POST = 100        # 每个帖子最多抓多少条评论（主评+楼中楼）
MAX_POSTS_PER_KEYWORD = 4          # 每个关键词最多抓多少条帖子
MAX_COMMENTS_PER_KEYWORD = 100     # 每个关键词最多抓多少条评论
MAX_USER_PAGES = 2                # 每个用户主页最多翻页数（0=不限；内部有安全上限防止异常死循环）
MAX_POSTS_PER_USER = 20           # 每个用户主页最多抓多少条帖子
