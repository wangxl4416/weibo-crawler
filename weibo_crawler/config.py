# -*- coding: utf-8 -*-
"""
配置管理模块
===========
集中管理爬虫配置，支持多种抓取模式。
用户日常建议只改 `weibo_crawler/user_config.py`。

支持模式：
    - 关键词模式
    - 链接模式（支持多种微博链接格式）
    - 用户模式（抓取用户主页帖子）

主页信息（profile）由独立开关控制，不再作为单独模式：
    - ENABLE_PROFILE_INFO
媒体抓取由独立开关控制，不再作为单独模式：
    - ENABLE_MEDIA_MODE
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from .user_config import (
    ENABLE_MEDIA_DOWNLOAD,
    ENABLE_MEDIA_MODE,
    MEDIA_TYPE_MODE,
    ENABLE_PROFILE_MEDIA,
    ENABLE_PROFILE_INFO,
    ENABLE_COMMENTS_FOR_KEYWORD,
    ENABLE_COMMENTS_FOR_POST_URL,
    ENABLE_COMMENTS_FOR_USER,
    FETCH_TOP_LEVEL_COMMENTS,
    FETCH_SUB_COMMENTS,
    MAX_COMMENTS_PER_KEYWORD,
    MAX_COMMENTS_PER_POST,
    MAX_POSTS_PER_KEYWORD,
    MAX_POSTS_PER_USER,
    MAX_USER_PAGES,
    MAX_POSTS_PER_SEARCH_PAGE,
    MEDIA_OUTPUT_DIR,
    OUTPUT_DIR,
    OVERWRITE_EXISTING_MEDIA,
    SAVE_FORMAT,
    SELECTED_MODES,
    TEXT_OUTPUT_DIR,
    TARGET_KEYWORDS,
    TARGET_MEDIA_URLS,
    TARGET_POST_URLS,
    TARGET_USER_TARGETS,
)


@dataclass
class ConcurrencyConfig:
    """并发控制配置"""
    keyword_concurrency: int = 2        # 同时处理关键词数
    post_detail_concurrency: int = 4    # 同时处理帖子详情数
    comment_concurrency: int = 3        # 同时抓取评论帖子数
    user_concurrency: int = 2           # 同时抓取用户数
    media_download_concurrency: int = 6 # 同时下载媒体文件数
    global_concurrency: int = 8         # 全局请求并发上限


@dataclass
class DelayConfig:
    """延迟策略配置（秒）"""
    request_delay: Tuple[float, float] = (0.3, 0.8)       # 每次请求后延迟
    page_delay: Tuple[float, float] = (0.8, 1.5)          # 搜索翻页延迟
    comment_page_delay: Tuple[float, float] = (0.3, 0.7)  # 评论翻页延迟
    user_page_delay: Tuple[float, float] = (0.6, 1.2)     # 用户时间线翻页延迟


@dataclass
class RequestConfig:
    """请求配置"""
    timeout: int = 15
    retry_times: int = 3
    max_search_pages: int = 30
    max_posts_per_search_page: int = MAX_POSTS_PER_SEARCH_PAGE
    max_comment_pages: int = 80
    max_sub_comment_pages: int = 30
    max_comments_per_post: int = MAX_COMMENTS_PER_POST
    max_posts_per_keyword: int = MAX_POSTS_PER_KEYWORD
    max_comments_per_keyword: int = MAX_COMMENTS_PER_KEYWORD
    max_records_per_keyword: int = MAX_COMMENTS_PER_KEYWORD  # 兼容旧字段（等同关键词评论上限）
    max_user_pages: int = MAX_USER_PAGES
    max_posts_per_user: int = MAX_POSTS_PER_USER


@dataclass
class TargetConfig:
    """抓取目标配置"""
    keywords: List[str] = field(default_factory=lambda: list(TARGET_KEYWORDS))
    post_urls: List[str] = field(default_factory=lambda: list(TARGET_POST_URLS))
    media_urls: List[str] = field(default_factory=lambda: list(TARGET_MEDIA_URLS))
    user_targets: List[str] = field(default_factory=lambda: list(TARGET_USER_TARGETS))  # uid / 主页链接 / 用户名


@dataclass
class OutputConfig:
    """输出配置（简化版：只保留目录配置）"""
    output_dir: str = OUTPUT_DIR
    text_output_dir: str = TEXT_OUTPUT_DIR
    media_output_dir: str = MEDIA_OUTPUT_DIR


@dataclass
class DownloadConfig:
    """下载行为配置"""
    enable_media_download: bool = ENABLE_MEDIA_DOWNLOAD
    overwrite_existing: bool = OVERWRITE_EXISTING_MEDIA


@dataclass
class MediaConfig:
    """媒体抓取配置"""
    enable_mode: bool = ENABLE_MEDIA_MODE
    media_type_mode: str = MEDIA_TYPE_MODE
    enable_profile_media: bool = ENABLE_PROFILE_MEDIA


@dataclass
class ProfileConfig:
    """主页信息抓取配置"""
    enable_profile_info: bool = ENABLE_PROFILE_INFO


@dataclass
class CommentConfig:
    """评论抓取配置"""
    enable_for_keyword: bool = ENABLE_COMMENTS_FOR_KEYWORD
    enable_for_post_url: bool = ENABLE_COMMENTS_FOR_POST_URL
    enable_for_user: bool = ENABLE_COMMENTS_FOR_USER
    fetch_top_level: bool = FETCH_TOP_LEVEL_COMMENTS
    fetch_sub_level: bool = FETCH_SUB_COMMENTS


@dataclass
class CrawlerConfig:
    """爬虫主配置"""

    # ===== 用户常改配置（推荐只改这两项） =====
    # 可选模式: keyword, post_url, user
    # 兼容别名: personal->user, link/url/comment->post_url（profile/media 为兼容旧配置，会忽略）
    selected_modes: List[str] = field(
        default_factory=lambda: list(SELECTED_MODES)
    )
    # 可选: csv / json / both
    save_format: str = SAVE_FORMAT

    targets: TargetConfig = field(default_factory=TargetConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    media: MediaConfig = field(default_factory=MediaConfig)
    profile: ProfileConfig = field(default_factory=ProfileConfig)
    comments: CommentConfig = field(default_factory=CommentConfig)

    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    delay: DelayConfig = field(default_factory=DelayConfig)
    request: RequestConfig = field(default_factory=RequestConfig)

    headers: Dict[str, str] = field(default_factory=lambda: {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en,zh-CN;q=0.9,zh;q=0.8",
        "client-version": "3.0.0",
        "priority": "u=1, i",
        "referer": "https://weibo.com/",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "server-version": "v2026.02.10.2",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    })
    cookies: Dict[str, str] = field(default_factory=dict)

    @property
    def keywords(self) -> List[str]:
        """
        向后兼容：保留旧版 `config.keywords` 访问方式。
        """
        return self.targets.keywords

    @keywords.setter
    def keywords(self, value: List[str]) -> None:
        self.targets.keywords = value

    @property
    def active_keyword_targets(self) -> List[str]:
        """
        仅返回当前启用模式对应的关键词目标。
        未启用 keyword 模式时，即使配置了关键词，也会被忽略。
        """
        if not self.is_mode_enabled("keyword"):
            return []
        return self._normalize_targets(self.targets.keywords)

    @property
    def active_post_url_targets(self) -> List[str]:
        """
        仅返回当前启用模式对应的链接目标。
        未启用 post_url 模式时，即使配置了链接，也会被忽略。
        """
        if not self.is_mode_enabled("post_url"):
            return []
        return self._normalize_targets(self.targets.post_urls)

    @property
    def active_user_targets(self) -> List[str]:
        """
        仅返回当前启用模式对应的用户目标。
        未启用 user 模式时，即使配置了用户目标，也会被忽略。
        """
        if not self.is_mode_enabled("user"):
            return []
        return self._normalize_targets(self.targets.user_targets)

    @property
    def cookie_string(self) -> str:
        """将 Cookie 字典转为请求头字符串"""
        return "; ".join(f"{k}={v}" for k, v in self.cookies.items())

    @property
    def headers_with_cookie(self) -> Dict[str, str]:
        """返回包含 Cookie 的完整请求头"""
        final_headers = {**self.headers, "cookie": self.cookie_string}
        if "XSRF-TOKEN" in self.cookies:
            final_headers["x-xsrf-token"] = self.cookies["XSRF-TOKEN"]
        return final_headers

    @property
    def comments_output_file(self) -> str:
        """评论 CSV 输出路径"""
        return os.path.join(
            self.output.output_dir,
            self.output.text_output_dir,
            "comments.csv",
        )

    def _resolved_modes(self) -> Set[str]:
        """
        解析模式配置（仅使用 selected_modes）。
        """
        aliases = {
            "keyword": "keyword",
            "post_url": "post_url",
            "url": "post_url",
            "link": "post_url",
            "comment": "post_url",
            "user": "user",
            "personal": "user",
            "profile": "profile",
        }

        normalized = {
            aliases.get(str(item).strip().lower(), str(item).strip().lower())
            for item in self.selected_modes
            if str(item).strip()
        }
        # profile/media 不再作为模式，兼容旧配置时忽略
        normalized.discard("profile")
        normalized.discard("media")
        if normalized:
            return normalized

        # 空配置时回到默认模式
        return {"keyword", "post_url", "user"}

    @property
    def enabled_modes(self) -> List[str]:
        return sorted(self._resolved_modes())

    def is_mode_enabled(self, mode: str) -> bool:
        aliases = {
            "url": "post_url",
            "link": "post_url",
            "comment": "post_url",
            "personal": "user",
        }
        normalized = aliases.get(mode.strip().lower(), mode.strip().lower())
        return normalized in self._resolved_modes()

    @property
    def media_mode_enabled(self) -> bool:
        return bool(self.media.enable_mode)

    @property
    def media_type_mode_enabled(self) -> str:
        value = str(self.media.media_type_mode or "").strip().lower()
        if value in {"all", "both"}:
            return "all"
        if value in {"image", "img", "photo", "pic", "pics"}:
            return "image"
        if value in {"video", "vid"}:
            return "video"
        return "all"

    @property
    def profile_media_enabled(self) -> bool:
        return self.media_mode_enabled and bool(self.media.enable_profile_media)

    @property
    def profile_info_enabled(self) -> bool:
        return bool(self.profile.enable_profile_info)

    @property
    def fetch_comments_enabled(self) -> bool:
        return (
            (self.comments.enable_for_keyword or self.comments.enable_for_post_url or self.comments.enable_for_user)
            and (self.comments.fetch_top_level or self.comments.fetch_sub_level)
        )

    @property
    def fetch_top_level_comments_enabled(self) -> bool:
        return self.comments.fetch_top_level

    @property
    def fetch_sub_comments_enabled(self) -> bool:
        return self.comments.fetch_sub_level

    def should_fetch_comments(self, source_mode: str) -> bool:
        normalized = str(source_mode or "").strip().lower()
        if normalized in {"post_url", "media_url"}:
            enabled = self.comments.enable_for_post_url
        elif normalized == "keyword":
            enabled = self.comments.enable_for_keyword
        elif normalized == "user":
            enabled = self.comments.enable_for_user
        else:
            enabled = False
        return enabled and (self.fetch_top_level_comments_enabled or self.fetch_sub_comments_enabled)

    @property
    def max_comments_per_post_enabled(self) -> int:
        return max(0, int(self.request.max_comments_per_post or 0))

    @property
    def max_user_pages_enabled(self) -> int:
        """
        用户主页翻页上限。
        - >0: 按配置限制页数
        - 0 : 不限（由调用方决定安全上限）
        """
        return max(0, int(self.request.max_user_pages or 0))

    def max_posts_for_source(self, source_mode: str) -> int:
        normalized = str(source_mode or "").strip().lower()
        if normalized == "keyword":
            return max(0, int(self.request.max_posts_per_keyword or 0))
        return 0

    def max_comments_for_source(self, source_mode: str) -> int:
        normalized = str(source_mode or "").strip().lower()
        if normalized == "keyword":
            return max(0, int(self.request.max_comments_per_keyword or 0))
        return 0

    @property
    def normalized_save_format(self) -> str:
        value = str(self.save_format or "").strip().lower()
        if value in {"csv", "json", "both"}:
            return value
        return "both"

    @property
    def write_csv(self) -> bool:
        return self.normalized_save_format in {"csv", "both"}

    @property
    def write_json(self) -> bool:
        return self.normalized_save_format in {"json", "both"}

    def should_capture_media_type(self, media_type: str) -> bool:
        """
        判断当前媒体类型是否应被采集/下载。
        """
        normalized = str(media_type or "").strip().lower()
        if normalized in {"img", "photo", "pic", "pics"}:
            normalized = "image"
        if normalized in {"vid"}:
            normalized = "video"
        mode = self.media_type_mode_enabled
        if mode == "all":
            return normalized in {"image", "video"}
        return normalized == mode

    @staticmethod
    def _normalize_targets(targets: List[str]) -> List[str]:
        """
        统一目标列表规范：
            - 去除空白项
            - strip 前后空格
            - 保留原顺序去重
        """
        result: List[str] = []
        seen: Set[str] = set()
        for item in targets:
            value = str(item or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result


COMMENT_COLUMNS = [
    "来源模式",
    "来源目标",
    "帖子ID",
    "帖子链接",
    "帖子标题",
    "帖子发布者",
    "帖子发布时间",
    "评论者",
    "评论IP属地",
    "评论内容",
    "评论时间",
    "评论层级",
]

MEDIA_COLUMNS = [
    "来源模式",
    "来源目标",
    "帖子ID",
    "帖子链接",
    "帖子发布者",
    "媒体类型",
    "媒体链接",
    "预览链接",
    "本地文件路径",
    "下载状态",
    "帖子发布时间",
]

POST_COLUMNS = [
    "来源模式",
    "来源目标",
    "帖子ID",
    "帖子链接",
    "帖子发布者",
    "是否带V",
    "帖子发布时间",
    "转发量",
    "评论量",
    "点赞量",
    "帖子标题",
    "帖子内容",
]

PROFILE_COLUMNS = [
    "用户ID",
    "昵称",
    "性别",
    "粉丝数",
    "关注数",
    "微博数",
    "是否认证",
    "简介",
    "地区",
    "头像链接",
    "封面链接",
    "认证说明",
    "昨日发博数",
    "昨日阅读数",
    "昨日互动数",
    "视频累计播放量",
    "主页链接",
    "来源目标",
    "抓取时间",
]


def get_default_config() -> CrawlerConfig:
    """获取默认配置实例"""
    return CrawlerConfig()
