# -*- coding: utf-8 -*-
"""
数据模型模块
===========
定义爬虫中的核心结构：
    - 帖子信息
    - 评论记录
    - 媒体记录
    - 用户主页信息
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from .utils import (
    clean_html,
    normalize_media_url,
    parse_count,
    parse_relative_time,
    truncate_text,
)


@dataclass
class PostInfo:
    """微博帖子信息"""
    post_id: str
    mid: str
    uid: str
    user_name: str
    author_verified: bool
    content: str
    post_time: str
    source_mode: str
    source_target: str
    reposts_count: int = 0
    comments_count: int = 0
    attitudes_count: int = 0
    post_url: str = ""
    raw_status: Dict[str, Any] = field(default_factory=dict)

    @property
    def title(self) -> str:
        """帖子标题（正文前 40 字）"""
        return truncate_text(self.content, max_length=40)

    @property
    def dedup_key(self) -> tuple[str, str]:
        """帖子去重键"""
        if self.post_id:
            return ("post_id", self.post_id)
        return ("user_content", f"{self.uid}:{self.content[:80]}")

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典（用于 JSONL）"""
        data = asdict(self)
        data["title"] = self.title
        return data

    def to_csv_row(self) -> Dict[str, Any]:
        return {
            "来源模式": self.source_mode,
            "来源目标": self.source_target,
            "帖子ID": self.post_id,
            "帖子链接": self.post_url,
            "帖子发布者": self.user_name,
            "是否带V": "是" if self.author_verified else "否",
            "帖子发布时间": self.post_time,
            "转发量": self.reposts_count,
            "评论量": self.comments_count,
            "点赞量": self.attitudes_count,
            "帖子标题": self.title,
            "帖子内容": self.content,
        }

    @staticmethod
    def build_post_url(uid: str, post_id: str) -> str:
        if uid and post_id:
            return f"https://weibo.com/{uid}/{post_id}"
        if post_id:
            return f"https://weibo.com/detail/{post_id}"
        return ""

    @classmethod
    def from_status(
        cls,
        status_data: Dict[str, Any],
        source_mode: str,
        source_target: str,
    ) -> "PostInfo":
        """从微博状态 JSON 构建 PostInfo"""
        if not isinstance(status_data, dict):
            status_data = {}

        user_raw = status_data.get("user") or {}
        user = user_raw if isinstance(user_raw, dict) else {}
        post_id = str(status_data.get("idstr") or status_data.get("id") or "")
        mid = str(status_data.get("mid") or post_id)
        uid = str(user.get("idstr") or user.get("id") or "")
        user_name = (
            user.get("screen_name")
            or user.get("name")
            or status_data.get("user_name")
            or "未知用户"
        )

        raw_text = status_data.get("text_raw") or status_data.get("text") or ""
        content = clean_html(raw_text).strip()
        post_time = parse_relative_time(str(status_data.get("created_at", "")))
        post_url = cls.build_post_url(uid, post_id)
        reposts_count = parse_count(status_data.get("reposts_count"))
        comments_count = parse_count(status_data.get("comments_count"))
        attitudes_count = parse_count(status_data.get("attitudes_count"))

        author_verified = bool(user.get("verified"))
        if not author_verified:
            verified_type = user.get("verified_type")
            try:
                if verified_type is not None and int(verified_type) >= 0:
                    author_verified = True
            except (TypeError, ValueError):
                pass
        if not author_verified and str(user.get("verified_reason") or "").strip():
            author_verified = True

        return cls(
            post_id=post_id,
            mid=mid,
            uid=uid,
            user_name=user_name,
            author_verified=author_verified,
            content=content,
            post_time=post_time,
            source_mode=source_mode,
            source_target=source_target,
            reposts_count=reposts_count,
            comments_count=comments_count,
            attitudes_count=attitudes_count,
            post_url=post_url,
            raw_status=status_data,
        )


@dataclass
class CommentRecord:
    """评论记录（CSV 行）"""
    source_mode: str
    source_target: str
    post_id: str
    post_url: str
    post_title: str
    post_author: str
    post_time: str
    commenter_name: str
    comment_ip_location: str
    comment_content: str
    comment_time: str
    comment_level: str

    def to_csv_row(self) -> Dict[str, Any]:
        return {
            "来源模式": self.source_mode,
            "来源目标": self.source_target,
            "帖子ID": self.post_id,
            "帖子链接": self.post_url,
            "帖子标题": self.post_title,
            "帖子发布者": self.post_author,
            "帖子发布时间": self.post_time,
            "评论者": self.commenter_name,
            "评论IP属地": self.comment_ip_location,
            "评论内容": self.comment_content,
            "评论时间": self.comment_time,
            "评论层级": self.comment_level,
        }

    @property
    def dedup_key(self) -> tuple[str, str, str, str]:
        return (
            self.post_id,
            self.commenter_name,
            self.comment_content[:120],
            self.comment_time,
        )

    @classmethod
    def from_api_data(
        cls,
        api_data: Dict[str, Any],
        post_info: PostInfo,
        is_reply: bool = False,
    ) -> "CommentRecord":
        """从评论 API 数据构建 CommentRecord"""
        if not isinstance(api_data, dict):
            api_data = {}

        user_info = api_data.get("user") or {}
        commenter_name = user_info.get("screen_name") or "未知用户"
        raw_text = api_data.get("text_raw") or api_data.get("text") or ""
        content = clean_html(raw_text).strip()
        created_at = str(api_data.get("created_at", ""))
        comment_time = parse_relative_time(created_at)
        ip_location = cls._extract_ip_location(api_data)

        if is_reply and content:
            content = f"[回复] {content}"

        return cls(
            source_mode=post_info.source_mode,
            source_target=post_info.source_target,
            post_id=post_info.post_id,
            post_url=post_info.post_url,
            post_title=post_info.title,
            post_author=post_info.user_name,
            post_time=post_info.post_time,
            commenter_name=commenter_name,
            comment_ip_location=ip_location,
            comment_content=content,
            comment_time=comment_time,
            comment_level="楼中楼" if is_reply else "主评论",
        )

    @staticmethod
    def _normalize_ip_location(value: Any) -> str:
        text = clean_html(str(value or "")).strip()
        if not text:
            return ""

        compact = " ".join(text.replace("\u00a0", " ").split())
        compact = compact.replace("：", ":")

        if "IP属地" in compact:
            idx = compact.find("IP属地")
            normalized = compact[idx:]
            normalized = normalized.replace("IP属地:", "IP属地 ").replace("IP属地：", "IP属地 ")
            return " ".join(normalized.split())

        lower = compact.lower()
        if lower.startswith("ip location"):
            tail = compact.split(":", 1)[1].strip() if ":" in compact else compact[len("ip location"):].strip()
            if tail:
                return f"IP属地 {tail}"

        if compact.startswith("发布于"):
            tail = compact.replace("发布于", "", 1).strip()
            if tail:
                return f"IP属地 {tail}"

        if compact.startswith("来自"):
            tail = compact.replace("来自", "", 1).strip()
            if tail:
                return f"IP属地 {tail}"

        return ""

    @classmethod
    def _extract_ip_location(cls, api_data: Dict[str, Any]) -> str:
        candidates: List[Any] = []

        direct_keys = [
            "ip_location",
            "ipLocation",
            "region_name",
            "regionName",
            "location",
            "source_location",
            "sourceLocation",
            "source",
        ]
        for key in direct_keys:
            if key in api_data:
                candidates.append(api_data.get(key))

        user_raw = api_data.get("user")
        user = user_raw if isinstance(user_raw, dict) else {}
        for key in ("ip_location", "ipLocation", "region_name", "regionName", "location"):
            if key in user:
                candidates.append(user.get(key))

        # 部分接口可能把 IP 字段塞在 extensions / ext 等容器
        for container_key in ("extensions", "ext", "extra"):
            container = api_data.get(container_key)
            if isinstance(container, dict):
                for key in direct_keys:
                    if key in container:
                        candidates.append(container.get(key))

        for value in candidates:
            normalized = cls._normalize_ip_location(value)
            if normalized:
                return normalized

        return ""


@dataclass
class MediaRecord:
    """媒体记录（图片/视频）"""
    source_mode: str
    source_target: str
    post_id: str
    post_url: str
    post_author: str
    media_type: str
    media_url: str
    preview_url: str
    post_time: str
    local_file_path: str = ""
    download_status: str = "pending"

    def to_csv_row(self) -> Dict[str, Any]:
        return {
            "来源模式": self.source_mode,
            "来源目标": self.source_target,
            "帖子ID": self.post_id,
            "帖子链接": self.post_url,
            "帖子发布者": self.post_author,
            "媒体类型": self.media_type,
            "媒体链接": self.media_url,
            "预览链接": self.preview_url,
            "本地文件路径": self.local_file_path,
            "下载状态": self.download_status,
            "帖子发布时间": self.post_time,
        }

    @property
    def dedup_key(self) -> str:
        normalized_url = normalize_media_url(self.media_url)
        return f"{self.post_id}|{self.media_type}|{normalized_url}"

    @classmethod
    def from_status_data(
        cls,
        status_data: Dict[str, Any],
        post_info: PostInfo,
    ) -> List["MediaRecord"]:
        """从微博状态中提取媒体数据"""
        if not isinstance(status_data, dict):
            return []

        records: List[MediaRecord] = []
        seen_local_keys: Set[Tuple[str, str]] = set()
        nodes: List[Dict[str, Any]] = [status_data]
        retweeted_raw = status_data.get("retweeted_status")
        if isinstance(retweeted_raw, dict):
            nodes.append(retweeted_raw)

        for node in nodes:
            for image_url in cls._collect_image_urls(node):
                normalized = normalize_media_url(image_url)
                dedup_key = ("image", normalized)
                if dedup_key in seen_local_keys:
                    continue
                seen_local_keys.add(dedup_key)
                records.append(
                    cls(
                        source_mode=post_info.source_mode,
                        source_target=post_info.source_target,
                        post_id=post_info.post_id,
                        post_url=post_info.post_url,
                        post_author=post_info.user_name,
                        media_type="image",
                        media_url=image_url,
                        preview_url=image_url,
                        local_file_path="",
                        download_status="pending",
                        post_time=post_info.post_time,
                    )
                )

            for video_url, preview_url in cls._collect_video_urls(node):
                normalized = normalize_media_url(video_url)
                dedup_key = ("video", normalized)
                if dedup_key in seen_local_keys:
                    continue
                seen_local_keys.add(dedup_key)
                records.append(
                    cls(
                        source_mode=post_info.source_mode,
                        source_target=post_info.source_target,
                        post_id=post_info.post_id,
                        post_url=post_info.post_url,
                        post_author=post_info.user_name,
                        media_type="video",
                        media_url=video_url,
                        preview_url=preview_url,
                        local_file_path="",
                        download_status="pending",
                        post_time=post_info.post_time,
                    )
                )

        return records

    @staticmethod
    def _split_url_candidates(value: Any) -> List[str]:
        text = str(value or "").strip()
        if not text:
            return []

        parts = [text]
        if ";http" in text or text.count("http") > 1:
            parts = [item.strip() for item in text.split(";") if item.strip()]
        elif "|http" in text or ("|" in text and text.count("http") > 1):
            parts = [item.strip() for item in text.split("|") if item.strip()]

        urls: List[str] = []
        seen: Set[str] = set()
        for item in parts:
            candidate = item.strip().strip(",")
            if candidate.startswith("//"):
                candidate = f"https:{candidate}"
            if not candidate.startswith(("http://", "https://")):
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            urls.append(candidate)
        return urls

    @staticmethod
    def _pick_nested_url(obj: Dict[str, Any], *path: str) -> str:
        current: Any = obj
        for key in path:
            if not isinstance(current, dict):
                return ""
            current = current.get(key)
        return str(current or "").strip()

    @classmethod
    def _pick_first_url(cls, obj: Dict[str, Any], paths: List[Tuple[str, ...]]) -> str:
        for path in paths:
            candidate = cls._pick_nested_url(obj, *path)
            urls = cls._split_url_candidates(candidate)
            if urls:
                return urls[0]
        return ""

    @classmethod
    def _collect_image_urls(cls, node: Dict[str, Any]) -> List[str]:
        image_urls: List[str] = []
        seen: Set[str] = set()

        def _append_urls(urls: List[str]) -> None:
            for url in urls:
                normalized = normalize_media_url(url)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                image_urls.append(url)

        # 1) pics 列表
        pics_raw = node.get("pics") or []
        pics = pics_raw if isinstance(pics_raw, list) else []
        for pic in pics:
            if not isinstance(pic, dict):
                continue
            media_url = cls._pick_first_url(
                pic,
                [
                    ("largest", "url"),
                    ("large", "url"),
                    ("mw2000", "url"),
                    ("url",),
                ],
            )
            _append_urls(cls._split_url_candidates(media_url))

        # 2) pic_infos 字典
        pic_infos_raw = node.get("pic_infos")
        pic_infos = pic_infos_raw if isinstance(pic_infos_raw, dict) else {}
        for info in pic_infos.values():
            if not isinstance(info, dict):
                continue
            media_url = cls._pick_first_url(
                info,
                [
                    ("largest", "url"),
                    ("original", "url"),
                    ("mw2000", "url"),
                    ("large", "url"),
                    ("bmiddle", "url"),
                    ("thumbnail", "url"),
                    ("url",),
                ],
            )
            _append_urls(cls._split_url_candidates(media_url))

        # 3) mix_media_info 图片项
        mix_media_raw = node.get("mix_media_info")
        mix_media = mix_media_raw if isinstance(mix_media_raw, dict) else {}
        items_raw = mix_media.get("items")
        items = items_raw if isinstance(items_raw, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "").lower()
            if item_type not in {"pic", "image"}:
                continue
            data_raw = item.get("data")
            data = data_raw if isinstance(data_raw, dict) else {}
            media_url = cls._pick_first_url(
                data,
                [
                    ("largest", "url"),
                    ("original", "url"),
                    ("mw2000", "url"),
                    ("large", "url"),
                    ("bmiddle", "url"),
                    ("thumbnail", "url"),
                    ("url",),
                ],
            )
            _append_urls(cls._split_url_candidates(media_url))

        # 4) 页面卡片图（非视频卡片时）
        page_info_raw = node.get("page_info")
        page_info = page_info_raw if isinstance(page_info_raw, dict) else {}
        media_info_raw = page_info.get("media_info")
        media_info = media_info_raw if isinstance(media_info_raw, dict) else {}
        has_video = bool(
            cls._pick_first_url(
                media_info,
                [
                    ("stream_url_hd",),
                    ("stream_url",),
                    ("mp4_hd_url",),
                    ("mp4_sd_url",),
                ],
            )
        )
        if not has_video:
            page_pic_raw = page_info.get("page_pic")
            page_pic = page_pic_raw if isinstance(page_pic_raw, dict) else {}
            _append_urls(cls._split_url_candidates(page_pic.get("url")))

        return image_urls

    @classmethod
    def _collect_video_urls(cls, node: Dict[str, Any]) -> List[Tuple[str, str]]:
        videos: List[Tuple[str, str]] = []
        seen: Set[str] = set()

        def _append_video(video_url: str, preview_url: str) -> None:
            if not video_url:
                return
            normalized = normalize_media_url(video_url)
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            videos.append((video_url, preview_url))

        # 1) page_info.media_info
        page_info_raw = node.get("page_info")
        page_info = page_info_raw if isinstance(page_info_raw, dict) else {}
        media_info_raw = page_info.get("media_info")
        media_info = media_info_raw if isinstance(media_info_raw, dict) else {}
        video_url = cls._pick_first_url(
            media_info,
            [
                ("stream_url_hd",),
                ("stream_url",),
                ("mp4_hd_url",),
                ("mp4_sd_url",),
            ],
        )
        page_pic_raw = page_info.get("page_pic")
        page_pic = page_pic_raw if isinstance(page_pic_raw, dict) else {}
        preview_url = ""
        preview_urls = cls._split_url_candidates(page_pic.get("url"))
        if preview_urls:
            preview_url = preview_urls[0]
        _append_video(video_url, preview_url)

        # 2) mix_media_info 视频项
        mix_media_raw = node.get("mix_media_info")
        mix_media = mix_media_raw if isinstance(mix_media_raw, dict) else {}
        items_raw = mix_media.get("items")
        items = items_raw if isinstance(items_raw, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "").lower()
            if item_type not in {"video", "story"}:
                continue
            data_raw = item.get("data")
            data = data_raw if isinstance(data_raw, dict) else {}
            item_video_url = cls._pick_first_url(
                data,
                [
                    ("stream_url_hd",),
                    ("stream_url",),
                    ("mp4_hd_url",),
                    ("mp4_sd_url",),
                ],
            )
            item_preview = cls._pick_first_url(
                data,
                [
                    ("poster",),
                    ("cover",),
                    ("cover_url",),
                    ("page_pic", "url"),
                ],
            )
            _append_video(item_video_url, item_preview)

        return videos


@dataclass
class UserProfile:
    """用户主页资料"""
    uid: str
    screen_name: str
    gender: str
    followers_count: int
    follow_count: int
    statuses_count: int
    verified: bool
    description: str
    location: str
    avatar_url: str
    cover_image_url: str
    verified_reason: str
    yesterday_posts_count: int
    yesterday_read_count: int
    yesterday_interaction_count: int
    video_play_count: int
    profile_url: str
    source_target: str
    crawled_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_csv_row(self) -> Dict[str, Any]:
        return {
            "用户ID": self.uid,
            "昵称": self.screen_name,
            "性别": self.gender,
            "粉丝数": self.followers_count,
            "关注数": self.follow_count,
            "微博数": self.statuses_count,
            "是否认证": self.verified,
            "简介": self.description,
            "地区": self.location,
            "头像链接": self.avatar_url,
            "封面链接": self.cover_image_url,
            "认证说明": self.verified_reason,
            "昨日发博数": self.yesterday_posts_count,
            "昨日阅读数": self.yesterday_read_count,
            "昨日互动数": self.yesterday_interaction_count,
            "视频累计播放量": self.video_play_count,
            "主页链接": self.profile_url,
            "来源目标": self.source_target,
            "抓取时间": self.crawled_at,
        }

    @staticmethod
    def _walk_for_key(node: Any, target_key: str, results: List[Any]) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == target_key:
                    results.append(value)
                UserProfile._walk_for_key(value, target_key, results)
        elif isinstance(node, list):
            for item in node:
                UserProfile._walk_for_key(item, target_key, results)

    @classmethod
    def _pick_str(cls, scopes: List[Dict[str, Any]], keys: List[str]) -> str:
        for scope in scopes:
            if not isinstance(scope, dict):
                continue
            for key in keys:
                value = scope.get(key)
                if value not in (None, ""):
                    return str(value).strip()

        for scope in scopes:
            if not isinstance(scope, dict):
                continue
            for key in keys:
                results: List[Any] = []
                cls._walk_for_key(scope, key, results)
                for value in results:
                    if value not in (None, ""):
                        return str(value).strip()
        return ""

    @classmethod
    def _pick_int(cls, scopes: List[Dict[str, Any]], keys: List[str]) -> int:
        for scope in scopes:
            if not isinstance(scope, dict):
                continue
            for key in keys:
                value = scope.get(key)
                if value is None:
                    continue
                parsed = parse_count(value)
                if parsed > 0:
                    return parsed

        for scope in scopes:
            if not isinstance(scope, dict):
                continue
            for key in keys:
                results: List[Any] = []
                cls._walk_for_key(scope, key, results)
                for value in results:
                    parsed = parse_count(value)
                    if parsed > 0:
                        return parsed
        return 0

    @classmethod
    def from_api_data(
        cls,
        profile_data: Dict[str, Any],
        source_target: str,
    ) -> "UserProfile":
        if not isinstance(profile_data, dict):
            profile_data = {}

        data_payload = profile_data.get("data")
        data_payload = data_payload if isinstance(data_payload, dict) else {}

        user = profile_data.get("user")
        if not isinstance(user, dict):
            user = data_payload.get("user")
        if not isinstance(user, dict):
            user = data_payload.get("userInfo")
        if not isinstance(user, dict):
            user = profile_data

        scopes = [user, data_payload, profile_data]
        uid = cls._pick_str(scopes, ["idstr", "id"])
        profile_url = f"https://weibo.com/u/{uid}" if uid else ""

        yesterday_posts_count = cls._pick_int(
            scopes,
            [
                "yesterday_statuses_count",
                "yesterday_mblog_count",
                "yesterday_post_count",
                "statuses_yesterday_count",
            ],
        )
        yesterday_read_count = cls._pick_int(
            scopes,
            [
                "yesterday_read_count",
                "yesterday_read_num",
                "read_count_yesterday",
            ],
        )
        yesterday_interaction_count = cls._pick_int(
            scopes,
            [
                "yesterday_interaction_count",
                "yesterday_interact_count",
                "interaction_count",
                "interact_count",
            ],
        )
        video_play_count = cls._pick_int(
            scopes,
            [
                "video_play_count_total",
                "video_total_play_count",
                "video_play_count",
                "video_play_total",
                "video_play_num",
            ],
        )

        return cls(
            uid=uid,
            screen_name=cls._pick_str(scopes, ["screen_name", "name"]),
            gender=cls._pick_str(scopes, ["gender"]),
            followers_count=cls._pick_int(scopes, ["followers_count", "fans_count", "followers"]),
            follow_count=cls._pick_int(scopes, ["friends_count", "follow_count", "following_count"]),
            statuses_count=cls._pick_int(scopes, ["statuses_count", "mblog_num", "weibo_count"]),
            verified=bool(user.get("verified") or False),
            description=cls._pick_str(scopes, ["description", "desc"]),
            location=cls._pick_str(scopes, ["location"]),
            avatar_url=cls._pick_str(scopes, ["avatar_hd", "avatar_large", "avatar_url"]),
            cover_image_url=cls._pick_str(scopes, ["cover_image_phone", "cover_image_hd", "cover_image"]),
            verified_reason=cls._pick_str(scopes, ["verified_reason"]),
            yesterday_posts_count=yesterday_posts_count,
            yesterday_read_count=yesterday_read_count,
            yesterday_interaction_count=yesterday_interaction_count,
            video_play_count=video_play_count,
            profile_url=profile_url,
            source_target=source_target,
            crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
