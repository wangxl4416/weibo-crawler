# -*- coding: utf-8 -*-
"""
用户模式抓取器
=============
支持：
    1. 抓取用户主页资料
    2. 抓取用户时间线帖子并联动评论/媒体采集
"""

import asyncio
import random
from typing import Dict, List, Set

from ..config import CrawlerConfig
from ..http_client import HttpClient
from ..logger import get_logger
from ..media_downloader import MediaDownloader
from ..models import MediaRecord, UserProfile
from ..storage import CsvStorage
from ..utils import extract_custom_from_target, extract_uid_from_target
from .post_scraper import PostScraper

logger = get_logger("scrapers.user")

USER_PROFILE_API = "https://weibo.com/ajax/profile/info"
USER_TIMELINE_APIS = [
    "https://weibo.com/ajax/statuses/mymblog",
    "https://weibo.com/ajax/profile/getWaterFallContent",
    "https://weibo.com/ajax/profile/getProfileFeed",
]


class UserScraper:
    """用户维度抓取调度器。"""

    def __init__(
        self,
        client: HttpClient,
        post_scraper: PostScraper,
        storage: CsvStorage,
        config: CrawlerConfig,
    ) -> None:
        self._client = client
        self._post_scraper = post_scraper
        self._storage = storage
        self._config = config
        self._media_downloader = MediaDownloader(client, config)
        self._user_semaphore = asyncio.Semaphore(config.concurrency.user_concurrency)

    async def process_users(
        self,
        targets: List[str],
        *,
        force_fetch_timeline: bool = False,
        force_fetch_profile: bool = False,
    ) -> int:
        if not targets:
            return 0
        tasks = [
            self._process_single_target(
                target,
                force_fetch_timeline=force_fetch_timeline,
                force_fetch_profile=force_fetch_profile,
            )
            for target in targets
            if target.strip()
        ]
        if not tasks:
            return 0
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_saved_comments = 0
        for result in results:
            if isinstance(result, int):
                total_saved_comments += result
            elif isinstance(result, Exception):
                logger.warning("用户模式任务异常: %s", result)
        logger.info(
            "👤 用户模式完成: 处理 %d 个目标，新增评论 %d 条",
            len(tasks),
            total_saved_comments,
        )
        return total_saved_comments

    async def _process_single_target(
        self,
        target: str,
        *,
        force_fetch_timeline: bool = False,
        force_fetch_profile: bool = False,
    ) -> int:
        async with self._user_semaphore:
            uid = extract_uid_from_target(target)
            custom = extract_custom_from_target(target)
            if not uid and not custom:
                logger.warning("⚠️ 无法解析用户目标: %s", target)
                return 0

            logger.info("👤 开始抓取用户目标: %s", target)
            total_saved_comments = 0
            should_fetch_profile = force_fetch_profile or self._config.profile_info_enabled
            should_fetch_timeline = force_fetch_timeline or self._config.is_mode_enabled("user")

            # 1) 抓取主页资料
            if should_fetch_profile:
                profile = await self._fetch_profile(uid=uid, custom=custom, source_target=target)
                if profile:
                    saved = await self._storage.save_profile(profile)
                    if saved:
                        logger.info("  ├─ 已保存主页资料: @%s (%s)", profile.screen_name, profile.uid)
                    profile_media_saved = await self._save_profile_media(profile)
                    if profile_media_saved:
                        logger.info(
                            "  ├─ 已保存主页媒体 %d 条: @%s",
                            profile_media_saved,
                            profile.screen_name,
                        )
                    if not uid and profile.uid:
                        uid = profile.uid

            # 2) 抓取用户时间线
            if should_fetch_timeline and uid:
                total_saved_comments += await self._crawl_user_timeline(uid, target)
            elif should_fetch_timeline and not uid:
                logger.warning("  ├─ 无 uid，跳过用户时间线抓取: %s", target)

            return total_saved_comments

    async def _fetch_profile(
        self,
        uid: str,
        custom: str,
        source_target: str,
    ) -> UserProfile | None:
        params = {}
        if uid:
            params["uid"] = uid
        elif custom:
            params["custom"] = custom
        else:
            return None

        data = await self._client.get_json(USER_PROFILE_API, params=params)
        if not data:
            logger.warning("  ├─ 用户资料请求失败: %s", source_target)
            return None
        try:
            profile = UserProfile.from_api_data(data, source_target)
            if uid and not profile.uid:
                profile.uid = uid
            return profile
        except Exception as exc:
            logger.warning("  ├─ 解析用户资料失败: %s", exc)
            return None

    async def _crawl_user_timeline(self, uid: str, source_target: str) -> int:
        configured_max_pages = self._config.max_user_pages_enabled
        # 0=不限时仍加安全上限，避免接口异常导致无限翻页
        safety_cap_when_unlimited = 500
        max_posts = self._config.request.max_posts_per_user
        delay_range = self._config.delay.user_page_delay

        total_saved = 0
        processed_posts = 0
        page = 1

        while True:
            if configured_max_pages > 0 and page > configured_max_pages:
                break
            if configured_max_pages == 0 and page > safety_cap_when_unlimited:
                logger.warning(
                    "  ├─ MAX_USER_PAGES=0（不限）触发安全上限 %d，停止 uid=%s",
                    safety_cap_when_unlimited,
                    uid,
                )
                break
            if max_posts > 0 and processed_posts >= max_posts:
                break

            statuses = await self._fetch_user_statuses(uid=uid, page=page)
            if not statuses:
                if page == 1:
                    logger.info("  ├─ 用户时间线暂无公开内容: uid=%s", uid)
                break

            for status in statuses:
                if max_posts > 0 and processed_posts >= max_posts:
                    break
                normalized = self._normalize_status(status)
                if not normalized:
                    continue
                processed_posts += 1
                total_saved += await self._post_scraper.process_status(
                    status_data=normalized,
                    source_mode="user",
                    source_target=source_target,
                )

            await asyncio.sleep(random.uniform(*delay_range))
            page += 1

        logger.info(
            "  ├─ 用户时间线完成: uid=%s, 处理帖子 %d, 新增评论 %d",
            uid,
            processed_posts,
            total_saved,
        )
        return total_saved

    async def _save_profile_media(self, profile: UserProfile) -> int:
        """
        将用户头像/背景图作为媒体记录处理（可下载到本地）。
        """
        if not self._config.profile_media_enabled:
            return 0
        if not self._config.should_capture_media_type("image"):
            return 0

        records: List[MediaRecord] = []
        seen_urls: Set[str] = set()
        author = profile.screen_name or profile.uid or "unknown_user"
        profile_post_id = f"profile_{profile.uid}" if profile.uid else "profile_unknown"

        for avatar_url in self._split_media_urls(profile.avatar_url):
            if avatar_url in seen_urls:
                continue
            seen_urls.add(avatar_url)
            records.append(
                MediaRecord(
                    source_mode="profile",
                    source_target=profile.source_target,
                    post_id=profile_post_id,
                    post_url=profile.profile_url,
                    post_author=author,
                    media_type="image",
                    media_url=avatar_url,
                    preview_url=avatar_url,
                    post_time=profile.crawled_at,
                )
            )

        for cover_url in self._split_media_urls(profile.cover_image_url):
            if cover_url in seen_urls:
                continue
            seen_urls.add(cover_url)
            records.append(
                MediaRecord(
                    source_mode="profile",
                    source_target=profile.source_target,
                    post_id=profile_post_id,
                    post_url=profile.profile_url,
                    post_author=author,
                    media_type="image",
                    media_url=cover_url,
                    preview_url=cover_url,
                    post_time=profile.crawled_at,
                )
            )

        if not records:
            return 0

        saved_media = 0

        async def _on_record_done(record: MediaRecord) -> None:
            nonlocal saved_media
            saved_media += await self._storage.save_media([record])

        await self._media_downloader.download_all(records, on_record_done=_on_record_done)
        return saved_media

    @staticmethod
    def _split_media_urls(raw_value: str) -> List[str]:
        text = str(raw_value or "").strip()
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

    async def _fetch_user_statuses(self, uid: str, page: int) -> List[Dict]:
        for api in USER_TIMELINE_APIS:
            params = {"uid": uid, "page": str(page)}
            data = await self._client.get_json(api, params=params)
            if not data:
                continue
            statuses = self._extract_status_list(data)
            if statuses:
                return statuses
        return []

    @staticmethod
    def _extract_status_list(data: Dict) -> List[Dict]:
        """
        尝试兼容多个接口返回结构：
            - {"data": {"list": [...]}}
            - {"data": {"statuses": [...]}}
            - {"statuses": [...]}
            - {"data": [...]}
        """
        if not isinstance(data, dict):
            return []

        if isinstance(data.get("statuses"), list):
            return data["statuses"]

        payload = data.get("data")
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if isinstance(payload.get("list"), list):
                return payload["list"]
            if isinstance(payload.get("statuses"), list):
                return payload["statuses"]

        if isinstance(data.get("list"), list):
            return data["list"]
        return []

    @staticmethod
    def _normalize_status(status: Dict) -> Dict:
        if not isinstance(status, dict):
            return {}
        if isinstance(status.get("mblog"), dict):
            return status["mblog"]
        return status
