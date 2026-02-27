# -*- coding: utf-8 -*-
"""
帖子处理器模块
=============
支持多来源帖子处理：
    - 关键词搜索得到的 mid
    - 外部微博链接解析出的 post_id
    - 用户时间线返回的状态对象
"""

import asyncio
from typing import Dict, Optional

from ..config import CrawlerConfig
from ..http_client import HttpClient
from ..logger import get_logger
from ..media_downloader import MediaDownloader
from ..models import MediaRecord, PostInfo
from ..storage import CsvStorage
from .comment_scraper import CommentScraper

logger = get_logger("scrapers.post")

POST_DETAIL_API = "https://weibo.com/ajax/statuses/show"


class PostScraper:
    """帖子处理协调器：详情、评论、媒体、存储。"""

    def __init__(
        self,
        client: HttpClient,
        comment_scraper: CommentScraper,
        storage: CsvStorage,
        config: CrawlerConfig,
    ) -> None:
        self._client = client
        self._comment_scraper = comment_scraper
        self._storage = storage
        self._config = config
        self._media_downloader = MediaDownloader(client, config)
        self._detail_semaphore = asyncio.Semaphore(
            config.concurrency.post_detail_concurrency
        )
        self._comment_semaphore = asyncio.Semaphore(
            config.concurrency.comment_concurrency
        )

    async def process_post_id(
        self,
        post_id: str,
        source_mode: str,
        source_target: str,
    ) -> int:
        """
        通过 post_id 拉取详情并处理。
        返回保存的评论数。
        """
        post_id = str(post_id or "").strip()
        if not post_id:
            return 0

        async with self._detail_semaphore:
            status = await self._fetch_status(post_id)
        if not status:
            logger.warning("⚠️ 无法获取帖子详情: %s", post_id)
            return 0

        return await self.process_status(status, source_mode, source_target)

    async def process_status(
        self,
        status_data: Dict,
        source_mode: str,
        source_target: str,
    ) -> int:
        """
        处理单条状态对象，返回保存评论数。
        """
        if not isinstance(status_data, dict):
            logger.warning("⚠️ 跳过异常帖子数据（非字典）: %s", type(status_data).__name__)
            return 0

        post_info = PostInfo.from_status(status_data, source_mode, source_target)
        if not post_info.post_id and not post_info.content:
            return 0

        is_new_post = await self._storage.save_post(post_info)
        if is_new_post:
            logger.info("▶ 新帖子: @%s | %s", post_info.user_name, post_info.title)

        if self._config.media_mode_enabled:
            media_records = MediaRecord.from_status_data(status_data, post_info)
            media_records = [
                record
                for record in media_records
                if self._config.should_capture_media_type(record.media_type)
            ]
            saved_media = 0

            async def _on_media_record_done(record: MediaRecord) -> None:
                nonlocal saved_media
                saved_media += await self._storage.save_media([record])

            if media_records:
                media_records = await self._media_downloader.download_all(
                    media_records,
                    on_record_done=_on_media_record_done,
                )
            if saved_media:
                success_count = sum(
                    1 for record in media_records if record.download_status == "success"
                )
                exists_count = sum(
                    1 for record in media_records if record.download_status == "exists"
                )
                logger.info(
                    "  ├─ 抽取媒体 %d 条（下载成功 %d，已存在 %d）@%s",
                    saved_media,
                    success_count,
                    exists_count,
                    post_info.user_name,
                )

        if not self._config.should_fetch_comments(source_mode):
            return 0

        async with self._comment_semaphore:
            saved = await self._comment_scraper.fetch_and_save(
                post_info=post_info,
                save_func=self._storage.save_comments,
            )

        if saved:
            logger.info(
                "  ├─ 评论新增 %d 条 @%s (累计评论: %d)",
                saved,
                post_info.user_name,
                self._storage.total_comments_saved,
            )
        else:
            logger.info("  ├─ [无评论] @%s", post_info.user_name)
        return saved

    async def process(
        self,
        mid: str,
        user_name: str,
        content: str,
        keyword: str,
    ) -> int:
        """
        向后兼容旧接口：忽略旧字段，直接按 mid 处理。
        """
        _ = (user_name, content)
        return await self.process_post_id(mid, source_mode="keyword", source_target=keyword)

    async def _fetch_status(self, post_id: str) -> Optional[Dict]:
        params = {"id": post_id}
        data = await self._client.get_json(POST_DETAIL_API, params=params)
        if not data:
            return None
        if "data" in data and isinstance(data["data"], dict):
            return data["data"]
        return data
