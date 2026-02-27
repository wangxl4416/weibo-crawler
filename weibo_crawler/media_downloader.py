# -*- coding: utf-8 -*-
"""
媒体下载器
==========
将微博图片/视频下载到本地目录，并把本地路径回填到 MediaRecord。
"""

import asyncio
import hashlib
import os
import re
from typing import Awaitable, Callable, Optional
from urllib.parse import urlparse

from .config import CrawlerConfig
from .http_client import HttpClient
from .logger import get_logger
from .models import MediaRecord
from .utils import normalize_media_url

logger = get_logger("media_downloader")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".avif"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".flv", ".wmv", ".webm", ".m4v"}


class MediaDownloader:
    """负责媒体文件下载与路径回填。"""

    def __init__(self, client: HttpClient, config: CrawlerConfig) -> None:
        self._client = client
        self._config = config
        self._semaphore = asyncio.Semaphore(config.concurrency.media_download_concurrency)

    async def download_all(
        self,
        records: list[MediaRecord],
        on_record_done: Optional[Callable[[MediaRecord], Awaitable[None]]] = None,
    ) -> list[MediaRecord]:
        if not records:
            return records

        if not self._config.download.enable_media_download:
            for record in records:
                record.download_status = "skipped"
                if on_record_done is not None:
                    await on_record_done(record)
            return records

        tasks = [
            self._download_and_callback(record, index, on_record_done)
            for index, record in enumerate(records, start=1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_records: list[MediaRecord] = []
        for record, result in zip(records, results):
            if isinstance(result, Exception):
                logger.warning("媒体下载异常: %s", result)
                record.download_status = "failed"
                if on_record_done is not None:
                    await on_record_done(record)
            final_records.append(record)
        return final_records

    async def _download_and_callback(
        self,
        record: MediaRecord,
        index: int,
        on_record_done: Optional[Callable[[MediaRecord], Awaitable[None]]],
    ) -> None:
        await self._download_one(record, index)
        if on_record_done is not None:
            await on_record_done(record)

    async def _download_one(self, record: MediaRecord, index: int) -> None:
        if not record.media_url:
            record.download_status = "empty_url"
            return

        async with self._semaphore:
            target_path = self._build_target_path(record, index)
            if (
                os.path.exists(target_path)
                and not self._config.download.overwrite_existing
            ):
                record.local_file_path = os.path.abspath(target_path)
                record.download_status = "exists"
                return

            referer = record.post_url or "https://weibo.com/"
            success = await self._client.download_file(
                url=record.media_url,
                file_path=target_path,
                extra_headers={"referer": referer},
            )
            if success:
                record.local_file_path = os.path.abspath(target_path)
                record.download_status = "success"
                return

            record.download_status = "failed"

    def _build_target_path(self, record: MediaRecord, index: int) -> str:
        root_dir = os.path.join(
            self._config.output.output_dir,
            self._config.output.media_output_dir,
        )
        mode_dir = self._normalize_mode_for_path(record.source_mode)
        safe_author = self._sanitize(record.post_author or "unknown_author")
        safe_post = self._sanitize(record.post_id or "unknown_post")
        folder = os.path.join(root_dir, mode_dir, safe_author, safe_post)
        os.makedirs(folder, exist_ok=True)

        stable_url = normalize_media_url(record.media_url)
        digest = hashlib.sha1(stable_url.encode("utf-8")).hexdigest()[:12]
        extension = self._guess_extension(stable_url or record.media_url, record.media_type)
        file_name = f"{record.media_type}_{index:02d}_{digest}{extension}"
        return os.path.join(folder, file_name)

    @staticmethod
    def _normalize_mode_for_path(source_mode: str) -> str:
        value = str(source_mode or "").strip().lower()
        aliases = {
            "keyword": "keyword",
            "post_url": "post_url",
            "url": "post_url",
            "link": "post_url",
            "comment": "post_url",
            "user": "user",
            "personal": "user",
            "profile": "user",
        }
        return aliases.get(value, "other")

    @staticmethod
    def _sanitize(value: str) -> str:
        sanitized = re.sub(r"[^\w\-.]+", "_", value.strip())
        return sanitized[:80] or "unknown"

    @staticmethod
    def _guess_extension(url: str, media_type: str) -> str:
        path = urlparse(url).path
        ext = os.path.splitext(path)[1].lower()

        if media_type == "image":
            if ext in IMAGE_EXTENSIONS:
                return ext
            return ".jpg"

        if media_type == "video":
            if ext in VIDEO_EXTENSIONS:
                return ext
            return ".mp4"

        return ext or ".bin"
