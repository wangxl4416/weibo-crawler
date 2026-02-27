# -*- coding: utf-8 -*-
"""
链接模式抓取器
=============
支持批量处理微博帖子链接，自动兼容多种链接格式并提取帖子 ID。
"""

import asyncio
from typing import List

from ..config import CrawlerConfig
from ..logger import get_logger
from ..utils import extract_post_id_from_url
from .post_scraper import PostScraper

logger = get_logger("scrapers.link")


class LinkScraper:
    """按链接列表抓取微博帖子数据。"""

    def __init__(self, post_scraper: PostScraper, config: CrawlerConfig) -> None:
        self._post_scraper = post_scraper
        self._config = config
        self._semaphore = asyncio.Semaphore(config.concurrency.post_detail_concurrency)

    async def process_links(self, links: List[str], source_mode: str = "post_url") -> int:
        if not links:
            return 0

        tasks = [self._process_one(link, source_mode) for link in links if link.strip()]
        if not tasks:
            return 0

        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_saved = 0
        for result in results:
            if isinstance(result, int):
                total_saved += result
            elif isinstance(result, Exception):
                logger.warning("链接模式任务异常: %s", result)
        logger.info("🔗 链接模式完成: 处理 %d 条链接，新增评论 %d 条", len(tasks), total_saved)
        return total_saved

    async def _process_one(self, link: str, source_mode: str) -> int:
        post_id = extract_post_id_from_url(link)
        if not post_id:
            logger.warning("⚠️ 无法从链接提取帖子 ID: %s", link)
            return 0

        async with self._semaphore:
            return await self._post_scraper.process_post_id(
                post_id=post_id,
                source_mode=source_mode,
                source_target=link,
            )
