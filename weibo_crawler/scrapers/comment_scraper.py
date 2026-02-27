# -*- coding: utf-8 -*-
"""
评论抓取器模块
=============
负责抓取微博帖子的主评论和楼中楼子评论。
支持分页遍历和子评论并发抓取。
"""

import asyncio
import random
from typing import Awaitable, Callable, List

from ..config import CrawlerConfig
from ..http_client import HttpClient
from ..logger import get_logger
from ..models import CommentRecord, PostInfo

logger = get_logger("scrapers.comment")

# 微博评论 API 地址
COMMENTS_API = "https://weibo.com/ajax/statuses/buildComments"


class CommentScraper:
    """
    评论抓取器。

    负责:
        - 遍历帖子的所有主评论（分页）
        - 抓取每条主评论的楼中楼子评论
        - 并发处理同一页内的所有子评论任务
    """

    def __init__(self, client: HttpClient, config: CrawlerConfig) -> None:
        self._client = client
        self._config = config

    async def fetch_and_save(
        self,
        post_info: PostInfo,
        save_func: Callable[[List[CommentRecord]], Awaitable[int]],
    ) -> int:
        """
        分页抓取并实时落盘，避免一次性缓存在内存中导致中断丢失。
        返回本次成功保存的评论数。
        """
        if not post_info.post_id:
            return 0

        fetch_top_level = self._config.fetch_top_level_comments_enabled
        fetch_sub_level = self._config.fetch_sub_comments_enabled
        if not fetch_top_level and not fetch_sub_level:
            return 0

        max_id = None
        page_count = 0
        max_pages = self._config.request.max_comment_pages
        delay_range = self._config.delay.comment_page_delay
        max_comments_per_post = self._config.max_comments_per_post_enabled
        accepted_for_post = 0
        total_saved = 0

        while page_count < max_pages:
            if max_comments_per_post > 0 and accepted_for_post >= max_comments_per_post:
                break

            params = {
                "flow": "0",
                "is_reload": "1",
                "id": str(post_info.post_id),
                "is_show_bulletin": "2",
                "is_mix": "0",
                "count": "20",
                "uid": str(post_info.uid),
                "fetch_level": "0",
                "locale": "zh-CN",
            }
            if max_id:
                params["max_id"] = max_id

            data = await self._client.get_json(COMMENTS_API, params=params)
            if not data:
                break
            if not isinstance(data, dict):
                logger.warning("评论接口返回异常类型: %s", type(data).__name__)
                break

            comment_list_raw = data.get("data", [])
            comment_list = comment_list_raw if isinstance(comment_list_raw, list) else []
            if not comment_list:
                break

            page_records: List[CommentRecord] = []
            sub_tasks = []
            for comment_data in comment_list:
                if not isinstance(comment_data, dict):
                    continue
                if fetch_top_level:
                    record = CommentRecord.from_api_data(comment_data, post_info)
                    if record.comment_content:
                        page_records.append(record)

                comment_id = comment_data.get("id")
                total_sub = comment_data.get("total_number", 0)
                if fetch_sub_level and total_sub > 0 and comment_id:
                    sub_tasks.append(
                        self._fetch_sub_comments(comment_id, post_info)
                    )

            if sub_tasks:
                sub_results = await asyncio.gather(*sub_tasks, return_exceptions=True)
                for result in sub_results:
                    if isinstance(result, list):
                        page_records.extend(result)
                    elif isinstance(result, Exception):
                        logger.warning("子评论抓取异常: %s", result)

            if page_records:
                if max_comments_per_post > 0:
                    remaining = max_comments_per_post - accepted_for_post
                    if remaining <= 0:
                        break
                    page_records = page_records[:remaining]
                saved_count = await save_func(page_records)
                accepted_for_post += saved_count
                total_saved += saved_count

            max_id = data.get("max_id")
            if not max_id or max_id == 0:
                break
            page_count += 1
            await asyncio.sleep(random.uniform(*delay_range))

        return total_saved

    async def fetch_all(self, post_info: PostInfo) -> List[CommentRecord]:
        """
        抓取帖子的全部评论（主评论 + 楼中楼）。

        Args:
            post_info: 帖子信息

        Returns:
            评论记录列表
        """
        all_records: List[CommentRecord] = []

        async def _collect(records: List[CommentRecord]) -> int:
            all_records.extend(records)
            return len(records)

        await self.fetch_and_save(post_info=post_info, save_func=_collect)
        return all_records

    async def _fetch_sub_comments(
        self,
        comment_id: int,
        post_info: PostInfo,
    ) -> List[CommentRecord]:
        """
        抓取单条主评论的楼中楼子评论。

        Args:
            comment_id: 主评论 ID
            post_info: 所属帖子信息

        Returns:
            子评论记录列表
        """
        records: List[CommentRecord] = []
        max_id = 0
        page_count = 0
        max_pages = self._config.request.max_sub_comment_pages
        delay_range = self._config.delay.comment_page_delay

        while page_count < max_pages:
            params = {
                "is_reload": "1" if page_count == 0 else "0",
                "id": str(comment_id),
                "is_show_bulletin": "2",
                "is_mix": "1",
                "fetch_level": "1",
                "count": "20",
                "uid": str(post_info.uid),
                "locale": "zh-CN",
            }
            if max_id:
                params["max_id"] = str(max_id)

            data = await self._client.get_json(COMMENTS_API, params=params)
            if not data:
                break
            if not isinstance(data, dict):
                logger.warning("子评论接口返回异常类型: %s", type(data).__name__)
                break

            sub_list_raw = data.get("data", [])
            sub_list = sub_list_raw if isinstance(sub_list_raw, list) else []
            if not sub_list:
                break

            for sub_data in sub_list:
                if not isinstance(sub_data, dict):
                    continue
                record = CommentRecord.from_api_data(sub_data, post_info, is_reply=True)
                if record.comment_content and record.comment_content != "[回复] ":
                    records.append(record)

            max_id = data.get("max_id")
            if not max_id or max_id == 0:
                break
            page_count += 1
            await asyncio.sleep(random.uniform(*delay_range))

        return records
