# -*- coding: utf-8 -*-
"""
关键词调度器模块
===============
负责搜索关键词、解析搜索结果页、发现帖子并并发调度 PostScraper。
"""

import asyncio
import random
from urllib.parse import urljoin
from typing import List

from lxml import etree

from ..config import CrawlerConfig
from ..http_client import HttpClient
from ..logger import get_logger
from ..storage import CsvStorage
from ..utils import extract_post_id_from_url
from .post_scraper import PostScraper

logger = get_logger("scrapers.keyword")

# 微博搜索页面 URL
SEARCH_URL = "https://s.weibo.com/weibo"


class KeywordScraper:
    """
    关键词调度器。

    职责:
        - 搜索关键词的所有结果页面
        - 解析 HTML 提取帖子列表
        - 去重过滤已抓取帖子
        - 并发调度 PostScraper 处理每个帖子

    这是抓取流程的最顶层入口。
    """

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

    async def process_keyword(self, keyword: str) -> int:
        """
        处理单个关键词的全部搜索结果页。

        Args:
            keyword: 搜索关键词

        Returns:
            该关键词下抓取的评论总数
        """
        logger.info("=" * 45)
        logger.info("🎯 正在采集关键词：「%s」", keyword)
        logger.info("=" * 45)

        max_posts = max(0, int(self._config.request.max_posts_per_keyword or 0))
        max_comments = max(0, int(self._config.request.max_comments_per_keyword or 0))
        existing_posts = self._storage.get_source_post_count("keyword", keyword)
        existing_comments = self._storage.get_keyword_count(keyword)

        if max_posts > 0 and existing_posts >= max_posts:
            logger.info(
                "⏭️ [%s] 已达到帖子上限：%d/%d，停止该关键词",
                keyword,
                existing_posts,
                max_posts,
            )
            return 0

        if max_comments > 0 and existing_comments >= max_comments:
            logger.info(
                "⏭️ [%s] 已达到评论上限：%d/%d，停止该关键词",
                keyword,
                existing_comments,
                max_comments,
            )
            return 0

        if existing_posts > 0 or existing_comments > 0:
            logger.info(
                "📊 [%s] 现有数据: 帖子 %d%s | 评论 %d%s",
                keyword,
                existing_posts,
                f"/{max_posts}" if max_posts > 0 else "",
                existing_comments,
                f"/{max_comments}" if max_comments > 0 else "",
            )

        total = 0
        page = 1
        max_pages = self._config.request.max_search_pages
        delay_range = self._config.delay.page_delay

        while page <= max_pages:
            current_posts = self._storage.get_source_post_count("keyword", keyword)
            current_comments = self._storage.get_source_count("keyword", keyword)

            if max_posts > 0 and current_posts >= max_posts:
                logger.info("🎯 [%s] 帖子达到上限 %d，停止翻页", keyword, max_posts)
                break
            if max_comments > 0 and current_comments >= max_comments:
                logger.info("🎯 [%s] 评论达到上限 %d，停止翻页", keyword, max_comments)
                break

            remaining_posts = max_posts - current_posts if max_posts > 0 else 0
            try:
                page_total = await self._process_page(
                    keyword=keyword,
                    page=page,
                    remaining_posts=remaining_posts,
                )
                if page_total is None:
                    break  # 无更多数据或需要登录
                total += page_total
                page += 1
                await asyncio.sleep(random.uniform(*delay_range))

            except Exception as e:
                logger.warning("⚠️ [%s] 第 %d 页抓取异常: %s", keyword, page, e)
                await asyncio.sleep(2)
                page += 1

        logger.info(
            "[%s] 完成，本次新增评论 %d 条；当前累计 帖子 %d%s | 评论 %d%s",
            keyword,
            total,
            self._storage.get_source_post_count("keyword", keyword),
            f"/{max_posts}" if max_posts > 0 else "",
            self._storage.get_source_count("keyword", keyword),
            f"/{max_comments}" if max_comments > 0 else "",
        )
        return total

    async def _process_page(
        self,
        keyword: str,
        page: int,
        remaining_posts: int,
    ) -> int | None:
        """
        处理搜索结果的单个页面。

        Args:
            keyword: 搜索关键词
            page: 页码

        Returns:
            抓取的评论数，None 表示应停止翻页
        """
        logger.info("📄 [%s] 扫描搜索结果第 %d 页...", keyword, page)

        html_text = await self._client.get_html(
            SEARCH_URL,
            params={"q": keyword, "page": str(page)}
        )

        if not html_text:
            logger.warning("⚠️ [%s] 第 %d 页请求失败，跳过", keyword, page)
            return 0

        # 检测登录拦截
        if "passport.weibo.com" in html_text:
            logger.error("❌ Cookie 失效或被要求登录！停止采集。")
            return None

        # 解析帖子列表
        cards = self._parse_feed_cards(html_text)
        if not cards:
            logger.info("👻 [%s] 第 %d 页没有数据了", keyword, page)
            return None

        page_post_limit = max(0, int(self._config.request.max_posts_per_search_page or 0))
        effective_limit = page_post_limit
        if remaining_posts > 0:
            if effective_limit <= 0:
                effective_limit = remaining_posts
            else:
                effective_limit = min(effective_limit, remaining_posts)

        # 提取有效帖子并发起并发抓取
        post_tasks = self._collect_post_tasks(
            cards=cards,
            keyword=keyword,
            max_posts=effective_limit,
        )
        if not post_tasks:
            return 0

        # 并发处理本页所有帖子
        results = await asyncio.gather(*post_tasks, return_exceptions=True)
        total = 0
        for r in results:
            if isinstance(r, int):
                total += r
            elif isinstance(r, Exception):
                logger.warning("⚠️ 帖子并发处理异常: %s", r)

        return total

    @staticmethod
    def _parse_feed_cards(html_text: str) -> list:
        """解析 HTML 获取帖子卡片元素列表"""
        html = etree.HTML(html_text)
        return html.xpath('//div[@action-type="feed_list_item"]')

    def _collect_post_tasks(
        self,
        cards: list,
        keyword: str,
        max_posts: int,
    ) -> List[asyncio.Task]:
        """
        从帖子卡片中提取有效帖子，返回异步任务列表。

        Args:
            cards: HTML 帖子卡片元素
            keyword: 搜索关键词

        Returns:
            待执行的异步任务列表
        """
        tasks: List[asyncio.Task] = []
        seen_post_ids = set()

        for card in cards:
            post_id = self._extract_post_id(card)
            if not post_id:
                continue
            if post_id in seen_post_ids:
                continue
            seen_post_ids.add(post_id)
            tasks.append(
                self._post_scraper.process_post_id(
                    post_id=post_id,
                    source_mode="keyword",
                    source_target=keyword,
                )
            )
            if max_posts > 0 and len(tasks) >= max_posts:
                break

        return tasks

    @staticmethod
    def _extract_post_id(card: etree._Element) -> str:
        """
        从搜索卡片中提取帖子 ID（mid/id/bid 多格式兼容）。
        """
        mid = card.xpath("./@mid")
        if mid and mid[0]:
            return str(mid[0]).strip()

        hrefs = card.xpath(".//a[@href]/@href")
        for href in hrefs:
            full_url = href
            if href.startswith("//"):
                full_url = f"https:{href}"
            elif href.startswith("/"):
                full_url = urljoin("https://weibo.com", href)
            post_id = extract_post_id_from_url(full_url)
            if post_id:
                return post_id
        return ""
