# -*- coding: utf-8 -*-
"""
应用主入口模块
=============
统一调度多抓取模式：
    - 关键词模式
    - 链接模式
    - 用户模式（主页 + 时间线）
"""

import asyncio
import os
import time

from .config import CrawlerConfig, get_default_config
from .cookie_manager import WeiboCookieManager
from .http_client import HttpClient
from .logger import get_logger, setup_logging
from .scrapers.comment_scraper import CommentScraper
from .scrapers.keyword_scraper import KeywordScraper
from .scrapers.link_scraper import LinkScraper
from .scrapers.post_scraper import PostScraper
from .scrapers.user_scraper import UserScraper
from .storage import CsvStorage

logger = get_logger("app")


class CrawlerApp:
    """微博爬虫主应用（多模式）。"""

    def __init__(self, config: CrawlerConfig | None = None) -> None:
        self._config = config or get_default_config()
        self._storage = CsvStorage(self._config)

    async def run(self) -> None:
        setup_logging()
        self._storage.load_history()

        cookie_manager = WeiboCookieManager()
        self._config.cookies = await cookie_manager.get_cookies()

        cc = self._config.concurrency
        keyword_targets = self._config.active_keyword_targets
        post_url_targets = self._config.active_post_url_targets
        user_targets = self._config.active_user_targets

        logger.info("🚀 开始微博多模式采集")
        logger.info(
            "⚡ 并发配置: 关键词×%d | 帖子详情×%d | 评论×%d | 用户×%d | 全局请求×%d",
            cc.keyword_concurrency,
            cc.post_detail_concurrency,
            cc.comment_concurrency,
            cc.user_concurrency,
            cc.global_concurrency,
        )
        logger.info(
            "🎯 目标统计: 关键词 %d | 帖子链接 %d | 用户目标 %d",
            len(keyword_targets),
            len(post_url_targets),
            len(user_targets),
        )
        logger.info(
            "🧩 运行模式: %s | 保存格式: %s",
            ",".join(self._config.enabled_modes),
            self._config.normalized_save_format,
        )
        logger.info(
            "💬 评论策略: keyword=%s post_url=%s user=%s | 一级=%s 二级=%s",
            self._config.comments.enable_for_keyword,
            self._config.comments.enable_for_post_url,
            self._config.comments.enable_for_user,
            self._config.comments.fetch_top_level,
            self._config.comments.fetch_sub_level,
        )
        logger.info(
            "🖼️ 媒体策略: mode=%s | 类型=%s | 主页媒体=%s | 下载=%s | 覆盖=%s",
            self._config.media_mode_enabled,
            self._config.media_type_mode_enabled,
            self._config.profile_media_enabled,
            self._config.download.enable_media_download,
            self._config.download.overwrite_existing,
        )
        logger.info("👤 主页信息抓取: %s", self._config.profile_info_enabled)
        logger.info(
            "📏 抓取上限: 每页帖子 %d | 每帖评论 %d | 每关键词帖子 %d | 每关键词评论 %d | 每用户翻页 %s | 每用户帖子 %d",
            self._config.request.max_posts_per_search_page,
            self._config.request.max_comments_per_post,
            self._config.request.max_posts_per_keyword,
            self._config.request.max_comments_per_keyword,
            str(self._config.max_user_pages_enabled) if self._config.max_user_pages_enabled > 0 else "不限",
            self._config.request.max_posts_per_user,
        )

        await self._storage.start_writer()
        try:
            async with HttpClient(self._config) as client:
                comment_scraper = CommentScraper(client, self._config)
                post_scraper = PostScraper(
                    client=client,
                    comment_scraper=comment_scraper,
                    storage=self._storage,
                    config=self._config,
                )
                keyword_scraper = KeywordScraper(
                    client=client,
                    post_scraper=post_scraper,
                    storage=self._storage,
                    config=self._config,
                )
                link_scraper = LinkScraper(post_scraper=post_scraper, config=self._config)
                user_scraper = UserScraper(
                    client=client,
                    post_scraper=post_scraper,
                    storage=self._storage,
                    config=self._config,
                )

                if keyword_targets:
                    await self._run_keyword_mode(keyword_scraper)

                if self._config.is_mode_enabled("post_url") and post_url_targets:
                    await link_scraper.process_links(
                        post_url_targets,
                        source_mode="post_url",
                    )
                elif self._config.is_mode_enabled("post_url"):
                    logger.warning("⚠️ 链接模式未获取到可用目标，已跳过")

                if self._config.is_mode_enabled("user") and user_targets:
                    await user_scraper.process_users(
                        user_targets,
                        force_fetch_timeline=True,
                        force_fetch_profile=self._config.profile_info_enabled,
                    )
                elif self._config.is_mode_enabled("user"):
                    logger.warning("⚠️ 用户模式未获取到可用目标，已跳过")

        except KeyboardInterrupt:
            logger.warning("🛑 程序被用户中断")
        except Exception as exc:
            logger.error("❌ 发生异常: %s", exc, exc_info=True)
        finally:
            await self._storage.stop_writer()
            self._log_final_summary()

    async def _run_keyword_mode(self, keyword_scraper: KeywordScraper) -> None:
        keywords = self._config.active_keyword_targets
        if not keywords:
            logger.warning("⚠️ keyword 模式已启用，但 TARGET_KEYWORDS 为空，已跳过")
            return
        batch_size = max(1, self._config.concurrency.keyword_concurrency)

        logger.info("")
        logger.info("🔍 启动关键词模式")
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i: i + batch_size]
            batch_num = i // batch_size + 1
            logger.info("📦 关键词批次 %d: %s", batch_num, ", ".join(batch))
            tasks = [keyword_scraper.process_keyword(keyword) for keyword in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("📊 当前累计评论: %d", self._storage.total_comments_saved)

    def _log_final_summary(self) -> None:
        logger.info("")
        logger.info(
            "🎉 采集完成: 评论 %d | 帖子 %d | 媒体 %d | 主页 %d",
            self._storage.total_comments_saved,
            self._storage.total_posts_saved,
            self._storage.total_media_saved,
            self._storage.total_profiles_saved,
        )
        for name, file_path in self._storage.output_summary.items():
            logger.info("📂 %-9s -> %s", name, os.path.abspath(file_path))


def main() -> None:
    start_time = time.time()
    app = CrawlerApp()
    asyncio.run(app.run())
    elapsed = time.time() - start_time
    logger.info("⏱ 总耗时: %.1f 秒 (%.1f 分钟)", elapsed, elapsed / 60)


if __name__ == "__main__":
    main()
