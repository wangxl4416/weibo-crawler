# -*- coding: utf-8 -*-
"""
异步 HTTP 客户端模块
==================
封装 aiohttp 会话管理，提供带限流和重试的异步请求能力。
使用 async context manager 自动管理连接池生命周期。
"""

import asyncio
import os
import random
from typing import Optional, Any, Dict

import aiohttp

from .config import CrawlerConfig
from .logger import get_logger

logger = get_logger("http_client")


class HttpClient:
    """
    异步 HTTP 客户端。

    特性:
        - TCP 连接池复用
        - Semaphore 全局限流
        - 指数退避重试
        - 自动超时管理

    Usage::

        async with HttpClient(config) as client:
            data = await client.get_json(url, params)
            html = await client.get_html(url, params)
    """

    def __init__(self, config: CrawlerConfig) -> None:
        self._config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        self._semaphore = asyncio.Semaphore(config.concurrency.global_concurrency)

    async def __aenter__(self) -> "HttpClient":
        """创建 HTTP 会话和连接池"""
        concurrency = self._config.concurrency.global_concurrency

        self._connector = aiohttp.TCPConnector(
            limit=concurrency + 5,
            limit_per_host=concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            ssl=False,
        )

        self._session = aiohttp.ClientSession(
            headers=self._config.headers_with_cookie,
            connector=self._connector,
        )

        logger.info("HTTP 客户端已初始化 (连接池: %d)", concurrency)
        return self

    async def __aexit__(self, *exc) -> None:
        """关闭会话和连接池"""
        if self._session:
            await self._session.close()
        logger.info("HTTP 客户端已关闭")

    async def _request(
        self,
        url: str,
        params: Optional[Dict[str, str]] = None,
        as_json: bool = True,
    ) -> Optional[Any]:
        """
        发送带限流和重试的 GET 请求。

        Args:
            url: 请求 URL
            params: 查询参数
            as_json: True 返回 JSON，False 返回文本

        Returns:
            响应数据，失败返回 None
        """
        retries = self._config.request.retry_times
        timeout = aiohttp.ClientTimeout(total=self._config.request.timeout)
        delay_range = self._config.delay.request_delay

        for attempt in range(retries):
            try:
                async with self._semaphore:
                    async with self._session.get(url, params=params, timeout=timeout) as resp:
                        await asyncio.sleep(random.uniform(*delay_range))

                        if resp.status != 200:
                            if attempt < retries - 1:
                                wait_time = 1.0 * (attempt + 1)
                                logger.warning(
                                    "请求失败 [%d] %s (重试 %d/%d, 等待 %.1fs)",
                                    resp.status, url[:60], attempt + 1, retries, wait_time
                                )
                                await asyncio.sleep(wait_time)
                                continue
                            logger.error("请求最终失败 [%d] %s", resp.status, url[:60])
                            return None

                        return await resp.json() if as_json else await resp.text()

            except asyncio.TimeoutError:
                if attempt < retries - 1:
                    logger.warning("请求超时 %s (重试 %d/%d)", url[:60], attempt + 1, retries)
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    logger.error("请求超时最终失败 %s", url[:60])
                    return None

            except Exception as e:
                if attempt < retries - 1:
                    logger.warning("请求异常 %s: %s (重试 %d/%d)", url[:60], e, attempt + 1, retries)
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    logger.error("请求异常最终失败 %s: %s", url[:60], e)
                    return None

        return None

    async def get_json(
        self,
        url: str,
        params: Optional[Dict[str, str]] = None
    ) -> Optional[Dict]:
        """
        发送 GET 请求，返回 JSON 数据。

        Args:
            url: 请求 URL
            params: 查询参数

        Returns:
            JSON 字典数据，失败返回 None
        """
        return await self._request(url, params, as_json=True)

    async def get_html(
        self,
        url: str,
        params: Optional[Dict[str, str]] = None
    ) -> Optional[str]:
        """
        发送 GET 请求，返回 HTML 文本。

        Args:
            url: 请求 URL
            params: 查询参数

        Returns:
            HTML 文本，失败返回 None
        """
        return await self._request(url, params, as_json=False)

    async def download_file(
        self,
        url: str,
        file_path: str,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        下载二进制文件到本地。

        Args:
            url: 文件 URL
            file_path: 本地保存路径
            extra_headers: 额外请求头（如 referer）

        Returns:
            bool: True=成功，False=失败
        """
        retries = self._config.request.retry_times
        timeout = aiohttp.ClientTimeout(total=self._config.request.timeout)
        delay_range = self._config.delay.request_delay

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        headers = {}
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(retries):
            try:
                async with self._semaphore:
                    async with self._session.get(
                        url,
                        timeout=timeout,
                        headers=headers or None,
                    ) as resp:
                        await asyncio.sleep(random.uniform(*delay_range))

                        if resp.status != 200:
                            if attempt < retries - 1:
                                await asyncio.sleep(1.0 * (attempt + 1))
                                continue
                            logger.warning("媒体下载失败 [%d] %s", resp.status, url[:80])
                            return False

                        content = await resp.read()
                        if not content:
                            logger.warning("媒体下载返回空内容: %s", url[:80])
                            return False

                        temp_path = f"{file_path}.part"
                        with open(temp_path, "wb") as file_obj:
                            file_obj.write(content)
                        os.replace(temp_path, file_path)
                        return True

            except Exception as exc:
                if attempt < retries - 1:
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                logger.warning("媒体下载异常: %s | %s", url[:80], exc)
                return False

        return False
