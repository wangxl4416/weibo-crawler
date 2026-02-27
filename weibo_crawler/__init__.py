# -*- coding: utf-8 -*-
"""
微博多模式高并发爬虫 (Weibo Multi-Mode Crawler)
==============================================
基于 asyncio + aiohttp 的微博数据采集框架。

支持特性:
    - 关键词模式 / 链接模式 / 用户模式 / 媒体模式 / 主页模式
    - 全局限流与重试
    - 增量去重与断点续采
    - 分类型结构化存储（comments/posts/media/profiles）
"""

__version__ = "2.1.0"
__author__ = "程序员Arise"
