# -*- coding: utf-8 -*-
"""
微博多模式爬虫 - 启动入口
=======================
@Author: 程序员Arise

使用方式:
    python run.py

等价于:
    python -m weibo_crawler

配置修改:
    编辑 weibo_crawler/user_config.py
"""

import sys

BANNER = """
=========================================================
 🕷️  微博多模式自动爬虫 (Weibo Async Crawler)
 👤  Author: 程序员Arise
 🚀  Powered by: aiohttp + Playwright
=========================================================
"""

def print_banner():
    print(f"\033[1;36m{BANNER}\033[0m")

print_banner()

from weibo_crawler.__main__ import main

if __name__ == "__main__":
    main()
