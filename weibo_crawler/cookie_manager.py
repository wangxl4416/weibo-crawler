# -*- coding: utf-8 -*-
"""
Cookie 管理模块
=============
通过 Playwright 持续化会话实现全自动微博登录与 Cookie 提取。
首次运行会在本地目录下生成 `.weibo_auth` 隐藏文件夹保存登录状态。
"""

import os
import asyncio
from typing import Dict
from playwright.async_api import async_playwright

from .logger import get_logger

logger = get_logger("cookie_manager")

class WeiboCookieManager:
    """管理微博 Cookie，封装 Playwright 提取逻辑"""
    
    def __init__(self, user_data_dir: str = ".weibo_auth"):
        self.user_data_dir = os.path.abspath(user_data_dir)
        self.target_url = "https://weibo.com"

    async def _check_cookies(self, cookies: list) -> bool:
        """检查是否有 SUB 和 SUBP cookie，代表已登录"""
        has_sub = False
        has_subp = False
        for cookie in cookies:
            if cookie["name"] == "SUB":
                has_sub = True
            if cookie["name"] == "SUBP":
                has_subp = True
        return has_sub and has_subp

    async def get_cookies(self) -> Dict[str, str]:
        """获取微博 Cookie。如果本地无有效 Cookie，则弹窗让用户扫码登录。"""
        os.makedirs(self.user_data_dir, exist_ok=True)
        
        async with async_playwright() as p:
            # 1. 尝试静默后台模式启动，检查当前登录状态是否依然有效
            logger.info("🔍 正在检查本地微博登录状态...")
            context = await p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=True
            )
            cookies = await context.cookies(self.target_url)
            
            if await self._check_cookies(cookies):
                logger.info("✅ 检测到本地有效缓存，静默提取 Cookie 成功！")
                cookie_dict = {c["name"]: c["value"] for c in cookies}
                await context.close()
                return cookie_dict
                
            await context.close()
            
            # 2. 如果无状态或状态失效，带界面启动让用户扫码
            logger.info("⚠️ 本地暂无有效的微博登录状态或已过期！")
            logger.info("👉 即将弹出浏览器，二维码将直接显示，请使用微博 App 扫码登录...")
            logger.info("👉 注意：登录成功后，浏览器窗口将自动关闭，请不要手动关闭它！")
            
            context = await p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=False,
                viewport={"width": 500, "height": 600}
            )
            
            page = await context.new_page()
            # 直接跳转到微博扫码登录页，二维码会自动显示
            login_url = "https://passport.weibo.com/sso/signin?entry=miniblog&source=miniblog"
            try:
                await page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                logger.warning(f"登录页加载可能未完全完成，但我们将继续: {e}")
            
            # 等待二维码元素加载
            try:
                await page.wait_for_selector(
                    "img[src*='qr'], img[src*='barcode'], canvas, .qrcode, .qr-img",
                    timeout=15000
                )
                logger.info("📱 二维码已显示，请打开微博 App 扫码登录！")
            except Exception:
                logger.info("📱 请在弹出的浏览器中完成扫码登录...")
            
            # 循环检测是否登录成功
            logger.info("⏳ 等待用户扫码登录中...")
            poll_count = 0
            while True:
                cookies = await context.cookies(self.target_url)
                if await self._check_cookies(cookies):
                    logger.info("🎉 登录成功！已接管登录状态。")
                    cookie_dict = {c["name"]: c["value"] for c in cookies}
                    break
                poll_count += 1
                # 每 60 秒（约 30 次轮询）提醒一次二维码可能过期
                if poll_count % 30 == 0:
                    logger.info("⏳ 二维码可能已过期，请在浏览器中刷新二维码后重新扫码...")
                await asyncio.sleep(2)
                
            # 给页面一点点时间让其种完所有次要 Cookie
            await asyncio.sleep(2)
            cookies = await context.cookies(self.target_url)
            cookie_dict.update({c["name"]: c["value"] for c in cookies})
            
            await context.close()
            return cookie_dict
