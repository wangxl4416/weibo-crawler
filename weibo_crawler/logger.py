# -*- coding: utf-8 -*-
"""
日志管理模块
===========
提供统一的日志系统，替代散落的 print 语句。
支持终端彩色输出 + 文件日志记录。
"""

import logging
import sys
from typing import Optional


# 自定义彩色格式化器
class ColorFormatter(logging.Formatter):
    """终端彩色日志输出"""

    COLORS = {
        logging.DEBUG:    "\033[36m",     # 青色
        logging.INFO:     "\033[32m",     # 绿色
        logging.WARNING:  "\033[33m",     # 黄色
        logging.ERROR:    "\033[31m",     # 红色
        logging.CRITICAL: "\033[1;31m",   # 加粗红色
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelno, self.RESET)
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = "crawler.log"
) -> None:
    """
    初始化全局日志系统。

    Args:
        level: 日志级别
        log_file: 日志文件路径，None 则不写文件
    """
    root_logger = logging.getLogger("weibo_crawler")
    root_logger.setLevel(level)

    # 避免重复添加 handler
    if root_logger.handlers:
        return

    # 终端输出（彩色）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_fmt = ColorFormatter(
        fmt="%(asctime)s │ %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)
    root_logger.addHandler(console_handler)

    # 文件输出
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_fmt = logging.Formatter(
            fmt="%(asctime)s │ %(name)-28s │ %(levelname)-7s │ %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_fmt)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    获取模块级 Logger 实例。

    Args:
        name: 模块名，通常传入 __name__

    Returns:
        配置好的 Logger 实例
    """
    return logging.getLogger(f"weibo_crawler.{name}")
