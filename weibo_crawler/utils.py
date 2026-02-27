# -*- coding: utf-8 -*-
"""
工具函数模块
===========
包含时间解析、文本清理与微博链接解析等通用能力。
"""

import re
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse


BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE62_MAP = {char: index for index, char in enumerate(BASE62_ALPHABET)}


def parse_relative_time(time_str: str) -> str:
    """
    将微博时间描述转换为标准时间字符串。

    返回格式: YYYY-MM-DD HH:MM:SS
    """
    now = datetime.now()
    if not time_str:
        return "未知时间"

    time_str = time_str.strip()
    try:
        if re.search(r"\d{4}年", time_str):
            dt = datetime.strptime(time_str, "%Y年%m月%d日 %H:%M")
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        if "分钟" in time_str:
            minutes = int(re.search(r"(\d+)", time_str).group(1))
            return (now - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")

        if "小时" in time_str:
            hours = int(re.search(r"(\d+)", time_str).group(1))
            return (now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")

        if "今天" in time_str:
            time_part = re.search(r"(\d{1,2}:\d{2})", time_str)
            if time_part:
                h, m = map(int, time_part.group(1).split(":"))
                return now.replace(hour=h, minute=m, second=0).strftime("%Y-%m-%d %H:%M:%S")

        if "昨天" in time_str:
            time_part = re.search(r"(\d{1,2}:\d{2})", time_str)
            if time_part:
                h, m = map(int, time_part.group(1).split(":"))
                dt = now - timedelta(days=1)
                return dt.replace(hour=h, minute=m, second=0).strftime("%Y-%m-%d %H:%M:%S")

        if re.match(r"\d{1,2}月\d{1,2}日", time_str):
            dt = datetime.strptime(time_str, "%m月%d日")
            return dt.replace(year=now.year).strftime("%Y-%m-%d %H:%M:%S")

        if re.search(r"\w{3} \w{3} \d{1,2}", time_str):
            dt = datetime.strptime(time_str, "%a %b %d %H:%M:%S %z %Y")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "解析失败"

    return "未知时间"


def clean_html(text: str) -> str:
    """清理 HTML 标签，保留纯文本"""
    return re.sub(r"<.*?>", "", text or "").strip()


def parse_count(value: object) -> int:
    """
    将微博常见计数字段统一转为整数。
    支持:
        - int/float
        - "123"
        - "2.5万" / "1.2亿"
        - "100万+" / "3,200"
    """
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().replace(",", "")
    if not text:
        return 0

    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return 0

    number = float(match.group(1))
    unit = ""
    unit_match = re.search(r"(亿|万|[kKmMbB])", text)
    if unit_match:
        unit = unit_match.group(1).lower()

    multiplier = 1
    if unit == "亿":
        multiplier = 100_000_000
    elif unit == "万":
        multiplier = 10_000
    elif unit == "k":
        multiplier = 1_000
    elif unit == "m":
        multiplier = 1_000_000
    elif unit == "b":
        multiplier = 1_000_000_000

    return int(number * multiplier)


def truncate_text(text: str, max_length: int = 30) -> str:
    """截断过长文本"""
    text = text or ""
    if len(text) > max_length:
        return text[:max_length] + "..."
    return text


def _base62_to_int(value: str) -> int:
    num = 0
    for char in value:
        num = num * 62 + BASE62_MAP[char]
    return num


def bid_to_mid(bid: str) -> str:
    """
    将微博 bid(base62) 转为 mid(数字 ID)。
    """
    bid = (bid or "").strip()
    if not bid:
        return ""
    if bid.isdigit():
        return bid

    result = ""
    for i in range(len(bid), 0, -4):
        start = max(i - 4, 0)
        chunk = bid[start:i]
        chunk_num = _base62_to_int(chunk)
        if start > 0:
            result = f"{chunk_num:07d}{result}"
        else:
            result = f"{chunk_num}{result}"
    return result.lstrip("0") or "0"


def extract_post_id_from_url(url: str) -> str:
    """
    从微博链接提取帖子 ID，支持多种格式：
        - https://weibo.com/detail/<id_or_bid>
        - https://weibo.com/<uid>/<bid>
        - https://weibo.com/u/<uid>?id=<id>
        - https://m.weibo.cn/detail/<id>
        - ...?mid=<id> / ?id=<id>
    """
    if not url:
        return ""

    cleaned_url = url.strip()
    parsed = urlparse(cleaned_url)
    if not parsed.scheme and "weibo.com" in cleaned_url:
        parsed = urlparse(f"https://{cleaned_url.lstrip('/')}")
    if not parsed.scheme:
        return ""

    query = parse_qs(parsed.query)
    for key in ("id", "mid"):
        values = query.get(key)
        if values and values[0]:
            return values[0].strip()
    layer_values = query.get("layerid")
    if layer_values and layer_values[0]:
        layer_id = layer_values[0].strip()
        # 兼容 https://weibo.com/?layerid=5270588752661663
        match = re.search(r"(\d{8,})", layer_id)
        if match:
            return match.group(1)
    bid_values = query.get("bid")
    if bid_values and bid_values[0]:
        return bid_to_mid(bid_values[0])

    path = (parsed.path or "").strip("/")
    if not path:
        return ""

    # 用户主页链接不应被识别为帖子链接
    if re.fullmatch(r"u/\d+", path):
        return ""
    if re.fullmatch(r"n/[^/]+", path):
        return ""

    patterns = [
        r"detail/([A-Za-z0-9]+)",
        r"status/([A-Za-z0-9]+)",
        r"tv/show/([A-Za-z0-9]+)",
        r"u/\d+/([A-Za-z0-9]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, path)
        if match:
            value = match.group(1)
            return value if value.isdigit() else bid_to_mid(value)

    parts = [part for part in path.split("/") if part]
    if len(parts) >= 2:
        candidate = parts[-1]
        if re.fullmatch(r"[A-Za-z0-9]+", candidate):
            return candidate if candidate.isdigit() else bid_to_mid(candidate)

    return ""


def extract_uid_from_target(target: str) -> str:
    """
    从目标字符串提取 uid。
    支持 uid 本身、主页链接、查询参数 uid。
    """
    if not target:
        return ""

    cleaned = target.strip()
    if cleaned.isdigit():
        return cleaned

    parsed = urlparse(cleaned)
    if not parsed.scheme and "weibo.com" in cleaned:
        parsed = urlparse(f"https://{cleaned.lstrip('/')}")
    query = parse_qs(parsed.query)
    uid_values = query.get("uid")
    if uid_values and uid_values[0].isdigit():
        return uid_values[0]

    path = (parsed.path or "").strip("/")
    m = re.search(r"(?:^|/)u/(\d+)(?:/|$)", path)
    if m:
        return m.group(1)

    if parsed.netloc and parsed.netloc.endswith("weibo.com"):
        parts = [item for item in path.split("/") if item]
        for part in parts:
            if part.isdigit():
                return part

    return ""


def extract_custom_from_target(target: str) -> str:
    """
    从用户目标中提取 custom 用户名（用于 profile/info?custom=xxx）。
    """
    if not target:
        return ""

    cleaned = target.strip().lstrip("@")
    if not cleaned:
        return ""
    if cleaned.isdigit():
        return ""

    parsed = urlparse(cleaned)
    if not parsed.scheme and "weibo.com" in cleaned:
        parsed = urlparse(f"https://{cleaned.lstrip('/')}")
    if parsed.netloc and parsed.netloc.endswith("weibo.com"):
        path = (parsed.path or "").strip("/")
        parts = [part for part in path.split("/") if part]
        if not parts:
            return ""
        if parts[0] == "n" and len(parts) >= 2:
            return parts[1]
        if parts[0] not in {"u", "detail", "status", "tv"}:
            return parts[0]
        return ""

    return cleaned


def normalize_media_url(url: str) -> str:
    """
    归一化媒体 URL，去掉动态 query（如 Expires/ssig）。
    """
    if not url:
        return ""
    parsed = urlparse(url.strip())
    if not parsed.scheme and parsed.path:
        return parsed.path
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
