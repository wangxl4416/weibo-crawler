# -*- coding: utf-8 -*-
"""
数据存储模块
===========
存储策略：
    - 按模式分目录：keyword / post_url / user
    - 每个模式目录内按数据类型分文件：
      comments.csv / comments.jsonl
      posts.csv / posts.jsonl
      media.csv / media.jsonl
      profiles.csv / profiles.jsonl
"""

import asyncio
import csv
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Set, Tuple

from .config import (
    COMMENT_COLUMNS,
    MEDIA_COLUMNS,
    POST_COLUMNS,
    PROFILE_COLUMNS,
    CrawlerConfig,
)
from .logger import get_logger
from .models import CommentRecord, MediaRecord, PostInfo, UserProfile
from .utils import normalize_media_url

logger = get_logger("storage")


@dataclass
class _WritePayload:
    """写入队列中的批处理载荷。"""
    csv_path: str = ""
    csv_columns: List[str] = field(default_factory=list)
    csv_rows: List[Dict[str, Any]] = field(default_factory=list)
    json_path: str = ""
    json_rows: List[Dict[str, Any]] = field(default_factory=list)


class CsvStorage:
    """
    结构化存储器（保留原类名 `CsvStorage` 兼容旧调用）。
    """

    def __init__(self, config: CrawlerConfig) -> None:
        self._config = config
        self._lock = asyncio.Lock()

        output_dir = config.output.output_dir
        self._output_dir = output_dir
        self._text_dir = os.path.join(output_dir, config.output.text_output_dir)
        self._media_dir = os.path.join(output_dir, config.output.media_output_dir)

        self._seen_post_keys: Set[Tuple[str, str]] = set()
        self._seen_comment_keys: Set[Tuple[str, str, str, str]] = set()
        self._seen_media_keys: Set[str] = set()
        self._seen_profile_keys: Set[str] = set()
        self._csv_header_checked: Set[str] = set()
        self._source_counts: Counter = Counter()
        self._source_post_counts: Counter = Counter()
        self._post_comment_counts: Counter = Counter()

        self._total_comments_saved = 0
        self._total_posts_saved = 0
        self._total_media_saved = 0
        self._total_profiles_saved = 0
        self._recent_post_urls: List[str] = []
        self._recent_user_ids: List[str] = []

        self._writer_queue: asyncio.Queue[Any] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._writer_stop_token = object()
        self._writer_batch_size = 200
        self._writer_flush_interval = 0.25

    @property
    def total_saved(self) -> int:
        """向后兼容：总评论保存数"""
        return self._total_comments_saved

    @property
    def total_comments_saved(self) -> int:
        return self._total_comments_saved

    @property
    def total_posts_saved(self) -> int:
        return self._total_posts_saved

    @property
    def total_media_saved(self) -> int:
        return self._total_media_saved

    @property
    def total_profiles_saved(self) -> int:
        return self._total_profiles_saved

    @property
    def output_file(self) -> str:
        """向后兼容：默认输出主文件为评论 CSV"""
        return self._mode_text_csv_path("keyword", "comments")

    @property
    def output_summary(self) -> Dict[str, str]:
        summary: Dict[str, str] = {}
        summary["text_dir"] = self._text_dir
        for mode in ("keyword", "post_url", "user", "other"):
            mode_dir = self._mode_text_dir(mode)
            if os.path.isdir(mode_dir):
                summary[f"text_{mode}"] = mode_dir
        summary["media_files"] = self._media_dir
        return summary

    def get_recent_post_urls(self, limit: int = 10) -> List[str]:
        """获取本次运行中发现的最近帖子链接（用于自动模式回填）"""
        if limit <= 0:
            return []
        return self._recent_post_urls[:limit]

    def get_recent_user_targets(self, limit: int = 5) -> List[str]:
        """获取本次运行中发现的最近用户 uid（用于自动模式回填）"""
        if limit <= 0:
            return []
        return self._recent_user_ids[:limit]

    @staticmethod
    def _normalize_output_mode(source_mode: str) -> str:
        value = str(source_mode or "").strip().lower()
        aliases = {
            "keyword": "keyword",
            "post_url": "post_url",
            "url": "post_url",
            "link": "post_url",
            "comment": "post_url",
            "user": "user",
            "personal": "user",
            "profile": "user",
        }
        return aliases.get(value, "other")

    def _mode_text_dir(self, source_mode: str) -> str:
        mode = self._normalize_output_mode(source_mode)
        return os.path.join(self._text_dir, mode)

    def _mode_text_csv_path(self, source_mode: str, data_kind: str) -> str:
        return os.path.join(self._mode_text_dir(source_mode), f"{data_kind}.csv")

    def _mode_text_json_path(self, source_mode: str, data_kind: str) -> str:
        return os.path.join(self._mode_text_dir(source_mode), f"{data_kind}.jsonl")

    def _iter_text_files(self, filename: str) -> List[str]:
        """
        扫描文本输出目录，返回指定文件名的所有路径（含旧结构兼容）。
        """
        paths: List[str] = []

        legacy_path = os.path.join(self._text_dir, filename)
        if os.path.isfile(legacy_path):
            paths.append(legacy_path)

        if os.path.isdir(self._text_dir):
            for entry in os.listdir(self._text_dir):
                mode_dir = os.path.join(self._text_dir, entry)
                if not os.path.isdir(mode_dir):
                    continue
                candidate = os.path.join(mode_dir, filename)
                if os.path.isfile(candidate):
                    paths.append(candidate)

        # 保序去重
        unique_paths: List[str] = []
        seen: Set[str] = set()
        for path in paths:
            if path in seen:
                continue
            seen.add(path)
            unique_paths.append(path)
        return unique_paths

    async def start_writer(self) -> None:
        """
        启动后台写入协程（批量刷盘）。
        """
        if self._has_active_writer():
            return
        self._writer_queue = asyncio.Queue()
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info(
            "🧵 存储写入队列已启动: batch=%d, flush=%.2fs",
            self._writer_batch_size,
            self._writer_flush_interval,
        )

    async def stop_writer(self) -> None:
        """
        停止后台写入协程并强制 flush 队列。
        """
        if not self._has_active_writer():
            self._writer_task = None
            self._writer_queue = None
            return
        if self._writer_queue is None or self._writer_task is None:
            return

        queue = self._writer_queue
        task = self._writer_task
        queue.put_nowait(self._writer_stop_token)
        await task
        self._writer_task = None
        self._writer_queue = None
        logger.info("🧵 存储写入队列已停止，缓存数据已落盘")

    def _has_active_writer(self) -> bool:
        return (
            self._writer_queue is not None
            and self._writer_task is not None
            and not self._writer_task.done()
        )

    async def _writer_loop(self) -> None:
        pending_payloads: List[_WritePayload] = []
        if self._writer_queue is None:
            return
        queue = self._writer_queue

        while True:
            try:
                item = await asyncio.wait_for(
                    queue.get(),
                    timeout=self._writer_flush_interval,
                )
            except asyncio.TimeoutError:
                if pending_payloads:
                    await self._flush_payloads(pending_payloads)
                    pending_payloads = []
                continue
            except Exception as exc:
                logger.warning("⚠️ writer 队列读取异常: %s", exc)
                continue

            if item is self._writer_stop_token:
                if pending_payloads:
                    await self._flush_payloads(pending_payloads)
                    pending_payloads = []
                break

            if isinstance(item, _WritePayload):
                pending_payloads.append(item)
                if len(pending_payloads) >= self._writer_batch_size:
                    await self._flush_payloads(pending_payloads)
                    pending_payloads = []

        # 停止前再兜底 flush
        if pending_payloads:
            await self._flush_payloads(pending_payloads)

    async def _flush_payloads(self, payloads: List[_WritePayload]) -> None:
        if not payloads:
            return
        await asyncio.to_thread(self._flush_payloads_sync, payloads)

    def _flush_payloads_sync(self, payloads: List[_WritePayload]) -> None:
        json_batches: Dict[str, List[Dict[str, Any]]] = {}
        csv_batches: Dict[Tuple[str, Tuple[str, ...]], List[Dict[str, Any]]] = {}

        for payload in payloads:
            if payload.json_path and payload.json_rows:
                json_batches.setdefault(payload.json_path, []).extend(payload.json_rows)
            if payload.csv_path and payload.csv_columns and payload.csv_rows:
                key = (payload.csv_path, tuple(payload.csv_columns))
                csv_batches.setdefault(key, []).extend(payload.csv_rows)

        for file_path, rows in json_batches.items():
            try:
                self._append_jsonl_batch(file_path, rows)
            except Exception as exc:
                logger.warning("⚠️ writer 批量写入 JSON 失败: %s | %s", file_path, exc)

        for (file_path, columns), rows in csv_batches.items():
            try:
                self._append_csv(file_path, list(columns), rows)
            except Exception as exc:
                logger.warning("⚠️ writer 批量写入 CSV 失败: %s | %s", file_path, exc)

    def _enqueue_payload(self, payload: _WritePayload) -> bool:
        if not self._has_active_writer():
            return False
        if self._writer_queue is None:
            return False
        self._writer_queue.put_nowait(payload)
        return True

    def _write_payload_sync(self, payload: _WritePayload) -> bool:
        json_written = False
        csv_written = False

        if payload.json_path and payload.json_rows:
            try:
                self._append_jsonl_batch(payload.json_path, payload.json_rows)
                json_written = True
            except Exception as exc:
                logger.warning("⚠️ 写入 JSON 失败: %s", exc)

        if payload.csv_path and payload.csv_columns and payload.csv_rows:
            try:
                self._append_csv(payload.csv_path, payload.csv_columns, payload.csv_rows)
                csv_written = True
            except Exception as exc:
                logger.warning("⚠️ 写入 CSV 失败: %s", exc)

        return json_written or csv_written

    def _build_payload(
        self,
        data_kind: str,
        source_mode: str,
        csv_rows: List[Dict[str, Any]],
        csv_columns: List[str],
        json_rows: List[Dict[str, Any]] | None = None,
    ) -> _WritePayload:
        if json_rows is None:
            json_rows = csv_rows
        return _WritePayload(
            csv_path=self._mode_text_csv_path(source_mode, data_kind) if self._config.write_csv else "",
            csv_columns=list(csv_columns) if self._config.write_csv else [],
            csv_rows=csv_rows if self._config.write_csv else [],
            json_path=self._mode_text_json_path(source_mode, data_kind) if self._config.write_json else "",
            json_rows=json_rows if self._config.write_json else [],
        )

    def _dispatch_payloads(self, payloads: List[_WritePayload]) -> bool:
        for payload in payloads:
            if self._enqueue_payload(payload):
                continue
            if not self._write_payload_sync(payload):
                return False
        return True

    def load_history(self) -> None:
        """加载历史数据，用于增量去重和统计"""
        os.makedirs(self._output_dir, exist_ok=True)
        os.makedirs(self._text_dir, exist_ok=True)
        os.makedirs(self._media_dir, exist_ok=True)
        self._load_posts_history()
        self._load_comments_history()
        self._load_media_history()
        self._load_profiles_history()
        logger.info(
            "📚 历史数据加载完成: 帖子 %d | 评论 %d | 媒体 %d | 主页 %d",
            len(self._seen_post_keys),
            len(self._seen_comment_keys),
            len(self._seen_media_keys),
            len(self._seen_profile_keys),
        )

    def _load_posts_history(self) -> None:
        def _ingest_post(
            post_id: str,
            uid: str,
            content: str,
            source_mode: str,
            source_target: str,
        ) -> None:
            post_id = str(post_id or "").strip()
            uid = str(uid or "").strip()
            content = str(content or "")
            source_mode = str(source_mode or "").strip()
            source_target = str(source_target or "").strip()

            dedup_key: Tuple[str, str] | None = None
            if post_id:
                dedup_key = ("post_id", post_id)
            elif uid and content:
                dedup_key = ("user_content", f"{uid}:{content[:80]}")

            if not dedup_key:
                return

            is_new = dedup_key not in self._seen_post_keys
            self._seen_post_keys.add(dedup_key)

            if is_new and source_mode and source_target:
                self._source_post_counts[(source_mode, source_target)] += 1

        for file_path in self._iter_text_files("posts.jsonl"):
            try:
                with open(file_path, "r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        if not line.strip():
                            continue
                        data = json.loads(line)
                        _ingest_post(
                            post_id=str(data.get("post_id") or ""),
                            uid=str(data.get("uid") or ""),
                            content=str(data.get("content") or ""),
                            source_mode=str(data.get("source_mode") or ""),
                            source_target=str(data.get("source_target") or ""),
                        )
            except Exception as exc:
                logger.warning("⚠️ 读取历史帖子(JSON)失败: %s | %s", file_path, exc)

        for file_path in self._iter_text_files("posts.csv"):
            try:
                with open(file_path, "r", encoding="utf-8-sig", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        _ingest_post(
                            post_id=str(row.get("帖子ID", "")),
                            uid="",
                            content=str(row.get("帖子内容", "")),
                            source_mode=str(row.get("来源模式", "")),
                            source_target=str(row.get("来源目标", "")),
                        )
            except Exception as exc:
                logger.warning("⚠️ 读取历史帖子(CSV)失败: %s | %s", file_path, exc)

        if self._seen_post_keys:
            logger.info("✅ 已加载历史帖子去重键: %d", len(self._seen_post_keys))

    def _load_comments_history(self) -> None:
        def _ingest_row(row: Dict[str, str]) -> None:
            key = (
                str(row.get("帖子ID", "")),
                str(row.get("评论者", "")),
                str(row.get("评论内容", ""))[:120],
                str(row.get("评论时间", "")),
            )
            if key in self._seen_comment_keys:
                return
            self._seen_comment_keys.add(key)
            post_id = str(row.get("帖子ID", ""))
            if post_id:
                self._post_comment_counts[post_id] += 1
            source_mode = str(row.get("来源模式", ""))
            source_target = str(row.get("来源目标", ""))
            if source_mode and source_target:
                self._source_counts[(source_mode, source_target)] += 1

        for file_path in self._iter_text_files("comments.csv"):
            try:
                with open(file_path, "r", encoding="utf-8-sig", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        _ingest_row(row)
            except Exception as exc:
                logger.warning("⚠️ 读取历史评论(CSV)失败: %s | %s", file_path, exc)

        for file_path in self._iter_text_files("comments.jsonl"):
            try:
                with open(file_path, "r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        if isinstance(row, dict):
                            _ingest_row(row)
            except Exception as exc:
                logger.warning("⚠️ 读取历史评论(JSON)失败: %s | %s", file_path, exc)

        if self._seen_comment_keys:
            logger.info("✅ 已加载历史评论去重键: %d", len(self._seen_comment_keys))

    def _load_media_history(self) -> None:
        def _ingest_row(row: Dict[str, str]) -> None:
            media_url = str(row.get("媒体链接", "")).strip()
            if media_url:
                post_id = str(row.get("帖子ID", "")).strip()
                media_type = str(row.get("媒体类型", "")).strip()
                normalized = normalize_media_url(media_url)
                dedup_key = f"{post_id}|{media_type}|{normalized}"
                self._seen_media_keys.add(dedup_key)

        for file_path in self._iter_text_files("media.csv"):
            try:
                with open(file_path, "r", encoding="utf-8-sig", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        _ingest_row(row)
            except Exception as exc:
                logger.warning("⚠️ 读取历史媒体(CSV)失败: %s | %s", file_path, exc)

        for file_path in self._iter_text_files("media.jsonl"):
            try:
                with open(file_path, "r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        if isinstance(row, dict):
                            _ingest_row(row)
            except Exception as exc:
                logger.warning("⚠️ 读取历史媒体(JSON)失败: %s | %s", file_path, exc)

        if self._seen_media_keys:
            logger.info("✅ 已加载历史媒体去重键: %d", len(self._seen_media_keys))

    def _load_profiles_history(self) -> None:
        for file_path in self._iter_text_files("profiles.jsonl"):
            try:
                with open(file_path, "r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        if not line.strip():
                            continue
                        data = json.loads(line)
                        uid = str(data.get("uid") or "").strip()
                        if uid:
                            self._seen_profile_keys.add(uid)
            except Exception as exc:
                logger.warning("⚠️ 读取历史主页(JSON)失败: %s | %s", file_path, exc)

        for file_path in self._iter_text_files("profiles.csv"):
            try:
                with open(file_path, "r", encoding="utf-8-sig", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        uid = str(row.get("用户ID", "")).strip()
                        if uid:
                            self._seen_profile_keys.add(uid)
            except Exception as exc:
                logger.warning("⚠️ 读取历史主页(CSV)失败: %s | %s", file_path, exc)

        if self._seen_profile_keys:
            logger.info("✅ 已加载历史主页去重键: %d", len(self._seen_profile_keys))

    def is_new_post(self, user_name: str, content: str) -> bool:
        """
        向后兼容：按“用户名 + 文本片段”去重。
        """
        key = ("user_content", f"{user_name}:{content[:80]}")
        if key in self._seen_post_keys:
            return False
        self._seen_post_keys.add(key)
        return True

    def has_post(self, post_info: PostInfo) -> bool:
        return post_info.dedup_key in self._seen_post_keys

    def get_keyword_count(self, keyword: str) -> int:
        """向后兼容：关键词累计评论数"""
        return self.get_source_count("keyword", keyword)

    def get_source_count(self, source_mode: str, source_target: str) -> int:
        return self._source_counts.get((source_mode, source_target), 0)

    def get_source_post_count(self, source_mode: str, source_target: str) -> int:
        return self._source_post_counts.get((source_mode, source_target), 0)

    async def save_post(self, post: PostInfo) -> bool:
        """
        保存帖子记录。返回 True 表示新增写入。
        """
        if not post.post_id and not post.content:
            return False

        if post.dedup_key in self._seen_post_keys:
            return False

        async with self._lock:
            if post.dedup_key in self._seen_post_keys:
                return False
            post_limit = self._config.max_posts_for_source(post.source_mode)
            if (
                post_limit > 0
                and self.get_source_post_count(post.source_mode, post.source_target) >= post_limit
            ):
                return False

            payload = self._build_payload(
                data_kind="posts",
                source_mode=post.source_mode,
                csv_rows=[post.to_csv_row()],
                csv_columns=POST_COLUMNS,
                json_rows=[post.to_dict()],
            )
            if not self._dispatch_payloads([payload]):
                return False

            self._seen_post_keys.add(post.dedup_key)
            self._remember_recent(post.post_url, post.uid)
            self._source_post_counts[(post.source_mode, post.source_target)] += 1
            self._total_posts_saved += 1
            return True

    async def save_comments(self, records: List[CommentRecord]) -> int:
        if not records:
            return 0

        async with self._lock:
            new_records = self._filter_new_comments(records)
            if not new_records:
                return 0

            limited_records = self._apply_comment_limits(new_records)
            if not limited_records:
                return 0

            mode_rows: Dict[str, List[Dict[str, Any]]] = {}
            for item in limited_records:
                mode = self._normalize_output_mode(item.source_mode)
                mode_rows.setdefault(mode, []).append(item.to_csv_row())

            payloads = [
                self._build_payload(
                    data_kind="comments",
                    source_mode=mode,
                    csv_rows=rows,
                    csv_columns=COMMENT_COLUMNS,
                )
                for mode, rows in mode_rows.items()
            ]
            if not self._dispatch_payloads(payloads):
                return 0

            self._total_comments_saved += len(limited_records)
            for item in limited_records:
                self._seen_comment_keys.add(item.dedup_key)
                if item.post_id:
                    self._post_comment_counts[item.post_id] += 1
                self._source_counts[(item.source_mode, item.source_target)] += 1

        return len(limited_records)

    async def save_media(self, records: List[MediaRecord]) -> int:
        if not records:
            return 0

        async with self._lock:
            new_records = self._filter_new_media(records)
            if not new_records:
                return 0

            mode_rows: Dict[str, List[Dict[str, Any]]] = {}
            for item in new_records:
                mode = self._normalize_output_mode(item.source_mode)
                mode_rows.setdefault(mode, []).append(item.to_csv_row())

            payloads = [
                self._build_payload(
                    data_kind="media",
                    source_mode=mode,
                    csv_rows=rows,
                    csv_columns=MEDIA_COLUMNS,
                )
                for mode, rows in mode_rows.items()
            ]
            if not self._dispatch_payloads(payloads):
                return 0

            for item in new_records:
                self._seen_media_keys.add(item.dedup_key)
            self._total_media_saved += len(new_records)

        return len(new_records)

    async def save_profile(self, profile: UserProfile) -> bool:
        if not profile.uid:
            return False
        if profile.uid in self._seen_profile_keys:
            return False

        async with self._lock:
            if profile.uid in self._seen_profile_keys:
                return False

            payload = self._build_payload(
                data_kind="profiles",
                source_mode="user",
                csv_rows=[profile.to_csv_row()],
                csv_columns=PROFILE_COLUMNS,
                json_rows=[profile.to_dict()],
            )
            if not self._dispatch_payloads([payload]):
                return False

            self._seen_profile_keys.add(profile.uid)
            self._total_profiles_saved += 1
            return True

    async def save(self, records: List[CommentRecord]) -> int:
        """向后兼容旧接口"""
        return await self.save_comments(records)

    def _filter_new_comments(self, records: List[CommentRecord]) -> List[CommentRecord]:
        new_records: List[CommentRecord] = []
        batch_keys: Set[Tuple[str, str, str, str]] = set()
        for item in records:
            if item.dedup_key in self._seen_comment_keys:
                continue
            if item.dedup_key in batch_keys:
                continue
            batch_keys.add(item.dedup_key)
            new_records.append(item)
        return new_records

    def _apply_comment_limits(self, records: List[CommentRecord]) -> List[CommentRecord]:
        max_comments_per_post = self._config.max_comments_per_post_enabled
        temp_source_counts: Counter = Counter()
        temp_post_counts: Counter = Counter()
        limited_records: List[CommentRecord] = []

        for item in records:
            source_key = (item.source_mode, item.source_target)

            source_limit = self._config.max_comments_for_source(item.source_mode)
            current_source_count = self._source_counts[source_key] + temp_source_counts[source_key]
            if source_limit > 0 and current_source_count >= source_limit:
                continue

            if item.post_id and max_comments_per_post > 0:
                current_post_count = (
                    self._post_comment_counts[item.post_id]
                    + temp_post_counts[item.post_id]
                )
                if current_post_count >= max_comments_per_post:
                    continue

            limited_records.append(item)
            temp_source_counts[source_key] += 1
            if item.post_id:
                temp_post_counts[item.post_id] += 1

        return limited_records

    def _filter_new_media(self, records: List[MediaRecord]) -> List[MediaRecord]:
        new_records: List[MediaRecord] = []
        batch_keys: Set[str] = set()
        for item in records:
            if not item.media_url:
                continue
            if item.dedup_key in self._seen_media_keys:
                continue
            if item.dedup_key in batch_keys:
                continue
            batch_keys.add(item.dedup_key)
            new_records.append(item)
        return new_records

    def _remember_recent(self, post_url: str, uid: str) -> None:
        """
        记录运行时发现的帖子/用户（用于调试和运行摘要）。
        """
        if post_url and post_url not in self._recent_post_urls:
            self._recent_post_urls.append(post_url)
        if uid and uid not in self._recent_user_ids:
            self._recent_user_ids.append(uid)

    @staticmethod
    def _append_jsonl(file_path: str, data: Dict) -> None:
        parent_dir = os.path.dirname(file_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as file_obj:
            file_obj.write(json.dumps(data, ensure_ascii=False) + "\n")

    @staticmethod
    def _append_jsonl_batch(file_path: str, rows: List[Dict]) -> None:
        if not rows:
            return
        parent_dir = os.path.dirname(file_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as file_obj:
            for row in rows:
                file_obj.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _append_csv(
        self,
        file_path: str,
        columns: List[str],
        rows: Iterable[Dict[str, str]],
    ) -> None:
        parent_dir = os.path.dirname(file_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        should_check_header = (
            file_path not in self._csv_header_checked
            or not os.path.exists(file_path)
        )
        write_header = False
        if should_check_header:
            write_header = CsvStorage._prepare_csv_header(file_path, columns)
            self._csv_header_checked.add(file_path)

        with open(file_path, "a", encoding="utf-8-sig", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=columns)
            if write_header:
                writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _prepare_csv_header(file_path: str, columns: List[str]) -> bool:
        """
        检查 CSV 表头是否与当前列定义一致。
        不一致时备份旧文件并返回 True（重建新表头）。
        """
        if not os.path.exists(file_path):
            return True

        try:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as file_obj:
                reader = csv.reader(file_obj)
                header = next(reader, [])
        except Exception:
            header = []

        if header == columns:
            return False

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{file_path}.legacy_{timestamp}"
        os.replace(file_path, backup_file)
        logger.warning("检测到 CSV 列结构变更，已备份旧文件: %s", backup_file)
        return True
