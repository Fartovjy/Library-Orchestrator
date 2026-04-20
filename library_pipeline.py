#!/usr/bin/env python3
"""
Надежный конвейер сортировки большой библиотеки:
1) Поиск файлов
2) Распаковка архивов (включая вложенные архивы)
3) Определение "книга / не книга"
4) Ранний отсев дубликатов по XXH64
5) Чтение тегов и метаданных
6) Доопределение метаданных через LM Studio (JSON)
7) Нормализация имени и пути назначения
8) Упаковка книг в ZIP с максимальным сжатием (7-Zip)

Управление:
- Esc / Ctrl+S: "остановить и очистить" (сброс очередей + очистка temp)
"""

from __future__ import annotations

import argparse
import ast
import ctypes
import json
import logging
import os
import queue
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    import xxhash
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Требуется пакет xxhash для XXH64. Установите: pip install xxhash"
    ) from exc

try:
    import setting  # Локальный файл путей: setting.py
except Exception:
    setting = None


_FALLBACK_SOURCE_DIR = (
    r"E:\Энциклопедии. Словари. Справочники\[Мартенс]_Техническая энциклопедия"
)


def _default_source_dirs() -> list[str]:
    if setting is not None:
        raw_list = getattr(setting, "SOURCE_DIRS", None)
        if isinstance(raw_list, (list, tuple)):
            values = [str(x).strip() for x in raw_list if str(x).strip()]
            if values:
                return values
        legacy_single = getattr(setting, "SOURCE_DIR", None)
        if legacy_single and str(legacy_single).strip():
            return [str(legacy_single).strip()]
    return [_FALLBACK_SOURCE_DIR]


DEFAULT_SOURCE_DIRS = _default_source_dirs()
DEFAULT_TARGET_DIR = getattr(setting, "TARGET_DIR", r"E:\Sorted_Library")
DEFAULT_DUPES_DIR = getattr(setting, "DUPES_DIR", r"E:\Sorted_Library\Duplicates")
DEFAULT_NOBOOK_DIR = getattr(setting, "NOBOOK_DIR", r"E:\Sorted_Library\NoBook")
_temp_from_setting = getattr(setting, "TEMP_BASE", None)
if _temp_from_setting:
    DEFAULT_TEMP_BASE = _temp_from_setting
else:
    DEFAULT_TEMP_BASE = str(Path(DEFAULT_TARGET_DIR) / "_TempPipeline")

DEFAULT_QUEUE_SIZE = int(getattr(setting, "QUEUE_SIZE", 2000))
DEFAULT_UNPACK_WORKERS = int(getattr(setting, "UNPACK_WORKERS", 2))
DEFAULT_DETECT_WORKERS = int(getattr(setting, "DETECT_WORKERS", 2))
DEFAULT_DEDUPE_WORKERS = int(getattr(setting, "DEDUPE_WORKERS", 1))
DEFAULT_TAG_WORKERS = int(getattr(setting, "TAG_WORKERS", 2))
DEFAULT_LM_WORKERS = int(getattr(setting, "LM_WORKERS", 1))
DEFAULT_RENAME_WORKERS = int(getattr(setting, "RENAME_WORKERS", 1))
DEFAULT_PACK_WORKERS = int(getattr(setting, "PACK_WORKERS", 3))
DEFAULT_MAX_PARALLEL_ARCHIVES = int(getattr(setting, "MAX_PARALLEL_ARCHIVES", 1))
DEFAULT_LM_TIMEOUT_SEC = int(getattr(setting, "LM_TIMEOUT_SEC", 40))
DEFAULT_LM_INPUT_CHARS = int(getattr(setting, "LM_INPUT_CHARS", 700))
DEFAULT_LM_MAX_OUTPUT_TOKENS = int(getattr(setting, "LM_MAX_OUTPUT_TOKENS", 120))
DEFAULT_LM_FORCE_FULL_METADATA = bool(getattr(setting, "LM_FORCE_FULL_METADATA", True))
DEFAULT_LM_ALWAYS_TRY_WITHOUT_SNIPPET = bool(
    getattr(setting, "LM_ALWAYS_TRY_WITHOUT_SNIPPET", True)
)
DEFAULT_LM_STRICT_JSON_MODE = bool(getattr(setting, "LM_STRICT_JSON_MODE", True))
DEFAULT_LM_MIN_SNIPPET_LETTERS = int(getattr(setting, "LM_MIN_SNIPPET_LETTERS", 24))
DEFAULT_SEED_HASHES_FROM_TARGET = bool(getattr(setting, "SEED_HASHES_FROM_TARGET", True))
DEFAULT_TARGET_HASH_SCAN_WORKERS = int(
    getattr(setting, "TARGET_HASH_SCAN_WORKERS", max(2, min(8, os.cpu_count() or 4)))
)

ARCHIVE_EXTENSIONS = {
    ".zip",
    ".7z",
    ".rar",
    ".tar",
    ".gz",
    ".bz2",
    ".xz",
    ".tgz",
    ".tbz",
    ".tbz2",
    ".txz",
    ".cbz",
    ".cbr",
    ".iso",
}

BOOK_EXTENSIONS = {
    ".epub",
    ".fb2",
    ".fb2.zip",
    ".pdf",
    ".djvu",
    ".mobi",
    ".azw",
    ".azw3",
    ".lit",
    ".lrf",
    ".prc",
    ".rtf",
    ".txt",
    ".doc",
    ".docx",
    ".odt",
    ".chm",
    ".html",
    ".htm",
    ".md",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".csv",
    ".xps",
    ".oxps",
}

STRONG_NONBOOK_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".webp",
    ".tif",
    ".tiff",
    ".svg",
    ".ico",
    ".cur",
    ".mp3",
    ".wav",
    ".flac",
    ".ogg",
    ".aac",
    ".m4a",
    ".wma",
    ".mp4",
    ".avi",
    ".mkv",
    ".mov",
    ".wmv",
    ".flv",
    ".exe",
    ".msi",
    ".dll",
    ".sys",
    ".bin",
    ".iso",
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".psd",
    ".ai",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".swf",
    ".js",
    ".css",
    ".ini",
    ".cfg",
    ".conf",
    ".inf",
    ".log",
    ".tmp",
    ".bak",
    ".old",
    ".dat",
    ".hex",
    ".cod",
    ".pjt",
    ".maa",
    ".mos",
    ".rom",
    ".nes",
    ".sfc",
    ".smc",
    ".gb",
    ".gbc",
    ".gba",
    ".sav",
    ".com",
    ".bat",
    ".cmd",
    ".vbs",
    ".ps1",
    ".obj",
    ".lib",
    ".o",
    ".class",
    ".jar",
    ".apk",
    ".cab",
}

TINY_UNKNOWN_BINARY_BYTES = 64 * 1024

TEXT_EXTENSIONS = {
    ".txt",
    ".md",
    ".csv",
    ".tsv",
    ".json",
    ".xml",
    ".html",
    ".htm",
    ".fb2",
    ".rtf",
}

AGENT_KEYS = ("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8")
AGENT_LABELS = {
    "A1": "Поиск",
    "A2": "Распаковка",
    "A3": "Книга?",
    "A4": "XXH64",
    "A5": "Теги",
    "A6": "LM Studio",
    "A7": "Переименование",
    "A8": "Упаковка",
}
AGENT_DB_SYNC_KEY = "A0"

INVALID_FS_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
SPACES_RE = re.compile(r"\s+")
JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


@dataclass
class Config:
    source_dirs: list[Path] = field(
        default_factory=lambda: [Path(p) for p in DEFAULT_SOURCE_DIRS]
    )
    target_dir: Path = Path(DEFAULT_TARGET_DIR)
    dupes_dir: Path = Path(DEFAULT_DUPES_DIR)
    nobook_dir: Path = Path(DEFAULT_NOBOOK_DIR)
    temp_base: Path = Path(DEFAULT_TEMP_BASE)
    lm_url: str = "http://127.0.0.1:1234/v1/chat/completions"
    lm_model: str = "google/gemma-4-e4b"
    queue_size: int = DEFAULT_QUEUE_SIZE
    unpack_workers: int = DEFAULT_UNPACK_WORKERS
    detect_workers: int = DEFAULT_DETECT_WORKERS
    tag_workers: int = DEFAULT_TAG_WORKERS
    lm_workers: int = DEFAULT_LM_WORKERS
    rename_workers: int = DEFAULT_RENAME_WORKERS
    dedupe_workers: int = DEFAULT_DEDUPE_WORKERS
    pack_workers: int = DEFAULT_PACK_WORKERS
    max_parallel_archives: int = DEFAULT_MAX_PARALLEL_ARCHIVES
    delete_source_after_pack: bool = True
    keep_temp_nobooks: bool = False
    lm_timeout_sec: int = DEFAULT_LM_TIMEOUT_SEC
    lm_input_chars: int = DEFAULT_LM_INPUT_CHARS
    lm_max_output_tokens: int = DEFAULT_LM_MAX_OUTPUT_TOKENS
    lm_force_full_metadata: bool = DEFAULT_LM_FORCE_FULL_METADATA
    lm_always_try_without_snippet: bool = DEFAULT_LM_ALWAYS_TRY_WITHOUT_SNIPPET
    lm_strict_json_mode: bool = DEFAULT_LM_STRICT_JSON_MODE
    lm_min_snippet_letters: int = DEFAULT_LM_MIN_SNIPPET_LETTERS
    seed_hashes_from_target: bool = DEFAULT_SEED_HASHES_FROM_TARGET
    target_hash_scan_workers: int = DEFAULT_TARGET_HASH_SCAN_WORKERS
    ephemeral_mode: bool = True


@dataclass
class Metadata:
    title: str = ""
    author: str = ""
    genre: str = ""
    subgenres: list[str] = field(default_factory=list)
    confidence: float = 0.0
    source: str = "none"


@dataclass
class FileTask:
    task_id: str
    path: Path
    origin: str  # source | temp
    source_root: Optional[Path] = None
    archive_chain: list[str] = field(default_factory=list)
    archive_source: Optional[Path] = None
    cleanup_root: Optional[Path] = None
    metadata: Metadata = field(default_factory=Metadata)
    dest_zip: Optional[Path] = None
    xxh64: Optional[str] = None
    is_book_candidate: bool = False
    book_seen_counted: bool = False
    book_done_counted: bool = False


def format_duration_hms(seconds: Optional[int]) -> str:
    if seconds is None:
        return "--:--:--"
    seconds = max(0, int(seconds))
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


class AtomicCounter:
    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def inc(self, delta: int = 1) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def dec(self, delta: int = 1) -> int:
        with self._lock:
            self._value -= delta
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value


class Metrics:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.start_ts = time.time()
        self.mode = "RUNNING"
        self.books_seen = 0
        self.books_done = 0
        self.total_tasks_seen = 0
        self.total_tasks_done = 0
        self.stage_processed: dict[str, int] = defaultdict(int)
        self.stage_errors: dict[str, int] = defaultdict(int)
        self.results: dict[str, int] = defaultdict(int)
        self.book_results: dict[str, int] = defaultdict(int)
        self.events: deque[str] = deque(maxlen=10)

    def set_mode(self, mode: str) -> None:
        with self.lock:
            self.mode = mode

    def add_event(self, text: str) -> None:
        with self.lock:
            ts = datetime.now().strftime("%H:%M:%S")
            self.events.appendleft(f"[{ts}] {text}")

    def mark_task_seen(self) -> None:
        with self.lock:
            self.total_tasks_seen += 1

    def mark_task_done(self, result: str) -> None:
        with self.lock:
            self.total_tasks_done += 1
            self.results[result] += 1

    def mark_book_seen(self) -> None:
        with self.lock:
            self.books_seen += 1

    def mark_book_done(self, result: str) -> None:
        with self.lock:
            self.books_done += 1
            self.book_results[result] += 1

    def mark_stage(self, stage: str, error: bool = False) -> None:
        with self.lock:
            self.stage_processed[stage] += 1
            if error:
                self.stage_errors[stage] += 1

    def snapshot(self, queue_sizes: dict[str, int]) -> dict[str, Any]:
        with self.lock:
            seen = self.books_seen
            done = self.books_done
            pct = 0.0 if seen == 0 else (done / max(1, seen)) * 100.0
            elapsed = int(time.time() - self.start_ts)
            if seen > 0 and done >= seen:
                eta_seconds: Optional[int] = 0
            elif done > 0:
                seconds_per_book = elapsed / max(1, done)
                eta_seconds = int(seconds_per_book * max(0, seen - done))
            else:
                eta_seconds = None
            return {
                "mode": self.mode,
                "seen": seen,
                "done": done,
                "tasks_seen": self.total_tasks_seen,
                "tasks_done": self.total_tasks_done,
                "pct": pct,
                "elapsed_sec": elapsed,
                "eta_sec": eta_seconds,
                "elapsed": format_duration_hms(elapsed),
                "eta": format_duration_hms(eta_seconds),
                "stage_processed": dict(self.stage_processed),
                "stage_errors": dict(self.stage_errors),
                "results": dict(self.results),
                "book_results": dict(self.book_results),
                "events": list(self.events),
                "queue_sizes": queue_sizes,
            }


class ManifestDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hashes (
                    xxh64 TEXT PRIMARY KEY,
                    canonical_zip TEXT NOT NULL,
                    title TEXT,
                    author TEXT,
                    genre TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    task_id TEXT PRIMARY KEY,
                    source_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lm_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                UPDATE files
                SET status = 'duplicate_temp'
                WHERE status = 'duplicate'
                  AND source_path LIKE '%_TempPipeline%'
                """
            )

    def mark_file(self, task: FileTask, status: str, message: str = "") -> None:
        now = datetime.utcnow().isoformat()
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO files(task_id, source_path, status, message, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    source_path=excluded.source_path,
                    status=excluded.status,
                    message=excluded.message,
                    updated_at=excluded.updated_at
                """,
                (task.task_id, str(task.path), status, message, now),
            )

    def claim_hash(
        self,
        xxh64_hex: str,
        canonical_zip: Path,
        title: str,
        author: str,
        genre: str,
    ) -> tuple[bool, Optional[str]]:
        now = datetime.utcnow().isoformat()
        with self._lock, self.conn:
            try:
                self.conn.execute(
                    """
                    INSERT INTO hashes(xxh64, canonical_zip, title, author, genre, created_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (xxh64_hex, str(canonical_zip), title, author, genre, now),
                )
                return True, None
            except sqlite3.IntegrityError:
                row = self.conn.execute(
                    "SELECT canonical_zip FROM hashes WHERE xxh64 = ?",
                    (xxh64_hex,),
                ).fetchone()
                return False, (row[0] if row else None)

    def get_lm_cache(self, cache_key: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT payload_json FROM lm_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if not row:
                return None
            try:
                return json.loads(row[0])
            except Exception:
                return None

    def set_lm_cache(self, cache_key: str, payload: dict[str, Any]) -> None:
        now = datetime.utcnow().isoformat()
        payload_json = json.dumps(payload, ensure_ascii=False)
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO lm_cache(cache_key, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (cache_key, payload_json, now),
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def remove_hash(self, xxh64_hex: str) -> None:
        with self._lock, self.conn:
            self.conn.execute("DELETE FROM hashes WHERE xxh64 = ?", (xxh64_hex,))

    def update_hash_record(
        self,
        xxh64_hex: str,
        canonical_zip: Path,
        title: str,
        author: str,
        genre: str,
    ) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                UPDATE hashes
                SET canonical_zip = ?, title = ?, author = ?, genre = ?
                WHERE xxh64 = ?
                """,
                (str(canonical_zip), title, author, genre, xxh64_hex),
            )

    def get_hash_rows(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT xxh64, canonical_zip FROM hashes"
            ).fetchall()
        return [(str(row[0]), str(row[1])) for row in rows]

    def upsert_hash(
        self,
        xxh64_hex: str,
        canonical_zip: Path,
        title: str,
        author: str,
        genre: str,
    ) -> None:
        now = datetime.utcnow().isoformat()
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO hashes(xxh64, canonical_zip, title, author, genre, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(xxh64) DO UPDATE SET
                    canonical_zip=excluded.canonical_zip,
                    title=excluded.title,
                    author=excluded.author,
                    genre=excluded.genre
                """,
                (xxh64_hex, str(canonical_zip), title, author, genre, now),
            )


class TempTracker:
    def __init__(self, logger: logging.Logger, metrics: Metrics) -> None:
        self._lock = threading.Lock()
        self._refs: dict[Path, int] = defaultdict(int)
        self.logger = logger
        self.metrics = metrics

    def register(self, root: Optional[Path]) -> None:
        if not root:
            return
        with self._lock:
            self._refs[root] += 1

    def release(self, root: Optional[Path]) -> None:
        if not root:
            return
        should_delete = False
        with self._lock:
            if root not in self._refs:
                return
            self._refs[root] -= 1
            if self._refs[root] <= 0:
                del self._refs[root]
                should_delete = True
        if should_delete:
            try:
                shutil.rmtree(root, ignore_errors=True)
                self.metrics.add_event(f"Temp cleaned: {root}")
            except Exception as exc:
                self.logger.exception("Ошибка очистки temp %s: %s", root, exc)


class LMStudioClient:
    def __init__(
        self,
        config: Config,
        logger: logging.Logger,
        metrics: Metrics,
        db: ManifestDB,
    ) -> None:
        self.config = config
        self.logger = logger
        self.metrics = metrics
        self.db = db
        self.available = requests is not None

    def _response_error_brief(self, resp: Any, max_len: int = 220) -> str:
        try:
            raw = resp.text or ""
        except Exception:
            raw = ""
        brief = clean_text(raw)
        return truncate(brief, max_len)

    def _post_chat(
        self,
        payload: dict[str, Any],
        timeout: tuple[int, int],
    ) -> tuple[Any, str]:
        if requests is None:
            raise RuntimeError("requests is unavailable")

        messages = payload.get("messages", [])
        attempts: list[tuple[str, dict[str, Any]]] = []
        if self.config.lm_strict_json_mode:
            strict_payload = {**payload, "response_format": {"type": "json_object"}}
            attempts.append(("strict_json", strict_payload))
        attempts.append(("default", payload))
        # Compatibility fallback for LM Studio builds that reject some OpenAI fields.
        attempts.append(
            (
                "minimal",
                {
                    "model": payload.get("model", self.config.lm_model),
                    "messages": messages,
                },
            )
        )
        if "max_tokens" in payload:
            attempts.append(
                (
                    "minimal_max_completion",
                    {
                        "model": payload.get("model", self.config.lm_model),
                        "messages": messages,
                        "max_completion_tokens": payload.get("max_tokens"),
                    },
                )
            )

        tried: set[str] = set()
        last_resp: Any = None
        last_mode = "none"
        for mode, body in attempts:
            try_key = json.dumps(body, ensure_ascii=False, sort_keys=True, default=str)
            if try_key in tried:
                continue
            tried.add(try_key)
            resp = requests.post(self.config.lm_url, json=body, timeout=timeout)
            last_resp = resp
            last_mode = mode
            if resp.status_code < 400:
                return resp, mode
            if resp.status_code not in {400, 404, 409, 415, 422}:
                return resp, mode
        return last_resp, last_mode

    def enrich(self, task: FileTask, snippet: str) -> Optional[dict[str, Any]]:
        if not self.available:
            self.logger.warning("LM full unavailable: requests import failed")
            return None
        snippet = snippet[: self.config.lm_input_chars]
        payload_seed = {
            "schema": "lm_results_genre_analysis_v1",
            "path": task.path.name,
            "metadata": task.metadata.__dict__,
            "snippet": snippet,
        }
        cache_key = xxhash.xxh64(
            json.dumps(payload_seed, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        cached = self.db.get_lm_cache(cache_key)
        if cached:
            self.logger.info("LM full cache_hit path=%s key=%s", task.path, cache_key[:10])
            return cached

        system_prompt = """РОЛЬ И ЗАДАЧА: Ты — машина для категоризации литературных произведений. Твоя задача — принять данные о книге и вернуть результат в строгом JSON-формате, где каждый объект описывает книгу и содержит максимально детализированный жанровый анализ. Не добавляй никаких вводных слов или заключений, только чистый JSON.

ПРИМЕР ВЫВОДА:
{
  "results": [
    {
      "title": "Название А",
      "author": "Автор X",
      "genre_analysis": {
        "primary_genre": "Научная фантастика",
        "subgenres": ["Транспортный триллер", "Дистопия"],
        "confidence_score": 5.0
      }
    }
  ]
}"""
        user_prompt = (
            "ДАННЫЕ О КНИГЕ:\n"
            f"Имя файла: {task.path.name}\n"
            f"Текущие теги и эвристики: {json.dumps(task.metadata.__dict__, ensure_ascii=False)}\n"
            "Фрагмент текста/контекст (ограниченный, не вся книга):\n"
            f"{snippet}"
        )
        archive_chain = " -> ".join(task.archive_chain) if task.archive_chain else "(none)"
        user_prompt += (
            f"\nПолный путь: {task.path}\n"
            f"Цепочка архивов: {archive_chain}\n"
            "Верни JSON строго по шаблону: один объект в массиве results для этой книги."
        )
        request_payload = {
            "model": self.config.lm_model,
            "temperature": 0.1,
            "max_tokens": self.config.lm_max_output_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            resp, req_mode = self._post_chat(
                request_payload, timeout=(6, self.config.lm_timeout_sec)
            )
            if resp.status_code >= 400:
                self.logger.warning(
                    "LM full HTTP %s mode=%s path=%s body=%s",
                    resp.status_code,
                    req_mode,
                    task.path,
                    self._response_error_brief(resp),
                )
                self.metrics.add_event(
                    f"LM Studio HTTP {resp.status_code}: fallback для {task.path.name}"
                )
                return None
            data = resp.json()
            content = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            parsed = parse_model_payload(content)
            retry_content = ""
            if not parsed:
                retry_payload = {
                    **request_payload,
                    "temperature": 0.0,
                    "messages": request_payload["messages"]
                    + [
                        {
                            "role": "user",
                            "content": (
                                "Верни только валидный JSON-объект без markdown и текста вокруг. "
                                "Строго структура: {\"results\":[{\"title\":\"...\",\"author\":\"...\","
                                "\"genre_analysis\":{\"primary_genre\":\"...\",\"subgenres\":[\"...\"],"
                                "\"confidence_score\":5.0}}]}"
                            ),
                        }
                    ],
                }
                retry, retry_mode = self._post_chat(
                    retry_payload, timeout=(6, self.config.lm_timeout_sec)
                )
                if retry.status_code < 400:
                    retry_data = retry.json()
                    retry_content = (
                        retry_data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                        .strip()
                    )
                    parsed = parse_model_payload(retry_content)
                else:
                    self.logger.warning(
                        "LM full retry HTTP %s mode=%s path=%s body=%s",
                        retry.status_code,
                        retry_mode,
                        task.path,
                        self._response_error_brief(retry),
                    )
            if not parsed:
                self.logger.warning(
                    "LM full non-json path=%s raw=%s retry=%s",
                    task.path,
                    truncate(clean_text(content), 200),
                    truncate(clean_text(retry_content), 200),
                )
                self.metrics.add_event(
                    f"LM Studio вернул не-JSON для {task.path.name}: fallback"
                )
                return None
            self.db.set_lm_cache(cache_key, parsed)
            self.logger.info(
                "LM full parsed path=%s payload=%s",
                task.path,
                json.dumps(parsed, ensure_ascii=False),
            )
            return parsed
        except Exception as exc:
            self.logger.warning("LM Studio недоступен: %s", exc)
            self.metrics.add_event(f"LM Studio недоступен: {exc}")
            return None

    def enrich_genre_only(self, task: FileTask) -> Optional[dict[str, Any]]:
        if not self.available:
            self.logger.warning("LM genre-only unavailable: requests import failed")
            return None
        payload_seed = {
            "mode": "genre_only",
            "path": task.path.name,
            "title": task.metadata.title,
            "author": task.metadata.author,
            "genre": task.metadata.genre,
        }
        cache_key = xxhash.xxh64(
            json.dumps(payload_seed, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        cached = self.db.get_lm_cache(cache_key)
        if cached:
            self.logger.info("LM genre-only cache_hit path=%s key=%s", task.path, cache_key[:10])
            return cached

        system_prompt = (
            "Ты библиотекарь-каталогизатор. Верни ТОЛЬКО JSON без пояснений. "
            "Формат: {\"genre\":\"...\",\"confidence\":0.0}. "
            "Если жанр определить нельзя, верни genre=\"Unknown\"."
        )
        user_prompt = (
            f"Имя файла: {task.path.name}\n"
            f"Название: {task.metadata.title}\n"
            f"Автор: {task.metadata.author}\n"
            f"Текущий жанр: {task.metadata.genre}"
        )
        archive_chain = " -> ".join(task.archive_chain) if task.archive_chain else "(none)"
        user_prompt += f"\nFull path: {task.path}\nArchive chain: {archive_chain}"
        request_payload = {
            "model": self.config.lm_model,
            "temperature": 0.0,
            "max_tokens": min(60, self.config.lm_max_output_tokens),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            resp, req_mode = self._post_chat(
                request_payload, timeout=(6, self.config.lm_timeout_sec)
            )
            if resp.status_code >= 400:
                self.logger.warning(
                    "LM genre-only HTTP %s mode=%s path=%s body=%s",
                    resp.status_code,
                    req_mode,
                    task.path,
                    self._response_error_brief(resp),
                )
                self.metrics.add_event(
                    f"LM genre-only HTTP {resp.status_code}: fallback для {task.path.name}"
                )
                return None
            data = resp.json()
            content = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            parsed = parse_model_payload(content)
            retry_content = ""
            if not parsed:
                retry_payload = {
                    **request_payload,
                    "temperature": 0.0,
                    "messages": request_payload["messages"]
                    + [
                        {
                            "role": "user",
                            "content": (
                                "Верни только валидный JSON-объект без markdown и текста вокруг. "
                                "Строго ключи: genre, confidence."
                            ),
                        }
                    ],
                }
                retry, retry_mode = self._post_chat(
                    retry_payload, timeout=(6, self.config.lm_timeout_sec)
                )
                if retry.status_code < 400:
                    retry_data = retry.json()
                    retry_content = (
                        retry_data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                        .strip()
                    )
                    parsed = parse_model_payload(retry_content)
                else:
                    self.logger.warning(
                        "LM genre-only retry HTTP %s mode=%s path=%s body=%s",
                        retry.status_code,
                        retry_mode,
                        task.path,
                        self._response_error_brief(retry),
                    )
            if not parsed:
                self.logger.warning(
                    "LM genre-only non-json path=%s raw=%s retry=%s",
                    task.path,
                    truncate(clean_text(content), 180),
                    truncate(clean_text(retry_content), 180),
                )
                return None
            self.db.set_lm_cache(cache_key, parsed)
            self.logger.info(
                "LM genre-only parsed path=%s payload=%s",
                task.path,
                json.dumps(parsed, ensure_ascii=False),
            )
            return parsed
        except Exception as exc:
            self.logger.warning("LM Studio genre-only недоступен: %s", exc)
            return None


class TerminalUI:
    def __init__(self, metrics: Metrics, pipeline: "LibrarySorter") -> None:
        self.metrics = metrics
        self.pipeline = pipeline
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="UI", daemon=True)

    def start(self) -> None:
        enable_ansi_on_windows()
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=3)
        # Показываем курсор обратно.
        sys.stdout.write("\x1b[?25h\n")
        sys.stdout.flush()

    def _run(self) -> None:
        sys.stdout.write("\x1b[?25l")
        while not self.stop_event.is_set():
            queue_sizes = self.pipeline.queue_sizes()
            snap = self.metrics.snapshot(queue_sizes)
            frame = self._render_frame(snap)
            sys.stdout.write("\x1b[H\x1b[2J")
            sys.stdout.write(frame)
            sys.stdout.flush()
            time.sleep(0.5)

    def _render_frame(self, snap: dict[str, Any]) -> str:
        width = 118
        bar_width = 48
        ratio = max(0.0, min(1.0, snap["pct"] / 100.0))
        filled = int(bar_width * ratio)
        bar = ("#" * filled).ljust(bar_width, "-")
        book_results = snap.get("book_results", {})

        lines = []
        lines.append("LIBRARY SORTER PIPELINE".ljust(width))
        lines.append(
            (
                f"State: {snap['mode']:<18} "
                f"Elapsed: {snap['elapsed']}   "
                "Keys: Esc/Ctrl+S=Stop+Cleanup"
            ).ljust(width)
        )
        lines.append(
            (
                f"Progress: [{bar}] {snap['pct']:6.2f}%   "
                f"Books found: {snap['seen']}   Books done: {snap['done']}   "
                f"Packed: {book_results.get('packed', 0)}   "
                f"Dupes: {book_results.get('duplicate', 0)}   "
                f"NoBook: {snap['results'].get('nobook', 0)}   "
                f"Book failed: {book_results.get('failed', 0)}"
            ).ljust(width)
        )
        lines.append("-" * width)
        lines.append(
            f"{'Agent':<6} {'Role':<16} {'Processed':>10} {'Errors':>8} {'Queue':>8}"
        )
        lines.append("-" * width)
        for key in AGENT_KEYS:
            role = AGENT_LABELS[key]
            processed = snap["stage_processed"].get(key, 0)
            errors = snap["stage_errors"].get(key, 0)
            qsize = snap["queue_sizes"].get(key, 0)
            lines.append(
                f"{key:<6} {role:<16} {processed:>10} {errors:>8} {qsize:>8}"
            )
        lines.append("-" * width)
        lines.append("Recent events:".ljust(width))
        events = snap["events"][:10]
        if not events:
            events = ["(пока без событий)"]
        for ev in events:
            lines.append(truncate(ev, width))
        while len(lines) < 26:
            lines.append("")
        return "\n".join(lines)


class KeyboardWatcher:
    def __init__(self, pipeline: "LibrarySorter", metrics: Metrics) -> None:
        self.pipeline = pipeline
        self.metrics = metrics
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="Keyboard", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)

    def _run(self) -> None:
        if os.name != "nt":
            return
        import msvcrt  # type: ignore

        while not self.stop_event.is_set():
            try:
                if msvcrt.kbhit():
                    ch = msvcrt.getch()
                    # ESC / Ctrl+S -> stop pipeline + cleanup temp
                    if ch in (b"\x1b", b"\x13"):
                        self.metrics.add_event("Esc/Ctrl+S: стоп конвейера и очистка temp")
                        self.pipeline.request_stop_and_cleanup()
                time.sleep(0.05)
            except Exception:
                time.sleep(0.2)


class LibrarySorter:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session_id = (
            datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
        )
        self.runtime_dir = self.config.temp_base / "runtime" / self.session_id
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.app_dir = Path(__file__).resolve().parent
        self.log_file = self.app_dir / "logs" / f"library_sorter_{self.session_id}.log"
        self.stop_event = threading.Event()
        self.cleanup_event = threading.Event()
        self.scan_done = threading.Event()
        self.unpack_done = threading.Event()
        self.detect_done = threading.Event()
        self.tag_done = threading.Event()
        self.lm_done = threading.Event()
        self.rename_done = threading.Event()
        self.dedupe_done = threading.Event()

        self.unpack_active = AtomicCounter(0)
        self.extract_semaphore = threading.Semaphore(config.max_parallel_archives)

        self.metrics = Metrics()
        self.logger = self._init_logger()
        self.db_path = self.config.target_dir / build_source_db_name(self.config.source_dirs)
        self.db = ManifestDB(self.db_path)
        self.temp_tracker = TempTracker(self.logger, self.metrics)
        self.lm_client = LMStudioClient(config, self.logger, self.metrics, self.db)
        self.seven_zip = self._detect_7zip()

        self._archive_state_lock = threading.Lock()
        self._archive_pending: dict[Path, int] = defaultdict(int)
        self._archive_failed: set[Path] = set()

        self.q12 = queue.Queue(maxsize=config.queue_size)
        self.q23 = queue.Queue(maxsize=config.queue_size)
        self.q34 = queue.Queue(maxsize=config.queue_size)
        self.q45 = queue.Queue(maxsize=config.queue_size)
        self.q56 = queue.Queue(maxsize=config.queue_size)
        self.q67 = queue.Queue(maxsize=config.queue_size)
        self.q78 = queue.Queue(maxsize=config.queue_size)

        self.threads: list[threading.Thread] = []
        self.ui = TerminalUI(self.metrics, self)
        self.keyboard = KeyboardWatcher(self, self.metrics)

    def _init_logger(self) -> logging.Logger:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(f"library_sorter.{self.session_id}")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.propagate = False

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
        )
        fh = logging.FileHandler(self.log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(formatter)
        sh.setLevel(logging.WARNING)
        logger.addHandler(fh)
        logger.addHandler(sh)
        return logger

    def _detect_7zip(self) -> str:
        seven = shutil.which("7z") or shutil.which("7za")
        if not seven and os.name == "nt":
            candidates = [
                Path(r"C:\Program Files\7-Zip\7z.exe"),
                Path(r"C:\Program Files (x86)\7-Zip\7z.exe"),
                Path(r"C:\Program Files\7-Zip\7za.exe"),
                Path(r"C:\Program Files (x86)\7-Zip\7za.exe"),
            ]
            for cand in candidates:
                if cand.exists():
                    seven = str(cand)
                    break
        if not seven:
            raise RuntimeError(
                "Не найден 7-Zip в PATH. Установите 7-Zip или добавьте 7z.exe в PATH."
            )
        return seven

    def request_stop_and_cleanup(self) -> None:
        self.metrics.set_mode("STOP_CLEANUP")
        self.cleanup_event.set()
        self.stop_event.set()

    def should_stop(self) -> bool:
        return self.stop_event.is_set()

    def queue_sizes(self) -> dict[str, int]:
        return {
            "A1": self.q12.qsize(),
            "A2": self.q12.qsize(),
            "A3": self.q23.qsize(),
            "A4": self.q34.qsize(),
            "A5": self.q45.qsize(),
            "A6": self.q56.qsize(),
            "A7": self.q67.qsize(),
            "A8": self.q78.qsize(),
        }

    def ensure_dirs(self) -> None:
        for src in self.config.source_dirs:
            src.mkdir(parents=True, exist_ok=True)
        self.config.target_dir.mkdir(parents=True, exist_ok=True)
        self.config.dupes_dir.mkdir(parents=True, exist_ok=True)
        self.config.nobook_dir.mkdir(parents=True, exist_ok=True)
        self.config.temp_base.mkdir(parents=True, exist_ok=True)
        (self.config.temp_base / "extract").mkdir(parents=True, exist_ok=True)

    def _path_key(self, value: Path | str) -> str:
        try:
            p = Path(value).resolve(strict=False)
        except Exception:
            p = Path(str(value))
        key = str(p)
        if os.name == "nt":
            return key.lower()
        return key

    def _is_under(self, path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except Exception:
            return False

    def _metadata_from_target_zip_path(self, zip_path: Path) -> tuple[str, str, str]:
        rel = safe_relative(zip_path, self.config.target_dir)
        parts = rel.parts
        title = clean_text(zip_path.stem) or zip_path.stem or "Unknown Title"
        author = "Unknown Author"
        genre = "Unknown"
        if len(parts) >= 2:
            author = clean_text(parts[-2]) or author
        if len(parts) >= 4:
            genre = clean_text(parts[-4]) or genre
        genre = normalize_genre(genre or "Unknown")
        return title, author, genre

    def _iter_target_zips_for_db_sync(self):
        target = self.config.target_dir.resolve(strict=False)
        excluded_roots: list[Path] = []
        for candidate in (self.config.dupes_dir, self.config.nobook_dir, self.config.temp_base):
            try:
                excluded_roots.append(candidate.resolve(strict=False))
            except Exception:
                continue

        for root, dirs, files in os.walk(target, topdown=True, followlinks=False):
            if self.should_stop():
                return
            root_path = Path(root)
            keep_dirs: list[str] = []
            for d in dirs:
                child = (root_path / d).resolve(strict=False)
                skip = False
                for ex in excluded_roots:
                    if self._is_under(child, ex):
                        skip = True
                        break
                if not skip:
                    keep_dirs.append(d)
            dirs[:] = keep_dirs

            for name in files:
                lname = name.lower()
                if not lname.endswith(".zip"):
                    continue
                if lname.startswith(".__tmp_"):
                    continue
                yield root_path / name

    def _run_startup_db_sync_agent(self) -> None:
        if not self.config.seed_hashes_from_target:
            self.metrics.add_event("Agent0: TARGET_DIR sync skipped by config")
            return

        error_box: dict[str, Exception] = {}

        def runner() -> None:
            try:
                self._sync_db_with_target_dir()
            except Exception as exc:
                error_box["exc"] = exc
                self.metrics.mark_stage(AGENT_DB_SYNC_KEY, error=True)
                self.logger.exception("Agent0 sync failed: %s", exc)

        sync_thread = threading.Thread(
            target=runner,
            name="A0-DBSync",
            daemon=False,
        )
        sync_thread.start()
        sync_thread.join()
        if "exc" in error_box:
            raise RuntimeError(f"Agent0 DB/TARGET sync failed: {error_box['exc']}") from error_box["exc"]

    def _sync_db_with_target_dir(self) -> None:
        started = time.time()
        workers = max(1, int(self.config.target_hash_scan_workers))
        self.metrics.add_event(
            f"Agent0: DB/TARGET sync start (workers={workers})"
        )
        self.logger.info(
            "A0 sync start target=%s workers=%s db=%s",
            self.config.target_dir,
            workers,
            self.db_path,
        )

        task_q: queue.Queue[Optional[Path]] = queue.Queue(maxsize=max(512, workers * 32))
        producer_done = threading.Event()
        lock = threading.Lock()

        scanned_by_hash: dict[str, tuple[Path, str, str, str]] = {}
        scanned_hash_by_path: dict[str, str] = {}
        counters: dict[str, int] = defaultdict(int)

        def worker_loop(worker_idx: int) -> None:
            while not self.should_stop():
                try:
                    zip_path = task_q.get(timeout=0.2)
                except queue.Empty:
                    if producer_done.is_set():
                        break
                    continue
                if zip_path is None:
                    task_q.task_done()
                    break
                try:
                    self.metrics.mark_stage(AGENT_DB_SYNC_KEY)
                    xxh_hex, note = xxh64_zip_payload(zip_path)
                    with lock:
                        counters["scanned"] += 1
                        scanned_now = counters["scanned"]
                    if xxh_hex:
                        title, author, genre = self._metadata_from_target_zip_path(zip_path)
                        with lock:
                            key = self._path_key(zip_path)
                            scanned_hash_by_path[key] = xxh_hex
                            prev = scanned_by_hash.get(xxh_hex)
                            if prev is None or self._path_key(zip_path) < self._path_key(prev[0]):
                                scanned_by_hash[xxh_hex] = (zip_path, title, author, genre)
                            else:
                                counters["hash_collisions"] += 1
                            counters["hashed_ok"] += 1
                    else:
                        with lock:
                            counters["hashed_failed"] += 1
                            failed_now = counters["hashed_failed"]
                        if failed_now <= 10 or (failed_now % 200 == 0):
                            self.metrics.add_event(
                                f"Agent0/W{worker_idx}: skip {zip_path.name} ({note})"
                            )
                    if scanned_now and (scanned_now % 500 == 0):
                        self.metrics.add_event(
                            f"Agent0: scanned {scanned_now} zips..."
                        )
                finally:
                    task_q.task_done()

        workers_threads = [
            threading.Thread(
                target=worker_loop,
                name=f"A0-W{i+1}",
                args=(i + 1,),
                daemon=False,
            )
            for i in range(workers)
        ]
        for t in workers_threads:
            t.start()

        discovered = 0
        for zip_path in self._iter_target_zips_for_db_sync():
            if self.should_stop():
                break
            discovered += 1
            if not self._put_with_stop(task_q, zip_path):
                break

        producer_done.set()
        if self.should_stop():
            while True:
                try:
                    _ = task_q.get_nowait()
                    task_q.task_done()
                except queue.Empty:
                    break
            for t in workers_threads:
                t.join(timeout=2)
            self.metrics.add_event("Agent0: sync interrupted by stop")
            return

        for _ in workers_threads:
            self._put_with_stop(task_q, None)
        task_q.join()
        for t in workers_threads:
            t.join()

        if self.should_stop():
            self.metrics.add_event("Agent0: sync interrupted by stop")
            return

        rows = self.db.get_hash_rows()
        db_hash_to_path: dict[str, str] = {xxh: canonical for xxh, canonical in rows}

        removed_missing = 0
        removed_mismatch = 0
        for xxh_hex, canonical in rows:
            canonical_path = Path(canonical)
            try:
                exists = canonical_path.is_file()
            except Exception:
                exists = False
            if not exists:
                self.db.remove_hash(xxh_hex)
                db_hash_to_path.pop(xxh_hex, None)
                removed_missing += 1
                continue

            path_key = self._path_key(canonical_path)
            scanned_hash = scanned_hash_by_path.get(path_key)
            if scanned_hash and scanned_hash != xxh_hex:
                self.db.remove_hash(xxh_hex)
                db_hash_to_path.pop(xxh_hex, None)
                removed_mismatch += 1

        inserted = 0
        updated = 0
        unchanged = 0
        for xxh_hex, (zip_path, title, author, genre) in scanned_by_hash.items():
            old_path = db_hash_to_path.get(xxh_hex)
            if old_path is None:
                inserted += 1
            elif self._path_key(old_path) != self._path_key(zip_path):
                updated += 1
            else:
                unchanged += 1
            self.db.upsert_hash(xxh_hex, zip_path, title, author, genre)

        elapsed = int(time.time() - started)
        self.metrics.add_event(
            "Agent0: sync done; "
            f"found={discovered} ok={counters.get('hashed_ok', 0)} fail={counters.get('hashed_failed', 0)} "
            f"db+={inserted} db~={updated} db=={unchanged} "
            f"rm_missing={removed_missing} rm_mismatch={removed_mismatch} "
            f"duphash={counters.get('hash_collisions', 0)} t={elapsed}s"
        )
        self.logger.info(
            "A0 sync done found=%s ok=%s fail=%s inserted=%s updated=%s unchanged=%s "
            "removed_missing=%s removed_mismatch=%s duphash=%s elapsed=%ss",
            discovered,
            counters.get("hashed_ok", 0),
            counters.get("hashed_failed", 0),
            inserted,
            updated,
            unchanged,
            removed_missing,
            removed_mismatch,
            counters.get("hash_collisions", 0),
            elapsed,
        )

    def run(self) -> int:
        exit_code = 0
        self.ensure_dirs()
        self.metrics.set_mode("RUNNING")
        self.metrics.add_event("Старт конвейера")
        self.metrics.add_event(
            f"SOURCE_DIRS={'; '.join(str(p) for p in self.config.source_dirs)}"
        )
        self.metrics.add_event(f"TARGET_DIR={self.config.target_dir}")
        self.metrics.add_event(f"LOG_PATH={self.log_file}")
        self.metrics.add_event(f"DB_PATH={self.db_path}")
        self.logger.info(
            "Run start sources=%s target=%s db_path=%s log=%s",
            [str(p) for p in self.config.source_dirs],
            self.config.target_dir,
            self.db_path,
            self.log_file,
        )

        self.ui.start()
        self.keyboard.start()

        try:
            self._run_startup_db_sync_agent()
            if not self.should_stop():
                self._start_threads()
                for t in self.threads:
                    t.join()
            if self.cleanup_event.is_set():
                self._clear_all_queues()
            self._cleanup_temp_base()
            self._cleanup_empty_source_dirs()
            self.metrics.add_event("Конвейер завершил работу")
        except KeyboardInterrupt:
            self.metrics.add_event("KeyboardInterrupt: остановка")
            self.request_stop_and_cleanup()
            self._clear_all_queues()
            self._cleanup_temp_base()
        except Exception as exc:
            self.logger.exception("Критическая ошибка: %s", exc)
            self.metrics.add_event(f"Критическая ошибка: {exc}")
            self._clear_all_queues()
            self._cleanup_temp_base()
            exit_code = 2
        finally:
            if exit_code == 0:
                self.metrics.set_mode("END")
            else:
                self.metrics.set_mode("END_ERROR")
            self.ui.stop()
            self.keyboard.stop()
            self.db.close()
            if self.config.ephemeral_mode:
                self._cleanup_runtime_state()
        return exit_code

    def _start_threads(self) -> None:
        scanner = threading.Thread(target=self._scanner_loop, name="Agent1-Scanner")
        self.threads.append(scanner)
        scanner.start()

        self._spawn_group("A2", self.config.unpack_workers, self._unpack_loop, self.unpack_done)
        self._spawn_group("A3", self.config.detect_workers, self._detect_loop, self.detect_done)
        self._spawn_group(
            "A4", self.config.dedupe_workers, self._dedupe_loop, self.dedupe_done
        )
        self._spawn_group("A5", self.config.tag_workers, self._tags_loop, self.tag_done)
        self._spawn_group("A6", self.config.lm_workers, self._lm_loop, self.lm_done)
        self._spawn_group(
            "A7", self.config.rename_workers, self._rename_loop, self.rename_done
        )
        self._spawn_group("A8", self.config.pack_workers, self._pack_loop, threading.Event())

    def _spawn_group(
        self,
        key: str,
        workers: int,
        fn,
        done_event: threading.Event,
    ) -> None:
        counter = AtomicCounter(workers)

        def wrapped(worker_idx: int) -> None:
            try:
                fn(worker_idx)
            except Exception as exc:
                self.logger.exception("Ошибка %s-%d: %s", key, worker_idx, exc)
                self.metrics.mark_stage(key, error=True)
            finally:
                left = counter.dec()
                if left == 0:
                    done_event.set()

        for i in range(workers):
            t = threading.Thread(
                target=wrapped,
                args=(i + 1,),
                name=f"{key}-W{i + 1}",
            )
            self.threads.append(t)
            t.start()

    def _put_with_stop(self, q: queue.Queue, item: Any, timeout: float = 0.2) -> bool:
        while not self.should_stop():
            try:
                q.put(item, timeout=timeout)
                return True
            except queue.Full:
                continue
        return False

    def _md_brief(self, md: Metadata) -> str:
        title = truncate(clean_text(md.title), 48)
        author = truncate(clean_text(md.author), 36)
        genre = truncate(clean_text(md.genre), 28)
        return (
            f"src={md.source}; conf={md.confidence:.2f}; "
            f"title={title!r}; author={author!r}; genre={genre!r}"
        )

    def _lm_decision(self, md: Metadata) -> str:
        if self.config.lm_force_full_metadata:
            return "full"
        if self._needs_lm(md):
            return "full"
        if self._needs_genre_only_lm(md):
            return "genre_only"
        if md.source == "tags" and md.confidence >= 0.75:
            return "skip:tags_confident"
        return "skip:metadata_sufficient"

    def _scanner_loop(self) -> None:
        self.metrics.add_event("Agent1: поиск файлов...")
        try:
            for source_root in self.config.source_dirs:
                if self.should_stop() or self.cleanup_event.is_set():
                    break
                stack = [source_root]
                while stack and not self.should_stop():
                    if self.cleanup_event.is_set():
                        break
                    cur = stack.pop()
                    try:
                        with os.scandir(cur) as it:
                            for entry in it:
                                if self.should_stop():
                                    break
                                try:
                                    if entry.is_dir(follow_symlinks=False):
                                        stack.append(Path(entry.path))
                                        continue
                                    if not entry.is_file(follow_symlinks=False):
                                        continue
                                    task = FileTask(
                                        task_id=str(uuid.uuid4()),
                                        path=Path(entry.path),
                                        origin="source",
                                        source_root=source_root,
                                    )
                                    self.metrics.mark_task_seen()
                                    self.metrics.mark_stage("A1")
                                    if not self._put_with_stop(self.q12, task):
                                        break
                                except Exception as exc:
                                    self.logger.warning(
                                        "Ошибка чтения entry %s: %s", entry.path, exc
                                    )
                                    self.metrics.mark_stage("A1", error=True)
                    except Exception as exc:
                        self.logger.warning(
                            "Ошибка входа в каталог %s: %s", cur, exc
                        )
                        self.metrics.mark_stage("A1", error=True)
        finally:
            self.scan_done.set()
            self.metrics.add_event("Agent1: поиск завершен")

    def _unpack_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent2/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q12.empty():
                break
            try:
                task: FileTask = self.q12.get(timeout=0.3)
            except queue.Empty:
                if (
                    self.scan_done.is_set()
                    and self.q12.empty()
                    and self.unpack_active.get() == 0
                ):
                    break
                continue
            self.unpack_active.inc()
            try:
                if self.cleanup_event.is_set():
                    continue
                self.metrics.mark_stage("A2")
                if is_archive(task.path):
                    self._extract_archive_and_route(task)
                    self._finalize_task(task, result="archive_unpacked")
                else:
                    self._put_with_stop(self.q23, task)
                self.db.mark_file(task, "unpack_done", "")
            except Exception as exc:
                self.logger.exception("Agent2: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A2", error=True)
                self.db.mark_file(task, "unpack_failed", str(exc))
                self._finalize_task(task, result="failed")
            finally:
                self.unpack_active.dec()
                self.q12.task_done()
        self.metrics.add_event(f"Agent2/W{worker_idx}: завершен")

    def _extract_archive_and_route(self, task: FileTask) -> None:
        temp_root = self.config.temp_base / "extract" / f"{task.task_id}_{uuid.uuid4().hex[:6]}"
        temp_root.mkdir(parents=True, exist_ok=True)
        source_archive = task.archive_source
        if source_archive is None and task.origin == "source":
            source_archive = task.path
        cmd = [
            self.seven_zip,
            "x",
            "-y",
            "-bd",
            "-bb0",
            f"-o{str(temp_root)}",
            str(task.path),
        ]
        with self.extract_semaphore:
            completed = self._run_cmd_with_cancel(cmd, timeout_sec=7200)
        if completed.returncode != 0:
            raise RuntimeError(
                f"7z extract failed ({completed.returncode}): "
                f"{format_subprocess_error(completed, 300)}"
            )

        found_any = False
        for file_path in iter_files(temp_root):
            if self.should_stop():
                break
            found_any = True
            new_task = FileTask(
                task_id=str(uuid.uuid4()),
                path=file_path,
                origin="temp",
                source_root=task.source_root,
                archive_chain=task.archive_chain + [task.path.name],
                archive_source=source_archive,
                cleanup_root=temp_root,
            )
            self.temp_tracker.register(temp_root)
            self.metrics.mark_task_seen()

            if is_archive(file_path):
                if not self._put_with_stop(self.q12, new_task):
                    self.temp_tracker.release(temp_root)
                    break
                self._register_archive_child(source_archive)
            else:
                if not self._put_with_stop(self.q23, new_task):
                    self.temp_tracker.release(temp_root)
                    break
                self._register_archive_child(source_archive)

        if not found_any:
            shutil.rmtree(temp_root, ignore_errors=True)
            self.metrics.add_event(f"Пустой архив: {task.path.name}")

    def _detect_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent3/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q23.empty():
                break
            try:
                task: FileTask = self.q23.get(timeout=0.3)
            except queue.Empty:
                if self.unpack_done.is_set() and self.q23.empty():
                    break
                continue
            try:
                self.metrics.mark_stage("A3")
                is_book_file, reason = self._is_book_candidate(task)
                if is_book_file:
                    task.is_book_candidate = True
                    if not task.book_seen_counted:
                        task.book_seen_counted = True
                        self.metrics.mark_book_seen()
                    self._put_with_stop(self.q34, task)
                    self.db.mark_file(task, "book_candidate", reason)
                    self.logger.info(
                        "A3 book path=%s origin=%s reason=%s",
                        task.path,
                        task.origin,
                        reason,
                    )
                else:
                    self._handle_nobook(task, reason)
                    self.db.mark_file(task, "nobook", reason)
                    self.logger.info(
                        "A3 nobook path=%s origin=%s reason=%s",
                        task.path,
                        task.origin,
                        reason,
                    )
                    self._finalize_task(task, result="nobook")
            except Exception as exc:
                self.logger.exception("Agent3: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A3", error=True)
                self.db.mark_file(task, "detect_failed", str(exc))
                self._finalize_task(task, result="failed")
            finally:
                self.q23.task_done()
        self.metrics.add_event(f"Agent3/W{worker_idx}: завершен")

    def _tags_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent5/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q45.empty():
                break
            try:
                task: FileTask = self.q45.get(timeout=0.3)
            except queue.Empty:
                if self.dedupe_done.is_set() and self.q45.empty():
                    break
                continue
            try:
                self.metrics.mark_stage("A5")
                task.metadata = self._extract_metadata(task)
                self.logger.info("A5 tags path=%s %s", task.path, self._md_brief(task.metadata))
                self._put_with_stop(self.q56, task)
                self.db.mark_file(task, "tags_done", json.dumps(task.metadata.__dict__, ensure_ascii=False))
            except Exception as exc:
                self.logger.exception("Agent5: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A5", error=True)
                self.db.mark_file(task, "tags_failed", str(exc))
                self._put_with_stop(self.q56, task)
            finally:
                self.q45.task_done()
        self.metrics.add_event(f"Agent5/W{worker_idx}: завершен")

    def _lm_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent6/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q56.empty():
                break
            try:
                task: FileTask = self.q56.get(timeout=0.3)
            except queue.Empty:
                if self.tag_done.is_set() and self.q56.empty():
                    break
                continue
            try:
                self.metrics.mark_stage("A6")
                decision = self._lm_decision(task.metadata)
                self.logger.info(
                    "A6 decision=%s path=%s %s",
                    decision,
                    task.path,
                    self._md_brief(task.metadata),
                )
                if decision == "full":
                    snippet = extract_text_snippet(task.path, max_chars=self.config.lm_input_chars)
                    if snippet and has_meaningful_lm_text(
                        snippet, min_letters=self.config.lm_min_snippet_letters
                    ):
                        lm_input = snippet
                        lm_input_mode = "snippet"
                    elif self.config.lm_force_full_metadata or self.config.lm_always_try_without_snippet:
                        lm_input = build_lm_fallback_context(
                            task, max_chars=self.config.lm_input_chars
                        )
                        lm_input_mode = "fallback_context"
                        self.metrics.add_event(f"LM fallback-context: {task.path.name}")
                    else:
                        lm_input = ""
                        lm_input_mode = "none"
                        self.metrics.add_event(f"LM skip(no text): {task.path.name}")
                    self.logger.info(
                        "A6 full_input path=%s mode=%s chars=%d",
                        task.path,
                        lm_input_mode,
                        len(lm_input),
                    )
                    if lm_input:
                        lm_data = self.lm_client.enrich(task, lm_input)
                        if lm_data:
                            self._merge_lm_metadata(task.metadata, lm_data)
                            self.logger.info(
                                "A6 full_ok path=%s data=%s",
                                task.path,
                                json.dumps(lm_data, ensure_ascii=False),
                            )
                        else:
                            self.logger.info("A6 full_no_result path=%s", task.path)
                elif decision == "genre_only":
                    lm_data = self.lm_client.enrich_genre_only(task)
                    if lm_data:
                        self._merge_lm_metadata(task.metadata, lm_data)
                        self.logger.info(
                            "A6 genre_ok path=%s data=%s",
                            task.path,
                            json.dumps(lm_data, ensure_ascii=False),
                        )
                    else:
                        self.logger.info("A6 genre_no_result path=%s", task.path)
                else:
                    self.logger.info("A6 skip path=%s reason=%s", task.path, decision)
                self._put_with_stop(self.q67, task)
                self.db.mark_file(task, "lm_done", json.dumps(task.metadata.__dict__, ensure_ascii=False))
            except Exception as exc:
                self.logger.exception("Agent6: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A6", error=True)
                self.db.mark_file(task, "lm_failed", str(exc))
                self._put_with_stop(self.q67, task)
            finally:
                self.q56.task_done()
        self.metrics.add_event(f"Agent6/W{worker_idx}: завершен")

    def _rename_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent7/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q67.empty():
                break
            try:
                task: FileTask = self.q67.get(timeout=0.3)
            except queue.Empty:
                if self.lm_done.is_set() and self.q67.empty():
                    break
                continue
            try:
                self.metrics.mark_stage("A7")
                task.dest_zip = self._build_destination(task)
                self.logger.info("A7 route path=%s dest=%s", task.path, task.dest_zip)
                self._put_with_stop(self.q78, task)
                self.db.mark_file(task, "rename_done", str(task.dest_zip))
            except Exception as exc:
                self.logger.exception("Agent7: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A7", error=True)
                self.db.mark_file(task, "rename_failed", str(exc))
                self._finalize_task(task, result="failed")
            finally:
                self.q67.task_done()
        self.metrics.add_event(f"Agent7/W{worker_idx}: завершен")

    def _dedupe_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent4/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q34.empty():
                break
            try:
                task: FileTask = self.q34.get(timeout=0.3)
            except queue.Empty:
                if self.detect_done.is_set() and self.q34.empty():
                    break
                continue
            try:
                self.metrics.mark_stage("A4")
                xxh = xxh64_file(task.path)
                task.xxh64 = xxh
                is_unique, canonical = self.db.claim_hash(
                    xxh,
                    task.path,
                    "",
                    "",
                    "",
                )
                if is_unique:
                    self._put_with_stop(self.q45, task)
                    self.db.mark_file(task, "unique", xxh)
                    self.logger.info(
                        "A4 unique path=%s hash=%s",
                        task.path,
                        xxh[:12],
                    )
                else:
                    duplicate_result = self._handle_duplicate(task, canonical)
                    self.db.mark_file(task, duplicate_result, canonical or "")
                    self.logger.info(
                        "A4 %s path=%s hash=%s canonical=%s",
                        duplicate_result,
                        task.path,
                        xxh[:12],
                        canonical or "",
                    )
                    self._finalize_task(task, result=duplicate_result)
            except Exception as exc:
                self.logger.exception("Agent4: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A4", error=True)
                self.db.mark_file(task, "dedupe_failed", str(exc))
                self._finalize_task(task, result="failed")
            finally:
                self.q34.task_done()
        self.metrics.add_event(f"Agent4/W{worker_idx}: завершен")

    def _pack_loop(self, worker_idx: int) -> None:
        self.metrics.add_event(f"Agent8/W{worker_idx}: старт")
        while True:
            if self.cleanup_event.is_set():
                break
            if self.should_stop() and self.q78.empty():
                break
            try:
                task: FileTask = self.q78.get(timeout=0.3)
            except queue.Empty:
                if self.rename_done.is_set() and self.q78.empty():
                    break
                continue
            try:
                self.metrics.mark_stage("A8")
                self._pack_task(task)
                if task.xxh64 and task.dest_zip:
                    self.db.update_hash_record(
                        task.xxh64,
                        task.dest_zip,
                        task.metadata.title,
                        task.metadata.author,
                        task.metadata.genre,
                    )
                self.db.mark_file(task, "packed", str(task.dest_zip))
                self.logger.info(
                    "A8 packed src=%s dest=%s hash=%s",
                    task.path,
                    task.dest_zip,
                    (task.xxh64 or "")[:12],
                )
                self._finalize_task(task, result="packed", delete_source=True)
            except Exception as exc:
                self.logger.exception("Agent8: ошибка %s: %s", task.path, exc)
                self.metrics.mark_stage("A8", error=True)
                self.db.mark_file(task, "pack_failed", str(exc))
                if task.xxh64:
                    self.db.remove_hash(task.xxh64)
                self.metrics.add_event(
                    f"Pack failed: {task.path.name} -> {truncate(str(exc), 160)}"
                )
                self._finalize_task(task, result="failed")
            finally:
                self.q78.task_done()
        self.metrics.add_event(f"Agent8/W{worker_idx}: завершен")

    def _is_book_candidate(self, task: FileTask) -> tuple[bool, str]:
        path = task.path
        ext = suffix_lower(path)
        if ext in BOOK_EXTENSIONS:
            return True, f"book_ext:{ext}"
        if ext in STRONG_NONBOOK_EXTENSIONS:
            return False, f"nonbook_ext:{ext}"

        size = safe_filesize(path)
        if size == 0:
            return False, "empty_file"
        if ext in TEXT_EXTENSIONS:
            snippet = extract_text_snippet(path, max_chars=400).lower()
            if any(token in snippet for token in ("isbn", "глава", "chapter", "автор")):
                return True, "text_book_signals"
            return True, "text_unknown_keep"

        # Неизвестные маленькие бинарники чаще оказываются прошивками/служебными файлами.
        if looks_binary(path):
            if size < TINY_UNKNOWN_BINARY_BYTES:
                return False, f"tiny_unknown_binary:{ext or 'no_ext'}"
            return True, "binary_unknown_keep"
        return True, "fallback_keep"

    def _handle_nobook(self, task: FileTask, reason: str) -> None:
        target_base = self.config.nobook_dir
        if task.origin == "source":
            base = task.source_root or (
                self.config.source_dirs[0] if self.config.source_dirs else task.path.parent
            )
            rel = safe_relative(task.path, base)
            dst = ensure_unique_file_path(target_base / rel)
            dst.parent.mkdir(parents=True, exist_ok=True)
            safe_move(task.path, dst)
            self.metrics.add_event(f"NoBook -> {dst.name} ({reason})")
        else:
            if self.config.keep_temp_nobooks:
                chain_dir = "__".join(sanitize_component(x) for x in task.archive_chain[-3:])
                ext = task.path.suffix.lower()
                filename = sanitize_component(task.path.stem) + ext
                out = ensure_unique_file_path(target_base / "FromArchives" / chain_dir / filename)
                out.parent.mkdir(parents=True, exist_ok=True)
                safe_move(task.path, out)

    def _extract_metadata(self, task: FileTask) -> Metadata:
        md = Metadata()
        ext = suffix_lower(task.path)

        # 1) Имя файла
        parsed = parse_filename(task.path.stem)
        if parsed.get("title"):
            md.title = parsed["title"]
            md.source = "filename"
            md.confidence = max(md.confidence, 0.35)
        if parsed.get("author"):
            md.author = parsed["author"]
            md.source = "filename"
            md.confidence = max(md.confidence, 0.35)

        # 2) Форматные теги
        fmt_tags = {}
        try:
            if ext == ".epub":
                fmt_tags = extract_epub_metadata(task.path)
            elif ext == ".fb2":
                fmt_tags = extract_fb2_metadata(task.path)
            elif ext in {".docx"}:
                fmt_tags = extract_docx_metadata(task.path)
            elif ext == ".pdf":
                fmt_tags = extract_pdf_metadata(task.path)
        except Exception as exc:
            self.logger.debug("Ошибка чтения тегов %s: %s", task.path, exc)

        if fmt_tags.get("title") and not md.title:
            md.title = fmt_tags["title"]
            md.source = "tags"
            md.confidence = max(md.confidence, 0.75)
        if fmt_tags.get("author") and not md.author:
            md.author = fmt_tags["author"]
            md.source = "tags"
            md.confidence = max(md.confidence, 0.75)
        if fmt_tags.get("genre"):
            md.genre = normalize_genre(fmt_tags["genre"])
            md.source = "tags"
            md.confidence = max(md.confidence, 0.75)

        # 3) Жанр из пути/цепочки
        if not md.genre:
            guess = infer_genre_from_path(task.path, task.archive_chain)
            if guess:
                md.genre = guess
                md.confidence = max(md.confidence, 0.5)
                if md.source == "none":
                    md.source = "path"

        # 4) Жанр из названия (дешево, без LM)
        if not md.genre or md.genre == "Unknown":
            guess = infer_genre_from_title(md.title)
            if guess:
                md.genre = guess
                md.confidence = max(md.confidence, 0.55)
                if md.source == "none":
                    md.source = "title"

        # 5) Безопасные fallback
        if not md.title:
            md.title = task.path.stem
        if not md.author:
            md.author = "Unknown Author"
        if not md.genre:
            md.genre = "Unknown"
        return md

    def _needs_lm(self, md: Metadata) -> bool:
        if md.source == "tags" and md.confidence >= 0.75:
            return False
        missing_title = not md.title or md.title.strip().lower() in {
            "unknown",
            "unknown title",
        }
        missing_author = not md.author or md.author.strip().lower() in {
            "unknown author",
        }
        # Не тратим LM только ради жанра, если автор и название уже определены.
        if not missing_title and not missing_author:
            return False
        return missing_title or missing_author

    def _needs_genre_only_lm(self, md: Metadata) -> bool:
        missing_title = not md.title or md.title.strip().lower() in {
            "unknown",
            "unknown title",
        }
        missing_author = not md.author or md.author.strip().lower() in {
            "unknown author",
        }
        missing_genre = not md.genre or md.genre.strip().lower() in {"unknown"}
        return (not missing_title) and (not missing_author) and missing_genre

    def _merge_lm_metadata(self, md: Metadata, lm_data: dict[str, Any]) -> None:
        title = clean_text(lm_data.get("title", ""))
        author = clean_text(lm_data.get("author", ""))
        genre = clean_text(lm_data.get("genre", ""))
        subgenres_raw = lm_data.get("subgenres", [])
        subgenres: list[str] = []
        if isinstance(subgenres_raw, list):
            subgenres = [clean_text(str(x)) for x in subgenres_raw if clean_text(str(x))]
        elif subgenres_raw:
            subgenres = [clean_text(str(subgenres_raw))]
        try:
            conf = float(lm_data.get("confidence", 0.0))
        except Exception:
            conf = 0.0

        if title:
            md.title = title
        if author:
            md.author = author
        if genre:
            md.genre = clean_text(genre)
        if subgenres:
            md.subgenres = subgenres
        if conf > md.confidence:
            md.confidence = conf
        if title or author or genre or subgenres:
            md.source = "lmstudio"

    def _build_destination(self, task: FileTask) -> Path:
        md = task.metadata
        genre = sanitize_component(md.genre or "Unknown", max_len=48)
        author = sanitize_component(md.author or "Unknown Author", max_len=64)
        first = first_letter(author)
        title = sanitize_component(md.title or task.path.stem, max_len=110)

        for _ in range(6):
            out_dir = self.config.target_dir / genre / first / author
            dest = out_dir / f"{title}.zip"
            if os.name != "nt" or len(str(dest)) <= 240:
                out_dir.mkdir(parents=True, exist_ok=True)
                return dest

            # На Windows сначала укорачиваем title, затем author, затем genre.
            if len(title) > 36:
                title = shorten_with_hash(title, max_len=max(36, len(title) - 18))
                continue
            if len(author) > 24:
                author = shorten_with_hash(author, max_len=max(24, len(author) - 10))
                first = first_letter(author)
                continue
            if len(genre) > 20:
                genre = shorten_with_hash(genre, max_len=20)
                continue
            break

        # Жесткий fallback, чтобы путь гарантированно поместился.
        genre = shorten_with_hash(genre, max_len=20)
        author = shorten_with_hash(author, max_len=24)
        first = first_letter(author)
        title = shorten_with_hash(title, max_len=36)
        out_dir = self.config.target_dir / genre / first / author
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"{title}.zip"

    def _handle_duplicate(self, task: FileTask, canonical: Optional[str]) -> str:
        if task.origin == "source":
            base_name = sanitize_component(task.path.stem)
            ext = task.path.suffix.lower()
            h8 = (task.xxh64 or "dup")[:12]
            out_dir = self.config.dupes_dir / h8
            out_dir.mkdir(parents=True, exist_ok=True)
            dst = ensure_unique_file_path(out_dir / f"{base_name}{ext}")
            safe_move(task.path, dst)
            self.metrics.add_event(
                f"Duplicate -> {dst.name}; canonical={Path(canonical).name if canonical else 'n/a'}"
            )
            return "duplicate"
        else:
            # Для временных файлов дубликат фиксируем в журнале и освобождаем temp.
            self.metrics.add_event(
                f"Duplicate temp: {task.path.name}; canonical={Path(canonical).name if canonical else 'n/a'}"
            )
            return "duplicate_temp"

    def _pack_task(self, task: FileTask) -> None:
        assert task.dest_zip is not None
        dest = resolve_collision(task.dest_zip, task.xxh64 or "")
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Создаем временный архив рядом с финальным выходом (обычно E:),
        # чтобы не делать междисковый перенос C: -> E: и не требовать место на TEMP.
        tmp_zip = dest.parent / f".__tmp_{dest.stem}_{uuid.uuid4().hex[:8]}.zip"
        tmp_zip.unlink(missing_ok=True)
        local_input = f".{os.sep}{task.path.name}"

        cmd_add = [
            self.seven_zip,
            "a",
            "-y",
            "-bd",
            "-bb0",
            "-tzip",
            "-mx=9",
            "-mfb=258",
            "-mpass=15",
            str(tmp_zip),
            local_input,
        ]
        result_add = self._run_cmd_with_cancel(
            cmd_add,
            timeout_sec=3600,
            cwd=task.path.parent,
        )
        if result_add.returncode != 0:
            tmp_zip.unlink(missing_ok=True)
            raise RuntimeError(
                f"7z add failed ({result_add.returncode}): "
                f"{format_subprocess_error(result_add, 350)}"
            )

        cmd_test = [self.seven_zip, "t", "-y", "-bd", "-bb0", str(tmp_zip)]
        result_test = self._run_cmd_with_cancel(cmd_test, timeout_sec=1800)
        if result_test.returncode != 0:
            tmp_zip.unlink(missing_ok=True)
            raise RuntimeError(
                f"7z test failed ({result_test.returncode}): "
                f"{format_subprocess_error(result_test, 350)}"
            )

        atomic_replace(tmp_zip, dest)
        task.dest_zip = dest

    def _finalize_task(
        self,
        task: FileTask,
        result: str,
        delete_source: bool = False,
    ) -> None:
        try:
            if (
                delete_source
                and self.config.delete_source_after_pack
                and task.origin == "source"
                and task.path.exists()
            ):
                task.path.unlink(missing_ok=True)
        except Exception as exc:
            self.logger.warning("Не удалось удалить исходник %s: %s", task.path, exc)

        self._mark_archive_progress(task, result)
        self.temp_tracker.release(task.cleanup_root)
        self.metrics.mark_task_done(result)
        if task.is_book_candidate and not task.book_done_counted:
            task.book_done_counted = True
            self.metrics.mark_book_done(result)

    def _clear_all_queues(self) -> None:
        for q in (self.q12, self.q23, self.q34, self.q45, self.q56, self.q67, self.q78):
            while True:
                try:
                    _ = q.get_nowait()
                    q.task_done()
                except queue.Empty:
                    break
                except Exception:
                    break

    def _is_safe_temp_base_for_cleanup(self) -> tuple[bool, str]:
        try:
            temp = self.config.temp_base.resolve(strict=False)
        except Exception as exc:
            return False, f"cannot resolve TEMP_BASE: {exc}"

        if not temp.exists():
            return True, "not exists"
        if not temp.is_dir():
            return False, "TEMP_BASE is not a directory"
        if temp.is_symlink():
            return False, "TEMP_BASE is a symlink"

        try:
            if temp == Path(temp.anchor).resolve(strict=False):
                return False, "TEMP_BASE points to drive/root"
        except Exception:
            pass

        protected = [
            self.config.target_dir,
            self.config.dupes_dir,
            self.config.nobook_dir,
            *self.config.source_dirs,
        ]
        for raw in protected:
            try:
                p = raw.resolve(strict=False)
            except Exception:
                continue
            if temp == p:
                return False, f"TEMP_BASE equals protected path: {p}"
            try:
                p.relative_to(temp)
                return False, f"TEMP_BASE is parent of protected path: {p}"
            except Exception:
                continue

        return True, "ok"

    def _cleanup_temp_base(self) -> None:
        safe, reason = self._is_safe_temp_base_for_cleanup()
        if not safe:
            self.logger.warning("Temp cleanup skipped: %s (%s)", reason, self.config.temp_base)
            self.metrics.add_event(f"Temp cleanup skipped: {reason}")
            return

        try:
            if not self.config.temp_base.exists():
                self.metrics.add_event("Temp уже пуст")
                return
            for child in self.config.temp_base.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            self.metrics.add_event("Temp очищен полностью")
        except Exception as exc:
            self.logger.exception("Ошибка очистки temp: %s", exc)
            self.metrics.add_event(f"Ошибка очистки temp: {exc}")

    def _source_dir_has_any_files(self, source_dir: Path) -> bool:
        for _root, _dirs, files in os.walk(source_dir, topdown=True, followlinks=False):
            if files:
                return True
        return False

    def _cleanup_empty_source_dirs(self) -> None:
        seen: set[str] = set()
        for source_dir in self.config.source_dirs:
            try:
                src = source_dir.resolve(strict=False)
            except Exception:
                src = source_dir

            src_key = str(src).lower()
            if src_key in seen:
                continue
            seen.add(src_key)

            if not src.exists() or not src.is_dir():
                continue

            if src.is_symlink():
                self.metrics.add_event(f"Source cleanup skipped (symlink): {src}")
                continue

            try:
                root = Path(src.anchor).resolve(strict=False)
                if src == root:
                    self.metrics.add_event(f"Source cleanup skipped (drive root): {src}")
                    continue
            except Exception:
                pass

            if self._source_dir_has_any_files(src):
                continue

            try:
                shutil.rmtree(src)
                self.metrics.add_event(f"Source dir removed (empty): {src}")
            except Exception as exc:
                self.logger.warning("Cannot remove empty source dir %s: %s", src, exc)
                self.metrics.add_event(f"Source dir cleanup error: {src}: {exc}")

    def _cleanup_runtime_state(self) -> None:
        try:
            shutil.rmtree(self.runtime_dir, ignore_errors=True)
        except Exception:
            pass

    def _register_archive_child(self, archive_path: Optional[Path]) -> None:
        if not archive_path:
            return
        with self._archive_state_lock:
            self._archive_pending[archive_path] += 1

    def _mark_archive_progress(self, task: FileTask, result: str) -> None:
        archive_path = task.archive_source
        if not archive_path:
            return

        should_delete = False
        with self._archive_state_lock:
            if archive_path not in self._archive_pending:
                return

            if result == "failed":
                self._archive_failed.add(archive_path)

            self._archive_pending[archive_path] -= 1
            pending = self._archive_pending[archive_path]
            failed = archive_path in self._archive_failed

            if pending <= 0:
                del self._archive_pending[archive_path]
                self._archive_failed.discard(archive_path)
                should_delete = (not failed) and self.config.delete_source_after_pack

        if should_delete and archive_path.exists():
            try:
                archive_path.unlink(missing_ok=True)
                self.metrics.add_event(f"Source archive removed after pipeline: {archive_path.name}")
            except Exception as exc:
                self.logger.warning(
                    "Не удалось удалить исходный архив %s: %s", archive_path, exc
                )

    def _run_cmd_with_cancel(
        self,
        cmd: list[str],
        timeout_sec: int,
        cwd: Optional[Path] = None,
    ) -> subprocess.CompletedProcess:
        started = time.time()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            cwd=str(cwd) if cwd else None,
        )
        interrupted = False
        while True:
            if self.should_stop():
                interrupted = True
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                break
            if proc.poll() is not None:
                break
            if (time.time() - started) > timeout_sec:
                try:
                    proc.kill()
                except Exception:
                    pass
                break
            time.sleep(0.2)

        try:
            out_bytes, err_bytes = proc.communicate(timeout=5)
        except Exception:
            out_bytes, err_bytes = b"", b"communicate_timeout"
        out = decode_subprocess_output(out_bytes)
        err = decode_subprocess_output(err_bytes)
        code = proc.returncode if proc.returncode is not None else -1
        if interrupted and code == 0:
            code = -1
        if interrupted and not err:
            err = "interrupted_by_stop_signal"
        if (time.time() - started) > timeout_sec and code == 0:
            code = 124
        return subprocess.CompletedProcess(cmd, code, out, err)


def enable_ansi_on_windows() -> None:
    if os.name != "nt":
        return
    try:
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    except Exception:
        pass


def iter_files(root: Path):
    stack = [root]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            yield Path(entry.path)
                    except Exception:
                        continue
        except Exception:
            continue


def suffix_lower(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".fb2.zip"):
        return ".fb2.zip"
    return path.suffix.lower()


def is_archive(path: Path) -> bool:
    ext = suffix_lower(path)
    if ext in ARCHIVE_EXTENSIONS:
        return True
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
        if sig.startswith(b"PK\x03\x04"):
            return True
        if sig.startswith(b"Rar!\x1a\x07"):
            return True
        if sig.startswith(b"7z\xbc\xaf'\x1c"):
            return True
    except Exception:
        return False
    return False


def xxh64_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = xxhash.xxh64()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def decode_subprocess_output(data: bytes | str | None) -> str:
    if not data:
        return ""
    if isinstance(data, str):
        return data

    candidates = ["utf-8-sig", "utf-8"]
    if os.name == "nt":
        try:
            oem_cp = int(ctypes.windll.kernel32.GetOEMCP())  # type: ignore[attr-defined]
            candidates.append(f"cp{oem_cp}")
        except Exception:
            pass
        try:
            acp = int(ctypes.windll.kernel32.GetACP())  # type: ignore[attr-defined]
            candidates.append(f"cp{acp}")
        except Exception:
            pass
        candidates.append("mbcs")
    candidates.extend(["cp866", "cp1251"])

    russian_letters = set(
        "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
        "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
    )
    common_text = set(
        " оеаинтсрвлкмдпуяызьбгчйхжюшцщэфъ"
        "ОЕАИНТСРВЛКМДПУЯЫЗЬБГЧЙХЖЮШЦЩЭФЪ"
    )

    def score(text: str) -> int:
        value = 0
        for ch in text:
            code = ord(ch)
            if ch == "\ufffd":
                value -= 100
            elif code < 32 and ch not in "\r\n\t":
                value -= 20
            elif 0x2500 <= code <= 0x259F:
                value -= 30
            elif "\u0400" <= ch <= "\u04ff" and ch not in russian_letters:
                value -= 12
            elif ch in common_text:
                value += 4
            elif ch in russian_letters:
                value += 2
            elif 32 <= code <= 126:
                value += 1
        return value

    best_text = ""
    best_score = -10**9
    seen: set[str] = set()
    for enc in candidates:
        key = enc.lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            decoded = data.decode(enc)
        except Exception:
            continue
        current_score = score(decoded)
        if current_score > best_score:
            best_text = decoded
            best_score = current_score
    if best_text:
        return best_text
    return data.decode("utf-8", errors="replace")


def xxh64_zip_payload(path: Path, chunk_size: int = 4 * 1024 * 1024) -> tuple[Optional[str], str]:
    try:
        with zipfile.ZipFile(path, "r") as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            if not infos:
                return None, "empty_zip"

            preferred = [
                info
                for info in infos
                if suffix_lower(Path(info.filename)) in BOOK_EXTENSIONS
            ]
            pool = preferred or infos
            chosen = max(pool, key=lambda x: int(getattr(x, "file_size", 0) or 0))

            h = xxhash.xxh64()
            with zf.open(chosen, "r") as entry:
                while True:
                    chunk = entry.read(chunk_size)
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest(), chosen.filename
    except zipfile.BadZipFile:
        return None, "bad_zip"
    except RuntimeError as exc:
        return None, f"runtime_error: {exc}"
    except Exception as exc:
        return None, f"zip_error: {exc}"


def safe_filesize(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


def looks_binary(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            data = f.read(4096)
        if not data:
            return False
        if b"\x00" in data:
            return True
        text_chars = sum((32 <= b <= 126) or b in b"\r\n\t\f\b" for b in data)
        return (text_chars / len(data)) < 0.65
    except Exception:
        return True


def extract_text_snippet(path: Path, max_chars: int = 1800) -> str:
    ext = suffix_lower(path)
    try:
        if ext in {".txt", ".md", ".csv", ".tsv", ".json", ".xml", ".html", ".htm", ".fb2", ".rtf"}:
            return read_text_head(path, max_chars)
        if ext == ".docx":
            with zipfile.ZipFile(path, "r") as zf:
                if "word/document.xml" in zf.namelist():
                    raw = zf.read("word/document.xml")
                    txt = strip_xml_tags(raw.decode("utf-8", errors="ignore"))
                    return clean_text(txt)[:max_chars]
        if ext == ".epub":
            with zipfile.ZipFile(path, "r") as zf:
                candidates = [n for n in zf.namelist() if n.lower().endswith((".xhtml", ".html", ".htm"))]
                for name in candidates[:3]:
                    raw = zf.read(name)
                    txt = strip_xml_tags(raw.decode("utf-8", errors="ignore"))
                    if txt.strip():
                        return clean_text(txt)[:max_chars]
        if ext == ".pdf":
            try:
                import pypdf  # type: ignore

                reader = pypdf.PdfReader(str(path))
                if reader.pages:
                    txt = reader.pages[0].extract_text() or ""
                    return clean_text(txt)[:max_chars]
            except Exception:
                return ""
    except Exception:
        return ""
    return ""


def read_text_head(path: Path, max_chars: int) -> str:
    raw = b""
    with open(path, "rb") as f:
        raw = f.read(max_chars * 4)
    for enc in ("utf-8", "utf-16", "cp1251", "latin1"):
        try:
            txt = raw.decode(enc, errors="ignore")
            return clean_text(txt)[:max_chars]
        except Exception:
            continue
    return ""


def parse_filename(stem: str) -> dict[str, str]:
    stem_clean = clean_text(stem.replace("_", " "))
    patterns = [
        r"^\s*(?P<author>.+?)\s*[-–—]\s*(?P<title>.+?)\s*$",
        r"^\s*(?P<title>.+?)\s*\((?P<author>.+?)\)\s*$",
    ]
    for pat in patterns:
        m = re.match(pat, stem_clean)
        if m:
            return {
                "author": clean_text(m.groupdict().get("author", "")),
                "title": clean_text(m.groupdict().get("title", "")),
            }
    return {"title": stem_clean}


def extract_epub_metadata(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with zipfile.ZipFile(path, "r") as zf:
        container = "META-INF/container.xml"
        if container not in zf.namelist():
            return out
        container_xml = zf.read(container).decode("utf-8", errors="ignore")
        m = re.search(r'full-path="([^"]+)"', container_xml)
        if not m:
            return out
        opf_path = m.group(1)
        if opf_path not in zf.namelist():
            return out
        opf_xml = zf.read(opf_path).decode("utf-8", errors="ignore")
        out["title"] = first_group(opf_xml, r"<dc:title[^>]*>(.*?)</dc:title>")
        out["author"] = first_group(opf_xml, r"<dc:creator[^>]*>(.*?)</dc:creator>")
        out["genre"] = first_group(opf_xml, r"<dc:subject[^>]*>(.*?)</dc:subject>")
    return {k: clean_text(v) for k, v in out.items() if v}


def extract_fb2_metadata(path: Path) -> dict[str, str]:
    raw = b""
    with open(path, "rb") as f:
        raw = f.read(600_000)
    txt = raw.decode("utf-8", errors="ignore")
    title = first_group(txt, r"<book-title>(.*?)</book-title>")
    genre = first_group(txt, r"<genre>(.*?)</genre>")
    first = first_group(txt, r"<first-name>(.*?)</first-name>")
    last = first_group(txt, r"<last-name>(.*?)</last-name>")
    author = clean_text(" ".join(x for x in (first, last) if x))
    out = {}
    if title:
        out["title"] = clean_text(title)
    if author:
        out["author"] = author
    if genre:
        out["genre"] = clean_text(genre)
    return out


def extract_docx_metadata(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with zipfile.ZipFile(path, "r") as zf:
        if "docProps/core.xml" not in zf.namelist():
            return out
        core = zf.read("docProps/core.xml").decode("utf-8", errors="ignore")
        title = first_group(core, r"<dc:title>(.*?)</dc:title>")
        creator = first_group(core, r"<dc:creator>(.*?)</dc:creator>")
        subj = first_group(core, r"<dc:subject>(.*?)</dc:subject>")
        if title:
            out["title"] = clean_text(title)
        if creator:
            out["author"] = clean_text(creator)
        if subj:
            out["genre"] = clean_text(subj)
    return out


def extract_pdf_metadata(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        import pypdf  # type: ignore

        reader = pypdf.PdfReader(str(path))
        meta = reader.metadata or {}
        title = clean_text(str(meta.get("/Title", "")))
        author = clean_text(str(meta.get("/Author", "")))
        subj = clean_text(str(meta.get("/Subject", "")))
        if title:
            out["title"] = title
        if author:
            out["author"] = author
        if subj:
            out["genre"] = subj
    except Exception:
        return {}
    return out


def infer_genre_from_path(path: Path, chain: list[str]) -> str:
    haystack = " ".join(
        [path.name.lower(), *[x.lower() for x in chain], *[p.lower() for p in path.parts]]
    )
    keywords = {
        "энциклопед": "Энциклопедии",
        "словар": "Словари",
        "справоч": "Справочники",
        "учеб": "Учебная литература",
        "истор": "История",
        "фантаст": "Фантастика",
        "science": "Наука",
        "наук": "Наука",
        "детск": "Детская литература",
        "роман": "Романы",
        "поэз": "Поэзия",
    }
    for key, val in keywords.items():
        if key in haystack:
            return val
    return ""


def infer_genre_from_title(title: str) -> str:
    t = clean_text(title).lower()
    if not t:
        return ""
    keywords = {
        "энциклопед": "Энциклопедии",
        "словар": "Словари",
        "справоч": "Справочники",
        "учеб": "Учебная литература",
        "истор": "История",
        "фантаст": "Фантастика",
        "роман": "Романы",
        "поэз": "Поэзия",
        "детск": "Детская литература",
        "science": "Наука",
        "наук": "Наука",
    }
    for key, val in keywords.items():
        if key in t:
            return val
    return ""


def normalize_genre(genre: str) -> str:
    g = clean_text(genre).lower()
    if not g:
        return "Unknown"
    mapping = {
        "энциклоп": "Энциклопедии",
        "словар": "Словари",
        "справоч": "Справочники",
        "учеб": "Учебная литература",
        "истор": "История",
        "фантаст": "Фантастика",
        "science": "Наука",
        "наука": "Наука",
        "дет": "Детская литература",
        "поэз": "Поэзия",
        "роман": "Романы",
        "документ": "Документальная литература",
    }
    for key, out in mapping.items():
        if key in g:
            return out
    return clean_text(genre) or "Unknown"


def _strip_code_fence(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t)
    if t.endswith("```"):
        t = t[:-3]
    return t.strip()


def _extract_first_braced_object(text: str) -> Optional[str]:
    if not text:
        return None
    start = text.find("{")
    while start >= 0:
        depth = 0
        in_str = False
        quote = ""
        escaped = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == quote:
                    in_str = False
                continue
            if ch in {'"', "'"}:
                in_str = True
                quote = ch
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        start = text.find("{", start + 1)
    return None


def _try_parse_json_like(candidate: str) -> Optional[dict[str, Any]]:
    if not candidate:
        return None
    try:
        payload = json.loads(candidate)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    repaired = re.sub(r",\s*([}\]])", r"\1", candidate)
    repaired = repaired.replace("“", '"').replace("”", '"').replace("’", "'")
    repaired = re.sub(r"(?<!\\)'", '"', repaired)
    try:
        payload = json.loads(repaired)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    try:
        payload = ast.literal_eval(candidate)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return None


def parse_json_object(text: str) -> Optional[dict[str, Any]]:
    if not text:
        return None
    text = _strip_code_fence(text)
    payload = _try_parse_json_like(text)
    if payload:
        return payload
    candidate = _extract_first_braced_object(text)
    if candidate:
        return _try_parse_json_like(candidate)
    m = JSON_OBJECT_RE.search(text)
    if not m:
        return None
    return _try_parse_json_like(m.group(0))


def parse_model_payload(text: str) -> Optional[dict[str, Any]]:
    """
    Устойчивый разбор ответа модели:
    1) обычный JSON-объект
    2) JSON-like c лишним текстом
    3) строки вида key: value
    """
    payload = parse_json_object(text)
    if payload:
        return normalize_model_payload(payload)

    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()

    fields: dict[str, Any] = {}
    sep = r"[:=\-–—]"

    # Английские ключи
    key_pattern_en = re.compile(
        rf'(?im)\b(title|author|genre|primary_genre|confidence|confidence_score)\b\s*{sep}\s*(?:"([^"]*)"|\'([^\']*)\'|([^\n,;]+))'
    )
    for m in key_pattern_en.finditer(cleaned):
        key = m.group(1).lower()
        if key == "primary_genre":
            key = "genre"
        elif key == "confidence_score":
            key = "confidence"
        raw = m.group(2) or m.group(3) or m.group(4) or ""
        fields[key] = clean_text(raw)

    # Русские синонимы ключей
    key_pattern_ru = re.compile(
        rf'(?im)\b(название(?:\s+книги)?|автор|жанр|уверенность|достоверность)\b\s*{sep}\s*(?:"([^"]*)"|\'([^\']*)\'|([^\n,;]+))'
    )
    ru_map = {
        "название": "title",
        "название книги": "title",
        "автор": "author",
        "жанр": "genre",
        "уверенность": "confidence",
        "достоверность": "confidence",
    }
    for m in key_pattern_ru.finditer(cleaned):
        key_raw = clean_text(m.group(1)).lower()
        key = ru_map.get(key_raw)
        if not key:
            continue
        raw = m.group(2) or m.group(3) or m.group(4) or ""
        fields[key] = clean_text(raw)

    if fields:
        return normalize_model_payload(fields)
    return None


def normalize_model_payload(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    if isinstance(payload.get("results"), list) and payload["results"]:
        first = payload["results"][0]
        if isinstance(first, dict):
            return normalize_model_payload(first)

    out: dict[str, Any] = {}
    if "title" in payload:
        out["title"] = clean_text(str(payload.get("title", "")))
    if "author" in payload:
        out["author"] = clean_text(str(payload.get("author", "")))

    genre_analysis = payload.get("genre_analysis")
    if isinstance(genre_analysis, dict):
        primary_genre = clean_text(str(genre_analysis.get("primary_genre", "")))
        if primary_genre:
            out["genre"] = primary_genre
        subgenres_raw = genre_analysis.get("subgenres", [])
        if isinstance(subgenres_raw, list):
            out["subgenres"] = [
                clean_text(str(x)) for x in subgenres_raw if clean_text(str(x))
            ]
        elif subgenres_raw:
            out["subgenres"] = [clean_text(str(subgenres_raw))]
        if "confidence_score" in genre_analysis:
            try:
                out["confidence"] = float(genre_analysis.get("confidence_score", 0.0))
            except Exception:
                out["confidence"] = 0.0

    if "genre" in payload and "genre" not in out:
        out["genre"] = clean_text(str(payload.get("genre", "")))
    if "primary_genre" in payload and "genre" not in out:
        out["genre"] = clean_text(str(payload.get("primary_genre", "")))
    if "subgenres" in payload and "subgenres" not in out:
        subgenres_raw = payload.get("subgenres", [])
        if isinstance(subgenres_raw, list):
            out["subgenres"] = [
                clean_text(str(x)) for x in subgenres_raw if clean_text(str(x))
            ]
        elif subgenres_raw:
            out["subgenres"] = [clean_text(str(subgenres_raw))]

    if "confidence" in payload and "confidence" not in out:
        try:
            out["confidence"] = float(payload.get("confidence", 0.0))
        except Exception:
            out["confidence"] = 0.0
    if "confidence_score" in payload and "confidence" not in out:
        try:
            out["confidence"] = float(payload.get("confidence_score", 0.0))
        except Exception:
            out["confidence"] = 0.0

    # Разрешаем частичный ответ (например только genre в режиме genre-only).
    if not out:
        return None
    return out


def build_lm_fallback_context(task: FileTask, max_chars: int = 700) -> str:
    chain = " -> ".join(task.archive_chain) if task.archive_chain else ""
    parts = [
        f"Filename: {task.path.name}",
        f"Stem: {task.path.stem}",
        f"Suffix: {task.path.suffix.lower()}",
        f"Parent: {task.path.parent.name}",
    ]
    if chain:
        parts.append(f"Archive chain: {chain}")
    parsed = parse_filename(task.path.stem)
    if parsed.get("author"):
        parts.append(f"Possible author from filename: {parsed['author']}")
    if parsed.get("title"):
        parts.append(f"Possible title from filename: {parsed['title']}")
    text = clean_text("\n".join(parts))
    return text[:max_chars]


def has_meaningful_lm_text(text: str, min_letters: int = 24) -> bool:
    t = clean_text(text)
    if not t:
        return False
    letters = sum(1 for ch in t if ch.isalpha())
    return letters >= min_letters


def sanitize_component(value: str, max_len: int = 90) -> str:
    value = clean_text(value)
    value = INVALID_FS_CHARS.sub("_", value)
    value = value.strip(" .")
    value = SPACES_RE.sub(" ", value)
    if not value:
        value = "Unknown"
    if len(value) > max_len:
        value = value[:max_len].rstrip(" .")
    return value or "Unknown"


def shorten_with_hash(value: str, max_len: int) -> str:
    value = sanitize_component(value, max_len=300)
    if len(value) <= max_len:
        return value
    h = xxhash.xxh64(value.encode("utf-8", errors="ignore")).hexdigest()[:8]
    keep = max(1, max_len - 9)
    return f"{value[:keep].rstrip(' .')}_{h}"


def clean_text(value: str) -> str:
    if value is None:
        return ""
    value = str(value)
    value = strip_xml_tags(value)
    value = value.replace("\ufeff", "").replace("\u200b", "")
    value = SPACES_RE.sub(" ", value).strip()
    return value


def strip_xml_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text)


def first_group(text: str, pattern: str) -> str:
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return clean_text(m.group(1))


def first_letter(author: str) -> str:
    author = clean_text(author)
    if not author:
        return "#"
    ch = author[0].upper()
    if not ch.isalnum():
        return "#"
    return ch


def safe_relative(path: Path, base: Path) -> Path:
    try:
        return path.relative_to(base)
    except Exception:
        return Path(path.name)


def ensure_unique_file_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    i = 1
    while True:
        cand = parent / f"{stem}_{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1


def resolve_collision(path: Path, hash_hex: str) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    h8 = (hash_hex or "x")[:8]
    cand = parent / f"{stem}_{h8}{suffix}"
    if not cand.exists():
        return cand
    i = 1
    while True:
        cand = parent / f"{stem}_{h8}_{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1


def safe_move(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst = ensure_unique_file_path(dst)
    shutil.move(str(src), str(dst))


def link_or_copy(src: Path, dst: Path) -> None:
    try:
        os.link(src, dst)
    except Exception:
        shutil.copy2(src, dst)


def atomic_replace(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(src, dst)
    except OSError:
        # Windows: fallback для разных дисков (например C: -> E:).
        shutil.move(str(src), str(dst))


def truncate(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def format_subprocess_error(result: subprocess.CompletedProcess, max_len: int = 350) -> str:
    stderr = clean_text(result.stderr or "")
    stdout = clean_text(result.stdout or "")
    combined = stderr or stdout or "no stderr/stdout"
    return truncate(combined, max_len)


def build_source_db_name(source_dirs: list[Path]) -> str:
    if not source_dirs:
        source_dirs = [Path("source")]
    canonical: list[str] = []
    for src in source_dirs:
        try:
            canonical.append(str(src.resolve()))
        except Exception:
            canonical.append(str(src))
    joined = "||".join(sorted(canonical))
    if len(source_dirs) == 1:
        base_src = source_dirs[0]
        base = sanitize_component(base_src.name or "source", max_len=12).lower()
        base = re.sub(r"[^a-zA-Z0-9_-]+", "_", base).strip("_").lower() or "source"
    else:
        base = f"multi{len(source_dirs)}"
    h6 = xxhash.xxh64(joined.encode("utf-8", errors="ignore")).hexdigest()[:6]
    return f"ls_{base}_{h6}.db"


def parse_sources_input(values: list[str]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for raw in values:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        for part in re.split(r"[;\n]+", text):
            value = part.strip()
            if not value:
                continue
            p = Path(value)
            key = str(p).lower() if os.name == "nt" else str(p)
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return out


def parse_args() -> Config:
    p = argparse.ArgumentParser(
        description="Многопоточный конвейер сортировки библиотеки с LM Studio и XXH64."
    )
    p.add_argument("--sources", nargs="+", default=DEFAULT_SOURCE_DIRS)
    p.add_argument("--source", action="append", dest="legacy_source", help=argparse.SUPPRESS)
    p.add_argument("--target", default=DEFAULT_TARGET_DIR)
    p.add_argument("--dupes", default=DEFAULT_DUPES_DIR)
    p.add_argument("--nobook", default=DEFAULT_NOBOOK_DIR)
    p.add_argument("--temp", default=DEFAULT_TEMP_BASE)
    p.add_argument("--lm-url", default="http://127.0.0.1:1234/v1/chat/completions")
    p.add_argument("--lm-model", default="google/gemma-4-e4b")
    p.add_argument("--queue-size", type=int, default=DEFAULT_QUEUE_SIZE)
    p.add_argument("--unpack-workers", type=int, default=DEFAULT_UNPACK_WORKERS)
    p.add_argument("--detect-workers", type=int, default=DEFAULT_DETECT_WORKERS)
    p.add_argument("--tag-workers", type=int, default=DEFAULT_TAG_WORKERS)
    p.add_argument("--lm-workers", type=int, default=DEFAULT_LM_WORKERS)
    p.add_argument("--rename-workers", type=int, default=DEFAULT_RENAME_WORKERS)
    p.add_argument("--dedupe-workers", type=int, default=DEFAULT_DEDUPE_WORKERS)
    p.add_argument("--pack-workers", type=int, default=DEFAULT_PACK_WORKERS)
    p.add_argument("--max-parallel-archives", type=int, default=DEFAULT_MAX_PARALLEL_ARCHIVES)
    p.add_argument(
        "--keep-source",
        action="store_true",
        help="Не удалять исходные файлы после успешной упаковки",
    )
    p.add_argument(
        "--keep-temp-nobooks",
        action="store_true",
        help="Сохранять некнижные временные файлы из архивов в NOBOOK_DIR",
    )
    p.add_argument("--lm-timeout-sec", type=int, default=DEFAULT_LM_TIMEOUT_SEC)
    p.add_argument("--lm-input-chars", type=int, default=DEFAULT_LM_INPUT_CHARS)
    p.add_argument("--lm-max-output-tokens", type=int, default=DEFAULT_LM_MAX_OUTPUT_TOKENS)
    p.add_argument(
        "--persist-state",
        action="store_true",
        help="Сохранять служебную БД/логи после завершения (по умолчанию удаляются).",
    )
    args = p.parse_args()
    source_values: list[str] = []
    source_values.extend(args.sources or [])
    source_values.extend(args.legacy_source or [])
    source_dirs = parse_sources_input(source_values)
    if not source_dirs:
        source_dirs = [Path(p) for p in DEFAULT_SOURCE_DIRS]

    return Config(
        source_dirs=source_dirs,
        target_dir=Path(args.target),
        dupes_dir=Path(args.dupes),
        nobook_dir=Path(args.nobook),
        temp_base=Path(args.temp),
        lm_url=args.lm_url,
        lm_model=args.lm_model,
        queue_size=max(100, args.queue_size),
        unpack_workers=max(1, args.unpack_workers),
        detect_workers=max(1, args.detect_workers),
        tag_workers=max(1, args.tag_workers),
        lm_workers=max(1, args.lm_workers),
        rename_workers=max(1, args.rename_workers),
        dedupe_workers=max(1, args.dedupe_workers),
        pack_workers=max(1, args.pack_workers),
        max_parallel_archives=max(1, args.max_parallel_archives),
        delete_source_after_pack=not args.keep_source,
        keep_temp_nobooks=bool(args.keep_temp_nobooks),
        lm_timeout_sec=max(10, args.lm_timeout_sec),
        lm_input_chars=max(200, args.lm_input_chars),
        lm_max_output_tokens=max(40, args.lm_max_output_tokens),
        ephemeral_mode=not args.persist_state,
    )


def main() -> int:
    config = parse_args()
    sorter = LibrarySorter(config)
    return sorter.run()


if __name__ == "__main__":
    raise SystemExit(main())

