from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from pathlib import Path

from .models import ContainerKind, ItemStatus, WorkItem, utc_now


class StateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT PRIMARY KEY,
                    source_path TEXT NOT NULL UNIQUE,
                    source_name TEXT NOT NULL,
                    container_kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    author TEXT NOT NULL,
                    title TEXT NOT NULL,
                    genre TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    source_hash TEXT NOT NULL,
                    packed_hash TEXT NOT NULL,
                    prepared_excerpt TEXT NOT NULL DEFAULT '',
                    unpack_dir TEXT,
                    packed_path TEXT,
                    final_path TEXT,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS known_hashes (
                    content_hash TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    final_path TEXT
                );
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(items)").fetchall()
            }
            if "prepared_excerpt" not in columns:
                connection.execute(
                    "ALTER TABLE items ADD COLUMN prepared_excerpt TEXT NOT NULL DEFAULT ''"
                )

    def get_or_create_item(self, source_path: Path, container_kind: ContainerKind) -> WorkItem:
        existing = self.get_item_by_source(source_path)
        if existing:
            return existing
        item = WorkItem(
            item_id=str(uuid.uuid4()),
            source_path=source_path,
            source_name=source_path.name,
            container_kind=container_kind,
        )
        self.save_item(item)
        self.add_event(item.item_id, "discover", "Source object discovered.")
        return item

    def get_item_by_source(self, source_path: Path) -> WorkItem | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM items WHERE source_path = ?",
                (str(source_path),),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_item(row)

    def list_existing_sources(self) -> set[str]:
        with self._lock, self._connect() as connection:
            rows = connection.execute("SELECT source_path FROM items").fetchall()
        return {row["source_path"] for row in rows}

    def list_terminal_sources(self) -> set[str]:
        terminal_statuses = (
            ItemStatus.PLACED.value,
            ItemStatus.DUPLICATE.value,
            ItemStatus.MANUAL_REVIEW.value,
            ItemStatus.TRASH.value,
            ItemStatus.DAMAGED.value,
        )
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT source_path FROM items WHERE status IN (?, ?, ?, ?, ?)",
                terminal_statuses,
            ).fetchall()
        return {row["source_path"] for row in rows}

    def get_item_by_id(self, item_id: str) -> WorkItem | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM items WHERE item_id = ?",
                (item_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_item(row)

    def list_ready_for_heavy_item_ids(self) -> list[str]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT item_id
                FROM items
                WHERE status = ?
                ORDER BY updated_at, created_at, item_id
                """,
                (ItemStatus.PREPARED.value,),
            ).fetchall()
        return [row["item_id"] for row in rows]

    def save_item(self, item: WorkItem) -> None:
        item.updated_at = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO items (
                    item_id, source_path, source_name, container_kind, status, author, title,
                    genre, confidence, source_hash, packed_hash, prepared_excerpt, unpack_dir, packed_path,
                    final_path, message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    status=excluded.status,
                    author=excluded.author,
                    title=excluded.title,
                    genre=excluded.genre,
                    confidence=excluded.confidence,
                    source_hash=excluded.source_hash,
                    packed_hash=excluded.packed_hash,
                    prepared_excerpt=excluded.prepared_excerpt,
                    unpack_dir=excluded.unpack_dir,
                    packed_path=excluded.packed_path,
                    final_path=excluded.final_path,
                    message=excluded.message,
                    updated_at=excluded.updated_at
                """,
                (
                    item.item_id,
                    str(item.source_path),
                    item.source_name,
                    item.container_kind.value,
                    item.status.value,
                    item.author,
                    item.title,
                    item.genre,
                    item.confidence,
                    item.source_hash,
                    item.packed_hash,
                    item.prepared_excerpt,
                    str(item.unpack_dir) if item.unpack_dir else None,
                    str(item.packed_path) if item.packed_path else None,
                    str(item.final_path) if item.final_path else None,
                    item.message,
                    item.created_at,
                    item.updated_at,
                ),
            )

    def add_event(self, item_id: str, stage: str, message: str, payload: dict | None = None) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO events (item_id, stage, message, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    stage,
                    message,
                    json.dumps(payload or {}, ensure_ascii=False),
                    utc_now(),
                ),
            )

    def register_hash(self, content_hash: str, item_id: str, final_path: Path | None) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO known_hashes (content_hash, item_id, final_path)
                VALUES (?, ?, ?)
                ON CONFLICT(content_hash) DO UPDATE SET
                    item_id=excluded.item_id,
                    final_path=excluded.final_path
                """,
                (content_hash, item_id, str(final_path) if final_path else None),
            )

    def find_duplicate(self, content_hash: str) -> sqlite3.Row | None:
        with self._lock, self._connect() as connection:
            return connection.execute(
                "SELECT * FROM known_hashes WHERE content_hash = ?",
                (content_hash,),
            ).fetchone()

    def status_counts(self) -> dict[str, int]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM items GROUP BY status"
            ).fetchall()
        return {row["status"]: int(row["count"]) for row in rows}

    def _row_to_item(self, row: sqlite3.Row) -> WorkItem:
        return WorkItem(
            item_id=row["item_id"],
            source_path=Path(row["source_path"]),
            source_name=row["source_name"],
            container_kind=ContainerKind(row["container_kind"]),
            status=ItemStatus(row["status"]),
            author=row["author"],
            title=row["title"],
            genre=row["genre"],
            confidence=float(row["confidence"]),
            source_hash=row["source_hash"],
            packed_hash=row["packed_hash"],
            prepared_excerpt=row["prepared_excerpt"] or "",
            unpack_dir=Path(row["unpack_dir"]) if row["unpack_dir"] else None,
            packed_path=Path(row["packed_path"]) if row["packed_path"] else None,
            final_path=Path(row["final_path"]) if row["final_path"] else None,
            message=row["message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
