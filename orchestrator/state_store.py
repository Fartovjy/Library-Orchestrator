from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from pathlib import Path

from .models import (
    BatchProgress,
    BatchRun,
    BatchStatus,
    ContainerKind,
    ItemStatus,
    TaskStage,
    TaskStatus,
    WorkItem,
    utc_now,
)


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
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    requested_limit INTEGER NOT NULL,
                    selected_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL DEFAULT '',
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

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    claimed_by TEXT,
                    claimed_at TEXT,
                    completed_at TEXT,
                    confidence REAL,
                    message TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(batch_id, item_id, stage)
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
            self._ensure_column(connection, "items", "batch_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "items", "prepared_excerpt", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "tasks", "confidence", "REAL")

    def _ensure_column(self, connection: sqlite3.Connection, table_name: str, column_name: str, spec: str) -> None:
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name not in columns:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {spec}")

    def create_batch(self, requested_limit: int, selected_count: int) -> BatchRun:
        batch = BatchRun(
            batch_id=str(uuid.uuid4()),
            requested_limit=requested_limit,
            selected_count=selected_count,
        )
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO batches (
                    batch_id, requested_limit, selected_count, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.requested_limit,
                    batch.selected_count,
                    batch.status.value,
                    batch.created_at,
                    batch.updated_at,
                ),
            )
        return batch

    def get_active_batch(self) -> BatchRun | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM batches
                WHERE status = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (BatchStatus.ACTIVE.value,),
            ).fetchone()
        return self._row_to_batch(row) if row is not None else None

    def finalize_batch(self, batch_id: str) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE batches
                SET status = ?, updated_at = ?
                WHERE batch_id = ?
                """,
                (BatchStatus.COMPLETED.value, now, batch_id),
            )

    def batch_has_pending_work(self, batch_id: str) -> bool:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM tasks
                WHERE batch_id = ? AND status IN (?, ?)
                """,
                (batch_id, TaskStatus.PENDING.value, TaskStatus.CLAIMED.value),
            ).fetchone()
        return bool(row and int(row["count"]) > 0)

    def reset_claimed_tasks(self, batch_id: str) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE tasks
                SET status = ?, claimed_by = NULL, claimed_at = NULL, updated_at = ?
                WHERE batch_id = ? AND status = ?
                """,
                (TaskStatus.PENDING.value, now, batch_id, TaskStatus.CLAIMED.value),
            )

    def get_or_create_item(self, source_path: Path, container_kind: ContainerKind, batch_id: str = "") -> WorkItem:
        existing = self.get_item_by_source(source_path)
        if existing:
            if batch_id and existing.batch_id != batch_id:
                existing.batch_id = batch_id
                self.save_item(existing)
            return existing
        item = WorkItem(
            item_id=str(uuid.uuid4()),
            batch_id=batch_id,
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
        return self._row_to_item(row) if row is not None else None

    def get_item_by_id(self, item_id: str) -> WorkItem | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM items WHERE item_id = ?",
                (item_id,),
            ).fetchone()
        return self._row_to_item(row) if row is not None else None

    def list_terminal_sources(self) -> set[str]:
        terminal_statuses = (
            ItemStatus.PLACED.value,
            ItemStatus.DUPLICATE.value,
            ItemStatus.NON_BOOK.value,
            ItemStatus.MANUAL_REVIEW.value,
            ItemStatus.TRASH.value,
            ItemStatus.DAMAGED.value,
        )
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT source_path FROM items WHERE status IN (?, ?, ?, ?, ?, ?)",
                terminal_statuses,
            ).fetchall()
        return {row["source_path"] for row in rows}

    def save_item(self, item: WorkItem) -> None:
        item.updated_at = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO items (
                    item_id, batch_id, source_path, source_name, container_kind, status, author, title,
                    genre, confidence, source_hash, packed_hash, prepared_excerpt, unpack_dir, packed_path,
                    final_path, message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    batch_id=excluded.batch_id,
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
                    item.batch_id,
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

    def ensure_task(self, batch_id: str, item_id: str, stage: TaskStage, status: TaskStatus = TaskStatus.PENDING) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO tasks (
                    batch_id, item_id, stage, status, message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, '', ?, ?)
                ON CONFLICT(batch_id, item_id, stage) DO UPDATE SET
                    status = CASE
                        WHEN tasks.status = ? THEN tasks.status
                        ELSE excluded.status
                    END,
                    updated_at = CASE
                        WHEN tasks.status = ? THEN tasks.updated_at
                        ELSE excluded.updated_at
                    END
                """,
                (
                    batch_id,
                    item_id,
                    stage.value,
                    status.value,
                    now,
                    now,
                    TaskStatus.DONE.value,
                    TaskStatus.DONE.value,
                ),
            )

    def claim_next_task(self, batch_id: str, stage: TaskStage, worker_name: str) -> str | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT task_id, item_id
                FROM tasks
                WHERE batch_id = ? AND stage = ? AND status = ?
                ORDER BY created_at, task_id
                LIMIT 1
                """,
                (batch_id, stage.value, TaskStatus.PENDING.value),
            ).fetchone()
            if row is None:
                return None
            updated = connection.execute(
                """
                UPDATE tasks
                SET status = ?, claimed_by = ?, claimed_at = ?, updated_at = ?
                WHERE task_id = ? AND status = ?
                """,
                (
                    TaskStatus.CLAIMED.value,
                    worker_name,
                    utc_now(),
                    utc_now(),
                    row["task_id"],
                    TaskStatus.PENDING.value,
                ),
            )
            if updated.rowcount != 1:
                return None
        return str(row["item_id"])

    def complete_task(
        self,
        batch_id: str,
        item_id: str,
        stage: TaskStage,
        message: str = "",
        confidence: float | None = None,
    ) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE tasks
                SET status = ?, claimed_by = NULL, completed_at = ?, updated_at = ?, message = ?, confidence = ?
                WHERE batch_id = ? AND item_id = ? AND stage = ?
                """,
                (
                    TaskStatus.DONE.value,
                    now,
                    now,
                    message,
                    confidence,
                    batch_id,
                    item_id,
                    stage.value,
                ),
            )

    def release_task(self, batch_id: str, item_id: str, stage: TaskStage, message: str = "") -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE tasks
                SET status = ?, claimed_by = NULL, claimed_at = NULL, updated_at = ?, message = ?
                WHERE batch_id = ? AND item_id = ? AND stage = ?
                """,
                (
                    TaskStatus.PENDING.value,
                    now,
                    message,
                    batch_id,
                    item_id,
                    stage.value,
                ),
            )

    def batch_progress(self, batch_id: str) -> BatchProgress:
        with self._lock, self._connect() as connection:
            batch_row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            total_items = int(batch_row["selected_count"]) if batch_row is not None else 0

            terminal_row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM items
                WHERE batch_id = ? AND status IN (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    ItemStatus.PLACED.value,
                    ItemStatus.DUPLICATE.value,
                    ItemStatus.NON_BOOK.value,
                    ItemStatus.MANUAL_REVIEW.value,
                    ItemStatus.TRASH.value,
                    ItemStatus.DAMAGED.value,
                ),
            ).fetchone()
            processed_items = int(terminal_row["count"]) if terminal_row is not None else 0

            stage_totals_rows = connection.execute(
                """
                SELECT stage, COUNT(*) AS count
                FROM tasks
                WHERE batch_id = ?
                GROUP BY stage
                """,
                (batch_id,),
            ).fetchall()
            stage_done_rows = connection.execute(
                """
                SELECT stage, COUNT(*) AS count
                FROM tasks
                WHERE batch_id = ? AND status = ?
                GROUP BY stage
                """,
                (batch_id, TaskStatus.DONE.value),
            ).fetchall()
            recognition_rows = connection.execute(
                """
                SELECT stage, AVG(confidence) AS avg_confidence
                FROM tasks
                WHERE batch_id = ?
                  AND status = ?
                  AND confidence IS NOT NULL
                GROUP BY stage
                """,
                (batch_id, TaskStatus.DONE.value),
            ).fetchall()
            status_rows = connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM items
                WHERE batch_id = ?
                GROUP BY status
                """,
                (batch_id,),
            ).fetchall()

        return BatchProgress(
            batch_id=batch_id,
            total_items=total_items,
            processed_items=processed_items,
            stage_totals={row["stage"]: int(row["count"]) for row in stage_totals_rows},
            stage_done={row["stage"]: int(row["count"]) for row in stage_done_rows},
            status_counts={row["status"]: int(row["count"]) for row in status_rows},
            recognition_avgs={
                row["stage"]: float(row["avg_confidence"])
                for row in recognition_rows
                if row["avg_confidence"] is not None
            },
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
                    item_id = excluded.item_id,
                    final_path = excluded.final_path
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

    def _row_to_batch(self, row: sqlite3.Row | None) -> BatchRun | None:
        if row is None:
            return None
        return BatchRun(
            batch_id=row["batch_id"],
            requested_limit=int(row["requested_limit"]),
            selected_count=int(row["selected_count"]),
            status=BatchStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_item(self, row: sqlite3.Row) -> WorkItem:
        return WorkItem(
            item_id=row["item_id"],
            batch_id=row["batch_id"] or "",
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
