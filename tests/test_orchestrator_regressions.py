from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from orchestrator.config import AppConfig, BehaviorConfig, LimitsConfig, LmStudioConfig, PathsConfig
from orchestrator.models import ContainerKind, ItemStatus, TaskStage
from orchestrator.orchestrator import LibraryOrchestrator


def _build_orchestrator(tmp_path: Path) -> LibraryOrchestrator:
    source_root = tmp_path / "source"
    output_root = tmp_path / "output"
    workspace_root = tmp_path / "workspace"
    runtime_root = tmp_path / "runtime"
    source_root.mkdir(parents=True, exist_ok=True)

    paths = PathsConfig(
        source_root=source_root,
        output_root=output_root,
        workspace_root=workspace_root,
        library_root=output_root / "Library",
        duplicates_root=output_root / "_Duplicates",
        non_book_root=output_root / "_Non_Books",
        manual_review_root=output_root / "_Manual_Review",
        trash_root=output_root / "_Trash",
        damaged_root=output_root / "_Damaged",
        failed_root=output_root / "_Failed",
        state_db=runtime_root / "state" / "orchestrator.db",
        logs_root=runtime_root / "logs",
        stop_file=runtime_root / "STOP",
        pause_file=runtime_root / "PAUSE",
        run_lock_file=runtime_root / "RUNNING.lock",
    )
    config = AppConfig(
        paths=paths,
        lmstudio=LmStudioConfig(),
        limits=LimitsConfig(),
        behavior=BehaviorConfig(),
    )
    config.paths.ensure_directories()
    return LibraryOrchestrator(config)


def test_repair_invalid_packed_item_drops_stale_duplicate_check(tmp_path: Path) -> None:
    orchestrator = _build_orchestrator(tmp_path)
    source = orchestrator.config.paths.source_root / "book.txt"
    source.write_text("book text", encoding="utf-8")

    batch = orchestrator.state_store.create_batch(requested_limit=1, selected_count=1)
    item = orchestrator.state_store.get_or_create_item(source, ContainerKind.FILE, batch.batch_id)
    item.status = ItemStatus.PACKED
    item.source_hash = "source-hash"
    item.packed_hash = "packed-hash"
    orchestrator.state_store.save_item(item)

    for stage in (
        TaskStage.PREPARE,
        TaskStage.DUPLICATE_CHECK,
        TaskStage.ARCHIVARIUS,
        TaskStage.EXPERT,
        TaskStage.PACK,
        TaskStage.PLACEMENT,
    ):
        orchestrator.state_store.ensure_task(batch.batch_id, item.item_id, stage)
        orchestrator.state_store.complete_task(batch.batch_id, item.item_id, stage, message="done")

    repaired = orchestrator._repair_invalid_packed_item(batch.batch_id, item)
    assert repaired is True

    with sqlite3.connect(orchestrator.config.paths.state_db) as connection:
        rows = connection.execute(
            """
            SELECT stage, status
            FROM tasks
            WHERE batch_id = ? AND item_id = ?
            """,
            (batch.batch_id, item.item_id),
        ).fetchall()
    tasks = {(row[0], row[1]) for row in rows}
    assert (TaskStage.DUPLICATE_CHECK.value, "done") not in tasks
    assert (TaskStage.PREPARE.value, "pending") in tasks


def test_run_lock_reclaims_stale_file(tmp_path: Path) -> None:
    orchestrator = _build_orchestrator(tmp_path)
    lock_path = orchestrator.config.paths.run_lock_file
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps({"pid": 99999999}), encoding="utf-8")

    orchestrator._acquire_run_lock()
    payload = json.loads(lock_path.read_text(encoding="utf-8"))
    assert payload["pid"] == os.getpid()

    orchestrator._release_run_lock()
    assert not lock_path.exists()


def test_run_lock_rejects_active_foreign_pid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = _build_orchestrator(tmp_path)
    lock_path = orchestrator.config.paths.run_lock_file
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps({"pid": 424242}), encoding="utf-8")

    monkeypatch.setattr(
        orchestrator,
        "_pid_is_running",
        lambda pid: pid == 424242,
    )
    with pytest.raises(RuntimeError, match="already active"):
        orchestrator._acquire_run_lock()


def test_discovery_does_not_walk_deep_tree_for_directory_sorting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orchestrator = _build_orchestrator(tmp_path)
    source_root = orchestrator.config.paths.source_root
    root_file = source_root / "small.txt"
    root_file.write_text("x", encoding="utf-8")

    deep_file = source_root / "big" / "nested" / "very" / "deep" / "book.txt"
    deep_file.parent.mkdir(parents=True, exist_ok=True)
    deep_file.write_text("deep content", encoding="utf-8")

    original_file_size = orchestrator._file_size
    seen: list[Path] = []

    def track_file_size(path: Path) -> int:
        seen.append(path)
        return original_file_size(path)

    monkeypatch.setattr(orchestrator, "_file_size", track_file_size)

    sources = orchestrator._discover_sources(limit=1)
    assert sources == [root_file]
    assert deep_file not in seen
