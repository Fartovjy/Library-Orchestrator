from __future__ import annotations

import json
import os
import shutil
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .agents import (
    AgentContext,
    ArchivariusAgent,
    DuplicateCheckAgent,
    ExpertAgent,
    PackAgent,
    PlacementAgent,
    RepairAgent,
    SplitterAgent,
    UnpackAgent,
)
from .archive_adapters import (
    classify_file_role,
    collect_excerpt,
    compute_sha256,
    detect_container_kind,
    EMPTY_ZIP_SHA256,
    is_valid_packed_archive,
    should_unpack_with_agent,
    stage_source,
)
from .config import AppConfig
from .dashboard import TerminalDashboard
from .hotkeys import RuntimeHotkeyWatcher
from .lmstudio import LmStudioClient
from .models import BatchRun, ItemStatus, TaskStage
from .queues import StageQueues
from .resource_monitor import ResourceMonitor
from .state_store import StateStore


TERMINAL_STATUSES = {
    ItemStatus.PLACED,
    ItemStatus.DUPLICATE,
    ItemStatus.NON_BOOK,
    ItemStatus.SPLIT,
    ItemStatus.MANUAL_REVIEW,
    ItemStatus.TRASH,
    ItemStatus.DAMAGED,
    ItemStatus.FAILED,
}


class LibraryOrchestrator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state_store = StateStore(config.paths.state_db)
        self.resource_monitor = ResourceMonitor(
            config.paths.source_root,
            config.limits.hdd_busy_threshold_percent,
        )
        self.queues = StageQueues()
        self.context = AgentContext(
            config=config,
            state_store=self.state_store,
            resource_monitor=self.resource_monitor,
            lmstudio=LmStudioClient(config),
            queues=self.queues,
        )
        self.dashboard = TerminalDashboard()
        self.hotkeys = RuntimeHotkeyWatcher()
        self.dashboard.reset(hotkey_hint=self.hotkeys.hint(), render=False)
        self.unpack_agent = UnpackAgent()
        self.archivarius_agent = ArchivariusAgent()
        self.duplicate_check_agent = DuplicateCheckAgent()
        self.expert_agent = ExpertAgent()
        self.pack_agent = PackAgent()
        self.placement_agent = PlacementAgent()
        self.repair_agent = RepairAgent()
        self.splitter_agent = SplitterAgent()
        self._unpack_slots = threading.Semaphore(max(1, self.config.limits.max_parallel_unpack))
        self._pack_slots = threading.Semaphore(max(1, self.config.limits.max_parallel_pack))
        self._placement_lock = threading.Lock()
        self._dashboard_lock = threading.RLock()
        self._stop_event = threading.Event()

    def run(self, limit: int | None = None) -> dict[str, int]:
        self.dashboard.reset(
            hotkey_hint=self.hotkeys.hint(),
            message="Starting orchestrator run.",
        )
        self._acquire_run_lock()
        try:
            return self._run_with_lock(limit)
        finally:
            self._release_run_lock()

    def _run_with_lock(self, limit: int | None = None) -> dict[str, int]:
        if self._stop_requested():
            self.clear_stop_file()
            self.dashboard.set_current(
                self.config.paths.source_root,
                "resume",
                "Cleared stale stop request from previous run.",
            )

        batch = self._prepare_batch(limit)
        self.dashboard.set_total(batch.selected_count)
        self._sync_dashboard(batch.batch_id, "Batch is ready.")
        if batch.selected_count == 0:
            self.state_store.finalize_batch(batch.batch_id)
            return self.state_store.status_counts()

        self._stop_event.clear()
        futures = []
        total_workers = (
            1
            + max(1, self.config.limits.max_parallel_unpack)
            + max(1, self.config.limits.max_parallel_items)
            + max(1, self.config.limits.max_parallel_heavy_agents)
            + max(1, self.config.limits.max_parallel_pack)
            + 1
        )
        with ThreadPoolExecutor(max_workers=total_workers, thread_name_prefix="stage") as executor:
            futures.append(
                executor.submit(
                    self._run_stage_loop,
                    batch.batch_id,
                    "discovery-1",
                    [TaskStage.DISCOVERY],
                )
            )
            for index in range(max(1, self.config.limits.max_parallel_unpack)):
                futures.append(
                    executor.submit(
                        self._run_stage_loop,
                        batch.batch_id,
                        f"unpack-{index + 1}",
                        [TaskStage.UNPACK],
                    )
                )
            for index in range(max(1, self.config.limits.max_parallel_items)):
                futures.append(
                    executor.submit(
                        self._run_stage_loop,
                        batch.batch_id,
                        f"light-{index + 1}",
                        [TaskStage.SPLITTER, TaskStage.PREPARE, TaskStage.DUPLICATE_CHECK, TaskStage.ARCHIVARIUS],
                    )
                )
            for index in range(max(1, self.config.limits.max_parallel_heavy_agents)):
                futures.append(
                    executor.submit(
                        self._run_stage_loop,
                        batch.batch_id,
                        f"expert-{index + 1}",
                        [TaskStage.EXPERT],
                    )
                )
            for index in range(max(1, self.config.limits.max_parallel_pack)):
                futures.append(
                    executor.submit(
                        self._run_stage_loop,
                        batch.batch_id,
                        f"pack-{index + 1}",
                        [TaskStage.PACK],
                    )
                )
            futures.append(
                executor.submit(
                    self._run_stage_loop,
                    batch.batch_id,
                    "placement-1",
                    [TaskStage.PLACEMENT],
                )
            )

            while any(not future.done() for future in futures):
                self._poll_runtime_action()
                self._sync_dashboard(batch.batch_id)
                if self._stop_requested():
                    self._stop_event.set()
                    break
                if self._pause_requested():
                    self.dashboard.set_current(
                        self.config.paths.source_root,
                        "paused",
                        "Dispatch paused. Press Ctrl+X / Esc / Q again or run resume.",
                    )
                    time.sleep(0.2)
                    continue
                if not self.state_store.batch_has_pending_work(batch.batch_id):
                    self.state_store.finalize_batch(batch.batch_id)
                    self._stop_event.set()
                    break
                time.sleep(0.2)

            for future in futures:
                future.result()

        self._sync_dashboard(batch.batch_id, "Batch completed.")
        return self.state_store.batch_status_counts(batch.batch_id)

    def _prepare_batch(self, limit: int | None) -> BatchRun:
        active_batch = self.state_store.get_active_batch()
        if active_batch is not None and self.state_store.batch_has_pending_work(active_batch.batch_id):
            self.state_store.reset_claimed_tasks(active_batch.batch_id)
            self._repair_invalid_packs(active_batch.batch_id)
            return active_batch
        if active_batch is not None:
            self.state_store.finalize_batch(active_batch.batch_id)

        requested_limit = limit if limit is not None else self.config.limits.max_items_per_run
        sources = self._discover_sources(limit=requested_limit)
        batch = self.state_store.create_batch(requested_limit=requested_limit, selected_count=len(sources))
        self._seed_batch(batch.batch_id, sources)
        return batch

    def _seed_batch(self, batch_id: str, sources: list[Path]) -> None:
        for source_path in sources:
            item = self.state_store.get_or_create_item(
                source_path,
                detect_container_kind(source_path),
                batch_id=batch_id,
            )
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.DISCOVERY)

    def _run_stage_loop(self, batch_id: str, worker_name: str, stages: list[TaskStage]) -> None:
        while not self._stop_event.is_set():
            self._poll_runtime_action()
            if self._stop_requested():
                self._stop_event.set()
                return
            if self._pause_requested():
                time.sleep(0.2)
                continue

            claimed = self._claim_from_stages(batch_id, worker_name, stages)
            if claimed is None:
                if not self.state_store.batch_has_pending_work(batch_id):
                    return
                time.sleep(0.2)
                continue

            stage, item_id = claimed
            item = self.state_store.get_item_by_id(item_id)
            if item is None:
                self.state_store.complete_task(batch_id, item_id, stage, "Item disappeared from state store.")
                self._sync_dashboard(batch_id, "Item disappeared from state store.")
                continue

            try:
                if stage in {TaskStage.UNPACK, TaskStage.PACK}:
                    self._throttle_for_disk(item.source_path)

                self.dashboard.begin_agent(item.source_path, stage.value, f"{stage.value} started.")
                message, confidence = self._process_stage(batch_id, stage, item)
                self.state_store.complete_task(batch_id, item_id, stage, message=message, confidence=confidence)
                self.dashboard.end_agent(stage.value, message)
                self._sync_dashboard(batch_id, message)
            except Exception as error:  # pragma: no cover
                self.dashboard.end_agent(stage.value, str(error))
                failure_root, failure_status = self._failure_target_for_error(stage, str(error))
                self._route_to_special(item, failure_root, failure_status, str(error))
                self.state_store.complete_task(batch_id, item_id, stage, message=item.message)
                self._sync_dashboard(batch_id, item.message)

    def _claim_from_stages(
        self,
        batch_id: str,
        worker_name: str,
        stages: list[TaskStage],
    ) -> tuple[TaskStage, str] | None:
        for stage in stages:
            item_id = self.state_store.claim_next_task(batch_id, stage, worker_name)
            if item_id is not None:
                return stage, item_id
        return None

    def _process_stage(self, batch_id: str, stage: TaskStage, item) -> tuple[str, float | None]:
        if stage == TaskStage.DISCOVERY:
            return self._process_discovery_task(batch_id, item)
        if stage == TaskStage.UNPACK:
            return self._process_unpack_task(batch_id, item)
        if stage == TaskStage.SPLITTER:
            return self._process_splitter_task(batch_id, item)
        if stage == TaskStage.PREPARE:
            return self._process_prepare_task(batch_id, item)
        if stage == TaskStage.DUPLICATE_CHECK:
            return self._process_duplicate_check_task(batch_id, item)
        if stage == TaskStage.ARCHIVARIUS:
            return self._process_archivarius_task(batch_id, item)
        if stage == TaskStage.EXPERT:
            return self._process_expert_task(batch_id, item)
        if stage == TaskStage.PACK:
            return self._process_pack_task(batch_id, item)
        if stage == TaskStage.PLACEMENT:
            return self._process_placement_task(batch_id, item)
        raise RuntimeError(f"Unsupported stage: {stage.value}")

    def _process_discovery_task(self, batch_id: str, item) -> tuple[str, float | None]:
        if item.status in TERMINAL_STATUSES:
            return f"Skipped already terminal item: {item.source_name}", None

        file_role = classify_file_role(item.source_path)
        if item.source_path.suffix.lower() in set(self.config.behavior.trash_extensions) or file_role == "trash":
            self._route_to_special(
                item,
                self.config.paths.trash_root,
                ItemStatus.TRASH,
                "Detected as trash by extension or file signature.",
            )
            return item.message, None
        if file_role == "non_book":
            self._route_to_special(
                item,
                self.config.paths.non_book_root,
                ItemStatus.NON_BOOK,
                "Detected as non-book source before classification.",
            )
            return item.message, None
        if file_role == "unknown" and item.source_path.is_file():
            self._route_to_special(
                item,
                self.config.paths.manual_review_root,
                ItemStatus.MANUAL_REVIEW,
                "Unknown file type by extension and signature.",
            )
            return item.message, None

        item.source_hash = compute_sha256(item.source_path)
        item.batch_id = batch_id
        item.message = "Discovery complete. Planned downstream work."
        self.state_store.save_item(item)
        self.state_store.add_event(
            item.item_id,
            "discovery",
            item.message,
            payload={"container_kind": item.container_kind.value, "source_hash": item.source_hash},
        )
        if should_unpack_with_agent(item.container_kind):
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.UNPACK)
        elif item.source_path.is_dir():
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.SPLITTER)
        else:
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PREPARE)
        return item.message, None

    def _process_unpack_task(self, batch_id: str, item) -> tuple[str, float | None]:
        with self._unpack_slots:
            item = self.unpack_agent.run(self.context, item)
        self.state_store.save_item(item)
        if item.status in TERMINAL_STATUSES:
            return item.message, None
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.SPLITTER)
        return item.message, None

    def _process_splitter_task(self, batch_id: str, item) -> tuple[str, float | None]:
        if item.unpack_dir is None or not item.unpack_dir.exists():
            item.unpack_dir, nested_count = stage_source(
                item.source_path,
                self.context.workspace_root / item.item_id,
                max_nested_depth=self.config.limits.max_nested_archive_depth,
            )
            self.state_store.add_event(
                item.item_id,
                "splitter_stage",
                "Source staged for splitter analysis.",
                payload={
                    "unpack_dir": str(item.unpack_dir),
                    "nested_archives_expanded": nested_count,
                },
            )

        item, child_items = self.splitter_agent.run(self.context, item)
        if not child_items:
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PREPARE)
            return item.message, None

        for child_item in child_items:
            self._schedule_child_item(batch_id, child_item)
        return item.message, None

    def _process_prepare_task(self, batch_id: str, item) -> tuple[str, float | None]:
        if item.unpack_dir and item.unpack_dir.exists():
            excerpt = collect_excerpt(item.unpack_dir, self.config.lmstudio.fast_excerpt_words)
        else:
            item.unpack_dir, nested_count = stage_source(
                item.source_path,
                self.context.workspace_root / item.item_id,
                max_nested_depth=self.config.limits.max_nested_archive_depth,
            )
            excerpt = collect_excerpt(item.unpack_dir, self.config.lmstudio.fast_excerpt_words)
            self.state_store.add_event(
                item.item_id,
                "prepare",
                "Source staged into workspace without archive unpack.",
                payload={
                    "unpack_dir": str(item.unpack_dir),
                    "nested_archives_expanded": nested_count,
                },
            )

        item.status = ItemStatus.PREPARED
        item.message = "Prepared for fast classification."
        self.state_store.save_item(item)
        self.state_store.add_event(
            item.item_id,
            "prepare_ready",
            item.message,
            payload={
                "unpack_dir": str(item.unpack_dir) if item.unpack_dir else "",
                "excerpt_words": len(excerpt.split()),
            },
        )
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.DUPLICATE_CHECK)
        return item.message, None

    def _process_duplicate_check_task(self, batch_id: str, item) -> tuple[str, float | None]:
        duplicate = self.duplicate_check_agent.run(self.context, item)
        if duplicate is not None:
            duplicate_label = duplicate["item_id"]
            if duplicate["final_path"]:
                duplicate_label = f"{duplicate_label} ({duplicate['final_path']})"
            self._route_to_special(
                item,
                self.config.paths.duplicates_root,
                ItemStatus.DUPLICATE,
                f"Early duplicate of {duplicate_label}",
            )
            return item.message, None
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.ARCHIVARIUS)
        return "No early duplicate found.", None

    def _process_archivarius_task(self, batch_id: str, item) -> tuple[str, float | None]:
        item, needs_deep = self.archivarius_agent.run(self.context, item)
        if needs_deep:
            item.message = "Queued for deep classification."
            self.state_store.save_item(item)
            self.state_store.add_event(
                item.item_id,
                "queue_deep",
                item.message,
                payload={"confidence": item.confidence},
            )
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.EXPERT)
        else:
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PACK)
        return item.message, item.confidence

    def _process_expert_task(self, batch_id: str, item) -> tuple[str, float | None]:
        item = self.expert_agent.run(self.context, item)
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PACK)
        return item.message, item.confidence

    def _process_pack_task(self, batch_id: str, item) -> tuple[str, float | None]:
        item = self._ensure_workspace_ready_for_pack(item)
        with self._pack_slots:
            item = self.pack_agent.run(self.context, item)
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PLACEMENT)
        return item.message, None

    def _process_placement_task(self, batch_id: str, item) -> tuple[str, float | None]:
        item = self._ensure_pack_ready_for_placement(item)
        with self._placement_lock:
            item = self.placement_agent.run(self.context, item)
            if self._should_remove_source_after_success():
                self._remove_source_path(item.source_path)
        if self.config.behavior.cleanup_workspace and item.unpack_dir:
            shutil.rmtree(item.unpack_dir.parent, ignore_errors=True)
        self._cleanup_root_workspace_if_complete(item.root_item_id)
        return item.message, None

    def _sync_dashboard(self, batch_id: str, message: str = "") -> None:
        with self._dashboard_lock:
            progress = self.state_store.batch_progress(batch_id)
            self.dashboard.sync_progress(
                total_items=progress.total_items,
                processed_items=progress.processed_items,
                stage_totals=progress.stage_totals,
                stage_done=progress.stage_done,
                status_counts=progress.status_counts,
                recognition_avgs=progress.recognition_avgs,
                message=message,
            )

    def _throttle_for_disk(self, source_path: Path) -> None:
        while self.resource_monitor.should_throttle() and not self._stop_event.is_set():
            self.dashboard.set_current(source_path, "throttle", "Waiting for disk load to drop.")
            time.sleep(self.config.limits.sleep_if_busy_seconds)
            self._poll_runtime_action()
            if self._stop_requested():
                self._stop_event.set()
                return

    def _discover_sources(self, limit: int | None = None) -> list[Path]:
        existing = self.state_store.list_terminal_sources()
        project_root = Path(__file__).resolve().parents[1]
        source_root = self.config.paths.source_root.resolve()
        skip_roots = {
            self.config.paths.workspace_root.resolve(),
            self.config.paths.output_root.resolve(),
            self.config.paths.library_root.resolve(),
            self.config.paths.duplicates_root.resolve(),
            self.config.paths.non_book_root.resolve(),
            self.config.paths.manual_review_root.resolve(),
            self.config.paths.trash_root.resolve(),
            self.config.paths.damaged_root.resolve(),
            self.config.paths.failed_root.resolve(),
            self.config.paths.logs_root.resolve(),
            self.config.paths.state_db.parent.resolve(),
        }
        if source_root != project_root and project_root not in source_root.parents:
            skip_roots.add(project_root)
        sources: list[Path] = []
        queue: deque[Path] = deque([self.config.paths.source_root])
        while queue:
            if limit is not None and len(sources) >= limit:
                break
            current_dir = queue.popleft()
            try:
                children = list(current_dir.iterdir())
            except OSError:
                continue

            eligible_files: list[tuple[int, Path]] = []
            leaf_dirs: list[tuple[int, Path]] = []
            branch_dirs: list[tuple[int, Path]] = []
            for child in children:
                resolved = child.resolve()
                if self._is_skipped(resolved, skip_roots):
                    continue
                if str(child) in existing:
                    continue
                if child.is_dir():
                    if self._directory_has_subdirs(child, skip_roots):
                        branch_dirs.append((self._directory_size(child), child))
                    else:
                        leaf_dirs.append((self._directory_size(child), child))
                else:
                    eligible_files.append((self._file_size(child), child))

            for _, path in sorted(eligible_files, key=lambda entry: (entry[0], entry[1].name.lower())):
                sources.append(path)
                if limit is not None and len(sources) >= limit:
                    return sources
            for _, path in sorted(leaf_dirs, key=lambda entry: (entry[0], entry[1].name.lower())):
                sources.append(path)
                if limit is not None and len(sources) >= limit:
                    return sources
            for _, path in sorted(branch_dirs, key=lambda entry: (entry[0], entry[1].name.lower())):
                queue.append(path)
        return sources

    def _route_to_special(self, item, root: Path, status: ItemStatus, message: str) -> None:
        root.mkdir(parents=True, exist_ok=True)
        target = self._unique_target_path(root / item.source_path.name)
        if item.source_path.exists():
            if self._should_move_source_for_special_route():
                shutil.move(str(item.source_path), target)
            elif item.source_path.is_dir():
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
                shutil.copytree(item.source_path, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item.source_path, target)
        item.status = status
        item.final_path = target
        item.message = message
        self.state_store.save_item(item)
        self.state_store.add_event(item.item_id, "route", message, payload={"final_path": str(target)})
        if self.config.behavior.cleanup_workspace and item.unpack_dir:
            shutil.rmtree(item.unpack_dir.parent, ignore_errors=True)
        self._cleanup_root_workspace_if_complete(item.root_item_id)

    def create_stop_file(self) -> Path:
        stop_file = self.config.paths.stop_file
        stop_file.parent.mkdir(parents=True, exist_ok=True)
        stop_file.write_text("stop\n", encoding="utf-8")
        return stop_file

    def create_pause_file(self) -> Path:
        pause_file = self.config.paths.pause_file
        pause_file.parent.mkdir(parents=True, exist_ok=True)
        pause_file.write_text("pause\n", encoding="utf-8")
        return pause_file

    def clear_stop_file(self) -> None:
        if self.config.paths.stop_file.exists():
            self.config.paths.stop_file.unlink()

    def clear_pause_file(self) -> None:
        if self.config.paths.pause_file.exists():
            self.config.paths.pause_file.unlink()

    def repair_database(self) -> dict[str, int]:
        self.dashboard.reset(
            hotkey_hint=self.hotkeys.hint(),
            message="Starting database repair.",
        )
        summary = self.repair_agent.run(self.context)
        active_batch = self.state_store.get_active_batch()
        if active_batch is None:
            return summary.as_dict()
        requeued = 0
        for item in self.state_store.list_items(
            batch_id=active_batch.batch_id,
            statuses=(ItemStatus.FAILED,),
        ):
            if item.message != "Cannot pack empty workspace." or not item.source_path.exists():
                continue
            item.status = ItemStatus.DISCOVERED
            item.unpack_dir = None
            item.packed_path = None
            item.packed_hash = ""
            item.final_path = None
            item.message = "Requeued by repair command after pack workspace recovery fix."
            self.state_store.save_item(item)
            self.state_store.delete_all_tasks_for_item(active_batch.batch_id, item.item_id)
            self.state_store.ensure_task(active_batch.batch_id, item.item_id, TaskStage.DISCOVERY)
            self.state_store.add_event(item.item_id, "repair", item.message)
            requeued += 1
        result = summary.as_dict()
        result["requeued_failed"] += requeued
        self._sync_dashboard(active_batch.batch_id, "Repair completed.")
        return result

    def _stop_requested(self) -> bool:
        return self.config.paths.stop_file.exists()

    def _pause_requested(self) -> bool:
        return self.config.paths.pause_file.exists()

    def _poll_runtime_action(self) -> None:
        action = self.hotkeys.poll_action()
        if action is None:
            return
        current_item = self.dashboard.state.current_item
        target = self.config.paths.source_root if current_item == "-" else Path(current_item)
        if action.action == "pause_toggle":
            if self._pause_requested():
                self.clear_pause_file()
                self.dashboard.set_current(target, "resume_requested", f"Resuming by {action.label}.")
            else:
                self.create_pause_file()
                self.dashboard.set_current(target, "paused", f"Pause requested by {action.label}.")
            return
        if action.action == "stop":
            self.create_stop_file()
            self.dashboard.set_current(target, "stop_requested", f"Safe stop requested by {action.label}.")

    def _is_skipped(self, path: Path, skip_roots: set[Path]) -> bool:
        for skip_root in skip_roots:
            if path == skip_root or skip_root in path.parents:
                return True
        return False

    def _directory_has_subdirs(self, path: Path, skip_roots: set[Path]) -> bool:
        try:
            for child in path.iterdir():
                resolved = child.resolve()
                if self._is_skipped(resolved, skip_roots):
                    continue
                if child.is_dir():
                    return True
        except OSError:
            return False
        return False

    def _file_size(self, path: Path) -> int:
        try:
            return path.stat().st_size
        except OSError:
            return 0

    def _directory_size(self, path: Path) -> int:
        total = 0
        try:
            for child in path.rglob("*"):
                if child.is_file():
                    total += self._file_size(child)
        except OSError:
            return total
        return total

    def _remove_source_path(self, path: Path) -> None:
        if not path.exists():
            return
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)

    def _unique_target_path(self, target_path: Path) -> Path:
        if not target_path.exists():
            return target_path
        stem = target_path.stem
        suffix = target_path.suffix
        parent = target_path.parent
        index = 2
        while True:
            candidate = parent / f"{stem} ({index}){suffix}"
            if not candidate.exists():
                return candidate
            index += 1

    def _schedule_child_item(self, batch_id: str, item) -> None:
        if should_unpack_with_agent(item.container_kind):
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.UNPACK)
        elif item.source_path.is_dir():
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.SPLITTER)
        else:
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PREPARE)

    def _cleanup_root_workspace_if_complete(self, root_item_id: str) -> None:
        if not self.config.behavior.cleanup_workspace:
            return
        if not self.state_store.root_is_complete(root_item_id):
            return
        root_item = self.state_store.get_item_by_id(root_item_id)
        if root_item is None:
            return
        if root_item.status == ItemStatus.SPLIT and self._should_remove_source_after_success():
            self._remove_source_path(root_item.source_path)
        if root_item.unpack_dir:
            shutil.rmtree(root_item.unpack_dir.parent, ignore_errors=True)

    def _repair_invalid_packs(self, batch_id: str) -> None:
        repaired = 0
        unrecoverable = 0
        for item in self.state_store.list_items_with_hash(batch_id, EMPTY_ZIP_SHA256):
            if self._repair_invalid_packed_item(batch_id, item):
                repaired += 1
            else:
                unrecoverable += 1
        if repaired or unrecoverable:
            self._sync_dashboard(
                batch_id,
                f"Recovered {repaired} empty archives; {unrecoverable} item(s) need manual recovery.",
            )

    def _repair_invalid_packed_item(self, batch_id: str, item) -> bool:
        deleted_outputs = self._discard_invalid_archives(item)
        item.packed_path = None
        item.packed_hash = ""
        if deleted_outputs:
            item.final_path = None

        if item.unpack_dir and item.unpack_dir.exists():
            item.status = ItemStatus.PREPARED
            item.message = "Invalid packed archive detected. Requeued for repack from workspace."
            self.state_store.save_item(item)
            self.state_store.delete_task(batch_id, item.item_id, TaskStage.PLACEMENT)
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PACK)
            self.state_store.add_event(item.item_id, "repair", item.message)
            return True

        if item.source_path.exists():
            item.unpack_dir = None
            item.status = ItemStatus.DISCOVERED
            item.message = "Invalid packed archive detected. Requeued from source."
            self.state_store.save_item(item)
            for stage in (
                TaskStage.UNPACK,
                TaskStage.SPLITTER,
                TaskStage.PREPARE,
                TaskStage.ARCHIVARIUS,
                TaskStage.EXPERT,
                TaskStage.PACK,
                TaskStage.PLACEMENT,
            ):
                self.state_store.delete_task(batch_id, item.item_id, stage)
            self._schedule_child_item(batch_id, item)
            self.state_store.add_event(item.item_id, "repair", item.message)
            return True

        item.status = ItemStatus.FAILED
        item.message = "Invalid packed archive detected, but source data is no longer available for rebuild."
        self.state_store.save_item(item)
        self.state_store.delete_task(batch_id, item.item_id, TaskStage.PACK)
        self.state_store.delete_task(batch_id, item.item_id, TaskStage.PLACEMENT)
        self.state_store.add_event(item.item_id, "repair", item.message)
        return False

    def _discard_invalid_archives(self, item) -> bool:
        deleted_any = False
        for path in (item.packed_path, item.final_path):
            if path is None or not path.exists():
                continue
            if is_valid_packed_archive(path):
                continue
            path.unlink(missing_ok=True)
            deleted_any = True
        return deleted_any

    def _ensure_pack_ready_for_placement(self, item):
        if (
            item.packed_path
            and item.packed_hash
            and item.packed_hash != EMPTY_ZIP_SHA256
            and is_valid_packed_archive(item.packed_path)
        ):
            return item

        self._discard_invalid_archives(item)
        item.packed_path = None
        item.packed_hash = ""

        if item.unpack_dir is None or not item.unpack_dir.exists():
            if not item.source_path.exists():
                raise RuntimeError("Packed archive is invalid and source data is unavailable.")
            if should_unpack_with_agent(item.container_kind):
                with self._unpack_slots:
                    item = self.unpack_agent.run(self.context, item)
                self.state_store.save_item(item)
            else:
                item.unpack_dir, nested_count = stage_source(
                    item.source_path,
                    self.context.workspace_root / item.item_id,
                    max_nested_depth=self.config.limits.max_nested_archive_depth,
                )
                self.state_store.add_event(
                    item.item_id,
                    "repair_stage",
                    "Source restaged before placement because packed archive was invalid.",
                    payload={
                        "unpack_dir": str(item.unpack_dir),
                        "nested_archives_expanded": nested_count,
                    },
                )

        with self._pack_slots:
            return self.pack_agent.run(self.context, item)

    def _ensure_workspace_ready_for_pack(self, item):
        if self._workspace_has_files(item.unpack_dir):
            return item

        if not item.source_path.exists():
            raise RuntimeError("Workspace is empty and source data is unavailable.")

        if should_unpack_with_agent(item.container_kind):
            with self._unpack_slots:
                item = self.unpack_agent.run(self.context, item)
            self.state_store.save_item(item)
            self.state_store.add_event(
                item.item_id,
                "repack_stage",
                "Workspace restored from source before pack.",
                payload={"unpack_dir": str(item.unpack_dir) if item.unpack_dir else ""},
            )
            return item

        item.unpack_dir, nested_count = stage_source(
            item.source_path,
            self.context.workspace_root / item.item_id,
            max_nested_depth=self.config.limits.max_nested_archive_depth,
        )
        self.state_store.add_event(
            item.item_id,
            "repack_stage",
            "Workspace restaged from source before pack.",
            payload={
                "unpack_dir": str(item.unpack_dir),
                "nested_archives_expanded": nested_count,
            },
        )
        return item

    def _failure_target_for_error(self, stage: TaskStage, message: str) -> tuple[Path, ItemStatus]:
        normalized = message.lower()
        if stage == TaskStage.UNPACK and (
            "archive extraction failed" in normalized
            or "file is not a zip file" in normalized
            or "not a rar file" in normalized
            or "unexpected end of archive" in normalized
            or "crc failed" in normalized
        ):
            return self.config.paths.damaged_root, ItemStatus.DAMAGED
        return self.config.paths.failed_root, ItemStatus.FAILED

    def _should_remove_source_after_success(self) -> bool:
        return self.config.behavior.move_outputs and not self.config.behavior.safe_mode

    def _should_move_source_for_special_route(self) -> bool:
        return self.config.behavior.move_outputs and not self.config.behavior.safe_mode

    def _workspace_has_files(self, unpack_dir: Path | None) -> bool:
        if unpack_dir is None or not unpack_dir.exists():
            return False
        return any(path.is_file() for path in unpack_dir.rglob("*"))

    def _acquire_run_lock(self) -> None:
        lock_path = self.config.paths.run_lock_file
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        if lock_path.exists():
            try:
                payload = json.loads(lock_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
            lock_pid = int(payload.get("pid", 0) or 0)
            if lock_pid and self._pid_is_running(lock_pid) and lock_pid != os.getpid():
                raise RuntimeError(
                    f"Another orchestrator run is already active (pid={lock_pid}). "
                    "Stop it first or wait for it to finish."
                )
        lock_payload = {
            "pid": os.getpid(),
            "started_at": time.time(),
            "source_root": str(self.config.paths.source_root),
            "state_db": str(self.config.paths.state_db),
        }
        lock_path.write_text(json.dumps(lock_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _release_run_lock(self) -> None:
        lock_path = self.config.paths.run_lock_file
        if not lock_path.exists():
            return
        try:
            payload = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        lock_pid = int(payload.get("pid", 0) or 0)
        if lock_pid in {0, os.getpid()}:
            lock_path.unlink(missing_ok=True)

    def _pid_is_running(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
