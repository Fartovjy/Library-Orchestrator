from __future__ import annotations

import shutil
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .agents import (
    AgentContext,
    ArchivariusAgent,
    ExpertAgent,
    PackAgent,
    PlacementAgent,
    UnpackAgent,
)
from .archive_adapters import (
    classify_file_role,
    collect_excerpt,
    compute_sha256,
    detect_container_kind,
    should_unpack_with_agent,
    stage_source,
)
from .config import AppConfig
from .dashboard import TerminalDashboard
from .hotkeys import StopHotkeyWatcher
from .lmstudio import LmStudioClient
from .models import BatchRun, ItemStatus, TaskStage
from .queues import StageQueues
from .resource_monitor import ResourceMonitor
from .state_store import StateStore


TERMINAL_STATUSES = {
    ItemStatus.PLACED,
    ItemStatus.DUPLICATE,
    ItemStatus.NON_BOOK,
    ItemStatus.MANUAL_REVIEW,
    ItemStatus.TRASH,
    ItemStatus.DAMAGED,
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
        self.hotkeys = StopHotkeyWatcher()
        self.dashboard.set_hotkey_hint(self.hotkeys.hint())
        self.unpack_agent = UnpackAgent()
        self.archivarius_agent = ArchivariusAgent()
        self.expert_agent = ExpertAgent()
        self.pack_agent = PackAgent()
        self.placement_agent = PlacementAgent()
        self._unpack_slots = threading.Semaphore(max(1, self.config.limits.max_parallel_unpack))
        self._pack_slots = threading.Semaphore(max(1, self.config.limits.max_parallel_pack))
        self._placement_lock = threading.Lock()
        self._dashboard_lock = threading.RLock()
        self._stop_event = threading.Event()

    def run(self, limit: int | None = None) -> dict[str, int]:
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
                        [TaskStage.PREPARE, TaskStage.ARCHIVARIUS],
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
                self._sync_dashboard(batch.batch_id)
                if self._poll_stop_requested():
                    self._stop_event.set()
                    break
                if not self.state_store.batch_has_pending_work(batch.batch_id):
                    self.state_store.finalize_batch(batch.batch_id)
                    self._stop_event.set()
                    break
                time.sleep(0.2)

            for future in futures:
                future.result()

        self._sync_dashboard(batch.batch_id, "Batch completed.")
        return self.state_store.status_counts()

    def _prepare_batch(self, limit: int | None) -> BatchRun:
        active_batch = self.state_store.get_active_batch()
        if active_batch is not None and self.state_store.batch_has_pending_work(active_batch.batch_id):
            self.state_store.reset_claimed_tasks(active_batch.batch_id)
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
            if self._poll_stop_requested():
                self._stop_event.set()
                return

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
                self._route_to_special(item, self.config.paths.damaged_root, ItemStatus.DAMAGED, str(error))
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
        if stage == TaskStage.PREPARE:
            return self._process_prepare_task(batch_id, item)
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
        else:
            self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PREPARE)
        return item.message, None

    def _process_unpack_task(self, batch_id: str, item) -> tuple[str, float | None]:
        with self._unpack_slots:
            item, excerpt = self.unpack_agent.run(self.context, item)
        item.prepared_excerpt = excerpt
        self.state_store.save_item(item)
        if item.status in TERMINAL_STATUSES:
            return item.message, None
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PREPARE)
        return item.message, None

    def _process_prepare_task(self, batch_id: str, item) -> tuple[str, float | None]:
        if item.unpack_dir and item.unpack_dir.exists():
            excerpt = item.prepared_excerpt or collect_excerpt(
                item.unpack_dir,
                self.config.lmstudio.fast_excerpt_words,
            )
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

        item.prepared_excerpt = excerpt
        item.status = ItemStatus.PREPARED
        item.message = "Prepared for fast classification."
        self.state_store.save_item(item)
        self.state_store.add_event(
            item.item_id,
            "prepare_ready",
            item.message,
            payload={
                "unpack_dir": str(item.unpack_dir) if item.unpack_dir else "",
                "excerpt_words": len(item.prepared_excerpt.split()),
            },
        )
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.ARCHIVARIUS)
        return item.message, None

    def _process_archivarius_task(self, batch_id: str, item) -> tuple[str, float | None]:
        item, needs_deep = self.archivarius_agent.run(self.context, item, item.prepared_excerpt)
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
        with self._pack_slots:
            item = self.pack_agent.run(self.context, item)
        self.state_store.ensure_task(batch_id, item.item_id, TaskStage.PLACEMENT)
        return item.message, None

    def _process_placement_task(self, batch_id: str, item) -> tuple[str, float | None]:
        with self._placement_lock:
            item = self.placement_agent.run(self.context, item)
            if self.config.behavior.move_outputs:
                self._remove_source_path(item.source_path)
        if self.config.behavior.cleanup_workspace and item.unpack_dir:
            shutil.rmtree(item.unpack_dir.parent, ignore_errors=True)
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
            if self._poll_stop_requested():
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
            if self.config.behavior.move_outputs:
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

    def create_stop_file(self) -> Path:
        stop_file = self.config.paths.stop_file
        stop_file.parent.mkdir(parents=True, exist_ok=True)
        stop_file.write_text("stop\n", encoding="utf-8")
        return stop_file

    def clear_stop_file(self) -> None:
        if self.config.paths.stop_file.exists():
            self.config.paths.stop_file.unlink()

    def _stop_requested(self) -> bool:
        return self.config.paths.stop_file.exists()

    def _poll_stop_requested(self) -> bool:
        if self._stop_requested():
            return True
        if not self.hotkeys.poll_stop_requested():
            return False
        self.create_stop_file()
        current_item = self.dashboard.state.current_item
        target = self.config.paths.source_root if current_item == "-" else Path(current_item)
        self.dashboard.set_current(target, "stop_requested", f"Safe stop requested by {self.hotkeys.last_trigger}.")
        return True

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
