from __future__ import annotations

import threading
import shutil
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
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
from .models import ItemStatus, QueueStage
from .queues import StageQueues
from .resource_monitor import ResourceMonitor
from .state_store import StateStore


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

    def run(self, limit: int | None = None) -> dict[str, int]:
        if self._stop_requested():
            self.clear_stop_file()
            self.dashboard.set_current(
                self.config.paths.source_root,
                "resume",
                "Cleared stale stop request from previous run.",
            )
        if limit == 0:
            self.dashboard.set_total(0)
            return self.state_store.status_counts()
        recovered_item_ids = deque(self.state_store.list_ready_for_heavy_item_ids())
        recovered_sources = set()
        for item_id in recovered_item_ids:
            item = self.state_store.get_item_by_id(item_id)
            if item is not None:
                recovered_sources.add(str(item.source_path))

        discovered_sources = self._discover_sources(limit=limit)
        sources = deque(
            source_path
            for source_path in discovered_sources
            if str(source_path) not in recovered_sources
        )
        self.dashboard.set_total(len(sources) + len(recovered_item_ids))
        max_light_workers = max(1, self.config.limits.max_parallel_items)
        max_heavy_workers = max(1, self.config.limits.max_parallel_heavy_agents)
        pending_light: dict[Future[str | None], Path] = {}
        pending_heavy: dict[Future[None], str] = {}
        ready_for_heavy = deque(recovered_item_ids)
        queued_heavy_ids = set(recovered_item_ids)
        stop_requested = False
        with (
            ThreadPoolExecutor(max_workers=max_light_workers, thread_name_prefix="light") as light_executor,
            ThreadPoolExecutor(max_workers=max_heavy_workers, thread_name_prefix="heavy") as heavy_executor,
        ):
            while sources or pending_light or ready_for_heavy or pending_heavy:
                while not stop_requested and ready_for_heavy and len(pending_heavy) < max_heavy_workers:
                    item_id = ready_for_heavy.popleft()
                    future = heavy_executor.submit(self.finish_prepared_item, item_id)
                    pending_heavy[future] = item_id

                while not stop_requested and sources and len(pending_light) < max_light_workers:
                    source_path = sources.popleft()
                    if self._try_process_special_source(source_path):
                        continue
                    while self.resource_monitor.should_throttle():
                        self.dashboard.set_current(source_path, "throttle", "Waiting for disk load to drop.")
                        time.sleep(self.config.limits.sleep_if_busy_seconds)
                        if self._poll_stop_requested():
                            stop_requested = True
                            break
                    if stop_requested:
                        break
                    future = light_executor.submit(self.prepare_source, source_path)
                    pending_light[future] = source_path

                if not pending_light and not pending_heavy:
                    if stop_requested:
                        break
                    continue

                if not stop_requested and sources and len(pending_light) >= max_light_workers:
                    if self._drain_special_sources(sources):
                        continue

                wait_targets = tuple(list(pending_light) + list(pending_heavy))
                done, _ = wait(wait_targets, timeout=0.2, return_when=FIRST_COMPLETED)
                if not done:
                    if self._poll_stop_requested():
                        stop_requested = True
                    continue

                for future in done:
                    if future in pending_light:
                        source_path = pending_light.pop(future)
                        try:
                            item_id = future.result()
                            if item_id and item_id not in queued_heavy_ids:
                                ready_for_heavy.append(item_id)
                                queued_heavy_ids.add(item_id)
                        except Exception as error:  # pragma: no cover
                            self.dashboard.set_current(source_path, "failed", f"Unhandled light worker error: {error}")
                            self.dashboard.finish_item(self.state_store.status_counts(), f"Unhandled light worker error: {error}")
                        continue

                    item_id = pending_heavy.pop(future)
                    queued_heavy_ids.discard(item_id)
                    try:
                        future.result()
                    except Exception as error:  # pragma: no cover
                        item = self.state_store.get_item_by_id(item_id)
                        target = item.source_path if item is not None else self.config.paths.source_root
                        self.dashboard.set_current(target, "failed", f"Unhandled heavy worker error: {error}")
                        self.dashboard.finish_item(self.state_store.status_counts(), f"Unhandled heavy worker error: {error}")
                if self._poll_stop_requested():
                    stop_requested = True
        return self.state_store.status_counts()

    def prepare_source(self, source_path: Path) -> str | None:
        self.dashboard.set_current(source_path, "discovery", "Item scheduled for processing.")
        container_kind = detect_container_kind(source_path)
        item = self.state_store.get_or_create_item(source_path, container_kind)
        self.dashboard.advance_agent("discovery", self.state_store.status_counts(), f"Discovered {item.source_name}")
        if item.status in {
            ItemStatus.PLACED,
            ItemStatus.DUPLICATE,
            ItemStatus.MANUAL_REVIEW,
            ItemStatus.TRASH,
            ItemStatus.DAMAGED,
        }:
            self.dashboard.finish_item(self.state_store.status_counts(), f"Skipped already processed item: {item.source_name}")
            return None
        if item.status == ItemStatus.PREPARED and item.unpack_dir and item.unpack_dir.exists():
            self.dashboard.begin_agent(source_path, "prepare", "Prepared item already waiting for heavy processing.")
            try:
                self.dashboard.advance_agent("prepare", self.state_store.status_counts(), item.message or "Prepared item already waiting for heavy processing.")
            finally:
                self.dashboard.end_agent("prepare")
            return item.item_id
        file_role = classify_file_role(source_path)
        if (
            source_path.suffix.lower() in set(self.config.behavior.trash_extensions)
            or file_role == "trash"
        ):
            self._route_to_special(item, self.config.paths.trash_root, ItemStatus.TRASH, "Detected as trash by extension or file signature.")
            self.dashboard.finish_item(self.state_store.status_counts(), item.message)
            return None
        if file_role == "unknown" and source_path.is_file():
            self._route_to_special(item, self.config.paths.manual_review_root, ItemStatus.MANUAL_REVIEW, "Unknown file type by extension and signature.")
            self.dashboard.finish_item(self.state_store.status_counts(), item.message)
            return None
        item.source_hash = compute_sha256(source_path)
        self.state_store.save_item(item)
        if should_unpack_with_agent(item.container_kind):
            self.queues.enqueue(QueueStage.UNPACK, item.item_id)

        try:
            if should_unpack_with_agent(item.container_kind):
                self.dashboard.begin_agent(source_path, "unpack", "Unpacking archive source.")
                try:
                    with self._unpack_slots:
                        item, excerpt = self.unpack_agent.run(self.context, item)
                    self.dashboard.advance_agent("unpack", self.state_store.status_counts(), item.message)
                    if item.status == ItemStatus.MANUAL_REVIEW:
                        self.dashboard.finish_item(self.state_store.status_counts(), item.message)
                        return None
                finally:
                    self.dashboard.end_agent("unpack")
            else:
                self.dashboard.begin_agent(source_path, "prepare", "Staging source into workspace.")
                try:
                    item.unpack_dir = stage_source(item.source_path, self.context.workspace_root / item.item_id)
                    excerpt = collect_excerpt(item.unpack_dir, self.config.lmstudio.fast_excerpt_words)
                    self.state_store.add_event(
                        item.item_id,
                        "prepare",
                        "Source staged into workspace without archive unpack.",
                        payload={"unpack_dir": str(item.unpack_dir)},
                    )
                finally:
                    self.dashboard.end_agent("prepare")
            item.prepared_excerpt = excerpt
            item.status = ItemStatus.PREPARED
            item.message = "Prepared for heavy classification."
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
            self.dashboard.begin_agent(source_path, "prepare", item.message)
            try:
                self.dashboard.advance_agent("prepare", self.state_store.status_counts(), item.message)
            finally:
                self.dashboard.end_agent("prepare")
            return item.item_id
        except Exception as error:  # pragma: no cover
            self._route_to_special(item, self.config.paths.damaged_root, ItemStatus.DAMAGED, str(error))
            self.dashboard.finish_item(self.state_store.status_counts(), item.message)
            return None

    def finish_prepared_item(self, item_id: str) -> None:
        item = self.state_store.get_item_by_id(item_id)
        if item is None:
            return
        if item.status in {
            ItemStatus.PLACED,
            ItemStatus.DUPLICATE,
            ItemStatus.MANUAL_REVIEW,
            ItemStatus.TRASH,
            ItemStatus.DAMAGED,
        }:
            return

        try:
            excerpt_source = item.unpack_dir or item.source_path
            excerpt = item.prepared_excerpt or collect_excerpt(
                excerpt_source,
                self.config.lmstudio.fast_excerpt_words,
            )
            self.dashboard.begin_agent(item.source_path, "archivarius", "Running fast classification.")
            try:
                item, needs_deep = self.archivarius_agent.run(self.context, item, excerpt)
                self.dashboard.advance_agent("archivarius", self.state_store.status_counts(), item.message)
            finally:
                self.dashboard.end_agent("archivarius")
            if needs_deep:
                self.dashboard.begin_agent(item.source_path, "expert", "Running deep classification.")
                try:
                    item = self.expert_agent.run(self.context, item)
                    self.dashboard.advance_agent("expert", self.state_store.status_counts(), item.message)
                finally:
                    self.dashboard.end_agent("expert")
            self.dashboard.begin_agent(item.source_path, "pack", "Packing normalized archive.")
            try:
                with self._pack_slots:
                    item = self.pack_agent.run(self.context, item)
                self.dashboard.advance_agent("pack", self.state_store.status_counts(), item.message)
            finally:
                self.dashboard.end_agent("pack")
            self.dashboard.begin_agent(item.source_path, "placement", "Placing archive into library tree.")
            try:
                with self._placement_lock:
                    self.placement_agent.run(self.context, item)
                    if self.config.behavior.move_outputs:
                        self._remove_source_path(item.source_path)
                self.dashboard.advance_agent("placement", self.state_store.status_counts(), item.message)
            finally:
                self.dashboard.end_agent("placement")
        except Exception as error:  # pragma: no cover
            self._route_to_special(item, self.config.paths.damaged_root, ItemStatus.DAMAGED, str(error))
        finally:
            if self.config.behavior.cleanup_workspace and item.unpack_dir:
                shutil.rmtree(item.unpack_dir.parent, ignore_errors=True)
        self.dashboard.finish_item(self.state_store.status_counts(), item.message)

    def _try_process_special_source(self, source_path: Path) -> bool:
        existing = self.state_store.get_item_by_source(source_path)
        if existing and existing.status in {
            ItemStatus.PLACED,
            ItemStatus.DUPLICATE,
            ItemStatus.MANUAL_REVIEW,
            ItemStatus.TRASH,
            ItemStatus.DAMAGED,
        }:
            self.dashboard.set_current(source_path, "discovery", "Item scheduled for processing.")
            self.dashboard.advance_agent("discovery", self.state_store.status_counts(), f"Discovered {existing.source_name}")
            self.dashboard.finish_item(self.state_store.status_counts(), f"Skipped already processed item: {existing.source_name}")
            return True

        file_role = classify_file_role(source_path)
        is_trash = source_path.suffix.lower() in set(self.config.behavior.trash_extensions) or file_role == "trash"
        is_unknown = file_role == "unknown" and source_path.is_file()
        if not is_trash and not is_unknown:
            return False

        item = existing or self.state_store.get_or_create_item(source_path, detect_container_kind(source_path))
        self.dashboard.set_current(source_path, "discovery", "Item scheduled for processing.")
        self.dashboard.advance_agent("discovery", self.state_store.status_counts(), f"Discovered {item.source_name}")
        if is_trash:
            self._route_to_special(item, self.config.paths.trash_root, ItemStatus.TRASH, "Detected as trash by extension or file signature.")
        else:
            self._route_to_special(item, self.config.paths.manual_review_root, ItemStatus.MANUAL_REVIEW, "Unknown file type by extension and signature.")
        self.dashboard.finish_item(self.state_store.status_counts(), item.message)
        return True

    def _drain_special_sources(self, sources: deque[Path]) -> bool:
        routed_any = False
        remaining: deque[Path] = deque()
        while sources:
            source_path = sources.popleft()
            if self._try_process_special_source(source_path):
                routed_any = True
            else:
                remaining.append(source_path)
        sources.extend(remaining)
        return routed_any

    def _discover_sources(self, limit: int | None = None) -> list[Path]:
        existing = self.state_store.list_terminal_sources()
        skip_roots = {
            self.config.paths.workspace_root.resolve(),
            self.config.paths.output_root.resolve(),
            self.config.paths.library_root.resolve(),
            self.config.paths.duplicates_root.resolve(),
            self.config.paths.manual_review_root.resolve(),
            self.config.paths.trash_root.resolve(),
            self.config.paths.damaged_root.resolve(),
            self.config.paths.logs_root.resolve(),
            self.config.paths.state_db.parent.resolve(),
            Path(__file__).resolve().parents[1],
        }
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
