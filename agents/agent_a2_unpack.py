#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _unpack_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent2/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
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
            self.metrics.set_active_item("A2", active_slot, task.path.name)
            self.metrics.mark_stage("A2")
            if is_archive(task.path):
                extracted_count = self._extract_archive_and_route(task)
                if extracted_count == 0 and task.origin == "source":
                    reason = "empty_archive"
                    self._handle_nobook(task, reason)
                    self.db.mark_file(task, "nobook", reason)
                    self._finalize_task(task, result="nobook")
                else:
                    self._finalize_task(task, result="archive_unpacked")
                    self.db.mark_file(task, "unpack_done", "")
            else:
                self._put_with_stop(self.q23, task)
                self.db.mark_file(task, "unpack_done", "")
        except Exception as exc:
            self.logger.exception("Agent2: ошибка %s: %s", task.path, exc)
            self.metrics.mark_stage("A2", error=True)
            self.db.mark_file(task, "unpack_failed", str(exc))
            if not self.should_stop() and not self.cleanup_event.is_set():
                self._move_to_error_dir(task, reason=f"A2 unpack: {str(exc)[:120]}")
            else:
                self.logger.info(
                    "A2 unpack interrupted by stop — source kept in place: %s", task.path
                )
            self._finalize_task(task, result="failed")
        finally:
            self.metrics.clear_active_item("A2", active_slot)
            self.unpack_active.dec()
            self.q12.task_done()
    self.metrics.add_event(f"Agent2/W{worker_idx}: завершен")


def _extract_archive_and_route(self, task: FileTask) -> int:
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

    extracted_count = 0
    self.temp_tracker.register(temp_root)
    try:
        for file_path in iter_files(temp_root):
            if self.should_stop():
                break
            extracted_count += 1
            new_task = FileTask(
                task_id=str(uuid.uuid4()),
                path=file_path,
                origin="temp",
                source_root=task.source_root,
                archive_chain=task.archive_chain + [task.path.name],
                archive_source=source_archive,
                cleanup_root=temp_root,
                size_bytes=safe_filesize(file_path),
            )
            self.temp_tracker.register(temp_root)
            self._mark_discovered_task(new_task)

            if is_archive(file_path):
                self._register_archive_child(source_archive)
                self._process_nested_archive_task(new_task)
            else:
                if not self._put_with_stop(self.q23, new_task):
                    self.temp_tracker.release(temp_root)
                    break
                self._register_archive_child(source_archive)
    finally:
        self.temp_tracker.release(temp_root)

    if extracted_count == 0:
        shutil.rmtree(temp_root, ignore_errors=True)
        self.metrics.add_event(f"Пустой архив: {task.path.name}")
    return extracted_count


def _process_nested_archive_task(self, task: FileTask) -> None:
    try:
        if self.cleanup_event.is_set() or self.should_stop():
            self._finalize_task(task, result="failed")
            return
        self.metrics.mark_stage("A2")
        extracted_count = self._extract_archive_and_route(task)
        if extracted_count == 0 and task.origin == "source":
            reason = "empty_archive"
            self._handle_nobook(task, reason)
            self.db.mark_file(task, "nobook", reason)
            self._finalize_task(task, result="nobook")
        else:
            self._finalize_task(task, result="archive_unpacked")
            self.db.mark_file(task, "unpack_done", "")
    except Exception as exc:
        self.logger.exception("Agent2 nested: ошибка %s: %s", task.path, exc)
        self.metrics.mark_stage("A2", error=True)
        self.db.mark_file(task, "unpack_failed", str(exc))
        if not self.should_stop() and not self.cleanup_event.is_set():
            self._move_to_error_dir(task, reason=f"A2 nested unpack: {str(exc)[:120]}")
        else:
            self.logger.info(
                "A2 nested unpack interrupted by stop — source kept in place: %s", task.path
            )
        self._finalize_task(task, result="failed")

