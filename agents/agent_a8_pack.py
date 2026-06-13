#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _pack_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent8/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
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
            display_name = task.dest_zip.name if task.dest_zip else task.path.name
            self.metrics.set_active_item("A8", active_slot, display_name)
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
            if not self.should_stop() and not self.cleanup_event.is_set():
                self.metrics.add_event(
                    f"Pack failed: {task.path.name} -> {truncate(str(exc), 160)}"
                )
                self._move_to_error_dir(task, reason=str(exc)[:120])
            else:
                self.logger.info(
                    "A8 pack interrupted by stop — source kept in place: %s", task.path
                )
            self._finalize_task(task, result="failed")
        finally:
            self.metrics.clear_active_item("A8", active_slot)
            self.q78.task_done()
    self.metrics.add_event(f"Agent8/W{worker_idx}: завершен")


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
        "-mx=3",   # fast zip: books are already compressed formats
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

    # Переименовываем файл внутри архива, чтобы он совпадал с именем zip.
    # Пример: Афганец.zip должен содержать Афганец.fb2, а не 10103.fb2.
    new_internal = dest.stem + task.path.suffix.lower()
    old_internal = task.path.name
    if new_internal != old_internal and not lm_value_is_garbage(dest.stem):
        cmd_rn = [self.seven_zip, "rn", "-y", "-bd", str(dest), old_internal, new_internal]
        result_rn = self._run_cmd_with_cancel(cmd_rn, timeout_sec=30)
        if result_rn.returncode != 0:
            self.logger.warning(
                "A8 rn failed for %s: %s", dest, format_subprocess_error(result_rn, 120)
            )

