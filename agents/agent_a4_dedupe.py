#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _dedupe_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent4/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
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
            self.metrics.set_active_item("A4", active_slot, task.path.name)
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
                task.canonical_zip = Path(canonical) if canonical else None
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
            self._move_to_error_dir(task, reason=f"A4 dedupe: {str(exc)[:120]}")
            self._finalize_task(task, result="failed")
        finally:
            self.metrics.clear_active_item("A4", active_slot)
            self.q34.task_done()
    self.metrics.add_event(f"Agent4/W{worker_idx}: завершен")


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

