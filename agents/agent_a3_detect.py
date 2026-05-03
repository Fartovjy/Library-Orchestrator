#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _detect_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent3/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
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
            self.metrics.set_active_item("A3", active_slot, task.path.name)
            self.metrics.mark_stage("A3")
            is_book_file, reason = self._is_book_candidate(task)
            if is_book_file:
                task.is_book_candidate = True
                self._mark_archive_has_book(task.archive_source)
                self._register_archive_book(task.archive_source)
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
                if task.book_seen_counted:
                    task.book_seen_counted = False
                    self.metrics.unmark_book_seen()
                if task.origin == "temp" and self._nobook_rejection_is_ambiguous(task, reason):
                    self._preserve_archive_source(task.archive_source)
                    self.logger.info(
                        "Archive source preserved after ambiguous nobook path=%s reason=%s archive=%s",
                        task.path,
                        reason,
                        task.archive_source or "",
                    )
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
            self.metrics.clear_active_item("A3", active_slot)
            self.q23.task_done()
    self.metrics.add_event(f"Agent3/W{worker_idx}: завершен")


def _is_book_candidate(self, task: FileTask) -> tuple[bool, str]:
    path = task.path
    ext = suffix_lower(path)
    size = safe_filesize(path)
    if size == 0:
        return False, "empty_file"

    kind_ext, mime = guess_filetype_kind(path)
    if filetype_is_book(kind_ext, mime, ext):
        detail = mime or kind_ext or "unknown"
        return True, f"book_filetype:{detail}"
    if filetype_is_nonbook(kind_ext, mime, ext):
        detail = mime or kind_ext or "unknown"
        return False, f"nonbook_filetype:{detail}"

    if ext in TEXT_EXTENSIONS:
        snippet = extract_text_snippet(path, max_chars=400).lower()
        if any(token in snippet for token in ("isbn", "глава", "chapter", "автор")):
            return True, "text_book_signals"
        return False, "text_no_book_signals"

    if ext in BOOK_EXTENSIONS:
        return True, f"book_ext:{ext}"
    if ext in STRONG_NONBOOK_EXTENSIONS:
        return False, f"nonbook_ext:{ext}"

    if looks_binary(path):
        if size < TINY_UNKNOWN_BINARY_BYTES:
            return False, f"tiny_unknown_binary:{ext or 'no_ext'}"
        return False, f"unknown_binary_not_book_type:{ext or 'no_ext'}"
    return False, f"unknown_text_not_book_type:{ext or 'no_ext'}"


def _handle_nobook(self, task: FileTask, reason: str) -> None:
    if task.origin == "source":
        dst = self._move_source_to_nobook(task.path, task.source_root, reason)
        self.metrics.add_event(f"NoBook -> {dst.name} ({reason})")
    else:
        if self.config.keep_temp_nobooks:
            chain_dir = "__".join(sanitize_component(x) for x in task.archive_chain[-3:])
            ext = task.path.suffix.lower()
            filename = sanitize_component(task.path.stem) + ext
            out = ensure_unique_file_path(
                self.config.nobook_dir / "FromArchives" / chain_dir / filename
            )
            out.parent.mkdir(parents=True, exist_ok=True)
            safe_move(task.path, out)
            self.metrics.mark_nobook_file_saved()


def _move_source_to_nobook(
    self,
    path: Path,
    source_root: Optional[Path],
    reason: str,
) -> Path:
    base = source_root or self._source_root_for_path(path)
    rel = safe_relative(path, base)
    dst = ensure_unique_file_path(self.config.nobook_dir / rel)
    dst.parent.mkdir(parents=True, exist_ok=True)
    safe_move(path, dst)
    self.metrics.mark_nobook_file_saved()
    self.logger.info("NoBook move source=%s dest=%s reason=%s", path, dst, reason)
    return dst


def _source_root_for_path(self, path: Path) -> Path:
    try:
        resolved = path.resolve(strict=False)
    except Exception:
        resolved = path
    for source_dir in self.config.source_dirs:
        try:
            root = source_dir.resolve(strict=False)
            resolved.relative_to(root)
            return source_dir
        except Exception:
            continue
    return path.parent

