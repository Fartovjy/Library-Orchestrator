#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _rename_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent7/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
    while True:
        if self.cleanup_event.is_set():
            break
        if self.should_stop() and self.q67.empty():
            break
        try:
            task: FileTask = self.q67.get(timeout=0.3)
        except queue.Empty:
            if self.lm_done.is_set() and self.q67.empty():
                break
            continue
        try:
            self.metrics.set_active_item("A7", active_slot, task.path.name)
            self.metrics.mark_stage("A7")
            task.dest_zip = self._build_destination(task)
            self.metrics.set_active_item("A7", active_slot, f"→ {task.dest_zip.name}")
            self.logger.info("A7 route path=%s dest=%s", task.path, task.dest_zip)
            self._put_with_stop(self.q78, task)
            self.db.mark_file(task, "rename_done", str(task.dest_zip))
        except Exception as exc:
            self.logger.exception("Agent7: ошибка %s: %s", task.path, exc)
            self.metrics.mark_stage("A7", error=True)
            self.db.mark_file(task, "rename_failed", str(exc))
            self._move_to_error_dir(task, reason=f"A7 rename: {str(exc)[:120]}")
            self._finalize_task(task, result="failed")
        finally:
            self.metrics.clear_active_item("A7", active_slot)
            self.q67.task_done()
    self.metrics.add_event(f"Agent7/W{worker_idx}: завершен")


def _build_destination(self, task: FileTask) -> Path:
    md = task.metadata
    md.genre = normalize_genre(md.genre or "Unknown")
    output_language = normalize_output_language(self.config.output_language)

    # Умный выбор: метаданные → парсинг имени файла → цепочка архивов → стем
    title_value  = pick_best_title(md, task)
    author_value = pick_best_author(md, task)
    # Чистим название: хвостовой цифровой ID + дубль автора в начале
    title_value  = clean_book_title(title_value, author_value) or title_value
    genre_value  = md.genre or "Unknown"

    if self.config.translate_output_names:
        translated = self.lm_client.translate_output_metadata(
            task, md, output_language
        )
        title_value = translated.get("title") or title_value
        author_value = translated.get("author") or author_value
        genre_value = translated.get("genre") or translate_genre_for_output(
            genre_value, output_language
        )

    genre = sanitize_component(genre_value, max_len=48)
    author = sanitize_component(author_value, max_len=64)
    first = first_letter(author)
    title = sanitize_component(title_value, max_len=110)

    for _ in range(6):
        out_dir = self.config.target_dir / genre / first / author
        dest = out_dir / f"{title}.zip"
        if os.name != "nt" or len(str(dest)) <= 240:
            out_dir.mkdir(parents=True, exist_ok=True)
            return dest

        # На Windows сначала укорачиваем title, затем author, затем genre.
        if len(title) > 36:
            title = shorten_with_hash(title, max_len=max(36, len(title) - 18))
            continue
        if len(author) > 24:
            author = shorten_with_hash(author, max_len=max(24, len(author) - 10))
            first = first_letter(author)
            continue
        if len(genre) > 20:
            genre = shorten_with_hash(genre, max_len=20)
            continue
        break

    # Жесткий fallback, чтобы путь гарантированно поместился.
    genre = shorten_with_hash(genre, max_len=20)
    author = shorten_with_hash(author, max_len=24)
    first = first_letter(author)
    title = shorten_with_hash(title, max_len=36)
    out_dir = self.config.target_dir / genre / first / author
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{title}.zip"

