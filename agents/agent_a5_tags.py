#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _tags_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent5/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
    while True:
        if self.cleanup_event.is_set():
            break
        if self.should_stop() and self.q45.empty():
            break
        try:
            task: FileTask = self.q45.get(timeout=0.3)
        except queue.Empty:
            if self.dedupe_done.is_set() and self.q45.empty():
                break
            continue
        try:
            self.metrics.set_active_item("A5", active_slot, task.path.name)
            self.metrics.mark_stage("A5")
            task.metadata = self._extract_metadata(task)
            self.logger.info("A5 tags path=%s %s", task.path, self._md_brief(task.metadata))
            self._put_with_stop(self.q56, task)
            self.db.mark_file(task, "tags_done", json.dumps(task.metadata.__dict__, ensure_ascii=False))
        except Exception as exc:
            self.logger.exception("Agent5: ошибка %s: %s", task.path, exc)
            self.metrics.mark_stage("A5", error=True)
            self.db.mark_file(task, "tags_failed", str(exc))
            self._put_with_stop(self.q56, task)
        finally:
            self.metrics.clear_active_item("A5", active_slot)
            self.q45.task_done()
    self.metrics.add_event(f"Agent5/W{worker_idx}: завершен")


def _extract_metadata(self, task: FileTask) -> Metadata:
    md = Metadata()
    ext = suffix_lower(task.path)

    # 1) Имя файла
    filename_stem = display_path_name(task.path).rsplit(".", 1)[0]
    parsed = parse_filename(filename_stem)
    if parsed.get("title"):
        md.title = parsed["title"]
        md.source = "filename"
        md.confidence = max(md.confidence, 0.35)
    if parsed.get("author"):
        md.author = parsed["author"]
        md.source = "filename"
        md.confidence = max(md.confidence, 0.35)

    # 2) Форматные теги
    fmt_tags = {}
    try:
        if ext == ".epub":
            fmt_tags = extract_epub_metadata(task.path)
        elif ext in {".fb2", ".fb2.zip"}:
            fmt_tags = extract_fb2_metadata(task.path)
        elif ext in {".docx"}:
            fmt_tags = extract_docx_metadata(task.path)
        elif ext == ".pdf":
            fmt_tags = extract_pdf_metadata(task.path)
    except Exception as exc:
        self.logger.debug("Ошибка чтения тегов %s: %s", task.path, exc)

    if fmt_tags.get("title") and not md.title:
        md.title = fmt_tags["title"]
        md.source = "tags"
        md.confidence = max(md.confidence, 0.75)
    if fmt_tags.get("author") and not md.author:
        md.author = fmt_tags["author"]
        md.source = "tags"
        md.confidence = max(md.confidence, 0.75)
    if fmt_tags.get("genre"):
        md.genre = normalize_genre(fmt_tags["genre"])
        md.source = "tags"
        md.confidence = max(md.confidence, 0.75)

    self._enrich_metadata_from_isbn(task, md, fmt_tags)

    # 3) Жанр из пути/цепочки
    if not md.genre:
        guess = infer_genre_from_path(task.path, task.archive_chain)
        if guess:
            md.genre = guess
            md.confidence = max(md.confidence, 0.5)
            if md.source == "none":
                md.source = "path"

    # 4) Жанр из названия (дешево, без LM)
    if not md.genre or md.genre == "Unknown":
        guess = infer_genre_from_title(md.title)
        if guess:
            md.genre = guess
            md.confidence = max(md.confidence, 0.55)
            if md.source == "none":
                md.source = "title"

    # 5) Безопасные fallback
    if not md.title:
        md.title = task.path.stem
    if not md.author:
        md.author = "Unknown Author"
    if not md.genre:
        md.genre = "Unknown"
    return md


def _needs_lm(self, md: Metadata) -> bool:
    if md.source == "tags" and md.confidence >= 0.75:
        return False
    missing_title = not md.title or md.title.strip().lower() in {
        "unknown",
        "unknown title",
    }
    missing_author = not md.author or md.author.strip().lower() in {
        "unknown",
        "unknown author",
    }
    if missing_author and not self.config.lm_fill_unknown_author:
        missing_author = False
    # Не тратим LM только ради жанра, если автор и название уже определены.
    if not missing_title and not missing_author:
        return False
    return missing_title or missing_author


def _needs_genre_only_lm(self, md: Metadata) -> bool:
    missing_title = not md.title or md.title.strip().lower() in {
        "unknown",
        "unknown title",
    }
    missing_author = not md.author or md.author.strip().lower() in {
        "unknown",
        "unknown author",
    }
    missing_genre = not md.genre or md.genre.strip().lower() in {"unknown"}
    return (not missing_title) and (not missing_author) and missing_genre


def _merge_lm_metadata(self, md: Metadata, lm_data: dict[str, Any]) -> None:
    title = clean_text(lm_data.get("title", ""))
    author = clean_text(lm_data.get("author", ""))
    genre = clean_text(lm_data.get("genre", ""))
    subgenres_raw = lm_data.get("subgenres", [])
    subgenres: list[str] = []
    if isinstance(subgenres_raw, list):
        subgenres = [clean_text(str(x)) for x in subgenres_raw if clean_text(str(x))]
    elif subgenres_raw:
        subgenres = [clean_text(str(subgenres_raw))]
    try:
        conf = float(lm_data.get("confidence", 0.0))
    except Exception:
        conf = 0.0

    if title:
        md.title = title
    if author:
        md.author = author
    if genre:
        md.genre = normalize_genre(genre)
    if subgenres:
        md.subgenres = subgenres
    if conf > md.confidence:
        md.confidence = conf
    if title or author or genre or subgenres:
        md.source = "lmstudio"

