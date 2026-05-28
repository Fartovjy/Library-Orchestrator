#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _scanner_loop(self) -> None:
    self.metrics.add_event("Agent1: поиск файлов...")
    active_slot = "W1"
    self.metrics.set_active_item("A1", active_slot, "scan")
    try:
        for source_root in self.config.source_dirs:
            if self.should_stop() or self.cleanup_event.is_set():
                break
            stack = [source_root]
            while stack and not self.should_stop():
                if self.cleanup_event.is_set():
                    break
                cur = stack.pop()
                self.metrics.set_active_item("A1", active_slot, cur.name or str(cur))
                try:
                    with os.scandir(cur) as it:
                        for entry in it:
                            if self.should_stop():
                                break
                            try:
                                if entry.is_dir(follow_symlinks=False):
                                    stack.append(Path(entry.path))
                                    continue
                                if not entry.is_file(follow_symlinks=False):
                                    continue
                                task = FileTask(
                                    task_id=str(uuid.uuid4()),
                                    path=Path(entry.path),
                                    origin="source",
                                    source_root=source_root,
                                )
                                self._mark_discovered_task(task)
                                if not self._put_with_stop(self.q12, task):
                                    break
                            except Exception as exc:
                                self.logger.warning(
                                    "Ошибка чтения entry %s: %s", entry.path, exc
                                )
                                self.metrics.mark_stage("A1", error=True)
                except Exception as exc:
                    self.logger.warning(
                        "Ошибка входа в каталог %s: %s", cur, exc
                    )
                    self.metrics.mark_stage("A1", error=True)
    finally:
        self.metrics.clear_active_item("A1", active_slot)
        self.scan_done.set()
        self.metrics.add_event("Agent1: поиск завершен")

