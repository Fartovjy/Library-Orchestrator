#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _scan_source_dir(self, source_root: Path, active_slot: str) -> None:
    """Scan one directory tree and push FileTask objects to q12."""
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
                        try:
                            size_bytes = entry.stat().st_size
                        except Exception:
                            size_bytes = 0
                        task = FileTask(
                            task_id=str(uuid.uuid4()),
                            path=Path(entry.path),
                            origin="source",
                            source_root=source_root,
                            size_bytes=size_bytes,
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


def _scanner_loop(self) -> None:
    self.metrics.add_event("Agent1: поиск файлов...")
    active_slot = "W1"
    self.metrics.set_active_item("A1", active_slot, "scan")
    try:
        # Начальный скан
        for source_root in self.config.source_dirs:
            if self.should_stop() or self.cleanup_event.is_set():
                break
            _scan_source_dir(self, source_root, active_slot)

        # Live-wait: ждём папки, брошенные через drop во время работы
        if not self.should_stop() and not self.cleanup_event.is_set():
            self.metrics.set_active_item("A1", active_slot, "ожидание...")
            idle_ticks = 0  # сколько раз подряд видели полный idle
            while not self.should_stop() and not self.cleanup_event.is_set():
                try:
                    extra: Optional[Path] = self._live_source_queue.get(timeout=1.0)
                except queue.Empty:
                    # Проверяем: может всё уже обработано?
                    if self._all_downstream_idle():
                        idle_ticks += 1
                        if idle_ticks >= 3:  # 3 секунды idle → завершаем
                            break
                    else:
                        idle_ticks = 0
                    continue

                if extra is None:  # сентинель — явная остановка
                    break

                idle_ticks = 0
                self.metrics.add_event(f"Agent1: новая папка: {extra.name}")
                self.logger.info("A1 live-inject: %s", extra)
                if extra not in self.config.source_dirs:
                    self.config.source_dirs.append(extra)
                _scan_source_dir(self, extra, active_slot)
    finally:
        self.metrics.clear_active_item("A1", active_slot)
        self.scan_done.set()
        self.metrics.add_event("Agent1: поиск завершен")
