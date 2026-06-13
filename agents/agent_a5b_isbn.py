#!/usr/bin/env python3
"""Agent A5b: rate-limited ISBN network lookup queue."""

from __future__ import annotations

import time

from library_pipeline import *  # noqa: F401,F403


def _isbn_loop(self, worker_idx: int) -> None:
    """Consume q5_isbn, enforce global rate limit, update metadata via network ISBN lookup."""
    self.metrics.add_event(f"Agent5b/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
    while True:
        if self.cleanup_event.is_set():
            break
        if self.should_stop() and self.q5_isbn.empty():
            break
        try:
            task: FileTask = self.q5_isbn.get(timeout=0.3)
        except queue.Empty:
            if self.tag_done.is_set() and self.q5_isbn.empty():
                break
            continue
        try:
            self.metrics.set_active_item("A5b", active_slot, task.path.name)
            self.metrics.mark_stage("A5b")

            # --- global rate limit: 28 req/min ≈ 1 per 2.15 s ---
            # Hold the lock only to compute/reserve the next slot; sleep outside.
            with ISBN_RATE_LOCK:
                now = time.monotonic()
                gap = ISBN_LAST_TS[0] + ISBN_MIN_INTERVAL - now
                if gap > 0:
                    deadline = now + gap
                    ISBN_LAST_TS[0] = deadline   # reserve this slot
                else:
                    deadline = now
                    ISBN_LAST_TS[0] = now        # reset to current time

            # Interruptible sleep so stop/cleanup is responsive.
            while time.monotonic() < deadline:
                if self.should_stop():
                    break
                time.sleep(0.15)

            if not self.should_stop():
                self._perform_isbn_network_lookup(task)
                self.logger.info(
                    "A5b done path=%s candidates=%s %s",
                    task.path,
                    task.isbn_candidates,
                    self._md_brief(task.metadata),
                )
        except Exception as exc:
            self.logger.warning("Agent5b: ошибка ISBN %s: %s", task.path, exc)
            self.metrics.mark_stage("A5b", error=True)
        finally:
            self.metrics.clear_active_item("A5b", active_slot)
            self._put_with_stop(self.q56, task)
            self.q5_isbn.task_done()
    self.metrics.add_event(f"Agent5b/W{worker_idx}: завершен")
