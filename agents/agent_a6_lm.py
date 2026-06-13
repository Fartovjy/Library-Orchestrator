#!/usr/bin/env python3
"""Agent implementation extracted from library_pipeline.py."""

from __future__ import annotations

from library_pipeline import *  # noqa: F401,F403


def _lm_loop(self, worker_idx: int) -> None:
    self.metrics.add_event(f"Agent6/W{worker_idx}: старт")
    active_slot = f"W{worker_idx}"
    while True:
        if self.cleanup_event.is_set():
            break
        if self.should_stop() and self.q56.empty():
            break
        try:
            task: FileTask = self.q56.get(timeout=0.3)
        except queue.Empty:
            if self.tag_done.is_set() and self.isbn_done.is_set() and self.q56.empty():
                break
            continue
        try:
            display_name = task.path.name
            self.metrics.set_active_item("A6", active_slot, display_name)
            self.metrics.mark_stage("A6")
            decision = self._lm_decision(task.metadata)
            self.logger.info(
                "A6 decision=%s path=%s %s",
                decision,
                task.path,
                self._md_brief(task.metadata),
            )
            if decision == "full":
                if self.config.lm_fast_precheck:
                    self.metrics.mark_lm_stat("fast_request")
                fast_data = self.lm_client._fast_enrich_from_context(task)
                if self.lm_client._lm_result_is_strong(fast_data):
                    self.metrics.mark_lm_stat("fast_ok")
                    self._merge_lm_metadata(task.metadata, fast_data)
                    self.logger.info(
                        "A6 fast_ok path=%s data=%s",
                        task.path,
                        json.dumps(fast_data, ensure_ascii=False),
                    )
                else:
                    if fast_data:
                        self.metrics.mark_lm_stat("fast_fallback")
                        self.logger.info(
                            "A6 fast_fallback path=%s conf=%s data=%s",
                            task.path,
                            fast_data.get("confidence"),
                            json.dumps(fast_data, ensure_ascii=False),
                        )
                    elif self.config.lm_fast_precheck:
                        self.metrics.mark_lm_stat("fast_no_result")

                    if self.config.lm_iterative_read:
                        # ── Глубокий итеративный режим ──────────────────────
                        self.metrics.mark_lm_stat("iter_request")
                        lm_data = self.lm_client.enrich_iterative(task)
                        if lm_data:
                            self.metrics.mark_lm_stat("iter_ok")
                            self._merge_lm_metadata(task.metadata, lm_data)
                            self.logger.info(
                                "A6 iter_ok path=%s conf=%.1f data=%s",
                                task.path,
                                float(lm_data.get("confidence", 0.0)),
                                json.dumps(lm_data, ensure_ascii=False),
                            )
                        else:
                            self.metrics.mark_lm_stat("iter_no_result")
                            self.logger.info("A6 iter_no_result path=%s", task.path)
                    else:
                        # ── Одиночный запрос (стандартный режим) ────────────
                        snippet = extract_text_snippet(
                            task.path, max_chars=self.config.lm_input_chars
                        )
                        if snippet and has_meaningful_lm_text(
                            snippet, min_letters=self.config.lm_min_snippet_letters
                        ):
                            lm_input = snippet
                            lm_input_mode = "snippet"
                        elif self.config.lm_force_full_metadata or self.config.lm_always_try_without_snippet:
                            lm_input = build_lm_fallback_context(
                                task, max_chars=self.config.lm_input_chars
                            )
                            lm_input_mode = "fallback_context"
                            self.metrics.add_event(f"LM fallback-context: {display_name}")
                        else:
                            lm_input = ""
                            lm_input_mode = "none"
                            self.metrics.add_event(f"LM skip(no text): {display_name}")
                        self.logger.info(
                            "A6 full_input path=%s mode=%s chars=%d",
                            task.path,
                            lm_input_mode,
                            len(lm_input),
                        )
                        if lm_input:
                            self.metrics.mark_lm_stat("full_request")
                            lm_data = self.lm_client.enrich(task, lm_input)
                            if lm_data:
                                self.metrics.mark_lm_stat("full_ok")
                                self._merge_lm_metadata(task.metadata, lm_data)
                                self.logger.info(
                                    "A6 full_ok path=%s data=%s",
                                    task.path,
                                    json.dumps(lm_data, ensure_ascii=False),
                                )
                            else:
                                self.metrics.mark_lm_stat("full_no_result")
                                self.logger.info("A6 full_no_result path=%s", task.path)
                        else:
                            self.metrics.mark_lm_stat("full_skipped_no_input")
            elif decision == "genre_only":
                self.metrics.mark_lm_stat("genre_only_request")
                lm_data = self.lm_client.enrich_genre_only(task)
                if lm_data:
                    self.metrics.mark_lm_stat("genre_only_ok")
                    self._merge_lm_metadata(task.metadata, lm_data)
                    self.logger.info(
                        "A6 genre_ok path=%s data=%s",
                        task.path,
                        json.dumps(lm_data, ensure_ascii=False),
                    )
                else:
                    self.metrics.mark_lm_stat("genre_only_no_result")
                    self.logger.info("A6 genre_no_result path=%s", task.path)
            else:
                self.metrics.mark_lm_stat("skipped")
                self.logger.info("A6 skip path=%s reason=%s", task.path, decision)
            lm_stats = self.metrics.mark_lm_stat("processed")
            processed_lm = int(lm_stats.get("processed", 0))
            if processed_lm and processed_lm % 50 == 0:
                self.metrics.add_event(
                    "A6 stats: "
                    f"req={lm_stats.get('fast_request', 0)} "
                    f"fast={lm_stats.get('fast_ok', 0)} "
                    f"fallback={lm_stats.get('fast_fallback', 0)} "
                    f"full={lm_stats.get('full_request', 0)} "
                    f"full_ok={lm_stats.get('full_ok', 0)}"
                )
            self._put_with_stop(self.q67, task)
            self.db.mark_file(task, "lm_done", json.dumps(task.metadata.__dict__, ensure_ascii=False))
        except Exception as exc:
            self.logger.exception("Agent6: ошибка %s: %s", task.path, exc)
            self.metrics.mark_stage("A6", error=True)
            self.db.mark_file(task, "lm_failed", str(exc))
            self._put_with_stop(self.q67, task)
        finally:
            self.metrics.clear_active_item("A6", active_slot)
            self.q56.task_done()
    self.metrics.add_event(f"Agent6/W{worker_idx}: завершен")

