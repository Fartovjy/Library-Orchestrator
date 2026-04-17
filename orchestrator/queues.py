from __future__ import annotations

import threading
from collections import defaultdict, deque

from .models import QueueStage


class StageQueues:
    def __init__(self) -> None:
        self._queues: dict[QueueStage, deque[str]] = defaultdict(deque)
        self._lock = threading.RLock()

    def enqueue(self, stage: QueueStage, item_id: str) -> None:
        with self._lock:
            self._queues[stage].append(item_id)

    def dequeue(self, stage: QueueStage) -> str | None:
        with self._lock:
            queue = self._queues[stage]
            if not queue:
                return None
            return queue.popleft()

    def counts(self) -> dict[str, int]:
        with self._lock:
            return {stage.value: len(items) for stage, items in self._queues.items()}
