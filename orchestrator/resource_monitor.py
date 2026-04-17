from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from shutil import disk_usage as stdlib_disk_usage

try:
    import psutil
except ModuleNotFoundError:  # pragma: no cover - optional dependency fallback
    psutil = None

from .models import ResourceSnapshot


@dataclass(slots=True)
class _DiskCountersSample:
    busy_ms: float
    timestamp: float


class ResourceMonitor:
    def __init__(self, source_root: Path, busy_threshold_percent: float) -> None:
        self.source_root = source_root
        self.busy_threshold_percent = busy_threshold_percent
        self._previous: _DiskCountersSample | None = None

    def snapshot(self) -> ResourceSnapshot:
        if psutil is None:
            usage = stdlib_disk_usage(self.source_root)
            disk_used_percent = round((usage.used / usage.total) * 100.0, 2) if usage.total else 0.0
            cpu_percent = 0.0
            io_busy_percent = 0.0
        else:
            usage = psutil.disk_usage(str(self.source_root.drive or self.source_root))
            disk_used_percent = usage.percent
            cpu_percent = psutil.cpu_percent(interval=0.1)
            io_busy_percent = self._sample_disk_busy_percent()
        return ResourceSnapshot(
            disk_used_percent=disk_used_percent,
            io_busy_percent=io_busy_percent,
            cpu_percent=cpu_percent,
        )

    def should_throttle(self) -> bool:
        snapshot = self.snapshot()
        return snapshot.io_busy_percent >= self.busy_threshold_percent

    def _sample_disk_busy_percent(self) -> float:
        if psutil is None:
            return 0.0
        counters = psutil.disk_io_counters()
        now = time.monotonic()
        current = _DiskCountersSample(
            busy_ms=float(counters.read_time + counters.write_time),
            timestamp=now,
        )
        if self._previous is None:
            self._previous = current
            return 0.0
        elapsed_ms = max((current.timestamp - self._previous.timestamp) * 1000.0, 1.0)
        busy_ms = max(current.busy_ms - self._previous.busy_ms, 0.0)
        self._previous = current
        return min((busy_ms / elapsed_ms) * 100.0, 100.0)
