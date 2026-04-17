from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ItemStatus(StrEnum):
    DISCOVERED = "discovered"
    UNPACKED = "unpacked"
    CLASSIFIED_FAST = "classified_fast"
    CLASSIFIED_DEEP = "classified_deep"
    PACKED = "packed"
    PLACED = "placed"
    DUPLICATE = "duplicate"
    MANUAL_REVIEW = "manual_review"
    TRASH = "trash"
    DAMAGED = "damaged"
    FAILED = "failed"


class ContainerKind(StrEnum):
    DIRECTORY = "directory"
    FILE = "file"
    ZIP = "zip"
    EPUB = "epub"
    FB2 = "fb2"
    PDF = "pdf"
    RAR = "rar"
    SEVEN_Z = "7z"
    UNKNOWN_ARCHIVE = "unknown_archive"


class QueueStage(StrEnum):
    DISCOVERY = "discovery"
    UNPACK = "unpack"
    FAST_CLASSIFY = "fast_classify"
    DEEP_CLASSIFY = "deep_classify"
    PACK = "pack"
    PLACE = "place"
    DONE = "done"


@dataclass(slots=True)
class Classification:
    author: str = ""
    title: str = ""
    genre: str = "Не распознано"
    confidence: float = 0.0
    reasoning: str = ""
    needs_deep_analysis: bool = False


@dataclass(slots=True)
class WorkItem:
    item_id: str
    source_path: Path
    source_name: str
    container_kind: ContainerKind
    status: ItemStatus = ItemStatus.DISCOVERED
    author: str = ""
    title: str = ""
    genre: str = "Не распознано"
    confidence: float = 0.0
    source_hash: str = ""
    packed_hash: str = ""
    unpack_dir: Path | None = None
    packed_path: Path | None = None
    final_path: Path | None = None
    message: str = ""
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)


@dataclass(slots=True)
class ResourceSnapshot:
    disk_used_percent: float
    io_busy_percent: float
    cpu_percent: float
    sampled_at: str = field(default_factory=utc_now)
