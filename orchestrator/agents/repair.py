from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..models import ItemStatus
from .base import AgentContext


@dataclass(slots=True)
class RepairSummary:
    scanned: int = 0
    relinked_final_paths: int = 0
    requeued_failed: int = 0
    reclassified_damaged: int = 0
    unresolved: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "scanned": self.scanned,
            "relinked_final_paths": self.relinked_final_paths,
            "requeued_failed": self.requeued_failed,
            "reclassified_damaged": self.reclassified_damaged,
            "unresolved": self.unresolved,
        }


class RepairAgent:
    name = "repair"

    def run(self, context: AgentContext) -> RepairSummary:
        summary = RepairSummary()
        output_index = self._build_output_index(context.config.paths.output_root)
        candidates = context.state_store.list_items(
            statuses=(ItemStatus.FAILED, ItemStatus.DAMAGED, ItemStatus.PLACED, ItemStatus.DUPLICATE),
        )
        for item in candidates:
            summary.scanned += 1
            if self._repair_final_path(context, item, output_index):
                summary.relinked_final_paths += 1
            if item.status == ItemStatus.FAILED and self._is_recoverable_pack_failure(item) and item.source_path.exists():
                summary.requeued_failed += 1
                continue
            if item.status == ItemStatus.DAMAGED and self._is_recoverable_pack_failure(item):
                if item.source_path.exists():
                    item.status = ItemStatus.FAILED
                    item.message = "Reclassified from damaged to failed during repair."
                    context.state_store.save_item(item)
                    context.state_store.add_event(item.item_id, self.name, item.message)
                    summary.reclassified_damaged += 1
                else:
                    summary.unresolved += 1
        return summary

    def _repair_final_path(self, context: AgentContext, item, output_index: dict[str, list[Path]]) -> bool:
        if item.final_path and item.final_path.exists():
            return False
        candidates = output_index.get(item.source_path.name, [])
        if not candidates and item.final_path is not None:
            candidates = output_index.get(item.final_path.name, [])
        if len(candidates) != 1:
            return False
        item.final_path = candidates[0]
        context.state_store.save_item(item)
        context.state_store.add_event(
            item.item_id,
            self.name,
            "Repaired final_path from output directory scan.",
            payload={"final_path": str(item.final_path)},
        )
        return True

    def _build_output_index(self, output_root: Path) -> dict[str, list[Path]]:
        index: dict[str, list[Path]] = {}
        if not output_root.exists():
            return index
        for path in output_root.rglob("*"):
            if not path.is_file():
                continue
            index.setdefault(path.name, []).append(path)
        return index

    def _is_recoverable_pack_failure(self, item) -> bool:
        return item.message == "Cannot pack empty workspace."
