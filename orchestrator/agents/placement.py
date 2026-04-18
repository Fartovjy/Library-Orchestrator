from __future__ import annotations

import shutil
from pathlib import Path

from ..archive_adapters import author_initial, sanitize_name
from ..models import ItemStatus
from .base import AgentContext, BaseAgent


class PlacementAgent(BaseAgent):
    name = "placement"

    def run(self, context: AgentContext, item):
        if item.packed_path is None:
            raise RuntimeError("Cannot place item without packed archive.")

        duplicate = None
        if context.config.behavior.detect_duplicates:
            duplicate = context.state_store.find_duplicate(item.packed_hash)
        if duplicate is not None and duplicate["item_id"] != item.item_id:
            target_dir = context.config.paths.duplicates_root
            item.status = ItemStatus.DUPLICATE
            item.message = f"Duplicate of {duplicate['item_id']}"
        elif item.genre == "Не распознано":
            target_dir = context.config.paths.manual_review_root
            item.status = ItemStatus.MANUAL_REVIEW
            item.message = "Moved to manual review because genre is unresolved."
        else:
            safe_author = sanitize_name(item.author or "Unknown Author")
            target_dir = (
                context.config.paths.library_root
                / sanitize_name(item.genre)
                / sanitize_name(author_initial(safe_author))
                / safe_author
            )
            item.status = ItemStatus.PLACED
            item.message = "Placed into target library tree."

        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = self._unique_target_path(target_dir / item.packed_path.name)
        if context.config.behavior.move_outputs:
            shutil.move(str(item.packed_path), target_path)
        else:
            shutil.copy2(item.packed_path, target_path)
            if item.packed_path.exists():
                item.packed_path.unlink(missing_ok=True)

        item.final_path = target_path
        context.state_store.save_item(item)
        context.state_store.register_hash(item.packed_hash, item.item_id, item.final_path)
        context.state_store.add_event(
            item.item_id,
            self.name,
            item.message,
            payload={"final_path": str(target_path)},
        )
        return item

    def _unique_target_path(self, target_path: Path) -> Path:
        if not target_path.exists():
            return target_path
        stem = target_path.stem
        suffix = target_path.suffix
        parent = target_path.parent
        index = 2
        while True:
            candidate = parent / f"{stem} ({index}){suffix}"
            if not candidate.exists():
                return candidate
            index += 1
