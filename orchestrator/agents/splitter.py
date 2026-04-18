from __future__ import annotations

from ..archive_adapters import detect_book_candidates, detect_container_kind
from ..models import ItemStatus
from .base import AgentContext, BaseAgent


class SplitterAgent(BaseAgent):
    name = "splitter"

    def run(self, context: AgentContext, item):
        if item.unpack_dir is None:
            raise RuntimeError("Cannot split container without unpack_dir.")

        candidates = detect_book_candidates(item.unpack_dir)
        if not candidates:
            item.message = "Container kept as a single book candidate."
            context.state_store.add_event(item.item_id, self.name, item.message)
            return item, []

        child_items = []
        for candidate_path in candidates:
            child_item = context.state_store.create_child_item(
                item,
                candidate_path,
                detect_container_kind(candidate_path),
            )
            child_items.append(child_item)

        item.status = ItemStatus.SPLIT
        item.message = f"Container split into {len(child_items)} child book item(s)."
        context.state_store.save_item(item)
        context.state_store.add_event(
            item.item_id,
            self.name,
            item.message,
            payload={"child_item_ids": [child.item_id for child in child_items]},
        )
        return item, child_items
