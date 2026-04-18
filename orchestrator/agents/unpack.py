from __future__ import annotations

from ..archive_adapters import is_supported_unpack_kind, unpack_source
from ..models import ItemStatus
from .base import AgentContext, BaseAgent


class UnpackAgent(BaseAgent):
    name = "unpack"

    def run(self, context: AgentContext, item):
        if not is_supported_unpack_kind(item.container_kind):
            item.status = ItemStatus.MANUAL_REVIEW
            item.message = f"Unsupported container kind: {item.container_kind.value}"
            context.state_store.save_item(item)
            context.state_store.add_event(item.item_id, self.name, item.message)
            return item

        unpack_dir = context.workspace_root / item.item_id
        item.unpack_dir, nested_count = unpack_source(
            item.source_path,
            unpack_dir,
            max_nested_depth=context.config.limits.max_nested_archive_depth,
        )
        item.status = ItemStatus.UNPACKED
        if nested_count > 0:
            item.message = f"Source unpacked into workspace. Expanded {nested_count} nested archive(s)."
        else:
            item.message = "Source unpacked into workspace."
        context.state_store.save_item(item)
        context.state_store.add_event(
            item.item_id,
            self.name,
            item.message,
            payload={"nested_archives_expanded": nested_count},
        )
        return item
