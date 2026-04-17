from __future__ import annotations

from ..archive_adapters import collect_excerpt, is_supported_unpack_kind, unpack_source
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
            return item, ""

        unpack_dir = context.workspace_root / item.item_id
        item.unpack_dir = unpack_source(item.source_path, unpack_dir)
        item.status = ItemStatus.UNPACKED
        item.message = "Source unpacked into workspace."
        context.state_store.save_item(item)
        context.state_store.add_event(item.item_id, self.name, item.message)
        excerpt_source = item.unpack_dir or item.source_path
        excerpt = collect_excerpt(excerpt_source, context.config.lmstudio.fast_excerpt_words)
        return item, excerpt
