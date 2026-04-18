from __future__ import annotations

from .base import AgentContext, BaseAgent


class DuplicateCheckAgent(BaseAgent):
    name = "duplicate_check"

    def run(self, context: AgentContext, item):
        if not context.config.behavior.detect_duplicates:
            return None
        if not item.source_hash:
            return None
        return context.state_store.find_source_duplicate(
            source_hash=item.source_hash,
            item_id=item.item_id,
            created_at=item.created_at,
        )
