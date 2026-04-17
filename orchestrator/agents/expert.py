from __future__ import annotations

from ..archive_adapters import collect_excerpt
from ..models import ItemStatus
from .base import AgentContext, BaseAgent


class ExpertAgent(BaseAgent):
    name = "expert"

    def run(self, context: AgentContext, item):
        excerpt = collect_excerpt(item.source_path, context.config.lmstudio.deep_excerpt_words)
        classification = context.lmstudio.classify_book(
            filename=item.source_name,
            excerpt=excerpt,
            allowed_genres=context.config.behavior.allowed_genres,
            deep=True,
        )
        item.author = classification.author or item.author
        item.title = classification.title or item.title or item.source_path.stem
        item.genre = classification.genre or item.genre
        item.confidence = max(item.confidence, classification.confidence)
        item.status = ItemStatus.CLASSIFIED_DEEP
        item.message = classification.reasoning or "Deep classification complete."
        context.state_store.save_item(item)
        context.state_store.add_event(
            item.item_id,
            self.name,
            item.message,
            payload={"genre": item.genre, "confidence": item.confidence},
        )
        return item
