from __future__ import annotations

from ..models import ItemStatus
from .base import AgentContext, BaseAgent


class ArchivariusAgent(BaseAgent):
    name = "archivarius"

    def run(self, context: AgentContext, item, excerpt: str):
        classification = context.lmstudio.classify_book(
            filename=item.source_name,
            excerpt=excerpt,
            allowed_genres=context.config.behavior.allowed_genres,
            deep=False,
        )
        item.author = classification.author
        item.title = classification.title or item.source_path.stem
        item.genre = classification.genre
        item.confidence = classification.confidence
        item.status = ItemStatus.CLASSIFIED_FAST
        item.message = classification.reasoning or "Fast classification complete."
        context.state_store.save_item(item)
        context.state_store.add_event(
            item.item_id,
            self.name,
            item.message,
            payload={
                "genre": item.genre,
                "confidence": item.confidence,
                "needs_deep_analysis": classification.needs_deep_analysis,
            },
        )
        return item, classification.needs_deep_analysis or item.confidence < 0.7
