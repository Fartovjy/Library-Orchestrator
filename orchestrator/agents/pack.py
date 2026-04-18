from __future__ import annotations

from ..archive_adapters import compute_sha256, normalize_title, pack_directory_to_zip, sanitize_name
from ..models import ItemStatus
from .base import AgentContext, BaseAgent


class PackAgent(BaseAgent):
    name = "pack"

    def run(self, context: AgentContext, item):
        author = sanitize_name(item.author or "Unknown Author")
        title = sanitize_name(normalize_title(item.title or item.source_path.stem))
        output_name = f"{author} - {title}.zip"
        staging_root = context.config.paths.workspace_root / "_packed" / item.item_id
        staging_root.mkdir(parents=True, exist_ok=True)
        output_path = staging_root / output_name
        if item.unpack_dir is None:
            raise RuntimeError("Cannot pack item without unpack_dir.")
        if not any(path.is_file() for path in item.unpack_dir.rglob("*")):
            raise RuntimeError("Cannot pack empty workspace.")
        pack_directory_to_zip(item.unpack_dir, output_path)
        item.packed_path = output_path
        item.packed_hash = compute_sha256(output_path)
        item.status = ItemStatus.PACKED
        item.message = "Workspace normalized into ZIP."
        context.state_store.save_item(item)
        context.state_store.add_event(
            item.item_id,
            self.name,
            item.message,
            payload={"packed_path": str(output_path), "packed_hash": item.packed_hash},
        )
        return item
