from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..config import AppConfig
from ..lmstudio import LmStudioClient
from ..queues import StageQueues
from ..resource_monitor import ResourceMonitor
from ..state_store import StateStore


@dataclass(slots=True)
class AgentContext:
    config: AppConfig
    state_store: StateStore
    resource_monitor: ResourceMonitor
    lmstudio: LmStudioClient
    queues: StageQueues

    @property
    def workspace_root(self) -> Path:
        return self.config.paths.workspace_root


class BaseAgent:
    name = "base"

    def run(self, context: AgentContext, item):  # pragma: no cover
        raise NotImplementedError
