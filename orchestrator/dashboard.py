from __future__ import annotations

import shutil
import sys
import time
import threading
import ctypes
from dataclasses import dataclass, field
from pathlib import Path


AGENT_ORDER = [
    "discovery",
    "unpack",
    "splitter",
    "prepare",
    "archivarius",
    "expert",
    "pack",
    "placement",
]


@dataclass(slots=True)
class DashboardState:
    total_items: int = 0
    processed_items: int = 0
    current_item: str = "-"
    current_stage: str = "idle"
    last_message: str = "-"
    started_at: float = field(default_factory=time.monotonic)
    agent_done_counts: dict[str, int] = field(default_factory=lambda: {name: 0 for name in AGENT_ORDER})
    agent_total_counts: dict[str, int] = field(default_factory=lambda: {name: 0 for name in AGENT_ORDER})
    agent_recognition_avgs: dict[str, float] = field(default_factory=dict)
    active_agent_counts: dict[str, int] = field(default_factory=lambda: {name: 0 for name in AGENT_ORDER})
    status_counts: dict[str, int] = field(default_factory=dict)
    active_agents: tuple[str, ...] = ()
    hotkey_hint: str = "-"


class TerminalDashboard:
    def __init__(self) -> None:
        self.state = DashboardState()
        self._lock = threading.RLock()
        self._ansi_ready = self._enable_ansi_if_possible()

    def set_total(self, total_items: int) -> None:
        with self._lock:
            self.state.total_items = total_items
            self.render()

    def sync_progress(
        self,
        total_items: int,
        processed_items: int,
        stage_totals: dict[str, int],
        stage_done: dict[str, int],
        status_counts: dict[str, int],
        recognition_avgs: dict[str, float] | None = None,
        message: str = "",
    ) -> None:
        with self._lock:
            self.state.total_items = total_items
            self.state.processed_items = processed_items
            self.state.status_counts = dict(status_counts)
            for agent_name in AGENT_ORDER:
                self.state.agent_total_counts[agent_name] = int(stage_totals.get(agent_name, 0))
                self.state.agent_done_counts[agent_name] = int(stage_done.get(agent_name, 0))
            self.state.agent_recognition_avgs = dict(recognition_avgs or {})
            if message:
                self.state.last_message = message
            self._refresh_active_agents()
            self.render()

    def set_hotkey_hint(self, hint: str) -> None:
        with self._lock:
            self.state.hotkey_hint = hint
            self.render()

    def set_current(self, item: Path, stage: str, message: str = "") -> None:
        with self._lock:
            self.state.current_item = str(item)
            self.state.current_stage = stage
            if message:
                self.state.last_message = message
            self._refresh_active_agents()
            self.render()

    def begin_agent(self, item: Path, agent_name: str, message: str = "") -> None:
        with self._lock:
            self.state.current_item = str(item)
            self.state.current_stage = agent_name
            if agent_name in self.state.active_agent_counts:
                self.state.active_agent_counts[agent_name] += 1
            if message:
                self.state.last_message = message
            self._refresh_active_agents()
            self.render()

    def end_agent(self, agent_name: str, message: str = "") -> None:
        with self._lock:
            if agent_name in self.state.active_agent_counts:
                self.state.active_agent_counts[agent_name] = max(
                    self.state.active_agent_counts[agent_name] - 1,
                    0,
                )
            if message:
                self.state.last_message = message
            self._refresh_active_agents()
            self.render()

    def render(self) -> None:
        width = shutil.get_terminal_size((100, 30)).columns
        total = max(self.state.total_items, 0)
        done = min(self.state.processed_items, total) if total else self.state.processed_items
        percent = (done / total) if total else 0.0
        progress_bar = self._progress_bar(percent, width=min(max(width - 30, 10), 40))
        remaining = max(total - done, 0)
        elapsed = int(time.monotonic() - self.state.started_at)
        lines = [
            f"Library Orchestrator | done {done}/{total} | remain {remaining} | elapsed {elapsed}s",
            f"{progress_bar} {percent * 100:6.2f}%",
            "",
            f"Active agents : {self._active_agents_line(width - 16)}",
            f"Controls      : {self._truncate(self.state.hotkey_hint, width - 16)}",
            f"Current stage : {self.state.current_stage}",
            f"Current item  : {self._truncate(self.state.current_item, width - 16)}",
            f"Last message  : {self._truncate(self.state.last_message, width - 16)}",
            "",
            self._agent_table(width),
            "",
            self._status_line(width),
        ]
        frame = "\n".join(lines) + "\n"
        if self._ansi_ready:
            sys.stdout.write("\x1b[H\x1b[J" + frame)
        else:
            sys.stdout.write("\r" + frame)
        sys.stdout.flush()

    def _agent_table(self, width: int) -> str:
        name_width = 12
        count_samples = ["done"]
        for agent_name in AGENT_ORDER:
            count = self.state.agent_done_counts.get(agent_name, 0)
            denominator = max(self.state.agent_total_counts.get(agent_name, 0), 0)
            count_samples.append(f"{count}/{denominator}")
        count_width = max(13, max(len(sample) for sample in count_samples) + 2)
        percent_width = 9
        recognized_width = 12
        bar_width = min(max(width - (name_width + count_width + percent_width + recognized_width + 16), 10), 30)
        header = (
            f"+{'-' * name_width}+{'-' * count_width}+{'-' * percent_width}+{'-' * recognized_width}+{'-' * (bar_width + 2)}+"
        )
        lines = [
            header,
            f"| {'agent'.ljust(name_width - 1)}| {'done'.ljust(count_width - 1)}| {'percent'.ljust(percent_width - 1)}| {'recognized'.ljust(recognized_width - 1)}| {'progress'.ljust(bar_width + 1)}|",
            header,
        ]
        for agent_name in AGENT_ORDER:
            count = self.state.agent_done_counts.get(agent_name, 0)
            denominator = max(self.state.agent_total_counts.get(agent_name, 0), 0)
            percent = min(count / denominator, 1.0) if denominator else 0.0
            bar = self._progress_bar(percent, width=bar_width, fill="#", empty=".")
            recognition_avg = self.state.agent_recognition_avgs.get(agent_name)
            if recognition_avg is not None:
                recognition_text = f"{recognition_avg * 100:5.1f}%"
            else:
                recognition_text = "-"
            count_text = f"{count}/{denominator}"
            percent_text = f"{percent * 100:5.1f}%"
            lines.append(
                f"| {agent_name.ljust(name_width - 1)}| {count_text.ljust(count_width - 1)}| {percent_text.ljust(percent_width - 1)}| {recognition_text.ljust(recognized_width - 1)}| {bar.ljust(bar_width + 1)}|"
            )
        lines.append(header)
        return "\n".join(lines)

    def _status_line(self, width: int) -> str:
        if not self.state.status_counts:
            return "Statuses      : no processed items yet"
        parts = [f"{name}={count}" for name, count in sorted(self.state.status_counts.items())]
        return f"Statuses      : {self._truncate(', '.join(parts), width - 16)}"

    def _active_agents_line(self, width: int) -> str:
        total_active = sum(self.state.active_agent_counts.values())
        if total_active <= 0:
            return "0"
        parts = []
        for agent_name in AGENT_ORDER:
            count = self.state.active_agent_counts.get(agent_name, 0)
            if count <= 0:
                continue
            parts.append(f"{agent_name} x{count}")
        value = f"{total_active} ({', '.join(parts)})"
        return self._truncate(value, width)

    def _refresh_active_agents(self) -> None:
        active = []
        for agent_name in AGENT_ORDER:
            count = self.state.active_agent_counts.get(agent_name, 0)
            active.extend([agent_name] * max(count, 0))
        self.state.active_agents = tuple(active)

    def _progress_bar(self, percent: float, width: int, fill: str = "=", empty: str = "-") -> str:
        filled = int(round(width * max(0.0, min(percent, 1.0))))
        return "[" + (fill * filled) + (empty * max(width - filled, 0)) + "]"

    def _truncate(self, value: str, width: int) -> str:
        if width < 5:
            return value[:width]
        if len(value) <= width:
            return value
        return value[: width - 3] + "..."

    def _enable_ansi_if_possible(self) -> bool:
        if sys.platform != "win32":
            return True
        try:
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.GetStdHandle(-11)
            mode = ctypes.c_uint32()
            if kernel32.GetConsoleMode(handle, ctypes.byref(mode)) == 0:
                return False
            enable_vt = 0x0004
            if kernel32.SetConsoleMode(handle, mode.value | enable_vt) == 0:
                return False
            return True
        except Exception:
            return False
