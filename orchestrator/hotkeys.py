from __future__ import annotations

from dataclasses import dataclass
import sys

if sys.platform == "win32":
    import msvcrt
else:  # pragma: no cover
    msvcrt = None


@dataclass(slots=True)
class HotkeyAction:
    action: str
    label: str


class RuntimeHotkeyWatcher:
    def __init__(self) -> None:
        self.last_trigger: str = ""

    def poll_action(self) -> HotkeyAction | None:
        if msvcrt is None:
            return None
        detected: HotkeyAction | None = None
        while msvcrt.kbhit():
            key = msvcrt.getch()
            action = self._map_key(key)
            if action is None:
                continue
            self.last_trigger = action.label
            detected = action
        return detected

    def hint(self) -> str:
        if msvcrt is None:
            return "pause via CLI, full stop via CLI"
        return "Esc = pause current run | Ctrl+S = full stop batch"

    def _map_key(self, key: bytes) -> HotkeyAction | None:
        if key == b"\x1b":
            return HotkeyAction("pause_run", "Esc")
        if key == b"\x13":
            return HotkeyAction("full_stop", "Ctrl+S")
        return None
