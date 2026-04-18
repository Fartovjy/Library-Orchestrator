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
            return "pause/resume via CLI, stop via CLI"
        return "Ctrl+X / Esc / Q = pause-resume | Ctrl+S = safe stop"

    def _map_key(self, key: bytes) -> HotkeyAction | None:
        if key in {b"\x18", b"\x1b", b"q", b"Q"}:
            return HotkeyAction("pause_toggle", self._describe_pause_key(key))
        if key == b"\x13":
            return HotkeyAction("stop", "Ctrl+S")
        return None

    def _describe_pause_key(self, key: bytes) -> str:
        if key == b"\x18":
            return "Ctrl+X"
        if key == b"\x1b":
            return "Esc"
        return "Q"
