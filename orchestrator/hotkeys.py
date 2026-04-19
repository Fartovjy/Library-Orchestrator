from __future__ import annotations

from dataclasses import dataclass
import ctypes
import sys

if sys.platform == "win32":
    user32 = ctypes.windll.user32
else:  # pragma: no cover
    user32 = None


VK_CONTROL = 0x11
VK_S = 0x53


@dataclass(slots=True)
class HotkeyAction:
    action: str
    label: str


class RuntimeHotkeyWatcher:
    def __init__(self) -> None:
        self.last_trigger: str = ""
        self._stop_pressed = False

    def poll_action(self) -> HotkeyAction | None:
        if user32 is None:
            return None
        stop_pressed = self._is_pressed(VK_CONTROL) and self._is_pressed(VK_S)

        if stop_pressed and not self._stop_pressed:
            action = HotkeyAction("full_stop", "Ctrl+S")
            self.last_trigger = action.label
            self._stop_pressed = True
            return action

        self._stop_pressed = stop_pressed
        return None

    def hint(self) -> str:
        if user32 is None:
            return "full stop via CLI"
        return "Ctrl+S = full stop batch"

    def _is_pressed(self, virtual_key: int) -> bool:
        return bool(user32.GetAsyncKeyState(virtual_key) & 0x8000)
