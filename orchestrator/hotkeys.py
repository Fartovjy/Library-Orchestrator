from __future__ import annotations

import sys

if sys.platform == "win32":
    import msvcrt
else:  # pragma: no cover
    msvcrt = None


class StopHotkeyWatcher:
    def __init__(self) -> None:
        self.last_trigger: str = ""

    def poll_stop_requested(self) -> bool:
        if msvcrt is None:
            return False
        triggered = False
        while msvcrt.kbhit():
            key = msvcrt.getch()
            if key in {b"\x18", b"\x1b", b"q", b"Q"}:
                self.last_trigger = self._describe_key(key)
                triggered = True
        return triggered

    def hint(self) -> str:
        if msvcrt is None:
            return "Use CLI stop command"
        return "Ctrl+X or Esc or Q = safe stop"

    def _describe_key(self, key: bytes) -> str:
        if key == b"\x18":
            return "Ctrl+X"
        if key == b"\x1b":
            return "Esc"
        return "Q"
