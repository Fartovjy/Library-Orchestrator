#!/usr/bin/env python3
"""Entry point for the library sorting GUI."""

from __future__ import annotations

from ui import LibraryGUIApp, create_root


def main() -> int:
    root, dnd_available = create_root()
    LibraryGUIApp(root, dnd_available)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
