#!/usr/bin/env python3
"""
GUI dashboard for the library sorting pipeline.

Requirements requested by user:
- compact static window (<= 960x540)
- font size >= 12
- color separation for A1..A8 agents
- drag-and-drop zone only for SOURCE_DIRS field
- browse buttons for SOURCE_DIRS and TARGET_DIR
- path validation + auto-create directories
- stop/cleanup control from GUI
- remember last TARGET_DIR in setting.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Optional

import library_pipeline as lp

try:
    import setting
except Exception:
    setting = None

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except Exception:
    DND_FILES = None
    TkinterDnD = None


WINDOW_MAX_WIDTH = 960
WINDOW_MAX_HEIGHT = 540
WINDOW_MIN_WIDTH = 640
WINDOW_MIN_HEIGHT = 420

AGENT_COLORS_DARK = {
    "A1": ("#0f4c5c", "#f1f5f9"),
    "A2": ("#33658a", "#f1f5f9"),
    "A3": ("#2f855a", "#f1f5f9"),
    "A4": ("#b7791f", "#fdf5e6"),
    "A5": ("#7b341e", "#fff7ed"),
    "A6": ("#6b46c1", "#f5f3ff"),
    "A7": ("#0f766e", "#f0fdfa"),
    "A8": ("#1f2937", "#f9fafb"),
}

AGENT_COLORS_LIGHT = {
    "A1": ("#bfe6f2", "#0b2a33"),
    "A2": ("#c7def5", "#10243f"),
    "A3": ("#cbeed9", "#173525"),
    "A4": ("#f4e3bf", "#4a3310"),
    "A5": ("#f2d1c4", "#4a2014"),
    "A6": ("#ddcef8", "#2f1f4f"),
    "A7": ("#c7ece8", "#0f3a37"),
    "A8": ("#d6dce7", "#1f2937"),
}

THEME_DARK = {
    "root_bg": "#0b1220",
    "panel_bg": "#111827",
    "ctrl_bg": "#0f172a",
    "drop_bg": "#1f2937",
    "drop_border": "#334155",
    "text_primary": "#f8fafc",
    "text_secondary": "#cbd5e1",
    "text_muted": "#94a3b8",
    "text_status": "#f8fafc",
    "mode_fg": "#93c5fd",
    "legend_fg": "#6b7280",
    "btn_secondary_bg": "#334155",
    "btn_secondary_fg": "#f8fafc",
    "btn_secondary_active_bg": "#475569",
    "btn_secondary_active_fg": "#f8fafc",
    "btn_check_bg": "#166534",
    "btn_check_fg": "#f0fdf4",
    "btn_check_active_bg": "#15803d",
    "btn_check_active_fg": "#f0fdf4",
    "btn_start_bg": "#16a34a",
    "btn_start_fg": "#f0fdf4",
    "btn_start_active_bg": "#15803d",
    "btn_start_active_fg": "#f0fdf4",
    "btn_stop_bg": "#b91c1c",
    "btn_stop_fg": "#fef2f2",
    "btn_stop_active_bg": "#991b1b",
    "btn_stop_active_fg": "#fef2f2",
    "progress_trough": "#1f2937",
    "progress_bg": "#22c55e",
    "progress_border": "#334155",
    "progress_dark": "#16a34a",
}

THEME_LIGHT = {
    "root_bg": "#f3f6fb",
    "panel_bg": "#e9eef6",
    "ctrl_bg": "#dfe7f3",
    "drop_bg": "#d7e3f4",
    "drop_border": "#a9bdd7",
    "text_primary": "#0f172a",
    "text_secondary": "#334155",
    "text_muted": "#64748b",
    "text_status": "#0f172a",
    "mode_fg": "#1d4ed8",
    "legend_fg": "#64748b",
    "btn_secondary_bg": "#c4d4ea",
    "btn_secondary_fg": "#0f172a",
    "btn_secondary_active_bg": "#afc3df",
    "btn_secondary_active_fg": "#0f172a",
    "btn_check_bg": "#0f766e",
    "btn_check_fg": "#ecfeff",
    "btn_check_active_bg": "#0d9488",
    "btn_check_active_fg": "#ecfeff",
    "btn_start_bg": "#16a34a",
    "btn_start_fg": "#f0fdf4",
    "btn_start_active_bg": "#15803d",
    "btn_start_active_fg": "#f0fdf4",
    "btn_stop_bg": "#dc2626",
    "btn_stop_fg": "#fef2f2",
    "btn_stop_active_bg": "#b91c1c",
    "btn_stop_active_fg": "#fef2f2",
    "progress_trough": "#d5dde8",
    "progress_bg": "#16a34a",
    "progress_border": "#9fb3cc",
    "progress_dark": "#15803d",
}

LANGUAGE_FILES = {
    "ru": "ui_ru.json",
    "en": "ui_en.json",
}


def _windows_is_dark_mode() -> Optional[bool]:
    if sys.platform != "win32":
        return None
    try:
        import winreg  # type: ignore

        key_path = r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:
            value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
        return int(value) == 0
    except Exception:
        return None


def _macos_is_dark_mode() -> Optional[bool]:
    if sys.platform != "darwin":
        return None
    try:
        proc = subprocess.run(
            ["defaults", "read", "-g", "AppleInterfaceStyle"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        return "Dark" in (proc.stdout or "")
    except Exception:
        return None


def detect_system_theme_mode() -> str:
    for detector in (_windows_is_dark_mode, _macos_is_dark_mode):
        value = detector()
        if value is None:
            continue
        return "dark" if value else "light"
    # Conservative fallback: keep previous visual style.
    return "dark"


def lighten_hex(color: str, factor: float = 0.3) -> str:
    if not isinstance(color, str):
        return color
    raw = color.strip().lstrip("#")
    if len(raw) != 6:
        return color
    try:
        r = int(raw[0:2], 16)
        g = int(raw[2:4], 16)
        b = int(raw[4:6], 16)
    except Exception:
        return color
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return f"#{r:02x}{g:02x}{b:02x}"


def fix_mojibake(text: str) -> str:
    """
    Восстанавливает типичный mojibake в строках.
    Для нормального UTF-8 текста безопасно возвращает исходную строку.
    """
    if not isinstance(text, str) or not text:
        return text
    try:
        fixed = text.encode("cp1251").decode("utf-8")
        if fixed:
            return fixed
    except Exception:
        pass
    return text


class _NoopController:
    def start(self) -> None:
        return

    def stop(self) -> None:
        return


class LibraryGUIApp:
    def __init__(self, root: tk.Tk, dnd_available: bool) -> None:
        self.root = root
        self.dnd_available = dnd_available
        self.theme_mode = detect_system_theme_mode()
        self.palette = THEME_DARK if self.theme_mode == "dark" else THEME_LIGHT
        self.agent_colors = (
            AGENT_COLORS_DARK if self.theme_mode == "dark" else AGENT_COLORS_LIGHT
        )
        self.translations = self._load_translations()
        self.language = self._default_language()
        self.last_snapshot: Optional[dict] = None
        self.current_mode = "IDLE"
        self.current_log_path: object = "-"
        self.status_key = "status_ready"
        self.status_kwargs: dict[str, object] = {}
        self.shutdown_started = False

        font_family = "Segoe UI"
        if setting is not None:
            try:
                candidate = str(getattr(setting, "GUI_FONT_FAMILY", font_family)).strip()
                if candidate:
                    font_family = candidate
            except Exception:
                pass
        self.font_main = (
            font_family,
            self._setting_int("GUI_FONT_MAIN_SIZE", 12, min_value=8),
        )
        self.font_title = (
            font_family,
            self._setting_int("GUI_FONT_TITLE_SIZE", 15, min_value=8),
            "bold",
        )
        self.font_small = (
            font_family,
            self._setting_int("GUI_FONT_SMALL_SIZE", 12, min_value=8),
        )
        self.font_drop_hint = (
            font_family,
            9,
        )
        self.font_stats = (
            font_family,
            self._setting_int("GUI_FONT_STATS_SIZE", 11, min_value=8),
        )
        self.font_counter_label = (
            font_family,
            self._setting_int("GUI_FONT_COUNTER_LABEL_SIZE", 9, min_value=6),
        )
        self.font_counter_value = (
            font_family,
            self._setting_int("GUI_FONT_STATS_SIZE", 11, min_value=8),
            "bold",
        )
        self.font_legend = (
            font_family,
            self._setting_int("GUI_FONT_LEGEND_SIZE", 9, min_value=6),
        )
        self.font_agent_title = (
            font_family,
            self._setting_int("GUI_FONT_AGENT_TITLE_SIZE", 11, min_value=8),
            "bold",
        )
        self.font_agent_value = (
            font_family,
            self._setting_int("GUI_FONT_AGENT_VALUE_SIZE", 10, min_value=8),
        )
        self.font_agent_metric_label = (
            font_family,
            self._setting_int("GUI_FONT_AGENT_METRIC_LABEL_SIZE", 9, min_value=6),
        )
        self.font_agent_metric_value = (
            font_family,
            self._setting_int("GUI_FONT_AGENT_VALUE_SIZE", 10, min_value=8),
            "bold",
        )

        width = self._setting_int("GUI_WINDOW_WIDTH", WINDOW_MAX_WIDTH, min_value=WINDOW_MIN_WIDTH)
        height = self._setting_int(
            "GUI_WINDOW_HEIGHT", WINDOW_MAX_HEIGHT, min_value=WINDOW_MIN_HEIGHT
        )
        self.window_width = min(WINDOW_MAX_WIDTH, width)
        self.window_height = min(WINDOW_MAX_HEIGHT, height)
        self.outer_pad = 8
        self.module_width = self.window_width - (self.outer_pad * 2)
        self.agent_cell_width = max(210, self.module_width // 4)
        self.browse_cell_width = 96

        self.sorter: Optional[lp.LibrarySorter] = None
        self.pipeline_thread: Optional[threading.Thread] = None
        self.pipeline_running = False
        self.pipeline_exit_code: Optional[int] = None
        self.pipeline_error = ""

        # По запросу: при запуске GUI выбор SOURCE_DIRS всегда пустой.
        self.source_var = tk.StringVar(value="")
        self.target_var = tk.StringVar(value=fix_mojibake(str(lp.DEFAULT_TARGET_DIR)))
        self.mode_var = tk.StringVar(value=self._mode_label("IDLE"))
        self.time_var = tk.StringVar(value="00:00:00/~--:--:--")
        self.status_var = tk.StringVar(value=self.tr("status_ready"))
        self.progress_text_var = tk.StringVar(value="0%")
        self.seen_var = tk.StringVar(value="0")
        self.done_var = tk.StringVar(value="0")
        self.packed_var = tk.StringVar(value="0")
        self.dupes_var = tk.StringVar(value="0")
        self.nobook_var = tk.StringVar(value="0")
        self.failed_var = tk.StringVar(value="0")
        self.event_var = tk.StringVar(value=f"{self.tr('events_prefix')}: -")
        self.log_var = tk.StringVar(value=f"{self.tr('log_prefix')}: -")
        self.shutdown_after_done_var = tk.BooleanVar(value=False)

        self.agent_processed: dict[str, tk.StringVar] = {}
        self.agent_errors: dict[str, tk.StringVar] = {}
        self.agent_queue: dict[str, tk.StringVar] = {}
        self.agent_cards: dict[str, tk.Frame] = {}
        self.agent_labels: dict[str, list[tk.Widget]] = {}
        self.stat_label_widgets: dict[str, tk.Label] = {}
        self.agent_title_labels: dict[str, tk.Label] = {}
        self.agent_last_processed: dict[str, int] = {k: 0 for k in lp.AGENT_KEYS}
        self.agent_active_until: dict[str, float] = {k: 0.0 for k in lp.AGENT_KEYS}
        self.agent_active_bg: dict[str, str] = {
            k: lighten_hex(self.agent_colors[k][0], 0.32) for k in lp.AGENT_KEYS
        }

        self._build_window()
        self._build_ui()
        self._apply_initial_dirs()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(300, self._poll_pipeline)

    def _load_translations(self) -> dict[str, dict[str, str]]:
        base = Path(__file__).resolve().parent
        loaded: dict[str, dict[str, str]] = {}
        for lang, filename in LANGUAGE_FILES.items():
            path = base / filename
            try:
                data = json.loads(path.read_text(encoding="utf-8-sig"))
            except Exception:
                data = {}
            loaded[lang] = {str(k): str(v) for k, v in data.items()}
        return loaded

    def _default_language(self) -> str:
        value = "ru"
        if setting is not None:
            try:
                value = str(getattr(setting, "GUI_DEFAULT_LANGUAGE", value)).strip().lower()
            except Exception:
                value = "ru"
        return value if value in LANGUAGE_FILES else "ru"

    def tr(self, key: str, **kwargs: object) -> str:
        text = (
            self.translations.get(self.language, {}).get(key)
            or self.translations.get("ru", {}).get(key)
            or key
        )
        if kwargs:
            try:
                return text.format(**kwargs)
            except Exception:
                return text
        return text

    def _set_status(self, key: str, **kwargs: object) -> None:
        self.status_key = key
        self.status_kwargs = dict(kwargs)
        self.status_var.set(self.tr(key, **kwargs))

    def _set_language(self, language: str) -> None:
        if language not in LANGUAGE_FILES:
            return
        self.language = language
        self._apply_language()

    def _apply_language(self) -> None:
        self.root.title(self.tr("window_title"))
        self.mode_var.set(self._mode_label(self.current_mode))
        self.status_var.set(self.tr(self.status_key, **self.status_kwargs))
        self.log_var.set(self._log_text(self.current_log_path))

        for key, label in self.stat_label_widgets.items():
            label.configure(text=self.tr(key))
        for key, label in self.agent_title_labels.items():
            label.configure(text=self._agent_title(key))

        if hasattr(self, "drop_title_label"):
            self.drop_title_label.configure(text=self.tr("drop_title"))
        if hasattr(self, "drop_hint_label"):
            self.drop_hint_label.configure(
                text=self.tr("drop_hint" if self.dnd_available else "dnd_unavailable")
            )
        if hasattr(self, "source_label"):
            self.source_label.configure(text=self.tr("source_label"))
        if hasattr(self, "target_label"):
            self.target_label.configure(text=self.tr("target_label"))
        if hasattr(self, "source_btn"):
            self.source_btn.configure(text=self.tr("browse"))
        if hasattr(self, "target_btn"):
            self.target_btn.configure(text=self.tr("browse"))
        if hasattr(self, "multi_source_label"):
            self.multi_source_label.configure(text=self.tr("multi_source_hint"))
        if hasattr(self, "start_btn"):
            self.start_btn.configure(text=self.tr("start"))
        if hasattr(self, "stop_btn"):
            self.stop_btn.configure(text=self.tr("stop"))
        if hasattr(self, "legend_label"):
            self.legend_label.configure(text=self.tr("legend_agents"))
        if hasattr(self, "lang_ru_btn"):
            self.lang_ru_btn.configure(text=self.tr("language_ru"))
        if hasattr(self, "lang_en_btn"):
            self.lang_en_btn.configure(text=self.tr("language_en"))
        if hasattr(self, "shutdown_check"):
            self.shutdown_check.configure(text=self.tr("shutdown_after_done"))

        self._render_events(self.last_snapshot)

    def _build_window(self) -> None:
        self.root.title(self.tr("window_title"))
        self.root.geometry(f"{self.window_width}x{self.window_height}")
        self.root.minsize(self.window_width, self.window_height)
        self.root.maxsize(self.window_width, self.window_height)
        self.root.resizable(False, False)
        self.root.configure(bg=self._c("root_bg"))

    def _build_ui(self) -> None:
        outer = tk.Frame(self.root, bg=self._c("root_bg"), padx=self.outer_pad, pady=8)
        outer.pack(fill=tk.BOTH, expand=True)

        top = tk.Frame(
            outer,
            bg=self._c("panel_bg"),
            bd=1,
            relief=tk.SOLID,
            padx=8,
            pady=8,
            width=self.module_width,
        )
        top.pack(fill=tk.X)
        self.top_frame = top

        drop = tk.Frame(
            top,
            bg=self._c("drop_bg"),
            width=self.agent_cell_width,
            height=121,
            bd=2,
            relief=tk.RIDGE,
            highlightthickness=2,
            highlightbackground=self._c("drop_border"),
        )
        drop.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        drop.pack_propagate(False)
        self.drop_frame = drop

        drop_title = tk.Label(
            drop,
            text=self.tr("drop_title"),
            font=self.font_title,
            bg=self._c("drop_bg"),
            fg=self._c("text_primary"),
            justify=tk.CENTER,
        )
        drop_title.pack(fill=tk.X, pady=(6, 4))
        self.drop_title_label = drop_title

        dnd_text = (
            self.tr("drop_hint")
            if self.dnd_available
            else self.tr("dnd_unavailable")
        )
        self.drop_hint_label = tk.Label(
            drop,
            text=dnd_text,
            font=self.font_drop_hint,
            bg=self._c("drop_bg"),
            fg=self._c("text_secondary"),
            justify=tk.CENTER,
        )
        self.drop_hint_label.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        if self.dnd_available and DND_FILES:
            drop.drop_target_register(DND_FILES)
            drop.dnd_bind("<<Drop>>", self._on_source_drop)

        paths = tk.Frame(top, bg=self._c("panel_bg"))
        paths.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.source_label = tk.Label(
            paths,
            text=self.tr("source_label"),
            font=self.font_main,
            bg=self._c("panel_bg"),
            fg=self._c("text_primary"),
        )
        self.source_label.grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.source_entry = tk.Entry(paths, textvariable=self.source_var, font=self.font_main)
        self.source_entry.grid(row=0, column=1, sticky="ew", padx=(8, 8), pady=(0, 6))
        self.source_btn = tk.Button(
            paths,
            text=self.tr("browse"),
            font=self.font_main,
            command=self._browse_source,
            bg=self._c("btn_secondary_bg"),
            fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
        )
        self.source_btn.grid(row=0, column=2, sticky="ew", pady=(0, 6))

        self.target_label = tk.Label(
            paths,
            text=self.tr("target_label"),
            font=self.font_main,
            bg=self._c("panel_bg"),
            fg=self._c("text_primary"),
        )
        self.target_label.grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.target_entry = tk.Entry(paths, textvariable=self.target_var, font=self.font_main)
        self.target_entry.grid(row=1, column=1, sticky="ew", padx=(8, 8), pady=(0, 6))
        self.target_btn = tk.Button(
            paths,
            text=self.tr("browse"),
            font=self.font_main,
            command=self._browse_target,
            bg=self._c("btn_secondary_bg"),
            fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
        )
        self.target_btn.grid(row=1, column=2, sticky="ew", pady=(0, 6))

        self.multi_source_label = tk.Label(
            paths,
            text=self.tr("multi_source_hint"),
            font=self.font_legend,
            bg=self._c("panel_bg"),
            fg=self._c("text_muted"),
        )
        self.multi_source_label.grid(row=2, column=0, columnspan=3, sticky="w", pady=(4, 0))
        paths.grid_columnconfigure(1, weight=1)
        paths.grid_columnconfigure(2, minsize=self.browse_cell_width, uniform="browse_buttons")

        ctrl = tk.Frame(
            outer,
            bg=self._c("ctrl_bg"),
            bd=1,
            relief=tk.SOLID,
            padx=8,
            pady=6,
            width=self.module_width,
        )
        ctrl.pack(fill=tk.X, pady=(6, 6))
        self.ctrl_frame = ctrl

        button_panel = tk.Frame(
            ctrl,
            bg=self._c("ctrl_bg"),
            width=self.agent_cell_width,
            height=64,
        )
        button_panel.grid(row=0, column=0, rowspan=2, sticky="nsew")
        button_panel.grid_propagate(False)
        button_panel.grid_columnconfigure(0, weight=1, uniform="ctrl_buttons")
        button_panel.grid_columnconfigure(1, weight=1, uniform="ctrl_buttons")
        self.button_panel = button_panel

        self.start_btn = tk.Button(
            button_panel,
            text=self.tr("start"),
            font=self.font_main,
            command=self._start_pipeline,
            bg=self._c("btn_start_bg"),
            fg=self._c("btn_start_fg"),
            activebackground=self._c("btn_start_active_bg"),
            activeforeground=self._c("btn_start_active_fg"),
        )
        self.start_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4), pady=1)

        self.stop_btn = tk.Button(
            button_panel,
            text=self.tr("stop"),
            font=self.font_main,
            command=self._stop_pipeline,
            bg=self._c("btn_stop_bg"),
            fg=self._c("btn_stop_fg"),
            activebackground=self._c("btn_stop_active_bg"),
            activeforeground=self._c("btn_stop_active_fg"),
            state=tk.DISABLED,
        )
        self.stop_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0), pady=1)
        self.mode_badge = tk.Label(
            button_panel,
            textvariable=self.mode_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("mode_fg"),
            anchor="center",
            justify=tk.CENTER,
        )
        self.mode_badge.grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=(4, 0))

        self.time_badge = tk.Label(
            button_panel,
            textvariable=self.time_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_secondary"),
            anchor="center",
            justify=tk.CENTER,
        )
        self.time_badge.grid(row=1, column=1, sticky="ew", padx=(4, 0), pady=(4, 0))

        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "Agent.Horizontal.TProgressbar",
            thickness=20,
            troughcolor=self._c("progress_trough"),
            background=self._c("progress_bg"),
            bordercolor=self._c("progress_border"),
            lightcolor=self._c("progress_bg"),
            darkcolor=self._c("progress_dark"),
        )

        self.progress = ttk.Progressbar(
            ctrl,
            style="Agent.Horizontal.TProgressbar",
            maximum=100.0,
            mode="determinate",
        )
        self.progress.grid(row=0, column=1, sticky="ew", padx=(10, 10))

        top_status = tk.Frame(ctrl, bg=self._c("ctrl_bg"), width=self.browse_cell_width)
        top_status.grid(row=0, column=2, sticky="ew")
        self.percent_frame = top_status
        tk.Label(
            top_status,
            textvariable=self.progress_text_var,
            font=self.font_title,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_primary"),
            anchor="center",
        ).pack(fill=tk.X)

        stats = tk.Frame(ctrl, bg=self._c("ctrl_bg"))
        stats.grid(row=1, column=1, sticky="w", padx=(10, 10), pady=(6, 0))
        self.stats_frame = stats
        for idx, (label, var) in enumerate(
            [
                ("stats_books_found", self.seen_var),
                ("stats_books_done", self.done_var),
                ("stats_duplicates", self.dupes_var),
                ("stats_nobooks", self.nobook_var),
                ("stats_book_errors", self.failed_var),
            ]
        ):
            item = tk.Frame(stats, bg=self._c("ctrl_bg"))
            item.grid(row=0, column=idx, sticky="w", padx=(0, 14))
            label_widget = tk.Label(
                item,
                text=self.tr(label),
                font=self.font_counter_label,
                bg=self._c("ctrl_bg"),
                fg=self._c("text_muted"),
            )
            label_widget.pack(side=tk.LEFT)
            self.stat_label_widgets[label] = label_widget
            tk.Label(
                item,
                textvariable=var,
                font=self.font_counter_value,
                bg=self._c("ctrl_bg"),
                fg=self._c("text_secondary"),
            ).pack(side=tk.LEFT, padx=(3, 0))

        ctrl.grid_columnconfigure(0, minsize=self.agent_cell_width)
        ctrl.grid_columnconfigure(1, weight=1)
        ctrl.grid_columnconfigure(2, minsize=self.browse_cell_width)

        agents_height = max(120, int(self.window_height * 0.22))
        agents = tk.Frame(
            outer,
            bg=self._c("root_bg"),
            bd=1,
            relief=tk.SOLID,
            height=agents_height,
            width=self.module_width,
        )
        agents.pack(fill=tk.X, expand=False, pady=(2, 0))
        agents.pack_propagate(False)
        self.agents_frame = agents
        agents.grid_rowconfigure(0, weight=1, uniform="agent_rows")
        agents.grid_rowconfigure(1, weight=1, uniform="agent_rows")
        for col in range(4):
            agents.grid_columnconfigure(col, weight=1, uniform="agent_cols")

        for idx, key in enumerate(lp.AGENT_KEYS):
            row = idx // 4
            col = idx % 4
            bg, fg = self.agent_colors[key]
            card = tk.Frame(
                agents,
                bg=bg,
                bd=1,
                relief=tk.SOLID,
                padx=5,
                pady=3,
            )
            card.grid(row=row, column=col, sticky="nsew", padx=2, pady=2)
            self.agent_cards[key] = card
            self.agent_labels[key] = []

            title_lbl = tk.Label(
                card,
                text=self._agent_title(key),
                font=self.font_agent_title,
                bg=bg,
                fg=fg,
                anchor="w",
            )
            title_lbl.pack(fill=tk.X)
            self.agent_labels[key].append(title_lbl)
            self.agent_title_labels[key] = title_lbl

            p = tk.StringVar(value="0")
            e = tk.StringVar(value="0")
            q = tk.StringVar(value="0")
            self.agent_processed[key] = p
            self.agent_errors[key] = e
            self.agent_queue[key] = q

            metrics_line = tk.Frame(card, bg=bg)
            metrics_line.pack(fill=tk.X, anchor="w")
            self.agent_labels[key].append(metrics_line)

            for label, var in (("P:", p), ("E:", e), ("Q:", q)):
                name_lbl = tk.Label(
                    metrics_line,
                    text=label,
                    font=self.font_agent_metric_label,
                    bg=bg,
                    fg=fg,
                )
                name_lbl.pack(side=tk.LEFT, anchor="w")
                self.agent_labels[key].append(name_lbl)

                value_lbl = tk.Label(
                    metrics_line,
                    textvariable=var,
                    font=self.font_agent_metric_value,
                    bg=bg,
                    fg=fg,
                )
                value_lbl.pack(side=tk.LEFT, anchor="w", padx=(2, 8))
                self.agent_labels[key].append(value_lbl)

        self.legend_label = tk.Label(
            outer,
            text=self.tr("legend_agents"),
            font=self.font_legend,
            bg=self._c("root_bg"),
            fg=self._c("legend_fg"),
            anchor="w",
            justify=tk.LEFT,
        )
        self.legend_label.pack(fill=tk.X, pady=(2, 0))

        footer_height = max(109, int(self.window_height * 0.21))
        footer = tk.Frame(
            outer,
            bg=self._c("ctrl_bg"),
            bd=1,
            relief=tk.SOLID,
            padx=6,
            pady=4,
            height=footer_height,
            width=self.module_width,
        )
        footer.pack(fill=tk.X, pady=(4, 0))
        footer.pack_propagate(False)
        self.footer_frame = footer
        tk.Label(
            footer,
            textvariable=self.event_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_secondary"),
            anchor="w",
            justify=tk.LEFT,
        ).pack(fill=tk.X, pady=(0, 2))
        tk.Label(
            footer,
            textvariable=self.log_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_muted"),
            anchor="w",
            justify=tk.LEFT,
        ).pack(fill=tk.X, pady=(0, 2))
        tk.Label(
            footer,
            textvariable=self.status_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_status"),
            anchor="w",
            justify=tk.LEFT,
        ).pack(fill=tk.X)

        footer_controls = tk.Frame(footer, bg=self._c("ctrl_bg"))
        footer_controls.pack(side=tk.BOTTOM, fill=tk.X, pady=(3, 0))
        self.footer_controls = footer_controls

        lang_frame = tk.Frame(footer_controls, bg=self._c("ctrl_bg"))
        lang_frame.pack(side=tk.LEFT, anchor="sw")

        self.lang_ru_btn = tk.Button(
            lang_frame,
            text=self.tr("language_ru"),
            font=self.font_legend,
            width=4,
            command=lambda: self._set_language("ru"),
            bg=self._c("btn_secondary_bg"),
            fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
        )
        self.lang_ru_btn.pack(side=tk.LEFT, padx=(0, 4))

        self.lang_en_btn = tk.Button(
            lang_frame,
            text=self.tr("language_en"),
            font=self.font_legend,
            width=4,
            command=lambda: self._set_language("en"),
            bg=self._c("btn_secondary_bg"),
            fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
        )
        self.lang_en_btn.pack(side=tk.LEFT)

        self.shutdown_check = tk.Checkbutton(
            footer_controls,
            text=self.tr("shutdown_after_done"),
            variable=self.shutdown_after_done_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_secondary"),
            activebackground=self._c("ctrl_bg"),
            activeforeground=self._c("text_primary"),
            selectcolor=self._c("ctrl_bg"),
        )
        self.shutdown_check.pack(side=tk.RIGHT, anchor="se")

    def _apply_initial_dirs(self) -> None:
        # SOURCE_DIRS не подставляем автоматически при старте.
        self.source_var.set("")
        target = Path(self.target_var.get())
        if not target.exists() and setting is not None:
            try:
                tgt = getattr(setting, "TARGET_DIR", "")
                if tgt:
                    self.target_var.set(str(tgt))
            except Exception:
                pass

    def _browse_source(self) -> None:
        initial = self._best_existing_dir(self.source_var.get())
        picked = filedialog.askdirectory(title=self.tr("dialog_source_title"), initialdir=initial)
        if picked:
            self._append_source_dir(Path(picked))
            self._set_status("status_source_updated")

    def _browse_target(self) -> None:
        initial = self._best_existing_dir(self.target_var.get())
        picked = filedialog.askdirectory(title=self.tr("dialog_target_title"), initialdir=initial)
        if picked:
            self.target_var.set(str(Path(picked)))
            self._persist_target_dir(Path(picked))
            self._set_status("status_target_updated")

    def _best_existing_dir(self, candidate: str) -> str:
        try:
            path = Path(candidate)
            if path.exists():
                return str(path)
            if path.parent.exists():
                return str(path.parent)
        except Exception:
            pass
        return str(Path.home())

    def _on_source_drop(self, event) -> None:
        data = event.data or ""
        paths = self._parse_dnd_paths(data)
        added = 0
        for raw in paths:
            p = Path(raw)
            if p.exists() and p.is_dir():
                self._append_source_dir(p)
                added += 1
        if added > 0:
            self._set_status("status_source_drop_updated", count=added)
            return
        messagebox.showwarning(self.tr("dialog_drop_title"), self.tr("dialog_drop_folder_only"))

    def _append_source_dir(self, path: Path) -> None:
        existing = lp.parse_sources_input([self.source_var.get()])
        merged = lp.parse_sources_input([*([str(x) for x in existing]), str(path)])
        text = "; ".join(str(x) for x in merged)
        self.source_var.set(text)

    def _parse_dnd_paths(self, data: str) -> list[str]:
        if not data:
            return []
        try:
            parts = list(self.root.tk.splitlist(data))
        except Exception:
            parts = [data]
        cleaned = []
        for part in parts:
            item = part.strip()
            if item.startswith("{") and item.endswith("}"):
                item = item[1:-1]
            if item:
                cleaned.append(item)
        return cleaned

    def _check_and_create_paths(self) -> bool:
        source_raw = self.source_var.get().strip()
        target_raw = self.target_var.get().strip()
        source_dirs = lp.parse_sources_input([source_raw])
        if not source_dirs:
            messagebox.showerror(self.tr("dialog_paths_title"), self.tr("dialog_source_missing"))
            return False
        existing_sources = [src for src in source_dirs if src.exists()]
        if not existing_sources:
            messagebox.showerror(
                self.tr("dialog_paths_title"),
                self.tr("dialog_source_not_found"),
            )
            return False
        if not target_raw:
            messagebox.showerror(self.tr("dialog_paths_title"), self.tr("dialog_target_missing"))
            return False
        target = Path(target_raw)

        try:
            for source in source_dirs:
                source.mkdir(parents=True, exist_ok=True)
            target.mkdir(parents=True, exist_ok=True)
            (target / "Duplicates").mkdir(parents=True, exist_ok=True)
            (target / "NoBook").mkdir(parents=True, exist_ok=True)
            temp_base = self._resolve_temp_base(target)
            temp_base.mkdir(parents=True, exist_ok=True)
            (temp_base / "extract").mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            messagebox.showerror(
                self.tr("dialog_paths_title"),
                self.tr("dialog_path_create_error", error=exc),
            )
            self._set_status("status_paths_error", error=exc)
            return False

        self._persist_target_dir(target)
        self._set_status("status_paths_checked", count=len(source_dirs))
        return True

    def _persist_target_dir(self, target: Path) -> None:
        setting_path = Path(__file__).resolve().parent / "setting.py"
        if not setting_path.exists():
            return
        try:
            text = setting_path.read_text(encoding="utf-8")
        except Exception:
            return

        target_line = f"TARGET_DIR = {str(target)!r}"
        pattern = re.compile(r"^TARGET_DIR\s*=.*$", flags=re.MULTILINE)
        if pattern.search(text):
            updated = pattern.sub(lambda _m: target_line, text, count=1)
        else:
            updated = text.rstrip() + "\n" + target_line + "\n"

        if updated != text:
            try:
                setting_path.write_text(updated, encoding="utf-8")
            except Exception:
                return

    def _resolve_temp_base(self, target_dir: Path) -> Path:
        if setting is not None:
            try:
                temp_base = getattr(setting, "TEMP_BASE", None)
                if temp_base:
                    return Path(str(temp_base))
            except Exception:
                pass
        return target_dir / "_TempPipeline"

    def _setting_int(self, name: str, default: int, min_value: int = 1) -> int:
        if setting is None:
            return max(min_value, int(default))
        try:
            value = int(getattr(setting, name, default))
            return max(min_value, value)
        except Exception:
            return max(min_value, int(default))

    def _setting_bool(self, name: str, default: bool) -> bool:
        if setting is None:
            return default
        try:
            return bool(getattr(setting, name, default))
        except Exception:
            return default

    def _c(self, key: str, fallback: str = "#000000") -> str:
        return str(self.palette.get(key, fallback))

    def _build_config(self) -> lp.Config:
        source_dirs = lp.parse_sources_input([self.source_var.get().strip()])
        target = Path(self.target_var.get().strip())
        dupes = target / "Duplicates"
        nobook = target / "NoBook"
        temp_base = self._resolve_temp_base(target)

        lm_url = "http://127.0.0.1:1234/v1/chat/completions"
        lm_model = "google/gemma-4-e4b"
        if setting is not None:
            lm_url = getattr(setting, "LM_URL", lm_url)
            lm_model = getattr(setting, "LM_MODEL", lm_model)

        return lp.Config(
            source_dirs=source_dirs,
            target_dir=target,
            dupes_dir=dupes,
            nobook_dir=nobook,
            temp_base=temp_base,
            lm_url=lm_url,
            lm_model=lm_model,
            queue_size=self._setting_int("QUEUE_SIZE", lp.DEFAULT_QUEUE_SIZE, min_value=100),
            unpack_workers=self._setting_int(
                "UNPACK_WORKERS", lp.DEFAULT_UNPACK_WORKERS, min_value=1
            ),
            detect_workers=self._setting_int(
                "DETECT_WORKERS", lp.DEFAULT_DETECT_WORKERS, min_value=1
            ),
            dedupe_workers=self._setting_int(
                "DEDUPE_WORKERS", lp.DEFAULT_DEDUPE_WORKERS, min_value=1
            ),
            tag_workers=self._setting_int("TAG_WORKERS", lp.DEFAULT_TAG_WORKERS, min_value=1),
            lm_workers=self._setting_int("LM_WORKERS", lp.DEFAULT_LM_WORKERS, min_value=1),
            rename_workers=self._setting_int(
                "RENAME_WORKERS", lp.DEFAULT_RENAME_WORKERS, min_value=1
            ),
            pack_workers=self._setting_int("PACK_WORKERS", lp.DEFAULT_PACK_WORKERS, min_value=1),
            max_parallel_archives=self._setting_int(
                "MAX_PARALLEL_ARCHIVES", lp.DEFAULT_MAX_PARALLEL_ARCHIVES, min_value=1
            ),
            delete_source_after_pack=True,
            keep_temp_nobooks=False,
            lm_timeout_sec=self._setting_int(
                "LM_TIMEOUT_SEC", lp.DEFAULT_LM_TIMEOUT_SEC, min_value=10
            ),
            lm_input_chars=self._setting_int(
                "LM_INPUT_CHARS", lp.DEFAULT_LM_INPUT_CHARS, min_value=200
            ),
            lm_max_output_tokens=self._setting_int(
                "LM_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_MAX_OUTPUT_TOKENS, min_value=40
            ),
            lm_force_full_metadata=self._setting_bool(
                "LM_FORCE_FULL_METADATA", getattr(lp, "DEFAULT_LM_FORCE_FULL_METADATA", True)
            ),
            lm_always_try_without_snippet=self._setting_bool(
                "LM_ALWAYS_TRY_WITHOUT_SNIPPET", lp.DEFAULT_LM_ALWAYS_TRY_WITHOUT_SNIPPET
            ),
            lm_strict_json_mode=self._setting_bool(
                "LM_STRICT_JSON_MODE", lp.DEFAULT_LM_STRICT_JSON_MODE
            ),
            lm_min_snippet_letters=self._setting_int(
                "LM_MIN_SNIPPET_LETTERS", lp.DEFAULT_LM_MIN_SNIPPET_LETTERS, min_value=1
            ),
            seed_hashes_from_target=self._setting_bool(
                "SEED_HASHES_FROM_TARGET", lp.DEFAULT_SEED_HASHES_FROM_TARGET
            ),
            target_hash_scan_workers=self._setting_int(
                "TARGET_HASH_SCAN_WORKERS", lp.DEFAULT_TARGET_HASH_SCAN_WORKERS, min_value=1
            ),
            ephemeral_mode=True,
        )

    def _start_pipeline(self) -> None:
        if self.pipeline_running:
            self._set_status("status_already_running")
            return
        if not self._check_and_create_paths():
            return

        self.pipeline_error = ""
        self.pipeline_exit_code = None

        try:
            config = self._build_config()
            sorter = lp.LibrarySorter(config)
            sorter.ui = _NoopController()
            sorter.keyboard = _NoopController()
        except Exception as exc:
            messagebox.showerror(self.tr("start"), self.tr("status_start_error", error=exc))
            self._set_status("status_start_error", error=exc)
            return

        self.sorter = sorter
        self.pipeline_running = True
        self.shutdown_started = False
        self.current_mode = "RUNNING"
        self.mode_var.set(self._mode_label("RUNNING"))
        self.time_var.set("00:00:00/~--:--:--")
        self._set_status("status_started")
        self.current_log_path = sorter.log_file
        self.log_var.set(self._log_text(self.current_log_path))
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)

        self.pipeline_thread = threading.Thread(
            target=self._pipeline_runner_thread,
            name="GUI-PipelineRunner",
            daemon=True,
        )
        self.pipeline_thread.start()

    def _pipeline_runner_thread(self) -> None:
        code = 2
        try:
            if self.sorter:
                code = self.sorter.run()
        except Exception as exc:
            self.pipeline_error = str(exc)
            code = 2
        self.pipeline_exit_code = code

    def _stop_pipeline(self) -> None:
        if not self.pipeline_running or not self.sorter:
            self._set_status("status_not_running")
            return
        self.sorter.request_stop_and_cleanup()
        self.current_mode = "STOP_CLEANUP"
        self.mode_var.set(self._mode_label("STOP_CLEANUP"))
        self._set_status("status_stopping_cleanup")

    def _poll_pipeline(self) -> None:
        if self.pipeline_running and self.pipeline_thread and not self.pipeline_thread.is_alive():
            if self.sorter:
                try:
                    snap = self.sorter.metrics.snapshot(self.sorter.queue_sizes())
                    self._render_snapshot(snap)
                except Exception:
                    pass
            self.pipeline_running = False
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            exit_code = self.pipeline_exit_code if self.pipeline_exit_code is not None else 2
            if exit_code == 0:
                self._set_status("status_finished_ok")
            else:
                if self.pipeline_error:
                    self._set_status("status_execution_error", error=self.pipeline_error)
                else:
                    self._set_status("status_finished_code", code=exit_code)
            self.current_mode = "END" if exit_code == 0 else "END_ERROR"
            self.mode_var.set(self._mode_label(self.current_mode))
            if exit_code == 0 and self.shutdown_after_done_var.get():
                self._shutdown_computer()
        elif self.sorter and self.pipeline_running:
            try:
                snap = self.sorter.metrics.snapshot(self.sorter.queue_sizes())
                self._render_snapshot(snap)
            except Exception:
                pass

        self.root.after(300, self._poll_pipeline)

    def _render_snapshot(self, snap: dict) -> None:
        self.last_snapshot = snap
        pct = float(snap.get("pct", 0.0))
        self.progress["value"] = max(0.0, min(100.0, pct))
        self.progress_text_var.set(f"{int(round(pct))}%")
        self.current_mode = fix_mojibake(str(snap.get("mode", "RUNNING")))
        self.mode_var.set(self._mode_label(self.current_mode))
        self.time_var.set(self._time_label(snap))

        seen = int(snap.get("seen", 0))
        done = int(snap.get("done", 0))
        results = snap.get("results", {}) or {}
        book_results = snap.get("book_results", {}) or {}
        self.seen_var.set(self._fmt_num(seen))
        self.done_var.set(self._fmt_num(done))
        self.packed_var.set(self._fmt_num(int(book_results.get("packed", 0))))
        self.dupes_var.set(self._fmt_num(int(book_results.get("duplicate", 0))))
        self.nobook_var.set(self._fmt_num(int(results.get("nobook", 0))))
        self.failed_var.set(self._fmt_num(int(book_results.get("failed", 0))))

        stage_processed = snap.get("stage_processed", {}) or {}
        stage_errors = snap.get("stage_errors", {}) or {}
        queue_sizes = snap.get("queue_sizes", {}) or {}
        for key in lp.AGENT_KEYS:
            processed = int(stage_processed.get(key, 0))
            errors = int(stage_errors.get(key, 0))
            qsize = int(queue_sizes.get(key, 0))
            self.agent_processed[key].set(self._fmt_num(processed))
            self.agent_errors[key].set(self._fmt_num(errors))
            self.agent_queue[key].set(self._fmt_num(qsize))
            self._update_agent_visual_state(key, processed, qsize)

        self._render_events(snap)

    def _fmt_num(self, value: int) -> str:
        return f"{value:,}".replace(",", " ")

    def _mode_label(self, mode: str) -> str:
        fixed = fix_mojibake(str(mode or ""))
        if not fixed:
            return self.tr("mode_IDLE")
        key = f"mode_{fixed}"
        text = self.tr(key)
        return fixed if text == key else text

    def _time_label(self, snap: dict) -> str:
        elapsed = fix_mojibake(str(snap.get("elapsed", "00:00:00")))
        eta = fix_mojibake(str(snap.get("eta", "--:--:--")))
        return f"{elapsed}/~{eta}"

    def _agent_title(self, key: str) -> str:
        return f"{key} {self.tr(f'agent_{key}')}"

    def _log_text(self, path: object = "-") -> str:
        return f"{self.tr('log_prefix')}: {path}"

    def _render_events(self, snap: Optional[dict]) -> None:
        events = (snap or {}).get("events", []) or []
        if events:
            top = fix_mojibake(str(events[0]).strip())
            self.event_var.set(f"{self.tr('events_prefix')}: {top}")
        else:
            self.event_var.set(f"{self.tr('events_prefix')}: -")

    def _shutdown_computer(self) -> None:
        if self.shutdown_started:
            return
        self.shutdown_started = True
        self._set_status("status_shutdown_started")
        try:
            subprocess.Popen(
                ["shutdown", "/s", "/f", "/t", "0"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as exc:
            self.shutdown_started = False
            self._set_status("status_shutdown_failed", error=exc)
            messagebox.showerror(
                self.tr("dialog_shutdown_title"),
                self.tr("dialog_shutdown_failed", error=exc),
            )

    def _update_agent_visual_state(self, key: str, processed: int, qsize: int) -> None:
        now = time.time()
        prev_processed = self.agent_last_processed.get(key, 0)
        if processed > prev_processed:
            self.agent_active_until[key] = max(self.agent_active_until.get(key, 0.0), now + 1.6)
        self.agent_last_processed[key] = processed

        if qsize > 0:
            self.agent_active_until[key] = max(self.agent_active_until.get(key, 0.0), now + 0.8)

        is_active = now < self.agent_active_until.get(key, 0.0)
        bg = self.agent_active_bg[key] if is_active else self.agent_colors[key][0]
        self._set_agent_card_bg(key, bg)

    def _set_agent_card_bg(self, key: str, bg: str) -> None:
        card = self.agent_cards.get(key)
        if not card:
            return
        try:
            if str(card.cget("bg")) == str(bg):
                return
            card.configure(bg=bg)
            for lbl in self.agent_labels.get(key, []):
                lbl.configure(bg=bg)
        except Exception:
            return

    def _on_close(self) -> None:
        if self.pipeline_running:
            self._stop_pipeline()
            self._set_status("status_closing_after_stop")
            self.root.after(250, self._close_when_stopped)
            return
        self.root.destroy()

    def _close_when_stopped(self) -> None:
        if self.pipeline_thread and self.pipeline_thread.is_alive():
            self.root.after(250, self._close_when_stopped)
            return
        self.root.destroy()


def create_root() -> tuple[tk.Tk, bool]:
    if TkinterDnD is not None:
        return TkinterDnD.Tk(), True
    return tk.Tk(), False


def main() -> int:
    root, dnd_available = create_root()
    LibraryGUIApp(root, dnd_available)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
