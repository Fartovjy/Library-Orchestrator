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
        self.stop_requested_by_user = False

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
        self._eta_display_value = "--:--:--"
        self._eta_display_updated_at = 0.0
        self.seen_var = tk.StringVar(value="0")
        self.done_var = tk.StringVar(value="0")
        self.packed_var = tk.StringVar(value="0")
        self.dupes_var = tk.StringVar(value="0")
        self.nobook_var = tk.StringVar(value="0")
        self.failed_var = tk.StringVar(value="0")
        self.event_var = tk.StringVar(value=f"{self.tr('events_prefix')}: -")
        self.log_var = tk.StringVar(value=f"{self.tr('log_prefix')}: -")
        self.shutdown_after_done_var = tk.BooleanVar(value=False)
        self.keep_sources_var = tk.BooleanVar(value=False)
        self.deep_analysis_var = tk.BooleanVar(value=False)
        self.dupes_var_path = tk.StringVar(value=fix_mojibake(str(lp.DEFAULT_DUPES_DIR)))
        self.nobook_var_path = tk.StringVar(value=fix_mojibake(str(lp.DEFAULT_NOBOOK_DIR)))
        self.temp_base_var = tk.StringVar(value=fix_mojibake(str(lp.DEFAULT_TEMP_BASE)))
        self.lm_url_var = tk.StringVar(value=self._setting_str("LM_URL", lp.DEFAULT_LM_URL))
        self.lm_model_var = tk.StringVar(value=self._setting_str("LM_MODEL", lp.DEFAULT_LM_MODEL))
        self.output_language_var = tk.StringVar(
            value=self._setting_choice("OUTPUT_LANGUAGE", lp.DEFAULT_OUTPUT_LANGUAGE, {"auto", "ru", "en"})
        )
        self.gui_font_family_var = tk.StringVar(value=font_family)
        self.worker_vars: dict[str, tk.StringVar] = {}
        for name, default in [
            ("UNPACK_WORKERS", lp.DEFAULT_UNPACK_WORKERS),
            ("DETECT_WORKERS", lp.DEFAULT_DETECT_WORKERS),
            ("DEDUPE_WORKERS", lp.DEFAULT_DEDUPE_WORKERS),
            ("TAG_WORKERS", lp.DEFAULT_TAG_WORKERS),
            ("LM_WORKERS", lp.DEFAULT_LM_WORKERS),
            ("RENAME_WORKERS", lp.DEFAULT_RENAME_WORKERS),
            ("PACK_WORKERS", lp.DEFAULT_PACK_WORKERS),
            ("MAX_PARALLEL_ARCHIVES", lp.DEFAULT_MAX_PARALLEL_ARCHIVES),
            ("QUEUE_SIZE", lp.DEFAULT_QUEUE_SIZE),
            ("TARGET_HASH_SCAN_WORKERS", lp.DEFAULT_TARGET_HASH_SCAN_WORKERS),
        ]:
            self.worker_vars[name] = tk.StringVar(value=str(self._setting_int(name, default, min_value=1)))
        self.lm_number_vars: dict[str, tk.StringVar] = {}
        for name, default in [
            ("LM_TIMEOUT_SEC", lp.DEFAULT_LM_TIMEOUT_SEC),
            ("LM_INPUT_CHARS", lp.DEFAULT_LM_INPUT_CHARS),
            ("LM_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_MAX_OUTPUT_TOKENS),
            ("LM_DEEP_TIMEOUT_SEC", 120),
            ("LM_DEEP_INPUT_CHARS", 16000),
            ("LM_DEEP_MAX_OUTPUT_TOKENS", 1024),
            ("LM_FAST_INPUT_CHARS", lp.DEFAULT_LM_FAST_INPUT_CHARS),
            ("LM_FAST_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_FAST_MAX_OUTPUT_TOKENS),
            ("LM_MIN_SNIPPET_LETTERS", lp.DEFAULT_LM_MIN_SNIPPET_LETTERS),
            ("GUI_WINDOW_WIDTH", self.window_width),
            ("GUI_WINDOW_HEIGHT", self.window_height),
        ]:
            self.lm_number_vars[name] = tk.StringVar(value=str(self._setting_int(name, default, min_value=1)))
        self.lm_confidence_var = tk.StringVar(
            value=str(self._setting_float("LM_FAST_CONFIDENCE_MIN", lp.DEFAULT_LM_FAST_CONFIDENCE_MIN, min_value=0.0))
        )
        self.gui_font_vars: dict[str, tk.StringVar] = {}
        for name, default in [
            ("GUI_FONT_MAIN_SIZE", 12),
            ("GUI_FONT_TITLE_SIZE", 15),
            ("GUI_FONT_SMALL_SIZE", 12),
            ("GUI_FONT_STATS_SIZE", 11),
            ("GUI_FONT_COUNTER_LABEL_SIZE", 9),
            ("GUI_FONT_LEGEND_SIZE", 9),
            ("GUI_FONT_AGENT_TITLE_SIZE", 11),
            ("GUI_FONT_AGENT_VALUE_SIZE", 10),
            ("GUI_FONT_AGENT_METRIC_LABEL_SIZE", 9),
        ]:
            self.gui_font_vars[name] = tk.StringVar(value=str(self._setting_int(name, default, min_value=6)))
        self.boolean_setting_vars: dict[str, tk.BooleanVar] = {
            "LM_FAST_PRECHECK": tk.BooleanVar(value=self._setting_bool("LM_FAST_PRECHECK", lp.DEFAULT_LM_FAST_PRECHECK)),
            "LM_FORCE_FULL_METADATA": tk.BooleanVar(value=self._setting_bool("LM_FORCE_FULL_METADATA", lp.DEFAULT_LM_FORCE_FULL_METADATA)),
            "LM_FILL_UNKNOWN_AUTHOR": tk.BooleanVar(value=self._setting_bool("LM_FILL_UNKNOWN_AUTHOR", lp.DEFAULT_LM_FILL_UNKNOWN_AUTHOR)),
            "LM_ALWAYS_TRY_WITHOUT_SNIPPET": tk.BooleanVar(value=self._setting_bool("LM_ALWAYS_TRY_WITHOUT_SNIPPET", lp.DEFAULT_LM_ALWAYS_TRY_WITHOUT_SNIPPET)),
            "LM_STRICT_JSON_MODE": tk.BooleanVar(value=self._setting_bool("LM_STRICT_JSON_MODE", lp.DEFAULT_LM_STRICT_JSON_MODE)),
            "ISBN_LOOKUP": tk.BooleanVar(value=self._setting_bool("ISBN_LOOKUP", lp.DEFAULT_ISBN_LOOKUP)),
            "SEED_HASHES_FROM_TARGET": tk.BooleanVar(value=self._setting_bool("SEED_HASHES_FROM_TARGET", lp.DEFAULT_SEED_HASHES_FROM_TARGET)),
        }
        self.rename_output_var = tk.BooleanVar(
            value=self._setting_bool(
                "TRANSLATE_OUTPUT_NAMES", lp.DEFAULT_TRANSLATE_OUTPUT_NAMES
            )
        )

        self.agent_processed: dict[str, tk.StringVar] = {}
        self.agent_errors: dict[str, tk.StringVar] = {}
        self.agent_queue: dict[str, tk.StringVar] = {}
        self.agent_cards: dict[str, tk.Frame] = {}
        self.agent_indicator_bars: dict[str, tk.Frame] = {}
        self.agent_indicator_segments: dict[str, list[tk.Frame]] = {}
        self.agent_indicator_totals: dict[str, int] = {}
        self.agent_labels: dict[str, list[tk.Widget]] = {}
        self.stat_label_widgets: dict[str, tk.Label] = {}
        self.agent_title_labels: dict[str, tk.Label] = {}

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
        self._refresh_status_line(self.last_snapshot)

    def _set_language(self, language: str) -> None:
        if language not in LANGUAGE_FILES:
            return
        self.language = language
        self._apply_language()

    def _apply_language(self) -> None:
        self.root.title(self.tr("window_title"))
        self.mode_var.set(self._mode_label(self.current_mode))
        self._refresh_status_line(self.last_snapshot)
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
        if hasattr(self, "notebook"):
            self.notebook.tab(self.main_tab, text=self.tr("tab_main"))
            self.notebook.tab(self.settings_tab, text=self.tr("tab_settings"))
        if hasattr(self, "shutdown_check"):
            self.shutdown_check.configure(text=self.tr("shutdown_after_done"))
        if hasattr(self, "keep_sources_check"):
            self.keep_sources_check.configure(text=self.tr("keep_sources"))
        if hasattr(self, "deep_analysis_check"):
            self.deep_analysis_check.configure(text=self.tr("deep_analysis"))
        if hasattr(self, "rename_output_check"):
            self.rename_output_check.configure(text=self.tr("rename_output"))
        if hasattr(self, "settings_text_widgets"):
            for widget, key in self.settings_text_widgets:
                widget.configure(text=self.tr(key))

        self._render_events(self.last_snapshot)

    def _build_window(self) -> None:
        self.root.title(self.tr("window_title"))
        self.root.geometry(f"{self.window_width}x{self.window_height}")
        self.root.minsize(self.window_width, self.window_height)
        self.root.maxsize(self.window_width, self.window_height)
        self.root.resizable(False, False)
        self.root.configure(bg=self._c("root_bg"))

    def _build_ui(self) -> None:
        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "App.TNotebook",
            background=self._c("root_bg"),
            borderwidth=0,
        )
        style.configure(
            "App.TNotebook.Tab",
            padding=(14, 5),
            font=self.font_legend,
        )

        self.notebook = ttk.Notebook(self.root, style="App.TNotebook")
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.main_tab = tk.Frame(self.notebook, bg=self._c("root_bg"))
        self.settings_tab = tk.Frame(self.notebook, bg=self._c("root_bg"))
        self.notebook.add(self.main_tab, text=self.tr("tab_main"))
        self.notebook.add(self.settings_tab, text=self.tr("tab_settings"))

        outer = tk.Frame(self.main_tab, bg=self._c("root_bg"), padx=self.outer_pad, pady=6)
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
        stats.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(10, 0), pady=(6, 0))
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
            card.grid_columnconfigure(0, weight=1)
            card.grid_columnconfigure(1, minsize=5)
            card.grid_rowconfigure(0, weight=1)

            content = tk.Frame(card, bg=bg)
            content.grid(row=0, column=0, sticky="nsew")
            self.agent_labels[key].append(content)

            indicator = tk.Frame(card, bg=self._c("root_bg"), width=6)
            indicator.grid(row=0, column=1, sticky="ns", padx=(4, 0))
            indicator.grid_propagate(False)
            self.agent_indicator_bars[key] = indicator
            self._rebuild_agent_indicator_segments(key)

            title_lbl = tk.Label(
                content,
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

            metrics_line = tk.Frame(content, bg=bg)
            metrics_line.pack(fill=tk.X, anchor="w")
            self.agent_labels[key].append(metrics_line)

            metric_items = [("P:", p), ("E:", e)]
            if key != "A1":
                metric_items.append(("Q:", q))

            for label, var in metric_items:
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

        self.keep_sources_check = tk.Checkbutton(
            footer_controls,
            text=self.tr("keep_sources"),
            variable=self.keep_sources_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_secondary"),
            activebackground=self._c("ctrl_bg"),
            activeforeground=self._c("text_primary"),
            selectcolor=self._c("ctrl_bg"),
        )
        self.keep_sources_check.pack(side=tk.RIGHT, anchor="se", padx=(0, 12))

        self.deep_analysis_check = tk.Checkbutton(
            footer_controls,
            text=self.tr("deep_analysis"),
            variable=self.deep_analysis_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_secondary"),
            activebackground=self._c("ctrl_bg"),
            activeforeground=self._c("text_primary"),
            selectcolor=self._c("ctrl_bg"),
        )
        self.deep_analysis_check.pack(side=tk.RIGHT, anchor="se", padx=(0, 12))

        self.rename_output_check = tk.Checkbutton(
            footer_controls,
            text=self.tr("rename_output"),
            variable=self.rename_output_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"),
            fg=self._c("text_secondary"),
            activebackground=self._c("ctrl_bg"),
            activeforeground=self._c("text_primary"),
            selectcolor=self._c("ctrl_bg"),
        )
        self.rename_output_check.pack(side=tk.RIGHT, anchor="se", padx=(0, 12))
        self._build_settings_tab()

    def _build_settings_tab(self) -> None:
        self.settings_text_widgets: list[tuple[tk.Widget, str]] = []
        canvas = tk.Canvas(
            self.settings_tab,
            bg=self._c("root_bg"),
            highlightthickness=0,
        )
        scrollbar = ttk.Scrollbar(self.settings_tab, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        content = tk.Frame(canvas, bg=self._c("root_bg"), padx=10, pady=8)
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")
        content.bind(
            "<Configure>",
            lambda _event: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.bind(
            "<Configure>",
            lambda event: canvas.itemconfigure(window_id, width=event.width),
        )

        row = 0

        def section(title_key: str) -> None:
            nonlocal row
            label = tk.Label(
                content,
                text=self.tr(title_key),
                font=self.font_agent_title,
                bg=self._c("root_bg"),
                fg=self._c("text_primary"),
                anchor="w",
            )
            label.grid(row=row, column=0, columnspan=4, sticky="ew", pady=(8 if row else 0, 4))
            self.settings_text_widgets.append((label, title_key))
            row += 1

        def add_entry(label_key: str, var: tk.StringVar, width: int = 18) -> None:
            nonlocal row
            label = tk.Label(
                content,
                text=self.tr(label_key),
                font=self.font_legend,
                bg=self._c("root_bg"),
                fg=self._c("text_secondary"),
                anchor="w",
            )
            label.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=2)
            self.settings_text_widgets.append((label, label_key))
            entry = tk.Entry(content, textvariable=var, font=self.font_legend, width=width)
            entry.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=2)
            row += 1

        def add_path(label_key: str, var: tk.StringVar) -> None:
            nonlocal row
            label = tk.Label(
                content,
                text=self.tr(label_key),
                font=self.font_legend,
                bg=self._c("root_bg"),
                fg=self._c("text_secondary"),
                anchor="w",
            )
            label.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=2)
            self.settings_text_widgets.append((label, label_key))
            entry = tk.Entry(content, textvariable=var, font=self.font_legend)
            entry.grid(row=row, column=1, columnspan=2, sticky="ew", padx=(0, 8), pady=2)
            btn = tk.Button(
                content,
                text=self.tr("browse"),
                font=self.font_legend,
                command=lambda v=var: self._browse_setting_dir(v),
                bg=self._c("btn_secondary_bg"),
                fg=self._c("btn_secondary_fg"),
                activebackground=self._c("btn_secondary_active_bg"),
                activeforeground=self._c("btn_secondary_active_fg"),
            )
            btn.grid(row=row, column=3, sticky="ew", pady=2)
            self.settings_text_widgets.append((btn, "browse"))
            row += 1

        def add_bool(label_key: str, var: tk.BooleanVar) -> None:
            nonlocal row
            check = tk.Checkbutton(
                content,
                text=self.tr(label_key),
                variable=var,
                font=self.font_legend,
                bg=self._c("root_bg"),
                fg=self._c("text_secondary"),
                activebackground=self._c("root_bg"),
                activeforeground=self._c("text_primary"),
                selectcolor=self._c("root_bg"),
                anchor="w",
            )
            check.grid(row=row, column=0, columnspan=2, sticky="w", pady=1)
            self.settings_text_widgets.append((check, label_key))
            row += 1

        section("settings_paths")
        add_path("settings_dupes_dir", self.dupes_var_path)
        add_path("settings_nobook_dir", self.nobook_var_path)
        add_path("settings_temp_base", self.temp_base_var)

        section("settings_workers")
        worker_items = [
            ("settings_unpack_workers", "UNPACK_WORKERS"),
            ("settings_detect_workers", "DETECT_WORKERS"),
            ("settings_dedupe_workers", "DEDUPE_WORKERS"),
            ("settings_tag_workers", "TAG_WORKERS"),
            ("settings_lm_workers", "LM_WORKERS"),
            ("settings_rename_workers", "RENAME_WORKERS"),
            ("settings_pack_workers", "PACK_WORKERS"),
            ("settings_max_parallel_archives", "MAX_PARALLEL_ARCHIVES"),
            ("settings_queue_size", "QUEUE_SIZE"),
            ("settings_target_hash_workers", "TARGET_HASH_SCAN_WORKERS"),
        ]
        for idx in range(0, len(worker_items), 2):
            label_a, name_a = worker_items[idx]
            add_entry(label_a, self.worker_vars[name_a], width=10)
            if idx + 1 < len(worker_items):
                prev_row = row - 1
                label_b, name_b = worker_items[idx + 1]
                label = tk.Label(
                    content,
                    text=self.tr(label_b),
                    font=self.font_legend,
                    bg=self._c("root_bg"),
                    fg=self._c("text_secondary"),
                    anchor="w",
                )
                label.grid(row=prev_row, column=2, sticky="w", padx=(4, 8), pady=2)
                self.settings_text_widgets.append((label, label_b))
                entry = tk.Entry(
                    content,
                    textvariable=self.worker_vars[name_b],
                    font=self.font_legend,
                    width=10,
                )
                entry.grid(row=prev_row, column=3, sticky="ew", pady=2)

        section("settings_ollama")
        add_entry("settings_lm_url", self.lm_url_var, width=34)
        add_entry("settings_lm_model", self.lm_model_var, width=20)
        for key, name in [
            ("settings_lm_timeout", "LM_TIMEOUT_SEC"),
            ("settings_lm_input_chars", "LM_INPUT_CHARS"),
            ("settings_lm_tokens", "LM_MAX_OUTPUT_TOKENS"),
            ("settings_lm_min_letters", "LM_MIN_SNIPPET_LETTERS"),
            ("settings_lm_deep_timeout", "LM_DEEP_TIMEOUT_SEC"),
            ("settings_lm_deep_input", "LM_DEEP_INPUT_CHARS"),
            ("settings_lm_deep_tokens", "LM_DEEP_MAX_OUTPUT_TOKENS"),
            ("settings_lm_fast_input", "LM_FAST_INPUT_CHARS"),
            ("settings_lm_fast_tokens", "LM_FAST_MAX_OUTPUT_TOKENS"),
        ]:
            add_entry(key, self.lm_number_vars[name], width=10)
        add_entry("settings_lm_fast_confidence", self.lm_confidence_var, width=10)

        section("settings_flags")
        add_bool("deep_analysis", self.deep_analysis_var)
        add_bool("rename_output", self.rename_output_var)
        add_bool("keep_sources", self.keep_sources_var)
        add_bool("settings_lm_fast_precheck", self.boolean_setting_vars["LM_FAST_PRECHECK"])
        add_bool("settings_lm_force_full", self.boolean_setting_vars["LM_FORCE_FULL_METADATA"])
        add_bool("settings_lm_fill_author", self.boolean_setting_vars["LM_FILL_UNKNOWN_AUTHOR"])
        add_bool("settings_lm_without_snippet", self.boolean_setting_vars["LM_ALWAYS_TRY_WITHOUT_SNIPPET"])
        add_bool("settings_lm_strict_json", self.boolean_setting_vars["LM_STRICT_JSON_MODE"])
        add_bool("settings_isbn_lookup", self.boolean_setting_vars["ISBN_LOOKUP"])
        add_bool("settings_seed_hashes", self.boolean_setting_vars["SEED_HASHES_FROM_TARGET"])

        section("settings_output")
        label = tk.Label(
            content,
            text=self.tr("settings_output_language"),
            font=self.font_legend,
            bg=self._c("root_bg"),
            fg=self._c("text_secondary"),
            anchor="w",
        )
        label.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=2)
        self.settings_text_widgets.append((label, "settings_output_language"))
        combo = ttk.Combobox(
            content,
            textvariable=self.output_language_var,
            values=("auto", "ru", "en"),
            width=10,
            state="readonly",
            font=self.font_legend,
        )
        combo.grid(row=row, column=1, sticky="w", pady=2)
        row += 1

        section("settings_gui")
        add_entry("settings_gui_font_family", self.gui_font_family_var, width=18)
        for key, name in [
            ("settings_gui_width", "GUI_WINDOW_WIDTH"),
            ("settings_gui_height", "GUI_WINDOW_HEIGHT"),
            ("settings_gui_font_main", "GUI_FONT_MAIN_SIZE"),
            ("settings_gui_font_title", "GUI_FONT_TITLE_SIZE"),
            ("settings_gui_font_small", "GUI_FONT_SMALL_SIZE"),
            ("settings_gui_font_stats", "GUI_FONT_STATS_SIZE"),
            ("settings_gui_font_counter", "GUI_FONT_COUNTER_LABEL_SIZE"),
            ("settings_gui_font_legend", "GUI_FONT_LEGEND_SIZE"),
            ("settings_gui_font_agent_title", "GUI_FONT_AGENT_TITLE_SIZE"),
            ("settings_gui_font_agent_value", "GUI_FONT_AGENT_VALUE_SIZE"),
            ("settings_gui_font_agent_metric", "GUI_FONT_AGENT_METRIC_LABEL_SIZE"),
        ]:
            var = self.lm_number_vars.get(name) or self.gui_font_vars[name]
            add_entry(key, var, width=10)

        save_btn = tk.Button(
            content,
            text=self.tr("settings_save"),
            font=self.font_main,
            command=self._save_settings_to_file,
            bg=self._c("btn_start_bg"),
            fg=self._c("btn_start_fg"),
            activebackground=self._c("btn_start_active_bg"),
            activeforeground=self._c("btn_start_active_fg"),
        )
        save_btn.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(10, 2))
        self.settings_text_widgets.append((save_btn, "settings_save"))

        hint = tk.Label(
            content,
            text=self.tr("settings_restart_hint"),
            font=self.font_legend,
            bg=self._c("root_bg"),
            fg=self._c("text_muted"),
            anchor="w",
            justify=tk.LEFT,
        )
        hint.grid(row=row, column=2, columnspan=2, sticky="ew", padx=(12, 0), pady=(10, 2))
        self.settings_text_widgets.append((hint, "settings_restart_hint"))

        for col in range(4):
            content.grid_columnconfigure(col, weight=1 if col in {1, 3} else 0)

    def _browse_setting_dir(self, var: tk.StringVar) -> None:
        initial = self._best_existing_dir(var.get())
        picked = filedialog.askdirectory(title=self.tr("dialog_target_title"), initialdir=initial)
        if picked:
            var.set(str(Path(picked)))

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
        dupes = Path(self.dupes_var_path.get().strip() or str(target / "Duplicates"))
        nobook = Path(self.nobook_var_path.get().strip() or str(target / "NoBook"))

        try:
            for source in source_dirs:
                source.mkdir(parents=True, exist_ok=True)
            target.mkdir(parents=True, exist_ok=True)
            dupes.mkdir(parents=True, exist_ok=True)
            nobook.mkdir(parents=True, exist_ok=True)
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
        if hasattr(self, "temp_base_var"):
            value = self.temp_base_var.get().strip()
            if value:
                return Path(value)
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

    def _setting_str(self, name: str, default: str) -> str:
        if setting is None:
            return str(default)
        try:
            return fix_mojibake(str(getattr(setting, name, default)))
        except Exception:
            return str(default)

    def _setting_choice(self, name: str, default: str, choices: set[str]) -> str:
        value = self._setting_str(name, default).strip().lower()
        return value if value in choices else default

    def _setting_float(self, name: str, default: float, min_value: float = 0.0) -> float:
        if setting is None:
            return max(min_value, float(default))
        try:
            value = float(getattr(setting, name, default))
            return max(min_value, value)
        except Exception:
            return max(min_value, float(default))

    def _entry_int(self, name: str, default: int, min_value: int = 1) -> int:
        var = (
            self.worker_vars.get(name)
            or self.lm_number_vars.get(name)
            or self.gui_font_vars.get(name)
        )
        raw = var.get().strip() if var else ""
        try:
            value = int(raw)
        except Exception:
            value = int(default)
            if var:
                var.set(str(value))
        value = max(min_value, value)
        if var:
            var.set(str(value))
        return value

    def _entry_float(self, var: tk.StringVar, default: float, min_value: float = 0.0) -> float:
        try:
            value = float(var.get().strip().replace(",", "."))
        except Exception:
            value = float(default)
        value = max(min_value, value)
        var.set(str(value))
        return value

    def _selected_output_language(self) -> str:
        value = self.output_language_var.get().strip().lower()
        if value == "auto":
            return self.language
        if value in {"ru", "en"}:
            return value
        self.output_language_var.set("auto")
        return self.language

    def _setting_value_repr(self, value: object) -> str:
        if isinstance(value, bool):
            return "True" if value else "False"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            items = ",\n".join(f"    {str(item)!r}" for item in value)
            return "[\n" + items + (",\n" if items else "") + "]"
        return repr(str(value))

    def _save_settings_to_file(self) -> None:
        setting_path = Path(__file__).resolve().parent / "setting.py"
        if not setting_path.exists():
            messagebox.showerror(self.tr("tab_settings"), self.tr("settings_save_error", error="setting.py not found"))
            return
        try:
            source_dirs = [str(p) for p in lp.parse_sources_input([self.source_var.get().strip()])]
            values: dict[str, object] = {
                "TARGET_DIR": self.target_var.get().strip(),
                "DUPES_DIR": self.dupes_var_path.get().strip(),
                "NOBOOK_DIR": self.nobook_var_path.get().strip(),
                "TEMP_BASE": self.temp_base_var.get().strip(),
                "LM_URL": self.lm_url_var.get().strip() or lp.DEFAULT_LM_URL,
                "LM_MODEL": self.lm_model_var.get().strip() or lp.DEFAULT_LM_MODEL,
                "MAX_PARALLEL_ARCHIVES": self._entry_int("MAX_PARALLEL_ARCHIVES", lp.DEFAULT_MAX_PARALLEL_ARCHIVES, min_value=1),
                "QUEUE_SIZE": self._entry_int("QUEUE_SIZE", lp.DEFAULT_QUEUE_SIZE, min_value=1),
                "LM_TIMEOUT_SEC": self._entry_int("LM_TIMEOUT_SEC", lp.DEFAULT_LM_TIMEOUT_SEC, min_value=10),
                "LM_INPUT_CHARS": self._entry_int("LM_INPUT_CHARS", lp.DEFAULT_LM_INPUT_CHARS, min_value=200),
                "LM_MAX_OUTPUT_TOKENS": self._entry_int("LM_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_MAX_OUTPUT_TOKENS, min_value=40),
                "LM_DEEP_TIMEOUT_SEC": self._entry_int("LM_DEEP_TIMEOUT_SEC", 120, min_value=10),
                "LM_DEEP_INPUT_CHARS": self._entry_int("LM_DEEP_INPUT_CHARS", 16000, min_value=200),
                "LM_DEEP_MAX_OUTPUT_TOKENS": self._entry_int("LM_DEEP_MAX_OUTPUT_TOKENS", 1024, min_value=40),
                "LM_FAST_INPUT_CHARS": self._entry_int("LM_FAST_INPUT_CHARS", lp.DEFAULT_LM_FAST_INPUT_CHARS, min_value=200),
                "LM_FAST_MAX_OUTPUT_TOKENS": self._entry_int("LM_FAST_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_FAST_MAX_OUTPUT_TOKENS, min_value=40),
                "LM_FAST_CONFIDENCE_MIN": self._entry_float(self.lm_confidence_var, lp.DEFAULT_LM_FAST_CONFIDENCE_MIN, min_value=0.0),
                "LM_MIN_SNIPPET_LETTERS": self._entry_int("LM_MIN_SNIPPET_LETTERS", lp.DEFAULT_LM_MIN_SNIPPET_LETTERS, min_value=1),
                "TRANSLATE_OUTPUT_NAMES": bool(self.rename_output_var.get()),
                "OUTPUT_LANGUAGE": self.output_language_var.get().strip().lower() or "auto",
                "GUI_DEFAULT_LANGUAGE": self.language,
                "GUI_WINDOW_WIDTH": self._entry_int("GUI_WINDOW_WIDTH", self.window_width, min_value=WINDOW_MIN_WIDTH),
                "GUI_WINDOW_HEIGHT": self._entry_int("GUI_WINDOW_HEIGHT", self.window_height, min_value=WINDOW_MIN_HEIGHT),
                "GUI_FONT_FAMILY": self.gui_font_family_var.get().strip() or "Segoe UI",
                "UNPACK_WORKERS": self._entry_int("UNPACK_WORKERS", lp.DEFAULT_UNPACK_WORKERS, min_value=1),
                "DETECT_WORKERS": self._entry_int("DETECT_WORKERS", lp.DEFAULT_DETECT_WORKERS, min_value=1),
                "DEDUPE_WORKERS": self._entry_int("DEDUPE_WORKERS", lp.DEFAULT_DEDUPE_WORKERS, min_value=1),
                "TAG_WORKERS": self._entry_int("TAG_WORKERS", lp.DEFAULT_TAG_WORKERS, min_value=1),
                "LM_WORKERS": self._entry_int("LM_WORKERS", lp.DEFAULT_LM_WORKERS, min_value=1),
                "RENAME_WORKERS": self._entry_int("RENAME_WORKERS", lp.DEFAULT_RENAME_WORKERS, min_value=1),
                "PACK_WORKERS": self._entry_int("PACK_WORKERS", lp.DEFAULT_PACK_WORKERS, min_value=1),
                "TARGET_HASH_SCAN_WORKERS": self._entry_int("TARGET_HASH_SCAN_WORKERS", lp.DEFAULT_TARGET_HASH_SCAN_WORKERS, min_value=1),
            }
            if source_dirs:
                values["SOURCE_DIRS"] = source_dirs
            for name, var in self.boolean_setting_vars.items():
                values[name] = bool(var.get())
            for name in self.gui_font_vars:
                values[name] = self._entry_int(name, 10, min_value=6)

            text = setting_path.read_text(encoding="utf-8")
            updated = text
            for name, value in values.items():
                line = f"{name} = {self._setting_value_repr(value)}"
                pattern = re.compile(
                    rf"^{re.escape(name)}\s*=\s*(?:\[[\s\S]*?\]|.*)$",
                    flags=re.MULTILINE,
                )
                if pattern.search(updated):
                    updated = pattern.sub(lambda _m, line=line: line, updated, count=1)
                else:
                    updated = updated.rstrip() + "\n" + line + "\n"
            setting_path.write_text(updated, encoding="utf-8")
            self._set_status("settings_saved")
        except Exception as exc:
            messagebox.showerror(
                self.tr("tab_settings"),
                self.tr("settings_save_error", error=exc),
            )

    def _c(self, key: str, fallback: str = "#000000") -> str:
        return str(self.palette.get(key, fallback))

    def _build_config(self) -> lp.Config:
        source_dirs = lp.parse_sources_input([self.source_var.get().strip()])
        target = Path(self.target_var.get().strip())
        dupes = Path(self.dupes_var_path.get().strip() or str(target / "Duplicates"))
        nobook = Path(self.nobook_var_path.get().strip() or str(target / "NoBook"))
        temp_base = self._resolve_temp_base(target)

        lm_url = self.lm_url_var.get().strip() or lp.DEFAULT_LM_URL
        lm_model = self.lm_model_var.get().strip() or lp.DEFAULT_LM_MODEL

        deep_analysis = bool(self.deep_analysis_var.get())
        lm_timeout_sec = self._entry_int("LM_TIMEOUT_SEC", lp.DEFAULT_LM_TIMEOUT_SEC, min_value=10)
        lm_input_chars = self._entry_int("LM_INPUT_CHARS", lp.DEFAULT_LM_INPUT_CHARS, min_value=200)
        lm_max_output_tokens = self._entry_int("LM_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_MAX_OUTPUT_TOKENS, min_value=40)
        if deep_analysis:
            lm_timeout_sec = max(
                lm_timeout_sec,
                self._entry_int("LM_DEEP_TIMEOUT_SEC", 120, min_value=10),
            )
            lm_input_chars = max(
                lm_input_chars,
                self._entry_int("LM_DEEP_INPUT_CHARS", 16000, min_value=200),
            )
            lm_max_output_tokens = max(
                lm_max_output_tokens,
                self._entry_int("LM_DEEP_MAX_OUTPUT_TOKENS", 1024, min_value=40),
            )

        return lp.Config(
            source_dirs=source_dirs,
            target_dir=target,
            dupes_dir=dupes,
            nobook_dir=nobook,
            temp_base=temp_base,
            lm_url=lm_url,
            lm_model=lm_model,
            queue_size=self._entry_int("QUEUE_SIZE", lp.DEFAULT_QUEUE_SIZE, min_value=1),
            unpack_workers=self._entry_int("UNPACK_WORKERS", lp.DEFAULT_UNPACK_WORKERS, min_value=1),
            detect_workers=self._entry_int("DETECT_WORKERS", lp.DEFAULT_DETECT_WORKERS, min_value=1),
            dedupe_workers=self._entry_int("DEDUPE_WORKERS", lp.DEFAULT_DEDUPE_WORKERS, min_value=1),
            tag_workers=self._entry_int("TAG_WORKERS", lp.DEFAULT_TAG_WORKERS, min_value=1),
            lm_workers=self._entry_int("LM_WORKERS", lp.DEFAULT_LM_WORKERS, min_value=1),
            rename_workers=self._entry_int("RENAME_WORKERS", lp.DEFAULT_RENAME_WORKERS, min_value=1),
            pack_workers=self._entry_int("PACK_WORKERS", lp.DEFAULT_PACK_WORKERS, min_value=1),
            max_parallel_archives=self._entry_int("MAX_PARALLEL_ARCHIVES", lp.DEFAULT_MAX_PARALLEL_ARCHIVES, min_value=1),
            delete_source_after_pack=not self.keep_sources_var.get(),
            keep_temp_nobooks=False,
            lm_timeout_sec=lm_timeout_sec,
            lm_input_chars=lm_input_chars,
            lm_max_output_tokens=lm_max_output_tokens,
            lm_fast_precheck=False if deep_analysis else bool(self.boolean_setting_vars["LM_FAST_PRECHECK"].get()),
            lm_fast_input_chars=self._entry_int("LM_FAST_INPUT_CHARS", lp.DEFAULT_LM_FAST_INPUT_CHARS, min_value=200),
            lm_fast_max_output_tokens=self._entry_int("LM_FAST_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_FAST_MAX_OUTPUT_TOKENS, min_value=40),
            lm_fast_confidence_min=self._entry_float(
                self.lm_confidence_var, lp.DEFAULT_LM_FAST_CONFIDENCE_MIN, min_value=0.0
            ),
            lm_force_full_metadata=deep_analysis or bool(self.boolean_setting_vars["LM_FORCE_FULL_METADATA"].get()),
            lm_fill_unknown_author=deep_analysis
            or bool(self.boolean_setting_vars["LM_FILL_UNKNOWN_AUTHOR"].get()),
            lm_always_try_without_snippet=bool(self.boolean_setting_vars["LM_ALWAYS_TRY_WITHOUT_SNIPPET"].get()),
            lm_strict_json_mode=bool(self.boolean_setting_vars["LM_STRICT_JSON_MODE"].get()),
            lm_min_snippet_letters=self._entry_int("LM_MIN_SNIPPET_LETTERS", lp.DEFAULT_LM_MIN_SNIPPET_LETTERS, min_value=1),
            seed_hashes_from_target=bool(self.boolean_setting_vars["SEED_HASHES_FROM_TARGET"].get()),
            target_hash_scan_workers=self._entry_int("TARGET_HASH_SCAN_WORKERS", lp.DEFAULT_TARGET_HASH_SCAN_WORKERS, min_value=1),
            isbn_lookup=bool(self.boolean_setting_vars["ISBN_LOOKUP"].get()),
            translate_output_names=bool(self.rename_output_var.get()),
            output_language=self._selected_output_language(),
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
        for key in lp.AGENT_KEYS:
            self._rebuild_agent_indicator_segments(key)
        self.pipeline_running = True
        self.shutdown_started = False
        self.stop_requested_by_user = False
        self.current_mode = "RUNNING"
        self.mode_var.set(self._mode_label("RUNNING"))
        self._eta_display_value = "--:--:--"
        self._eta_display_updated_at = 0.0
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
        self.stop_requested_by_user = True
        self.sorter.request_stop_and_cleanup()
        self.current_mode = "STOP_CLEANUP"
        self.mode_var.set(self._mode_label("STOP_CLEANUP"))
        self._set_status("status_stopping_cleanup")

    def _poll_pipeline(self) -> None:
        if self.pipeline_running and self.pipeline_thread and not self.pipeline_thread.is_alive():
            if self.sorter:
                try:
                    snap = self.sorter.metrics.snapshot(
                        self.sorter.queue_sizes(),
                        self.sorter.stage_flags(),
                    )
                    self._render_snapshot(snap)
                except Exception:
                    pass
            self.pipeline_running = False
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            exit_code = self.pipeline_exit_code if self.pipeline_exit_code is not None else 2
            stopped_by_user = self.stop_requested_by_user
            if exit_code == 0 and stopped_by_user:
                self._set_status("status_stopped_cleanup_done")
            elif exit_code == 0:
                self._set_status("status_finished_ok")
            else:
                if self.pipeline_error:
                    self._set_status("status_execution_error", error=self.pipeline_error)
                else:
                    self._set_status("status_finished_code", code=exit_code)
            if exit_code == 0 and stopped_by_user:
                self.current_mode = "STOPPED"
            else:
                self.current_mode = "END" if exit_code == 0 else "END_ERROR"
            self.mode_var.set(self._mode_label(self.current_mode))
            if (
                exit_code == 0
                and not stopped_by_user
                and self.shutdown_after_done_var.get()
            ):
                self._shutdown_computer()
        elif self.sorter and self.pipeline_running:
            try:
                snap = self.sorter.metrics.snapshot(
                    self.sorter.queue_sizes(),
                    self.sorter.stage_flags(),
                )
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
        self._refresh_status_line(snap)

        seen = int(snap.get("seen", 0))
        done = int(snap.get("done", 0))
        results = snap.get("results", {}) or {}
        book_results = snap.get("book_results", {}) or {}
        self.seen_var.set(self._fmt_num(seen))
        self.done_var.set(self._fmt_num(done))
        self.packed_var.set(self._fmt_num(int(book_results.get("packed", 0))))
        self.dupes_var.set(self._fmt_num(int(book_results.get("duplicate", 0))))
        self.nobook_var.set(self._fmt_num(int(snap.get("nobook_files", 0))))
        self.failed_var.set(self._fmt_num(int(book_results.get("failed", 0))))

        stage_processed = snap.get("stage_processed", {}) or {}
        stage_errors = snap.get("stage_errors", {}) or {}
        queue_sizes = snap.get("queue_sizes", {}) or {}
        active_stage_slots = snap.get("active_stage_slots", {}) or {}
        for key in lp.AGENT_KEYS:
            processed = int(stage_processed.get(key, 0))
            errors = int(stage_errors.get(key, 0))
            qsize = int(queue_sizes.get(key, 0))
            self.agent_processed[key].set(self._fmt_num(processed))
            self.agent_errors[key].set(self._fmt_num(errors))
            self.agent_queue[key].set(self._fmt_num(qsize))
            self._update_agent_visual_state(key, active_stage_slots.get(key, []))

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
        now = time.monotonic()
        mode = fix_mojibake(str(snap.get("mode", "")))
        if (
            self._eta_display_updated_at <= 0
            or now - self._eta_display_updated_at >= 10
            or mode not in {"RUNNING", "INIT"}
        ):
            self._eta_display_value = eta
            self._eta_display_updated_at = now
        eta = self._eta_display_value
        return f"{self._time_without_seconds(elapsed)}/~{self._time_without_seconds(eta)}"

    def _time_without_seconds(self, value: str) -> str:
        parts = str(value or "").split(":")
        if len(parts) >= 3:
            return ":".join(parts[:2])
        return str(value or "--:--")

    def _agent_title(self, key: str) -> str:
        return f"{key} {self.tr(f'agent_{key}')}"

    def _agent_indicator_color(self, key: str, active: bool) -> str:
        if active:
            return "#ffd166" if self.theme_mode == "dark" else "#b45309"
        return "#1f2937" if self.theme_mode == "dark" else "#cbd5e1"

    def _agent_worker_total(self, key: str) -> int:
        if key == "A1":
            return 1
        worker_map = {
            "A2": ("unpack_workers", "UNPACK_WORKERS", lp.DEFAULT_UNPACK_WORKERS),
            "A3": ("detect_workers", "DETECT_WORKERS", lp.DEFAULT_DETECT_WORKERS),
            "A4": ("dedupe_workers", "DEDUPE_WORKERS", lp.DEFAULT_DEDUPE_WORKERS),
            "A5": ("tag_workers", "TAG_WORKERS", lp.DEFAULT_TAG_WORKERS),
            "A6": ("lm_workers", "LM_WORKERS", lp.DEFAULT_LM_WORKERS),
            "A7": ("rename_workers", "RENAME_WORKERS", lp.DEFAULT_RENAME_WORKERS),
            "A8": ("pack_workers", "PACK_WORKERS", lp.DEFAULT_PACK_WORKERS),
        }
        attr_name, setting_name, default = worker_map.get(key, ("", "", 1))
        sorter = getattr(self, "sorter", None)
        if sorter is not None and attr_name:
            try:
                return max(1, int(getattr(sorter.config, attr_name, default)))
            except Exception:
                pass
        if setting_name:
            return self._setting_int(setting_name, default, min_value=1)
        return 1

    def _rebuild_agent_indicator_segments(self, key: str) -> None:
        bar = self.agent_indicator_bars.get(key)
        if not bar:
            return
        total = self._agent_worker_total(key)
        previous_total = self.agent_indicator_totals.get(key, 0)
        if previous_total == total and self.agent_indicator_segments.get(key):
            return
        for child in bar.winfo_children():
            child.destroy()
        for row in range(max(previous_total, total)):
            bar.grid_rowconfigure(row, weight=0, uniform="")
        bar.grid_columnconfigure(0, weight=1)

        gap = 1 if total <= 12 else 0
        segments: list[tk.Frame] = []
        for row in range(total):
            bar.grid_rowconfigure(row, weight=1, uniform=f"{key}_indicator")
            segment = tk.Frame(bar, bg=self._agent_indicator_color(key, False), height=1)
            segment.grid(row=row, column=0, sticky="nsew", pady=(gap, 0))
            segments.append(segment)
        self.agent_indicator_segments[key] = segments
        self.agent_indicator_totals[key] = total

    def _log_text(self, path: object = "-") -> str:
        return f"{self.tr('log_prefix')}: {path}"

    def _refresh_status_line(self, snap: Optional[dict]) -> None:
        current = snap or self.last_snapshot or {}
        active_stage_items = current.get("active_stage_items", {}) or {}
        active_a6 = fix_mojibake(str(active_stage_items.get("A6", "")).strip())
        if self.pipeline_running and self.current_mode == "RUNNING":
            if active_a6:
                self.status_var.set(self.tr("status_a6_current", file=active_a6))
            else:
                self.status_var.set(self.tr("status_a6_waiting"))
            return
        self.status_var.set(self.tr(self.status_key, **self.status_kwargs))

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
                ["shutdown", "/s", "/f", "/t", "300"],
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

    def _update_agent_visual_state(self, key: str, active_slots: object) -> None:
        try:
            title = self.agent_title_labels.get(key)
            if title:
                title.configure(text=self._agent_title(key))
            self._rebuild_agent_indicator_segments(key)

            if isinstance(active_slots, (list, tuple, set)):
                active_set = {fix_mojibake(str(slot)) for slot in active_slots}
            else:
                active_set = set()

            for idx, segment in enumerate(self.agent_indicator_segments.get(key, []), start=1):
                active = f"W{idx}" in active_set
                segment.configure(bg=self._agent_indicator_color(key, active))
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

