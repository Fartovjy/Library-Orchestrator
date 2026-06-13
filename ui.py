#!/usr/bin/env python3
"""
GUI dashboard for the library sorting pipeline.

Layout: full-screen-height vertical panel, anchored to right screen edge.
Single-column layout optimised for a running pipeline at 500 px width.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Optional

import library_pipeline as lp

# When running as a single-file PyInstaller EXE, setting.py lives next to the EXE.
if getattr(sys, "frozen", False):
    import os as _os
    _exe_dir = _os.path.dirname(sys.executable)
    if _exe_dir not in sys.path:
        sys.path.insert(0, _exe_dir)

try:
    import setting
except Exception:
    setting = None

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except Exception:
    DND_FILES = None
    TkinterDnD = None


# ── Default window geometry ───────────────────────────────────────────────────
WINDOW_DEFAULT_WIDTH  = 580
WINDOW_MIN_WIDTH      = 360
WINDOW_MIN_HEIGHT     = 600

# ── Agent colour map (dark theme) ────────────────────────────────────────────
AGENT_COLORS_DARK = {
    "A1":  ("#0f4c5c", "#e2f4fb"),
    "A2":  ("#1a3f5c", "#d6eaf8"),
    "A3":  ("#1a4731", "#d4edda"),
    "A4":  ("#4a3520", "#fdebd0"),
    "A5":  ("#4a2014", "#fce8e0"),
    "A5b": ("#3d1a50", "#f0d9f8"),
    "A6":  ("#2f1f4f", "#ddd6fe"),
    "A7":  ("#0f3a37", "#ccf2ee"),
    "A8":  ("#1c2435", "#dce4f0"),
}

AGENT_COLORS_LIGHT = {
    "A1":  ("#cce8f2", "#0b2a33"),
    "A2":  ("#c9dff5", "#10243f"),
    "A3":  ("#caeedd", "#173525"),
    "A4":  ("#f5e6c8", "#4a3310"),
    "A5":  ("#f3d4c6", "#4a2014"),
    "A5b": ("#eeddfa", "#3d1a50"),
    "A6":  ("#ddd3f9", "#2f1f4f"),
    "A7":  ("#c8ecea", "#0f3a37"),
    "A8":  ("#d8dee9", "#1c2435"),
}

THEME_DARK = {
    "root_bg":                 "#0b1220",
    "panel_bg":                "#111827",
    "card_bg":                 "#161e2e",
    "ctrl_bg":                 "#0f172a",
    "drop_bg":                 "#1a2436",
    "drop_bg_active":          "#1e3a2f",   # green tint when pipeline running
    "drop_border":             "#334155",
    "drop_border_active":      "#22c55e",
    "section_sep":             "#1e2d40",
    "text_primary":            "#f1f5f9",
    "text_secondary":          "#94a3b8",
    "text_muted":              "#475569",
    "text_status":             "#cbd5e1",
    "text_event_error":        "#f87171",
    "text_event_info":         "#94a3b8",
    "legend_fg":               "#4b5563",
    "btn_secondary_bg":        "#1e293b",
    "btn_secondary_fg":        "#cbd5e1",
    "btn_secondary_active_bg": "#334155",
    "btn_secondary_active_fg": "#f1f5f9",
    "btn_start_bg":            "#15803d",
    "btn_start_fg":            "#f0fdf4",
    "btn_start_active_bg":     "#16a34a",
    "btn_start_active_fg":     "#f0fdf4",
    "btn_start_disabled_fg":   "#0a3c1c",
    "btn_stop_bg":             "#991b1b",
    "btn_stop_fg":             "#fef2f2",
    "btn_stop_active_bg":      "#b91c1c",
    "btn_stop_active_fg":      "#fef2f2",
    "btn_stop_disabled_fg":    "#450a0a",
    "progress_trough":         "#1e293b",
    "progress_bg":             "#22c55e",
    "progress_border":         "#1e293b",
    "progress_dark":           "#16a34a",
    "stat_tile_bg":            "#161e2e",
    "stat_tile_border":        "#1e293b",
    "stat_value_fg":           "#f1f5f9",
    "stat_label_fg":           "#64748b",
    "indicator_active":        "#fbbf24",
    "indicator_idle":          "#1e293b",
    "events_bg":               "#0f172a",
    "footer_bg":               "#0d1525",
    "check_select_bg":         "#0f172a",
    "entry_bg":                "#1e293b",
    "entry_fg":                "#f1f5f9",
    "entry_insert":            "#f1f5f9",
    "entry_select_bg":         "#2d4a70",
}

THEME_LIGHT = {
    "root_bg":                 "#f0f4fb",
    "panel_bg":                "#e4eaf5",
    "card_bg":                 "#dce6f5",
    "ctrl_bg":                 "#d8e3f2",
    "drop_bg":                 "#cfdcef",
    "drop_bg_active":          "#d4f0dd",
    "drop_border":             "#9db4d0",
    "drop_border_active":      "#16a34a",
    "section_sep":             "#c4d0e5",
    "text_primary":            "#0f172a",
    "text_secondary":          "#334155",
    "text_muted":              "#64748b",
    "text_status":             "#0f172a",
    "text_event_error":        "#dc2626",
    "text_event_info":         "#334155",
    "legend_fg":               "#64748b",
    "btn_secondary_bg":        "#bfcfe5",
    "btn_secondary_fg":        "#0f172a",
    "btn_secondary_active_bg": "#a8bdd8",
    "btn_secondary_active_fg": "#0f172a",
    "btn_start_bg":            "#16a34a",
    "btn_start_fg":            "#f0fdf4",
    "btn_start_active_bg":     "#15803d",
    "btn_start_active_fg":     "#f0fdf4",
    "btn_start_disabled_fg":   "#14532d",
    "btn_stop_bg":             "#dc2626",
    "btn_stop_fg":             "#fef2f2",
    "btn_stop_active_bg":      "#b91c1c",
    "btn_stop_active_fg":      "#fef2f2",
    "btn_stop_disabled_fg":    "#7f1d1d",
    "progress_trough":         "#c4d0e5",
    "progress_bg":             "#16a34a",
    "progress_border":         "#9db4d0",
    "progress_dark":           "#15803d",
    "stat_tile_bg":            "#dce6f5",
    "stat_tile_border":        "#9db4d0",
    "stat_value_fg":           "#0f172a",
    "stat_label_fg":           "#64748b",
    "indicator_active":        "#b45309",
    "indicator_idle":          "#c4d0e5",
    "events_bg":               "#d4dff0",
    "footer_bg":               "#ccd8ec",
    "check_select_bg":         "#d8e3f2",
    "entry_bg":                "#e8eef8",
    "entry_fg":                "#0f172a",
    "entry_insert":            "#0f172a",
    "entry_select_bg":         "#a8bdd8",
}

# ── Warm landing-page palette (default) ─────────────────────────────────────
THEME_WARM = {
    "root_bg":                 "#F5F0E8",   # тёплый кремовый холст
    "panel_bg":                "#FFFFFF",   # белые карточки/панели
    "card_bg":                 "#FFFFFF",
    "ctrl_bg":                 "#FFFFFF",
    "drop_bg":                 "#FAF5EC",
    "drop_bg_active":          "#EEF6F0",   # лёгкий зелёный когда запущен
    "drop_border":             "#C8A86E",   # янтарная рамка
    "drop_border_active":      "#4E9E65",   # зелёная рамка
    "section_sep":             "#EBE2D2",
    "text_primary":            "#1C1510",   # почти чёрный тёплый
    "text_secondary":          "#6A5E52",   # средне-коричневый
    "text_muted":              "#A89C8E",   # приглушённый
    "text_status":             "#1C1510",
    "text_event_error":        "#B04040",
    "text_event_info":         "#6A5E52",
    "legend_fg":               "#A89C8E",
    "btn_secondary_bg":        "#E8DDD0",
    "btn_secondary_fg":        "#3C3028",
    "btn_secondary_active_bg": "#D8CDBF",
    "btn_secondary_active_fg": "#1C1510",
    "btn_start_bg":            "#4E7A52",   # приглушённый зелёный
    "btn_start_fg":            "#FFFFFF",
    "btn_start_active_bg":     "#3E6842",
    "btn_start_active_fg":     "#FFFFFF",
    "btn_start_disabled_fg":   "#223c25",
    "btn_stop_bg":             "#A83C3C",   # терракота
    "btn_stop_fg":             "#FFFFFF",
    "btn_stop_active_bg":      "#8C2E2E",
    "btn_stop_active_fg":      "#FFFFFF",
    "btn_stop_disabled_fg":    "#541e1e",
    "progress_trough":         "#EBE2D2",
    "progress_bg":             "#B87A40",   # янтарный прогресс
    "progress_border":         "#EBE2D2",
    "progress_dark":           "#9C6030",
    "stat_tile_bg":            "#FFFFFF",
    "stat_tile_border":        "#EBE2D2",
    "stat_value_fg":           "#1C1510",
    "stat_label_fg":           "#A89C8E",
    "indicator_active":        "#B87A40",
    "indicator_idle":          "#EBE2D2",
    "events_bg":               "#FDFAF4",
    "footer_bg":               "#EEE5D4",
    "check_select_bg":         "#E8DDD0",
    "entry_bg":                "#E8DDD0",
    "entry_fg":                "#1C1510",
    "entry_insert":            "#B87A40",
    "entry_select_bg":         "#D4C8B0",
}

# Все карточки агентов белые; акцент — в тексте заголовка и левой полоске
AGENT_COLORS_WARM = {
    "A1":  ("#FFFFFF", "#3A70A2"),
    "A2":  ("#FFFFFF", "#6458A0"),
    "A3":  ("#FFFFFF", "#347850"),
    "A4":  ("#FFFFFF", "#9C6C30"),
    "A5":  ("#FFFFFF", "#9C4232"),
    "A5b": ("#FFFFFF", "#6C4C9C"),
    "A6":  ("#FFFFFF", "#B87A40"),
    "A7":  ("#FFFFFF", "#247C74"),
    "A8":  ("#FFFFFF", "#24588C"),
}
# Цвет левой акцентной полоски карточки агента
_AGENT_STRIPE: dict[str, str] = {k: v[1] for k, v in AGENT_COLORS_WARM.items()}

LANGUAGE_FILES = {
    "ru": "ui_ru.json",
    "en": "ui_en.json",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _windows_is_dark_mode() -> Optional[bool]:
    if sys.platform != "win32":
        return None
    try:
        import winreg
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
            capture_output=True, text=True, timeout=2, check=False,
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
    return "dark"


def fix_mojibake(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text
    try:
        fixed = text.encode("cp1251").decode("utf-8")
        if fixed:
            return fixed
    except Exception:
        pass
    return text


def _resource_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", __file__))
    return Path(__file__).resolve().parent


def _config_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


# ── Tooltip ───────────────────────────────────────────────────────────────────

class _Tooltip:
    """Single floating tooltip that follows the cursor."""

    def __init__(self, root: tk.Tk) -> None:
        self._root = root
        self._win: Optional[tk.Toplevel] = None
        self._label: Optional[tk.Label] = None

    def show(self, text: str, x: int, y: int) -> None:
        if not text:
            self.hide()
            return
        if self._win is None:
            self._win = tk.Toplevel(self._root)
            self._win.wm_overrideredirect(True)
            self._win.wm_attributes("-topmost", True)
            self._label = tk.Label(
                self._win, text=text,
                bg="#2C2018", fg="#F2E8D4",
                font=("Segoe UI", 9),
                padx=9, pady=6,
                relief=tk.FLAT,
                wraplength=400,
                justify="left",
            )
            self._label.pack()
        else:
            assert self._label is not None
            self._label.configure(text=text)
        offset_x, offset_y = 14, 10
        self._win.wm_geometry(f"+{x + offset_x}+{y + offset_y}")

    def hide(self) -> None:
        if self._win is not None:
            self._win.destroy()
            self._win = None
            self._label = None


# ── Main app class ─────────────────────────────────────────────────────────────

class LibraryGUIApp:
    def __init__(self, root: tk.Tk, dnd_available: bool) -> None:
        self.root = root
        self.dnd_available = dnd_available
        # Предпочтение темы: warm (по умолчанию) → light → dark
        _theme_pref = (
            str(getattr(setting, "GUI_THEME", "warm")).strip().lower()
            if setting is not None else "warm"
        )
        if _theme_pref == "dark":
            self.theme_mode = "dark"
        elif _theme_pref == "light":
            self.theme_mode = "light"
        else:
            self.theme_mode = "warm"
        if self.theme_mode == "dark":
            self.palette = THEME_DARK
            self.agent_colors = AGENT_COLORS_DARK
        elif self.theme_mode == "light":
            self.palette = THEME_LIGHT
            self.agent_colors = AGENT_COLORS_LIGHT
        else:
            self.palette = THEME_WARM
            self.agent_colors = AGENT_COLORS_WARM
        self.translations = self._load_translations()
        self.language = self._default_language()
        self.last_snapshot: Optional[dict] = None
        self.current_mode = "IDLE"
        self.current_log_path: object = "-"
        self.status_key = "status_ready"
        self.status_kwargs: dict[str, object] = {}
        self.shutdown_started = False
        self.stop_requested_by_user = False
        self._tip_registry: list[tuple[tk.Widget, str]] = []

        # ── Fonts ──────────────────────────────────────────────────────
        font_family = "Segoe UI"
        if setting is not None:
            try:
                candidate = str(getattr(setting, "GUI_FONT_FAMILY", font_family)).strip()
                if candidate:
                    font_family = candidate
            except Exception:
                pass
        self.font_main    = (font_family, self._setting_int("GUI_FONT_MAIN_SIZE", 12, min_value=8))
        self.font_title   = (font_family, self._setting_int("GUI_FONT_TITLE_SIZE", 14, min_value=8), "bold")
        self.font_small   = (font_family, self._setting_int("GUI_FONT_SMALL_SIZE", 11, min_value=8))
        self.font_stats   = (font_family, self._setting_int("GUI_FONT_STATS_SIZE", 10, min_value=7))
        self.font_counter_label = (font_family, self._setting_int("GUI_FONT_COUNTER_LABEL_SIZE", 9, min_value=6))
        self.font_counter_value = (font_family, 20, "bold")   # большие цифры в плитках статистики
        self.font_legend  = (font_family, self._setting_int("GUI_FONT_LEGEND_SIZE", 9, min_value=6))
        self.font_agent_title  = (font_family, self._setting_int("GUI_FONT_AGENT_TITLE_SIZE", 10, min_value=7), "bold")
        self.font_agent_value  = (font_family, self._setting_int("GUI_FONT_AGENT_VALUE_SIZE", 9, min_value=7))
        self.font_agent_active = (font_family, self._setting_int("GUI_FONT_AGENT_VALUE_SIZE", 9, min_value=7))
        self.font_agent_metric_label = (font_family, self._setting_int("GUI_FONT_AGENT_METRIC_LABEL_SIZE", 8, min_value=6))
        self.font_agent_metric_value = (font_family, self._setting_int("GUI_FONT_AGENT_VALUE_SIZE", 9, min_value=7), "bold")
        self.font_drop_hint = (font_family, 9)
        self.font_events = (font_family, self._setting_int("GUI_FONT_SMALL_SIZE", 9, min_value=8))

        # ── Window size ─────────────────────────────────────────────────
        self.window_width = max(
            WINDOW_MIN_WIDTH,
            self._setting_int("GUI_WINDOW_WIDTH", WINDOW_DEFAULT_WIDTH, min_value=WINDOW_MIN_WIDTH),
        )
        self.browse_cell_width = 72

        # ── State variables ─────────────────────────────────────────────
        self.sorter: Optional[lp.LibrarySorter] = None
        self.pipeline_thread: Optional[threading.Thread] = None
        self.pipeline_running = False
        self.pipeline_exit_code: Optional[int] = None
        self.pipeline_error = ""

        self.source_var   = tk.StringVar(value="")
        self.target_var   = tk.StringVar(value=fix_mojibake(str(lp.DEFAULT_TARGET_DIR)))
        self.temp_base_var = tk.StringVar(value=self._setting_str("TEMP_BASE", str(lp.DEFAULT_TEMP_BASE)))
        self.mode_var     = tk.StringVar(value=self._mode_label("IDLE"))
        self.time_var     = tk.StringVar(value="")
        self.status_var   = tk.StringVar(value=self.tr("status_ready"))
        self.progress_text_var = tk.StringVar(value="0 %")
        self._eta_display_value    = "--:--"
        self._eta_display_updated_at = 0.0

        self.seen_var   = tk.StringVar(value="0")
        self.done_var   = tk.StringVar(value="0")
        self.packed_var = tk.StringVar(value="0")
        self.dupes_var  = tk.StringVar(value="0")
        self.nobook_var = tk.StringVar(value="0")
        self.failed_var = tk.StringVar(value="0")

        self.log_var    = tk.StringVar(value=f"{self.tr('log_prefix')}: -")
        self.shutdown_after_done_var = tk.BooleanVar(value=False)
        self.keep_sources_var  = tk.BooleanVar(value=self._setting_bool("KEEP_SOURCES", True))
        self.deep_analysis_var = tk.BooleanVar(value=self._setting_bool("LM_ITERATIVE_READ", False))
        self.rename_output_var = tk.BooleanVar(
            value=self._setting_bool("TRANSLATE_OUTPUT_NAMES", lp.DEFAULT_TRANSLATE_OUTPUT_NAMES)
        )

        # LM / ISBN settings
        self.lm_url_var          = tk.StringVar(value=self._setting_str("LM_URL", lp.DEFAULT_LM_URL))
        self.lm_model_var        = tk.StringVar(value=self._setting_str("LM_MODEL", lp.DEFAULT_LM_MODEL))
        self.lm_api_key_var      = tk.StringVar(value=self._setting_str("LM_API_KEY", lp.DEFAULT_LM_API_KEY))
        self.lm_url_rename_var   = tk.StringVar(value=self._setting_str("LM_URL_RENAME", lp.DEFAULT_LM_URL_RENAME))
        self.lm_model_rename_var = tk.StringVar(value=self._setting_str("LM_MODEL_RENAME", lp.DEFAULT_LM_MODEL_RENAME))
        self.lm_api_key_rename_var = tk.StringVar(value=self._setting_str("LM_API_KEY_RENAME", lp.DEFAULT_LM_API_KEY_RENAME))
        self.isbn_provider_var   = tk.StringVar(
            value=self._setting_choice("ISBN_PROVIDER", lp.DEFAULT_ISBN_PROVIDER
                  if lp.DEFAULT_ISBN_PROVIDER in {"auto","openlibrary","googlebooks"} else "auto",
                  {"auto","openlibrary","googlebooks"})
        )
        self.output_language_var = tk.StringVar(
            value=self._setting_choice("OUTPUT_LANGUAGE", lp.DEFAULT_OUTPUT_LANGUAGE, {"auto","ru","en"})
        )
        self.gui_font_family_var = tk.StringVar(value=font_family)

        # Worker vars
        self.worker_vars: dict[str, tk.StringVar] = {}
        for name, default in [
            ("UNPACK_WORKERS",          lp.DEFAULT_UNPACK_WORKERS),
            ("DETECT_WORKERS",          lp.DEFAULT_DETECT_WORKERS),
            ("DEDUPE_WORKERS",          lp.DEFAULT_DEDUPE_WORKERS),
            ("TAG_WORKERS",             lp.DEFAULT_TAG_WORKERS),
            ("ISBN_WORKERS",            lp.DEFAULT_ISBN_WORKERS),
            ("LM_WORKERS",              lp.DEFAULT_LM_WORKERS),
            ("RENAME_WORKERS",          lp.DEFAULT_RENAME_WORKERS),
            ("PACK_WORKERS",            lp.DEFAULT_PACK_WORKERS),
            ("MAX_PARALLEL_ARCHIVES",   lp.DEFAULT_MAX_PARALLEL_ARCHIVES),
            ("QUEUE_SIZE",              lp.DEFAULT_QUEUE_SIZE),
            ("TARGET_HASH_SCAN_WORKERS",lp.DEFAULT_TARGET_HASH_SCAN_WORKERS),
        ]:
            self.worker_vars[name] = tk.StringVar(value=str(self._setting_int(name, default, min_value=1)))

        self.lm_number_vars: dict[str, tk.StringVar] = {}
        for name, default in [
            ("LM_TIMEOUT_SEC",          lp.DEFAULT_LM_TIMEOUT_SEC),
            ("LM_INPUT_CHARS",          lp.DEFAULT_LM_INPUT_CHARS),
            ("LM_MAX_OUTPUT_TOKENS",    lp.DEFAULT_LM_MAX_OUTPUT_TOKENS),
            ("LM_DEEP_TIMEOUT_SEC",     120),
            ("LM_DEEP_INPUT_CHARS",     16000),
            ("LM_DEEP_MAX_OUTPUT_TOKENS", 1024),
            ("LM_FAST_INPUT_CHARS",     lp.DEFAULT_LM_FAST_INPUT_CHARS),
            ("LM_FAST_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_FAST_MAX_OUTPUT_TOKENS),
            ("LM_MIN_SNIPPET_LETTERS",  lp.DEFAULT_LM_MIN_SNIPPET_LETTERS),
            ("GUI_WINDOW_WIDTH",        self.window_width),
        ]:
            self.lm_number_vars[name] = tk.StringVar(value=str(self._setting_int(name, default, min_value=1)))
        self.lm_confidence_var = tk.StringVar(
            value=str(self._setting_float("LM_FAST_CONFIDENCE_MIN", lp.DEFAULT_LM_FAST_CONFIDENCE_MIN, min_value=0.0))
        )
        self.gui_font_vars: dict[str, tk.StringVar] = {}
        for name, default in [
            ("GUI_FONT_MAIN_SIZE", 12), ("GUI_FONT_TITLE_SIZE", 14),
            ("GUI_FONT_SMALL_SIZE", 11), ("GUI_FONT_STATS_SIZE", 10),
            ("GUI_FONT_COUNTER_LABEL_SIZE", 9), ("GUI_FONT_LEGEND_SIZE", 9),
            ("GUI_FONT_AGENT_TITLE_SIZE", 10), ("GUI_FONT_AGENT_VALUE_SIZE", 9),
            ("GUI_FONT_AGENT_METRIC_LABEL_SIZE", 8),
        ]:
            self.gui_font_vars[name] = tk.StringVar(value=str(self._setting_int(name, default, min_value=6)))
        self.boolean_setting_vars: dict[str, tk.BooleanVar] = {
            "LM_FAST_PRECHECK":          tk.BooleanVar(value=self._setting_bool("LM_FAST_PRECHECK", lp.DEFAULT_LM_FAST_PRECHECK)),
            "LM_FORCE_FULL_METADATA":    tk.BooleanVar(value=self._setting_bool("LM_FORCE_FULL_METADATA", lp.DEFAULT_LM_FORCE_FULL_METADATA)),
            "LM_FILL_UNKNOWN_AUTHOR":    tk.BooleanVar(value=self._setting_bool("LM_FILL_UNKNOWN_AUTHOR", lp.DEFAULT_LM_FILL_UNKNOWN_AUTHOR)),
            "LM_ALWAYS_TRY_WITHOUT_SNIPPET": tk.BooleanVar(value=self._setting_bool("LM_ALWAYS_TRY_WITHOUT_SNIPPET", lp.DEFAULT_LM_ALWAYS_TRY_WITHOUT_SNIPPET)),
            "LM_STRICT_JSON_MODE":       tk.BooleanVar(value=self._setting_bool("LM_STRICT_JSON_MODE", lp.DEFAULT_LM_STRICT_JSON_MODE)),
            "ISBN_LOOKUP":               tk.BooleanVar(value=self._setting_bool("ISBN_LOOKUP", lp.DEFAULT_ISBN_LOOKUP)),
            "SEED_HASHES_FROM_TARGET":   tk.BooleanVar(value=self._setting_bool("SEED_HASHES_FROM_TARGET", lp.DEFAULT_SEED_HASHES_FROM_TARGET)),
        }

        # Per-agent display dicts
        self.agent_processed:    dict[str, tk.StringVar] = {}
        self.agent_errors:       dict[str, tk.StringVar] = {}
        self.agent_queue:        dict[str, tk.StringVar] = {}
        self.agent_active_vars:  dict[str, tk.StringVar] = {}
        self.agent_cards:        dict[str, tk.Frame]    = {}
        self.agent_indicator_bars:     dict[str, tk.Frame]       = {}
        self.agent_indicator_segments: dict[str, list[tk.Frame]] = {}
        self.agent_indicator_totals:   dict[str, int]            = {}
        self.agent_labels:       dict[str, list[tk.Widget]]      = {}
        self.agent_title_labels: dict[str, tk.Label]             = {}
        self.stat_label_widgets: dict[str, tk.Label]             = {}
        self.settings_text_widgets: list[tuple[tk.Widget, str]]  = []
        self.events_text: Optional[tk.Text] = None
        self._tooltip = _Tooltip(self.root)

        self._build_window()
        self._build_ui()
        self._apply_initial_dirs()
        self._setup_keyboard_shortcuts()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(300, self._poll_pipeline)

    def _setup_keyboard_shortcuts(self) -> None:
        """Set up standard clipboard and selection keyboard shortcuts for Cyrillic and English layouts."""
        # 1. Select All for Entry
        def select_all_entry(event: tk.Event) -> str:
            event.widget.select_range(0, tk.END)
            event.widget.icursor(tk.END)
            return "break"

        self.root.bind_class("Entry", "<Control-a>", select_all_entry)
        self.root.bind_class("Entry", "<Control-A>", select_all_entry)
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_a>", select_all_entry)
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_A>", select_all_entry)

        # 2. Clipboard bindings for Entry (Cyrillic layout support)
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_es>", lambda e: e.widget.event_generate("<<Copy>>"))
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_ES>", lambda e: e.widget.event_generate("<<Copy>>"))
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_em>", lambda e: e.widget.event_generate("<<Paste>>"))
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_EM>", lambda e: e.widget.event_generate("<<Paste>>"))
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_che>", lambda e: e.widget.event_generate("<<Cut>>"))
        self.root.bind_class("Entry", "<Control-KeyPress-Cyrillic_CHE>", lambda e: e.widget.event_generate("<<Cut>>"))

        # 3. Select All for Text
        def select_all_text(event: tk.Event) -> str:
            event.widget.tag_add("sel", "1.0", "end")
            return "break"

        self.root.bind_class("Text", "<Control-a>", select_all_text)
        self.root.bind_class("Text", "<Control-A>", select_all_text)
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_a>", select_all_text)
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_A>", select_all_text)

        # 4. Clipboard bindings for Text (Cyrillic layout support)
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_es>", lambda e: e.widget.event_generate("<<Copy>>"))
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_ES>", lambda e: e.widget.event_generate("<<Copy>>"))
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_em>", lambda e: e.widget.event_generate("<<Paste>>"))
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_EM>", lambda e: e.widget.event_generate("<<Paste>>"))
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_che>", lambda e: e.widget.event_generate("<<Cut>>"))
        self.root.bind_class("Text", "<Control-KeyPress-Cyrillic_CHE>", lambda e: e.widget.event_generate("<<Cut>>"))

    # ── Translation ──────────────────────────────────────────────────────────

    def _load_translations(self) -> dict[str, dict[str, str]]:
        base = _resource_dir()
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
        if hasattr(self, "lang_ru_btn"):
            self.lang_ru_btn.configure(text=self.tr("language_ru"))
        if hasattr(self, "lang_en_btn"):
            self.lang_en_btn.configure(text=self.tr("language_en"))
        if hasattr(self, "notebook"):
            self.notebook.tab(self.main_tab,     text=self.tr("tab_main"))
            self.notebook.tab(self.settings_tab, text=self.tr("tab_settings"))
        if hasattr(self, "shutdown_check"):
            self.shutdown_check.configure(text=self.tr("shutdown_after_done"))
        if hasattr(self, "keep_sources_check"):
            self.keep_sources_check.configure(text=self.tr("keep_sources"))
        if hasattr(self, "deep_analysis_check"):
            self.deep_analysis_check.configure(text=self.tr("deep_analysis"))
        if hasattr(self, "rename_output_check"):
            self.rename_output_check.configure(text=self.tr("rename_output"))
        for widget, key in self.settings_text_widgets:
            widget.configure(text=self.tr(key))

        # Refresh all tooltip bindings (new language)
        for widget, key in self._tip_registry:
            self._bind_tip(widget, key)

        self._render_events(self.last_snapshot)

    # ── Tooltip helper ───────────────────────────────────────────────────────

    def _tip(self, widget: tk.Widget, key: str) -> None:
        """Register a tooltip on a widget, identified by translation key."""
        self._tip_registry.append((widget, key))
        self._bind_tip(widget, key)

    def _bind_tip(self, widget: tk.Widget, key: str) -> None:
        text = self.tr(key)
        try:
            widget.bind("<Enter>", lambda e, t=text: self._tooltip.show(t, e.x_root, e.y_root))
            widget.bind("<Leave>", lambda _e: self._tooltip.hide())
        except Exception:
            pass

    # ── Window construction ──────────────────────────────────────────────────

    def _build_window(self) -> None:
        self.root.title(self.tr("window_title"))
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        w  = self.window_width
        # Pin to right edge by default; GUI_WINDOW_EDGE = "left" moves to left
        edge = self._setting_str("GUI_WINDOW_EDGE", "right").strip().lower()
        x = max(0, sw - w) if edge != "left" else 0
        self.root.geometry(f"{w}x{sh}+{x}+0")
        self.root.minsize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
        self.root.resizable(True, True)
        self.root.configure(bg=self._c("root_bg"))

    def _build_ui(self) -> None:
        # ── TTK styles ───────────────────────────────────────────────────
        style = ttk.Style()
        style.theme_use("default")
        style.configure("App.TNotebook",
            background=self._c("root_bg"), borderwidth=0)
        style.configure("App.TNotebook.Tab",
            padding=(14, 5), font=self.font_legend)
        style.configure("Prog.Horizontal.TProgressbar",
            thickness=14,
            troughcolor=self._c("progress_trough"),
            background=self._c("progress_bg"),
            bordercolor=self._c("progress_border"),
            lightcolor=self._c("progress_bg"),
            darkcolor=self._c("progress_dark"),
        )

        # ── Notebook ─────────────────────────────────────────────────────
        self.notebook = ttk.Notebook(self.root, style="App.TNotebook")
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.main_tab     = tk.Frame(self.notebook, bg=self._c("root_bg"))
        self.settings_tab = tk.Frame(self.notebook, bg=self._c("root_bg"))
        self.notebook.add(self.main_tab,     text=self.tr("tab_main"))
        self.notebook.add(self.settings_tab, text=self.tr("tab_settings"))

        outer = tk.Frame(self.main_tab, bg=self._c("root_bg"), padx=8, pady=6)
        outer.pack(fill=tk.BOTH, expand=True)

        self._build_drop_section(outer)
        self._build_paths_section(outer)
        self._build_controls_section(outer)
        self._build_stats_section(outer)
        self._build_agents_section(outer)
        self._build_events_section(outer)     # expand=True
        self._build_footer_section(outer)

        self._build_settings_tab()

    # ── Drop zone ────────────────────────────────────────────────────────────

    def _build_drop_section(self, parent: tk.Frame) -> None:
        # Внешняя рамка с янтарной границей — «drag here» зона
        self.drop_frame = tk.Frame(
            parent,
            bg=self._c("drop_bg"),
            highlightthickness=2,
            highlightbackground=self._c("drop_border"),
            highlightcolor=self._c("drop_border"),
            height=92,
        )
        self.drop_frame.pack(fill=tk.X, pady=(0, 10))
        self.drop_frame.pack_propagate(False)

        inner = tk.Frame(self.drop_frame, bg=self._c("drop_bg"))
        inner.place(relx=0.5, rely=0.5, anchor="center")

        self.drop_title_label = tk.Label(
            inner,
            text=self.tr("drop_title"),
            font=(self.font_main[0], self.font_main[1], "bold"),
            bg=self._c("drop_bg"),
            fg=self._c("drop_border"),   # янтарный текст заголовка
            justify=tk.CENTER,
        )
        self.drop_title_label.pack()

        dnd_text = self.tr("drop_hint") if self.dnd_available else self.tr("dnd_unavailable")
        self.drop_hint_label = tk.Label(
            inner,
            text=dnd_text,
            font=self.font_drop_hint,
            bg=self._c("drop_bg"),
            fg=self._c("text_muted"),
            justify=tk.CENTER,
        )
        self.drop_hint_label.pack()

        self._tip(self.drop_frame,       "tip_drop_zone")
        self._tip(self.drop_title_label, "tip_drop_zone")
        self._tip(self.drop_hint_label,  "tip_drop_zone")

        if self.dnd_available and DND_FILES:
            self.drop_frame.drop_target_register(DND_FILES)
            self.drop_frame.dnd_bind("<<Drop>>", self._on_source_drop)

    def _update_drop_zone_state(self) -> None:
        """Переключает цвета drop-зоны при старте/остановке конвейера."""
        if self.pipeline_running:
            bg       = self._c("drop_bg_active")
            border   = self._c("drop_border_active")
            title_fg = self._c("drop_border_active")
        else:
            bg       = self._c("drop_bg")
            border   = self._c("drop_border")
            title_fg = self._c("drop_border")
        self.drop_frame.configure(
            bg=bg, highlightbackground=border, highlightcolor=border
        )
        self.drop_title_label.configure(bg=bg, fg=title_fg)
        self.drop_hint_label.configure(bg=bg)
        for w in self.drop_frame.winfo_children():
            try:
                w.configure(bg=bg)
            except Exception:
                pass

    # ── Paths ─────────────────────────────────────────────────────────────────

    def _build_paths_section(self, parent: tk.Frame) -> None:
        frame = tk.Frame(parent, bg=self._c("panel_bg"), padx=10, pady=8)
        frame.pack(fill=tk.X, pady=(0, 8))
        frame.grid_columnconfigure(1, weight=1)

        # SOURCE
        self.source_label = tk.Label(
            frame, text=self.tr("source_label"), font=self.font_small,
            bg=self._c("panel_bg"), fg=self._c("text_secondary"), anchor="w", width=8,
        )
        self.source_label.grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.source_entry = tk.Entry(
            frame, textvariable=self.source_var, font=self.font_small,
            bg=self._c("entry_bg"), fg=self._c("entry_fg"),
            insertbackground=self._c("entry_insert"),
            selectbackground=self._c("entry_select_bg"),
            relief=tk.FLAT, bd=0,
        )
        self.source_entry.grid(row=0, column=1, sticky="ew", padx=(6, 6), pady=(0, 4))
        self.source_btn = tk.Button(
            frame, text=self.tr("browse"), font=self.font_legend,
            command=self._browse_source,
            bg=self._c("btn_secondary_bg"), fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
            relief=tk.FLAT, padx=6,
        )
        self.source_btn.grid(row=0, column=2, sticky="ew", pady=(0, 4))

        # TARGET
        self.target_label = tk.Label(
            frame, text=self.tr("target_label"), font=self.font_small,
            bg=self._c("panel_bg"), fg=self._c("text_secondary"), anchor="w", width=8,
        )
        self.target_label.grid(row=1, column=0, sticky="w")
        self.target_entry = tk.Entry(
            frame, textvariable=self.target_var, font=self.font_small,
            bg=self._c("entry_bg"), fg=self._c("entry_fg"),
            insertbackground=self._c("entry_insert"),
            selectbackground=self._c("entry_select_bg"),
            relief=tk.FLAT, bd=0,
        )
        self.target_entry.grid(row=1, column=1, sticky="ew", padx=(6, 6))
        self.target_btn = tk.Button(
            frame, text=self.tr("browse"), font=self.font_legend,
            command=self._browse_target,
            bg=self._c("btn_secondary_bg"), fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
            relief=tk.FLAT, padx=6,
        )
        self.target_btn.grid(row=1, column=2, sticky="ew")

        # Hint
        self.multi_source_label = tk.Label(
            frame, text=self.tr("multi_source_hint"), font=self.font_legend,
            bg=self._c("panel_bg"), fg=self._c("text_muted"), anchor="w",
        )
        self.multi_source_label.grid(row=2, column=0, columnspan=3, sticky="w", pady=(4, 0))

        self._tip(self.source_entry,  "tip_source_entry")
        self._tip(self.source_label,  "tip_source_entry")
        self._tip(self.source_btn,    "tip_source_browse")
        self._tip(self.target_entry,  "tip_target_entry")
        self._tip(self.target_label,  "tip_target_entry")
        self._tip(self.target_btn,    "tip_target_browse")

    # ── Controls: Start / Stop + progress ────────────────────────────────────

    def _build_controls_section(self, parent: tk.Frame) -> None:
        frame = tk.Frame(parent, bg=self._c("ctrl_bg"), padx=10, pady=8)
        frame.pack(fill=tk.X, pady=(0, 8))
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_columnconfigure(1, weight=1)

        # Row 0 – buttons
        btn_font = (self.font_title[0], self.font_title[1], "bold")
        self.start_btn = tk.Button(
            frame, text=self.tr("start"), font=btn_font,
            command=self._start_pipeline,
            bg=self._c("btn_start_bg"), fg=self._c("btn_start_fg"),
            activebackground=self._c("btn_start_active_bg"),
            activeforeground=self._c("btn_start_active_fg"),
            disabledforeground=self._c("btn_start_disabled_fg"),
            relief=tk.FLAT, height=2, padx=10,
        )
        self.start_btn.grid(row=0, column=0, sticky="ew", padx=(0, 5), pady=(0, 6))

        self.stop_btn = tk.Button(
            frame, text=self.tr("stop"), font=btn_font,
            command=self._stop_pipeline,
            bg=self._c("btn_stop_bg"), fg=self._c("btn_stop_fg"),
            activebackground=self._c("btn_stop_active_bg"),
            activeforeground=self._c("btn_stop_active_fg"),
            disabledforeground=self._c("btn_stop_disabled_fg"),
            relief=tk.FLAT, height=2, padx=10,
            state=tk.DISABLED,
        )
        self.stop_btn.grid(row=0, column=1, sticky="ew", padx=(5, 0), pady=(0, 6))

        self._tip(self.start_btn, "tip_start_btn")
        self._tip(self.stop_btn,  "tip_stop_btn")

        # Row 1 – progress bar + percent
        prog_frame = tk.Frame(frame, bg=self._c("ctrl_bg"))
        prog_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 2))
        prog_frame.grid_columnconfigure(0, weight=1)

        self.progress = ttk.Progressbar(
            prog_frame, style="Prog.Horizontal.TProgressbar",
            maximum=100.0, mode="determinate",
        )
        self.progress.grid(row=0, column=0, sticky="ew", padx=(0, 6))

        tk.Label(
            prog_frame, textvariable=self.progress_text_var,
            font=self.font_main,
            bg=self._c("ctrl_bg"), fg=self._c("text_primary"),
            anchor="e", width=5,
        ).grid(row=0, column=1, sticky="e")

        self._tip(self.progress, "tip_progress")

        # Row 2 – GB / time label
        self.time_label = tk.Label(
            frame, textvariable=self.time_var,
            font=self.font_legend,
            bg=self._c("ctrl_bg"), fg=self._c("text_secondary"),
            anchor="w",
        )
        self.time_label.grid(row=2, column=0, columnspan=2, sticky="ew")
        self._tip(self.time_label, "tip_time_label")

    # ── Stats tiles (3 × 2) ─────────────────────────────────────────────────

    def _build_stats_section(self, parent: tk.Frame) -> None:
        frame = tk.Frame(parent, bg=self._c("root_bg"))
        frame.pack(fill=tk.X, pady=(0, 8))
        for col in range(3):
            frame.grid_columnconfigure(col, weight=1, uniform="stat_tiles")

        # 6 плиток: Найдено · Завершено · Упаковано / Дубликаты · Не книги · Ошибки
        stat_defs = [
            ("stats_books_found", self.seen_var,   "tip_stat_seen"),
            ("stats_books_done",  self.done_var,   "tip_stat_done"),
            ("stats_packed",      self.packed_var, "tip_stat_packed"),
            ("stats_duplicates",  self.dupes_var,  "tip_stat_dupes"),
            ("stats_nobooks",     self.nobook_var, "tip_stat_nobook"),
            ("stats_book_errors", self.failed_var, "tip_stat_failed"),
        ]
        GAP = 6  # пространство между плитками
        for idx, (label_key, var, tip_key) in enumerate(stat_defs):
            r = idx // 3
            c = idx % 3
            px_left  = 0 if c == 0 else GAP // 2
            px_right = 0 if c == 2 else GAP // 2
            py_top   = 0 if r == 0 else GAP // 2
            py_bot   = 0 if r == 1 else GAP // 2

            tile = tk.Frame(
                frame, bg=self._c("stat_tile_bg"),
                highlightthickness=1,
                highlightbackground=self._c("stat_tile_border"),
                padx=6, pady=8,
            )
            tile.grid(row=r, column=c, sticky="nsew",
                      padx=(px_left, px_right), pady=(py_top, py_bot))
            tile.grid_columnconfigure(0, weight=1)

            lbl = tk.Label(
                tile, text=self.tr(label_key),
                font=self.font_counter_label,
                bg=self._c("stat_tile_bg"), fg=self._c("stat_label_fg"),
                anchor="center",
            )
            lbl.pack(fill=tk.X)
            self.stat_label_widgets[label_key] = lbl

            tk.Label(
                tile, textvariable=var,
                font=self.font_counter_value,
                bg=self._c("stat_tile_bg"), fg=self._c("stat_value_fg"),
                anchor="center",
            ).pack(fill=tk.X)

            self._tip(tile, tip_key)
            self._tip(lbl,  tip_key)

    # ── Agent grid (3 × 3) ───────────────────────────────────────────────────

    def _build_agents_section(self, parent: tk.Frame) -> None:
        COLS = 3
        agents_frame = tk.Frame(parent, bg=self._c("root_bg"))
        agents_frame.pack(fill=tk.X, pady=(0, 6))
        for col in range(COLS):
            agents_frame.grid_columnconfigure(col, weight=1, uniform="agent_cols")
        self.agents_frame = agents_frame

        for idx, key in enumerate(lp.AGENT_KEYS):
            row = idx // COLS
            col = idx % COLS
            bg, fg = self.agent_colors[key]   # ("FFFFFF", accent) в warm-теме
            # Цвет левой полоски: из AGENT_COLORS_WARM если warm-тема, иначе fg
            stripe_color = (
                _AGENT_STRIPE.get(key, fg)
                if self.theme_mode == "warm" else fg
            )
            GAP = 6
            px_left  = 0 if col == 0 else GAP // 2
            px_right = 0 if col == COLS - 1 else GAP // 2
            py_top   = 0 if row == 0 else GAP // 2
            py_bot   = GAP // 2

            card = tk.Frame(
                agents_frame, bg=bg,
                highlightthickness=1,
                highlightbackground=self._c("stat_tile_border"),
            )
            card.grid(row=row, column=col, sticky="nsew",
                      padx=(px_left, px_right), pady=(py_top, py_bot))
            self.agent_cards[key] = card
            self.agent_labels[key] = []

            # Левая акцентная полоска
            stripe = tk.Frame(card, bg=stripe_color, width=4)
            stripe.pack(side=tk.LEFT, fill=tk.Y)
            stripe.pack_propagate(False)

            # Правый индикатор воркеров
            indicator = tk.Frame(card, bg=self._c("stat_tile_border"), width=12)
            indicator.pack(side=tk.RIGHT, fill=tk.Y)
            indicator.pack_propagate(False)
            self.agent_indicator_bars[key] = indicator
            self._rebuild_agent_indicator_segments(key)

            # Контентная область
            content = tk.Frame(card, bg=bg)
            content.pack(side=tk.LEFT, fill=tk.BOTH, expand=True,
                         padx=(6, 3), pady=(4, 4))

            # Заголовок агента (акцентный цвет)
            title_lbl = tk.Label(
                content, text=self._agent_title(key),
                font=self.font_agent_title, bg=bg, fg=stripe_color, anchor="w",
            )
            title_lbl.pack(fill=tk.X)
            self.agent_title_labels[key] = title_lbl
            self.agent_labels[key].append(title_lbl)

            # Активный файл
            active_var = tk.StringVar(value="")
            self.agent_active_vars[key] = active_var
            active_lbl = tk.Label(
                content, textvariable=active_var,
                font=self.font_agent_active, bg=bg,
                fg=self._c("text_muted"), anchor="w",
            )
            active_lbl.pack(fill=tk.X)
            self.agent_labels[key].append(active_lbl)

            # Метрики P / E / Q
            p = tk.StringVar(value="0")
            e = tk.StringVar(value="0")
            q = tk.StringVar(value="0")
            self.agent_processed[key] = p
            self.agent_errors[key]    = e
            self.agent_queue[key]     = q

            metrics_line = tk.Frame(content, bg=bg)
            metrics_line.pack(fill=tk.X, anchor="w")
            self.agent_labels[key].append(metrics_line)

            metric_items = [("P:", p), ("E:", e)]
            if key != "A1":
                metric_items.append(("Q:", q))
            for ml, mv in metric_items:
                tk.Label(metrics_line, text=ml,
                         font=self.font_agent_metric_label, bg=bg,
                         fg=self._c("text_muted"),
                ).pack(side=tk.LEFT)
                tk.Label(metrics_line, textvariable=mv,
                         font=self.font_agent_metric_value, bg=bg,
                         fg=self._c("text_secondary"),
                ).pack(side=tk.LEFT, padx=(1, 6))

            tip_key = f"tip_agent_{key}"
            self._tip(card,       tip_key)
            self._tip(title_lbl,  tip_key)
            self._tip(active_lbl, tip_key)

    # ── Events ───────────────────────────────────────────────────────────────

    def _build_events_section(self, parent: tk.Frame) -> None:
        frame = tk.Frame(parent, bg=self._c("events_bg"),
                         highlightthickness=1,
                         highlightbackground=self._c("section_sep"))
        frame.pack(fill=tk.BOTH, expand=True, pady=(0, 6))

        header = tk.Label(
            frame, text=self.tr("events_prefix"),
            font=self.font_legend,
            bg=self._c("events_bg"), fg=self._c("text_muted"),
            anchor="w", padx=6, pady=2,
        )
        header.pack(fill=tk.X)
        self._tip(header, "tip_events")

        text_frame = tk.Frame(frame, bg=self._c("events_bg"))
        text_frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=(0, 4))

        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.events_text = tk.Text(
            text_frame,
            font=self.font_events,
            bg=self._c("events_bg"), fg=self._c("text_event_info"),
            insertbackground=self._c("entry_insert"),
            selectbackground=self._c("entry_select_bg"),
            yscrollcommand=scrollbar.set,
            relief=tk.FLAT, bd=0,
            state=tk.DISABLED,
            wrap=tk.WORD,
            cursor="arrow",
        )
        self.events_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.configure(command=self.events_text.yview)

        # Colour tags
        self.events_text.tag_configure("err",   foreground=self._c("text_event_error"))
        self.events_text.tag_configure("info",  foreground=self._c("text_event_info"))
        self.events_text.tag_configure("muted", foreground=self._c("text_muted"))

        self._tip(self.events_text, "tip_events")

    # ── Footer: checkboxes + status + log ────────────────────────────────────

    def _build_footer_section(self, parent: tk.Frame) -> None:
        # Тонкий разделитель над футером
        tk.Frame(parent, bg=self._c("section_sep"), height=1).pack(fill=tk.X)
        frame = tk.Frame(parent, bg=self._c("footer_bg"), padx=10, pady=6)
        frame.pack(fill=tk.X)
        self.footer_frame = frame

        # Row 0 – check-boxes
        checks_row = tk.Frame(frame, bg=self._c("footer_bg"))
        checks_row.pack(fill=tk.X, pady=(0, 4))

        ck_cfg = dict(
            font=self.font_legend,
            bg=self._c("footer_bg"),
            fg=self._c("text_secondary"),
            activebackground=self._c("footer_bg"),
            activeforeground=self._c("text_primary"),
            selectcolor=self._c("check_select_bg"),
            relief=tk.FLAT,
        )
        self.keep_sources_check = tk.Checkbutton(
            checks_row, text=self.tr("keep_sources"),
            variable=self.keep_sources_var, **ck_cfg)
        self.keep_sources_check.pack(side=tk.LEFT, padx=(0, 8))
        self._tip(self.keep_sources_check, "tip_keep_sources")

        self.deep_analysis_check = tk.Checkbutton(
            checks_row, text=self.tr("deep_analysis"),
            variable=self.deep_analysis_var, **ck_cfg)
        self.deep_analysis_check.pack(side=tk.LEFT, padx=(0, 8))
        self._tip(self.deep_analysis_check, "tip_deep_analysis")

        self.rename_output_check = tk.Checkbutton(
            checks_row, text=self.tr("rename_output"),
            variable=self.rename_output_var, **ck_cfg)
        self.rename_output_check.pack(side=tk.LEFT, padx=(0, 8))
        self._tip(self.rename_output_check, "tip_rename_output")

        self.shutdown_check = tk.Checkbutton(
            checks_row, text=self.tr("shutdown_after_done"),
            variable=self.shutdown_after_done_var, **ck_cfg)
        self.shutdown_check.pack(side=tk.RIGHT)
        self._tip(self.shutdown_check, "tip_shutdown")

        # Row 1 – language buttons + status
        bottom_row = tk.Frame(frame, bg=self._c("footer_bg"))
        bottom_row.pack(fill=tk.X)

        lang_frame = tk.Frame(bottom_row, bg=self._c("footer_bg"))
        lang_frame.pack(side=tk.LEFT)
        btn_cfg = dict(
            font=self.font_legend, width=3, relief=tk.FLAT,
            bg=self._c("btn_secondary_bg"), fg=self._c("btn_secondary_fg"),
            activebackground=self._c("btn_secondary_active_bg"),
            activeforeground=self._c("btn_secondary_active_fg"),
        )
        self.lang_ru_btn = tk.Button(lang_frame, text=self.tr("language_ru"),
                                     command=lambda: self._set_language("ru"), **btn_cfg)
        self.lang_ru_btn.pack(side=tk.LEFT, padx=(0, 3))
        self.lang_en_btn = tk.Button(lang_frame, text=self.tr("language_en"),
                                     command=lambda: self._set_language("en"), **btn_cfg)
        self.lang_en_btn.pack(side=tk.LEFT)

        # Status
        status_lbl = tk.Label(
            bottom_row, textvariable=self.status_var,
            font=self.font_legend,
            bg=self._c("footer_bg"), fg=self._c("text_status"),
            anchor="w",
        )
        status_lbl.pack(side=tk.LEFT, padx=(10, 0), fill=tk.X, expand=True)
        self._tip(status_lbl, "tip_status_bar")

        # Row 2 – log file path
        log_lbl = tk.Label(
            frame, textvariable=self.log_var,
            font=self.font_legend,
            bg=self._c("footer_bg"), fg=self._c("text_muted"),
            anchor="w", cursor="hand2",
        )
        log_lbl.pack(fill=tk.X, pady=(2, 0))
        log_lbl.bind("<Button-1>", self._open_log_dir)
        self._tip(log_lbl, "tip_log")

    # ── Settings tab ─────────────────────────────────────────────────────────

    def _build_settings_tab(self) -> None:
        self.settings_text_widgets = []

        # Top bar for Save button (always visible)
        top_bar = tk.Frame(self.settings_tab, bg=self._c("root_bg"), padx=10, pady=8)
        top_bar.pack(side=tk.TOP, fill=tk.X)

        save_btn = tk.Button(top_bar, text=self.tr("settings_save"),
                             font=self.font_main, command=self._save_settings_to_file,
                             bg=self._c("btn_start_bg"), fg=self._c("btn_start_fg"),
                             activebackground=self._c("btn_start_active_bg"),
                             activeforeground=self._c("btn_start_active_fg"))
        save_btn.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        self.settings_text_widgets.append((save_btn, "settings_save"))

        hint_restart = tk.Label(top_bar, text=self.tr("settings_restart_hint"),
                                font=self.font_legend,
                                bg=self._c("root_bg"), fg=self._c("text_muted"),
                                anchor="w", justify=tk.LEFT, wraplength=550)
        hint_restart.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.settings_text_widgets.append((hint_restart, "settings_restart_hint"))

        sep_top = tk.Frame(self.settings_tab, height=1, bg=self._c("text_muted"))
        sep_top.pack(side=tk.TOP, fill=tk.X)

        # Scrollable settings below
        canvas = tk.Canvas(self.settings_tab, bg=self._c("root_bg"), highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.settings_tab, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        content = tk.Frame(canvas, bg=self._c("root_bg"), padx=10, pady=8)
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")
        content.bind("<Configure>",
            lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
            lambda e: canvas.itemconfigure(window_id, width=e.width))

        row = 0

        def section(title_key: str) -> None:
            nonlocal row
            if row > 0:
                sep = tk.Frame(content, height=1, bg=self._c("text_muted"))
                sep.grid(row=row, column=0, columnspan=4, sticky="ew", pady=(10, 0))
                row += 1
            lbl = tk.Label(content, text=self.tr(title_key),
                           font=self.font_agent_title,
                           bg=self._c("root_bg"), fg=self._c("text_primary"), anchor="w")
            lbl.grid(row=row, column=0, columnspan=4, sticky="ew", pady=(4, 4))
            self.settings_text_widgets.append((lbl, title_key))
            row += 1

        def add_entry(label_key: str, var: tk.Variable, width: int = 18) -> None:
            nonlocal row
            lbl = tk.Label(content, text=self.tr(label_key),
                           font=self.font_legend,
                           bg=self._c("root_bg"), fg=self._c("text_secondary"), anchor="w")
            lbl.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=2)
            self.settings_text_widgets.append((lbl, label_key))
            ent = tk.Entry(content, textvariable=var, font=self.font_legend, width=width)
            ent.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=2)
            row += 1

        def add_pair(
            label_a: str, var_a: tk.Variable,
            label_b: Optional[str] = None, var_b: Optional[tk.Variable] = None,
            width: int = 10,
        ) -> None:
            nonlocal row
            lbl_a = tk.Label(content, text=self.tr(label_a), font=self.font_legend,
                             bg=self._c("root_bg"), fg=self._c("text_secondary"), anchor="w")
            lbl_a.grid(row=row, column=0, sticky="w", padx=(0, 6), pady=2)
            self.settings_text_widgets.append((lbl_a, label_a))
            tk.Entry(content, textvariable=var_a, font=self.font_legend, width=width).grid(
                row=row, column=1, sticky="ew", padx=(0, 16), pady=2)
            if label_b and var_b is not None:
                lbl_b = tk.Label(content, text=self.tr(label_b), font=self.font_legend,
                                 bg=self._c("root_bg"), fg=self._c("text_secondary"), anchor="w")
                lbl_b.grid(row=row, column=2, sticky="w", padx=(0, 6), pady=2)
                self.settings_text_widgets.append((lbl_b, label_b))
                tk.Entry(content, textvariable=var_b, font=self.font_legend, width=width).grid(
                    row=row, column=3, sticky="ew", pady=2)
            row += 1

        def add_path_entry(label_key: str, var: tk.StringVar,
                           browse_command: Callable[[], None], width: int = 28) -> None:
            nonlocal row
            lbl = tk.Label(content, text=self.tr(label_key), font=self.font_legend,
                           bg=self._c("root_bg"), fg=self._c("text_secondary"), anchor="w")
            lbl.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=2)
            self.settings_text_widgets.append((lbl, label_key))
            tk.Entry(content, textvariable=var, font=self.font_legend, width=width).grid(
                row=row, column=1, columnspan=2, sticky="ew", padx=(0, 8), pady=2)
            btn = tk.Button(content, text=self.tr("browse"), font=self.font_legend,
                            command=browse_command,
                            bg=self._c("btn_secondary_bg"), fg=self._c("btn_secondary_fg"),
                            activebackground=self._c("btn_secondary_active_bg"),
                            activeforeground=self._c("btn_secondary_active_fg"))
            btn.grid(row=row, column=3, sticky="ew", pady=2)
            self.settings_text_widgets.append((btn, "browse"))
            row += 1

        def add_bool_with_hint(label_key: str, hint_key: str, var: tk.BooleanVar) -> None:
            nonlocal row
            check = tk.Checkbutton(content, text=self.tr(label_key), variable=var,
                                   font=self.font_legend,
                                   bg=self._c("root_bg"), fg=self._c("text_primary"),
                                   activebackground=self._c("root_bg"),
                                   activeforeground=self._c("text_primary"),
                                   selectcolor=self._c("root_bg"), anchor="w")
            check.grid(row=row, column=0, columnspan=4, sticky="w", pady=(6, 0))
            self.settings_text_widgets.append((check, label_key))
            row += 1
            hint = tk.Label(content, text=self.tr(hint_key), font=self.font_legend,
                            bg=self._c("root_bg"), fg=self._c("text_muted"),
                            anchor="w", justify=tk.LEFT, wraplength=820)
            hint.grid(row=row, column=0, columnspan=4, sticky="w", padx=(22, 8), pady=(0, 3))
            self.settings_text_widgets.append((hint, hint_key))
            row += 1

        # ── Paths ───────────────────────────────────────────────────────
        section("settings_paths")
        add_path_entry("settings_temp_base", self.temp_base_var, self._browse_temp_base)

        # ── Workers ─────────────────────────────────────────────────────
        section("settings_workers")
        for la, na, lb, nb in [
            ("settings_unpack_workers",       "UNPACK_WORKERS",        "settings_detect_workers",  "DETECT_WORKERS"),
            ("settings_dedupe_workers",       "DEDUPE_WORKERS",        "settings_tag_workers",     "TAG_WORKERS"),
            ("settings_isbn_workers",         "ISBN_WORKERS",          "settings_lm_workers",      "LM_WORKERS"),
            ("settings_rename_workers",       "RENAME_WORKERS",        "settings_pack_workers",    "PACK_WORKERS"),
            ("settings_max_parallel_archives","MAX_PARALLEL_ARCHIVES", "settings_queue_size",      "QUEUE_SIZE"),
        ]:
            add_pair(la, self.worker_vars[na], lb, self.worker_vars[nb])
        add_entry("settings_target_hash_workers", self.worker_vars["TARGET_HASH_SCAN_WORKERS"], width=8)

        # ── A6 provider ─────────────────────────────────────────────────
        section("settings_lm_a6")
        add_entry("settings_lm_url",     self.lm_url_var,     width=34)
        add_entry("settings_lm_model",   self.lm_model_var,   width=22)
        add_entry("settings_lm_api_key", self.lm_api_key_var, width=34)

        # ── A7 provider ─────────────────────────────────────────────────
        section("settings_lm_a7")
        add_entry("settings_lm_url_rename",     self.lm_url_rename_var,     width=34)
        add_entry("settings_lm_model_rename",   self.lm_model_rename_var,   width=22)
        add_entry("settings_lm_api_key_rename", self.lm_api_key_rename_var, width=34)

        # ── LM parameters ───────────────────────────────────────────────
        section("settings_lm_params")
        lm = self.lm_number_vars
        add_pair("settings_lm_timeout",     lm["LM_TIMEOUT_SEC"],   "settings_lm_tokens",      lm["LM_MAX_OUTPUT_TOKENS"])
        add_pair("settings_lm_input_chars", lm["LM_INPUT_CHARS"],   "settings_lm_min_letters", lm["LM_MIN_SNIPPET_LETTERS"])
        add_entry("settings_isbn_provider", self.isbn_provider_var, width=16)

        # ── Deep analysis ───────────────────────────────────────────────
        section("settings_lm_deep_section")
        add_pair("settings_lm_deep_timeout", lm["LM_DEEP_TIMEOUT_SEC"], "settings_lm_deep_tokens", lm["LM_DEEP_MAX_OUTPUT_TOKENS"])
        add_entry("settings_lm_deep_input",  lm["LM_DEEP_INPUT_CHARS"], width=10)

        # ── Fast precheck ───────────────────────────────────────────────
        section("settings_lm_fast_section")
        add_pair("settings_lm_fast_input",  lm["LM_FAST_INPUT_CHARS"], "settings_lm_fast_tokens", lm["LM_FAST_MAX_OUTPUT_TOKENS"])
        add_entry("settings_lm_fast_confidence", self.lm_confidence_var, width=10)

        # ── Behaviour ───────────────────────────────────────────────────
        section("settings_behavior")
        add_bool_with_hint("keep_sources",         "keep_sources_hint",         self.keep_sources_var)
        add_bool_with_hint("settings_seed_hashes", "settings_seed_hashes_hint", self.boolean_setting_vars["SEED_HASHES_FROM_TARGET"])
        add_bool_with_hint("settings_isbn_lookup", "settings_isbn_lookup_hint", self.boolean_setting_vars["ISBN_LOOKUP"])

        # ── Analysis flags ──────────────────────────────────────────────
        section("settings_lm_flags")
        add_bool_with_hint("deep_analysis",               "deep_analysis_hint",               self.deep_analysis_var)
        add_bool_with_hint("settings_lm_fast_precheck",   "settings_lm_fast_precheck_hint",   self.boolean_setting_vars["LM_FAST_PRECHECK"])
        add_bool_with_hint("settings_lm_force_full",      "settings_lm_force_full_hint",      self.boolean_setting_vars["LM_FORCE_FULL_METADATA"])
        add_bool_with_hint("settings_lm_fill_author",     "settings_lm_fill_author_hint",     self.boolean_setting_vars["LM_FILL_UNKNOWN_AUTHOR"])
        add_bool_with_hint("settings_lm_without_snippet", "settings_lm_without_snippet_hint", self.boolean_setting_vars["LM_ALWAYS_TRY_WITHOUT_SNIPPET"])
        add_bool_with_hint("settings_lm_strict_json",     "settings_lm_strict_json_hint",     self.boolean_setting_vars["LM_STRICT_JSON_MODE"])

        # ── Output names ────────────────────────────────────────────────
        section("settings_output")
        lbl_lang = tk.Label(content, text=self.tr("settings_output_language"),
                            font=self.font_legend,
                            bg=self._c("root_bg"), fg=self._c("text_secondary"), anchor="w")
        lbl_lang.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=2)
        self.settings_text_widgets.append((lbl_lang, "settings_output_language"))
        ttk.Combobox(content, textvariable=self.output_language_var,
                     values=("auto", "ru", "en"), width=10, state="readonly",
                     font=self.font_legend).grid(row=row, column=1, sticky="w", pady=2)
        row += 1
        add_bool_with_hint("rename_output", "rename_output_hint", self.rename_output_var)

        # ── GUI ─────────────────────────────────────────────────────────
        section("settings_gui")
        add_entry("settings_gui_font_family", self.gui_font_family_var, width=18)
        for la, na, lb, nb in [
            ("settings_gui_width",            "GUI_WINDOW_WIDTH",            "settings_gui_font_main",        "GUI_FONT_MAIN_SIZE"),
            ("settings_gui_font_title",       "GUI_FONT_TITLE_SIZE",         "settings_gui_font_small",       "GUI_FONT_SMALL_SIZE"),
            ("settings_gui_font_stats",       "GUI_FONT_STATS_SIZE",         "settings_gui_font_counter",     "GUI_FONT_COUNTER_LABEL_SIZE"),
            ("settings_gui_font_legend",      "GUI_FONT_LEGEND_SIZE",        "settings_gui_font_agent_title", "GUI_FONT_AGENT_TITLE_SIZE"),
            ("settings_gui_font_agent_value", "GUI_FONT_AGENT_VALUE_SIZE",   "settings_gui_font_agent_metric","GUI_FONT_AGENT_METRIC_LABEL_SIZE"),
        ]:
            va = self.lm_number_vars.get(na) or self.gui_font_vars[na]
            vb = self.lm_number_vars.get(nb) or self.gui_font_vars[nb]
            add_pair(la, va, lb, vb)

        for col in range(4):
            content.grid_columnconfigure(col, weight=1 if col in {1, 3} else 0)

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _apply_initial_dirs(self) -> None:
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
            self._set_source_dirs([Path(picked)])
            self._set_status("status_source_updated")

    def _browse_target(self) -> None:
        initial = self._best_existing_dir(self.target_var.get())
        picked = filedialog.askdirectory(title=self.tr("dialog_target_title"), initialdir=initial)
        if picked:
            self.target_var.set(str(Path(picked)))
            self._persist_target_dir(Path(picked))
            self._set_status("status_target_updated")

    def _browse_temp_base(self) -> None:
        initial = self._best_existing_dir(self.temp_base_var.get() or self.target_var.get())
        picked = filedialog.askdirectory(title=self.tr("dialog_temp_title"), initialdir=initial)
        if picked:
            self.temp_base_var.set(str(Path(picked)))
            self._set_status("status_temp_updated")

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

    def _open_log_dir(self, _event=None) -> None:
        try:
            path = Path(str(self.current_log_path))
            folder = path.parent if path.is_file() else path
            if folder.exists() and sys.platform == "win32":
                os.startfile(str(folder))
        except Exception:
            pass

    def _on_source_drop(self, event) -> None:
        data = event.data or ""
        paths = self._parse_dnd_paths(data)
        dropped_dirs: list[Path] = []
        for raw in paths:
            p = Path(raw)
            if p.exists() and p.is_dir():
                dropped_dirs.append(p)
        if not dropped_dirs:
            messagebox.showwarning(self.tr("dialog_drop_title"), self.tr("dialog_drop_folder_only"))
            return
        if self.pipeline_running and self.sorter:
            for d in dropped_dirs:
                self.sorter.add_live_source_dir(d)
                self._append_source_dir(d)
            self._set_status("status_source_live_injected", count=len(dropped_dirs))
        else:
            self._set_source_dirs(dropped_dirs)
            self._set_status("status_source_drop_updated", count=len(dropped_dirs))

    def _append_source_dir(self, path: Path) -> None:
        existing = lp.parse_sources_input([self.source_var.get()])
        merged = lp.parse_sources_input([*([str(x) for x in existing]), str(path)])
        self._set_source_dirs(merged)

    def _set_source_dirs(self, paths: list[Path]) -> None:
        merged = lp.parse_sources_input([str(x) for x in paths])
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
            messagebox.showerror(self.tr("dialog_paths_title"), self.tr("dialog_source_not_found"))
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
            messagebox.showerror(self.tr("dialog_paths_title"),
                                 self.tr("dialog_path_create_error", error=exc))
            self._set_status("status_paths_error", error=exc)
            return False
        self._persist_target_dir(target)
        self._set_status("status_paths_checked", count=len(source_dirs))
        return True

    def _persist_target_dir(self, target: Path) -> None:
        setting_path = _config_dir() / "setting.py"
        if not setting_path.exists():
            return
        try:
            text = setting_path.read_text(encoding="utf-8")
        except Exception:
            return
        target_line = f"TARGET_DIR = {str(target)!r}"
        pattern = re.compile(r"^TARGET_DIR\s*=.*$", flags=re.MULTILINE)
        updated = pattern.sub(lambda _m: target_line, text, count=1) if pattern.search(text) \
                  else text.rstrip() + "\n" + target_line + "\n"
        if updated != text:
            try:
                setting_path.write_text(updated, encoding="utf-8")
            except Exception:
                pass

    def _resolve_temp_base(self, target_dir: Path) -> Path:
        raw = self.temp_base_var.get().strip()
        if raw:
            return Path(raw)
        return target_dir / "_TempPipeline"

    # ── Settings accessors ───────────────────────────────────────────────────

    def _setting_int(self, name: str, default: int, min_value: int = 1) -> int:
        if setting is None:
            return max(min_value, int(default))
        try:
            return max(min_value, int(getattr(setting, name, default)))
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
            return max(min_value, float(getattr(setting, name, default)))
        except Exception:
            return max(min_value, float(default))

    def _entry_int(self, name: str, default: int, min_value: int = 1) -> int:
        var = (self.worker_vars.get(name)
               or self.lm_number_vars.get(name)
               or self.gui_font_vars.get(name))
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
        setting_path = _config_dir() / "setting.py"
        if not setting_path.exists():
            messagebox.showerror(self.tr("tab_settings"),
                                 self.tr("settings_save_error", error="setting.py not found"))
            return
        try:
            source_dirs = [str(p) for p in lp.parse_sources_input([self.source_var.get().strip()])]
            target_dir  = Path(self.target_var.get().strip() or str(lp.DEFAULT_TARGET_DIR))
            values: dict[str, object] = {
                "TARGET_DIR":    self.target_var.get().strip(),
                "TEMP_BASE":     str(self._resolve_temp_base(target_dir)),
                "LM_URL":        self.lm_url_var.get().strip() or lp.DEFAULT_LM_URL,
                "LM_MODEL":      self.lm_model_var.get().strip() or lp.DEFAULT_LM_MODEL,
                "LM_API_KEY":    self.lm_api_key_var.get().strip(),
                "LM_URL_RENAME": self.lm_url_rename_var.get().strip(),
                "LM_MODEL_RENAME":      self.lm_model_rename_var.get().strip(),
                "LM_API_KEY_RENAME":    self.lm_api_key_rename_var.get().strip(),
                "ISBN_PROVIDER":         self.isbn_provider_var.get().strip().lower() or "auto",
                "MAX_PARALLEL_ARCHIVES": self._entry_int("MAX_PARALLEL_ARCHIVES", lp.DEFAULT_MAX_PARALLEL_ARCHIVES),
                "QUEUE_SIZE":            self._entry_int("QUEUE_SIZE", lp.DEFAULT_QUEUE_SIZE),
                "LM_TIMEOUT_SEC":        self._entry_int("LM_TIMEOUT_SEC", lp.DEFAULT_LM_TIMEOUT_SEC, min_value=10),
                "LM_INPUT_CHARS":        self._entry_int("LM_INPUT_CHARS", lp.DEFAULT_LM_INPUT_CHARS, min_value=200),
                "LM_MAX_OUTPUT_TOKENS":  self._entry_int("LM_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_MAX_OUTPUT_TOKENS, min_value=40),
                "LM_DEEP_TIMEOUT_SEC":   self._entry_int("LM_DEEP_TIMEOUT_SEC", 120, min_value=10),
                "LM_DEEP_INPUT_CHARS":   self._entry_int("LM_DEEP_INPUT_CHARS", 16000, min_value=200),
                "LM_DEEP_MAX_OUTPUT_TOKENS":  self._entry_int("LM_DEEP_MAX_OUTPUT_TOKENS", 1024, min_value=40),
                "LM_FAST_INPUT_CHARS":        self._entry_int("LM_FAST_INPUT_CHARS", lp.DEFAULT_LM_FAST_INPUT_CHARS, min_value=200),
                "LM_FAST_MAX_OUTPUT_TOKENS":  self._entry_int("LM_FAST_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_FAST_MAX_OUTPUT_TOKENS, min_value=40),
                "LM_FAST_CONFIDENCE_MIN":     self._entry_float(self.lm_confidence_var, lp.DEFAULT_LM_FAST_CONFIDENCE_MIN),
                "LM_MIN_SNIPPET_LETTERS":     self._entry_int("LM_MIN_SNIPPET_LETTERS", lp.DEFAULT_LM_MIN_SNIPPET_LETTERS),
                "TRANSLATE_OUTPUT_NAMES": bool(self.rename_output_var.get()),
                "KEEP_SOURCES":           bool(self.keep_sources_var.get()),
                "LM_ITERATIVE_READ":      bool(self.deep_analysis_var.get()),
                "OUTPUT_LANGUAGE":        self.output_language_var.get().strip().lower() or "auto",
                "GUI_DEFAULT_LANGUAGE":   self.language,
                "GUI_WINDOW_WIDTH":       self._entry_int("GUI_WINDOW_WIDTH", self.window_width, min_value=WINDOW_MIN_WIDTH),
                "GUI_FONT_FAMILY":        self.gui_font_family_var.get().strip() or "Segoe UI",
                "UNPACK_WORKERS":   self._entry_int("UNPACK_WORKERS",   lp.DEFAULT_UNPACK_WORKERS),
                "DETECT_WORKERS":   self._entry_int("DETECT_WORKERS",   lp.DEFAULT_DETECT_WORKERS),
                "DEDUPE_WORKERS":   self._entry_int("DEDUPE_WORKERS",   lp.DEFAULT_DEDUPE_WORKERS),
                "TAG_WORKERS":      self._entry_int("TAG_WORKERS",      lp.DEFAULT_TAG_WORKERS),
                "ISBN_WORKERS":     self._entry_int("ISBN_WORKERS",     lp.DEFAULT_ISBN_WORKERS),
                "LM_WORKERS":       self._entry_int("LM_WORKERS",       lp.DEFAULT_LM_WORKERS),
                "RENAME_WORKERS":   self._entry_int("RENAME_WORKERS",   lp.DEFAULT_RENAME_WORKERS),
                "PACK_WORKERS":     self._entry_int("PACK_WORKERS",     lp.DEFAULT_PACK_WORKERS),
                "TARGET_HASH_SCAN_WORKERS": self._entry_int("TARGET_HASH_SCAN_WORKERS", lp.DEFAULT_TARGET_HASH_SCAN_WORKERS),
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
            messagebox.showerror(self.tr("tab_settings"),
                                 self.tr("settings_save_error", error=exc))

    def _c(self, key: str, fallback: str = "#000000") -> str:
        return str(self.palette.get(key, fallback))

    # ── Config builder (no change) ───────────────────────────────────────────

    def _build_config(self) -> lp.Config:
        source_dirs = lp.parse_sources_input([self.source_var.get().strip()])
        target      = Path(self.target_var.get().strip())
        dupes       = target / "Duplicates"
        nobook      = target / "NoBook"
        error       = Path(lp.DEFAULT_ERROR_DIR)
        temp_base   = self._resolve_temp_base(target)

        lm_url          = self.lm_url_var.get().strip() or lp.DEFAULT_LM_URL
        lm_model        = self.lm_model_var.get().strip() or lp.DEFAULT_LM_MODEL
        lm_api_key      = self.lm_api_key_var.get().strip()
        lm_url_rename   = self.lm_url_rename_var.get().strip()
        lm_model_rename = self.lm_model_rename_var.get().strip()
        lm_api_key_rename = self.lm_api_key_rename_var.get().strip()

        deep_analysis       = bool(self.deep_analysis_var.get())
        lm_timeout_sec      = self._entry_int("LM_TIMEOUT_SEC",      lp.DEFAULT_LM_TIMEOUT_SEC,      min_value=10)
        lm_input_chars      = self._entry_int("LM_INPUT_CHARS",      lp.DEFAULT_LM_INPUT_CHARS,      min_value=200)
        lm_max_output_tokens = self._entry_int("LM_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_MAX_OUTPUT_TOKENS, min_value=40)
        if deep_analysis:
            lm_timeout_sec      = max(lm_timeout_sec,       self._entry_int("LM_DEEP_TIMEOUT_SEC",       120,  min_value=10))
            lm_input_chars      = max(lm_input_chars,       self._entry_int("LM_DEEP_INPUT_CHARS",       16000, min_value=200))
            lm_max_output_tokens = max(lm_max_output_tokens, self._entry_int("LM_DEEP_MAX_OUTPUT_TOKENS", 1024, min_value=40))

        return lp.Config(
            source_dirs=source_dirs, target_dir=target, dupes_dir=dupes,
            nobook_dir=nobook, error_dir=error, temp_base=temp_base,
            lm_url=lm_url, lm_model=lm_model, lm_api_key=lm_api_key,
            lm_url_rename=lm_url_rename, lm_model_rename=lm_model_rename,
            lm_api_key_rename=lm_api_key_rename,
            queue_size=self._entry_int("QUEUE_SIZE",   lp.DEFAULT_QUEUE_SIZE),
            unpack_workers=self._entry_int("UNPACK_WORKERS",  lp.DEFAULT_UNPACK_WORKERS),
            detect_workers=self._entry_int("DETECT_WORKERS",  lp.DEFAULT_DETECT_WORKERS),
            dedupe_workers=self._entry_int("DEDUPE_WORKERS",  lp.DEFAULT_DEDUPE_WORKERS),
            tag_workers=self._entry_int("TAG_WORKERS",        lp.DEFAULT_TAG_WORKERS),
            isbn_workers=self._entry_int("ISBN_WORKERS",      lp.DEFAULT_ISBN_WORKERS),
            lm_workers=self._entry_int("LM_WORKERS",          lp.DEFAULT_LM_WORKERS),
            rename_workers=self._entry_int("RENAME_WORKERS",  lp.DEFAULT_RENAME_WORKERS),
            pack_workers=self._entry_int("PACK_WORKERS",      lp.DEFAULT_PACK_WORKERS),
            max_parallel_archives=self._entry_int("MAX_PARALLEL_ARCHIVES", lp.DEFAULT_MAX_PARALLEL_ARCHIVES),
            delete_source_after_pack=not self.keep_sources_var.get(),
            keep_temp_nobooks=False,
            lm_timeout_sec=lm_timeout_sec,
            lm_input_chars=lm_input_chars,
            lm_max_output_tokens=lm_max_output_tokens,
            lm_fast_precheck=False if deep_analysis else bool(self.boolean_setting_vars["LM_FAST_PRECHECK"].get()),
            lm_fast_input_chars=self._entry_int("LM_FAST_INPUT_CHARS",      lp.DEFAULT_LM_FAST_INPUT_CHARS),
            lm_fast_max_output_tokens=self._entry_int("LM_FAST_MAX_OUTPUT_TOKENS", lp.DEFAULT_LM_FAST_MAX_OUTPUT_TOKENS),
            lm_fast_confidence_min=self._entry_float(self.lm_confidence_var, lp.DEFAULT_LM_FAST_CONFIDENCE_MIN),
            lm_force_full_metadata=deep_analysis or bool(self.boolean_setting_vars["LM_FORCE_FULL_METADATA"].get()),
            lm_fill_unknown_author=deep_analysis or bool(self.boolean_setting_vars["LM_FILL_UNKNOWN_AUTHOR"].get()),
            lm_iterative_read=deep_analysis,
            lm_always_try_without_snippet=bool(self.boolean_setting_vars["LM_ALWAYS_TRY_WITHOUT_SNIPPET"].get()),
            lm_strict_json_mode=bool(self.boolean_setting_vars["LM_STRICT_JSON_MODE"].get()),
            lm_min_snippet_letters=self._entry_int("LM_MIN_SNIPPET_LETTERS", lp.DEFAULT_LM_MIN_SNIPPET_LETTERS),
            seed_hashes_from_target=bool(self.boolean_setting_vars["SEED_HASHES_FROM_TARGET"].get()),
            target_hash_scan_workers=self._entry_int("TARGET_HASH_SCAN_WORKERS", lp.DEFAULT_TARGET_HASH_SCAN_WORKERS),
            isbn_lookup=bool(self.boolean_setting_vars["ISBN_LOOKUP"].get()),
            isbn_provider=self.isbn_provider_var.get().strip().lower() or "auto",
            translate_output_names=bool(self.rename_output_var.get()),
            output_language=self._selected_output_language(),
            ephemeral_mode=True,
        )

    # ── Pipeline control ─────────────────────────────────────────────────────

    def _start_pipeline(self) -> None:
        if self.pipeline_running:
            self._set_status("status_already_running")
            return
        if not self._check_and_create_paths():
            return
        self.pipeline_error  = ""
        self.pipeline_exit_code = None
        try:
            config = self._build_config()
            sorter = lp.LibrarySorter(config)
            sorter.ui       = _NoopController()
            sorter.keyboard = _NoopController()
        except Exception as exc:
            messagebox.showerror(self.tr("start"), self.tr("status_start_error", error=exc))
            self._set_status("status_start_error", error=exc)
            return
        self.sorter = sorter
        for key in lp.AGENT_KEYS:
            self._rebuild_agent_indicator_segments(key)
            if key in self.agent_active_vars:
                self.agent_active_vars[key].set("")
        self.pipeline_running = True
        self.shutdown_started = False
        self.stop_requested_by_user = False
        self.current_mode = "RUNNING"
        self.mode_var.set(self._mode_label("RUNNING"))
        self._eta_display_value = "--:--"
        self._eta_display_updated_at = 0.0
        self.time_var.set("")
        self._set_status("status_started")
        self.current_log_path = sorter.log_file
        self.log_var.set(self._log_text(self.current_log_path))
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self._update_drop_zone_state()
        self.pipeline_thread = threading.Thread(
            target=self._pipeline_runner_thread,
            name="GUI-PipelineRunner", daemon=True,
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
                        self.sorter.queue_sizes(), self.sorter.stage_flags())
                    self._render_snapshot(snap)
                except Exception:
                    pass
            self.pipeline_running = False
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self._update_drop_zone_state()
            exit_code    = self.pipeline_exit_code if self.pipeline_exit_code is not None else 2
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
            self.current_mode = "STOPPED" if (exit_code == 0 and stopped_by_user) \
                                else "END" if exit_code == 0 else "END_ERROR"
            self.mode_var.set(self._mode_label(self.current_mode))
            if exit_code == 0 and not stopped_by_user and self.shutdown_after_done_var.get():
                self._shutdown_computer()
        elif self.sorter and self.pipeline_running:
            try:
                snap = self.sorter.metrics.snapshot(
                    self.sorter.queue_sizes(), self.sorter.stage_flags())
                self._render_snapshot(snap)
            except Exception:
                pass
        self.root.after(300, self._poll_pipeline)

    # ── Rendering ────────────────────────────────────────────────────────────

    def _render_snapshot(self, snap: dict) -> None:
        self.last_snapshot = snap
        pct = float(snap.get("pct", 0.0))
        self.progress["value"] = max(0.0, min(100.0, pct))
        self.progress_text_var.set(f"{int(round(pct))} %")
        self.current_mode = fix_mojibake(str(snap.get("mode", "RUNNING")))
        self.mode_var.set(self._mode_label(self.current_mode))
        self.time_var.set(self._time_label(snap))
        self._refresh_status_line(snap)

        seen = int(snap.get("seen", 0))
        done = int(snap.get("done", 0))
        book_results = snap.get("book_results", {}) or {}
        self.seen_var.set(self._fmt_num(seen))
        self.done_var.set(self._fmt_num(done))
        self.packed_var.set(self._fmt_num(int(book_results.get("packed", 0))))
        self.dupes_var.set(self._fmt_num(
            int(book_results.get("duplicate", 0)) + int(book_results.get("duplicate_temp", 0))
        ))
        self.nobook_var.set(self._fmt_num(int(snap.get("nobook_files", 0))))
        self.failed_var.set(self._fmt_num(int(book_results.get("failed", 0))))

        stage_processed  = snap.get("stage_processed",  {}) or {}
        stage_errors     = snap.get("stage_errors",     {}) or {}
        queue_sizes      = snap.get("queue_sizes",      {}) or {}
        active_stage_slots = snap.get("active_stage_slots", {}) or {}
        active_stage_items = snap.get("active_stage_items", {}) or {}
        for key in lp.AGENT_KEYS:
            self.agent_processed[key].set(self._fmt_num(int(stage_processed.get(key, 0))))
            self.agent_errors[key].set(   self._fmt_num(int(stage_errors.get(key, 0))))
            self.agent_queue[key].set(    self._fmt_num(int(queue_sizes.get(key, 0))))
            # Active item for card display
            raw_item = fix_mojibake(str(active_stage_items.get(key, "")).strip())
            if raw_item:
                if len(raw_item) > 28:
                    raw_item = "…" + raw_item[-26:]
                if key in self.agent_active_vars:
                    self.agent_active_vars[key].set(raw_item)
            self._update_agent_visual_state(key, active_stage_slots.get(key, []))

        self._render_events(snap)

    def _fmt_num(self, value: int) -> str:
        return f"{value:,}".replace(",", " ")   # narrow no-break space

    def _mode_label(self, mode: str) -> str:
        fixed = fix_mojibake(str(mode or ""))
        if not fixed:
            return self.tr("mode_IDLE")
        key  = f"mode_{fixed}"
        text = self.tr(key)
        return fixed if text == key else text

    def _time_label(self, snap: dict) -> str:
        eta  = fix_mojibake(str(snap.get("eta", "--:--")))
        now  = time.monotonic()
        mode = fix_mojibake(str(snap.get("mode", "")))
        if (self._eta_display_updated_at <= 0
                or now - self._eta_display_updated_at >= 10
                or mode not in {"RUNNING", "INIT"}):
            self._eta_display_value    = eta
            self._eta_display_updated_at = now
        eta = self._eta_display_value
        gb_done    = float(snap.get("gb_done",    0.0))
        gb_seen    = float(snap.get("gb_seen",    0.0))
        gb_per_min = float(snap.get("gb_per_min", 0.0))
        if gb_seen > 0:
            speed = f"  {gb_per_min:.2f} ГБ/мин" if gb_per_min > 0 else ""
            eta_short = self._time_without_seconds(eta)
            return f"{gb_done:.2f} / {gb_seen:.2f} ГБ{speed}  ~{eta_short}"
        return ""

    def _time_without_seconds(self, value: str) -> str:
        parts = str(value or "").split(":")
        if len(parts) >= 3:
            return ":".join(parts[:2])
        return str(value or "--:--")

    def _agent_title(self, key: str) -> str:
        return f"{key} {self.tr(f'agent_{key}')}"

    def _agent_indicator_color(self, key: str, active: bool) -> str:
        if active:
            return self._c("indicator_active")
        return self._c("indicator_idle")

    def _agent_worker_total(self, key: str) -> int:
        if key == "A1":
            return 1
        worker_map = {
            "A2":  ("unpack_workers",  "UNPACK_WORKERS",  lp.DEFAULT_UNPACK_WORKERS),
            "A3":  ("detect_workers",  "DETECT_WORKERS",  lp.DEFAULT_DETECT_WORKERS),
            "A4":  ("dedupe_workers",  "DEDUPE_WORKERS",  lp.DEFAULT_DEDUPE_WORKERS),
            "A5":  ("tag_workers",     "TAG_WORKERS",     lp.DEFAULT_TAG_WORKERS),
            "A5b": ("isbn_workers",    "ISBN_WORKERS",    lp.DEFAULT_ISBN_WORKERS),
            "A6":  ("lm_workers",      "LM_WORKERS",      lp.DEFAULT_LM_WORKERS),
            "A7":  ("rename_workers",  "RENAME_WORKERS",  lp.DEFAULT_RENAME_WORKERS),
            "A8":  ("pack_workers",    "PACK_WORKERS",    lp.DEFAULT_PACK_WORKERS),
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
        total          = self._agent_worker_total(key)
        previous_total = self.agent_indicator_totals.get(key, 0)
        if previous_total == total and self.agent_indicator_segments.get(key):
            return
        for child in bar.winfo_children():
            child.destroy()
        for r in range(max(previous_total, total)):
            bar.grid_rowconfigure(r, weight=0, uniform="")
        bar.grid_columnconfigure(0, weight=1)
        gap = 1 if total <= 12 else 0
        segments: list[tk.Frame] = []
        for r in range(total):
            bar.grid_rowconfigure(r, weight=1, uniform=f"{key}_seg")
            seg = tk.Frame(bar, bg=self._agent_indicator_color(key, False), height=1)
            seg.grid(row=r, column=0, sticky="nsew", pady=(gap, 0))
            slot_name = f"W{r + 1}"

            def _on_enter(event, k=key, s=slot_name):
                per_slot = (self.last_snapshot or {}).get("active_stage_per_slot", {})
                name = fix_mojibake(str(per_slot.get(k, {}).get(s, "")).strip())
                self._tooltip.show(name, event.x_root, event.y_root)
            seg.bind("<Enter>", _on_enter)
            seg.bind("<Leave>", lambda _e: self._tooltip.hide())
            segments.append(seg)
        self.agent_indicator_segments[key] = segments
        self.agent_indicator_totals[key]   = total

    def _update_agent_visual_state(self, key: str, active_slots: object) -> None:
        try:
            title = self.agent_title_labels.get(key)
            if title:
                title.configure(text=self._agent_title(key))
            self._rebuild_agent_indicator_segments(key)
            if isinstance(active_slots, (list, tuple, set)):
                active_set = {fix_mojibake(str(s)) for s in active_slots}
            else:
                active_set = set()
            for idx, seg in enumerate(self.agent_indicator_segments.get(key, []), start=1):
                seg.configure(bg=self._agent_indicator_color(key, f"W{idx}" in active_set))
        except Exception:
            pass

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
        if self.events_text is None:
            return
        events = (snap or {}).get("events", []) or []
        try:
            self.events_text.configure(state=tk.NORMAL)
            self.events_text.delete("1.0", tk.END)
            for ev in events[:12]:
                line = fix_mojibake(str(ev)).strip()
                if not line:
                    continue
                lo = line.lower()
                tag = "err" if ("ошибк" in lo or "error" in lo or "failed" in lo) else "info"
                self.events_text.insert(tk.END, line + "\n", tag)
            self.events_text.configure(state=tk.DISABLED)
        except Exception:
            pass

    def _shutdown_computer(self) -> None:
        if self.shutdown_started:
            return
        self.shutdown_started = True
        self._set_status("status_shutdown_started")
        try:
            subprocess.Popen(["shutdown", "/s", "/f", "/t", "300"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as exc:
            self.shutdown_started = False
            self._set_status("status_shutdown_failed", error=exc)
            messagebox.showerror(self.tr("dialog_shutdown_title"),
                                 self.tr("dialog_shutdown_failed", error=exc))

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


# ── Noop controller (replaces terminal UI / keyboard watcher) ─────────────────

class _NoopController:
    def start(self) -> None: pass
    def stop(self)  -> None: pass


# ── Entry point ───────────────────────────────────────────────────────────────

def create_root() -> tuple[tk.Tk, bool]:
    if TkinterDnD is not None:
        return TkinterDnD.Tk(), True
    return tk.Tk(), False


if __name__ == "__main__":
    root, dnd_ok = create_root()
    LibraryGUIApp(root, dnd_ok)
    root.mainloop()
