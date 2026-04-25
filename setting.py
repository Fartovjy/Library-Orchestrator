#!/usr/bin/env python3
"""
Центральные пути для конвейера сортировки библиотеки.
Редактируйте только этот файл, чтобы менять рабочие каталоги.
"""

SOURCE_DIRS = [
    r"E:\Энциклопедии. Словари. Справочники",
]
TARGET_DIR = 'E:\\Sorted_Library'
DUPES_DIR = r"E:\Sorted_Library\Duplicates"
NOBOOK_DIR = r"E:\Sorted_Library\NoBook"

# Необязательно.
# Если не задавать TEMP_BASE, конвейер автоматически использует:
#   <TARGET_DIR>\_TempPipeline
# TEMP_BASE = r"E:\Sorted_Library\_TempPipeline"

# Количество агентов/воркеров по стадиям:
# A2 Распаковка, A3 Книга?, A4 XXH64, A5 Теги, A6 LM Studio, A7 Переименование, A8 Упаковка
UNPACK_WORKERS = 3
DETECT_WORKERS = 2
DEDUPE_WORKERS = 3
TAG_WORKERS = 3
LM_WORKERS = 3
RENAME_WORKERS = 1
PACK_WORKERS = 9

# Дополнительно:
MAX_PARALLEL_ARCHIVES = 3
QUEUE_SIZE = 33

# LM Studio quality tuning:
# Increase these if you want higher recall from LM on difficult files.
LM_TIMEOUT_SEC = 90
LM_INPUT_CHARS = 4800
LM_MAX_OUTPUT_TOKENS = 1024

# V3: fast LM precheck before heavy full request.
# Safe logic: fast answer is accepted only if it is full and confident,
# otherwise the pipeline falls back to the current heavy LM request.
LM_FAST_PRECHECK = True
LM_FAST_INPUT_CHARS = 900
LM_FAST_MAX_OUTPUT_TOKENS = 180
LM_FAST_CONFIDENCE_MIN = 4.0

# True: route every book through the full-metadata LM branch.
# In V3, a fast LM precheck may satisfy this branch early;
# otherwise the pipeline falls back to the heavy full request.
LM_FORCE_FULL_METADATA = True

# True: call LM even when snippet text is weak/empty (uses filename/path context).
LM_ALWAYS_TRY_WITHOUT_SNIPPET = True
# True: ask LM Studio for strict JSON output (auto-fallback if unsupported).
LM_STRICT_JSON_MODE = True
# Minimum letters in snippet to consider it meaningful text.
LM_MIN_SNIPPET_LETTERS = 24

# GUI settings:
# Window is fixed (min=max) and clamped in code to keep compact dashboard layout.
GUI_WINDOW_WIDTH = 960
GUI_WINDOW_HEIGHT = 530
GUI_DEFAULT_LANGUAGE = "ru"

# Font settings for GUI:
GUI_FONT_FAMILY = "Segoe UI"
GUI_FONT_MAIN_SIZE = 12
GUI_FONT_TITLE_SIZE = 15
GUI_FONT_SMALL_SIZE = 12
GUI_FONT_STATS_SIZE = 11
GUI_FONT_COUNTER_LABEL_SIZE = 9
GUI_FONT_LEGEND_SIZE = 9

# Agent cards (A1..A8)
GUI_FONT_AGENT_TITLE_SIZE = 11
GUI_FONT_AGENT_VALUE_SIZE = 10
GUI_FONT_AGENT_METRIC_LABEL_SIZE = 9
