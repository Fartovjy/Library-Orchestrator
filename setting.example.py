#!/usr/bin/env python3
"""
Центральные пути для конвейера сортировки библиотеки.
Скопируйте этот файл в `setting.py` и подставьте свои значения.
Реальный setting.py добавлен в .gitignore, чтобы ключи не утекли в git.
"""

SOURCE_DIRS = [
    r'D:\Library\Source',
]
TARGET_DIR = r'D:\Library\Sorted'
DUPES_DIR = r"D:\Library\Sorted\Duplicates"
NOBOOK_DIR = r"D:\Library\Sorted\NoBook"
ERROR_DIR = r"D:\Library\Sorted\Error"

# LM-провайдер (OpenAI-compatible API).
#
# --- Ollama (локально, бесплатно) ---
# LM_URL = 'http://127.0.0.1:11434/v1/chat/completions'
# LM_MODEL = 'gemma4:e4b'
# LM_API_KEY = ''   # Ollama — ключ не нужен
#
# --- OpenRouter (облако, есть бесплатные модели) ---
# Зарегистрируйтесь на https://openrouter.ai, получите ключ в Settings → Keys.
# Бесплатные модели (суффикс :free, лимит ~20 запр/мин на модель):
#   meta-llama/llama-4-scout:free   — multimodal, понимает русский
#   google/gemini-2.0-flash-exp:free — быстрый, хорошее качество
#   deepseek/deepseek-r1:free       — умный, но медленный (reasoning)
# Платные модели без :free — без жёстких лимитов.
# При использовании бесплатных моделей снизьте LM_WORKERS до 1-2.
LM_URL = 'https://openrouter.ai/api/v1/chat/completions'
LM_MODEL = 'mistralai/mistral-nemo'
LM_API_KEY = 'sk-or-v1-PASTE_YOUR_OPENROUTER_KEY_HERE'

# --- Отдельный провайдер для А7 (переименование) ---
# Если не задано (пусто) — А7 использует те же LM_URL / LM_MODEL / LM_API_KEY что и А6.
# Смысл: А6 (жанр/метаданные) → большая облачная модель (OpenRouter),
#         А7 (перевод имён) → быстрая локальная модель (Ollama).
# LM_URL_RENAME = 'http://127.0.0.1:11434/v1/chat/completions'
# LM_MODEL_RENAME = 'gemma4:e4b'
# LM_API_KEY_RENAME = ''
LM_URL_RENAME = ''
LM_MODEL_RENAME = ''
LM_API_KEY_RENAME = ''

# Необязательно. Если не задавать TEMP_BASE — используется <TARGET_DIR>\_TempPipeline.
# TEMP_BASE = r"D:\Library\_TempPipeline"

# Количество агентов/воркеров по стадиям:
# A2 Распаковка, A3 Книга?, A4 XXH64, A5 Теги, A6 Ollama, A7 Переименование, A8 Упаковка
UNPACK_WORKERS = 3
DETECT_WORKERS = 2
DEDUPE_WORKERS = 3
TAG_WORKERS = 3
LM_WORKERS = 1
RENAME_WORKERS = 3
PACK_WORKERS = 6

# Дополнительно:
MAX_PARALLEL_ARCHIVES = 3
QUEUE_SIZE = 33

# LM quality tuning:
LM_TIMEOUT_SEC = 20
LM_INPUT_CHARS = 1200
LM_MAX_OUTPUT_TOKENS = 120

# GUI "Глубокий анализ" uses the larger context window.
LM_DEEP_INPUT_CHARS = 16000
LM_DEEP_TIMEOUT_SEC = 120
LM_DEEP_MAX_OUTPUT_TOKENS = 1024

# V3: fast LM precheck before heavy full request.
LM_FAST_PRECHECK = True
LM_FAST_INPUT_CHARS = 900
LM_FAST_MAX_OUTPUT_TOKENS = 180
LM_FAST_CONFIDENCE_MIN = 4.0

LM_FORCE_FULL_METADATA = False
LM_FILL_UNKNOWN_AUTHOR = True
LM_ALWAYS_TRY_WITHOUT_SNIPPET = True
LM_STRICT_JSON_MODE = True
LM_MIN_SNIPPET_LETTERS = 24
ISBN_LOOKUP = True
ISBN_PROVIDER = 'auto'
TRANSLATE_OUTPUT_NAMES = True
KEEP_SOURCES = False
OUTPUT_LANGUAGE = 'ru'

# GUI:
GUI_WINDOW_WIDTH = 960
GUI_WINDOW_HEIGHT = 530
GUI_DEFAULT_LANGUAGE = 'ru'

GUI_FONT_FAMILY = 'Segoe UI'
GUI_FONT_MAIN_SIZE = 12
GUI_FONT_TITLE_SIZE = 15
GUI_FONT_SMALL_SIZE = 12
GUI_FONT_STATS_SIZE = 11
GUI_FONT_COUNTER_LABEL_SIZE = 9
GUI_FONT_LEGEND_SIZE = 9
GUI_FONT_AGENT_TITLE_SIZE = 11
GUI_FONT_AGENT_VALUE_SIZE = 10
GUI_FONT_AGENT_METRIC_LABEL_SIZE = 9

TARGET_HASH_SCAN_WORKERS = 3
SEED_HASHES_FROM_TARGET = True
