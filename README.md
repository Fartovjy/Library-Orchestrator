# LibSort

## Русский

LibSort - одноразовый конвейер для сортировки большой книжной папки, где книги могут лежать как отдельными файлами, так и внутри обычных или вложенных архивов.

Главная цель приложения: найти именно книги, отсечь мусор и технические файлы, определить метаданные, удалить дубликаты по XXH64 и сложить результат в структуру:

```text
TARGET_DIR\Жанр\Первая буква автора\Автор\Книга.zip
```

Каждая книга упаковывается в отдельный ZIP с максимальным сжатием через 7-Zip.

## Быстрый Запуск

1. Убедитесь, что установлены `7z.exe` и Python.
2. Установите зависимости:

```powershell
pip install -r requirements.txt
```

3. Запустите GUI:

```powershell
python .\library_gui.py
```

4. В GUI выберите один или несколько `SOURCE_DIRS`, проверьте `TARGET_DIR`, нажмите `Старт`.

## Настройки

Основные настройки находятся в [setting.py](setting.py).

Ключевые параметры:

```python
SOURCE_DIRS = [
    r"E:\Энциклопедии. Словари. Справочники",
]
TARGET_DIR = r"E:\Sorted_Library"
DUPES_DIR = r"E:\Sorted_Library\Duplicates"
NOBOOK_DIR = r"E:\Sorted_Library\NoBook"
```

`TEMP_BASE` можно не задавать. Если он отсутствует, приложение использует:

```text
<TARGET_DIR>\_TempPipeline
```

Это сделано специально: временная упаковка на том же диске, что и `TARGET_DIR`, обычно быстрее и безопаснее для атомарной замены итоговых архивов.

Количество воркеров тоже задается в [setting.py](setting.py):

```python
UNPACK_WORKERS = 6
DETECT_WORKERS = 2
DEDUPE_WORKERS = 3
TAG_WORKERS = 3
LM_WORKERS = 1
RENAME_WORKERS = 1
PACK_WORKERS = 6
```

## Логика Конвейера

Перед основным запуском работает служебный агент:

```text
A0 DB Sync
```

Он сверяет постоянную БД с уже существующими ZIP-файлами в `TARGET_DIR`. Для ZIP в целевой папке считается XXH64 не самого ZIP-архива, а полезной нагрузки внутри ZIP, то есть файла книги.

Основные агенты:

```text
A1 Поиск
A2 Распаковка
A3 Книга?
A4 XXH64
A5 Теги
A6 LM Studio
A7 Переименование
A8 Упаковка
```

Порядок работы:

1. `A1` ищет файлы в выбранных `SOURCE_DIRS`.
2. `A2` распаковывает архивы, включая вложенные архивы.
3. `A3` решает, книга это или нет.
4. `A4` считает XXH64 и отсекает дубликаты до дорогих стадий.
5. `A5` читает теги и метаданные из файла.
6. `A6` обращается к LM Studio только для книжных кандидатов.
7. `A7` строит финальный путь.
8. `A8` упаковывает книгу в ZIP, проверяет архив и только после успешной упаковки завершает задачу.

## Что Считается Книгой

Книжные расширения включают, например:

```text
.pdf, .djvu, .epub, .fb2, .mobi, .azw3, .doc, .docx, .rtf, .txt, .chm, .html, .ppt, .xls
```

Сильные не-книжные расширения отсекаются до LM Studio:

```text
.jpg, .png, .tif, .tiff, .mp3, .mp4, .exe, .dll, .hex, .cod, .pjt, .maa, .mos, .swf, .js, .css
```

Одиночные raster-изображения, включая `.tif/.tiff`, сейчас не считаются книгами. Это сделано, чтобы не отправлять в LM Studio страницы, прошивки, схемы и технический мусор как книги.

Маленькие неизвестные бинарные файлы также отсекаются как не книги.

## Счетчики В GUI

Верхние счетчики считают книги, а не операции.

```text
Книг найдено
```

Сколько файлов `A3` признал книгами.

```text
Книг завершено
```

Сколько найденных книг уже дошли до финального результата: упакованы, признаны дубликатами или завершились ошибкой как книжные задачи.

```text
Дубликаты
```

Сколько книжных дубликатов найдено по XXH64.

```text
Не книги
```

Сколько файлов `A3` признал не книгами. Этот счетчик не входит в книжный прогресс.

```text
Ошибки книг
```

Сколько книжных задач завершились ошибкой.

Процент выполнения и примерное оставшееся время считаются только от книжного прогресса:

```text
Книг завершено / Книг найдено
```

## Счетчики Агентов

В карточках агентов:

```text
P = обработано
E = ошибок
Q = очередь
```

Это счетчики стадий, а не счетчики книг. Например, `A2` может обработать много архивов-контейнеров, а `A3` увидит только обычные файлы, извлеченные из этих архивов.

## Язык Интерфейса И Выключение ПК

GUI поддерживает два языка интерфейса:

```text
ui_ru.json
ui_en.json
```

Переключение выполняется кнопками `RU` и `EN` внизу окна, без перезапуска программы.

Справа внизу находится чекбокс:

```text
⏻ Выключить ПК после завершения
```

По умолчанию он выключен. Если включить его, приложение выполнит принудительное выключение компьютера только после успешного завершения всех операций конвейера. При остановке вручную или ошибке выключение не запускается.

## Дубликаты

Дубликаты проверяются по `XXH64`.

Важное правило: для уже упакованных книг в `TARGET_DIR` хэш считается по файлу книги внутри ZIP, а не по ZIP-архиву. Поэтому сравниваются одинаковые сущности: книга с книгой.

Если дубликат найден среди исходных файлов, он переносится в `DUPES_DIR`. Если дубликат найден среди временных файлов, извлеченных из архива, он не учитывается как пользовательский `Дубликат` в GUI, чтобы не создавать ложное ощущение, что в `Duplicates` должны появиться файлы.

## LM Studio

LM Studio используется только после того, как файл прошел `A3` как книга и `A4` как уникальная книга.

Модель:

```text
google/gemma-4-e4b
```

URL по умолчанию:

```text
http://127.0.0.1:1234/v1/chat/completions
```

Приложение не отправляет в модель всю книгу. В LM Studio отправляется:

1. короткий текстовый фрагмент, если его удалось безопасно извлечь;
2. или fallback-контекст: имя файла, расширение, папка, цепочка архивов и предположения из имени файла.

Основной запрос требует строгий JSON вида:

```json
{
  "results": [
    {
      "title": "Название А",
      "author": "Автор X",
      "genre_analysis": {
        "primary_genre": "Научная фантастика",
        "subgenres": ["Транспортный триллер", "Дистопия"],
        "confidence_score": 5.0
      }
    }
  ]
}
```

Основная температура запроса:

```text
temperature = 0.1
```

Если LM Studio вернул не-JSON, приложение делает дополнительную попытку с более строгим требованием JSON.

## Удаление Исходников

Исходный файл удаляется только после успешной упаковки и проверки ZIP в `A8`.

Для исходного архива действует похожая логика: если архив был распакован, все его книжные задачи успешно завершены и не было ошибок, исходный архив может быть удален после завершения всех дочерних задач.

Если файл из `SOURCE_DIRS` признан не книгой, он переносится в `NOBOOK_DIR`.

Файлы, извлеченные во временную папку из архивов, являются рабочими копиями. После завершения конвейера временная папка очищается.

## Остановка

В GUI есть кнопка:

```text
Стоп
```

Она останавливает конвейер, сбрасывает очереди и очищает временные папки.

В терминальном режиме также поддерживается:

```text
Esc
Ctrl+S
```

Обе комбинации означают остановку с очисткой временной папки.

## База Данных

БД постоянная и хранится в `TARGET_DIR`.

Имя БД строится по выбранным `SOURCE_DIRS`, чтобы при переключении на другую папку не уничтожать историю предыдущей папки.

БД хранит:

1. хэши книг;
2. финальные пути ZIP;
3. метаданные;
4. кэш ответов LM Studio;
5. статусы задач.

## Логи

Логи пишутся в папку:

```text
logs
```

В логах можно увидеть:

1. какие файлы нашел `A1`;
2. что распаковал `A2`;
3. почему `A3` признал файл книгой или не книгой;
4. какой XXH64 посчитал `A4`;
5. что вернул LM Studio;
6. куда `A7` направил книгу;
7. какой ZIP создал `A8`.

## Результирующие Папки

```text
TARGET_DIR
```

Основная отсортированная библиотека.

```text
DUPES_DIR
```

Пользовательские дубликаты книг.

```text
NOBOOK_DIR
```

Файлы из источников, которые не являются книгами.

```text
TARGET_DIR\_TempPipeline
```

Временная рабочая зона. После завершения или остановки очищается.

## English

LibSort is a one-off pipeline for sorting a large book collection where books may exist as standalone files, inside archives, or inside nested archives.

The main goal is to find actual books, reject technical/non-book files, detect metadata, deduplicate by XXH64, and place each result into this structure:

```text
TARGET_DIR\Genre\Author first letter\Author\Book.zip
```

Each book is packed into its own ZIP archive with maximum compression via 7-Zip.

## Quick Start

1. Make sure `7z.exe` and Python are installed.
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Start the GUI:

```powershell
python .\library_gui.py
```

4. In the GUI, select one or more `SOURCE_DIRS`, check `TARGET_DIR`, and press `Старт`.

## Settings

Main settings are stored in [setting.py](setting.py).

Key paths:

```python
SOURCE_DIRS = [
    r"E:\Энциклопедии. Словари. Справочники",
]
TARGET_DIR = r"E:\Sorted_Library"
DUPES_DIR = r"E:\Sorted_Library\Duplicates"
NOBOOK_DIR = r"E:\Sorted_Library\NoBook"
```

`TEMP_BASE` is optional. If it is not set, the application uses:

```text
<TARGET_DIR>\_TempPipeline
```

This is intentional: keeping temporary packing work on the same disk as `TARGET_DIR` is usually faster and safer for atomic replacement of final archives.

Worker counts are also configured in [setting.py](setting.py):

```python
UNPACK_WORKERS = 6
DETECT_WORKERS = 2
DEDUPE_WORKERS = 3
TAG_WORKERS = 3
LM_WORKERS = 1
RENAME_WORKERS = 1
PACK_WORKERS = 6
```

## Pipeline Logic

Before the main run, a service agent starts:

```text
A0 DB Sync
```

It compares the persistent database with existing ZIP files in `TARGET_DIR`. For ZIP files already in the target directory, XXH64 is calculated from the book payload inside the ZIP, not from the ZIP archive itself.

Main agents:

```text
A1 Search
A2 Unpack
A3 Book?
A4 XXH64
A5 Tags
A6 LM Studio
A7 Rename/Route
A8 Pack
```

Processing order:

1. `A1` searches files in selected `SOURCE_DIRS`.
2. `A2` unpacks archives, including nested archives.
3. `A3` decides whether a file is a book.
4. `A4` calculates XXH64 and removes duplicates before expensive stages.
5. `A5` reads tags and metadata.
6. `A6` asks LM Studio only for book candidates.
7. `A7` builds the final destination path.
8. `A8` packs the book into ZIP, tests the archive, and only then finishes the task.

## What Counts As A Book

Book extensions include, for example:

```text
.pdf, .djvu, .epub, .fb2, .mobi, .azw3, .doc, .docx, .rtf, .txt, .chm, .html, .ppt, .xls
```

Strong non-book extensions are rejected before LM Studio:

```text
.jpg, .png, .tif, .tiff, .mp3, .mp4, .exe, .dll, .hex, .cod, .pjt, .maa, .mos, .swf, .js, .css
```

Single raster images, including `.tif/.tiff`, are currently not treated as books. This prevents pages, firmware, diagrams, and technical artifacts from being sent to LM Studio as books.

Small unknown binary files are also rejected as non-books.

## GUI Counters

Top-level counters count books, not pipeline operations.

```text
Books found
```

Number of files that `A3` accepted as books.

```text
Books done
```

Number of accepted books that reached a final result: packed, duplicate, or failed as a book task.

```text
Duplicates
```

Number of book duplicates found by XXH64.

```text
Not books
```

Number of files that `A3` rejected as non-books. This counter is not part of book progress.

```text
Book errors
```

Number of book tasks that ended with an error.

Progress percentage and ETA are calculated only from book progress:

```text
Books done / Books found
```

## Agent Counters

Agent cards show:

```text
P = processed
E = errors
Q = queue
```

These are stage counters, not book counters. For example, `A2` can process many archive containers, while `A3` only sees regular files extracted from those archives.

## Interface Language And PC Shutdown

The GUI supports two interface languages:

```text
ui_ru.json
ui_en.json
```

The language can be switched live with the `RU` and `EN` buttons at the bottom of the window. No restart is required.

The bottom-right checkbox controls automatic shutdown:

```text
⏻ Shut down PC when done
```

It is disabled by default. If enabled, the application forcibly shuts down the computer only after the pipeline finishes all operations successfully. Manual stop or pipeline errors do not trigger shutdown.

## Duplicates

Duplicates are checked by `XXH64`.

Important rule: for books already packed in `TARGET_DIR`, the hash is calculated from the book file inside the ZIP, not from the ZIP archive. Therefore the comparison is book-to-book.

If a duplicate is found among source files, it is moved to `DUPES_DIR`. If a duplicate is found among temporary files extracted from an archive, it is not counted as a user-visible `Дубликат` in the GUI, so the `Duplicates` folder is not expected to receive those temporary files.

## LM Studio

LM Studio is used only after a file has passed `A3` as a book and `A4` as a unique book.

Model:

```text
google/gemma-4-e4b
```

Default URL:

```text
http://127.0.0.1:1234/v1/chat/completions
```

The application does not send the whole book to the model. It sends:

1. a short text snippet if it can be safely extracted;
2. or fallback context: filename, extension, parent folder, archive chain, and filename-based guesses.

The main prompt asks for strict JSON:

```json
{
  "results": [
    {
      "title": "Название А",
      "author": "Автор X",
      "genre_analysis": {
        "primary_genre": "Научная фантастика",
        "subgenres": ["Транспортный триллер", "Дистопия"],
        "confidence_score": 5.0
      }
    }
  ]
}
```

Main request temperature:

```text
temperature = 0.1
```

If LM Studio returns non-JSON, the application performs an additional stricter JSON-only retry.

## Source Deletion

A source file is deleted only after successful ZIP creation and archive test in `A8`.

For source archives, similar logic is used: if an archive was unpacked, all its book tasks finished successfully, and there were no failures, the source archive may be deleted after all child tasks are complete.

If a file from `SOURCE_DIRS` is classified as non-book, it is moved to `NOBOOK_DIR`.

Files extracted into the temporary folder from archives are working copies. The temporary folder is cleaned after completion or stop.

## Stop Behavior

The GUI has a button:

```text
Стоп
```

It stops the pipeline, clears queues, and cleans temporary folders.

Terminal mode also supports:

```text
Esc
Ctrl+S
```

Both mean stop with temporary-folder cleanup.

## Database

The database is persistent and stored in `TARGET_DIR`.

The database name is derived from selected `SOURCE_DIRS`, so switching to another source folder does not destroy history for the previous one.

The database stores:

1. book hashes;
2. final ZIP paths;
3. metadata;
4. LM Studio response cache;
5. task statuses.

## Logs

Logs are written to:

```text
logs
```

Logs show:

1. what `A1` found;
2. what `A2` unpacked;
3. why `A3` accepted or rejected a file;
4. which XXH64 value `A4` calculated;
5. what LM Studio returned;
6. where `A7` routed the book;
7. which ZIP `A8` created.

## Output Folders

```text
TARGET_DIR
```

Main sorted library.

```text
DUPES_DIR
```

User-visible duplicate books.

```text
NOBOOK_DIR
```

Source files that are not books.

```text
TARGET_DIR\_TempPipeline
```

Temporary workspace. It is cleaned after completion or stop.
