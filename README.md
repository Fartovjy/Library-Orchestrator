# Orchestrator Project

## Purpose

Create a resilient multi-agent pipeline that:

- scans a large mixed library incrementally;
- unpacks archives safely into temporary work areas;
- identifies books, authors, titles, series, and genres;
- repacks normalized results into ZIP archives;
- places outputs into a deterministic library structure;
- tracks duplicates, trash, manual review cases, and failures;
- survives long runs, restarts, and partial interruptions.

## Unified Understanding Of Both Specs

The system is centered around an `Orchestrator` that manages queues, state, resources, and back-pressure.

Primary agents:

- `Unpack Agent`
- `Splitter Agent` for multi-book containers
- `Archivarius` fast recognition agent via LM Studio
- `Expert` deep-analysis agent for low-confidence cases
- `Pack Agent`
- `Placement Agent`

Core storage and control requirements:

- central state store for every object and processing stage;
- idempotent processing with resume after crash;
- HDD-aware concurrency control;
- duplicate detection and routing to `_Duplicates`;
- unresolved objects routed to `_Manual_Review`;
- trash files routed to `_Trash`;
- obvious non-book sources routed to `_Non_Books`.

## Target Output Structure

`Library/<Genre>/<AuthorInitial>/<Author>/<Author - Title>.zip`

Special branches:

- `_Duplicates`
- `_Non_Books`
- `_Manual_Review`
- `_Trash`
- error / damaged archive quarantine

## Processing Rules

1. Select a batch of source objects from the library root.
2. Create discovery tasks for the whole batch in SQLite.
3. Route trash and obvious non-book sources before LM classification.
4. Unpack only archive sources into a temporary workspace.
5. Split unpacked shelves into child book items when one container clearly contains multiple books.
6. Prepare excerpts from temp workspace and run fast recognition.
7. Escalate only low-confidence cases to deep analysis.
8. Normalize the work result into a single ZIP archive.
9. Compute hashes, resolve duplicates, and place outputs into the target tree.
10. Persist every task transition into the state store for safe resume after restart.

## Implementation Direction

- Python 3.10+
- `psutil`, `hashlib`, `zipfile`, `requests`
- LM Studio adapter with model switching, retries, and timeouts
- modular queues and agents
- explicit config file for thresholds, limits, paths, and models

## Recommended Models

- Fast model for `Archivarius`: `vikhr-qwen-2.5-1.5b-instruct`
- Deep model for `Expert`: `google/gemma-4-e4b`
- Keep the fast model small and Russian-friendly for filename and short-excerpt genre routing
- Keep the deep model stronger for low-confidence and ambiguous cases

## Next Step

Build the new project around:

- orchestrator
- state store
- queue model
- LM Studio adapter
- archive adapters
- pack / place pipeline

## Project Layout

```text
orchestrator_project/
  config.example.json
  pyproject.toml
  orchestrator/
    agents/
      archivarius.py
      expert.py
      pack.py
      placement.py
      unpack.py
    archive_adapters.py
    config.py
    lmstudio.py
    main.py
    models.py
    orchestrator.py
    queues.py
    resource_monitor.py
    state_store.py
```

## What Is Already Implemented

- JSON config with runtime paths, models, limits, and behavior flags
- separate `output_root` for final outputs so results do not loop back into the source tree
- default temp workspace at `C:/Users/Home/Documents/orchestrator_project/temp`
- SQLite state store for items, events, and known content hashes
- SQLite batch/task queue so workers claim work by stage instead of keeping the pipeline in RAM
- parent/root item tracking in SQLite so one archive can expand into many child book items
- resource monitor with IO busy sampling via `psutil`
- LM Studio client for fast and deep classification passes
- unpack, fast-classify, deep-classify, pack, and placement agents
- duplicate routing, manual-review routing, trash routing, and damaged-file routing
- CLI commands for `run`, `status`, `stop`, and `clear-stop`
- terminal dashboard with current file, current stage, global progress, and per-agent progress
- dashboard percent now uses each stage's own denominator:
  - `discovery` from the selected batch size
  - `unpack` only from archive items in the batch
  - `splitter` only from containers that need bookshelf analysis
  - `expert` only from low-confidence items that actually escalated
- dashboard agent table includes both completion percent and average recognition percent for classifiers
- file-type detection by magic bytes before LM Studio classification
- Windows terminal dashboard with ANSI re-rendering and active-agent indicator
- Safe stop hotkeys in the running console: `Ctrl+X`, `Esc`, or `Q`

## Current Scope Of This First Build

- Supported unpack now: directories, plain files, ZIP-like archives, FB2, PDF, `rar`, and `7z`
- `rar` and `7z` support uses installed Windows tools such as `7-Zip` or `WinRAR`
- Nested `zip`/`epub`/`rar`/`7z` archives are unpacked recursively in temp workspace with a configurable depth limit
- Multi-book archives are split into child items before `Archivarius`, so a shelf of books is no longer forced into one record
- Output normalization is always a ZIP archive with LZMA compression level 9
- The pipeline is stateful, resumable, and processes multiple source items in parallel
- DB-first workers can resume after stop/restart by resetting claimed tasks back to pending
- Fast trash/non-book/manual-review routing can proceed while LM Studio is classifying other files
- Typical tuning is 4 light workers with 1 shared heavy LM classification slot
- Light workers work from SQLite tasks and keep temp excerpts in the DB before heavy processing
- Queue abstraction, worker limits, and HDD throttling are in place for controlled scaling
- Discovery order is breadth-first: current folder level first, then subfolders
- Smaller files are scheduled before larger ones within each folder level
- Obvious code/program sources are filtered before `Archivarius`, which improves useful recognition rate
- `max_nested_archive_depth` controls how many archive layers are expanded inside temp workspace
- LM Studio input is capped by approximate token budgets, not only by word counts

## First Run

```powershell
cd E:\Разобрать\orchestrator_project
python -m orchestrator.main --config config.example.json run --limit 5
```

Safe stop:

```powershell
python -m orchestrator.main --config config.example.json stop
```
