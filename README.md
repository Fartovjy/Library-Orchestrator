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
- trash and non-book files routed to `_Trash`.

## Target Output Structure

`Library/<Genre>/<AuthorInitial>/<Author>/<Author - Title>.zip`

Special branches:

- `_Duplicates`
- `_Manual_Review`
- `_Trash`
- error / damaged archive quarantine

## Processing Rules

1. Discover one source object at a time from the library root.
2. Detect archive/container type.
3. Unpack safely into a temporary workspace.
4. Run fast recognition on metadata, filename, and short excerpt.
5. Escalate to deep analysis when confidence is below threshold.
6. Normalize the work result into a single ZIP archive.
7. Compute hashes and resolve duplicates.
8. Move the final archive into the target library tree.
9. Persist every step into the state store and logs.

## Implementation Direction

- Python 3.10+
- `psutil`, `hashlib`, `zipfile`, `requests`
- LM Studio adapter with model switching, retries, and timeouts
- modular queues and agents
- explicit config file for thresholds, limits, paths, and models

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
- SQLite state store for items, events, and known content hashes
- resource monitor with IO busy sampling via `psutil`
- LM Studio client for fast and deep classification passes
- unpack, fast-classify, deep-classify, pack, and placement agents
- duplicate routing, manual-review routing, trash routing, and damaged-file routing
- CLI commands for `run`, `status`, `stop`, and `clear-stop`
- terminal dashboard with current file, current stage, global progress, and per-agent progress
- dashboard agent table includes both completion percent and average recognition percent for classifiers
- file-type detection by magic bytes before LM Studio classification
- Windows terminal dashboard with ANSI re-rendering and active-agent indicator
- Safe stop hotkeys in the running console: `Ctrl+X`, `Esc`, or `Q`

## Current Scope Of This First Build

- Supported unpack now: directories, plain files, ZIP-like archives, FB2, PDF, `rar`, and `7z`
- `rar` and `7z` support uses installed Windows tools such as `7-Zip` or `WinRAR`
- Output normalization is always a ZIP archive with LZMA compression level 9
- The pipeline is stateful, resumable, and processes multiple source items in parallel
- Fast trash/manual-review routing can proceed while LM Studio is classifying other files
- Typical tuning is 4 light workers with 1 shared heavy LM classification slot
- Light workers persist prepared items and excerpts into SQLite before heavy processing
- Queue abstraction, worker limits, and HDD throttling are in place for controlled scaling
- Discovery order is breadth-first: current folder level first, then subfolders
- Smaller files are scheduled before larger ones within each folder level
- Non-book binaries are filtered using file signatures, not only filename extensions
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
