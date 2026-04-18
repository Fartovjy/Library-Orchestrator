from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class PathsConfig:
    source_root: Path
    output_root: Path
    workspace_root: Path
    library_root: Path
    duplicates_root: Path
    non_book_root: Path
    manual_review_root: Path
    trash_root: Path
    damaged_root: Path
    failed_root: Path
    state_db: Path
    logs_root: Path
    stop_file: Path
    pause_file: Path
    run_lock_file: Path

    def ensure_directories(self) -> None:
        for path in (
            self.output_root,
            self.workspace_root,
            self.library_root,
            self.duplicates_root,
            self.non_book_root,
            self.manual_review_root,
            self.trash_root,
            self.damaged_root,
            self.failed_root,
            self.state_db.parent,
            self.logs_root,
            self.run_lock_file.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


@dataclass(slots=True)
class LmStudioConfig:
    base_url: str = "http://127.0.0.1:1234/v1"
    fast_model: str = "google/gemma-4-e4b"
    deep_model: str = "google/gemma-4-e4b"
    timeout_seconds: int = 90
    temperature: float = 0.1
    fast_excerpt_words: int = 180
    deep_excerpt_words: int = 1200
    fast_max_input_tokens: int = 500
    deep_max_input_tokens: int = 1400
    fast_max_output_tokens: int = 80
    deep_max_output_tokens: int = 120


@dataclass(slots=True)
class LimitsConfig:
    max_items_per_run: int = 10
    max_parallel_items: int = 4
    max_parallel_heavy_agents: int = 1
    max_parallel_unpack: int = 1
    max_parallel_pack: int = 1
    max_nested_archive_depth: int = 3
    hdd_busy_threshold_percent: float = 85.0
    sleep_if_busy_seconds: float = 5.0


@dataclass(slots=True)
class BehaviorConfig:
    cleanup_workspace: bool = True
    move_outputs: bool = False
    detect_duplicates: bool = True
    safe_mode: bool = True
    allowed_genres: list[str] = field(default_factory=list)
    trash_extensions: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AppConfig:
    paths: PathsConfig
    lmstudio: LmStudioConfig = field(default_factory=LmStudioConfig)
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    behavior: BehaviorConfig = field(default_factory=BehaviorConfig)

    @classmethod
    def from_file(cls, config_path: str | Path) -> "AppConfig":
        path = Path(config_path).expanduser().resolve()
        data = json.loads(path.read_text(encoding="utf-8"))
        paths = cls._load_paths(data["paths"])
        lmstudio = LmStudioConfig(**data.get("lmstudio", {}))
        limits = LimitsConfig(**data.get("limits", {}))
        behavior = BehaviorConfig(**data.get("behavior", {}))
        config = cls(paths=paths, lmstudio=lmstudio, limits=limits, behavior=behavior)
        config.paths.ensure_directories()
        return config

    @staticmethod
    def _load_paths(raw_paths: dict) -> PathsConfig:
        path_values = {key: Path(value) for key, value in raw_paths.items()}
        repo_root = Path(__file__).resolve().parents[1]
        output_root = path_values.get("output_root")
        if output_root is None:
            output_root = path_values.get("library_root", repo_root / "organized_output").parent
        library_root = path_values.get("library_root", output_root / "Library")
        duplicates_root = path_values.get("duplicates_root", output_root / "_Duplicates")
        non_book_root = path_values.get("non_book_root", output_root / "_Non_Books")
        manual_review_root = path_values.get("manual_review_root", output_root / "_Manual_Review")
        trash_root = path_values.get("trash_root", output_root / "_Trash")
        damaged_root = path_values.get("damaged_root", output_root / "_Damaged")
        failed_root = path_values.get("failed_root", output_root / "_Failed")
        workspace_root = path_values.get("workspace_root", repo_root / "temp")
        runtime_root = AppConfig._detect_runtime_root(path_values, repo_root)
        return PathsConfig(
            source_root=path_values["source_root"],
            output_root=output_root,
            workspace_root=workspace_root,
            library_root=library_root,
            duplicates_root=duplicates_root,
            non_book_root=non_book_root,
            manual_review_root=manual_review_root,
            trash_root=trash_root,
            damaged_root=damaged_root,
            failed_root=failed_root,
            state_db=path_values.get("state_db", runtime_root / "state" / "orchestrator.db"),
            logs_root=path_values.get("logs_root", runtime_root / "logs"),
            stop_file=path_values.get("stop_file", runtime_root / "STOP"),
            pause_file=path_values.get("pause_file", runtime_root / "PAUSE"),
            run_lock_file=path_values.get("run_lock_file", runtime_root / "RUNNING.lock"),
        )

    @staticmethod
    def _detect_runtime_root(path_values: dict[str, Path], repo_root: Path) -> Path:
        if "pause_file" in path_values:
            return path_values["pause_file"].parent
        if "stop_file" in path_values:
            return path_values["stop_file"].parent
        if "run_lock_file" in path_values:
            return path_values["run_lock_file"].parent
        if "logs_root" in path_values:
            return path_values["logs_root"].parent
        if "state_db" in path_values:
            state_parent = path_values["state_db"].parent
            if state_parent.name.lower() == "state":
                return state_parent.parent
            return state_parent
        return repo_root / "runtime"
