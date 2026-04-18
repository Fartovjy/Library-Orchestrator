from __future__ import annotations

import argparse
import json

from .config import AppConfig
from .orchestrator import LibraryOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Library orchestrator CLI")
    parser.add_argument(
        "--config",
        default="config.example.json",
        help="Path to JSON config file.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run orchestrator.")
    run_parser.add_argument("--limit", type=int, default=None, help="Limit processed items.")

    subparsers.add_parser("repair", help="Repair SQLite state against current files.")
    subparsers.add_parser("status", help="Print state summary.")
    subparsers.add_parser("pause", help="Pause dispatching new work.")
    subparsers.add_parser("resume", help="Resume a paused run.")
    subparsers.add_parser("stop", help="Request safe stop.")
    subparsers.add_parser("clear-stop", help="Remove stop request.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = AppConfig.from_file(args.config)
    orchestrator = LibraryOrchestrator(config)

    if args.command == "run":
        limit = args.limit if args.limit is not None else config.limits.max_items_per_run
        status = orchestrator.run(limit=limit)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 0

    if args.command == "status":
        active_batch = orchestrator.state_store.get_active_batch()
        if active_batch is not None:
            print(
                json.dumps(
                    orchestrator.state_store.batch_status_counts(active_batch.batch_id),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            print(json.dumps(orchestrator.state_store.status_counts(), ensure_ascii=False, indent=2))
        return 0

    if args.command == "repair":
        summary = orchestrator.repair_database()
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "pause":
        pause_file = orchestrator.create_pause_file()
        print(f"Pause requested: {pause_file}")
        return 0

    if args.command == "resume":
        orchestrator.clear_pause_file()
        print("Pause cleared.")
        return 0

    if args.command == "stop":
        stop_file = orchestrator.create_stop_file()
        print(f"Stop requested: {stop_file}")
        return 0

    if args.command == "clear-stop":
        orchestrator.clear_stop_file()
        print("Stop request cleared.")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
