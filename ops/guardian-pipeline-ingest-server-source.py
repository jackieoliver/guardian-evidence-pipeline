#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
INGEST_SCRIPT = SCRIPT_DIR / "guardian-pipeline-ingest.py"


def load_ingest_module():
    spec = importlib.util.spec_from_file_location("guardian_pipeline_ingest", INGEST_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {INGEST_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run capture ingest directly against the server-staged Guardian source tree.")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--device-id", required=True, help="Device identifier")
    parser.add_argument(
        "--source",
        required=True,
        choices=["desktop-linux", "laptop-macos"],
        help="Server capture source to ingest",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to ingest. Use YYYY-MM-DD for macOS capture and either YYYY-MM-DD or YYYY/MM/DD for Linux capture.",
    )
    parser.add_argument("--gap-seconds", type=float, default=45.0)
    parser.add_argument("--image-limit", type=int, default=3)
    parser.add_argument("--processor-version", default="v1")
    return parser.parse_args()


def canonical_server_root(source: str, date_text: str) -> Path:
    if source == "laptop-macos":
        return Path("./data/incoming/example-user/laptop-macos/desktop-capture") / date_text

    normalized_date = date_text.replace("-", "/")
    return Path("./data/incoming/example-user/desktop-linux") / normalized_date


def main() -> int:
    args = parse_args()
    ingest = load_ingest_module()
    requested_root = canonical_server_root(args.source, args.date)
    result = ingest.run_ingest(
        db_path=Path(args.db).expanduser().resolve(),
        requested_root=requested_root,
        user_id=args.user_id,
        device_id=args.device_id,
        gap_seconds=args.gap_seconds,
        image_limit=args.image_limit,
        processor_version=args.processor_version,
    )
    print(f"db={result['db']}")
    print(f"requested_input_root={result['requested_input_root']}")
    print(f"resolved_input_root={result['resolved_input_root']}")
    print(f"observations={result['observations']} segments={result['segments']}")
    print(f"queued_segment_jobs={result['queued_segment_jobs']}")
    print(f"skipped_segment_jobs={result['skipped_segment_jobs']}")
    print(f"batch_job_id={result['batch_job_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
