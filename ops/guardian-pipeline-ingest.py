#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from guardian_capture_segments import (
    build_segment_packets,
    load_observations,
    resolve_processing_path,
    segment_observations,
    stable_id,
)
from guardian_pipeline_db import (
    artifact_record_from_file,
    connect_db,
    init_db,
    insert_job_if_missing,
    iso_z_now,
    job_record,
    stable_file_artifact_id,
    upsert_artifact,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Guardian capture files into the local pipeline DB and queue Codex interpretation jobs."
    )
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument(
        "--input",
        required=True,
        help="Root capture directory to scan recursively. Canonical server /data/... paths are supported when the SFTP mount is available locally.",
    )
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--device-id", required=True, help="Device identifier")
    parser.add_argument(
        "--gap-seconds",
        type=float,
        default=45.0,
        help="Maximum gap between captures before forcing a new segment",
    )
    parser.add_argument("--image-limit", type=int, default=3, help="Sample image count per segment packet")
    parser.add_argument(
        "--processor-version",
        default="v1",
        help="Processor version label for the queued segment interpretation jobs",
    )
    return parser.parse_args()


def observation_monotonic_ms(observation: Any) -> int | None:
    value = observation.raw.get("timestampMonotonicMs")
    if value is None:
        value = observation.raw.get("timestamp_monotonic_ms")
    return int(value) if value is not None else None


def observation_clock_offset_ms(observation: Any) -> int | None:
    value = observation.raw.get("clockOffsetMs")
    if value is None:
        value = observation.raw.get("clock_offset_ms")
    return int(value) if value is not None else None


def observation_clock_confidence(observation: Any) -> str:
    if observation_clock_offset_ms(observation) is not None:
        return "high"
    if observation.platform in {"macos", "linux"}:
        return "medium"
    return "low"


def run_ingest(
    *,
    db_path: Path,
    requested_root: Path,
    user_id: str,
    device_id: str,
    gap_seconds: float,
    image_limit: int,
    processor_version: str,
) -> dict[str, Any]:
    root = resolve_processing_path(requested_root, anchors=[Path.cwd()]) or requested_root.expanduser()
    observations = load_observations(root)
    if not observations:
        raise SystemExit(f"No usable sidecar JSON files found under {root}")

    segments = segment_observations(observations, gap_seconds)
    packets = build_segment_packets(segments, user_id, device_id, image_limit=image_limit)

    conn = connect_db(db_path)
    init_db(conn)

    received_at = iso_z_now()
    image_artifact_ids: dict[str, str] = {}
    metadata_artifact_ids: dict[str, str] = {}

    for observation in observations:
        if observation.image_path and observation.image_path.exists():
            image_artifact_id = stable_file_artifact_id(
                "screenshot",
                observation.image_path,
                user_id,
                device_id,
            )
            image_record = artifact_record_from_file(
                artifact_id=image_artifact_id,
                user_id=user_id,
                device_id=device_id,
                artifact_type="screenshot",
                file_path=observation.image_path,
                captured_at_wall=observation.timestamp.isoformat().replace("+00:00", "Z"),
                received_at=received_at,
                normalized_at=observation.timestamp.isoformat().replace("+00:00", "Z"),
                metadata={
                    "foreground_app": observation.app_name,
                    "window_title": observation.window_title,
                    "window_id": observation.window_id,
                    "capture_method": observation.raw.get("captureMethod") or observation.raw.get("capture_method"),
                    "window_bounds": {
                        "width": observation.width,
                        "height": observation.height,
                    },
                    "linked_json_path": str(observation.json_path),
                    "platform": observation.platform,
                },
                captured_at_monotonic_ms=observation_monotonic_ms(observation),
                clock_offset_ms=observation_clock_offset_ms(observation),
                clock_confidence=observation_clock_confidence(observation),
            )
            upsert_artifact(conn, image_record)
            image_artifact_ids[str(observation.image_path.resolve())] = image_artifact_id

        metadata_artifact_id = stable_file_artifact_id(
            "app_metadata",
            observation.json_path,
            user_id,
            device_id,
        )
        metadata_record = artifact_record_from_file(
            artifact_id=metadata_artifact_id,
            user_id=user_id,
            device_id=device_id,
            artifact_type="app_metadata",
            file_path=observation.json_path,
            captured_at_wall=observation.timestamp.isoformat().replace("+00:00", "Z"),
            received_at=received_at,
            normalized_at=observation.timestamp.isoformat().replace("+00:00", "Z"),
            metadata={
                "frontmost_app_name": observation.app_name,
                "window_title": observation.window_title,
                "window_id": observation.window_id,
                "window_bounds": {
                    "width": observation.width,
                    "height": observation.height,
                },
                "platform": observation.platform,
                "capture_method": observation.raw.get("captureMethod") or observation.raw.get("capture_method"),
                "image_path": str(observation.image_path) if observation.image_path else None,
                "source_payload": observation.raw,
            },
            captured_at_monotonic_ms=observation_monotonic_ms(observation),
            clock_offset_ms=observation_clock_offset_ms(observation),
            clock_confidence=observation_clock_confidence(observation),
        )
        upsert_artifact(conn, metadata_record)
        metadata_artifact_ids[str(observation.json_path.resolve())] = metadata_artifact_id

    batch_job_id = stable_id(
        "guardian-batch-interpret-job",
        user_id,
        device_id,
        str(requested_root),
        str(gap_seconds),
        processor_version,
    )
    parent_job = job_record(
        job_id=batch_job_id,
        user_id=user_id,
        device_id=device_id,
        parent_job_id=None,
        job_type="fuse",
        target_type="batch",
        target_id=str(requested_root),
        processor_name="guardian_segment_interpret_batch",
        processor_version=processor_version,
        priority=100,
        max_attempts=1,
        input_data={
            "input_root": str(requested_root),
            "resolved_input_root": str(root),
            "capture_count": len(observations),
            "segment_count": len(segments),
            "gap_seconds": gap_seconds,
        },
        queued_at=received_at,
    )
    insert_job_if_missing(conn, parent_job)

    new_child_jobs = 0
    skipped_child_jobs = 0
    for segment, packet in zip(segments, packets):
        if packet.get("should_queue_interpretation") is False:
            skipped_child_jobs += 1
            continue

        source_artifact_ids: list[str] = []
        for observation in segment:
            metadata_artifact_id = metadata_artifact_ids[str(observation.json_path.resolve())]
            source_artifact_ids.append(metadata_artifact_id)
            if observation.image_path and observation.image_path.exists():
                image_artifact_id = image_artifact_ids[str(observation.image_path.resolve())]
                source_artifact_ids.append(image_artifact_id)

        deduped_source_artifact_ids = list(dict.fromkeys(source_artifact_ids))
        child_job_id = stable_id(
            "guardian-segment-interpret-job",
            batch_job_id,
            packet["id"],
            processor_version,
        )
        child_job = job_record(
            job_id=child_job_id,
            user_id=user_id,
            device_id=device_id,
            parent_job_id=batch_job_id,
            job_type="fuse",
            target_type="segment_packet",
            target_id=packet["id"],
            processor_name="codex_segment_interpreter",
            processor_version=processor_version,
            priority=10,
            max_attempts=2,
            input_data={
                "packet": packet,
                "packet_id": packet["id"],
                "source_artifact_ids": deduped_source_artifact_ids,
                "input_root": str(requested_root),
                "resolved_input_root": str(root),
            },
            queued_at=received_at,
        )
        if insert_job_if_missing(conn, child_job):
            new_child_jobs += 1

    conn.close()
    return {
        "db": str(db_path),
        "requested_input_root": str(requested_root),
        "resolved_input_root": str(root),
        "observations": len(observations),
        "segments": len(segments),
        "queued_segment_jobs": new_child_jobs,
        "skipped_segment_jobs": skipped_child_jobs,
        "batch_job_id": batch_job_id,
    }


def main() -> int:
    args = parse_args()
    result = run_ingest(
        db_path=Path(args.db).expanduser().resolve(),
        requested_root=Path(args.input).expanduser(),
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
