#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from guardian_capture_segments import resolve_processing_path, stable_id
from guardian_capture_segments import iso_z
from guardian_pipeline_db import (
    artifact_record_from_file,
    connect_db,
    init_db,
    iso_z_now,
    json_dumps,
    stable_file_artifact_id,
    upsert_artifact,
    upsert_scene_window,
    upsert_scene_window_artifact,
    virtual_json_artifact_record,
)
from guardian_source_priority import is_direct_device_source, source_priority_for_type


@dataclass(frozen=True)
class RecorderChunk:
    chunk_index: int
    start_seconds: float
    end_seconds: float
    duration_seconds: float
    source_type: str
    primary_language_code: str | None
    transcript: str
    english_translation: str | None
    translation_note: str | None
    notes: str | None


@dataclass(frozen=True)
class RecorderRow:
    file_name: str
    source_path: Path | None
    duration_seconds: float
    recorder_timestamp: str | None
    modified_time: str | None
    language: str | None
    transcript: str
    chunks: list[RecorderChunk]
    segment_count: int
    avg_logprob: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest recorder transcript outputs into the canonical Guardian DB.")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--device-id", required=True, help="Device identifier for the recorder")
    parser.add_argument(
        "--input-json",
        nargs="+",
        required=True,
        help="One or more recorder transcript JSON files produced by recorder_audio_transcribe.py",
    )
    parser.add_argument(
        "--timezone",
        default=os.environ.get("GUARDIAN_RECORDER_TIMEZONE", "America/Los_Angeles"),
        help="IANA timezone used when recorder_timestamp lacks an explicit offset",
    )
    parser.add_argument(
        "--clock-offset-ms",
        type=int,
        default=None,
        help="Optional manual offset to subtract from recorder wall time when computing normalized_at",
    )
    parser.add_argument(
        "--clock-confidence",
        choices=["high", "medium", "low"],
        default=None,
        help="Clock confidence label. Defaults to medium when clock-offset-ms is provided, otherwise low.",
    )
    parser.add_argument(
        "--processor-version",
        default="v1",
        help="Version label stamped into transcript artifacts and recorder scene windows",
    )
    return parser.parse_args()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_recorder_timestamp(value: str, timezone_name: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo(timezone_name))


def resolve_source_path(raw_value: str | None, *, transcript_json_path: Path) -> Path | None:
    if not raw_value:
        return None
    resolved = resolve_processing_path(raw_value, anchors=[transcript_json_path.parent, Path.cwd()])
    if resolved is not None and resolved.exists():
        return resolved
    raw_path = Path(raw_value).expanduser()
    return raw_path.resolve() if raw_path.exists() else None


def load_rows(path: Path) -> list[RecorderRow]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: list[RecorderRow] = []
    for row in payload.get("files", []):
        if not isinstance(row, dict):
            continue
        chunks: list[RecorderChunk] = []
        for chunk in row.get("chunks") or []:
            if not isinstance(chunk, dict):
                continue
            chunks.append(
                RecorderChunk(
                    chunk_index=int(chunk.get("chunk_index") or 0),
                    start_seconds=float(chunk.get("start_seconds") or 0.0),
                    end_seconds=float(chunk.get("end_seconds") or 0.0),
                    duration_seconds=float(chunk.get("duration_seconds") or 0.0),
                    source_type=str(chunk.get("source_type") or "unclear"),
                    primary_language_code=(str(chunk.get("primary_language_code")).strip() if chunk.get("primary_language_code") else None),
                    transcript=str(chunk.get("transcript") or "").strip(),
                    english_translation=(str(chunk.get("english_translation")).strip() if chunk.get("english_translation") else None),
                    translation_note=(str(chunk.get("translation_note")).strip() if chunk.get("translation_note") else None),
                    notes=(str(chunk.get("notes")).strip() if chunk.get("notes") else None),
                )
            )
        rows.append(
            RecorderRow(
                file_name=str(row.get("file_name") or ""),
                source_path=resolve_source_path(row.get("source_path"), transcript_json_path=path),
                duration_seconds=float(row.get("duration_seconds") or 0.0),
                recorder_timestamp=(str(row.get("recorder_timestamp")).strip() if row.get("recorder_timestamp") else None),
                modified_time=(str(row.get("modified_time")).strip() if row.get("modified_time") else None),
                language=(str(row.get("language")).strip() if row.get("language") else None),
                transcript=str(row.get("transcript") or "").strip(),
                chunks=chunks,
                segment_count=int(row.get("segment_count") or 0),
                avg_logprob=float(row["avg_logprob"]) if row.get("avg_logprob") is not None else None,
            )
        )
    return rows


def choose_captured_at_wall(row: RecorderRow, timezone_name: str) -> datetime:
    if row.recorder_timestamp:
        return parse_recorder_timestamp(row.recorder_timestamp, timezone_name)
    if row.modified_time:
        return parse_iso(row.modified_time)
    if row.source_path and row.source_path.exists():
        return datetime.fromtimestamp(row.source_path.stat().st_mtime, tz=ZoneInfo(timezone_name))
    raise ValueError(f"Row {row.file_name} has no usable timestamp")


def normalize_time(captured_at_wall: datetime, clock_offset_ms: int | None) -> datetime:
    if clock_offset_ms is None:
        return captured_at_wall
    return captured_at_wall - timedelta(milliseconds=clock_offset_ms)


def preferred_clock_confidence(offset_ms: int | None, explicit: str | None) -> str:
    if explicit:
        return explicit
    return "medium" if offset_ms is not None else "low"


def chunk_confidence(source_type: str, transcript: str) -> float:
    base = {
        "room": 0.82,
        "mixed": 0.58,
        "media": 0.3,
        "unclear": 0.18,
    }.get(source_type, 0.2)
    if len(transcript.strip()) < 40:
        base -= 0.1
    return max(0.05, min(0.95, round(base, 3)))


def clip_text(text: str, limit: int = 500) -> str:
    stripped = text.strip()
    if len(stripped) <= limit:
        return stripped
    return stripped[: limit - 1].rstrip() + "…"


def ingest_transcript_json(
    *,
    conn: Any,
    transcript_json_path: Path,
    user_id: str,
    device_id: str,
    timezone_name: str,
    clock_offset_ms: int | None,
    clock_confidence: str,
    processor_version: str,
) -> dict[str, int]:
    rows = load_rows(transcript_json_path)
    received_at = iso_z_now()
    transcript_processor_name = "recorder_audio_transcribe"
    scene_builder_name = "recorder_chunk_ingest"

    artifact_count = 0
    scene_window_count = 0
    raw_audio_missing_count = 0

    for row in rows:
        captured_at_wall = choose_captured_at_wall(row, timezone_name)
        normalized_start = normalize_time(captured_at_wall, clock_offset_ms)
        captured_at_wall_iso = iso_z(captured_at_wall)
        normalized_start_iso = iso_z(normalized_start)

        raw_audio_artifact_id: str | None = None
        if row.source_path and row.source_path.exists():
            raw_audio_artifact_id = stable_file_artifact_id("audio_chunk", row.source_path, user_id, device_id)
            raw_audio_metadata = {
                "source_kind": "recorder_wav",
                "file_name": row.file_name,
                "duration_seconds": row.duration_seconds,
                "recorder_timestamp": row.recorder_timestamp,
                "modified_time": row.modified_time,
                "language": row.language,
                "segment_count": row.segment_count,
                "avg_logprob": row.avg_logprob,
            }
            upsert_artifact(
                conn,
                artifact_record_from_file(
                    artifact_id=raw_audio_artifact_id,
                    user_id=user_id,
                    device_id=device_id,
                    artifact_type="audio_chunk",
                    file_path=row.source_path,
                    captured_at_wall=captured_at_wall_iso,
                    received_at=received_at,
                    normalized_at=normalized_start_iso,
                    metadata=raw_audio_metadata,
                    clock_offset_ms=clock_offset_ms,
                    clock_confidence=clock_confidence,
                    processor_name=None,
                    processor_version=None,
                ),
            )
            artifact_count += 1
        else:
            raw_audio_missing_count += 1

        transcript_file_artifact_id = stable_id(
            "guardian-recorder-transcript-file",
            user_id,
            device_id,
            row.file_name,
            processor_version,
        )
        transcript_file_payload = {
            "file_name": row.file_name,
            "source_path": str(row.source_path) if row.source_path else None,
            "duration_seconds": row.duration_seconds,
            "recorder_timestamp": row.recorder_timestamp,
            "modified_time": row.modified_time,
            "language": row.language,
            "segment_count": row.segment_count,
            "avg_logprob": row.avg_logprob,
            "transcript": row.transcript,
            "chunk_count": len(row.chunks),
        }
        transcript_file_metadata = {
            "source_kind": "recorder_transcript_file",
            "transcript_json_path": str(transcript_json_path),
            "file_name": row.file_name,
            "language": row.language,
            "segment_count": row.segment_count,
            "avg_logprob": row.avg_logprob,
            "chunk_count": len(row.chunks),
            "raw_audio_available": raw_audio_artifact_id is not None,
        }
        upsert_artifact(
            conn,
            virtual_json_artifact_record(
                artifact_id=transcript_file_artifact_id,
                user_id=user_id,
                device_id=device_id,
                artifact_type="transcript",
                storage_uri=f"{transcript_json_path.resolve()}#file={row.file_name}",
                payload=transcript_file_payload,
                captured_at_wall=captured_at_wall_iso,
                received_at=received_at,
                normalized_at=normalized_start_iso,
                metadata=transcript_file_metadata,
                clock_offset_ms=clock_offset_ms,
                clock_confidence=clock_confidence,
                processor_name=transcript_processor_name,
                processor_version=processor_version,
                schema_version="recorder_audio_transcripts.v2",
            ),
        )
        artifact_count += 1

        if not row.chunks:
            synthetic_chunk = RecorderChunk(
                chunk_index=1,
                start_seconds=0.0,
                end_seconds=row.duration_seconds,
                duration_seconds=row.duration_seconds,
                source_type="unclear",
                primary_language_code=row.language,
                transcript=row.transcript,
                english_translation=None,
                translation_note=None,
                notes="No chunked transcript was available; file-level transcript used as a fallback.",
            )
            chunks = [synthetic_chunk]
        else:
            chunks = row.chunks

        for chunk in chunks:
            # Skip media-only chunks — song lyrics are not ambient audio
            if chunk.source_type == "media":
                continue
            # Skip non-English room/media chunks — foreign audio is background music, not user speech
            chunk_lang = (chunk.primary_language_code or "").strip().lower()
            if chunk.source_type in {"room", "media"} and chunk_lang and not chunk_lang.startswith("en"):
                continue

            chunk_wall_start = captured_at_wall + timedelta(seconds=chunk.start_seconds)
            chunk_wall_end = captured_at_wall + timedelta(seconds=chunk.end_seconds)
            chunk_normalized_start = normalize_time(chunk_wall_start, clock_offset_ms)
            chunk_normalized_end = normalize_time(chunk_wall_end, clock_offset_ms)
            chunk_wall_start_iso = iso_z(chunk_wall_start)
            chunk_wall_end_iso = iso_z(chunk_wall_end)
            chunk_normalized_start_iso = iso_z(chunk_normalized_start)
            chunk_normalized_end_iso = iso_z(chunk_normalized_end)
            transcript_chunk_artifact_id = stable_id(
                "guardian-recorder-transcript-chunk",
                user_id,
                device_id,
                row.file_name,
                str(chunk.chunk_index),
                processor_version,
            )
            chunk_payload = {
                "file_name": row.file_name,
                "chunk_index": chunk.chunk_index,
                "start_seconds": chunk.start_seconds,
                "end_seconds": chunk.end_seconds,
                "duration_seconds": chunk.duration_seconds,
                "source_type": chunk.source_type,
                "primary_language_code": chunk.primary_language_code,
                "transcript": chunk.transcript,
                "english_translation": chunk.english_translation,
                "translation_note": chunk.translation_note,
                "notes": chunk.notes,
            }
            chunk_metadata = {
                "source_kind": "recorder_transcript_chunk",
                "transcript_json_path": str(transcript_json_path),
                "file_name": row.file_name,
                "chunk_index": chunk.chunk_index,
                "source_type": chunk.source_type,
                "primary_language_code": chunk.primary_language_code,
                "has_translation": bool(chunk.english_translation),
                "raw_audio_available": raw_audio_artifact_id is not None,
            }
            upsert_artifact(
                conn,
                virtual_json_artifact_record(
                    artifact_id=transcript_chunk_artifact_id,
                    user_id=user_id,
                    device_id=device_id,
                    artifact_type="transcript",
                    storage_uri=f"{transcript_json_path.resolve()}#file={row.file_name}&chunk={chunk.chunk_index}",
                    payload=chunk_payload,
                    captured_at_wall=chunk_wall_start_iso,
                    received_at=received_at,
                    normalized_at=chunk_normalized_start_iso,
                    metadata=chunk_metadata,
                    parent_artifact_id=transcript_file_artifact_id,
                    clock_offset_ms=clock_offset_ms,
                    clock_confidence=clock_confidence,
                    processor_name=transcript_processor_name,
                    processor_version=processor_version,
                    schema_version="recorder_audio_transcripts.v2",
                ),
            )
            artifact_count += 1

            confidence = chunk_confidence(chunk.source_type, chunk.transcript)
            primary_source_type = "ambient_audio"
            scene_window_id = stable_id(
                "guardian-recorder-scene-window",
                user_id,
                device_id,
                row.file_name,
                str(chunk.chunk_index),
                processor_version,
            )
            time_alignment_basis = {
                "method": "recorder_filename_timestamp_plus_manual_offset",
                "timezone": timezone_name,
                "recorder_timestamp": row.recorder_timestamp,
                "modified_time": row.modified_time,
                "median_clock_offset_ms": clock_offset_ms,
                "clock_offset_ms": clock_offset_ms,
                "clock_confidence": clock_confidence,
            }
            scene_metadata = {
                "file_name": row.file_name,
                "source_path": str(row.source_path) if row.source_path else None,
                "chunk_index": chunk.chunk_index,
                "chunk_start_seconds": chunk.start_seconds,
                "chunk_end_seconds": chunk.end_seconds,
                "chunk_duration_seconds": chunk.duration_seconds,
                "source_type": chunk.source_type,
                "primary_language_code": chunk.primary_language_code,
                "notes": chunk.notes,
                "translation_note": chunk.translation_note,
                "english_translation": chunk.english_translation,
                "transcript": chunk.transcript,
                "transcript_excerpt": clip_text(chunk.transcript, limit=600),
                "time_alignment_basis": time_alignment_basis,
                "raw_audio_artifact_id": raw_audio_artifact_id,
                "transcript_artifact_id": transcript_chunk_artifact_id,
                "transcript_file_artifact_id": transcript_file_artifact_id,
                "raw_audio_available": raw_audio_artifact_id is not None,
                "builder_version": processor_version,
            }
            upsert_scene_window(
                conn,
                {
                    "id": scene_window_id,
                    "user_id": user_id,
                    "primary_device_id": device_id,
                    "window_type": "device_segment" if is_direct_device_source(primary_source_type) else "sensor_segment",
                    "start_at": chunk_wall_start_iso,
                    "end_at": chunk_wall_end_iso,
                    "source_priority": source_priority_for_type(primary_source_type),
                    "confidence": confidence,
                    "primary_source_type": primary_source_type,
                    "status": "stable",
                    "metadata_json": json_dumps(scene_metadata),
                },
            )
            scene_window_count += 1

            if raw_audio_artifact_id:
                upsert_scene_window_artifact(
                    conn,
                    scene_window_id,
                    raw_audio_artifact_id,
                    role="primary",
                    confidence=confidence,
                )
            upsert_scene_window_artifact(
                conn,
                scene_window_id,
                transcript_chunk_artifact_id,
                role="primary" if raw_audio_artifact_id is None else "supporting",
                confidence=1.0,
            )
            upsert_scene_window_artifact(
                conn,
                scene_window_id,
                transcript_file_artifact_id,
                role="supporting",
                confidence=0.9,
            )

    return {
        "rows": len(rows),
        "artifacts": artifact_count,
        "scene_windows": scene_window_count,
        "missing_raw_audio": raw_audio_missing_count,
    }


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    conn = connect_db(db_path)
    init_db(conn)

    totals = {
        "input_json_files": 0,
        "rows": 0,
        "artifacts": 0,
        "scene_windows": 0,
        "missing_raw_audio": 0,
    }
    clock_confidence = preferred_clock_confidence(args.clock_offset_ms, args.clock_confidence)
    for raw_path in args.input_json:
        input_path = Path(raw_path).expanduser().resolve()
        result = ingest_transcript_json(
            conn=conn,
            transcript_json_path=input_path,
            user_id=args.user_id,
            device_id=args.device_id,
            timezone_name=args.timezone,
            clock_offset_ms=args.clock_offset_ms,
            clock_confidence=clock_confidence,
            processor_version=args.processor_version,
        )
        totals["input_json_files"] += 1
        for key, value in result.items():
            totals[key] += value

    conn.close()
    print(f"db={db_path}")
    print(f"input_json_files={totals['input_json_files']}")
    print(f"rows={totals['rows']}")
    print(f"artifacts={totals['artifacts']}")
    print(f"scene_windows={totals['scene_windows']}")
    print(f"missing_raw_audio={totals['missing_raw_audio']}")
    print(f"clock_confidence={clock_confidence}")
    print(f"clock_offset_ms={args.clock_offset_ms}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
