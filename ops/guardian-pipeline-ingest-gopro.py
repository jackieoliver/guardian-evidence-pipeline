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

from guardian_capture_segments import iso_z, resolve_processing_path, stable_id
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
class GoProClip:
    clip_stem: str
    recording_group: str
    clip_start_wall: datetime
    duration_seconds: float
    wav_path: Path | None
    mp4_path: Path | None
    clip_metadata: dict[str, Any]


@dataclass(frozen=True)
class TranscriptUnit:
    transcription_unit_id: str
    clip_stem: str
    recording_group: str
    start_seconds: float
    end_seconds: float
    duration_seconds: float
    transcript: str
    language: str | None
    source_path: Path | None
    quality_flags: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest processed GoPro context/transcript/visual outputs into the canonical Guardian DB.")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--device-id", required=True, help="Device identifier for the GoPro")
    parser.add_argument("--context-json", required=True, help="GoPro context JSON")
    parser.add_argument("--transcripts-json", required=True, help="GoPro transcripts JSON")
    parser.add_argument("--visual-json", default=None, help="Optional Gemini visual JSON")
    parser.add_argument(
        "--timezone",
        default=os.environ.get("GUARDIAN_GOPRO_TIMEZONE", "America/Los_Angeles"),
        help="IANA timezone used for naive GoPro clip timestamps",
    )
    parser.add_argument(
        "--clock-offset-ms",
        type=int,
        default=None,
        help="Optional manual offset to subtract from GoPro wall time when computing normalized alignment values.",
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
        help="Version label stamped into GoPro artifacts and scene windows",
    )
    return parser.parse_args()


def parse_local_wall(value: str, timezone_name: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo(timezone_name))


def normalize_time(captured_at_wall: datetime, clock_offset_ms: int | None) -> datetime:
    if clock_offset_ms is None:
        return captured_at_wall
    return captured_at_wall - timedelta(milliseconds=clock_offset_ms)


def preferred_clock_confidence(offset_ms: int | None, explicit: str | None) -> str:
    if explicit:
        return explicit
    return "medium" if offset_ms is not None else "low"


def resolve_path(raw_value: str | None, *, anchors: list[Path]) -> Path | None:
    if not raw_value:
        return None
    resolved = resolve_processing_path(raw_value, anchors=anchors)
    if resolved is not None and resolved.exists():
        return resolved
    candidate = Path(raw_value).expanduser()
    return candidate.resolve() if candidate.exists() else None


def load_context(path: Path, timezone_name: str) -> dict[str, GoProClip]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    anchors = [path.parent, Path.cwd()]
    clips: dict[str, GoProClip] = {}
    for clip in payload.get("clips", []):
        clip_stem = str(clip.get("clip_stem") or "").strip()
        timestamp = str(clip.get("timestamp") or "").strip()
        if not clip_stem or not timestamp:
            continue
        source_paths = clip.get("source_paths") or {}
        clip_start_wall = parse_local_wall(timestamp, timezone_name)
        clips[clip_stem] = GoProClip(
            clip_stem=clip_stem,
            recording_group=str(clip.get("recording_group") or clip_stem),
            clip_start_wall=clip_start_wall,
            duration_seconds=float(clip.get("duration_seconds") or 0.0),
            wav_path=resolve_path(source_paths.get("wav"), anchors=anchors),
            mp4_path=resolve_path(source_paths.get("mp4"), anchors=anchors),
            clip_metadata=clip.get("clip_metadata") or {},
        )
    return clips


def load_transcripts(path: Path) -> list[TranscriptUnit]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    anchors = [path.parent, Path.cwd()]
    units: list[TranscriptUnit] = []
    for row in payload.get("transcripts", []):
        if not isinstance(row, dict):
            continue
        units.append(
            TranscriptUnit(
                transcription_unit_id=str(row.get("transcription_unit_id") or ""),
                clip_stem=str(row.get("clip_stem") or ""),
                recording_group=str(row.get("recording_group") or ""),
                start_seconds=float(row.get("start_seconds") or 0.0),
                end_seconds=float(row.get("end_seconds") or 0.0),
                duration_seconds=float(row.get("duration_seconds") or 0.0),
                transcript=str(row.get("transcript") or "").strip(),
                language=(str(row.get("language")).strip() if row.get("language") else None),
                source_path=resolve_path(row.get("source_path"), anchors=anchors),
                quality_flags=[str(flag) for flag in (row.get("quality_flags") or []) if str(flag).strip()],
            )
        )
    return units


def load_visual_map(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(row.get("transcription_unit_id")): row
        for row in payload.get("records", [])
        if isinstance(row, dict) and row.get("transcription_unit_id")
    }


def clip_text(text: str, limit: int = 500) -> str:
    stripped = text.strip()
    if len(stripped) <= limit:
        return stripped
    return stripped[: limit - 1].rstrip() + "…"


def camera_confidence(transcript: str, visual_record: dict[str, Any] | None) -> float:
    visual_confidence = None
    if visual_record:
        gemini = visual_record.get("gemini") or {}
        value = gemini.get("confidence")
        if value is not None:
            try:
                visual_confidence = float(value)
            except (TypeError, ValueError):
                visual_confidence = None
    if visual_confidence is not None:
        if transcript.strip():
            return max(0.25, min(0.98, round(0.45 + 0.45 * visual_confidence, 3)))
        return max(0.2, min(0.95, round(0.35 + 0.55 * visual_confidence, 3)))
    if len(transcript.strip()) >= 40:
        return 0.62
    if transcript.strip():
        return 0.42
    return 0.2


def ingest(
    *,
    db_path: Path,
    user_id: str,
    device_id: str,
    context_json_path: Path,
    transcripts_json_path: Path,
    visual_json_path: Path | None,
    timezone_name: str,
    clock_offset_ms: int | None,
    clock_confidence: str,
    processor_version: str,
) -> dict[str, int]:
    conn = connect_db(db_path)
    init_db(conn)
    received_at = iso_z_now()

    context = load_context(context_json_path, timezone_name)
    units = load_transcripts(transcripts_json_path)
    visual_map = load_visual_map(visual_json_path)

    artifact_count = 0
    scene_window_count = 0
    missing_clip_count = 0
    raw_audio_artifacts: set[str] = set()
    raw_video_artifacts: set[str] = set()

    for unit in units:
        clip = context.get(unit.clip_stem)
        if clip is None:
            missing_clip_count += 1
            continue

        chunk_wall_start = clip.clip_start_wall + timedelta(seconds=unit.start_seconds)
        chunk_wall_end = clip.clip_start_wall + timedelta(seconds=unit.end_seconds)
        chunk_normalized_start = normalize_time(chunk_wall_start, clock_offset_ms)
        chunk_normalized_end = normalize_time(chunk_wall_end, clock_offset_ms)
        chunk_wall_start_iso = iso_z(chunk_wall_start)
        chunk_wall_end_iso = iso_z(chunk_wall_end)
        chunk_normalized_start_iso = iso_z(chunk_normalized_start)
        visual_record = visual_map.get(unit.transcription_unit_id)

        raw_audio_artifact_id: str | None = None
        raw_video_artifact_id: str | None = None
        if clip.wav_path and clip.wav_path.exists():
            raw_audio_artifact_id = stable_file_artifact_id("camera_audio", clip.wav_path, user_id, device_id)
            if raw_audio_artifact_id not in raw_audio_artifacts:
                upsert_artifact(
                    conn,
                    artifact_record_from_file(
                        artifact_id=raw_audio_artifact_id,
                        user_id=user_id,
                        device_id=device_id,
                        artifact_type="camera_audio",
                        file_path=clip.wav_path,
                        captured_at_wall=iso_z(clip.clip_start_wall),
                        received_at=received_at,
                        normalized_at=iso_z(normalize_time(clip.clip_start_wall, clock_offset_ms)),
                        metadata={
                            "source_kind": "gopro_wav",
                            "clip_stem": clip.clip_stem,
                            "recording_group": clip.recording_group,
                            "duration_seconds": clip.duration_seconds,
                            "clip_metadata": clip.clip_metadata,
                        },
                        clock_offset_ms=clock_offset_ms,
                        clock_confidence=clock_confidence,
                    ),
                )
                raw_audio_artifacts.add(raw_audio_artifact_id)
                artifact_count += 1

        if clip.mp4_path and clip.mp4_path.exists():
            raw_video_artifact_id = stable_file_artifact_id("camera_visual", clip.mp4_path, user_id, device_id)
            if raw_video_artifact_id not in raw_video_artifacts:
                upsert_artifact(
                    conn,
                    artifact_record_from_file(
                        artifact_id=raw_video_artifact_id,
                        user_id=user_id,
                        device_id=device_id,
                        artifact_type="camera_visual",
                        file_path=clip.mp4_path,
                        captured_at_wall=iso_z(clip.clip_start_wall),
                        received_at=received_at,
                        normalized_at=iso_z(normalize_time(clip.clip_start_wall, clock_offset_ms)),
                        metadata={
                            "source_kind": "gopro_mp4",
                            "clip_stem": clip.clip_stem,
                            "recording_group": clip.recording_group,
                            "duration_seconds": clip.duration_seconds,
                            "clip_metadata": clip.clip_metadata,
                        },
                        clock_offset_ms=clock_offset_ms,
                        clock_confidence=clock_confidence,
                    ),
                )
                raw_video_artifacts.add(raw_video_artifact_id)
                artifact_count += 1

        transcript_artifact_id = stable_id(
            "guardian-gopro-transcript-unit",
            user_id,
            device_id,
            unit.transcription_unit_id,
            processor_version,
        )
        transcript_payload = {
            "transcription_unit_id": unit.transcription_unit_id,
            "clip_stem": unit.clip_stem,
            "recording_group": unit.recording_group,
            "start_seconds": unit.start_seconds,
            "end_seconds": unit.end_seconds,
            "duration_seconds": unit.duration_seconds,
            "transcript": unit.transcript,
            "language": unit.language,
            "quality_flags": unit.quality_flags,
        }
        transcript_metadata = {
            "source_kind": "gopro_transcript_unit",
            "transcripts_json_path": str(transcripts_json_path),
            "clip_stem": unit.clip_stem,
            "recording_group": unit.recording_group,
            "has_visual_context": visual_record is not None,
            "quality_flags": unit.quality_flags,
        }
        upsert_artifact(
            conn,
            virtual_json_artifact_record(
                artifact_id=transcript_artifact_id,
                user_id=user_id,
                device_id=device_id,
                artifact_type="transcript",
                storage_uri=f"{transcripts_json_path.resolve()}#unit={unit.transcription_unit_id}",
                payload=transcript_payload,
                captured_at_wall=chunk_wall_start_iso,
                received_at=received_at,
                normalized_at=chunk_normalized_start_iso,
                metadata=transcript_metadata,
                parent_artifact_id=raw_audio_artifact_id,
                clock_offset_ms=clock_offset_ms,
                clock_confidence=clock_confidence,
                processor_name="gopro_episode_transcribe",
                processor_version=processor_version,
                schema_version="gopro_transcripts.v1",
            ),
        )
        artifact_count += 1

        visual_artifact_id: str | None = None
        if visual_record is not None:
            visual_artifact_id = stable_id(
                "guardian-gopro-visual-window",
                user_id,
                device_id,
                unit.transcription_unit_id,
                processor_version,
            )
            upsert_artifact(
                conn,
                virtual_json_artifact_record(
                    artifact_id=visual_artifact_id,
                    user_id=user_id,
                    device_id=device_id,
                    artifact_type="codex_interpretation",
                    storage_uri=f"{visual_json_path.resolve()}#window={visual_record['visual_window_id']}" if visual_json_path else unit.transcription_unit_id,
                    payload=visual_record,
                    captured_at_wall=chunk_wall_start_iso,
                    received_at=received_at,
                    normalized_at=chunk_normalized_start_iso,
                    metadata={
                        "source_kind": "gopro_visual_window",
                        "visual_window_id": visual_record.get("visual_window_id"),
                        "clip_stem": unit.clip_stem,
                        "recording_group": unit.recording_group,
                    },
                    parent_artifact_id=raw_video_artifact_id,
                    clock_offset_ms=clock_offset_ms,
                    clock_confidence=clock_confidence,
                    processor_name="gopro_gemini_visual_pass",
                    processor_version=processor_version,
                    schema_version="gopro_gemini_visual_context.v1",
                ),
            )
            artifact_count += 1

        primary_source_type = "camera_visual" if visual_record is not None else "camera_audio"
        scene_window_id = stable_id(
            "guardian-gopro-scene-window",
            user_id,
            device_id,
            unit.transcription_unit_id,
            processor_version,
        )
        gemini_block = (visual_record or {}).get("gemini") or {}
        time_alignment_basis = {
            "method": "gopro_clip_local_timestamp_plus_manual_offset",
            "timezone": timezone_name,
            "clip_start_local_timestamp": clip.clip_start_wall.isoformat(),
            "clip_stem": clip.clip_stem,
            "median_clock_offset_ms": clock_offset_ms,
            "clock_offset_ms": clock_offset_ms,
            "clock_confidence": clock_confidence,
            "gps_datetime": clip.clip_metadata.get("gps_datetime"),
        }
        scene_metadata = {
            "clip_stem": clip.clip_stem,
            "recording_group": clip.recording_group,
            "source_paths": {
                "wav": str(clip.wav_path) if clip.wav_path else None,
                "mp4": str(clip.mp4_path) if clip.mp4_path else None,
            },
            "clip_metadata": clip.clip_metadata,
            "transcription_unit_id": unit.transcription_unit_id,
            "transcript": unit.transcript,
            "transcript_excerpt": clip_text(unit.transcript, limit=500),
            "language": unit.language,
            "quality_flags": unit.quality_flags,
            "start_seconds": unit.start_seconds,
            "end_seconds": unit.end_seconds,
            "duration_seconds": unit.duration_seconds,
            "visual_summary": gemini_block.get("window_summary"),
            "setting": gemini_block.get("setting"),
            "scene_labels": gemini_block.get("scene_labels"),
            "actions": gemini_block.get("actions"),
            "people_present": gemini_block.get("people_present"),
            "notable_objects": gemini_block.get("notable_objects"),
            "visible_text": gemini_block.get("visible_text"),
            "timeline_events": gemini_block.get("timeline_events"),
            "visual_confidence": gemini_block.get("confidence"),
            "time_alignment_basis": time_alignment_basis,
            "raw_audio_artifact_id": raw_audio_artifact_id,
            "raw_video_artifact_id": raw_video_artifact_id,
            "transcript_artifact_id": transcript_artifact_id,
            "visual_artifact_id": visual_artifact_id,
            "builder_version": processor_version,
        }
        confidence = camera_confidence(unit.transcript, visual_record)
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
            upsert_scene_window_artifact(conn, scene_window_id, raw_audio_artifact_id, role="supporting", confidence=0.8)
        if raw_video_artifact_id:
            upsert_scene_window_artifact(conn, scene_window_id, raw_video_artifact_id, role="supporting", confidence=0.8)
        upsert_scene_window_artifact(
            conn,
            scene_window_id,
            transcript_artifact_id,
            role="primary" if raw_video_artifact_id is None else "supporting",
            confidence=1.0,
        )
        if visual_artifact_id:
            upsert_scene_window_artifact(conn, scene_window_id, visual_artifact_id, role="primary", confidence=confidence)

    conn.close()
    return {
        "clips": len(context),
        "transcript_units": len(units),
        "artifacts": artifact_count,
        "scene_windows": scene_window_count,
        "missing_clips": missing_clip_count,
    }


def main() -> int:
    args = parse_args()
    result = ingest(
        db_path=Path(args.db).expanduser().resolve(),
        user_id=args.user_id,
        device_id=args.device_id,
        context_json_path=Path(args.context_json).expanduser().resolve(),
        transcripts_json_path=Path(args.transcripts_json).expanduser().resolve(),
        visual_json_path=(Path(args.visual_json).expanduser().resolve() if args.visual_json else None),
        timezone_name=args.timezone,
        clock_offset_ms=args.clock_offset_ms,
        clock_confidence=preferred_clock_confidence(args.clock_offset_ms, args.clock_confidence),
        processor_version=args.processor_version,
    )
    print(f"clips={result['clips']}")
    print(f"transcript_units={result['transcript_units']}")
    print(f"artifacts={result['artifacts']}")
    print(f"scene_windows={result['scene_windows']}")
    print(f"missing_clips={result['missing_clips']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

