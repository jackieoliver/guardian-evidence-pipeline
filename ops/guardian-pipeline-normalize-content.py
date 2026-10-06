#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from guardian_capture_segments import stable_id
from guardian_pipeline_db import (
    connect_db,
    content_span_record,
    fetch_events_for_day,
    fetch_scene_windows,
    init_db,
    iso_z_now,
    json_loads,
    list_event_days,
    upsert_content_span,
)


NORMALIZER_NAME = "content_normalizer"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize canonical content spans from Guardian events and scene windows."
    )
    parser.add_argument("--db", required=True, help="Guardian pipeline SQLite DB path")
    parser.add_argument("--user-id", required=True, help="Guardian user identifier")
    parser.add_argument("--day", action="append", default=[], help="Optional day(s) in YYYY-MM-DD form")
    parser.add_argument(
        "--processor-version",
        default="v1",
        help="Version stamped into created_by and stable content span ids",
    )
    return parser.parse_args()


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = " ".join(value.split()).strip()
        return cleaned or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        parts = [normalize_text(item) for item in value]
        joined = "; ".join(part for part in parts if part)
        return joined or None
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            item_text = normalize_text(item)
            if item_text:
                parts.append(f"{key}: {item_text}")
        joined = "; ".join(parts)
        return joined or None
    return normalize_text(str(value))


def join_parts(parts: Iterable[str | None]) -> str | None:
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = normalize_text(part)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    if not deduped:
        return None
    return "\n".join(deduped)


def build_visual_summary_text(metadata: dict[str, Any]) -> str | None:
    parts = [
        metadata.get("visual_summary"),
        f"Setting: {normalize_text(metadata.get('setting'))}" if metadata.get("setting") else None,
        f"Scene labels: {normalize_text(metadata.get('scene_labels'))}" if metadata.get("scene_labels") else None,
        f"Actions: {normalize_text(metadata.get('actions'))}" if metadata.get("actions") else None,
        f"People present: {normalize_text(metadata.get('people_present'))}" if metadata.get("people_present") else None,
        f"Objects: {normalize_text(metadata.get('notable_objects'))}" if metadata.get("notable_objects") else None,
        f"Visible text: {normalize_text(metadata.get('visible_text'))}" if metadata.get("visible_text") else None,
        f"Timeline events: {normalize_text(metadata.get('timeline_events'))}" if metadata.get("timeline_events") else None,
    ]
    return join_parts(parts)


def build_activity_summary_text(payload: dict[str, Any]) -> str | None:
    observed_facts = payload.get("observed_facts") or []
    facts_text = normalize_text(observed_facts[:4]) if isinstance(observed_facts, list) else None
    parts = [
        payload.get("summary"),
        payload.get("specific_activity"),
        f"Facts: {facts_text}" if facts_text else None,
    ]
    return join_parts(parts)


def reference_scene_map(conn: sqlite3.Connection, user_id: str, day: str) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT rsm.scene_window_id, rsm.reference_scene_id
        FROM reference_scene_member rsm
        JOIN reference_scene rs ON rs.id = rsm.reference_scene_id
        WHERE rs.user_id = ?
          AND rs.start_at >= ?
          AND rs.start_at < ?
        ORDER BY rs.start_at ASC, rsm.reference_scene_id ASC
        """,
        (user_id, f"{day}T07:00:00Z", _next_day_start(day)),
    ).fetchall()
    mapping: dict[str, str] = {}
    for row in rows:
        scene_window_id = str(row["scene_window_id"])
        mapping.setdefault(scene_window_id, str(row["reference_scene_id"]))
    return mapping


def _next_day_start(day: str) -> str:
    """Return UTC timestamp for end of a PDT day (next midnight PDT = 07:00 UTC next day)."""
    from datetime import datetime, timedelta
    next_day = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return f"{next_day}T07:00:00Z"


def replace_day_spans(conn: sqlite3.Connection, user_id: str, day: str, created_by_prefix: str) -> None:
    conn.execute(
        """
        DELETE FROM content_span
        WHERE user_id = ?
          AND start_at >= ?
          AND start_at < ?
          AND created_by LIKE ?
        """,
        (user_id, f"{day}T07:00:00Z", _next_day_start(day), f"{created_by_prefix}%"),
    )
    conn.commit()


def upsert_scene_window_spans(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    ref_scene_id: str | None,
    created_by: str,
    created_at: str,
    processor_version: str,
) -> int:
    metadata = json_loads(row["metadata_json"], default={}) or {}
    scene_window_id = str(row["id"])
    count = 0

    transcript_text = normalize_text(metadata.get("english_translation")) or normalize_text(metadata.get("transcript"))
    # Skip media-only transcripts — song lyrics should not become content spans
    chunk_source_type = str(metadata.get("source_type") or "").strip().lower()
    if transcript_text and chunk_source_type == "media":
        transcript_text = None
    # Skip non-English room/media audio — background foreign music, not user speech
    chunk_lang = str(metadata.get("primary_language_code") or "").strip().lower()
    if transcript_text and chunk_source_type in {"room", "media"} and chunk_lang and not chunk_lang.startswith("en"):
        transcript_text = None
    if transcript_text:
        modality = (
            "camera_audio"
            if row["primary_source_type"] in {"camera_audio", "camera_visual"}
            else "ambient_audio"
            if row["primary_source_type"] == "ambient_audio"
            else str(row["primary_source_type"])
        )
        span_id = stable_id("guardian-content-span", scene_window_id, "transcript", processor_version)
        upsert_content_span(
            conn,
            content_span_record(
                span_id=span_id,
                user_id=row["user_id"],
                device_id=row["primary_device_id"],
                artifact_id=metadata.get("transcript_artifact_id"),
                scene_window_id=scene_window_id,
                reference_scene_id=ref_scene_id,
                span_type="transcript",
                modality=modality,
                title=metadata.get("file_name") or metadata.get("clip_stem"),
                text=transcript_text,
                language=metadata.get("primary_language_code") or metadata.get("language"),
                start_at=row["start_at"],
                end_at=row["end_at"],
                source_priority=row["source_priority"],
                confidence=row["confidence"],
                created_by=created_by,
                created_at=created_at,
                metadata={
                    "source_table": "scene_window",
                    "primary_source_type": row["primary_source_type"],
                    "has_translation": bool(metadata.get("english_translation")),
                    "quality_flags": metadata.get("quality_flags") or [],
                    "source_type": metadata.get("source_type"),
                },
            ),
        )
        count += 1

    visual_text = build_visual_summary_text(metadata)
    if visual_text:
        span_id = stable_id("guardian-content-span", scene_window_id, "visual_summary", processor_version)
        upsert_content_span(
            conn,
            content_span_record(
                span_id=span_id,
                user_id=row["user_id"],
                device_id=row["primary_device_id"],
                artifact_id=metadata.get("visual_artifact_id"),
                scene_window_id=scene_window_id,
                reference_scene_id=ref_scene_id,
                span_type="visual_summary",
                modality="camera_visual",
                title=metadata.get("clip_stem") or metadata.get("recording_group"),
                text=visual_text,
                start_at=row["start_at"],
                end_at=row["end_at"],
                source_priority=row["source_priority"],
                confidence=float(metadata.get("visual_confidence") or row["confidence"]),
                created_by=created_by,
                created_at=created_at,
                metadata={
                    "source_table": "scene_window",
                    "primary_source_type": row["primary_source_type"],
                    "clip_stem": metadata.get("clip_stem"),
                    "recording_group": metadata.get("recording_group"),
                    "setting": metadata.get("setting"),
                    "scene_labels": metadata.get("scene_labels"),
                    "actions": metadata.get("actions"),
                    "people_present": metadata.get("people_present"),
                    "notable_objects": metadata.get("notable_objects"),
                    "visible_text": metadata.get("visible_text"),
                    "timeline_events": metadata.get("timeline_events"),
                },
            ),
        )
        count += 1

    return count


def upsert_event_spans(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    created_by: str,
    created_at: str,
    processor_version: str,
) -> int:
    payload = json_loads(row["payload_json"], default={}) or {}
    count = 0

    if row["event_type"] == "desktop_audio_context":
        transcript = normalize_text(payload.get("transcription"))
        if transcript:
            span_id = stable_id("guardian-content-span", row["id"], "desktop_audio", processor_version)
            upsert_content_span(
                conn,
                content_span_record(
                    span_id=span_id,
                    user_id=row["user_id"],
                    device_id=row["device_id"],
                    event_id=row["id"],
                    span_type="transcript",
                    modality="desktop_audio",
                    title=payload.get("app_name") or payload.get("device_name"),
                    text=transcript,
                    speaker_label=(
                        f"speaker_{payload['speaker_id']}" if payload.get("speaker_id") is not None else None
                    ),
                    start_at=row["start_at"],
                    end_at=row["end_at"],
                    source_priority=row["source_priority"],
                    confidence=row["confidence"],
                    created_by=created_by,
                    created_at=created_at,
                    metadata={
                        "source_table": "event",
                        "event_type": row["event_type"],
                        "device_name": payload.get("device_name"),
                        "app_name": payload.get("app_name"),
                        "machine_id": payload.get("machine_id"),
                    },
                ),
            )
            count += 1

    summary_text = build_activity_summary_text(payload)
    if row["event_type"] == "activity_context" and summary_text:
        span_id = stable_id("guardian-content-span", row["id"], "activity_summary", processor_version)
        upsert_content_span(
            conn,
            content_span_record(
                span_id=span_id,
                user_id=row["user_id"],
                device_id=row["device_id"],
                event_id=row["id"],
                span_type="activity_summary",
                modality=str(row["primary_source_type"]),
                title=payload.get("activity_type") or payload.get("coarse_category"),
                text=summary_text,
                start_at=row["start_at"],
                end_at=row["end_at"],
                source_priority=row["source_priority"],
                confidence=row["confidence"],
                created_by=created_by,
                created_at=created_at,
                metadata={
                    "source_table": "event",
                    "event_type": row["event_type"],
                    "activity_type": payload.get("activity_type"),
                    "coarse_category": payload.get("coarse_category"),
                    "primary_app_name": payload.get("primary_app_name"),
                },
            ),
        )
        count += 1

    return count


def all_days(conn: sqlite3.Connection, user_id: str) -> list[str]:
    event_days = set(list_event_days(conn, user_id=user_id))
    rows = conn.execute(
        """
        SELECT DISTINCT substr(start_at, 1, 10) AS day
        FROM scene_window
        WHERE user_id = ?
        ORDER BY day ASC
        """,
        (user_id,),
    ).fetchall()
    scene_days = {str(row["day"]) for row in rows if row["day"]}
    return sorted(event_days | scene_days)


def normalize_day(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    day: str,
    processor_version: str,
) -> dict[str, int]:
    created_by = f"{NORMALIZER_NAME}_{processor_version}"
    created_at = iso_z_now()
    replace_day_spans(conn, user_id, day, f"{NORMALIZER_NAME}_")
    ref_map = reference_scene_map(conn, user_id, day)
    start_at = f"{day}T07:00:00Z"
    end_at = _next_day_start(day)
    scene_windows = fetch_scene_windows(conn, user_id=user_id, start_at=start_at, end_at=end_at)
    events = fetch_events_for_day(conn, user_id=user_id, day=day, event_types=["desktop_audio_context", "activity_context"])

    scene_span_count = 0
    for row in scene_windows:
        scene_span_count += upsert_scene_window_spans(
            conn,
            row=row,
            ref_scene_id=ref_map.get(str(row["id"])),
            created_by=created_by,
            created_at=created_at,
            processor_version=processor_version,
        )

    event_span_count = 0
    for row in events:
        event_span_count += upsert_event_spans(
            conn,
            row=row,
            created_by=created_by,
            created_at=created_at,
            processor_version=processor_version,
        )

    return {
        "scene_windows": len(scene_windows),
        "events": len(events),
        "content_spans": scene_span_count + event_span_count,
    }


def main() -> int:
    args = parse_args()
    conn = connect_db(Path(args.db))
    init_db(conn)

    days = sorted(set(args.day or all_days(conn, args.user_id)))
    print(f"Normalizing content for {len(days)} day(s)")
    for day in days:
        stats = normalize_day(conn, user_id=args.user_id, day=day, processor_version=args.processor_version)
        print(
            f"  {day}: scene_windows={stats['scene_windows']} events={stats['events']} "
            f"content_spans={stats['content_spans']}"
        )
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
