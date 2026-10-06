#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from guardian_capture_segments import category_for_app, iso_z, stable_id
from guardian_pipeline_db import (
    artifact_record_from_file,
    connect_db,
    event_record,
    init_db,
    iso_z_now,
    stable_file_artifact_id,
    upsert_artifact,
    upsert_event,
    upsert_event_artifact,
    virtual_json_artifact_record,
)
from guardian_source_priority import source_priority_for_type


DIRECT_CLOCK_CONFIDENCE = "medium"
FRAME_EVENT_TYPE = "activity_context"
FRAME_EVENT_CREATED_BY_PREFIX = "codex_segment_interpreter_screenpipe"
FRAME_METADATA_SCHEMA = "screenpipe.frame.v1"
AUDIO_TRANSCRIPT_SCHEMA = "screenpipe.audio_transcription.v1"
UI_EVENT_SCHEMA = "screenpipe.ui_event.v1"


@dataclass(frozen=True)
class ScreenpipeFrame:
    frame_id: int
    timestamp: datetime
    app_name: str | None
    window_name: str | None
    browser_url: str | None
    device_name: str | None
    machine_id: str | None
    snapshot_path: Path | None
    capture_trigger: str | None
    text_source: str | None
    full_text: str | None
    accessibility_text: str | None
    accessibility_tree_json: str | None
    content_hash: int | None
    simhash: int | None
    focused: bool | None
    ocr_text: str | None
    ocr_text_json: str | None
    ocr_engine: str | None


@dataclass(frozen=True)
class ScreenpipeAudio:
    transcription_id: int
    audio_chunk_id: int
    timestamp: datetime
    transcription: str
    device_name: str | None
    is_input_device: bool | None
    transcription_engine: str | None
    start_time: float | None
    end_time: float | None
    speaker_id: int | None
    file_path: Path | None
    machine_id: str | None


@dataclass(frozen=True)
class ScreenpipeUiEvent:
    event_id: int
    timestamp: datetime
    session_id: str | None
    relative_ms: int
    event_type: str
    x: int | None
    y: int | None
    delta_x: int | None
    delta_y: int | None
    button: int | None
    click_count: int | None
    key_code: int | None
    modifiers: int | None
    text_content: str | None
    app_name: str | None
    window_title: str | None
    browser_url: str | None
    element_role: str | None
    element_name: str | None
    frame_id: int | None
    machine_id: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Screenpipe desktop capture data into the canonical Guardian DB."
    )
    parser.add_argument("--db", required=True, help="Guardian pipeline SQLite DB path")
    parser.add_argument(
        "--screenpipe-db",
        default=str(Path.home() / ".screenpipe" / "db.sqlite"),
        help="Screenpipe SQLite path (default: ~/.screenpipe/db.sqlite)",
    )
    parser.add_argument("--user-id", required=True, help="Guardian user identifier")
    parser.add_argument("--device-id", required=True, help="Guardian device identifier for the Screenpipe-backed Linux box")
    parser.add_argument(
        "--machine-id",
        default=None,
        help="Optional Screenpipe machine_id filter. Defaults to all rows in the DB.",
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="Optional inclusive UTC/offset timestamp filter for Screenpipe timestamps",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="Optional exclusive UTC/offset timestamp filter for Screenpipe timestamps",
    )
    parser.add_argument(
        "--frame-gap-seconds",
        type=float,
        default=45.0,
        help="Maximum frame gap to keep neighboring Screenpipe frames in one activity_context event",
    )
    parser.add_argument(
        "--frame-limit",
        type=int,
        default=None,
        help="Optional frame row limit for smoke-testing",
    )
    parser.add_argument(
        "--audio-limit",
        type=int,
        default=None,
        help="Optional audio transcription row limit for smoke-testing",
    )
    parser.add_argument(
        "--ui-event-limit",
        type=int,
        default=None,
        help="Optional UI event row limit for smoke-testing",
    )
    parser.add_argument(
        "--processor-version",
        default="v1",
        help="Version label stamped into Guardian artifacts and events",
    )
    return parser.parse_args()


def parse_timestamp(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        prefix, suffix = text.split(".", 1)
        tz_index = max(suffix.rfind("+"), suffix.rfind("-"))
        if tz_index >= 0:
            frac = suffix[:tz_index]
            tz_part = suffix[tz_index:]
        else:
            frac = suffix
            tz_part = ""
        frac = (frac[:6]).ljust(6, "0")
        text = f"{prefix}.{frac}{tz_part}"
    return datetime.fromisoformat(text).astimezone(UTC)


def parse_optional_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return parse_timestamp(value)


def parse_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_text(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).split()).strip()
    return cleaned or None


def excerpt(value: str | None, limit: int = 220) -> str | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def browser_host(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    return parsed.netloc or None


def screenshot_sample_indices(length: int) -> list[int]:
    if length <= 0:
        return []
    if length == 1:
        return [0]
    if length == 2:
        return [0, 1]
    return sorted({0, length // 2, length - 1})


def connect_screenpipe_db(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_frames(
    conn: sqlite3.Connection,
    *,
    machine_id: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    limit: int | None,
) -> list[ScreenpipeFrame]:
    sql = """
        SELECT
            f.id,
            f.timestamp,
            f.app_name,
            f.window_name,
            f.browser_url,
            f.device_name,
            f.machine_id,
            f.snapshot_path,
            f.capture_trigger,
            f.text_source,
            f.full_text,
            f.accessibility_text,
            f.accessibility_tree_json,
            f.content_hash,
            f.simhash,
            f.focused,
            o.text AS ocr_text,
            o.text_json AS ocr_text_json,
            o.ocr_engine
        FROM frames f
        LEFT JOIN ocr_text o ON o.frame_id = f.id
        WHERE (?1 IS NULL OR f.machine_id = ?1)
          AND (?2 IS NULL OR f.timestamp >= ?2)
          AND (?3 IS NULL OR f.timestamp < ?3)
        ORDER BY f.timestamp ASC
    """
    params: list[Any] = [
        machine_id,
        iso_z(start_time) if start_time else None,
        iso_z(end_time) if end_time else None,
    ]
    if limit is not None:
        sql += "\nLIMIT ?4"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    frames: list[ScreenpipeFrame] = []
    for row in rows:
        snapshot_path = Path(row["snapshot_path"]).expanduser() if row["snapshot_path"] else None
        frames.append(
            ScreenpipeFrame(
                frame_id=int(row["id"]),
                timestamp=parse_timestamp(str(row["timestamp"])),
                app_name=normalize_text(row["app_name"]),
                window_name=normalize_text(row["window_name"]),
                browser_url=normalize_text(row["browser_url"]),
                device_name=normalize_text(row["device_name"]),
                machine_id=normalize_text(row["machine_id"]),
                snapshot_path=snapshot_path if snapshot_path and snapshot_path.exists() else snapshot_path,
                capture_trigger=normalize_text(row["capture_trigger"]),
                text_source=normalize_text(row["text_source"]),
                full_text=normalize_text(row["full_text"]),
                accessibility_text=normalize_text(row["accessibility_text"]),
                accessibility_tree_json=row["accessibility_tree_json"],
                content_hash=parse_optional_int(row["content_hash"]),
                simhash=parse_optional_int(row["simhash"]),
                focused=parse_optional_bool(row["focused"]),
                ocr_text=normalize_text(row["ocr_text"]),
                ocr_text_json=row["ocr_text_json"],
                ocr_engine=normalize_text(row["ocr_engine"]),
            )
        )
    return frames


def fetch_audio_rows(
    conn: sqlite3.Connection,
    *,
    machine_id: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    limit: int | None,
) -> list[ScreenpipeAudio]:
    sql = """
        SELECT
            at.id,
            at.audio_chunk_id,
            at.timestamp,
            at.transcription,
            at.device,
            at.is_input_device,
            at.transcription_engine,
            at.start_time,
            at.end_time,
            at.speaker_id,
            ac.file_path,
            ac.machine_id
        FROM audio_transcriptions at
        JOIN audio_chunks ac ON ac.id = at.audio_chunk_id
        WHERE (?1 IS NULL OR ac.machine_id = ?1)
          AND (?2 IS NULL OR at.timestamp >= ?2)
          AND (?3 IS NULL OR at.timestamp < ?3)
        ORDER BY at.timestamp ASC
    """
    params: list[Any] = [
        machine_id,
        iso_z(start_time) if start_time else None,
        iso_z(end_time) if end_time else None,
    ]
    if limit is not None:
        sql += "\nLIMIT ?4"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    audio_rows: list[ScreenpipeAudio] = []
    for row in rows:
        file_path = Path(row["file_path"]).expanduser() if row["file_path"] else None
        audio_rows.append(
            ScreenpipeAudio(
                transcription_id=int(row["id"]),
                audio_chunk_id=int(row["audio_chunk_id"]),
                timestamp=parse_timestamp(str(row["timestamp"])),
                transcription=normalize_text(row["transcription"]) or "",
                device_name=normalize_text(row["device"]),
                is_input_device=parse_optional_bool(row["is_input_device"]),
                transcription_engine=normalize_text(row["transcription_engine"]),
                start_time=parse_optional_float(row["start_time"]),
                end_time=parse_optional_float(row["end_time"]),
                speaker_id=parse_optional_int(row["speaker_id"]),
                file_path=file_path if file_path and file_path.exists() else file_path,
                machine_id=normalize_text(row["machine_id"]),
            )
        )
    return audio_rows


def fetch_ui_events(
    conn: sqlite3.Connection,
    *,
    machine_id: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    limit: int | None,
) -> list[ScreenpipeUiEvent]:
    sql = """
        SELECT
            id,
            timestamp,
            session_id,
            relative_ms,
            event_type,
            x,
            y,
            delta_x,
            delta_y,
            button,
            click_count,
            key_code,
            modifiers,
            text_content,
            app_name,
            window_title,
            browser_url,
            element_role,
            element_name,
            frame_id,
            machine_id
        FROM ui_events
        WHERE (?1 IS NULL OR machine_id = ?1)
          AND (?2 IS NULL OR timestamp >= ?2)
          AND (?3 IS NULL OR timestamp < ?3)
        ORDER BY timestamp ASC
    """
    params: list[Any] = [
        machine_id,
        iso_z(start_time) if start_time else None,
        iso_z(end_time) if end_time else None,
    ]
    if limit is not None:
        sql += "\nLIMIT ?4"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    events: list[ScreenpipeUiEvent] = []
    for row in rows:
        events.append(
            ScreenpipeUiEvent(
                event_id=int(row["id"]),
                timestamp=parse_timestamp(str(row["timestamp"])),
                session_id=normalize_text(row["session_id"]),
                relative_ms=parse_optional_int(row["relative_ms"]) or 0,
                event_type=normalize_text(row["event_type"]) or "unknown",
                x=parse_optional_int(row["x"]),
                y=parse_optional_int(row["y"]),
                delta_x=parse_optional_int(row["delta_x"]),
                delta_y=parse_optional_int(row["delta_y"]),
                button=parse_optional_int(row["button"]),
                click_count=parse_optional_int(row["click_count"]),
                key_code=parse_optional_int(row["key_code"]),
                modifiers=parse_optional_int(row["modifiers"]),
                text_content=normalize_text(row["text_content"]),
                app_name=normalize_text(row["app_name"]),
                window_title=normalize_text(row["window_title"]),
                browser_url=normalize_text(row["browser_url"]),
                element_role=normalize_text(row["element_role"]),
                element_name=normalize_text(row["element_name"]),
                frame_id=parse_optional_int(row["frame_id"]),
                machine_id=normalize_text(row["machine_id"]),
            )
        )
    return events


GNOME_COMPOSITOR_NAMES = {"gnome-shell", "mutter", "gala"}

# Patterns matched against OCR text to infer the real foreground app when
# screenpipe only reports the GNOME compositor (app_name = "gnome-shell").
# Each tuple is (substring_to_find_in_ocr_lower, canonical_app_name).
# Order matters: first match wins.
_OCR_APP_HINTS: list[tuple[str, str]] = [
    ("visual studio code", "Visual Studio Code"),
    ("cursor", "Cursor"),
    ("— claude", "Claude Code"),
    # Browsers — look for tab-bar / URL-bar artefacts typical of OCR output
    ("chrome://", "Google Chrome"),
    ("chrome available", "Google Chrome"),
    ("newchrome", "Google Chrome"),
    ("new chrome", "Google Chrome"),
    ("google chrome", "Google Chrome"),
    ("firefox", "Firefox"),
    ("mozilla firefox", "Firefox"),
    ("brave browser", "Brave Browser"),
    # Terminals
    ("ptyxis", "Terminal"),
    ("ghostty", "Ghostty"),
    ("terminal\nnew terminal", "Terminal"),
    # Communication
    ("slack |", "Slack"),
    ("discord |", "Discord"),
    ("signal |", "Signal"),
    # Documents
    ("notion.so", "Notion"),
    ("google docs", "Google Docs"),
]


def infer_app_from_ocr(ocr_text: str | None) -> str | None:
    """Try to detect the real foreground app from OCR content.

    On Linux with GNOME, screenpipe often reports app_name='gnome-shell'
    because it captures the compositor window.  The OCR text however contains
    browser tab bars, terminal headers, etc. that reveal the actual app.
    """
    if not ocr_text:
        return None
    lower = ocr_text[:2000].lower()
    for hint, app_name in _OCR_APP_HINTS:
        if hint in lower:
            return app_name
    return None


def resolve_app_name(frame: ScreenpipeFrame) -> str:
    """Return the best available app name for a frame.

    If screenpipe reported a real app (not the GNOME compositor), use that.
    Otherwise try to infer from OCR text.
    """
    raw = (frame.app_name or "").strip().lower()
    if raw and raw not in GNOME_COMPOSITOR_NAMES:
        return frame.app_name or "unknown"
    # Compositor — try OCR, then window_name, then fall back
    inferred = infer_app_from_ocr(frame.ocr_text)
    if inferred:
        return inferred
    # Some non-compositor frames still have useful window_name
    if frame.window_name and frame.window_name.lower() != "main stage":
        return frame.window_name
    return frame.app_name or "unknown"


def should_merge_frames(previous: ScreenpipeFrame, candidate: ScreenpipeFrame, gap_seconds: float) -> bool:
    if previous.device_name != candidate.device_name:
        return False
    if previous.machine_id != candidate.machine_id:
        return False
    if previous.app_name != candidate.app_name:
        return False
    if (candidate.timestamp - previous.timestamp).total_seconds() > gap_seconds:
        return False
    previous_host = browser_host(previous.browser_url)
    candidate_host = browser_host(candidate.browser_url)
    if previous_host and candidate_host:
        return previous_host == candidate_host
    previous_window = previous.window_name or ""
    candidate_window = candidate.window_name or ""
    if previous_window and candidate_window:
        return previous_window == candidate_window
    return True


def group_frame_segments(frames: list[ScreenpipeFrame], gap_seconds: float) -> list[list[ScreenpipeFrame]]:
    if not frames:
        return []
    segments: list[list[ScreenpipeFrame]] = [[frames[0]]]
    for frame in frames[1:]:
        current = segments[-1]
        if should_merge_frames(current[-1], frame, gap_seconds):
            current.append(frame)
        else:
            segments.append([frame])
    return segments


def _best_frame_text(frame: ScreenpipeFrame) -> str | None:
    """Pick the richest text signal from a frame.

    Prefer OCR text (captures actual screen pixels — browser tabs, code, etc.)
    over accessibility text (on Linux/GNOME this is often just the app-launcher
    list rather than the focused window content).  Fall back to full_text and
    then accessibility_text when OCR is unavailable.
    """
    return frame.ocr_text or frame.full_text or frame.accessibility_text


def segment_confidence(segment: list[ScreenpipeFrame]) -> float:
    base = 0.62
    if any(_best_frame_text(frame) for frame in segment):
        base += 0.12
    if any(frame.snapshot_path for frame in segment):
        base += 0.08
    if len(segment) >= 3:
        base += 0.08
    return min(0.95, round(base, 3))


def frame_metadata_payload(frame: ScreenpipeFrame) -> dict[str, Any]:
    return {
        "screenpipe_frame_id": frame.frame_id,
        "timestamp": iso_z(frame.timestamp),
        "app_name": frame.app_name,
        "window_name": frame.window_name,
        "browser_url": frame.browser_url,
        "device_name": frame.device_name,
        "machine_id": frame.machine_id,
        "snapshot_path": str(frame.snapshot_path) if frame.snapshot_path else None,
        "capture_trigger": frame.capture_trigger,
        "text_source": frame.text_source,
        "focused": frame.focused,
        "full_text": frame.full_text,
        "accessibility_text": frame.accessibility_text,
        "accessibility_tree_json": frame.accessibility_tree_json,
        "content_hash": frame.content_hash,
        "simhash": frame.simhash,
        "ocr_text": frame.ocr_text,
        "ocr_text_json": frame.ocr_text_json,
        "ocr_engine": frame.ocr_engine,
    }


def frame_metadata_artifact_id(user_id: str, device_id: str, frame_id: int, processor_version: str) -> str:
    return stable_id(
        "guardian-screenpipe-frame-metadata",
        user_id,
        device_id,
        str(frame_id),
        processor_version,
    )


def audio_transcript_artifact_id(user_id: str, device_id: str, transcription_id: int, processor_version: str) -> str:
    return stable_id(
        "guardian-screenpipe-audio-transcript",
        user_id,
        device_id,
        str(transcription_id),
        processor_version,
    )


def ui_event_artifact_id(user_id: str, device_id: str, ui_event_id: int, processor_version: str) -> str:
    return stable_id(
        "guardian-screenpipe-ui-event",
        user_id,
        device_id,
        str(ui_event_id),
        processor_version,
    )


def build_segment_summary(segment: list[ScreenpipeFrame]) -> tuple[str | None, str | None, list[str]]:
    first = segment[0]
    app_name = resolve_app_name(first)
    window_title = first.window_name
    host = browser_host(first.browser_url)
    text_excerpt = next((excerpt(_best_frame_text(frame), limit=180) for frame in segment if _best_frame_text(frame)), None)
    summary_parts = [app_name]
    if window_title:
        summary_parts.append(window_title)
    elif host:
        summary_parts.append(host)
    summary = " · ".join(summary_parts) if summary_parts else None

    specific_activity = None
    if host:
        specific_activity = f"Browser context on {host}"
    elif window_title:
        specific_activity = window_title
    elif text_excerpt:
        specific_activity = text_excerpt

    observed_facts: list[str] = []
    if first.device_name:
        observed_facts.append(f"screenpipe device: {first.device_name}")
    if host:
        observed_facts.append(f"browser host: {host}")
    triggers = sorted({frame.capture_trigger for frame in segment if frame.capture_trigger})
    if triggers:
        observed_facts.append("capture triggers: " + ", ".join(triggers))
    text_sources = sorted({frame.text_source for frame in segment if frame.text_source})
    if text_sources:
        observed_facts.append("text sources: " + ", ".join(text_sources))
    if text_excerpt:
        observed_facts.append("text excerpt: " + text_excerpt)
    return summary, specific_activity, observed_facts[:6]


def ingest_frames(
    *,
    conn: sqlite3.Connection,
    user_id: str,
    device_id: str,
    screenpipe_db_path: Path,
    frames: list[ScreenpipeFrame],
    gap_seconds: float,
    processor_version: str,
) -> dict[str, int]:
    received_at = iso_z_now()
    metadata_artifact_ids: dict[int, str] = {}
    screenshot_artifact_ids: dict[int, str] = {}
    segments = group_frame_segments(frames, gap_seconds)
    activity_context_count = 0
    screenshot_artifact_count = 0
    metadata_artifact_count = 0

    for frame in frames:
        resolved_app = resolve_app_name(frame)
        metadata_id = frame_metadata_artifact_id(user_id, device_id, frame.frame_id, processor_version)
        metadata_payload = frame_metadata_payload(frame)
        metadata = {
            "source_kind": "screenpipe_frame",
            "platform": "linux",
            "screenpipe_frame_id": frame.frame_id,
            "frontmost_app_name": resolved_app,
            "raw_app_name": frame.app_name,
            "window_title": frame.window_name,
            "browser_url": frame.browser_url,
            "device_name": frame.device_name,
            "machine_id": frame.machine_id,
            "capture_trigger": frame.capture_trigger,
            "text_source": frame.text_source,
            "snapshot_available": frame.snapshot_path is not None and frame.snapshot_path.exists(),
        }
        upsert_artifact(
            conn,
            virtual_json_artifact_record(
                artifact_id=metadata_id,
                user_id=user_id,
                device_id=device_id,
                artifact_type="app_metadata",
                storage_uri=f"{screenpipe_db_path}#frames/{frame.frame_id}",
                payload=metadata_payload,
                captured_at_wall=iso_z(frame.timestamp),
                received_at=received_at,
                normalized_at=iso_z(frame.timestamp),
                metadata=metadata,
                clock_confidence=DIRECT_CLOCK_CONFIDENCE,
                processor_name="screenpipe_frame_ingest",
                processor_version=processor_version,
                schema_version=FRAME_METADATA_SCHEMA,
            ),
        )
        metadata_artifact_ids[frame.frame_id] = metadata_id
        metadata_artifact_count += 1

        if frame.snapshot_path and frame.snapshot_path.exists():
            screenshot_id = stable_file_artifact_id("screenshot", frame.snapshot_path, user_id, device_id)
            screenshot_metadata = {
                "foreground_app": resolved_app,
                "raw_app_name": frame.app_name,
                "window_title": frame.window_name,
                "browser_url": frame.browser_url,
                "platform": "linux",
                "device_name": frame.device_name,
                "machine_id": frame.machine_id,
                "capture_trigger": frame.capture_trigger,
                "text_source": frame.text_source,
                "screenpipe_frame_id": frame.frame_id,
                "linked_screenpipe_metadata_uri": f"{screenpipe_db_path}#frames/{frame.frame_id}",
            }
            upsert_artifact(
                conn,
                artifact_record_from_file(
                    artifact_id=screenshot_id,
                    user_id=user_id,
                    device_id=device_id,
                    artifact_type="screenshot",
                    file_path=frame.snapshot_path,
                    captured_at_wall=iso_z(frame.timestamp),
                    received_at=received_at,
                    normalized_at=iso_z(frame.timestamp),
                    metadata=screenshot_metadata,
                    clock_confidence=DIRECT_CLOCK_CONFIDENCE,
                ),
            )
            screenshot_artifact_ids[frame.frame_id] = screenshot_id
            screenshot_artifact_count += 1

    created_by = f"{FRAME_EVENT_CREATED_BY_PREFIX}_{processor_version}"
    for segment in segments:
        start_at = iso_z(segment[0].timestamp)
        end_at = iso_z(segment[-1].timestamp)
        dominant_app = resolve_app_name(segment[0])
        dominant_category = category_for_app(dominant_app)
        summary, specific_activity, observed_facts = build_segment_summary(segment)
        event_id = stable_id(
            "guardian-screenpipe-activity-context",
            user_id,
            device_id,
            str(segment[0].frame_id),
            str(segment[-1].frame_id),
            processor_version,
        )
        sample_screenshot_ids = [
            screenshot_artifact_ids[segment[index].frame_id]
            for index in screenshot_sample_indices(len(segment))
            if screenshot_artifact_ids.get(segment[index].frame_id)
        ]
        payload = {
            "summary_type": "screenpipe_frame_segment",
            "summary": summary,
            "specific_activity": specific_activity,
            "observed_facts": observed_facts,
            "coarse_category": dominant_category,
            "frontmost_app_name": dominant_app,
            "window_title_examples": [frame.window_name for frame in segment if frame.window_name][:4],
            "capture_count": len(segment),
            "screenpipe_frame_ids": [frame.frame_id for frame in segment],
            "screenpipe_device_name": segment[0].device_name,
            "screenpipe_machine_id": segment[0].machine_id,
            "browser_hosts": sorted({host for host in (browser_host(frame.browser_url) for frame in segment) if host}),
            "capture_triggers": sorted({value for value in (frame.capture_trigger for frame in segment) if value}),
            "text_sources": sorted({value for value in (frame.text_source for frame in segment) if value}),
            "first_snapshot_path": str(segment[0].snapshot_path) if segment[0].snapshot_path else None,
            "last_snapshot_path": str(segment[-1].snapshot_path) if segment[-1].snapshot_path else None,
        }
        event = {
            "id": event_id,
            "user_id": user_id,
            "device_id": device_id,
            "event_type": FRAME_EVENT_TYPE,
            "start_at": start_at,
            "end_at": end_at,
            "source_priority": source_priority_for_type("app_metadata"),
            "confidence": segment_confidence(segment),
            "primary_source_type": "app_metadata",
            "created_by": created_by,
            "created_at": received_at,
            "payload": payload,
        }
        upsert_event(conn, event_record(event))
        for frame in segment:
            upsert_event_artifact(
                conn,
                event_id,
                metadata_artifact_ids[frame.frame_id],
                role="supporting",
                confidence=event["confidence"],
            )
        for screenshot_id in sample_screenshot_ids:
            upsert_event_artifact(conn, event_id, screenshot_id, role="primary", confidence=event["confidence"])
        activity_context_count += 1

    return {
        "frame_rows": len(frames),
        "frame_segments": len(segments),
        "frame_metadata_artifacts": metadata_artifact_count,
        "frame_screenshot_artifacts": screenshot_artifact_count,
        "activity_context_events": activity_context_count,
    }


def audio_event_times(row: ScreenpipeAudio) -> tuple[str, str]:
    start_at = row.timestamp
    end_at = row.timestamp
    if row.start_time is not None:
        start_at = row.timestamp + timedelta(seconds=row.start_time)
    if row.end_time is not None:
        end_at = row.timestamp + timedelta(seconds=row.end_time)
    elif row.start_time is not None:
        end_at = start_at
    return iso_z(start_at), iso_z(end_at)


def ingest_audio(
    *,
    conn: sqlite3.Connection,
    user_id: str,
    device_id: str,
    screenpipe_db_path: Path,
    audio_rows: list[ScreenpipeAudio],
    processor_version: str,
) -> dict[str, int]:
    received_at = iso_z_now()
    raw_audio_artifacts = 0
    transcript_artifacts = 0
    audio_events = 0

    for row in audio_rows:
        start_at, end_at = audio_event_times(row)
        raw_audio_artifact_id: str | None = None
        if row.file_path and row.file_path.exists():
            raw_audio_artifact_id = stable_file_artifact_id("desktop_audio", row.file_path, user_id, device_id)
            upsert_artifact(
                conn,
                artifact_record_from_file(
                    artifact_id=raw_audio_artifact_id,
                    user_id=user_id,
                    device_id=device_id,
                    artifact_type="desktop_audio",
                    file_path=row.file_path,
                    captured_at_wall=start_at,
                    received_at=received_at,
                    normalized_at=start_at,
                    metadata={
                        "source_kind": "screenpipe_audio_chunk",
                        "screenpipe_audio_chunk_id": row.audio_chunk_id,
                        "screenpipe_transcription_id": row.transcription_id,
                        "device_name": row.device_name,
                        "machine_id": row.machine_id,
                        "is_input_device": row.is_input_device,
                        "transcription_engine": row.transcription_engine,
                    },
                    clock_confidence=DIRECT_CLOCK_CONFIDENCE,
                ),
            )
            raw_audio_artifacts += 1

        transcript_id = audio_transcript_artifact_id(user_id, device_id, row.transcription_id, processor_version)
        transcript_payload = {
            "screenpipe_transcription_id": row.transcription_id,
            "screenpipe_audio_chunk_id": row.audio_chunk_id,
            "timestamp": iso_z(row.timestamp),
            "transcription": row.transcription,
            "device_name": row.device_name,
            "is_input_device": row.is_input_device,
            "transcription_engine": row.transcription_engine,
            "start_time": row.start_time,
            "end_time": row.end_time,
            "speaker_id": row.speaker_id,
            "file_path": str(row.file_path) if row.file_path else None,
            "machine_id": row.machine_id,
        }
        upsert_artifact(
            conn,
            virtual_json_artifact_record(
                artifact_id=transcript_id,
                user_id=user_id,
                device_id=device_id,
                artifact_type="transcript",
                storage_uri=f"{screenpipe_db_path}#audio_transcriptions/{row.transcription_id}",
                payload=transcript_payload,
                captured_at_wall=start_at,
                received_at=received_at,
                normalized_at=start_at,
                metadata={
                    "source_kind": "screenpipe_audio_transcription",
                    "screenpipe_transcription_id": row.transcription_id,
                    "screenpipe_audio_chunk_id": row.audio_chunk_id,
                    "device_name": row.device_name,
                    "machine_id": row.machine_id,
                    "is_input_device": row.is_input_device,
                    "speaker_id": row.speaker_id,
                },
                parent_artifact_id=raw_audio_artifact_id,
                clock_confidence=DIRECT_CLOCK_CONFIDENCE,
                processor_name="screenpipe_audio_ingest",
                processor_version=processor_version,
                schema_version=AUDIO_TRANSCRIPT_SCHEMA,
            ),
        )
        transcript_artifacts += 1

        event_id = stable_id(
            "guardian-screenpipe-audio-event",
            user_id,
            device_id,
            str(row.transcription_id),
            processor_version,
        )
        event = {
            "id": event_id,
            "user_id": user_id,
            "device_id": device_id,
            "event_type": "desktop_audio_context",
            "start_at": start_at,
            "end_at": end_at,
            "source_priority": source_priority_for_type("desktop_audio"),
            "confidence": 0.78 if row.transcription else 0.45,
            "primary_source_type": "desktop_audio",
            "created_by": f"screenpipe_audio_ingest_{processor_version}",
            "created_at": received_at,
            "payload": {
                "transcription": row.transcription,
                "summary": excerpt(row.transcription, limit=180),
                "device_name": row.device_name,
                "is_input_device": row.is_input_device,
                "speaker_id": row.speaker_id,
                "transcription_engine": row.transcription_engine,
                "screenpipe_audio_chunk_id": row.audio_chunk_id,
                "screenpipe_transcription_id": row.transcription_id,
                "machine_id": row.machine_id,
            },
        }
        upsert_event(conn, event_record(event))
        if raw_audio_artifact_id:
            upsert_event_artifact(conn, event_id, raw_audio_artifact_id, role="primary", confidence=event["confidence"])
        upsert_event_artifact(conn, event_id, transcript_id, role="supporting", confidence=1.0)
        audio_events += 1

    return {
        "audio_rows": len(audio_rows),
        "desktop_audio_artifacts": raw_audio_artifacts,
        "audio_transcript_artifacts": transcript_artifacts,
        "audio_events": audio_events,
    }


def ingest_ui_events(
    *,
    conn: sqlite3.Connection,
    user_id: str,
    device_id: str,
    screenpipe_db_path: Path,
    ui_events: list[ScreenpipeUiEvent],
    frame_metadata_artifact_ids: dict[int, str],
    processor_version: str,
) -> dict[str, int]:
    received_at = iso_z_now()
    artifact_count = 0
    event_count = 0

    for row in ui_events:
        artifact_id = ui_event_artifact_id(user_id, device_id, row.event_id, processor_version)
        payload = {
            "screenpipe_ui_event_id": row.event_id,
            "timestamp": iso_z(row.timestamp),
            "session_id": row.session_id,
            "relative_ms": row.relative_ms,
            "event_type": row.event_type,
            "x": row.x,
            "y": row.y,
            "delta_x": row.delta_x,
            "delta_y": row.delta_y,
            "button": row.button,
            "click_count": row.click_count,
            "key_code": row.key_code,
            "modifiers": row.modifiers,
            "text_content": row.text_content,
            "app_name": row.app_name,
            "window_title": row.window_title,
            "browser_url": row.browser_url,
            "element_role": row.element_role,
            "element_name": row.element_name,
            "frame_id": row.frame_id,
            "machine_id": row.machine_id,
        }
        upsert_artifact(
            conn,
            virtual_json_artifact_record(
                artifact_id=artifact_id,
                user_id=user_id,
                device_id=device_id,
                artifact_type="app_metadata",
                storage_uri=f"{screenpipe_db_path}#ui_events/{row.event_id}",
                payload=payload,
                captured_at_wall=iso_z(row.timestamp),
                received_at=received_at,
                normalized_at=iso_z(row.timestamp),
                metadata={
                    "source_kind": "screenpipe_ui_event",
                    "screenpipe_ui_event_id": row.event_id,
                    "event_type": row.event_type,
                    "frontmost_app_name": row.app_name,
                    "window_title": row.window_title,
                    "browser_url": row.browser_url,
                    "machine_id": row.machine_id,
                    "frame_id": row.frame_id,
                },
                clock_confidence=DIRECT_CLOCK_CONFIDENCE,
                processor_name="screenpipe_ui_ingest",
                processor_version=processor_version,
                schema_version=UI_EVENT_SCHEMA,
            ),
        )
        artifact_count += 1

        event_id = stable_id(
            "guardian-screenpipe-ui-event",
            user_id,
            device_id,
            str(row.event_id),
            processor_version,
        )
        event = {
            "id": event_id,
            "user_id": user_id,
            "device_id": device_id,
            "event_type": "screen_input_event",
            "start_at": iso_z(row.timestamp),
            "end_at": iso_z(row.timestamp),
            "source_priority": source_priority_for_type("app_metadata"),
            "confidence": 0.74,
            "primary_source_type": "app_metadata",
            "created_by": f"screenpipe_ui_ingest_{processor_version}",
            "created_at": received_at,
            "payload": payload,
        }
        upsert_event(conn, event_record(event))
        upsert_event_artifact(conn, event_id, artifact_id, role="primary", confidence=event["confidence"])
        if row.frame_id is not None and row.frame_id in frame_metadata_artifact_ids:
            upsert_event_artifact(conn, event_id, frame_metadata_artifact_ids[row.frame_id], role="supporting", confidence=0.9)
        event_count += 1

    return {
        "ui_rows": len(ui_events),
        "ui_event_artifacts": artifact_count,
        "ui_events": event_count,
    }


def main() -> int:
    args = parse_args()
    guardian_db_path = Path(args.db).expanduser().resolve()
    screenpipe_db_path = Path(args.screenpipe_db).expanduser().resolve()
    if not screenpipe_db_path.exists():
        raise SystemExit(f"Screenpipe DB not found: {screenpipe_db_path}")

    start_time = parse_optional_timestamp(args.start_time)
    end_time = parse_optional_timestamp(args.end_time)

    screenpipe_conn = connect_screenpipe_db(screenpipe_db_path)
    guardian_conn = connect_db(guardian_db_path)
    init_db(guardian_conn)

    frames = fetch_frames(
        screenpipe_conn,
        machine_id=args.machine_id,
        start_time=start_time,
        end_time=end_time,
        limit=args.frame_limit,
    )
    frame_results = ingest_frames(
        conn=guardian_conn,
        user_id=args.user_id,
        device_id=args.device_id,
        screenpipe_db_path=screenpipe_db_path,
        frames=frames,
        gap_seconds=args.frame_gap_seconds,
        processor_version=args.processor_version,
    )

    audio_rows = fetch_audio_rows(
        screenpipe_conn,
        machine_id=args.machine_id,
        start_time=start_time,
        end_time=end_time,
        limit=args.audio_limit,
    )
    audio_results = ingest_audio(
        conn=guardian_conn,
        user_id=args.user_id,
        device_id=args.device_id,
        screenpipe_db_path=screenpipe_db_path,
        audio_rows=audio_rows,
        processor_version=args.processor_version,
    )

    ui_events = fetch_ui_events(
        screenpipe_conn,
        machine_id=args.machine_id,
        start_time=start_time,
        end_time=end_time,
        limit=args.ui_event_limit,
    )
    frame_metadata_artifact_ids = {
        frame.frame_id: frame_metadata_artifact_id(args.user_id, args.device_id, frame.frame_id, args.processor_version)
        for frame in frames
    }
    ui_results = ingest_ui_events(
        conn=guardian_conn,
        user_id=args.user_id,
        device_id=args.device_id,
        screenpipe_db_path=screenpipe_db_path,
        ui_events=ui_events,
        frame_metadata_artifact_ids=frame_metadata_artifact_ids,
        processor_version=args.processor_version,
    )

    guardian_conn.close()
    screenpipe_conn.close()

    print(f"guardian_db={guardian_db_path}")
    print(f"screenpipe_db={screenpipe_db_path}")
    print(f"screenpipe_machine_id={args.machine_id or 'all'}")
    print(f"start_time={iso_z(start_time) if start_time else 'none'}")
    print(f"end_time={iso_z(end_time) if end_time else 'none'}")
    for key, value in {**frame_results, **audio_results, **ui_results}.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
