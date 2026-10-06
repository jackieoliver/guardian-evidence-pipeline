#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import mimetypes
import socket
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from guardian_capture_segments import iso_z, stable_id


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS artifact (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    capture_session_id TEXT NULL,
    parent_artifact_id TEXT NULL,
    artifact_type TEXT NOT NULL,
    mime_type TEXT NULL,
    storage_uri TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    captured_at_wall TEXT NOT NULL,
    captured_at_monotonic_ms INTEGER NULL,
    received_at TEXT NOT NULL,
    normalized_at TEXT NOT NULL,
    clock_offset_ms INTEGER NULL,
    clock_confidence TEXT NOT NULL,
    processor_name TEXT NULL,
    processor_version TEXT NULL,
    schema_version TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    device_id TEXT NULL,
    parent_job_id TEXT NULL,
    job_type TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NULL,
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    queued_at TEXT NOT NULL,
    started_at TEXT NULL,
    finished_at TEXT NULL,
    error_code TEXT NULL,
    error_message TEXT NULL,
    input_json TEXT NOT NULL,
    output_json TEXT NULL
);

CREATE TABLE IF NOT EXISTS processor_run (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    run_number INTEGER NOT NULL,
    worker_name TEXT NOT NULL,
    worker_host TEXT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NULL,
    status TEXT NOT NULL,
    logs_uri TEXT NULL,
    metrics_json TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS event (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    device_id TEXT NULL,
    event_type TEXT NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NULL,
    source_priority INTEGER NOT NULL,
    confidence REAL NOT NULL,
    primary_source_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_artifact (
    event_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    role TEXT NOT NULL,
    confidence REAL NULL,
    PRIMARY KEY (event_id, artifact_id),
    FOREIGN KEY (event_id) REFERENCES event(id),
    FOREIGN KEY (artifact_id) REFERENCES artifact(id)
);

CREATE TABLE IF NOT EXISTS scene_window (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    primary_device_id TEXT NULL,
    window_type TEXT NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    source_priority INTEGER NOT NULL,
    confidence REAL NOT NULL,
    primary_source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scene_window_artifact (
    scene_window_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    role TEXT NOT NULL,
    confidence REAL NULL,
    PRIMARY KEY (scene_window_id, artifact_id),
    FOREIGN KEY (scene_window_id) REFERENCES scene_window(id),
    FOREIGN KEY (artifact_id) REFERENCES artifact(id)
);

CREATE TABLE IF NOT EXISTS candidate_claim (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    scene_window_id TEXT NOT NULL,
    claim_type TEXT NOT NULL,
    value_json TEXT NOT NULL,
    base_confidence REAL NOT NULL,
    quality_score REAL NOT NULL,
    support_score REAL NOT NULL,
    conflict_score REAL NOT NULL,
    final_confidence REAL NOT NULL,
    disposition TEXT NOT NULL,
    produced_by TEXT NOT NULL,
    produced_at TEXT NOT NULL,
    FOREIGN KEY (scene_window_id) REFERENCES scene_window(id)
);

CREATE TABLE IF NOT EXISTS candidate_claim_evidence (
    candidate_claim_id TEXT NOT NULL,
    artifact_id TEXT NULL,
    event_id TEXT NULL,
    evidence_role TEXT NOT NULL,
    confidence REAL NULL,
    notes TEXT NULL,
    PRIMARY KEY (candidate_claim_id, artifact_id, event_id, evidence_role),
    FOREIGN KEY (candidate_claim_id) REFERENCES candidate_claim(id),
    FOREIGN KEY (artifact_id) REFERENCES artifact(id),
    FOREIGN KEY (event_id) REFERENCES event(id)
);

CREATE TABLE IF NOT EXISTS reference_scene (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    primary_device_id TEXT NULL,
    primary_scene_window_id TEXT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    source_priority INTEGER NOT NULL,
    confidence REAL NOT NULL,
    primary_source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY (primary_scene_window_id) REFERENCES scene_window(id)
);

CREATE TABLE IF NOT EXISTS reference_scene_member (
    reference_scene_id TEXT NOT NULL,
    scene_window_id TEXT NOT NULL,
    role TEXT NOT NULL,
    alignment_offset_ms INTEGER NULL,
    confidence REAL NULL,
    PRIMARY KEY (reference_scene_id, scene_window_id),
    FOREIGN KEY (reference_scene_id) REFERENCES reference_scene(id),
    FOREIGN KEY (scene_window_id) REFERENCES scene_window(id)
);

CREATE TABLE IF NOT EXISTS content_span (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    device_id TEXT NULL,
    artifact_id TEXT NULL,
    event_id TEXT NULL,
    scene_window_id TEXT NULL,
    reference_scene_id TEXT NULL,
    span_type TEXT NOT NULL,
    modality TEXT NOT NULL,
    title TEXT NULL,
    text TEXT NOT NULL,
    language TEXT NULL,
    speaker_label TEXT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NULL,
    source_priority INTEGER NULL,
    confidence REAL NOT NULL,
    status TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY (artifact_id) REFERENCES artifact(id),
    FOREIGN KEY (event_id) REFERENCES event(id),
    FOREIGN KEY (scene_window_id) REFERENCES scene_window(id),
    FOREIGN KEY (reference_scene_id) REFERENCES reference_scene(id)
);

CREATE TABLE IF NOT EXISTS person (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS person_voiceprint (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    label TEXT NOT NULL,
    voiceprint TEXT NOT NULL,
    status TEXT NOT NULL,
    source_storage_uri TEXT NULL,
    source_artifact_id TEXT NULL,
    source_start_seconds REAL NULL,
    source_end_seconds REAL NULL,
    enrolled_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY (person_id) REFERENCES person(id)
);

CREATE TABLE IF NOT EXISTS identity_hypothesis (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    person_id TEXT NULL,
    speaker_key TEXT NOT NULL,
    clip_normalized_stem TEXT NOT NULL,
    chunk_index INTEGER NULL,
    start_at TEXT NULL,
    end_at TEXT NULL,
    display_label TEXT NOT NULL,
    basis TEXT NOT NULL,
    confidence REAL NOT NULL,
    disposition TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (person_id) REFERENCES person(id)
);

CREATE TABLE IF NOT EXISTS identity_hypothesis_evidence (
    identity_hypothesis_id TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    evidence_ref TEXT NOT NULL,
    confidence REAL NULL,
    notes TEXT NULL,
    PRIMARY KEY (identity_hypothesis_id, evidence_type, evidence_ref),
    FOREIGN KEY (identity_hypothesis_id) REFERENCES identity_hypothesis(id)
);

CREATE INDEX IF NOT EXISTS idx_artifact_user_normalized_at ON artifact(user_id, normalized_at);
CREATE INDEX IF NOT EXISTS idx_artifact_device_normalized_at ON artifact(device_id, normalized_at);
CREATE INDEX IF NOT EXISTS idx_artifact_parent ON artifact(parent_artifact_id);
CREATE INDEX IF NOT EXISTS idx_artifact_type_normalized_at ON artifact(artifact_type, normalized_at);
CREATE INDEX IF NOT EXISTS idx_job_state_priority_queued_at ON job(state, priority, queued_at);
CREATE INDEX IF NOT EXISTS idx_job_parent ON job(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_event_user_start_at ON event(user_id, start_at);
CREATE INDEX IF NOT EXISTS idx_event_type_start_at ON event(event_type, start_at);
CREATE INDEX IF NOT EXISTS idx_event_device_start_at ON event(device_id, start_at);
CREATE INDEX IF NOT EXISTS idx_scene_window_user_start_at ON scene_window(user_id, start_at);
CREATE INDEX IF NOT EXISTS idx_scene_window_device_start_at ON scene_window(primary_device_id, start_at);
CREATE INDEX IF NOT EXISTS idx_candidate_claim_scene_type ON candidate_claim(scene_window_id, claim_type);
CREATE INDEX IF NOT EXISTS idx_candidate_claim_disposition_confidence ON candidate_claim(disposition, final_confidence);
CREATE INDEX IF NOT EXISTS idx_reference_scene_user_start_at ON reference_scene(user_id, start_at);
CREATE INDEX IF NOT EXISTS idx_reference_scene_device_start_at ON reference_scene(primary_device_id, start_at);
CREATE INDEX IF NOT EXISTS idx_content_span_user_start_at ON content_span(user_id, start_at);
CREATE INDEX IF NOT EXISTS idx_content_span_span_type ON content_span(span_type);
CREATE INDEX IF NOT EXISTS idx_content_span_modality ON content_span(modality);
CREATE INDEX IF NOT EXISTS idx_content_span_scene_window ON content_span(scene_window_id);
CREATE INDEX IF NOT EXISTS idx_content_span_event ON content_span(event_id);
CREATE INDEX IF NOT EXISTS idx_person_user_name ON person(user_id, display_name);
CREATE INDEX IF NOT EXISTS idx_person_voiceprint_user_person ON person_voiceprint(user_id, person_id);
CREATE INDEX IF NOT EXISTS idx_person_voiceprint_user_status ON person_voiceprint(user_id, status);
CREATE INDEX IF NOT EXISTS idx_identity_hypothesis_user_clip ON identity_hypothesis(user_id, clip_normalized_stem);
CREATE INDEX IF NOT EXISTS idx_identity_hypothesis_user_person ON identity_hypothesis(user_id, person_id);
CREATE INDEX IF NOT EXISTS idx_identity_hypothesis_disposition_confidence ON identity_hypothesis(disposition, confidence);

CREATE TABLE IF NOT EXISTS episode (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    day TEXT NOT NULL,
    episode_type TEXT NOT NULL,
    title TEXT NULL,
    search_text TEXT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    confidence REAL NOT NULL,
    status TEXT NOT NULL,
    builder_version INTEGER NOT NULL,
    built_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS episode_event (
    episode_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY (episode_id, event_id),
    FOREIGN KEY (episode_id) REFERENCES episode(id),
    FOREIGN KEY (event_id) REFERENCES event(id)
);

CREATE INDEX IF NOT EXISTS idx_episode_user_day ON episode(user_id, day);
CREATE INDEX IF NOT EXISTS idx_episode_day_type ON episode(day, episode_type);
CREATE INDEX IF NOT EXISTS idx_episode_started_at ON episode(started_at);
"""


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path = db_path.expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def json_loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def guess_mime_type(path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(str(path))
    return mime_type or "application/octet-stream"


def stable_file_artifact_id(artifact_type: str, path: Path, user_id: str, device_id: str) -> str:
    return stable_id("guardian-artifact-file", user_id, device_id, artifact_type, str(path.resolve()))


def upsert_artifact(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO artifact (
            id, user_id, device_id, capture_session_id, parent_artifact_id, artifact_type, mime_type,
            storage_uri, sha256, byte_size, captured_at_wall, captured_at_monotonic_ms, received_at,
            normalized_at, clock_offset_ms, clock_confidence, processor_name, processor_version,
            schema_version, status, metadata_json
        ) VALUES (
            :id, :user_id, :device_id, :capture_session_id, :parent_artifact_id, :artifact_type, :mime_type,
            :storage_uri, :sha256, :byte_size, :captured_at_wall, :captured_at_monotonic_ms, :received_at,
            :normalized_at, :clock_offset_ms, :clock_confidence, :processor_name, :processor_version,
            :schema_version, :status, :metadata_json
        )
        ON CONFLICT(id) DO UPDATE SET
            device_id = excluded.device_id,
            capture_session_id = excluded.capture_session_id,
            parent_artifact_id = excluded.parent_artifact_id,
            artifact_type = excluded.artifact_type,
            mime_type = excluded.mime_type,
            storage_uri = excluded.storage_uri,
            sha256 = excluded.sha256,
            byte_size = excluded.byte_size,
            captured_at_wall = excluded.captured_at_wall,
            captured_at_monotonic_ms = excluded.captured_at_monotonic_ms,
            received_at = excluded.received_at,
            normalized_at = excluded.normalized_at,
            clock_offset_ms = excluded.clock_offset_ms,
            clock_confidence = excluded.clock_confidence,
            processor_name = excluded.processor_name,
            processor_version = excluded.processor_version,
            schema_version = excluded.schema_version,
            status = excluded.status,
            metadata_json = excluded.metadata_json
        """,
        record,
    )
    conn.commit()


def insert_job_if_missing(conn: sqlite3.Connection, record: dict[str, Any]) -> bool:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO job (
            id, user_id, device_id, parent_job_id, job_type, target_type, target_id,
            processor_name, processor_version, state, priority, attempt_count, max_attempts,
            queued_at, started_at, finished_at, error_code, error_message, input_json, output_json
        ) VALUES (
            :id, :user_id, :device_id, :parent_job_id, :job_type, :target_type, :target_id,
            :processor_name, :processor_version, :state, :priority, :attempt_count, :max_attempts,
            :queued_at, :started_at, :finished_at, :error_code, :error_message, :input_json, :output_json
        )
        """,
        record,
    )
    conn.commit()
    return cursor.rowcount > 0


def upsert_event(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO event (
            id, user_id, device_id, event_type, start_at, end_at, source_priority,
            confidence, primary_source_type, payload_json, created_by, created_at
        ) VALUES (
            :id, :user_id, :device_id, :event_type, :start_at, :end_at, :source_priority,
            :confidence, :primary_source_type, :payload_json, :created_by, :created_at
        )
        ON CONFLICT(id) DO UPDATE SET
            confidence = excluded.confidence,
            payload_json = excluded.payload_json,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        """,
        record,
    )
    conn.commit()


def upsert_event_artifact(
    conn: sqlite3.Connection,
    event_id: str,
    artifact_id: str,
    role: str,
    confidence: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO event_artifact (event_id, artifact_id, role, confidence)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(event_id, artifact_id) DO UPDATE SET
            role = excluded.role,
            confidence = excluded.confidence
        """,
        (event_id, artifact_id, role, confidence),
    )
    conn.commit()


def upsert_scene_window(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO scene_window (
            id, user_id, primary_device_id, window_type, start_at, end_at,
            source_priority, confidence, primary_source_type, status, metadata_json
        ) VALUES (
            :id, :user_id, :primary_device_id, :window_type, :start_at, :end_at,
            :source_priority, :confidence, :primary_source_type, :status, :metadata_json
        )
        ON CONFLICT(id) DO UPDATE SET
            primary_device_id = excluded.primary_device_id,
            window_type = excluded.window_type,
            start_at = excluded.start_at,
            end_at = excluded.end_at,
            source_priority = excluded.source_priority,
            confidence = excluded.confidence,
            primary_source_type = excluded.primary_source_type,
            status = excluded.status,
            metadata_json = excluded.metadata_json
        """,
        record,
    )
    conn.commit()


def upsert_scene_window_artifact(
    conn: sqlite3.Connection,
    scene_window_id: str,
    artifact_id: str,
    role: str,
    confidence: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO scene_window_artifact (scene_window_id, artifact_id, role, confidence)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(scene_window_id, artifact_id) DO UPDATE SET
            role = excluded.role,
            confidence = excluded.confidence
        """,
        (scene_window_id, artifact_id, role, confidence),
    )
    conn.commit()


def replace_day_episodes(conn: sqlite3.Connection, user_id: str, day: str) -> None:
    conn.execute(
        """
        DELETE FROM episode_event
        WHERE episode_id IN (
            SELECT id FROM episode WHERE user_id = ? AND day = ?
        )
        """,
        (user_id, day),
    )
    conn.execute("DELETE FROM episode WHERE user_id = ? AND day = ?", (user_id, day))
    conn.commit()


def upsert_episode(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO episode (
            id, user_id, day, episode_type, title, search_text,
            started_at, ended_at, confidence, status, builder_version, built_at, metadata_json
        ) VALUES (
            :id, :user_id, :day, :episode_type, :title, :search_text,
            :started_at, :ended_at, :confidence, :status, :builder_version, :built_at, :metadata_json
        )
        ON CONFLICT(id) DO UPDATE SET
            episode_type = excluded.episode_type,
            title = excluded.title,
            search_text = excluded.search_text,
            started_at = excluded.started_at,
            ended_at = excluded.ended_at,
            confidence = excluded.confidence,
            status = excluded.status,
            builder_version = excluded.builder_version,
            built_at = excluded.built_at,
            metadata_json = excluded.metadata_json
        """,
        record,
    )
    conn.commit()


def insert_episode_event(
    conn: sqlite3.Connection,
    episode_id: str,
    event_id: str,
    ordinal: int,
    role: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO episode_event (episode_id, event_id, ordinal, role)
        VALUES (?, ?, ?, ?)
        """,
        (episode_id, event_id, ordinal, role),
    )
    conn.commit()


def upsert_candidate_claim(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO candidate_claim (
            id, user_id, scene_window_id, claim_type, value_json,
            base_confidence, quality_score, support_score, conflict_score,
            final_confidence, disposition, produced_by, produced_at
        ) VALUES (
            :id, :user_id, :scene_window_id, :claim_type, :value_json,
            :base_confidence, :quality_score, :support_score, :conflict_score,
            :final_confidence, :disposition, :produced_by, :produced_at
        )
        ON CONFLICT(id) DO UPDATE SET
            value_json = excluded.value_json,
            base_confidence = excluded.base_confidence,
            quality_score = excluded.quality_score,
            support_score = excluded.support_score,
            conflict_score = excluded.conflict_score,
            final_confidence = excluded.final_confidence,
            disposition = excluded.disposition,
            produced_by = excluded.produced_by,
            produced_at = excluded.produced_at
        """,
        record,
    )
    conn.commit()


def upsert_candidate_claim_evidence(
    conn: sqlite3.Connection,
    candidate_claim_id: str,
    *,
    artifact_id: str | None,
    event_id: str | None,
    evidence_role: str,
    confidence: float | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO candidate_claim_evidence (
            candidate_claim_id, artifact_id, event_id, evidence_role, confidence, notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (candidate_claim_id, artifact_id, event_id, evidence_role, confidence, notes),
    )
    conn.commit()


def upsert_reference_scene(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO reference_scene (
            id, user_id, primary_device_id, primary_scene_window_id, start_at, end_at,
            source_priority, confidence, primary_source_type, status, metadata_json
        ) VALUES (
            :id, :user_id, :primary_device_id, :primary_scene_window_id, :start_at, :end_at,
            :source_priority, :confidence, :primary_source_type, :status, :metadata_json
        )
        ON CONFLICT(id) DO UPDATE SET
            primary_device_id = excluded.primary_device_id,
            primary_scene_window_id = excluded.primary_scene_window_id,
            start_at = excluded.start_at,
            end_at = excluded.end_at,
            source_priority = excluded.source_priority,
            confidence = excluded.confidence,
            primary_source_type = excluded.primary_source_type,
            status = excluded.status,
            metadata_json = excluded.metadata_json
        """,
        record,
    )
    conn.commit()


def upsert_reference_scene_member(
    conn: sqlite3.Connection,
    *,
    reference_scene_id: str,
    scene_window_id: str,
    role: str,
    alignment_offset_ms: int | None = None,
    confidence: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO reference_scene_member (
            reference_scene_id, scene_window_id, role, alignment_offset_ms, confidence
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(reference_scene_id, scene_window_id) DO UPDATE SET
            role = excluded.role,
            alignment_offset_ms = excluded.alignment_offset_ms,
            confidence = excluded.confidence
        """,
        (reference_scene_id, scene_window_id, role, alignment_offset_ms, confidence),
    )
    conn.commit()


def upsert_content_span(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO content_span (
            id, user_id, device_id, artifact_id, event_id, scene_window_id, reference_scene_id,
            span_type, modality, title, text, language, speaker_label, start_at, end_at,
            source_priority, confidence, status, created_by, created_at, metadata_json
        ) VALUES (
            :id, :user_id, :device_id, :artifact_id, :event_id, :scene_window_id, :reference_scene_id,
            :span_type, :modality, :title, :text, :language, :speaker_label, :start_at, :end_at,
            :source_priority, :confidence, :status, :created_by, :created_at, :metadata_json
        )
        ON CONFLICT(id) DO UPDATE SET
            device_id = excluded.device_id,
            artifact_id = excluded.artifact_id,
            event_id = excluded.event_id,
            scene_window_id = excluded.scene_window_id,
            reference_scene_id = excluded.reference_scene_id,
            span_type = excluded.span_type,
            modality = excluded.modality,
            title = excluded.title,
            text = excluded.text,
            language = excluded.language,
            speaker_label = excluded.speaker_label,
            start_at = excluded.start_at,
            end_at = excluded.end_at,
            source_priority = excluded.source_priority,
            confidence = excluded.confidence,
            status = excluded.status,
            created_by = excluded.created_by,
            created_at = excluded.created_at,
            metadata_json = excluded.metadata_json
        """,
        record,
    )
    conn.commit()


def upsert_person(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO person (
            id, user_id, display_name, status, metadata_json, created_at, updated_at
        ) VALUES (
            :id, :user_id, :display_name, :status, :metadata_json, :created_at, :updated_at
        )
        ON CONFLICT(id) DO UPDATE SET
            display_name = excluded.display_name,
            status = excluded.status,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        record,
    )
    conn.commit()


def upsert_person_voiceprint(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO person_voiceprint (
            id, user_id, person_id, provider, model, label, voiceprint, status,
            source_storage_uri, source_artifact_id, source_start_seconds, source_end_seconds,
            enrolled_at, updated_at, metadata_json
        ) VALUES (
            :id, :user_id, :person_id, :provider, :model, :label, :voiceprint, :status,
            :source_storage_uri, :source_artifact_id, :source_start_seconds, :source_end_seconds,
            :enrolled_at, :updated_at, :metadata_json
        )
        ON CONFLICT(id) DO UPDATE SET
            person_id = excluded.person_id,
            provider = excluded.provider,
            model = excluded.model,
            label = excluded.label,
            voiceprint = excluded.voiceprint,
            status = excluded.status,
            source_storage_uri = excluded.source_storage_uri,
            source_artifact_id = excluded.source_artifact_id,
            source_start_seconds = excluded.source_start_seconds,
            source_end_seconds = excluded.source_end_seconds,
            updated_at = excluded.updated_at,
            metadata_json = excluded.metadata_json
        """,
        record,
    )
    conn.commit()


def update_person_voiceprint_status(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    person_id: str,
    from_status: str | None = "active",
    to_status: str = "superseded",
    except_ids: Iterable[str] | None = None,
    updated_at: str | None = None,
) -> None:
    sql = """
        UPDATE person_voiceprint
        SET status = ?, updated_at = ?
        WHERE user_id = ? AND person_id = ?
    """
    params: list[Any] = [to_status, updated_at or iso_z_now(), user_id, person_id]
    if from_status is not None:
        sql += " AND status = ?"
        params.append(from_status)
    excluded = [value for value in (except_ids or []) if value]
    if excluded:
        placeholders = ",".join("?" for _ in excluded)
        sql += f" AND id NOT IN ({placeholders})"
        params.extend(excluded)
    conn.execute(sql, params)
    conn.commit()


def upsert_identity_hypothesis(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO identity_hypothesis (
            id, user_id, person_id, speaker_key, clip_normalized_stem, chunk_index,
            start_at, end_at, display_label, basis, confidence, disposition,
            payload_json, created_by, created_at
        ) VALUES (
            :id, :user_id, :person_id, :speaker_key, :clip_normalized_stem, :chunk_index,
            :start_at, :end_at, :display_label, :basis, :confidence, :disposition,
            :payload_json, :created_by, :created_at
        )
        ON CONFLICT(id) DO UPDATE SET
            person_id = excluded.person_id,
            display_label = excluded.display_label,
            basis = excluded.basis,
            confidence = excluded.confidence,
            disposition = excluded.disposition,
            payload_json = excluded.payload_json,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        """,
        record,
    )
    conn.commit()


def upsert_identity_hypothesis_evidence(
    conn: sqlite3.Connection,
    identity_hypothesis_id: str,
    *,
    evidence_type: str,
    evidence_ref: str,
    confidence: float | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO identity_hypothesis_evidence (
            identity_hypothesis_id, evidence_type, evidence_ref, confidence, notes
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (identity_hypothesis_id, evidence_type, evidence_ref, confidence, notes),
    )
    conn.commit()


def fetch_events_for_user_device(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    device_id: str | None = None,
    event_type: str | None = None,
    start_at: str | None = None,
    end_at: str | None = None,
    created_by_like: str | None = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM event WHERE user_id = ?"
    params: list[Any] = [user_id]
    if device_id is not None:
        sql += " AND device_id = ?"
        params.append(device_id)
    if event_type is not None:
        sql += " AND event_type = ?"
        params.append(event_type)
    if start_at is not None:
        sql += " AND start_at >= ?"
        params.append(start_at)
    if end_at is not None:
        sql += " AND start_at < ?"
        params.append(end_at)
    if created_by_like is not None:
        sql += " AND created_by LIKE ?"
        params.append(created_by_like)
    sql += " ORDER BY start_at ASC, created_at ASC"
    return conn.execute(sql, params).fetchall()


def list_activity_context_device_days(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    start_at: str | None = None,
    end_at: str | None = None,
    device_ids: Iterable[str] | None = None,
    created_by_like: str = "%interpreter%",
) -> list[tuple[str, str]]:
    sql = """
        SELECT DISTINCT substr(start_at, 1, 10) AS day, device_id
        FROM event
        WHERE user_id = ?
          AND event_type = 'activity_context'
          AND created_by LIKE ?
          AND device_id IS NOT NULL
    """
    params: list[Any] = [user_id, created_by_like]
    if start_at is not None:
        sql += " AND start_at >= ?"
        params.append(start_at)
    if end_at is not None:
        sql += " AND start_at < ?"
        params.append(end_at)
    device_id_list = [value for value in (device_ids or []) if value]
    if device_id_list:
        placeholders = ",".join("?" for _ in device_id_list)
        sql += f" AND device_id IN ({placeholders})"
        params.extend(device_id_list)
    sql += " ORDER BY day ASC, device_id ASC"
    rows = conn.execute(sql, params).fetchall()
    return [(str(row["day"]), str(row["device_id"])) for row in rows if row["day"] and row["device_id"]]


def fetch_scene_windows(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    primary_device_id: str | None = None,
    start_at: str | None = None,
    end_at: str | None = None,
    window_type: str | None = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM scene_window WHERE user_id = ?"
    params: list[Any] = [user_id]
    if primary_device_id is not None:
        sql += " AND primary_device_id = ?"
        params.append(primary_device_id)
    if start_at is not None:
        sql += " AND start_at >= ?"
        params.append(start_at)
    if end_at is not None:
        sql += " AND start_at < ?"
        params.append(end_at)
    if window_type is not None:
        sql += " AND window_type = ?"
        params.append(window_type)
    sql += " ORDER BY start_at ASC, end_at ASC"
    return conn.execute(sql, params).fetchall()


def fetch_content_spans(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    start_at: str | None = None,
    end_at: str | None = None,
    day: str | None = None,
    span_types: Iterable[str] | None = None,
    modalities: Iterable[str] | None = None,
    status: str | None = "active",
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM content_span WHERE user_id = ?"
    params: list[Any] = [user_id]
    if day is not None:
        start_at = start_at or f"{day}T00:00:00Z"
        end_at = end_at or _next_day_start(day)
    if start_at is not None:
        sql += " AND start_at >= ?"
        params.append(start_at)
    if end_at is not None:
        sql += " AND start_at < ?"
        params.append(end_at)
    span_type_list = [value for value in (span_types or []) if value]
    if span_type_list:
        placeholders = ",".join("?" for _ in span_type_list)
        sql += f" AND span_type IN ({placeholders})"
        params.extend(span_type_list)
    modality_list = [value for value in (modalities or []) if value]
    if modality_list:
        placeholders = ",".join("?" for _ in modality_list)
        sql += f" AND modality IN ({placeholders})"
        params.extend(modality_list)
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    sql += " ORDER BY start_at ASC, end_at ASC, id ASC"
    return conn.execute(sql, params).fetchall()


def update_scene_window_metadata(conn: sqlite3.Connection, scene_window_id: str, metadata_json: str) -> None:
    conn.execute(
        """
        UPDATE scene_window
        SET metadata_json = ?
        WHERE id = ?
        """,
        (metadata_json, scene_window_id),
    )
    conn.commit()


def delete_content_spans_for_day(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    day: str,
    created_by_like: str = "content_normalizer_%",
) -> int:
    start_at = f"{day}T00:00:00Z"
    end_at = _next_day_start(day)
    count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM content_span
            WHERE user_id = ?
              AND start_at >= ?
              AND start_at < ?
              AND created_by LIKE ?
            """,
            (user_id, start_at, end_at, created_by_like),
        ).fetchone()[0]
    )
    conn.execute(
        """
        DELETE FROM content_span
        WHERE user_id = ?
          AND start_at >= ?
          AND start_at < ?
          AND created_by LIKE ?
        """,
        (user_id, start_at, end_at, created_by_like),
    )
    conn.commit()
    return count


def delete_derived_scene_bundle(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    device_id: str,
    day: str,
) -> dict[str, int]:
    start_at = f"{day}T00:00:00Z"
    end_at = _next_day_start(day)
    claim_rows = conn.execute(
        """
        SELECT cc.id, cc.scene_window_id
        FROM candidate_claim cc
        JOIN scene_window sw ON sw.id = cc.scene_window_id
        WHERE sw.user_id = ?
          AND sw.primary_device_id = ?
          AND sw.start_at >= ?
          AND sw.start_at < ?
        """,
        (user_id, device_id, start_at, end_at),
    ).fetchall()
    claim_ids = {str(row["id"]) for row in claim_rows if row["id"]}
    scene_window_ids = {str(row["scene_window_id"]) for row in claim_rows if row["scene_window_id"]}

    event_rows = conn.execute(
        """
        SELECT id, payload_json
        FROM event
        WHERE user_id = ?
          AND device_id = ?
          AND event_type = 'fused_interaction'
          AND start_at >= ?
          AND start_at < ?
          AND created_by LIKE 'scene_window_claim_builder_%'
        """,
        (user_id, device_id, start_at, end_at),
    ).fetchall()
    event_ids = {str(row["id"]) for row in event_rows if row["id"]}
    for row in event_rows:
        payload = json_loads(row["payload_json"], default={}) or {}
        scene_window_id = payload.get("scene_window_id")
        if scene_window_id:
            scene_window_ids.add(str(scene_window_id))

    counts = {
        "scene_windows": len(scene_window_ids),
        "candidate_claims": len(claim_ids),
        "fused_events": len(event_ids),
    }
    if claim_ids:
        placeholders = ",".join("?" for _ in claim_ids)
        conn.execute(
            f"DELETE FROM candidate_claim_evidence WHERE candidate_claim_id IN ({placeholders})",
            tuple(claim_ids),
        )
        conn.execute(f"DELETE FROM candidate_claim WHERE id IN ({placeholders})", tuple(claim_ids))
    if event_ids:
        placeholders = ",".join("?" for _ in event_ids)
        conn.execute(f"DELETE FROM event_artifact WHERE event_id IN ({placeholders})", tuple(event_ids))
        conn.execute(f"DELETE FROM event WHERE id IN ({placeholders})", tuple(event_ids))
    if scene_window_ids:
        placeholders = ",".join("?" for _ in scene_window_ids)
        conn.execute(
            f"DELETE FROM scene_window_artifact WHERE scene_window_id IN ({placeholders})",
            tuple(scene_window_ids),
        )
        conn.execute(f"DELETE FROM scene_window WHERE id IN ({placeholders})", tuple(scene_window_ids))
    conn.commit()
    return counts


def delete_reference_scene_bundle(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    day: str,
) -> dict[str, int]:
    start_at = f"{day}T00:00:00Z"
    end_at = _next_day_start(day)
    reference_rows = conn.execute(
        """
        SELECT id
        FROM reference_scene
        WHERE user_id = ?
          AND start_at >= ?
          AND start_at < ?
        """,
        (user_id, start_at, end_at),
    ).fetchall()
    reference_scene_ids = {str(row["id"]) for row in reference_rows if row["id"]}

    event_rows = conn.execute(
        """
        SELECT id
        FROM event
        WHERE user_id = ?
          AND event_type = 'cross_device_reference'
          AND start_at >= ?
          AND start_at < ?
          AND created_by LIKE 'cross_device_aligner_%'
        """,
        (user_id, start_at, end_at),
    ).fetchall()
    event_ids = {str(row["id"]) for row in event_rows if row["id"]}

    counts = {
        "reference_scenes": len(reference_scene_ids),
        "cross_device_events": len(event_ids),
    }
    if event_ids:
        placeholders = ",".join("?" for _ in event_ids)
        conn.execute(f"DELETE FROM event_artifact WHERE event_id IN ({placeholders})", tuple(event_ids))
        conn.execute(f"DELETE FROM event WHERE id IN ({placeholders})", tuple(event_ids))
    if reference_scene_ids:
        placeholders = ",".join("?" for _ in reference_scene_ids)
        conn.execute(
            f"DELETE FROM reference_scene_member WHERE reference_scene_id IN ({placeholders})",
            tuple(reference_scene_ids),
        )
        conn.execute(f"DELETE FROM reference_scene WHERE id IN ({placeholders})", tuple(reference_scene_ids))
    conn.commit()
    return counts


def fetch_artifacts(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    device_id: str | None = None,
    artifact_types: Iterable[str] | None = None,
    start_at: str | None = None,
    end_at: str | None = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM artifact WHERE user_id = ?"
    params: list[Any] = [user_id]
    if device_id is not None:
        sql += " AND device_id = ?"
        params.append(device_id)
    artifact_types_list = [value for value in (artifact_types or []) if value]
    if artifact_types_list:
        placeholders = ",".join("?" for _ in artifact_types_list)
        sql += f" AND artifact_type IN ({placeholders})"
        params.extend(artifact_types_list)
    if start_at is not None:
        sql += " AND normalized_at >= ?"
        params.append(start_at)
    if end_at is not None:
        sql += " AND normalized_at < ?"
        params.append(end_at)
    sql += " ORDER BY normalized_at ASC"
    return conn.execute(sql, params).fetchall()


def fetch_event_artifacts(conn: sqlite3.Connection, event_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            ea.event_id,
            ea.artifact_id,
            ea.role,
            ea.confidence,
            a.storage_uri,
            a.artifact_type,
            a.clock_offset_ms,
            a.clock_confidence,
            a.metadata_json
        FROM event_artifact ea
        JOIN artifact a ON a.id = ea.artifact_id
        WHERE ea.event_id = ?
        ORDER BY CASE ea.role WHEN 'primary' THEN 0 WHEN 'supporting' THEN 1 ELSE 2 END, a.normalized_at ASC
        """,
        (event_id,),
    ).fetchall()


def list_person_voiceprints(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    person_id: str | None = None,
    status: str | None = "active",
) -> list[sqlite3.Row]:
    sql = """
        SELECT
            pv.*,
            p.display_name
        FROM person_voiceprint pv
        JOIN person p ON p.id = pv.person_id
        WHERE pv.user_id = ?
    """
    params: list[Any] = [user_id]
    if person_id is not None:
        sql += " AND pv.person_id = ?"
        params.append(person_id)
    if status is not None:
        sql += " AND pv.status = ?"
        params.append(status)
    sql += " ORDER BY pv.enrolled_at ASC, pv.updated_at ASC"
    return conn.execute(sql, params).fetchall()


def list_people(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    status: str | None = "active",
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM person WHERE user_id = ?"
    params: list[Any] = [user_id]
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    sql += " ORDER BY display_name ASC"
    return conn.execute(sql, params).fetchall()


def fetch_events_for_day(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    day: str,
    event_types: Iterable[str] | None = None,
) -> list[sqlite3.Row]:
    sql = """
        SELECT *
        FROM event
        WHERE user_id = ?
          AND start_at >= ?
          AND start_at < ?
    """
    params: list[Any] = [user_id, f"{day}T00:00:00Z", _next_day_start(day)]
    if event_types:
        types = list(event_types)
        sql += f" AND event_type IN ({','.join('?' for _ in types)})"
        params.extend(types)
    sql += " ORDER BY start_at ASC, end_at ASC, id ASC"
    return conn.execute(sql, params).fetchall()


def list_event_days(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    event_types: Iterable[str] | None = None,
) -> list[str]:
    sql = """
        SELECT DISTINCT substr(start_at, 1, 10) AS day
        FROM event
        WHERE user_id = ?
    """
    params: list[Any] = [user_id]
    if event_types:
        types = list(event_types)
        sql += f" AND event_type IN ({','.join('?' for _ in types)})"
        params.extend(types)
    sql += " ORDER BY day ASC"
    rows = conn.execute(sql, params).fetchall()
    return [row["day"] for row in rows]


def fetch_jobs(
    conn: sqlite3.Connection,
    states: Iterable[str],
    processor_name: str | None = None,
    limit: int = 0,
) -> list[sqlite3.Row]:
    state_list = list(states)
    placeholders = ",".join("?" for _ in state_list)
    sql = f"""
        SELECT *
        FROM job
        WHERE state IN ({placeholders})
    """
    params: list[Any] = list(state_list)
    if processor_name:
        sql += " AND processor_name = ?"
        params.append(processor_name)
    sql += " ORDER BY priority DESC, queued_at ASC"
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM job WHERE id = ?", (job_id,)).fetchone()


def set_job_state(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    state: str,
    started_at: str | None = None,
    finished_at: str | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    output: Any | None = None,
) -> None:
    output_json = json_dumps(output) if output is not None else None
    conn.execute(
        """
        UPDATE job
        SET state = ?,
            started_at = COALESCE(?, started_at),
            finished_at = COALESCE(?, finished_at),
            error_code = ?,
            error_message = ?,
            output_json = CASE WHEN ? IS NULL THEN output_json ELSE ? END
        WHERE id = ?
        """,
        (
            state,
            started_at,
            finished_at,
            error_code,
            error_message,
            output_json,
            output_json,
            job_id,
        ),
    )
    conn.commit()


def start_job_attempt(
    conn: sqlite3.Connection,
    job_id: str,
    worker_name: str,
    worker_host: str | None = None,
) -> tuple[sqlite3.Row, dict[str, Any]]:
    job = get_job(conn, job_id)
    if job is None:
        raise KeyError(f"job not found: {job_id}")

    now = iso_z_now()
    attempt_count = int(job["attempt_count"]) + 1
    conn.execute(
        """
        UPDATE job
        SET state = 'running',
            attempt_count = ?,
            started_at = COALESCE(started_at, ?),
            error_code = NULL,
            error_message = NULL
        WHERE id = ?
        """,
        (attempt_count, now, job_id),
    )
    run_id = stable_id("guardian-processor-run", job_id, str(attempt_count))
    run_record = {
        "id": run_id,
        "job_id": job_id,
        "run_number": attempt_count,
        "worker_name": worker_name,
        "worker_host": worker_host or socket.gethostname(),
        "started_at": now,
        "finished_at": None,
        "status": "running",
        "logs_uri": None,
        "metrics_json": json_dumps({}),
    }
    conn.execute(
        """
        INSERT OR REPLACE INTO processor_run (
            id, job_id, run_number, worker_name, worker_host, started_at,
            finished_at, status, logs_uri, metrics_json
        ) VALUES (
            :id, :job_id, :run_number, :worker_name, :worker_host, :started_at,
            :finished_at, :status, :logs_uri, :metrics_json
        )
        """,
        run_record,
    )
    conn.commit()
    return get_job(conn, job_id), run_record


def finish_processor_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    status: str,
    metrics: dict[str, Any] | None = None,
    logs_uri: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE processor_run
        SET finished_at = ?,
            status = ?,
            logs_uri = ?,
            metrics_json = ?
        WHERE id = ?
        """,
        (iso_z_now(), status, logs_uri, json_dumps(metrics or {}), run_id),
    )
    conn.commit()


def mark_job_done(conn: sqlite3.Connection, job_id: str, output: Any | None = None) -> None:
    set_job_state(conn, job_id, state="done", finished_at=iso_z_now(), output=output)


def mark_job_failed(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    error_code: str,
    error_message: str,
) -> None:
    job = get_job(conn, job_id)
    if job is None:
        raise KeyError(f"job not found: {job_id}")
    next_state = "retrying" if int(job["attempt_count"]) < int(job["max_attempts"]) else "failed"
    set_job_state(
        conn,
        job_id,
        state=next_state,
        finished_at=iso_z_now() if next_state == "failed" else None,
        error_code=error_code,
        error_message=error_message,
    )


def maybe_complete_parent_job(conn: sqlite3.Connection, parent_job_id: str | None) -> None:
    if not parent_job_id:
        return
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN state NOT IN ('done', 'canceled') THEN 1 ELSE 0 END) AS remaining,
            SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed_count
        FROM job
        WHERE parent_job_id = ?
        """,
        (parent_job_id,),
    ).fetchone()
    if row is None:
        return
    remaining = int(row["remaining"] or 0)
    failed_count = int(row["failed_count"] or 0)
    if remaining == 0:
        state = "failed" if failed_count > 0 else "done"
        set_job_state(conn, parent_job_id, state=state, finished_at=iso_z_now())


def iso_z_now() -> str:
    from datetime import datetime, timezone

    return iso_z(datetime.now(timezone.utc))


def _next_day_start(day: str) -> str:
    from datetime import datetime, timedelta, timezone

    current = datetime.fromisoformat(f"{day}T00:00:00+00:00").astimezone(timezone.utc)
    return iso_z(current + timedelta(days=1))


def artifact_record_from_file(
    *,
    artifact_id: str,
    user_id: str,
    device_id: str,
    artifact_type: str,
    file_path: Path,
    captured_at_wall: str,
    received_at: str,
    normalized_at: str,
    metadata: dict[str, Any],
    captured_at_monotonic_ms: int | None = None,
    clock_offset_ms: int | None = None,
    clock_confidence: str = "medium",
    parent_artifact_id: str | None = None,
    processor_name: str | None = None,
    processor_version: str | None = None,
    schema_version: str = "v1",
    status: str = "active",
) -> dict[str, Any]:
    file_path = file_path.expanduser().resolve()
    return {
        "id": artifact_id,
        "user_id": user_id,
        "device_id": device_id,
        "capture_session_id": None,
        "parent_artifact_id": parent_artifact_id,
        "artifact_type": artifact_type,
        "mime_type": guess_mime_type(file_path),
        "storage_uri": str(file_path),
        "sha256": sha256_file(file_path),
        "byte_size": file_path.stat().st_size,
        "captured_at_wall": captured_at_wall,
        "captured_at_monotonic_ms": captured_at_monotonic_ms,
        "received_at": received_at,
        "normalized_at": normalized_at,
        "clock_offset_ms": clock_offset_ms,
        "clock_confidence": clock_confidence,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "schema_version": schema_version,
        "status": status,
        "metadata_json": json_dumps(metadata),
    }


def virtual_json_artifact_record(
    *,
    artifact_id: str,
    user_id: str,
    device_id: str,
    artifact_type: str,
    storage_uri: str,
    payload: Any,
    captured_at_wall: str,
    received_at: str,
    normalized_at: str,
    metadata: dict[str, Any],
    mime_type: str = "application/json",
    parent_artifact_id: str | None = None,
    clock_offset_ms: int | None = None,
    clock_confidence: str = "low",
    processor_name: str | None = None,
    processor_version: str | None = None,
    schema_version: str = "v1",
    status: str = "active",
) -> dict[str, Any]:
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256()
    digest.update(encoded)
    return {
        "id": artifact_id,
        "user_id": user_id,
        "device_id": device_id,
        "capture_session_id": None,
        "parent_artifact_id": parent_artifact_id,
        "artifact_type": artifact_type,
        "mime_type": mime_type,
        "storage_uri": storage_uri,
        "sha256": digest.hexdigest(),
        "byte_size": len(encoded),
        "captured_at_wall": captured_at_wall,
        "captured_at_monotonic_ms": None,
        "received_at": received_at,
        "normalized_at": normalized_at,
        "clock_offset_ms": clock_offset_ms,
        "clock_confidence": clock_confidence,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "schema_version": schema_version,
        "status": status,
        "metadata_json": json_dumps(metadata),
    }


def job_record(
    *,
    job_id: str,
    user_id: str,
    device_id: str | None,
    parent_job_id: str | None,
    job_type: str,
    target_type: str,
    target_id: str | None,
    processor_name: str,
    processor_version: str,
    priority: int,
    max_attempts: int,
    input_data: dict[str, Any],
    queued_at: str | None = None,
) -> dict[str, Any]:
    return {
        "id": job_id,
        "user_id": user_id,
        "device_id": device_id,
        "parent_job_id": parent_job_id,
        "job_type": job_type,
        "target_type": target_type,
        "target_id": target_id,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "state": "queued",
        "priority": priority,
        "attempt_count": 0,
        "max_attempts": max_attempts,
        "queued_at": queued_at or iso_z_now(),
        "started_at": None,
        "finished_at": None,
        "error_code": None,
        "error_message": None,
        "input_json": json_dumps(input_data),
        "output_json": None,
    }


def event_record(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": event["id"],
        "user_id": event["user_id"],
        "device_id": event.get("device_id"),
        "event_type": event["event_type"],
        "start_at": event["start_at"],
        "end_at": event.get("end_at"),
        "source_priority": event["source_priority"],
        "confidence": event["confidence"],
        "primary_source_type": event["primary_source_type"],
        "payload_json": json_dumps(event["payload"]),
        "created_by": event["created_by"],
        "created_at": event["created_at"],
    }


def person_record(
    *,
    person_id: str,
    user_id: str,
    display_name: str,
    metadata: dict[str, Any] | None = None,
    status: str = "active",
    created_at: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    now = iso_z_now()
    return {
        "id": person_id,
        "user_id": user_id,
        "display_name": display_name,
        "status": status,
        "metadata_json": json_dumps(metadata or {}),
        "created_at": created_at or now,
        "updated_at": updated_at or now,
    }


def person_voiceprint_record(
    *,
    voiceprint_id: str,
    user_id: str,
    person_id: str,
    provider: str,
    model: str,
    label: str,
    voiceprint: str,
    source_storage_uri: str | None,
    source_artifact_id: str | None = None,
    source_start_seconds: float | None = None,
    source_end_seconds: float | None = None,
    metadata: dict[str, Any] | None = None,
    status: str = "active",
    enrolled_at: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    now = iso_z_now()
    return {
        "id": voiceprint_id,
        "user_id": user_id,
        "person_id": person_id,
        "provider": provider,
        "model": model,
        "label": label,
        "voiceprint": voiceprint,
        "status": status,
        "source_storage_uri": source_storage_uri,
        "source_artifact_id": source_artifact_id,
        "source_start_seconds": source_start_seconds,
        "source_end_seconds": source_end_seconds,
        "enrolled_at": enrolled_at or now,
        "updated_at": updated_at or now,
        "metadata_json": json_dumps(metadata or {}),
    }


def identity_hypothesis_record(
    *,
    hypothesis_id: str,
    user_id: str,
    speaker_key: str,
    clip_normalized_stem: str,
    display_label: str,
    basis: str,
    confidence: float,
    disposition: str,
    payload: dict[str, Any],
    created_by: str,
    created_at: str,
    person_id: str | None = None,
    chunk_index: int | None = None,
    start_at: str | None = None,
    end_at: str | None = None,
) -> dict[str, Any]:
    return {
        "id": hypothesis_id,
        "user_id": user_id,
        "person_id": person_id,
        "speaker_key": speaker_key,
        "clip_normalized_stem": clip_normalized_stem,
        "chunk_index": chunk_index,
        "start_at": start_at,
        "end_at": end_at,
        "display_label": display_label,
        "basis": basis,
        "confidence": confidence,
        "disposition": disposition,
        "payload_json": json_dumps(payload),
        "created_by": created_by,
        "created_at": created_at,
    }


def episode_record(
    *,
    episode_id: str,
    user_id: str,
    day: str,
    episode_type: str,
    title: str | None,
    search_text: str | None,
    started_at: str,
    ended_at: str,
    confidence: float,
    builder_version: int,
    built_at: str,
    metadata: dict[str, Any],
    status: str = "provisional",
) -> dict[str, Any]:
    return {
        "id": episode_id,
        "user_id": user_id,
        "day": day,
        "episode_type": episode_type,
        "title": title,
        "search_text": search_text,
        "started_at": started_at,
        "ended_at": ended_at,
        "confidence": confidence,
        "status": status,
        "builder_version": builder_version,
        "built_at": built_at,
        "metadata_json": json_dumps(metadata),
    }


def content_span_record(
    *,
    span_id: str,
    user_id: str,
    span_type: str,
    modality: str,
    text: str,
    start_at: str,
    confidence: float,
    created_by: str,
    created_at: str,
    device_id: str | None = None,
    artifact_id: str | None = None,
    event_id: str | None = None,
    scene_window_id: str | None = None,
    reference_scene_id: str | None = None,
    title: str | None = None,
    language: str | None = None,
    speaker_label: str | None = None,
    end_at: str | None = None,
    source_priority: int | None = None,
    status: str = "active",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": span_id,
        "user_id": user_id,
        "device_id": device_id,
        "artifact_id": artifact_id,
        "event_id": event_id,
        "scene_window_id": scene_window_id,
        "reference_scene_id": reference_scene_id,
        "span_type": span_type,
        "modality": modality,
        "title": title,
        "text": text,
        "language": language,
        "speaker_label": speaker_label,
        "start_at": start_at,
        "end_at": end_at,
        "source_priority": source_priority,
        "confidence": confidence,
        "status": status,
        "created_by": created_by,
        "created_at": created_at,
        "metadata_json": json_dumps(metadata or {}),
    }


# =============================================================================
# TODO: VOICEPRINT AUTO-ENROLLMENT SUPPORT
# =============================================================================
# New tables/functions needed:
#
#   voiceprint_candidate:
#     - id, user_id, session_date, speaker_label (e.g. SPEAKER_01)
#     - source_clip, start_seconds, end_seconds, duration_seconds
#     - isolation_score (how cleanly separated from other speakers)
#     - status: pending_enrollment | enrolled | rejected
#     - enrolled_voiceprint_id (FK to person_voiceprint after enrollment)
#
#   Functions:
#     - upsert_voiceprint_candidate()
#     - list_pending_candidates(user_id, min_duration, min_isolation)
#     - promote_candidate_to_voiceprint(candidate_id, label, person_id)
#     - merge_voiceprints(old_id, new_id) — when user confirms two unknowns
#       are the same person
#
# This enables the auto-enrollment pipeline in gopro_ralph_loop.py and
# gopro_pyannote_speaker_pass.py to store unknown speaker segments and
# automatically enroll them when pyannote API credits are available.
# =============================================================================
