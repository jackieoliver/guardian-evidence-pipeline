#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from guardian_capture_segments import stable_id
from guardian_pipeline_db import connect_db as connect_pipeline_db
from guardian_pipeline_db import fetch_content_spans, init_db as init_pipeline_db, iso_z_now


MEMORY_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS memory_build (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_hash TEXT NULL,
    status TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    notes_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS day_summary (
    id TEXT PRIMARY KEY,
    day TEXT NOT NULL,
    user_id TEXT NOT NULL,
    build_id TEXT NOT NULL,
    status TEXT NOT NULL,
    completeness REAL NOT NULL,
    biography_md TEXT NULL,
    day_arc TEXT NULL,
    modality_coverage_json TEXT NOT NULL,
    source_counts_json TEXT NOT NULL,
    sidecar_json TEXT NULL,
    notes_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    FOREIGN KEY (build_id) REFERENCES memory_build(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_day_summary_build_day ON day_summary(build_id, day);
CREATE INDEX IF NOT EXISTS idx_day_summary_day ON day_summary(day);
CREATE INDEX IF NOT EXISTS idx_day_summary_status ON day_summary(status);

CREATE TABLE IF NOT EXISTS coverage_span (
    id TEXT PRIMARY KEY,
    day TEXT NOT NULL,
    user_id TEXT NOT NULL,
    build_id TEXT NOT NULL,
    start_at TEXT NULL,
    end_at TEXT NULL,
    span_kind TEXT NOT NULL,
    modality TEXT NULL,
    completeness REAL NOT NULL,
    confidence REAL NOT NULL,
    notes TEXT NULL,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY (build_id) REFERENCES memory_build(id)
);

CREATE INDEX IF NOT EXISTS idx_coverage_span_day ON coverage_span(day);
CREATE INDEX IF NOT EXISTS idx_coverage_span_modality ON coverage_span(modality);

CREATE TABLE IF NOT EXISTS memory_chunk (
    id TEXT PRIMARY KEY,
    day TEXT NULL,
    user_id TEXT NOT NULL,
    build_id TEXT NOT NULL,
    day_summary_id TEXT NULL,
    chunk_type TEXT NOT NULL,
    title TEXT NULL,
    text TEXT NOT NULL,
    start_at TEXT NULL,
    end_at TEXT NULL,
    sort_key INTEGER NOT NULL,
    completeness REAL NOT NULL,
    confidence REAL NOT NULL,
    salience REAL NOT NULL,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY (build_id) REFERENCES memory_build(id),
    FOREIGN KEY (day_summary_id) REFERENCES day_summary(id)
);

CREATE INDEX IF NOT EXISTS idx_memory_chunk_day ON memory_chunk(day);
CREATE INDEX IF NOT EXISTS idx_memory_chunk_type ON memory_chunk(chunk_type);
CREATE INDEX IF NOT EXISTS idx_memory_chunk_build ON memory_chunk(build_id);

CREATE TABLE IF NOT EXISTS chunk_tag (
    chunk_id TEXT NOT NULL,
    tag_type TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (chunk_id, tag_type, tag_value),
    FOREIGN KEY (chunk_id) REFERENCES memory_chunk(id)
);

CREATE INDEX IF NOT EXISTS idx_chunk_tag_lookup ON chunk_tag(tag_type, tag_value);

CREATE TABLE IF NOT EXISTS chunk_ref (
    chunk_id TEXT NOT NULL,
    ref_kind TEXT NOT NULL,
    ref_id TEXT NOT NULL,
    role TEXT NOT NULL,
    weight REAL NULL,
    PRIMARY KEY (chunk_id, ref_kind, ref_id, role),
    FOREIGN KEY (chunk_id) REFERENCES memory_chunk(id)
);

CREATE INDEX IF NOT EXISTS idx_chunk_ref_lookup ON chunk_ref(ref_kind, ref_id);

CREATE VIRTUAL TABLE IF NOT EXISTS memory_chunk_fts USING fts5(
    chunk_id UNINDEXED,
    title,
    text
);
"""


def connect_memory_db(path: Path) -> sqlite3.Connection:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_memory_db(conn: sqlite3.Connection) -> None:
    conn.executescript(MEMORY_SCHEMA_SQL)
    conn.commit()


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def load_text(path: Path) -> str | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    return text or None


def strip_frontmatter(markdown: str | None) -> str | None:
    if not markdown:
        return None
    text = markdown.strip()
    if not text.startswith("---"):
        return text
    parts = text.split("\n---", 1)
    if len(parts) != 2:
        return text
    body = parts[1].lstrip()
    if body.startswith("\n"):
        body = body[1:]
    return body.strip() or None


def parse_clock_fragment(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if ":" not in text:
        text = f"{text}:00"
    try:
        hours, minutes = text.split(":", 1)
        return int(hours), int(minutes)
    except ValueError:
        return None


def block_range(day: str, block: dict[str, Any]) -> tuple[str | None, str | None]:
    start_fragment = block.get("start_utc") or block.get("start_hour")
    end_fragment = block.get("end_utc") or block.get("end_hour")
    start_parts = parse_clock_fragment(start_fragment)
    end_parts = parse_clock_fragment(end_fragment)
    if start_parts is None or end_parts is None:
        return None, None
    base = datetime.fromisoformat(f"{day}T07:00:00+00:00").astimezone(timezone.utc)
    start_at = base.replace(hour=start_parts[0], minute=start_parts[1])
    end_at = base.replace(hour=end_parts[0], minute=end_parts[1])
    if end_at <= start_at:
        end_at += timedelta(days=1)
    return (
        start_at.isoformat().replace("+00:00", "Z"),
        end_at.isoformat().replace("+00:00", "Z"),
    )


def normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split()).strip()
    return cleaned or None


def normalize_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = normalize_string(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def summarize_counts(rows: list[sqlite3.Row], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row[key] or "")
        counts[value] = counts.get(value, 0) + 1
    return counts


def active_build_id(conn: sqlite3.Connection, user_id: str) -> str | None:
    row = conn.execute(
        """
        SELECT id
        FROM memory_build
        WHERE user_id = ? AND status = 'active'
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    return str(row["id"]) if row else None


def supersede_active_builds(conn: sqlite3.Connection, user_id: str) -> None:
    conn.execute(
        "UPDATE memory_build SET status = 'superseded' WHERE user_id = ? AND status = 'active'",
        (user_id,),
    )
    conn.commit()


def insert_fts_row(conn: sqlite3.Connection, chunk_id: str, title: str | None, text: str) -> None:
    conn.execute(
        "INSERT INTO memory_chunk_fts (chunk_id, title, text) VALUES (?, ?, ?)",
        (chunk_id, title or "", text),
    )


def add_tag(conn: sqlite3.Connection, chunk_id: str, tag_type: str, tag_value: str | None) -> None:
    cleaned = normalize_string(tag_value)
    if not cleaned:
        return
    conn.execute(
        "INSERT OR IGNORE INTO chunk_tag (chunk_id, tag_type, tag_value) VALUES (?, ?, ?)",
        (chunk_id, tag_type, cleaned),
    )


def add_ref(conn: sqlite3.Connection, chunk_id: str, ref_kind: str, ref_id: str | None, role: str, weight: float | None = None) -> None:
    cleaned = normalize_string(ref_id)
    if not cleaned:
        return
    conn.execute(
        "INSERT OR IGNORE INTO chunk_ref (chunk_id, ref_kind, ref_id, role, weight) VALUES (?, ?, ?, ?, ?)",
        (chunk_id, ref_kind, cleaned, role, weight),
    )


def insert_memory_chunk(
    conn: sqlite3.Connection,
    *,
    chunk_id: str,
    day: str | None,
    user_id: str,
    build_id: str,
    day_summary_id: str | None,
    chunk_type: str,
    title: str | None,
    text: str,
    start_at: str | None,
    end_at: str | None,
    sort_key: int,
    completeness: float,
    confidence: float,
    salience: float,
    metadata: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO memory_chunk (
            id, day, user_id, build_id, day_summary_id, chunk_type, title, text,
            start_at, end_at, sort_key, completeness, confidence, salience, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            chunk_id,
            day,
            user_id,
            build_id,
            day_summary_id,
            chunk_type,
            title,
            text,
            start_at,
            end_at,
            sort_key,
            completeness,
            confidence,
            salience,
            json_dumps(metadata or {}),
        ),
    )
    insert_fts_row(conn, chunk_id, title, text)


def resolve_day_status(
    *,
    biography_text: str | None,
    sidecar: dict[str, Any] | None,
    incomplete_note: str | None,
    content_span_count: int,
) -> tuple[str, float]:
    if incomplete_note:
        return "incomplete", 0.6
    if biography_text and sidecar:
        return "complete", 0.95
    if sidecar or biography_text:
        return "partial", 0.55
    if content_span_count > 0:
        return "evidence_only", 0.35
    return "empty", 0.0


def all_days(pipeline_conn: sqlite3.Connection, biography_dir: Path, user_id: str) -> list[str]:
    db_days = [
        str(row["day"])
        for row in pipeline_conn.execute(
            "SELECT DISTINCT date(start_at, '-7 hours') AS day FROM content_span WHERE user_id = ? ORDER BY day",
            (user_id,),
        ).fetchall()
        if row["day"]
    ]
    fs_days = [path.name for path in biography_dir.iterdir() if path.is_dir()]
    return sorted(set(db_days) | set(fs_days))


def build_day(
    *,
    memory_conn: sqlite3.Connection,
    pipeline_conn: sqlite3.Connection,
    build_id: str,
    user_id: str,
    day: str,
    biography_dir: Path,
    generated_at: str,
) -> None:
    day_dir = biography_dir / day
    biography_raw = load_text(day_dir / "biography.md")
    biography_text = strip_frontmatter(biography_raw)
    sidecar = load_json(day_dir / "sidecar.json")
    incomplete_note = load_text(day_dir / "INCOMPLETE.md")

    content_spans = fetch_content_spans(pipeline_conn, user_id=user_id, day=day)
    spans = [dict(row) for row in content_spans]
    counts_by_modality = summarize_counts(content_spans, "modality")
    counts_by_type = summarize_counts(content_spans, "span_type")

    status, completeness = resolve_day_status(
        biography_text=biography_text,
        sidecar=sidecar,
        incomplete_note=incomplete_note,
        content_span_count=len(spans),
    )

    modality_coverage = {
        "modalities": counts_by_modality,
        "present_modalities": sorted([key for key, value in counts_by_modality.items() if value > 0]),
        "has_biography": bool(biography_text),
        "has_sidecar": sidecar is not None,
        "incomplete_note": incomplete_note,
    }
    source_counts = {
        "content_spans_total": len(spans),
        "content_spans_by_type": counts_by_type,
        "content_spans_by_modality": counts_by_modality,
    }

    day_summary_id = stable_id("guardian-memory-day-summary", user_id, build_id, day)
    day_arc = normalize_string((sidecar or {}).get("day_arc"))
    memory_conn.execute(
        """
        INSERT INTO day_summary (
            id, day, user_id, build_id, status, completeness, biography_md, day_arc,
            modality_coverage_json, source_counts_json, sidecar_json, notes_json, generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            day_summary_id,
            day,
            user_id,
            build_id,
            status,
            completeness,
            biography_text,
            day_arc,
            json_dumps(modality_coverage),
            json_dumps(source_counts),
            json_dumps(sidecar) if sidecar is not None else None,
            json_dumps(
                {
                    "biography_path": str(day_dir / "biography.md") if (day_dir / "biography.md").exists() else None,
                    "sidecar_path": str(day_dir / "sidecar.json") if (day_dir / "sidecar.json").exists() else None,
                    "incomplete_note_path": str(day_dir / "INCOMPLETE.md") if (day_dir / "INCOMPLETE.md").exists() else None,
                    "incomplete_note": incomplete_note,
                }
            ),
            generated_at,
        ),
    )

    day_start = f"{day}T07:00:00Z"
    day_end = (datetime.fromisoformat(f"{day}T07:00:00+00:00").astimezone(timezone.utc) + timedelta(days=1)).isoformat().replace("+00:00", "Z")

    for modality, count in sorted(counts_by_modality.items()):
        coverage_id = stable_id("guardian-memory-coverage", build_id, day, modality, "day_modality")
        memory_conn.execute(
            """
            INSERT INTO coverage_span (
                id, day, user_id, build_id, start_at, end_at, span_kind, modality,
                completeness, confidence, notes, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                coverage_id,
                day,
                user_id,
                build_id,
                day_start,
                day_end,
                "day_modality",
                modality,
                completeness,
                0.95,
                f"{count} normalized content spans",
                json_dumps(
                    {
                        "content_span_count": count,
                        "counts_by_type": {
                            span_type: value
                            for span_type, value in counts_by_type.items()
                            if span_type and any(
                                str(row.get("modality") or "") == modality and str(row.get("span_type") or "") == span_type
                                for row in spans
                            )
                        },
                    }
                ),
            ),
        )

    sidecar_blocks = (sidecar or {}).get("time_blocks") or []
    for index, block in enumerate(sidecar_blocks):
        if not isinstance(block, dict):
            continue
        start_at, end_at = block_range(day, block)
        title = normalize_string(block.get("primary_activity") or block.get("label") or block.get("source"))
        description = normalize_string(block.get("description"))
        coverage_id = stable_id("guardian-memory-coverage", build_id, day, "time_block", str(index))
        memory_conn.execute(
            """
            INSERT INTO coverage_span (
                id, day, user_id, build_id, start_at, end_at, span_kind, modality,
                completeness, confidence, notes, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                coverage_id,
                day,
                user_id,
                build_id,
                start_at,
                end_at,
                "time_block",
                title,
                completeness,
                0.7,
                description,
                json_dumps(block),
            ),
        )

    topics = normalize_list((sidecar or {}).get("topics"))
    tools = normalize_list((sidecar or {}).get("tools_used"))
    people_rows = [row for row in ((sidecar or {}).get("people") or []) if isinstance(row, dict)]
    activity_map = (sidecar or {}).get("activities") or (sidecar or {}).get("activities_hours") or {}

    if biography_text:
        chunk_id = stable_id("guardian-memory-chunk", build_id, day, "biography")
        insert_memory_chunk(
            memory_conn,
            chunk_id=chunk_id,
            day=day,
            user_id=user_id,
            build_id=build_id,
            day_summary_id=day_summary_id,
            chunk_type="biography",
            title=f"{day} biography",
            text=biography_text,
            start_at=day_start,
            end_at=day_end,
            sort_key=0,
            completeness=completeness,
            confidence=0.92,
            salience=1.0,
            metadata={"status": status},
        )
        add_ref(memory_conn, chunk_id, "day_summary", day_summary_id, "summary", 1.0)
        for topic in topics:
            add_tag(memory_conn, chunk_id, "topic", topic)
        for tool in tools:
            add_tag(memory_conn, chunk_id, "tool", tool)
        for person in people_rows:
            add_tag(memory_conn, chunk_id, "person", person.get("name"))

    if day_arc:
        chunk_id = stable_id("guardian-memory-chunk", build_id, day, "day_arc")
        insert_memory_chunk(
            memory_conn,
            chunk_id=chunk_id,
            day=day,
            user_id=user_id,
            build_id=build_id,
            day_summary_id=day_summary_id,
            chunk_type="day_arc",
            title=f"{day} arc",
            text=day_arc,
            start_at=day_start,
            end_at=day_end,
            sort_key=10,
            completeness=completeness,
            confidence=0.88,
            salience=0.95,
            metadata={},
        )
        add_ref(memory_conn, chunk_id, "day_summary", day_summary_id, "summary", 1.0)

    for index, block in enumerate(sidecar_blocks):
        if not isinstance(block, dict):
            continue
        text = normalize_string(block.get("description"))
        if not text:
            continue
        start_at, end_at = block_range(day, block)
        title = normalize_string(block.get("primary_activity") or block.get("label") or f"time block {index + 1}")
        chunk_id = stable_id("guardian-memory-chunk", build_id, day, "time_block", str(index))
        insert_memory_chunk(
            memory_conn,
            chunk_id=chunk_id,
            day=day,
            user_id=user_id,
            build_id=build_id,
            day_summary_id=day_summary_id,
            chunk_type="time_block",
            title=title,
            text=text,
            start_at=start_at,
            end_at=end_at,
            sort_key=100 + index,
            completeness=completeness,
            confidence=0.8,
            salience=0.85,
            metadata=block,
        )
        add_ref(memory_conn, chunk_id, "day_summary", day_summary_id, "summary", 1.0)
        add_tag(memory_conn, chunk_id, "activity", block.get("primary_activity") or block.get("label"))

    def emit_simple_chunks(values: list[Any], chunk_type: str, start_sort: int, *, tag_type: str | None = None, title_prefix: str | None = None) -> None:
        for index, value in enumerate(values):
            text = None
            title = None
            metadata: dict[str, Any] = {}
            if isinstance(value, dict):
                title = normalize_string(value.get("name") or value.get("title") or value.get("label"))
                text = normalize_string(value.get("context") or value.get("description") or value.get("value"))
                metadata = value
            else:
                text = normalize_string(value)
            if not text and not title:
                continue
            if not text:
                text = title or ""
            if not title and title_prefix:
                title = title_prefix
            chunk_id = stable_id("guardian-memory-chunk", build_id, day, chunk_type, str(index))
            insert_memory_chunk(
                memory_conn,
                chunk_id=chunk_id,
                day=day,
                user_id=user_id,
                build_id=build_id,
                day_summary_id=day_summary_id,
                chunk_type=chunk_type,
                title=title,
                text=text,
                start_at=None,
                end_at=None,
                sort_key=start_sort + index,
                completeness=completeness,
                confidence=0.78,
                salience=0.7,
                metadata=metadata,
            )
            add_ref(memory_conn, chunk_id, "day_summary", day_summary_id, "summary", 1.0)
            if tag_type:
                add_tag(memory_conn, chunk_id, tag_type, title or text)

    emit_simple_chunks((sidecar or {}).get("decisions_observed") or [], "decision", 300)
    emit_simple_chunks((sidecar or {}).get("work_patterns") or [], "work_pattern", 400)
    emit_simple_chunks((sidecar or {}).get("notable_moments") or [], "notable_moment", 500)
    emit_simple_chunks((sidecar or {}).get("emotional_signals") or [], "emotional_signal", 600)
    emit_simple_chunks(people_rows, "person_context", 700, tag_type="person", title_prefix="person")
    emit_simple_chunks(topics, "topic", 800, tag_type="topic", title_prefix="topic")
    emit_simple_chunks(tools, "tool_used", 900, tag_type="tool", title_prefix="tool")

    # Location chunks from GPS + reverse geocoding
    location_items = (sidecar or {}).get("locations") or []
    emit_simple_chunks(location_items, "location", 950, tag_type="location", title_prefix="location")

    if isinstance(activity_map, dict):
        for index, (activity, hours) in enumerate(sorted(activity_map.items())):
            text = f"{activity}: {hours} hours"
            chunk_id = stable_id("guardian-memory-chunk", build_id, day, "activity_total", activity)
            insert_memory_chunk(
                memory_conn,
                chunk_id=chunk_id,
                day=day,
                user_id=user_id,
                build_id=build_id,
                day_summary_id=day_summary_id,
                chunk_type="activity_total",
                title=activity,
                text=text,
                start_at=None,
                end_at=None,
                sort_key=1000 + index,
                completeness=completeness,
                confidence=0.82,
                salience=0.72,
                metadata={"hours": hours},
            )
            add_ref(memory_conn, chunk_id, "day_summary", day_summary_id, "summary", 1.0)
            add_tag(memory_conn, chunk_id, "activity", activity)

    for index, row in enumerate(spans):
        chunk_type = f"content_{row['span_type']}"
        chunk_id = stable_id("guardian-memory-chunk", build_id, row["id"])
        insert_memory_chunk(
            memory_conn,
            chunk_id=chunk_id,
            day=day,
            user_id=user_id,
            build_id=build_id,
            day_summary_id=day_summary_id,
            chunk_type=chunk_type,
            title=normalize_string(row.get("title")) or f"{row['modality']} {row['span_type']}",
            text=normalize_string(row.get("text")) or "",
            start_at=row.get("start_at"),
            end_at=row.get("end_at"),
            sort_key=2000 + index,
            completeness=1.0 if status == "complete" else completeness,
            confidence=float(row.get("confidence") or 0.5),
            salience=0.6 if row.get("span_type") == "transcript" else 0.78,
            metadata={
                "content_span_id": row["id"],
                "modality": row["modality"],
                "span_type": row["span_type"],
                "speaker_label": row.get("speaker_label"),
            },
        )
        add_ref(memory_conn, chunk_id, "content_span", row["id"], "source", 1.0)
        add_ref(memory_conn, chunk_id, "artifact", row.get("artifact_id"), "source", 0.95)
        add_ref(memory_conn, chunk_id, "event", row.get("event_id"), "source", 0.95)
        add_ref(memory_conn, chunk_id, "scene_window", row.get("scene_window_id"), "source", 0.95)
        add_ref(memory_conn, chunk_id, "reference_scene", row.get("reference_scene_id"), "source", 0.95)
        add_tag(memory_conn, chunk_id, "modality", row.get("modality"))
        add_tag(memory_conn, chunk_id, "span_type", row.get("span_type"))
        add_tag(memory_conn, chunk_id, "speaker", row.get("speaker_label"))


def run_build(args: argparse.Namespace) -> int:
    pipeline_db = Path(args.pipeline_db)
    memory_db = Path(args.output_db)
    biography_dir = Path(args.biography_dir)

    if args.rebuild and memory_db.exists():
        memory_db.unlink()

    pipeline_conn = connect_pipeline_db(pipeline_db)
    init_pipeline_db(pipeline_conn)
    memory_conn = connect_memory_db(memory_db)
    init_memory_db(memory_conn)

    generated_at = iso_z_now()
    supersede_active_builds(memory_conn, args.user_id)
    build_id = stable_id("guardian-memory-build", args.user_id, generated_at)
    memory_conn.execute(
        """
        INSERT INTO memory_build (id, user_id, source_kind, source_hash, status, generated_at, notes_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            build_id,
            args.user_id,
            "content_span+daily_biographies",
            None,
            "active",
            generated_at,
            json_dumps(
                {
                    "pipeline_db": str(pipeline_db.resolve()),
                    "biography_dir": str(biography_dir.resolve()),
                }
            ),
        ),
    )

    days = sorted(set(args.day or all_days(pipeline_conn, biography_dir, args.user_id)))
    print(f"Building memory DB for {len(days)} day(s) -> {memory_db}")
    for day in days:
        build_day(
            memory_conn=memory_conn,
            pipeline_conn=pipeline_conn,
            build_id=build_id,
            user_id=args.user_id,
            day=day,
            biography_dir=biography_dir,
            generated_at=generated_at,
        )
        chunk_count = memory_conn.execute(
            "SELECT COUNT(*) FROM memory_chunk WHERE build_id = ? AND day = ?",
            (build_id, day),
        ).fetchone()[0]
        print(f"  {day}: chunks={chunk_count}")

    memory_conn.commit()
    pipeline_conn.close()
    memory_conn.close()
    return 0


def run_day(args: argparse.Namespace) -> int:
    conn = connect_memory_db(Path(args.db))
    build_id = active_build_id(conn, args.user_id)
    if not build_id:
        raise SystemExit("No active memory build found")
    row = conn.execute(
        """
        SELECT day, status, completeness, modality_coverage_json, source_counts_json, biography_md, day_arc
        FROM day_summary
        WHERE build_id = ? AND day = ?
        """,
        (build_id, args.day),
    ).fetchone()
    if row is None:
        raise SystemExit(f"No day summary found for {args.day}")
    payload = dict(row)
    for key in ("modality_coverage_json", "source_counts_json"):
        raw_value = payload.get(key)
        if raw_value:
            try:
                payload[key] = json.loads(raw_value)
            except json.JSONDecodeError:
                pass
    print(json.dumps(payload, indent=2))
    print("\nChunk counts:")
    for chunk_row in conn.execute(
        """
        SELECT chunk_type, COUNT(*) AS c
        FROM memory_chunk
        WHERE build_id = ? AND day = ?
        GROUP BY chunk_type
        ORDER BY c DESC, chunk_type ASC
        """,
        (build_id, args.day),
    ):
        print(f"  {chunk_row['chunk_type']}: {chunk_row['c']}")
    conn.close()
    return 0


def run_search(args: argparse.Namespace) -> int:
    conn = connect_memory_db(Path(args.db))
    build_id = active_build_id(conn, args.user_id)
    if not build_id:
        raise SystemExit("No active memory build found")
    rows = conn.execute(
        """
        SELECT
            mc.day,
            mc.chunk_type,
            mc.title,
            snippet(memory_chunk_fts, 2, '[', ']', '...', 12) AS snippet,
            bm25(memory_chunk_fts) AS rank
        FROM memory_chunk_fts
        JOIN memory_chunk mc ON mc.id = memory_chunk_fts.chunk_id
        WHERE memory_chunk_fts MATCH ?
          AND mc.build_id = ?
        ORDER BY rank
        LIMIT ?
        """,
        (args.query, build_id, args.limit),
    ).fetchall()
    for row in rows:
        title = f" | {row['title']}" if row["title"] else ""
        print(f"{row['day']} | {row['chunk_type']}{title}")
        print(f"  {row['snippet']}")
    conn.close()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and inspect Guardian memory DB")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Build guardian_memory.db from content spans and day outputs")
    build.add_argument("--pipeline-db", default="data/guardian_pipeline.db", help="Path to guardian_pipeline.db")
    build.add_argument("--output-db", default="data/guardian_memory.db", help="Path to guardian_memory.db")
    build.add_argument("--biography-dir", default="data/daily-biographies", help="Path to day output directory")
    build.add_argument("--user-id", default="example-user", help="Guardian user id")
    build.add_argument("--day", action="append", default=[], help="Optional day(s) to include")
    build.add_argument("--rebuild", action="store_true", help="Delete and rebuild the memory DB")

    day = subparsers.add_parser("day", help="Inspect one day from guardian_memory.db")
    day.add_argument("--db", default="data/guardian_memory.db", help="Path to guardian_memory.db")
    day.add_argument("--user-id", default="example-user", help="Guardian user id")
    day.add_argument("--day", required=True, help="Day in YYYY-MM-DD form")

    search = subparsers.add_parser("search", help="FTS search over memory chunks")
    search.add_argument("--db", default="data/guardian_memory.db", help="Path to guardian_memory.db")
    search.add_argument("--user-id", default="example-user", help="Guardian user id")
    search.add_argument("--query", required=True, help="FTS query string")
    search.add_argument("--limit", type=int, default=10, help="Max rows to return")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "build":
        return run_build(args)
    if args.command == "day":
        return run_day(args)
    if args.command == "search":
        return run_search(args)
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
