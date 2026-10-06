#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts.base import UserMessage


REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs" / "company-knowledge"
DATA_DIR = REPO_ROOT / "data"


@dataclass(frozen=True)
class GuardianConfig:
    user_id: str
    memory_db: Path
    pipeline_db: Path
    bootstrap_file: Path
    corrections_file: Path
    user_context_file: Path
    open_questions_file: Path
    user_context_notes_dir: Path
    corrections_notes_dir: Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Guardian MCP server")
    parser.add_argument(
        "--user-id",
        default=os.environ.get("GUARDIAN_USER_ID", "example-user"),
        help="Guardian user id",
    )
    parser.add_argument(
        "--memory-db",
        default=os.environ.get("GUARDIAN_MEMORY_DB", str(DATA_DIR / "guardian_memory.db")),
        help="Path to guardian_memory.db",
    )
    parser.add_argument(
        "--pipeline-db",
        default=os.environ.get("GUARDIAN_PIPELINE_DB", str(DATA_DIR / "guardian_pipeline.db")),
        help="Path to guardian_pipeline.db",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default=os.environ.get("GUARDIAN_MCP_TRANSPORT", "stdio"),
        help="MCP transport",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("GUARDIAN_MCP_HOST")
        or os.environ.get("FASTMCP_HOST")
        or "127.0.0.1",
        help="Bind host for HTTP transports",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(
            os.environ.get("GUARDIAN_MCP_PORT")
            or os.environ.get("FASTMCP_PORT")
            or "8000"
        ),
        help="Bind port for HTTP transports",
    )
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> GuardianConfig:
    return GuardianConfig(
        user_id=args.user_id,
        memory_db=Path(args.memory_db).expanduser().resolve(),
        pipeline_db=Path(args.pipeline_db).expanduser().resolve(),
        bootstrap_file=DOCS_DIR / "2026-03-26-guardian-agent-bootstrap.md",
        corrections_file=DOCS_DIR / "guardian-corrections.md",
        user_context_file=DOCS_DIR / "guardian-user-context.md",
        open_questions_file=DOCS_DIR / "guardian-open-questions.md",
        user_context_notes_dir=DOCS_DIR / "notes" / "user-context",
        corrections_notes_dir=DOCS_DIR / "notes" / "corrections",
    )


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return path.read_text(encoding="utf-8")


def connect_readonly_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"Missing database: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA trusted_schema = OFF")
    return conn


def json_loads(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def shorten(value: str | None, limit: int = 1200) -> str | None:
    if not value:
        return value
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


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


def normalize_limit(limit: int, *, minimum: int = 1, maximum: int = 50) -> int:
    return max(minimum, min(limit, maximum))


def tags_for_chunk_ids(conn: sqlite3.Connection, chunk_ids: list[str]) -> dict[str, dict[str, list[str]]]:
    if not chunk_ids:
        return {}
    placeholders = ",".join("?" for _ in chunk_ids)
    rows = conn.execute(
        f"""
        SELECT chunk_id, tag_type, tag_value
        FROM chunk_tag
        WHERE chunk_id IN ({placeholders})
        ORDER BY chunk_id, tag_type, tag_value
        """,
        chunk_ids,
    ).fetchall()
    grouped: dict[str, dict[str, list[str]]] = {chunk_id: {} for chunk_id in chunk_ids}
    for row in rows:
        grouped.setdefault(str(row["chunk_id"]), {}).setdefault(str(row["tag_type"]), []).append(str(row["tag_value"]))
    return grouped


def refs_for_chunk_ids(conn: sqlite3.Connection, chunk_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not chunk_ids:
        return {}
    placeholders = ",".join("?" for _ in chunk_ids)
    rows = conn.execute(
        f"""
        SELECT chunk_id, ref_kind, ref_id, role, weight
        FROM chunk_ref
        WHERE chunk_id IN ({placeholders})
        ORDER BY chunk_id, ref_kind, role, ref_id
        """,
        chunk_ids,
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {chunk_id: [] for chunk_id in chunk_ids}
    for row in rows:
        grouped.setdefault(str(row["chunk_id"]), []).append(
            {
                "ref_kind": str(row["ref_kind"]),
                "ref_id": str(row["ref_id"]),
                "role": str(row["role"]),
                "weight": row["weight"],
            }
        )
    return grouped


def serialize_chunk_row(row: sqlite3.Row, *, tags: dict[str, list[str]] | None = None, refs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "chunk_id": row["id"],
        "day": row["day"],
        "chunk_type": row["chunk_type"],
        "title": row["title"],
        "text": row["text"],
        "start_at": row["start_at"],
        "end_at": row["end_at"],
        "sort_key": row["sort_key"],
        "completeness": row["completeness"],
        "confidence": row["confidence"],
        "salience": row["salience"],
        "metadata": json_loads(row["metadata_json"]),
        "tags": tags or {},
        "refs": refs or [],
    }


def day_summary_payload(conn: sqlite3.Connection, user_id: str, day: str) -> dict[str, Any]:
    build_id = active_build_id(conn, user_id)
    if not build_id:
        raise ValueError(f"No active memory build found for user {user_id}")

    row = conn.execute(
        """
        SELECT *
        FROM day_summary
        WHERE build_id = ? AND day = ?
        """,
        (build_id, day),
    ).fetchone()
    if row is None:
        raise ValueError(f"No day summary found for {day}")

    chunk_counts = {
        str(count_row["chunk_type"]): int(count_row["count"])
        for count_row in conn.execute(
            """
            SELECT chunk_type, COUNT(*) AS count
            FROM memory_chunk
            WHERE build_id = ? AND day = ?
            GROUP BY chunk_type
            ORDER BY count DESC, chunk_type ASC
            """,
            (build_id, day),
        ).fetchall()
    }

    coverage_rows = conn.execute(
        """
        SELECT span_kind, modality, start_at, end_at, completeness, confidence, notes, metadata_json
        FROM coverage_span
        WHERE build_id = ? AND day = ?
        ORDER BY start_at ASC, end_at ASC, id ASC
        """,
        (build_id, day),
    ).fetchall()

    timeline_rows = conn.execute(
        """
        SELECT *
        FROM memory_chunk
        WHERE build_id = ? AND day = ? AND chunk_type = 'time_block'
        ORDER BY sort_key ASC, start_at ASC, id ASC
        LIMIT 24
        """,
        (build_id, day),
    ).fetchall()
    timeline_ids = [str(item["id"]) for item in timeline_rows]
    timeline_tags = tags_for_chunk_ids(conn, timeline_ids)
    timeline_refs = refs_for_chunk_ids(conn, timeline_ids)

    return {
        "build_id": build_id,
        "day": row["day"],
        "status": row["status"],
        "completeness": row["completeness"],
        "biography_md": row["biography_md"],
        "day_arc": row["day_arc"],
        "modality_coverage": json_loads(row["modality_coverage_json"]),
        "source_counts": json_loads(row["source_counts_json"]),
        "sidecar": json_loads(row["sidecar_json"]),
        "notes": json_loads(row["notes_json"]),
        "generated_at": row["generated_at"],
        "chunk_counts": chunk_counts,
        "timeline": [
            serialize_chunk_row(timeline_row, tags=timeline_tags.get(str(timeline_row["id"])), refs=timeline_refs.get(str(timeline_row["id"])))
            for timeline_row in timeline_rows
        ],
        "coverage_spans": [
            {
                "span_kind": coverage_row["span_kind"],
                "modality": coverage_row["modality"],
                "start_at": coverage_row["start_at"],
                "end_at": coverage_row["end_at"],
                "completeness": coverage_row["completeness"],
                "confidence": coverage_row["confidence"],
                "notes": coverage_row["notes"],
                "metadata": json_loads(coverage_row["metadata_json"]),
            }
            for coverage_row in coverage_rows
        ],
    }


def day_timeline_payload(conn: sqlite3.Connection, user_id: str, day: str, limit: int) -> dict[str, Any]:
    build_id = active_build_id(conn, user_id)
    if not build_id:
        raise ValueError(f"No active memory build found for user {user_id}")
    rows = conn.execute(
        """
        SELECT *
        FROM memory_chunk
        WHERE build_id = ? AND day = ? AND chunk_type = 'time_block'
        ORDER BY sort_key ASC, start_at ASC, id ASC
        LIMIT ?
        """,
        (build_id, day, normalize_limit(limit, maximum=200)),
    ).fetchall()
    chunk_ids = [str(row["id"]) for row in rows]
    tags = tags_for_chunk_ids(conn, chunk_ids)
    refs = refs_for_chunk_ids(conn, chunk_ids)
    return {
        "build_id": build_id,
        "day": day,
        "timeline": [serialize_chunk_row(row, tags=tags.get(str(row["id"])), refs=refs.get(str(row["id"]))) for row in rows],
    }


def chunk_payload(conn: sqlite3.Connection, user_id: str, chunk_id: str) -> dict[str, Any]:
    build_id = active_build_id(conn, user_id)
    if not build_id:
        raise ValueError(f"No active memory build found for user {user_id}")
    row = conn.execute(
        """
        SELECT *
        FROM memory_chunk
        WHERE build_id = ? AND id = ?
        """,
        (build_id, chunk_id),
    ).fetchone()
    if row is None:
        raise ValueError(f"No memory chunk found for id {chunk_id}")
    tags = tags_for_chunk_ids(conn, [chunk_id]).get(chunk_id, {})
    refs = refs_for_chunk_ids(conn, [chunk_id]).get(chunk_id, [])
    return serialize_chunk_row(row, tags=tags, refs=refs)


def search_memory_payload(
    conn: sqlite3.Connection,
    user_id: str,
    *,
    query: str,
    day: str | None,
    day_from: str | None,
    day_to: str | None,
    chunk_types: list[str] | None,
    tags: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    build_id = active_build_id(conn, user_id)
    if not build_id:
        raise ValueError(f"No active memory build found for user {user_id}")

    cleaned_limit = normalize_limit(limit)
    cleaned_query = query.strip()
    chunk_types = [value for value in (chunk_types or []) if value]
    tags = [value for value in (tags or []) if value]

    filters_sql: list[str] = ["mc.build_id = ?"]
    params: list[Any] = [build_id]
    if day:
        filters_sql.append("mc.day = ?")
        params.append(day)
    if day_from:
        filters_sql.append("mc.day >= ?")
        params.append(day_from)
    if day_to:
        filters_sql.append("mc.day <= ?")
        params.append(day_to)
    if chunk_types:
        placeholders = ",".join("?" for _ in chunk_types)
        filters_sql.append(f"mc.chunk_type IN ({placeholders})")
        params.extend(chunk_types)
    if tags:
        placeholders = ",".join("?" for _ in tags)
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM chunk_tag ct WHERE ct.chunk_id = mc.id AND ct.tag_value IN ({placeholders}))"
        )
        params.extend(tags)

    where_sql = " AND ".join(filters_sql)

    if cleaned_query:
        sql = f"""
            SELECT
                mc.id,
                mc.day,
                mc.chunk_type,
                mc.title,
                mc.start_at,
                mc.end_at,
                mc.completeness,
                mc.confidence,
                mc.salience,
                mc.metadata_json,
                snippet(memory_chunk_fts, 2, '[', ']', '...', 16) AS snippet,
                bm25(memory_chunk_fts) AS rank
            FROM memory_chunk_fts
            JOIN memory_chunk mc ON mc.id = memory_chunk_fts.chunk_id
            WHERE memory_chunk_fts MATCH ?
              AND {where_sql}
            ORDER BY rank ASC
            LIMIT ?
        """
        try:
            rows = conn.execute(sql, [cleaned_query, *params, cleaned_limit]).fetchall()
            strategy = "fts"
        except sqlite3.OperationalError:
            like_sql = f"""
                SELECT
                    mc.id,
                    mc.day,
                    mc.chunk_type,
                    mc.title,
                    mc.start_at,
                    mc.end_at,
                    mc.completeness,
                    mc.confidence,
                    mc.salience,
                    mc.metadata_json,
                    substr(mc.text, 1, 240) AS snippet,
                    NULL AS rank
                FROM memory_chunk mc
                WHERE {where_sql}
                  AND (mc.title LIKE ? OR mc.text LIKE ?)
                ORDER BY mc.salience DESC, mc.confidence DESC, mc.sort_key ASC
                LIMIT ?
            """
            like_query = f"%{cleaned_query}%"
            rows = conn.execute(like_sql, [*params, like_query, like_query, cleaned_limit]).fetchall()
            strategy = "like"
    else:
        rows = conn.execute(
            f"""
            SELECT
                mc.id,
                mc.day,
                mc.chunk_type,
                mc.title,
                mc.start_at,
                mc.end_at,
                mc.completeness,
                mc.confidence,
                mc.salience,
                mc.metadata_json,
                substr(mc.text, 1, 240) AS snippet,
                NULL AS rank
            FROM memory_chunk mc
            WHERE {where_sql}
            ORDER BY mc.salience DESC, mc.confidence DESC, mc.sort_key ASC
            LIMIT ?
            """,
            [*params, cleaned_limit],
        ).fetchall()
        strategy = "latest"

    chunk_ids = [str(row["id"]) for row in rows]
    chunk_tags = tags_for_chunk_ids(conn, chunk_ids)
    chunk_refs = refs_for_chunk_ids(conn, chunk_ids)

    return {
        "build_id": build_id,
        "strategy": strategy,
        "query": cleaned_query,
        "result_count": len(rows),
        "results": [
            {
                "chunk_id": row["id"],
                "day": row["day"],
                "chunk_type": row["chunk_type"],
                "title": row["title"],
                "start_at": row["start_at"],
                "end_at": row["end_at"],
                "snippet": row["snippet"],
                "completeness": row["completeness"],
                "confidence": row["confidence"],
                "salience": row["salience"],
                "rank": row["rank"],
                "metadata": json_loads(row["metadata_json"]),
                "tags": chunk_tags.get(str(row["id"]), {}),
                "refs": chunk_refs.get(str(row["id"]), []),
            }
            for row in rows
        ],
    }


def evidence_payload(memory_conn: sqlite3.Connection, pipeline_conn: sqlite3.Connection, user_id: str, chunk_id: str) -> dict[str, Any]:
    chunk = chunk_payload(memory_conn, user_id, chunk_id)
    evidence: list[dict[str, Any]] = []
    for ref in chunk["refs"]:
        ref_kind = ref["ref_kind"]
        ref_id = ref["ref_id"]
        if ref_kind == "content_span":
            row = pipeline_conn.execute(
                """
                SELECT id, span_type, modality, title, text, language, speaker_label,
                       start_at, end_at, confidence, status, created_by, created_at,
                       artifact_id, event_id, scene_window_id, reference_scene_id, metadata_json
                FROM content_span
                WHERE id = ?
                """,
                (ref_id,),
            ).fetchone()
            if row:
                evidence.append(
                    {
                        "ref": ref,
                        "kind": "content_span",
                        "record": {
                            "id": row["id"],
                            "span_type": row["span_type"],
                            "modality": row["modality"],
                            "title": row["title"],
                            "text": row["text"],
                            "language": row["language"],
                            "speaker_label": row["speaker_label"],
                            "start_at": row["start_at"],
                            "end_at": row["end_at"],
                            "confidence": row["confidence"],
                            "status": row["status"],
                            "created_by": row["created_by"],
                            "created_at": row["created_at"],
                            "artifact_id": row["artifact_id"],
                            "event_id": row["event_id"],
                            "scene_window_id": row["scene_window_id"],
                            "reference_scene_id": row["reference_scene_id"],
                            "metadata": json_loads(row["metadata_json"]),
                        },
                    }
                )
        elif ref_kind == "artifact":
            row = pipeline_conn.execute(
                """
                SELECT id, artifact_type, storage_uri, byte_size, captured_at_wall, normalized_at,
                       status, metadata_json
                FROM artifact
                WHERE id = ?
                """,
                (ref_id,),
            ).fetchone()
            if row:
                evidence.append(
                    {
                        "ref": ref,
                        "kind": "artifact",
                        "record": {
                            "id": row["id"],
                            "artifact_type": row["artifact_type"],
                            "storage_uri": row["storage_uri"],
                            "byte_size": row["byte_size"],
                            "captured_at_wall": row["captured_at_wall"],
                            "normalized_at": row["normalized_at"],
                            "status": row["status"],
                            "metadata_excerpt": shorten(row["metadata_json"]),
                        },
                    }
                )
        elif ref_kind == "event":
            row = pipeline_conn.execute(
                """
                SELECT id, event_type, start_at, end_at, confidence, source_priority,
                       primary_source_type, created_by, created_at, payload_json
                FROM event
                WHERE id = ?
                """,
                (ref_id,),
            ).fetchone()
            if row:
                evidence.append(
                    {
                        "ref": ref,
                        "kind": "event",
                        "record": {
                            "id": row["id"],
                            "event_type": row["event_type"],
                            "start_at": row["start_at"],
                            "end_at": row["end_at"],
                            "confidence": row["confidence"],
                            "source_priority": row["source_priority"],
                            "primary_source_type": row["primary_source_type"],
                            "created_by": row["created_by"],
                            "created_at": row["created_at"],
                            "payload_excerpt": shorten(row["payload_json"]),
                        },
                    }
                )
        elif ref_kind == "scene_window":
            row = pipeline_conn.execute(
                """
                SELECT id, primary_device_id, window_type, start_at, end_at, confidence,
                       source_priority, primary_source_type, status, metadata_json
                FROM scene_window
                WHERE id = ?
                """,
                (ref_id,),
            ).fetchone()
            if row:
                evidence.append(
                    {
                        "ref": ref,
                        "kind": "scene_window",
                        "record": {
                            "id": row["id"],
                            "primary_device_id": row["primary_device_id"],
                            "window_type": row["window_type"],
                            "start_at": row["start_at"],
                            "end_at": row["end_at"],
                            "confidence": row["confidence"],
                            "source_priority": row["source_priority"],
                            "primary_source_type": row["primary_source_type"],
                            "status": row["status"],
                            "metadata_excerpt": shorten(row["metadata_json"]),
                        },
                    }
                )
        elif ref_kind == "reference_scene":
            row = pipeline_conn.execute(
                """
                SELECT id, primary_device_id, primary_scene_window_id, start_at, end_at,
                       confidence, source_priority, primary_source_type, status, metadata_json
                FROM reference_scene
                WHERE id = ?
                """,
                (ref_id,),
            ).fetchone()
            if row:
                evidence.append(
                    {
                        "ref": ref,
                        "kind": "reference_scene",
                        "record": {
                            "id": row["id"],
                            "primary_device_id": row["primary_device_id"],
                            "primary_scene_window_id": row["primary_scene_window_id"],
                            "start_at": row["start_at"],
                            "end_at": row["end_at"],
                            "confidence": row["confidence"],
                            "source_priority": row["source_priority"],
                            "primary_source_type": row["primary_source_type"],
                            "status": row["status"],
                            "metadata_excerpt": shorten(row["metadata_json"]),
                        },
                    }
                )
        elif ref_kind == "day_summary":
            row = memory_conn.execute(
                """
                SELECT day, status, completeness, day_arc, generated_at
                FROM day_summary
                WHERE id = ?
                """,
                (ref_id,),
            ).fetchone()
            if row:
                evidence.append(
                    {
                        "ref": ref,
                        "kind": "day_summary",
                        "record": dict(row),
                    }
                )
    return {
        "chunk": chunk,
        "evidence": evidence,
    }


def describe_environment_payload(memory_conn: sqlite3.Connection, config: GuardianConfig) -> dict[str, Any]:
    build_id = active_build_id(memory_conn, config.user_id)
    if not build_id:
        raise ValueError(f"No active memory build found for user {config.user_id}")

    summary_rows = memory_conn.execute(
        """
        SELECT day, status, completeness
        FROM day_summary
        WHERE build_id = ?
        ORDER BY day ASC
        """,
        (build_id,),
    ).fetchall()
    status_counts: dict[str, int] = {}
    incomplete_days: list[str] = []
    for row in summary_rows:
        status = str(row["status"])
        status_counts[status] = status_counts.get(status, 0) + 1
        if status != "complete":
            incomplete_days.append(str(row["day"]))

    return {
        "user_id": config.user_id,
        "build_id": build_id,
        "memory_db": str(config.memory_db),
        "pipeline_db": str(config.pipeline_db),
        "day_count": len(summary_rows),
        "status_counts": status_counts,
        "incomplete_days": incomplete_days,
        "days": [str(row["day"]) for row in summary_rows],
        "resources": [
            "guardian://bootstrap",
            "guardian://corrections",
            "guardian://user-context",
            "guardian://open-questions",
        ],
        "note_templates": [
            "guardian://notes/user-context/{date}",
            "guardian://notes/corrections/{date}",
        ],
        "tool_retrieval_order": [
            "describe_environment",
            "search_memory",
            "get_day",
            "get_timeline",
            "get_chunk",
            "get_evidence",
        ],
    }


def create_server(config: GuardianConfig) -> FastMCP:
    instructions = (
        "Read-only Guardian memory and evidence server. Use guardian_memory.db and "
        "content_span-backed evidence for retrieval. Do not write, mutate, or infer "
        "beyond available evidence without stating uncertainty."
    )
    mcp = FastMCP(
        "guardian-memory",
        instructions=instructions,
        dependencies=(),
    )

    @mcp.resource("guardian://bootstrap", name="Guardian bootstrap", mime_type="text/markdown")
    def bootstrap_resource() -> str:
        return read_text(config.bootstrap_file)

    @mcp.resource("guardian://corrections", name="Guardian corrections", mime_type="text/markdown")
    def corrections_resource() -> str:
        return read_text(config.corrections_file)

    @mcp.resource("guardian://user-context", name="Guardian user context", mime_type="text/markdown")
    def user_context_resource() -> str:
        return read_text(config.user_context_file)

    @mcp.resource("guardian://open-questions", name="Guardian open questions", mime_type="text/markdown")
    def open_questions_resource() -> str:
        return read_text(config.open_questions_file)

    @mcp.resource("guardian://notes/user-context/{date}", name="Guardian user context note", mime_type="text/markdown")
    def user_context_note_resource(date: str) -> str:
        return read_text(config.user_context_notes_dir / f"{date}.md")

    @mcp.resource("guardian://notes/corrections/{date}", name="Guardian corrections note", mime_type="text/markdown")
    def corrections_note_resource(date: str) -> str:
        return read_text(config.corrections_notes_dir / f"{date}.md")

    @mcp.resource("guardian://day/{day}", name="Guardian day summary", mime_type="application/json")
    def day_resource(day: str) -> str:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return json.dumps(day_summary_payload(memory_conn, config.user_id, day), indent=2, sort_keys=True)

    @mcp.resource("guardian://chunk/{chunk_id}", name="Guardian memory chunk", mime_type="application/json")
    def chunk_resource(chunk_id: str) -> str:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return json.dumps(chunk_payload(memory_conn, config.user_id, chunk_id), indent=2, sort_keys=True)

    @mcp.resource("guardian://coverage/{day}", name="Guardian coverage view", mime_type="application/json")
    def coverage_resource(day: str) -> str:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            payload = day_summary_payload(memory_conn, config.user_id, day)
            return json.dumps(
                {
                    "day": payload["day"],
                    "status": payload["status"],
                    "completeness": payload["completeness"],
                    "modality_coverage": payload["modality_coverage"],
                    "coverage_spans": payload["coverage_spans"],
                },
                indent=2,
                sort_keys=True,
            )

    @mcp.prompt(name="guardian-chat", description="Bootstrap a read-only Guardian-aware session for this repo")
    def guardian_chat(question: str | None = None) -> list[UserMessage]:
        lines = [
            "Operate as a Guardian-aware, read-only memory assistant for this repository.",
            "Start by reading these resources:",
            "- guardian://bootstrap",
            "- guardian://corrections",
            "- guardian://user-context",
            "- guardian://open-questions",
            "",
            "Use this tool order unless there is a good reason not to:",
            "1. describe_environment",
            "2. search_memory",
            "3. get_day or get_timeline",
            "4. get_chunk",
            "5. get_evidence",
            "",
            "Always surface incompleteness and uncertainty before making strong claims.",
            "Treat guardian_pipeline.db as source of truth over guardian_memory.db if they disagree.",
            "Do not write or mutate files or databases through this server.",
        ]
        messages = [UserMessage("\n".join(lines))]
        if question:
            messages.append(UserMessage(question))
        return messages

    @mcp.tool(description="Describe the available Guardian environment, build status, and incomplete days.")
    def describe_environment() -> dict[str, Any]:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return describe_environment_payload(memory_conn, config)

    @mcp.tool(description="Search Guardian memory chunks with optional day, chunk-type, and tag filters.")
    def search_memory(
        query: str,
        day: str | None = None,
        day_from: str | None = None,
        day_to: str | None = None,
        chunk_types: list[str] | None = None,
        tags: list[str] | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return search_memory_payload(
                memory_conn,
                config.user_id,
                query=query,
                day=day,
                day_from=day_from,
                day_to=day_to,
                chunk_types=chunk_types,
                tags=tags,
                limit=limit,
            )

    @mcp.tool(description="Get one day summary, coverage, and timeline preview from Guardian memory.")
    def get_day(day: str) -> dict[str, Any]:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return day_summary_payload(memory_conn, config.user_id, day)

    @mcp.tool(description="Get the ordered time-block timeline for a specific day.")
    def get_timeline(day: str, limit: int = 50) -> dict[str, Any]:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return day_timeline_payload(memory_conn, config.user_id, day, limit)

    @mcp.tool(description="Get one memory chunk with tags and provenance references.")
    def get_chunk(chunk_id: str) -> dict[str, Any]:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn:
            return chunk_payload(memory_conn, config.user_id, chunk_id)

    @mcp.tool(description="Get the underlying evidence records linked to a memory chunk.")
    def get_evidence(chunk_id: str) -> dict[str, Any]:
        with closing(connect_readonly_db(config.memory_db)) as memory_conn, closing(connect_readonly_db(config.pipeline_db)) as pipeline_conn:
            return evidence_payload(memory_conn, pipeline_conn, config.user_id, chunk_id)

    return mcp


def main() -> int:
    args = parse_args()
    config = build_config(args)
    server = create_server(config)
    server.settings.host = args.host
    server.settings.port = args.port
    server.run(transport=args.transport)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
