#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from guardian_capture_segments import iso_z, stable_id
from guardian_pipeline_db import (
    connect_db,
    event_record,
    fetch_scene_windows,
    init_db,
    json_dumps,
    json_loads,
    upsert_event,
    upsert_reference_scene,
    upsert_reference_scene_member,
)
from guardian_source_priority import is_direct_device_source, source_priority_for_type


@dataclass(frozen=True)
class SceneMember:
    row: Any
    metadata: dict[str, Any]
    start_at: datetime
    end_at: datetime
    adjusted_start_at: datetime
    adjusted_end_at: datetime
    alignment_offset_ms: int
    clock_confidence: str
    primary_source_type: str
    source_priority: int
    confidence: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Align scene windows across devices using wall-clock timestamps plus clock offsets.")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--date", default=None, help="Optional UTC date filter in YYYY-MM-DD form")
    parser.add_argument(
        "--tolerance-seconds",
        type=float,
        default=8.0,
        help="Allowed cross-device gap after clock-offset normalization when grouping windows into one reference scene",
    )
    parser.add_argument(
        "--processor-version",
        default="v1",
        help="Version label stamped into cross-device alignment outputs",
    )
    parser.add_argument(
        "--max-group-span-seconds",
        type=float,
        default=900.0,
        help="Maximum total aligned span allowed for a single reference scene group before forcing a split.",
    )
    return parser.parse_args()


def parse_iso(timestamp: str) -> datetime:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)


def date_range(date_text: str) -> tuple[str, str]:
    base = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return iso_z(base), iso_z(base + timedelta(days=1))


def clock_confidence_rank(value: str | None) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(value or "", 0)


def build_member(row: Any) -> SceneMember:
    metadata = json_loads(row["metadata_json"], default={}) or {}
    start_at = parse_iso(row["start_at"])
    end_at = parse_iso(row["end_at"])
    time_basis = metadata.get("time_alignment_basis") or {}
    offset_ms = int(time_basis.get("median_clock_offset_ms") or time_basis.get("clock_offset_ms") or 0)
    clock_confidence = str(time_basis.get("clock_confidence") or "low")
    adjusted_start_at = start_at - timedelta(milliseconds=offset_ms)
    adjusted_end_at = end_at - timedelta(milliseconds=offset_ms)
    primary_source_type = str(row["primary_source_type"] or "unknown")
    return SceneMember(
        row=row,
        metadata=metadata,
        start_at=start_at,
        end_at=end_at,
        adjusted_start_at=adjusted_start_at,
        adjusted_end_at=adjusted_end_at,
        alignment_offset_ms=offset_ms,
        clock_confidence=clock_confidence,
        primary_source_type=primary_source_type,
        source_priority=source_priority_for_type(primary_source_type),
        confidence=float(row["confidence"]),
    )


def group_members(members: list[SceneMember], tolerance_seconds: float, max_group_span_seconds: float) -> list[list[SceneMember]]:
    if not members:
        return []
    ordered = sorted(members, key=lambda member: (member.adjusted_start_at, member.adjusted_end_at))
    groups: list[list[SceneMember]] = [[ordered[0]]]
    current_min_start = ordered[0].adjusted_start_at
    current_max_end = ordered[0].adjusted_end_at
    for member in ordered[1:]:
        proposed_min_start = min(current_min_start, member.adjusted_start_at)
        proposed_max_end = max(current_max_end, member.adjusted_end_at)
        proposed_span_seconds = (proposed_max_end - proposed_min_start).total_seconds()
        if (
            member.adjusted_start_at <= current_max_end + timedelta(seconds=tolerance_seconds)
            and proposed_span_seconds <= max_group_span_seconds
        ):
            groups[-1].append(member)
            current_min_start = proposed_min_start
            current_max_end = proposed_max_end
            continue
        groups.append([member])
        current_min_start = member.adjusted_start_at
        current_max_end = member.adjusted_end_at
    return groups


def choose_primary_member(group: list[SceneMember]) -> SceneMember:
    return min(
        group,
        key=lambda member: (
            member.source_priority,
            0 if is_direct_device_source(member.primary_source_type) else 1,
            -member.confidence,
            -clock_confidence_rank(member.clock_confidence),
            member.adjusted_start_at,
        ),
    )


def reference_scene_record(
    *,
    user_id: str,
    group: list[SceneMember],
    primary_member: SceneMember,
    tolerance_seconds: float,
    processor_version: str,
) -> dict[str, Any]:
    start_at = min(member.adjusted_start_at for member in group)
    end_at = max(member.adjusted_end_at for member in group)
    device_ids = sorted({member.row["primary_device_id"] for member in group if member.row["primary_device_id"]})
    direct_member_count = sum(1 for member in group if is_direct_device_source(member.primary_source_type))
    screen_context_rows = []
    for member in group:
        screen_context = member.metadata.get("screen_context")
        if isinstance(screen_context, dict):
            screen_context_rows.append(
                {
                    "scene_window_id": member.row["id"],
                    "device_id": member.row["primary_device_id"],
                    "status": screen_context.get("status"),
                    "attention_score": screen_context.get("attention_score"),
                    "screen_device_hits": screen_context.get("screen_device_hits") or [],
                }
            )
    attached_screen_rows = [row for row in screen_context_rows if row.get("status") == "attached"]
    metadata = {
        "member_scene_window_ids": [member.row["id"] for member in group],
        "device_ids": device_ids,
        "member_count": len(group),
        "direct_member_count": direct_member_count,
        "source_types": sorted({member.primary_source_type for member in group}),
        "time_alignment": {
            "method": "scene_window_overlap_plus_clock_offset",
            "tolerance_seconds": round(tolerance_seconds, 3),
            "aligned_span_seconds": round((end_at - start_at).total_seconds(), 3),
            "member_offsets_ms": {member.row["id"]: member.alignment_offset_ms for member in group},
            "member_clock_confidence": {member.row["id"]: member.clock_confidence for member in group},
        },
        "arbitration": {
            "method": "source_priority_then_confidence",
            "preferred_direct_device": True,
            "primary_scene_window_id": primary_member.row["id"],
            "primary_source_type": primary_member.primary_source_type,
            "primary_device_id": primary_member.row["primary_device_id"],
        },
        "screen_context": {
            "attached_member_count": len(attached_screen_rows),
            "evaluated_member_count": len(screen_context_rows),
            "attached_members": attached_screen_rows,
        },
        "builder_version": processor_version,
    }
    return {
        "id": stable_id(
            "guardian-reference-scene",
            user_id,
            iso_z(start_at),
            iso_z(end_at),
            primary_member.row["id"],
            processor_version,
        ),
        "user_id": user_id,
        "primary_device_id": primary_member.row["primary_device_id"],
        "primary_scene_window_id": primary_member.row["id"],
        "start_at": iso_z(start_at),
        "end_at": iso_z(end_at),
        "source_priority": primary_member.source_priority,
        "confidence": round(max(member.confidence for member in group), 3),
        "primary_source_type": primary_member.primary_source_type,
        "status": "stable",
        "metadata_json": json_dumps(metadata),
    }


def reference_event(reference_scene: dict[str, Any], group: list[SceneMember], primary_member: SceneMember, processor_version: str) -> dict[str, Any]:
    return {
        "id": stable_id("guardian-reference-event", reference_scene["id"], processor_version),
        "user_id": reference_scene["user_id"],
        "device_id": primary_member.row["primary_device_id"],
        "event_type": "cross_device_reference",
        "start_at": reference_scene["start_at"],
        "end_at": reference_scene["end_at"],
        "source_priority": reference_scene["source_priority"],
        "confidence": reference_scene["confidence"],
        "primary_source_type": reference_scene["primary_source_type"],
        "created_by": f"cross_device_aligner_{processor_version}",
        "created_at": iso_z(datetime.now(timezone.utc)),
        "payload": {
            "reference_scene_id": reference_scene["id"],
            "primary_device_id": primary_member.row["primary_device_id"],
            "primary_scene_window_id": primary_member.row["id"],
            "member_scene_windows": [
                {
                    "scene_window_id": member.row["id"],
                    "device_id": member.row["primary_device_id"],
                    "primary_source_type": member.primary_source_type,
                    "confidence": member.confidence,
                    "alignment_offset_ms": member.alignment_offset_ms,
                    "clock_confidence": member.clock_confidence,
                }
                for member in group
            ],
            "preferred_direct_device": True,
            "time_alignment_method": "scene_window_overlap_plus_clock_offset",
        },
    }


def main() -> int:
    args = parse_args()
    conn = connect_db(Path(args.db))
    init_db(conn)

    start_at = end_at = None
    if args.date:
        start_at, end_at = date_range(args.date)

    members = [
        build_member(row)
        for row in fetch_scene_windows(conn, user_id=args.user_id, start_at=start_at, end_at=end_at)
    ]
    groups = [
        group
        for group in group_members(members, args.tolerance_seconds, args.max_group_span_seconds)
        if len({member.row["primary_device_id"] for member in group}) > 1
    ]

    reference_count = 0
    member_count = 0
    event_count = 0
    for group in groups:
        primary_member = choose_primary_member(group)
        ref_scene = reference_scene_record(
            user_id=args.user_id,
            group=group,
            primary_member=primary_member,
            tolerance_seconds=args.tolerance_seconds,
            processor_version=args.processor_version,
        )
        upsert_reference_scene(conn, ref_scene)
        reference_count += 1

        for member in group:
            upsert_reference_scene_member(
                conn,
                reference_scene_id=ref_scene["id"],
                scene_window_id=member.row["id"],
                role="primary" if member.row["id"] == primary_member.row["id"] else "supporting",
                alignment_offset_ms=member.alignment_offset_ms,
                confidence=member.confidence,
            )
            member_count += 1

        upsert_event(conn, event_record(reference_event(ref_scene, group, primary_member, args.processor_version)))
        event_count += 1

    conn.close()
    print(f"scene_windows={len(members)}")
    print(f"reference_scenes={reference_count}")
    print(f"reference_scene_members={member_count}")
    print(f"cross_device_events={event_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
