#!/usr/bin/env python3
from __future__ import annotations

import argparse
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from guardian_capture_segments import iso_z, stable_id
from guardian_pipeline_db import (
    connect_db,
    event_record,
    fetch_event_artifacts,
    fetch_events_for_user_device,
    init_db,
    json_dumps,
    json_loads,
    upsert_candidate_claim,
    upsert_candidate_claim_evidence,
    upsert_event,
    upsert_event_artifact,
    upsert_scene_window,
    upsert_scene_window_artifact,
)
from guardian_source_priority import choose_primary_source_type, is_direct_device_source, source_priority_for_type


@dataclass
class EventContext:
    row: Any
    payload: dict[str, Any]
    start_at: datetime
    end_at: datetime
    app_name: str | None
    category: str | None
    window_title: str | None
    artifacts: list[Any]
    summaries: list[str]
    source_types: list[str]
    clock_offsets_ms: list[int]
    clock_confidences: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic scene windows and initial candidate claims from interpreted Guardian events."
    )
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--device-id", required=True, help="Device identifier")
    parser.add_argument("--date", default=None, help="Optional UTC date filter in YYYY-MM-DD form")
    parser.add_argument(
        "--gap-seconds",
        type=float,
        default=75.0,
        help="Maximum gap between neighboring interpreted events to keep them in one scene window",
    )
    parser.add_argument(
        "--max-window-seconds",
        type=float,
        default=180.0,
        help="Maximum duration for a single scene window before forcing a split",
    )
    parser.add_argument(
        "--processor-version",
        default="v1",
        help="Version label stamped into scene-window and claim outputs",
    )
    return parser.parse_args()


def parse_iso(timestamp: str) -> datetime:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)


def date_range(date_text: str) -> tuple[str, str]:
    base = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return iso_z(base), iso_z(base + timedelta(days=1))


def first_nonempty(values: list[str | None]) -> str | None:
    for value in values:
        if value:
            cleaned = value.strip()
            if cleaned:
                return cleaned
    return None


def collect_summaries(payload: dict[str, Any]) -> list[str]:
    summaries: list[str] = []
    for key in ("summary", "specific_activity"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            summaries.append(value.strip())
    for item in payload.get("observed_facts") or []:
        if isinstance(item, str) and item.strip():
            summaries.append(item.strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for item in summaries:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped[:4]


def derive_event_context(event_row: Any, artifacts: list[Any]) -> EventContext:
    payload = json_loads(event_row["payload_json"], default={}) or {}
    app_names: list[str | None] = []
    titles: list[str | None] = []
    source_types: list[str] = []
    clock_offsets_ms: list[int] = []
    clock_confidences: list[str] = []
    for artifact in artifacts:
        metadata = json_loads(artifact["metadata_json"], default={}) or {}
        if artifact["artifact_type"] == "app_metadata":
            app_names.append(metadata.get("frontmost_app_name"))
            titles.append(metadata.get("window_title"))
            source_types.append("app_metadata")
        elif artifact["artifact_type"] == "screenshot":
            app_names.append(metadata.get("foreground_app"))
            titles.append(metadata.get("window_title"))
            source_types.append("screenshot")
        elif artifact["artifact_type"] == "desktop_audio":
            source_types.append("desktop_audio")
        elif artifact["artifact_type"] == "audio_chunk":
            source_types.append("ambient_audio")
        elif artifact["artifact_type"] in {"camera_frame", "video_excerpt", "gopro_frame"}:
            source_types.append("camera_visual")
        elif artifact["artifact_type"] in {"audio_excerpt", "gopro_audio"}:
            source_types.append("camera_audio")
        if artifact["clock_offset_ms"] is not None:
            clock_offsets_ms.append(int(artifact["clock_offset_ms"]))
        if artifact["clock_confidence"]:
            clock_confidences.append(str(artifact["clock_confidence"]))

    app_name = first_nonempty(app_names)
    category = payload.get("coarse_category")
    return EventContext(
        row=event_row,
        payload=payload,
        start_at=parse_iso(event_row["start_at"]),
        end_at=parse_iso(event_row["end_at"] or event_row["start_at"]),
        app_name=app_name,
        category=category if isinstance(category, str) and category.strip() else None,
        window_title=first_nonempty(titles),
        artifacts=artifacts,
        summaries=collect_summaries(payload),
        source_types=source_types or [str(event_row["primary_source_type"])],
        clock_offsets_ms=clock_offsets_ms,
        clock_confidences=clock_confidences,
    )


def should_merge(current: list[EventContext], candidate: EventContext, gap_seconds: float, max_window_seconds: float) -> bool:
    previous = current[-1]
    gap = (candidate.start_at - previous.end_at).total_seconds()
    if gap > gap_seconds:
        return False
    window_span = (candidate.end_at - current[0].start_at).total_seconds()
    if window_span > max_window_seconds:
        return False
    same_app = candidate.app_name and current[-1].app_name and candidate.app_name == current[-1].app_name
    same_category = candidate.category and current[-1].category and candidate.category == current[-1].category
    return bool(same_app or same_category)


def group_scene_windows(
    events: list[EventContext],
    *,
    gap_seconds: float,
    max_window_seconds: float,
) -> list[list[EventContext]]:
    if not events:
        return []
    windows: list[list[EventContext]] = [[events[0]]]
    for event in events[1:]:
        current = windows[-1]
        if should_merge(current, event, gap_seconds, max_window_seconds):
            current.append(event)
        else:
            windows.append([event])
    return windows


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def merged_clock_confidence(values: list[str]) -> str:
    ranked = {"high": 3, "medium": 2, "low": 1}
    if not values:
        return "low"
    return max(values, key=lambda value: ranked.get(value, 0))


def score_window(window: list[EventContext]) -> tuple[float, float, dict[str, Any]]:
    app_counts = Counter(event.app_name for event in window if event.app_name)
    category_counts = Counter(event.category for event in window if event.category)
    title_examples: list[str] = []
    for event in window:
        if event.window_title and event.window_title not in title_examples:
            title_examples.append(event.window_title)
        if len(title_examples) >= 4:
            break

    source_types = sorted({source_type for event in window for source_type in event.source_types if source_type})
    direct_evidence_ratio = sum(1 for event in window if any(is_direct_device_source(source_type) for source_type in event.source_types)) / len(window)
    clock_offsets = [offset for event in window for offset in event.clock_offsets_ms]
    median_clock_offset_ms = round(statistics.median(clock_offsets)) if clock_offsets else 0
    clock_confidence = merged_clock_confidence([confidence for event in window for confidence in event.clock_confidences])

    base_conf = sum(float(event.row["confidence"]) for event in window) / len(window)
    dominant_app, dominant_app_count = (app_counts.most_common(1)[0] if app_counts else (None, 0))
    dominant_category, dominant_category_count = (category_counts.most_common(1)[0] if category_counts else (None, 0))
    app_consistency = dominant_app_count / len(window) if window else 0.0
    category_consistency = dominant_category_count / len(window) if window else 0.0
    title_rate = sum(1 for event in window if event.window_title) / len(window)
    conflict_penalty = 0.15 if len(app_counts) > 1 else 0.0
    conflict_penalty += 0.1 if len(category_counts) > 1 else 0.0

    quality_score = clamp(base_conf * 0.5 + title_rate * 0.2 + app_consistency * 0.15 + direct_evidence_ratio * 0.15)
    final_conf = clamp(
        base_conf * 0.5
        + quality_score * 0.2
        + app_consistency * 0.1
        + category_consistency * 0.1
        + direct_evidence_ratio * 0.15
        - conflict_penalty
    )
    metadata = {
        "dominant_app_name": dominant_app,
        "dominant_category": dominant_category,
        "app_counts": dict(app_counts),
        "category_counts": dict(category_counts),
        "title_examples": title_examples,
        "source_types": source_types,
        "direct_evidence_ratio": round(direct_evidence_ratio, 3),
        "median_clock_offset_ms": median_clock_offset_ms,
        "clock_confidence": clock_confidence,
        "quality_flags": [
            flag
            for flag, enabled in [
                ("missing_window_titles", title_rate < 0.5),
                ("mixed_app_focus", len(app_counts) > 1),
                ("mixed_category_context", len(category_counts) > 1),
                ("indirect_only", direct_evidence_ratio <= 0.0),
            ]
            if enabled
        ],
    }
    return round(quality_score, 3), round(final_conf, 3), metadata


def build_scene_window_record(
    window: list[EventContext],
    *,
    user_id: str,
    device_id: str,
    processor_version: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    start_at = iso_z(window[0].start_at)
    end_at = iso_z(window[-1].end_at)
    quality_score, final_confidence, metadata = score_window(window)
    primary_source_type = choose_primary_source_type(metadata.get("source_types") or [], fallback="codex_interpretation")
    source_priority = source_priority_for_type(primary_source_type)
    scene_window_id = stable_id(
        "guardian-scene-window",
        user_id,
        device_id,
        start_at,
        end_at,
        metadata.get("dominant_app_name") or "",
        processor_version,
    )
    metadata.update(
        {
            "event_ids": [event.row["id"] for event in window],
            "event_count": len(window),
            "participating_device_ids": sorted({event.row["device_id"] for event in window if event.row["device_id"]}),
            "summary_examples": [summary for event in window for summary in event.summaries][:6],
            "time_alignment_basis": {
                "method": "wall_clock_plus_median_clock_offset",
                "median_clock_offset_ms": metadata.get("median_clock_offset_ms"),
                "clock_confidence": metadata.get("clock_confidence"),
            },
            "direct_device_preferred": is_direct_device_source(primary_source_type),
            "builder_version": processor_version,
        }
    )
    record = {
        "id": scene_window_id,
        "user_id": user_id,
        "primary_device_id": device_id,
        "window_type": "device_segment" if is_direct_device_source(primary_source_type) else "sensor_segment",
        "start_at": start_at,
        "end_at": end_at,
        "source_priority": source_priority,
        "confidence": final_confidence,
        "primary_source_type": primary_source_type,
        "status": "stable",
        "metadata_json": json_dumps(metadata),
    }
    return record, metadata


def build_claims_for_window(
    scene_window_id: str,
    window: list[EventContext],
    scene_metadata: dict[str, Any],
    scene_confidence: float,
    *,
    user_id: str,
    device_id: str,
    processor_version: str,
) -> list[dict[str, Any]]:
    produced_at = iso_z(datetime.now(timezone.utc))
    dominant_app = scene_metadata.get("dominant_app_name")
    dominant_category = scene_metadata.get("dominant_category")
    app_counts = scene_metadata.get("app_counts") or {}
    category_counts = scene_metadata.get("category_counts") or {}
    primary_source_type = choose_primary_source_type(scene_metadata.get("source_types") or [], fallback="codex_interpretation")
    direct_device_basis = is_direct_device_source(primary_source_type)
    app_consistency = max(app_counts.values()) / len(window) if app_counts else 0.0
    category_consistency = max(category_counts.values()) / len(window) if category_counts else 0.0
    conflict_score = 0.35 if "mixed_app_focus" in (scene_metadata.get("quality_flags") or []) else 0.0
    quality_score = clamp(scene_confidence)

    claims: list[dict[str, Any]] = []

    def make_claim(
        claim_type: str,
        value: dict[str, Any],
        *,
        base_confidence: float,
        support_score: float,
        conflict: float,
    ) -> dict[str, Any]:
        final_confidence = clamp(base_confidence * 0.55 + quality_score * 0.2 + support_score * 0.25 - conflict * 0.3)
        disposition = "accepted" if final_confidence >= 0.72 else "ambiguous" if final_confidence >= 0.45 else "rejected"
        return {
            "id": stable_id("guardian-candidate-claim", scene_window_id, claim_type),
            "user_id": user_id,
            "scene_window_id": scene_window_id,
            "claim_type": claim_type,
            "value_json": json_dumps(value),
            "base_confidence": round(base_confidence, 3),
            "quality_score": round(quality_score, 3),
            "support_score": round(support_score, 3),
            "conflict_score": round(conflict, 3),
            "final_confidence": round(final_confidence, 3),
            "disposition": disposition,
            "produced_by": f"scene_window_claim_builder_{processor_version}",
            "produced_at": produced_at,
        }

    claims.append(
        make_claim(
            "device_attended",
            {
                "device_id": device_id,
                "basis": "direct_device_segment" if direct_device_basis else "indirect_sensor_inference",
                "time_alignment": scene_metadata.get("time_alignment_basis") or {},
            },
            base_confidence=max(scene_confidence, 0.75 if direct_device_basis else 0.55),
            support_score=0.95 if direct_device_basis else 0.6,
            conflict=0.0,
        )
    )

    if dominant_app:
        claims.append(
            make_claim(
                "app_in_focus",
                {
                    "app_name": dominant_app,
                    "window_title_examples": scene_metadata.get("title_examples") or [],
                },
                base_confidence=scene_confidence * max(app_consistency, 0.5),
                support_score=app_consistency,
                conflict=conflict_score,
            )
        )

    if dominant_category:
        claims.append(
            make_claim(
                "task_context",
                {
                    "coarse_category": dominant_category,
                    "summary_examples": scene_metadata.get("summary_examples") or [],
                },
                base_confidence=scene_confidence * max(category_consistency, 0.5),
                support_score=category_consistency,
                conflict=0.2 if "mixed_category_context" in (scene_metadata.get("quality_flags") or []) else 0.0,
            )
        )

    capture_control = (dominant_category == "capture_control") or (
        isinstance(dominant_app, str) and "guardian" in dominant_app.lower()
    )
    if capture_control:
        claims.append(
            make_claim(
                "capture_control",
                {
                    "coarse_category": dominant_category or "capture_control",
                    "app_name": dominant_app,
                },
                base_confidence=max(scene_confidence, 0.8),
                support_score=0.95,
                conflict=0.0,
            )
        )

    return claims


def scene_event_from_claims(
    scene_window: dict[str, Any],
    scene_metadata: dict[str, Any],
    claims: list[dict[str, Any]],
    *,
    processor_version: str,
) -> dict[str, Any]:
    accepted_claims = [
        {
            "claim_type": claim["claim_type"],
            "value": json_loads(claim["value_json"], default={}),
            "confidence": claim["final_confidence"],
        }
        for claim in claims
        if claim["disposition"] == "accepted"
    ]
    return {
        "id": stable_id("guardian-scene-event", scene_window["id"], processor_version),
        "user_id": scene_window["user_id"],
        "device_id": scene_window["primary_device_id"],
        "event_type": "fused_interaction",
        "start_at": scene_window["start_at"],
        "end_at": scene_window["end_at"],
        "source_priority": scene_window["source_priority"],
        "confidence": scene_window["confidence"],
        "primary_source_type": scene_window["primary_source_type"],
        "created_by": f"scene_window_claim_builder_{processor_version}",
        "created_at": iso_z(datetime.now(timezone.utc)),
        "payload": {
            "scene_window_id": scene_window["id"],
            "dominant_app_name": scene_metadata.get("dominant_app_name"),
            "dominant_category": scene_metadata.get("dominant_category"),
            "title_examples": scene_metadata.get("title_examples") or [],
            "quality_flags": scene_metadata.get("quality_flags") or [],
            "summary_examples": scene_metadata.get("summary_examples") or [],
            "accepted_claims": accepted_claims,
            "source_event_ids": scene_metadata.get("event_ids") or [],
        },
    }


def attach_claim_evidence(conn: Any, claim: dict[str, Any], window: list[EventContext], scene_metadata: dict[str, Any]) -> None:
    dominant_app = scene_metadata.get("dominant_app_name")
    for event in window:
        event_app_matches = not dominant_app or event.app_name == dominant_app
        evidence_role = "supporting" if event_app_matches else "contradictory"
        upsert_candidate_claim_evidence(
            conn,
            claim["id"],
            artifact_id=None,
            event_id=event.row["id"],
            evidence_role=evidence_role,
            confidence=float(event.row["confidence"]),
            notes=f"activity_context app={event.app_name or 'unknown'} category={event.category or 'unknown'}",
        )
        for artifact in event.artifacts:
            metadata = json_loads(artifact["metadata_json"], default={}) or {}
            artifact_role = "supporting"
            notes = None
            if artifact["artifact_type"] == "app_metadata":
                artifact_app = metadata.get("frontmost_app_name")
                if dominant_app and artifact_app and artifact_app != dominant_app:
                    artifact_role = "contradictory"
                else:
                    artifact_role = "metadata_support"
                notes = f"app_metadata app={artifact_app or 'unknown'}"
            elif artifact["artifact_type"] == "screenshot":
                notes = f"screenshot path={artifact['storage_uri']}"
            upsert_candidate_claim_evidence(
                conn,
                claim["id"],
                artifact_id=artifact["artifact_id"],
                event_id=None,
                evidence_role=artifact_role,
                confidence=artifact["confidence"],
                notes=notes,
            )


def main() -> int:
    args = parse_args()
    conn = connect_db(Path(args.db))
    init_db(conn)

    start_at = end_at = None
    if args.date:
        start_at, end_at = date_range(args.date)

    event_rows = fetch_events_for_user_device(
        conn,
        user_id=args.user_id,
        device_id=args.device_id,
        event_type="activity_context",
        start_at=start_at,
        end_at=end_at,
        created_by_like="%interpreter%",
    )
    contexts = [
        derive_event_context(row, fetch_event_artifacts(conn, row["id"]))
        for row in event_rows
    ]
    windows = group_scene_windows(
        contexts,
        gap_seconds=args.gap_seconds,
        max_window_seconds=args.max_window_seconds,
    )

    scene_count = 0
    claim_count = 0
    event_count = 0
    for window in windows:
        scene_window, scene_metadata = build_scene_window_record(
            window,
            user_id=args.user_id,
            device_id=args.device_id,
            processor_version=args.processor_version,
        )
        upsert_scene_window(conn, scene_window)
        scene_count += 1

        artifact_roles: dict[str, tuple[str, float | None]] = {}
        preferred_artifact_type = choose_primary_source_type(
            [
                "app_metadata" if artifact["artifact_type"] == "app_metadata"
                else "screenshot" if artifact["artifact_type"] == "screenshot"
                else "desktop_audio" if artifact["artifact_type"] == "desktop_audio"
                else "ambient_audio" if artifact["artifact_type"] == "audio_chunk"
                else "camera_visual" if artifact["artifact_type"] in {"camera_frame", "video_excerpt", "gopro_frame"}
                else "camera_audio" if artifact["artifact_type"] in {"audio_excerpt", "gopro_audio"}
                else "codex_interpretation"
                for event in window
                for artifact in event.artifacts
            ],
            fallback="codex_interpretation",
        )
        for event in window:
            for artifact in event.artifacts:
                role = "supporting"
                artifact_source_type = (
                    "app_metadata"
                    if artifact["artifact_type"] == "app_metadata"
                    else "screenshot"
                    if artifact["artifact_type"] == "screenshot"
                    else "desktop_audio"
                    if artifact["artifact_type"] == "desktop_audio"
                    else "ambient_audio"
                    if artifact["artifact_type"] == "audio_chunk"
                    else "camera_visual"
                    if artifact["artifact_type"] in {"camera_frame", "video_excerpt", "gopro_frame"}
                    else "camera_audio"
                    if artifact["artifact_type"] in {"audio_excerpt", "gopro_audio"}
                    else "codex_interpretation"
                )
                if artifact_source_type == preferred_artifact_type and artifact_roles.get(artifact["artifact_id"]) is None:
                    role = "primary"
                artifact_roles.setdefault(artifact["artifact_id"], (role, artifact["confidence"]))
        for artifact_id, (role, confidence) in artifact_roles.items():
            upsert_scene_window_artifact(conn, scene_window["id"], artifact_id, role=role, confidence=confidence)

        claims = build_claims_for_window(
            scene_window["id"],
            window,
            scene_metadata,
            scene_window["confidence"],
            user_id=args.user_id,
            device_id=args.device_id,
            processor_version=args.processor_version,
        )
        for claim in claims:
            upsert_candidate_claim(conn, claim)
            attach_claim_evidence(conn, claim, window, scene_metadata)
            claim_count += 1

        fused_event = scene_event_from_claims(
            scene_window,
            scene_metadata,
            claims,
            processor_version=args.processor_version,
        )
        upsert_event(conn, event_record(fused_event))
        primary_artifact_id = next((artifact_id for artifact_id, (role, _) in artifact_roles.items() if role == "primary"), None)
        if primary_artifact_id:
            upsert_event_artifact(conn, fused_event["id"], primary_artifact_id, role="primary", confidence=fused_event["confidence"])
        for artifact_id in artifact_roles:
            if artifact_id == primary_artifact_id:
                continue
            upsert_event_artifact(conn, fused_event["id"], artifact_id, role="supporting")
        event_count += 1

    conn.close()
    print(f"source_events={len(contexts)}")
    print(f"scene_windows={scene_count}")
    print(f"candidate_claims={claim_count}")
    print(f"fused_events={event_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
