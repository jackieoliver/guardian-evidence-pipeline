#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from guardian_capture_segments import iso_z, stable_id
from guardian_pipeline_db import (
    connect_db,
    episode_record,
    fetch_events_for_day,
    init_db,
    insert_episode_event,
    json_loads,
    list_event_days,
    replace_day_episodes,
    upsert_episode,
)


WORK_ACTIVITY_TYPES = {"development", "mixed_work"}
CAPTURE_ACTIVITY_TYPES = {"capture_control"}
INTERRUPTION_ACTIVITY_TYPES = {"browser", "browser_research", "unknown", "documents", "system_navigation", "system_settings"}


@dataclass
class EpisodeEventInput:
    row: sqlite3.Row
    event_id: str
    event_type: str
    start_at: datetime
    end_at: datetime
    activity_type: str
    app_name: str | None
    confidence: float
    summary: str
    signal_quality: str | None
    payload: dict[str, Any]

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.end_at - self.start_at).total_seconds())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic daily Guardian episodes from interpreted events."
    )
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--day", action="append", default=[], help="Specific UTC day(s) to rebuild, YYYY-MM-DD")
    parser.add_argument(
        "--event-type",
        action="append",
        dest="event_types",
        default=["activity_context"],
        help="Event type(s) to include. Repeatable; defaults to activity_context.",
    )
    parser.add_argument("--max-gap-seconds", type=float, default=300.0, help="Gap threshold for merging nearby events")
    parser.add_argument(
        "--interruption-max-seconds",
        type=float,
        default=120.0,
        help="Maximum duration for a non-work interruption inside a work block",
    )
    parser.add_argument("--builder-version", type=int, default=1, help="Episode builder version")
    return parser.parse_args()


def parse_iso(value: str | None) -> datetime:
    if not value:
        raise ValueError("timestamp is required")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def normalize_activity_type(row: sqlite3.Row, payload: dict[str, Any]) -> str:
    if row["event_type"] == "app_focus":
        activity = payload.get("category")
    else:
        activity = payload.get("activity_type") or payload.get("coarse_category")
    if not activity:
        return "unknown"
    return str(activity).strip().lower()


def resolve_app_name(row: sqlite3.Row, payload: dict[str, Any]) -> str | None:
    if row["event_type"] == "app_focus":
        return payload.get("frontmost_app_name")
    return payload.get("primary_app_name")


def resolve_summary(payload: dict[str, Any]) -> str:
    return payload.get("summary") or payload.get("specific_activity") or payload.get("coarse_category") or "Unclassified activity"


def resolve_signal_quality(payload: dict[str, Any]) -> str | None:
    quality = payload.get("quality_assessment") or {}
    signal_quality = quality.get("signal_quality")
    return str(signal_quality).lower() if signal_quality else None


def event_inputs(rows: list[sqlite3.Row]) -> list[EpisodeEventInput]:
    values: list[EpisodeEventInput] = []
    for row in rows:
        payload = json_loads(row["payload_json"], default={})
        start_at = parse_iso(row["start_at"])
        end_at = parse_iso(row["end_at"]) if row["end_at"] else start_at
        values.append(
            EpisodeEventInput(
                row=row,
                event_id=row["id"],
                event_type=row["event_type"],
                start_at=start_at,
                end_at=end_at,
                activity_type=normalize_activity_type(row, payload),
                app_name=resolve_app_name(row, payload),
                confidence=float(row["confidence"]),
                summary=resolve_summary(payload),
                signal_quality=resolve_signal_quality(payload),
                payload=payload,
            )
        )
    return values


def is_work_like(event: EpisodeEventInput) -> bool:
    return event.activity_type in WORK_ACTIVITY_TYPES


def is_capture_control(event: EpisodeEventInput) -> bool:
    return event.activity_type in CAPTURE_ACTIVITY_TYPES


def is_interruption_candidate(event: EpisodeEventInput) -> bool:
    return event.activity_type in INTERRUPTION_ACTIVITY_TYPES


def gap_seconds(left: EpisodeEventInput, right: EpisodeEventInput) -> float:
    return max(0.0, (right.start_at - left.end_at).total_seconds())


def should_absorb_as_interruption(
    events: list[EpisodeEventInput],
    current_idx: int,
    last_primary: EpisodeEventInput,
    max_gap_seconds: float,
    interruption_max_seconds: float,
) -> bool:
    candidate = events[current_idx]
    if not is_interruption_candidate(candidate):
        return False
    if candidate.duration_seconds > interruption_max_seconds:
        return False
    if gap_seconds(last_primary, candidate) > max_gap_seconds:
        return False
    if current_idx + 1 >= len(events):
        return False
    resumed = events[current_idx + 1]
    if not is_work_like(resumed):
        return False
    if gap_seconds(candidate, resumed) > max_gap_seconds:
        return False
    return True


def build_episode_groups(
    events: list[EpisodeEventInput],
    *,
    max_gap_seconds: float,
    interruption_max_seconds: float,
) -> list[tuple[str, list[tuple[EpisodeEventInput, str]]]]:
    groups: list[tuple[str, list[tuple[EpisodeEventInput, str]]]] = []
    idx = 0
    while idx < len(events):
        current = events[idx]

        if is_capture_control(current):
            refs: list[tuple[EpisodeEventInput, str]] = [(current, "primary")]
            idx += 1
            while idx < len(events):
                nxt = events[idx]
                if not is_capture_control(nxt) or gap_seconds(refs[-1][0], nxt) > max_gap_seconds:
                    break
                refs.append((nxt, "primary"))
                idx += 1
            groups.append(("capture_control", refs))
            continue

        if is_work_like(current):
            refs = [(current, "primary")]
            last_primary = current
            idx += 1
            while idx < len(events):
                nxt = events[idx]
                if is_capture_control(nxt) or gap_seconds(refs[-1][0], nxt) > max_gap_seconds:
                    break
                if is_work_like(nxt):
                    refs.append((nxt, "primary"))
                    last_primary = nxt
                    idx += 1
                    continue
                if should_absorb_as_interruption(
                    events,
                    idx,
                    last_primary,
                    max_gap_seconds=max_gap_seconds,
                    interruption_max_seconds=interruption_max_seconds,
                ):
                    refs.append((nxt, "interruption"))
                    idx += 1
                    continue
                break
            groups.append(("work_block", refs))
            continue

        refs = [(current, "primary")]
        idx += 1
        while idx < len(events):
            nxt = events[idx]
            if is_capture_control(nxt) or is_work_like(nxt) or gap_seconds(refs[-1][0], nxt) > max_gap_seconds:
                break
            refs.append((nxt, "primary"))
            idx += 1
        groups.append(("unclassified", refs))
    return groups


def dominant_value(values: list[str | None], fallback: str | None = None) -> str | None:
    counts: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    if not counts:
        return fallback
    return max(counts, key=counts.get)


def build_search_text(
    title: str | None,
    episode_type: str,
    refs: list[tuple[EpisodeEventInput, str]],
) -> str:
    fragments: list[str] = []
    if title:
        fragments.append(title)
    fragments.append(episode_type)
    fragments.extend(ref.summary for ref, _ in refs if ref.summary)
    fragments.extend(ref.activity_type for ref, _ in refs)
    fragments.extend(ref.app_name for ref, _ in refs if ref.app_name)
    search_text = " | ".join(dict.fromkeys(fragment.strip() for fragment in fragments if fragment))
    return search_text[:2000]


def build_title(episode_type: str, refs: list[tuple[EpisodeEventInput, str]]) -> str:
    apps = [ref.app_name for ref, role in refs if role == "primary"]
    dominant_app = dominant_value(apps, fallback=None)
    if episode_type == "capture_control":
        return f"Capture control{f' in {dominant_app}' if dominant_app else ''}"
    if episode_type == "work_block":
        return f"Work block{f' in {dominant_app}' if dominant_app else ''}"
    if dominant_app:
        return f"Unclassified activity in {dominant_app}"
    return "Unclassified activity"


def compute_confidence(episode_type: str, refs: list[tuple[EpisodeEventInput, str]]) -> float:
    primary = [ref for ref, role in refs if role == "primary"]
    interruptions = [ref for ref, role in refs if role == "interruption"]
    base = sum(ref.confidence for ref in (primary or [ref for ref, _ in refs])) / max(1, len(primary or refs))
    interruption_duration = sum(ref.duration_seconds for ref in interruptions)
    total_duration = max(1.0, sum(ref.duration_seconds for ref, _ in refs))
    low_signal_ratio = sum(1 for ref in primary if ref.signal_quality == "low") / max(1, len(primary))
    app_switches = len({ref.app_name for ref in primary if ref.app_name})

    if episode_type == "unclassified":
        base -= 0.15
    if interruptions:
        base -= 0.05
        base -= min(0.2, (interruption_duration / total_duration) * 0.25)
    if low_signal_ratio > 0.5:
        base -= 0.1
    if episode_type == "work_block" and len(primary) >= 2 and total_duration >= 600:
        base += 0.05
    if app_switches > 2:
        base -= 0.05
    return round(max(0.15, min(0.95, base)), 2)


def build_episode_metadata(
    day: str,
    episode_type: str,
    refs: list[tuple[EpisodeEventInput, str]],
    title: str,
) -> dict[str, Any]:
    primary = [ref for ref, role in refs if role == "primary"]
    interruptions = [ref for ref, role in refs if role == "interruption"]
    activity_counts: dict[str, int] = {}
    app_counts: dict[str, int] = {}
    signal_quality_counts: dict[str, int] = {}
    for ref, role in refs:
        activity_counts[ref.activity_type] = activity_counts.get(ref.activity_type, 0) + 1
        if ref.app_name:
            app_counts[ref.app_name] = app_counts.get(ref.app_name, 0) + 1
        if ref.signal_quality:
            signal_quality_counts[ref.signal_quality] = signal_quality_counts.get(ref.signal_quality, 0) + 1

    return {
        "day": day,
        "title": title,
        "event_count": len(refs),
        "primary_event_count": len(primary),
        "interruption_count": len(interruptions),
        "dominant_activity_type": dominant_value([ref.activity_type for ref in primary], fallback=episode_type),
        "dominant_app_name": dominant_value([ref.app_name for ref in primary], fallback=None),
        "activity_counts": activity_counts,
        "app_counts": app_counts,
        "signal_quality_counts": signal_quality_counts,
        "primary_duration_seconds": round(sum(ref.duration_seconds for ref in primary), 3),
        "interruption_duration_seconds": round(sum(ref.duration_seconds for ref in interruptions), 3),
        "event_summaries": [ref.summary for ref, _ in refs],
    }


def build_day_episodes(
    *,
    user_id: str,
    day: str,
    rows: list[sqlite3.Row],
    builder_version: int,
    max_gap_seconds: float,
    interruption_max_seconds: float,
) -> list[tuple[dict[str, Any], list[tuple[str, str]]]]:
    inputs = event_inputs(rows)
    groups = build_episode_groups(
        inputs,
        max_gap_seconds=max_gap_seconds,
        interruption_max_seconds=interruption_max_seconds,
    )
    built_at = iso_z(datetime.now(timezone.utc))
    episodes: list[tuple[dict[str, Any], list[tuple[str, str]]]] = []
    for refs_index, (episode_type, refs) in enumerate(groups, start=1):
        started_at = iso_z(refs[0][0].start_at)
        ended_at = iso_z(max(ref.end_at for ref, _ in refs))
        title = build_title(episode_type, refs)
        confidence = compute_confidence(episode_type, refs)
        metadata = build_episode_metadata(day, episode_type, refs, title)
        search_text = build_search_text(title, episode_type, refs)
        episode_id = stable_id(
            "guardian-episode",
            user_id,
            day,
            str(builder_version),
            str(refs_index),
            episode_type,
            started_at,
            ended_at,
        )
        record = episode_record(
            episode_id=episode_id,
            user_id=user_id,
            day=day,
            episode_type=episode_type,
            title=title,
            search_text=search_text,
            started_at=started_at,
            ended_at=ended_at,
            confidence=confidence,
            builder_version=builder_version,
            built_at=built_at,
            metadata=metadata,
        )
        links = [(ref.event_id, role) for ref, role in refs]
        episodes.append((record, links))
    return episodes


def main() -> int:
    args = parse_args()
    conn = connect_db(Path(args.db))
    init_db(conn)

    event_types = tuple(dict.fromkeys(args.event_types))
    days = args.day or list_event_days(conn, user_id=args.user_id, event_types=event_types)
    if not days:
        print("days=0 episodes=0")
        return 0

    total_episodes = 0
    for day in days:
        rows = fetch_events_for_day(conn, user_id=args.user_id, day=day, event_types=event_types)
        replace_day_episodes(conn, args.user_id, day)
        if not rows:
            print(f"day={day} events=0 episodes=0")
            continue
        episodes = build_day_episodes(
            user_id=args.user_id,
            day=day,
            rows=rows,
            builder_version=args.builder_version,
            max_gap_seconds=args.max_gap_seconds,
            interruption_max_seconds=args.interruption_max_seconds,
        )
        for record, links in episodes:
            upsert_episode(conn, record)
            for ordinal, (event_id, role) in enumerate(links, start=1):
                insert_episode_event(conn, record["id"], event_id, ordinal, role)
        total_episodes += len(episodes)
        print(f"day={day} events={len(rows)} episodes={len(episodes)}")

    print(f"days={len(days)} episodes={total_episodes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
