#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import statistics
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from PIL import Image, ImageOps
except Exception:  # pragma: no cover - optional runtime dependency
    Image = None
    ImageOps = None


APP_CATEGORIES = {
    # Terminals (macOS + Linux)
    "terminal": "development",
    "iterm2": "development",
    "warp": "development",
    "ghostty": "development",
    "ptyxis": "development",
    "gnome-terminal": "development",
    "konsole": "development",
    "alacritty": "development",
    "kitty": "development",
    "claude code": "development",
    # Editors / IDEs
    "visual studio code": "development",
    "cursor": "development",
    "xcode": "development",
    # Browsers
    "google chrome": "browser",
    "chrome": "browser",
    "safari": "browser",
    "arc": "browser",
    "firefox": "browser",
    "brave browser": "browser",
    # Capture control
    "guardiancapturemac": "capture_control",
    "guardian capture mac": "capture_control",
    # GNOME compositor (screenpipe reports this, not the real app)
    "gnome-shell": "unknown",
    "mutter": "unknown",
    # Communication
    "slack": "communication",
    "discord": "communication",
    "signal": "communication",
    "messages": "communication",
    # Meetings
    "zoom": "meeting",
    "microsoft teams": "meeting",
    "meet": "meeting",
    # Documents
    "notion": "documents",
    "preview": "documents",
    "google docs": "documents",
    "loupe": "documents",
    "eye of gnome": "documents",
    # System / navigation (macOS + Linux)
    "finder": "system_navigation",
    "org.gnome.nautilus": "system_navigation",
    "nautilus": "system_navigation",
    "system settings": "system_settings",
    "gnome-control-center": "system_settings",
}

VISUAL_DUPLICATE_HAMMING_THRESHOLD = 6
VISUAL_SEGMENT_BRIDGE_HAMMING_THRESHOLD = 10
VISUAL_COMPARE_GAP_SECONDS = 300.0
IDLE_RATIO_FLAG_THRESHOLD = 0.6
UNCHANGED_RATIO_FLAG_THRESHOLD = 0.6
INTERPRETATION_SKIP_MIN_CAPTURES = 4
INTERPRETATION_SKIP_MAX_NOVEL_CAPTURES = 2
INTERPRETATION_SKIP_NEAR_DUPLICATE_RATIO = 0.85
INTERPRETATION_SKIP_IDLE_RATIO = 0.5
RETENTION_ACTIVE = "active"
RETENTION_SUPPRESSED = "suppressed"
RETENTION_QUARANTINED = "quarantined"


@dataclass
class Observation:
    json_path: Path
    image_path: Path | None
    timestamp: datetime
    app_name: str
    window_title: str
    window_id: int | None
    width: float | None
    height: float | None
    platform: str | None
    raw: dict[str, Any]
    seconds_since_last_user_input: float | None = None
    was_input_idle: bool | None = None
    sha256: str | None = None
    visual_dhash: int | None = None
    exact_duplicate_previous: bool = False
    near_duplicate_previous: bool = False
    visual_hamming_distance_previous: int | None = None


def server_gvfs_roots() -> list[Path]:
    gvfs_root = Path(f"/run/user/{os.getuid()}/gvfs")
    if not gvfs_root.exists():
        return []
    return sorted(path for path in gvfs_root.iterdir() if path.is_dir() and path.name.startswith("sftp:host="))


def resolve_processing_path(path_value: str | Path | None, *, anchors: list[Path] | None = None) -> Path | None:
    if path_value is None:
        return None

    raw_path = Path(path_value).expanduser()
    candidates: list[Path] = []

    if raw_path.is_absolute():
        candidates.append(raw_path)
        if str(raw_path).startswith("/data/"):
            relative = raw_path.relative_to("/")
            for root in server_gvfs_roots():
                candidates.append(root / relative)
    else:
        for anchor in anchors or []:
            candidates.append(anchor / raw_path)

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    return raw_path.resolve() if raw_path.exists() else None


def resolve_capture_image_path(json_path: Path, payload: dict[str, Any]) -> Path | None:
    image_path_value = payload.get("imagePath") or payload.get("image_path")
    resolved = resolve_processing_path(image_path_value, anchors=[json_path.parent, json_path.parent.parent])
    if resolved is not None:
        return resolved

    for suffix in (".jpg", ".png", ".jpeg"):
        sibling = json_path.with_suffix(suffix)
        if sibling.exists():
            return sibling.resolve()
    return None


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return None
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def parse_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def category_for_app(app_name: str) -> str:
    key = app_name.strip().lower()
    if key in APP_CATEGORIES:
        return APP_CATEGORIES[key]
    if "chrome" in key or "safari" in key or "firefox" in key or "browser" in key:
        return "browser"
    if "terminal" in key or "shell" in key or "console" in key:
        return "development"
    if "code" in key or "vim" in key or "emacs" in key or "neovim" in key:
        return "development"
    if "nautilus" in key or "files" in key:
        return "system_navigation"
    return "unknown"


def stable_id(prefix: str, *parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, prefix + "|" + "|".join(parts)))


def file_sha256(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def image_dhash(path: Path) -> int | None:
    if Image is None or ImageOps is None:
        return None
    try:
        with Image.open(path) as image:
            grayscale = ImageOps.exif_transpose(image).convert("L").resize((9, 8))
            pixels = list(grayscale.getdata())
    except Exception:
        return None

    bits = 0
    for y in range(8):
        row_offset = y * 9
        for x in range(8):
            bits = (bits << 1) | int(pixels[row_offset + x] > pixels[row_offset + x + 1])
    return bits


def visual_hamming_distance(left: int | None, right: int | None) -> int | None:
    if left is None or right is None:
        return None
    return (left ^ right).bit_count()


def observations_are_visually_similar(left: Observation, right: Observation, *, threshold: int) -> bool:
    if left.sha256 and right.sha256 and left.sha256 == right.sha256:
        return True
    distance = visual_hamming_distance(left.visual_dhash, right.visual_dhash)
    return distance is not None and distance <= threshold


def maybe_persist_visual_cache(observation: Observation) -> None:
    if not observation.image_path or not observation.image_path.exists():
        return

    updates: dict[str, Any] = {}
    if observation.sha256 and observation.raw.get("sha256") != observation.sha256:
        updates["sha256"] = observation.sha256
    if observation.visual_dhash is not None and observation.raw.get("visual_dhash") != observation.visual_dhash:
        updates["visual_dhash"] = observation.visual_dhash
    if observation.raw.get("visual_hash_algorithm") != "dhash-v1":
        updates["visual_hash_algorithm"] = "dhash-v1"

    if not updates:
        return

    observation.raw.update(updates)
    try:
        observation.json_path.write_text(json.dumps(observation.raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError:
        return


def enrich_visual_change_signals(observations: list[Observation]) -> None:
    previous: Observation | None = None
    for observation in observations:
        if observation.image_path and observation.image_path.exists():
            observation.sha256 = observation.sha256 or file_sha256(observation.image_path)
            observation.visual_dhash = observation.visual_dhash if observation.visual_dhash is not None else image_dhash(observation.image_path)
            maybe_persist_visual_cache(observation)

        if previous is None:
            previous = observation
            continue

        gap_seconds = (observation.timestamp - previous.timestamp).total_seconds()
        if gap_seconds > VISUAL_COMPARE_GAP_SECONDS:
            previous = observation
            continue

        if observation.sha256 and previous.sha256 and observation.sha256 == previous.sha256:
            observation.exact_duplicate_previous = True
            observation.near_duplicate_previous = True
            observation.visual_hamming_distance_previous = 0
            previous = observation
            continue

        distance = visual_hamming_distance(observation.visual_dhash, previous.visual_dhash)
        observation.visual_hamming_distance_previous = distance
        if distance is not None and distance <= VISUAL_DUPLICATE_HAMMING_THRESHOLD:
            observation.near_duplicate_previous = True

        previous = observation


def retention_state(payload: dict[str, Any]) -> str:
    value = str(payload.get("retention_state") or "").strip().lower()
    if value == "restored":
        return RETENTION_ACTIVE
    if value in {RETENTION_ACTIVE, RETENTION_SUPPRESSED, RETENTION_QUARANTINED}:
        return value
    legacy_upload_status = str(payload.get("upload_sync_status") or "").strip().lower()
    if legacy_upload_status == RETENTION_SUPPRESSED:
        return RETENTION_SUPPRESSED
    return RETENTION_ACTIVE


def load_observations(root: Path, *, include_suppressed: bool = False) -> list[Observation]:
    observations: list[Observation] = []
    resolved_root = resolve_processing_path(root, anchors=[Path.cwd()]) or root.expanduser()
    for path in sorted(resolved_root.rglob("*.json")):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if not include_suppressed and retention_state(payload) in {RETENTION_SUPPRESSED, RETENTION_QUARANTINED}:
            continue

        timestamp_utc = payload.get("timestampUTC") or payload.get("timestamp_utc")
        if not timestamp_utc:
            continue

        app_name = (
            payload.get("frontmostAppName")
            or payload.get("foregroundApp")
            or payload.get("foreground_app")
            or payload.get("foreground_window")
            or payload.get("foregroundWindow")
            or "unknown"
        )
        bounds = payload.get("windowBounds") or payload.get("window_bounds") or {}
        observations.append(
            Observation(
                json_path=path,
                image_path=resolve_capture_image_path(path, payload),
                timestamp=parse_timestamp(timestamp_utc),
                app_name=app_name,
                window_title=(
                    payload.get("windowTitle")
                    or payload.get("foreground_window")
                    or payload.get("foregroundWindow")
                    or ""
                ).strip(),
                window_id=payload.get("windowID"),
                width=bounds.get("width") or payload.get("width"),
                height=bounds.get("height") or payload.get("height"),
                platform=payload.get("platform") or ("linux" if payload.get("capture_method") or payload.get("captureMethod") else None),
                raw=payload,
                seconds_since_last_user_input=parse_optional_float(
                    first_present(payload, "secondsSinceLastUserInput", "seconds_since_last_user_input")
                ),
                was_input_idle=parse_optional_bool(first_present(payload, "wasInputIdle", "was_input_idle")),
                sha256=(first_present(payload, "sha256") or None),
                visual_dhash=parse_optional_int(first_present(payload, "visual_dhash")),
            )
        )
    observations.sort(key=lambda item: item.timestamp)
    enrich_visual_change_signals(observations)
    return observations


def segment_observations(observations: list[Observation], gap_seconds: float) -> list[list[Observation]]:
    if not observations:
        return []

    segments: list[list[Observation]] = [[observations[0]]]
    visual_anchor = observations[0]
    for current in observations[1:]:
        current_segment = segments[-1]
        previous = current_segment[-1]
        current_app = current.app_name.strip().lower()
        previous_app = previous.app_name.strip().lower()
        same_app = current_app == previous_app and current_app not in {"", "unknown"}
        same_title = bool(current.window_title and previous.window_title and current.window_title == previous.window_title)
        same_visual_run = current.near_duplicate_previous
        same_visual_anchor = observations_are_visually_similar(
            current,
            visual_anchor,
            threshold=VISUAL_SEGMENT_BRIDGE_HAMMING_THRESHOLD,
        )
        gap = (current.timestamp - previous.timestamp).total_seconds()
        if gap <= gap_seconds and (same_app or same_title or same_visual_run or same_visual_anchor):
            current_segment.append(current)
            if not same_visual_anchor and not current.near_duplicate_previous:
                visual_anchor = current
            continue
        segments.append([current])
        visual_anchor = current
    return segments


def summarize_titles(segment: list[Observation], limit: int = 3) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    for item in segment:
        title = item.window_title
        if not title or title in seen:
            continue
        seen.add(title)
        titles.append(title)
        if len(titles) == limit:
            break
    return titles


def summarize_flags(segment: list[Observation], category: str) -> list[str]:
    heights = [item.height for item in segment if item.height is not None]
    title_count = sum(1 for item in segment if item.window_title)
    title_rate = title_count / len(segment)
    idle_count = sum(1 for item in segment if item.was_input_idle is True)
    unchanged_count = sum(1 for item in segment[1:] if item.near_duplicate_previous)
    flags: list[str] = []

    if heights and statistics.median(heights) <= 100:
        flags.append("thin_window_strip")
    if title_rate < 0.2:
        flags.append("missing_window_titles")
    if len(segment) == 1:
        flags.append("brief_glimpse")
    if category == "capture_control":
        flags.append("self_capture")
    if idle_count / len(segment) >= IDLE_RATIO_FLAG_THRESHOLD:
        flags.append("mostly_idle")
    if unchanged_count / max(1, len(segment) - 1) >= UNCHANGED_RATIO_FLAG_THRESHOLD:
        flags.append("mostly_unchanged")
    if "mostly_idle" in flags and "mostly_unchanged" in flags:
        flags.append("idle_unchanged_run")
    return flags


def segment_confidence(segment: list[Observation], flags: list[str]) -> float:
    score = 0.9
    if "thin_window_strip" in flags:
        score -= 0.25
    if "missing_window_titles" in flags:
        score -= 0.15
    if "brief_glimpse" in flags:
        score -= 0.1
    if "mostly_idle" in flags:
        score -= 0.2
    if "mostly_unchanged" in flags:
        score -= 0.15
    if "idle_unchanged_run" in flags:
        score -= 0.1
    return round(max(0.2, score), 2)


def select_sample_images(segment: list[Observation], limit: int = 3) -> list[str]:
    representative_candidates = [
        item.image_path
        for index, item in enumerate(segment)
        if item.image_path
        and item.image_path.exists()
        and (index == 0 or not item.near_duplicate_previous)
    ]
    image_candidates = representative_candidates or [item.image_path for item in segment if item.image_path and item.image_path.exists()]
    if not image_candidates:
        return []

    if len(image_candidates) <= limit:
        return [str(path) for path in image_candidates]

    indexes = sorted({0, len(image_candidates) // 2, len(image_candidates) - 1})
    sampled = [image_candidates[idx] for idx in indexes][:limit]
    return [str(path) for path in sampled]


def interpretation_skip_reasons(
    *,
    category: str,
    capture_count: int,
    idle_capture_ratio: float,
    near_duplicate_ratio: float,
    novel_capture_count: int,
    title_present_rate: float,
    flags: list[str],
) -> list[str]:
    reasons: list[str] = []
    low_signal_unchanged_run = (
        capture_count >= 3
        and "mostly_unchanged" in flags
        and "missing_window_titles" in flags
    )

    if low_signal_unchanged_run:
        reasons.append("low_signal_duplicate_run")

    if "idle_unchanged_run" in flags and capture_count >= 2:
        reasons.append("idle_unchanged_run")

    duplicate_heavy = (
        capture_count >= INTERPRETATION_SKIP_MIN_CAPTURES
        and near_duplicate_ratio >= INTERPRETATION_SKIP_NEAR_DUPLICATE_RATIO
        and novel_capture_count <= INTERPRETATION_SKIP_MAX_NOVEL_CAPTURES
    )
    low_signal = (
        title_present_rate < 0.2
        or "missing_window_titles" in flags
        or category == "unknown"
    )

    if duplicate_heavy and low_signal and "low_signal_duplicate_run" not in reasons:
        reasons.append("low_signal_duplicate_run")
    elif duplicate_heavy and idle_capture_ratio >= INTERPRETATION_SKIP_IDLE_RATIO:
        reasons.append("idle_duplicate_run")

    return reasons


def build_segment_packet(
    segment: list[Observation],
    user_id: str,
    device_id: str,
    segment_index: int | None = None,
    image_limit: int = 3,
) -> dict[str, Any]:
    start = segment[0]
    end = segment[-1]
    category = category_for_app(start.app_name)
    flags = summarize_flags(segment, category)
    confidence = segment_confidence(segment, flags)
    widths = [item.width for item in segment if item.width is not None]
    heights = [item.height for item in segment if item.height is not None]
    titles = summarize_titles(segment)
    title_present_rate = round(sum(1 for item in segment if item.window_title) / len(segment), 3)
    idle_count = sum(1 for item in segment if item.was_input_idle is True)
    exact_duplicate_count = sum(1 for item in segment[1:] if item.exact_duplicate_previous)
    near_duplicate_count = sum(1 for item in segment[1:] if item.near_duplicate_previous)
    novel_capture_count = max(1, len(segment) - near_duplicate_count)
    idle_capture_ratio = round(idle_count / len(segment), 3)
    near_duplicate_ratio = round(near_duplicate_count / max(1, len(segment) - 1), 3)
    skip_reasons = interpretation_skip_reasons(
        category=category,
        capture_count=len(segment),
        idle_capture_ratio=idle_capture_ratio,
        near_duplicate_ratio=near_duplicate_ratio,
        novel_capture_count=novel_capture_count,
        title_present_rate=title_present_rate,
        flags=flags,
    )
    packet_id = stable_id(
        "guardian-segment-packet",
        user_id,
        device_id,
        start.timestamp.isoformat(),
        end.timestamp.isoformat(),
        start.app_name,
        str(start.window_id or ""),
    )

    duration_seconds = round((end.timestamp - start.timestamp).total_seconds(), 3)
    return {
        "id": packet_id,
        "segment_index": segment_index,
        "user_id": user_id,
        "device_id": device_id,
        "start_at": iso_z(start.timestamp),
        "end_at": iso_z(end.timestamp),
        "duration_seconds": duration_seconds,
        "primary_app_name": start.app_name,
        "coarse_category": category,
        "platform": start.platform,
        "capture_count": len(segment),
        "idle_capture_count": idle_count,
        "idle_capture_ratio": idle_capture_ratio,
        "exact_duplicate_capture_count": exact_duplicate_count,
        "near_duplicate_capture_count": near_duplicate_count,
        "near_duplicate_ratio": near_duplicate_ratio,
        "novel_capture_count": novel_capture_count,
        "window_id": start.window_id,
        "window_title_examples": titles,
        "window_title_present_rate": title_present_rate,
        "window_bounds_summary": {
            "median_width": round(statistics.median(widths), 1) if widths else None,
            "median_height": round(statistics.median(heights), 1) if heights else None,
        },
        "quality_flags": flags,
        "segment_confidence": confidence,
        "should_queue_interpretation": not skip_reasons,
        "interpretation_skip_reasons": skip_reasons,
        "sample_image_paths": select_sample_images(segment, limit=image_limit),
        "source_paths": {
            "first_json_path": str(start.json_path),
            "last_json_path": str(end.json_path),
            "first_image_path": str(start.image_path) if start.image_path else None,
            "last_image_path": str(end.image_path) if end.image_path else None,
        },
    }


def build_segment_packets(
    segments: list[list[Observation]],
    user_id: str,
    device_id: str,
    image_limit: int = 3,
) -> list[dict[str, Any]]:
    packets: list[dict[str, Any]] = []
    for index, segment in enumerate(segments, start=1):
        packets.append(build_segment_packet(segment, user_id, device_id, segment_index=index, image_limit=image_limit))
    return packets


def build_app_focus_event(
    segment: list[Observation],
    user_id: str,
    device_id: str,
    created_by: str = "guardian_metadata_categorizer_v1",
) -> dict[str, Any]:
    packet = build_segment_packet(segment, user_id, device_id)
    created_at = iso_z(datetime.now(timezone.utc))
    return {
        "id": stable_id(
            "guardian-event",
            user_id,
            device_id,
            packet["start_at"],
            packet["end_at"],
            packet["primary_app_name"],
            str(packet["window_id"] or ""),
        ),
        "user_id": user_id,
        "device_id": device_id,
        "event_type": "app_focus",
        "start_at": packet["start_at"],
        "end_at": packet["end_at"],
        "source_priority": 1,
        "confidence": packet["segment_confidence"],
        "primary_source_type": "app_metadata",
        "created_by": created_by,
        "created_at": created_at,
        "payload": {
            "category": packet["coarse_category"],
            "frontmost_app_name": packet["primary_app_name"],
            "platform": packet["platform"],
            "capture_count": packet["capture_count"],
            "idle_capture_count": packet["idle_capture_count"],
            "idle_capture_ratio": packet["idle_capture_ratio"],
            "exact_duplicate_capture_count": packet["exact_duplicate_capture_count"],
            "near_duplicate_capture_count": packet["near_duplicate_capture_count"],
            "near_duplicate_ratio": packet["near_duplicate_ratio"],
            "novel_capture_count": packet["novel_capture_count"],
            "should_queue_interpretation": packet["should_queue_interpretation"],
            "interpretation_skip_reasons": packet["interpretation_skip_reasons"],
            "window_id": packet["window_id"],
            "window_title_examples": packet["window_title_examples"],
            "window_title_present_rate": packet["window_title_present_rate"],
            "window_bounds_summary": packet["window_bounds_summary"],
            "flags": packet["quality_flags"],
            "first_json_path": packet["source_paths"]["first_json_path"],
            "last_json_path": packet["source_paths"]["last_json_path"],
            "first_image_path": packet["source_paths"]["first_image_path"],
            "last_image_path": packet["source_paths"]["last_image_path"],
            "segment_packet_id": packet["id"],
        },
    }


def build_day_summary_event(
    observations: list[Observation],
    events: list[dict[str, Any]],
    user_id: str,
    device_id: str,
    created_by: str = "guardian_metadata_categorizer_v1",
) -> dict[str, Any]:
    first = observations[0]
    last = observations[-1]
    category_counts: dict[str, int] = {}
    app_counts: dict[str, int] = {}
    idle_capture_count = 0
    near_duplicate_capture_count = 0
    suppressed_segment_count = 0
    suppressed_capture_count = 0

    for event in events:
        category = event["payload"]["category"]
        app_name = event["payload"]["frontmost_app_name"]
        category_counts[category] = category_counts.get(category, 0) + event["payload"]["capture_count"]
        app_counts[app_name] = app_counts.get(app_name, 0) + event["payload"]["capture_count"]
        idle_capture_count += int(event["payload"].get("idle_capture_count") or 0)
        near_duplicate_capture_count += int(event["payload"].get("near_duplicate_capture_count") or 0)
        if event["payload"].get("should_queue_interpretation") is False:
            suppressed_segment_count += 1
            suppressed_capture_count += int(event["payload"].get("capture_count") or 0)

    dominant_category = max(category_counts, key=category_counts.get) if category_counts else "unknown"
    dominant_app = max(app_counts, key=app_counts.get) if app_counts else "unknown"
    created_at = iso_z(datetime.now(timezone.utc))
    return {
        "id": stable_id(
            "guardian-summary",
            user_id,
            device_id,
            first.timestamp.date().isoformat(),
            first.timestamp.isoformat(),
            last.timestamp.isoformat(),
        ),
        "user_id": user_id,
        "device_id": device_id,
        "event_type": "activity_context",
        "start_at": iso_z(first.timestamp),
        "end_at": iso_z(last.timestamp),
        "source_priority": 1,
        "confidence": 0.75,
        "primary_source_type": "app_metadata",
        "created_by": created_by,
        "created_at": created_at,
        "payload": {
            "summary_type": "capture_day_overview",
            "capture_count": len(observations),
            "segment_count": len(events),
            "idle_capture_count": idle_capture_count,
            "near_duplicate_capture_count": near_duplicate_capture_count,
            "suppressed_segment_count": suppressed_segment_count,
            "suppressed_capture_count": suppressed_capture_count,
            "dominant_category": dominant_category,
            "dominant_app_name": dominant_app,
            "category_counts": category_counts,
            "app_counts": app_counts,
        },
    }
