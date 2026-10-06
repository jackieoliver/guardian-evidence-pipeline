#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from guardian_pipeline_db import connect_db, fetch_scene_windows, init_db, json_loads


STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "for", "from",
    "get", "got", "had", "have", "i", "i'm", "if", "in", "is", "it", "its", "just",
    "like", "me", "my", "no", "not", "of", "oh", "on", "or", "our", "so", "that",
    "the", "their", "them", "there", "they", "this", "to", "uh", "um", "was", "we",
    "were", "what", "when", "with", "yeah", "you", "your",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce a human-readable overlap report between two device timelines.")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--user-id", required=True, help="User identifier")
    parser.add_argument("--left-device-id", required=True, help="First device ID")
    parser.add_argument("--right-device-id", required=True, help="Second device ID")
    parser.add_argument("--date", default=None, help="Optional UTC date filter YYYY-MM-DD")
    parser.add_argument(
        "--max-gap-minutes",
        type=float,
        default=90.0,
        help="Maximum wall-clock separation to consider when scoring candidate overlaps.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=12,
        help="Maximum candidate pairings to print.",
    )
    return parser.parse_args()


def parse_iso(timestamp: str) -> datetime:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)


def utc_day_bounds(date_text: str) -> tuple[str, str]:
    start = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start.replace(day=start.day)  # explicit copy
    from datetime import timedelta
    end = start + timedelta(days=1)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def text_blob(metadata: dict[str, Any]) -> str:
    pieces = [
        metadata.get("transcript_excerpt"),
        metadata.get("transcript"),
        metadata.get("visual_summary"),
        metadata.get("setting"),
    ]
    for key in ("scene_labels", "actions", "people_present", "notable_objects", "visible_text"):
        value = metadata.get(key)
        if isinstance(value, list):
            pieces.extend(str(item) for item in value)
    return " ".join(piece for piece in pieces if piece)


def tokenize(text: str) -> Counter[str]:
    tokens = re.findall(r"[a-z0-9][a-z0-9'-]+", text.lower())
    filtered = [token for token in tokens if token not in STOPWORDS and len(token) > 2]
    return Counter(filtered)


def cosine_similarity(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    shared = set(left) & set(right)
    numerator = sum(left[token] * right[token] for token in shared)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return numerator / (left_norm * right_norm)


def time_score(left_start: datetime, right_start: datetime, max_gap_minutes: float) -> float:
    gap_minutes = abs((left_start - right_start).total_seconds()) / 60.0
    if gap_minutes > max_gap_minutes:
        return 0.0
    return max(0.0, 1.0 - (gap_minutes / max_gap_minutes))


def excerpt(metadata: dict[str, Any], limit: int = 180) -> str:
    text = text_blob(metadata).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def main() -> int:
    args = parse_args()
    conn = connect_db(Path(args.db).expanduser().resolve())
    init_db(conn)

    start_at = end_at = None
    if args.date:
        start_at, end_at = utc_day_bounds(args.date)

    left_rows = fetch_scene_windows(conn, user_id=args.user_id, primary_device_id=args.left_device_id, start_at=start_at, end_at=end_at)
    right_rows = fetch_scene_windows(conn, user_id=args.user_id, primary_device_id=args.right_device_id, start_at=start_at, end_at=end_at)

    candidates: list[tuple[float, dict[str, Any]]] = []
    for left in left_rows:
        left_meta = json_loads(left["metadata_json"], default={}) or {}
        left_start = parse_iso(left["start_at"])
        left_tokens = tokenize(text_blob(left_meta))
        for right in right_rows:
            right_meta = json_loads(right["metadata_json"], default={}) or {}
            right_start = parse_iso(right["start_at"])
            token_score = cosine_similarity(left_tokens, tokenize(text_blob(right_meta)))
            timing = time_score(left_start, right_start, args.max_gap_minutes)
            if token_score == 0.0 and timing == 0.0:
                continue
            score = round((0.65 * token_score) + (0.35 * timing), 4)
            if score <= 0.0:
                continue
            candidates.append(
                (
                    score,
                    {
                        "left_id": left["id"],
                        "right_id": right["id"],
                        "left_start": left["start_at"],
                        "right_start": right["start_at"],
                        "gap_minutes": round(abs((left_start - right_start).total_seconds()) / 60.0, 2),
                        "token_score": round(token_score, 4),
                        "time_score": round(timing, 4),
                        "score": score,
                        "left_excerpt": excerpt(left_meta),
                        "right_excerpt": excerpt(right_meta),
                        "left_source": left["primary_source_type"],
                        "right_source": right["primary_source_type"],
                    },
                )
            )

    seen: set[tuple[str, str]] = set()
    ranked = []
    for _, payload in sorted(candidates, key=lambda item: (-item[0], item[1]["gap_minutes"], item[1]["left_start"])):
        key = (payload["left_id"], payload["right_id"])
        if key in seen:
            continue
        seen.add(key)
        ranked.append(payload)
        if len(ranked) >= args.top:
            break

    print(f"left_device={args.left_device_id} windows={len(left_rows)}")
    print(f"right_device={args.right_device_id} windows={len(right_rows)}")
    print(f"candidates={len(ranked)}")
    for idx, payload in enumerate(ranked, start=1):
        print()
        print(f"[{idx}] score={payload['score']:.4f} gap_min={payload['gap_minutes']:.2f} token={payload['token_score']:.4f} time={payload['time_score']:.4f}")
        print(f"  left  {payload['left_start']} {payload['left_source']}")
        print(f"    {payload['left_excerpt']}")
        print(f"  right {payload['right_start']} {payload['right_source']}")
        print(f"    {payload['right_excerpt']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
