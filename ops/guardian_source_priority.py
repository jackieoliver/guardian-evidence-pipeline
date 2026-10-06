#!/usr/bin/env python3
from __future__ import annotations

from typing import Iterable


SOURCE_PRIORITY_ORDER = {
    "app_metadata": 1,
    "screenshot": 2,
    "screen_ocr": 3,
    "desktop_audio": 4,
    "ambient_audio": 5,
    "camera_visual": 6,
    "camera_audio": 7,
    "codex_interpretation": 8,
    "unknown": 99,
}


DIRECT_DEVICE_SOURCE_TYPES = {
    "app_metadata",
    "screenshot",
    "screen_ocr",
    "desktop_audio",
    "ambient_audio",
}


INDIRECT_CAMERA_SOURCE_TYPES = {
    "camera_visual",
    "camera_audio",
}


def source_priority_for_type(source_type: str | None) -> int:
    if not source_type:
        return SOURCE_PRIORITY_ORDER["unknown"]
    return SOURCE_PRIORITY_ORDER.get(source_type, SOURCE_PRIORITY_ORDER["unknown"])


def is_direct_device_source(source_type: str | None) -> bool:
    return bool(source_type and source_type in DIRECT_DEVICE_SOURCE_TYPES)


def choose_primary_source_type(source_types: Iterable[str], fallback: str = "codex_interpretation") -> str:
    available = [source_type for source_type in source_types if source_type]
    if not available:
        return fallback
    return min(available, key=source_priority_for_type)
