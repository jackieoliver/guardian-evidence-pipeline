"""Shared timezone helpers for Guardian pipeline.

Guardian operates in Pacific time (PDT/PST). All day boundaries should use
local midnight, not UTC midnight.

PDT = UTC-7 (March-November)
PST = UTC-8 (November-March)

For simplicity we use PDT (-7) year-round since most of our data is in
March-April 2026. A proper implementation would use zoneinfo.
"""
from datetime import datetime, timedelta, timezone

# PDT offset from UTC
PDT_OFFSET_HOURS = 7


def day_start_utc(day: str) -> str:
    """Return UTC timestamp for start of a PDT day (midnight PDT = 07:00 UTC)."""
    return f"{day}T{PDT_OFFSET_HOURS:02d}:00:00Z"


def day_end_utc(day: str) -> str:
    """Return UTC timestamp for end of a PDT day (next midnight PDT)."""
    next_day = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return f"{next_day}T{PDT_OFFSET_HOURS:02d}:00:00Z"


def utc_to_pdt_day(utc_timestamp: str) -> str:
    """Extract the PDT date from a UTC timestamp string."""
    # Parse and subtract 7 hours to get PDT
    if utc_timestamp and len(utc_timestamp) >= 19:
        try:
            dt = datetime.fromisoformat(utc_timestamp.replace("Z", "+00:00"))
            pdt = dt - timedelta(hours=PDT_OFFSET_HOURS)
            return pdt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    return utc_timestamp[:10] if utc_timestamp else ""


# SQL helper: use in queries as date(start_at, '-7 hours') for PDT day grouping
SQL_PDT_DATE = "date(start_at, '-7 hours')"
