"""Common utility functions for MCP tools."""

from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Australia/Melbourne")


def normalize_datetime(dt_str: str | None) -> str | None:
    """Normalize datetime string to local (Melbourne) time in ISO 8601 format."""
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(LOCAL_TZ).isoformat()
    except (ValueError, AttributeError):
        return dt_str
