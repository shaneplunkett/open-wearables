"""MCP tools for querying sleep records."""

import logging

from fastmcp import FastMCP

from app.services.api_client import client
from app.utils import normalize_datetime

logger = logging.getLogger(__name__)

USER_ID = "23b9eb57-9f74-424e-b07e-e9b7174aa0c9"

sleep_router = FastMCP(name="Sleep Tools")


@sleep_router.tool
async def get_sleep_summary(start_date: str, end_date: str) -> dict:
    """Get daily sleep summaries (duration, stages, physio metrics).

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
    """
    try:
        sleep_response = await client.get_sleep_summaries(
            user_id=USER_ID,
            start_date=start_date,
            end_date=end_date,
        )

        records_data = sleep_response.get("data", [])

        records = []
        durations: list[int] = []

        for record in records_data:
            duration = record.get("duration_minutes")
            if duration is not None:
                durations.append(duration)

            source = record.get("source", {})
            entry: dict = {
                "date": str(record.get("date")),
                "start_datetime": normalize_datetime(record.get("start_time")),
                "end_datetime": normalize_datetime(record.get("end_time")),
                "duration_minutes": duration,
                "time_in_bed_minutes": record.get("time_in_bed_minutes"),
                "efficiency_percent": record.get("efficiency_percent"),
                "source": source.get("provider") if isinstance(source, dict) else source,
            }

            stages = record.get("stages")
            if stages:
                entry["stages"] = stages

            for key in (
                "avg_heart_rate_bpm",
                "avg_hrv_sdnn_ms",
                "avg_respiratory_rate",
                "avg_spo2_percent",
            ):
                val = record.get(key)
                if val is not None:
                    entry[key] = val

            records.append(entry)

        summary: dict = {
            "total_nights": len(records),
            "nights_with_data": len(durations),
            "avg_duration_minutes": None,
            "min_duration_minutes": None,
            "max_duration_minutes": None,
        }

        if durations:
            avg = sum(durations) / len(durations)
            summary.update(
                {
                    "avg_duration_minutes": round(avg),
                    "min_duration_minutes": min(durations),
                    "max_duration_minutes": max(durations),
                }
            )

        return {
            "period": {"start": start_date, "end": end_date},
            "records": records,
            "summary": summary,
        }

    except Exception as e:
        logger.exception(f"Error in get_sleep_summary: {e}")
        return {"error": str(e)}
