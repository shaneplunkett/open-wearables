"""MCP tool for querying hydration data."""

import logging
from collections import defaultdict
from datetime import datetime

from fastmcp import FastMCP

from app.services.api_client import client
from app.utils import LOCAL_TZ

logger = logging.getLogger(__name__)

USER_ID = "23b9eb57-9f74-424e-b07e-e9b7174aa0c9"

hydration_router = FastMCP(name="Hydration Tools")


@hydration_router.tool
async def get_hydration_summary(start_date: str, end_date: str) -> dict:
    """Get daily hydration totals in mL.

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
    """
    try:
        response = await client.get_timeseries(
            user_id=USER_ID,
            start_time=f"{start_date}T00:00:00+11:00",
            end_time=f"{end_date}T23:59:59+11:00",
            types=["hydration"],
        )

        samples = response.get("data", [])

        # Group by Melbourne date
        by_date: dict[str, list[float]] = defaultdict(list)
        for sample in samples:
            ts = sample.get("timestamp")
            value = sample.get("value")
            if ts is None or value is None:
                continue
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
            by_date[dt.strftime("%Y-%m-%d")].append(float(value))

        records = []
        totals: list[float] = []
        for date_str in sorted(by_date):
            entries = by_date[date_str]
            total = round(sum(entries))
            totals.append(total)
            records.append(
                {
                    "date": date_str,
                    "total_ml": total,
                    "entries": len(entries),
                }
            )

        summary = {
            "total_days": len(records),
            "total_ml": round(sum(totals)) if totals else 0,
            "avg_daily_ml": round(sum(totals) / len(totals)) if totals else None,
        }

        return {
            "period": {"start": start_date, "end": end_date},
            "records": records,
            "summary": summary,
        }

    except Exception as e:
        logger.exception(f"Error in get_hydration_summary: {e}")
        return {"error": str(e)}
