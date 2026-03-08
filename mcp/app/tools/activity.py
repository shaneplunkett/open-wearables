"""MCP tools for querying activity records."""

import logging

from fastmcp import FastMCP

from app.services.api_client import client

logger = logging.getLogger(__name__)

USER_ID = "23b9eb57-9f74-424e-b07e-e9b7174aa0c9"

activity_router = FastMCP(name="Activity Tools")


@activity_router.tool
async def get_activity_summary(start_date: str, end_date: str) -> dict:
    """Get daily activity summaries (steps, calories, distance, heart rate).

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
    """
    try:
        activity_response = await client.get_activity_summaries(
            user_id=USER_ID,
            start_date=start_date,
            end_date=end_date,
        )

        records_data = activity_response.get("data", [])

        records = []
        steps_list: list[int] = []
        distances: list[float] = []
        active_calories: list[float] = []
        total_calories: list[float] = []
        active_minutes_list: list[int] = []

        for record in records_data:
            steps = record.get("steps")
            distance = record.get("distance_meters")
            active_cal = record.get("active_calories_kcal")
            total_cal = record.get("total_calories_kcal")
            active_mins = record.get("active_minutes")
            intensity = record.get("intensity_minutes") or {}
            heart_rate = record.get("heart_rate")

            if steps is not None:
                steps_list.append(steps)
            if distance is not None:
                distances.append(distance)
            if active_cal is not None:
                active_calories.append(active_cal)
            if total_cal is not None:
                total_calories.append(total_cal)
            if active_mins is not None:
                active_minutes_list.append(active_mins)

            source = record.get("source", {})
            records.append(
                {
                    "date": str(record.get("date")),
                    "steps": steps,
                    "distance_meters": distance,
                    "active_calories_kcal": active_cal,
                    "total_calories_kcal": total_cal,
                    "active_minutes": active_mins,
                    "sedentary_minutes": record.get("sedentary_minutes"),
                    "heart_rate": heart_rate,
                    "intensity_minutes": intensity if intensity else None,
                    "floors_climbed": record.get("floors_climbed"),
                    "source": source.get("provider") if isinstance(source, dict) else source,
                }
            )

        total_steps = sum(steps_list) if steps_list else None
        summary = {
            "total_days": len(records),
            "days_with_data": len(steps_list),
            "total_steps": total_steps,
            "avg_steps": round(total_steps / len(steps_list)) if steps_list and total_steps is not None else None,
            "total_distance_meters": sum(distances) if distances else None,
            "total_active_calories_kcal": round(sum(active_calories), 1) if active_calories else None,
            "total_calories_kcal": round(sum(total_calories), 1) if total_calories else None,
            "avg_active_minutes": (
                round(sum(active_minutes_list) / len(active_minutes_list)) if active_minutes_list else None
            ),
        }

        return {
            "period": {"start": start_date, "end": end_date},
            "records": records,
            "summary": summary,
        }

    except Exception as e:
        logger.exception(f"Error in get_activity_summary: {e}")
        return {"error": str(e)}
