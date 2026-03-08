"""MCP tools for querying cardiac/POTS health data."""

import logging

from fastmcp import FastMCP

from app.services.api_client import client

logger = logging.getLogger(__name__)

USER_ID = "23b9eb57-9f74-424e-b07e-e9b7174aa0c9"

cardiac_router = FastMCP(name="Cardiac Tools")


@cardiac_router.tool
async def get_cardiac_summary(
    start_date: str,
    end_date: str,
    timezone: str = "Australia/Melbourne",
) -> dict:
    """Get daily cardiac summaries with POTS metrics (orthostatic delta, HRV, tachycardia, SpO2, time blocks).

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        timezone: IANA timezone for time blocks (default: Australia/Melbourne)
    """
    try:
        cardiac_response = await client.get_cardiac_summaries(
            user_id=USER_ID,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
        )

        records_data = cardiac_response.get("data", [])

        resting_hrs: list[int] = []
        orthostatic_deltas: list[int] = []
        hrv_avgs: list[float] = []
        spo2_avgs: list[float] = []
        worst_spo2_day = None
        highest_delta_day = None

        for record in records_data:
            rhr = record.get("resting_heart_rate_bpm")
            delta = record.get("orthostatic_hr_delta_bpm")
            hrv = record.get("hrv")
            spo2 = record.get("spo2")
            record_date = record.get("date")

            if rhr is not None:
                resting_hrs.append(rhr)
            if delta is not None:
                orthostatic_deltas.append(delta)
                if highest_delta_day is None or delta > highest_delta_day["delta_bpm"]:
                    highest_delta_day = {"date": record_date, "delta_bpm": delta}
            if hrv and hrv.get("avg_sdnn_ms") is not None:
                hrv_avgs.append(hrv["avg_sdnn_ms"])
            if spo2 and spo2.get("min_percent") is not None:
                spo2_avgs.append(spo2["avg_percent"])
                if worst_spo2_day is None or spo2["min_percent"] < worst_spo2_day["min_percent"]:
                    worst_spo2_day = {"date": record_date, "min_percent": spo2["min_percent"]}

        summary = {
            "total_days": len(records_data),
            "avg_resting_hr_bpm": round(sum(resting_hrs) / len(resting_hrs)) if resting_hrs else None,
            "avg_orthostatic_delta_bpm": (
                round(sum(orthostatic_deltas) / len(orthostatic_deltas)) if orthostatic_deltas else None
            ),
            "avg_hrv_sdnn_ms": round(sum(hrv_avgs) / len(hrv_avgs), 1) if hrv_avgs else None,
            "avg_spo2_percent": round(sum(spo2_avgs) / len(spo2_avgs), 1) if spo2_avgs else None,
            "worst_spo2_day": worst_spo2_day,
            "highest_orthostatic_delta_day": highest_delta_day,
        }

        return {
            "period": {"start": start_date, "end": end_date},
            "records": records_data,
            "summary": summary,
        }

    except Exception as e:
        logger.exception(f"Error in get_cardiac_summary: {e}")
        return {"error": str(e)}
