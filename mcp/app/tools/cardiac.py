"""MCP tools for querying cardiac/POTS health data."""

import logging

from fastmcp import FastMCP

from app.services.api_client import client

logger = logging.getLogger(__name__)

# Create router for cardiac-related tools
cardiac_router = FastMCP(name="Cardiac Tools")


@cardiac_router.tool
async def get_cardiac_summary(
    user_id: str,
    start_date: str,
    end_date: str,
    timezone: str = "Australia/Melbourne",
) -> dict:
    """
    Get daily cardiac summaries with POTS-relevant metrics for a user.

    This tool retrieves pre-computed cardiac data optimised for autonomic
    nervous system monitoring, particularly Postural Orthostatic Tachycardia
    Syndrome (POTS). It replaces the need to fetch raw heart rate, HRV, and
    SpO2 time series and manually aggregate them.

    Key POTS metrics:
    - orthostatic_hr_delta_bpm: Walking HR minus resting HR.
      A delta >= 30bpm is the diagnostic threshold for POTS.
    - tachycardia_minutes: Minutes with HR > 100bpm at rest.
      More tachycardia minutes = worse autonomic day.
    - time_blocks: Shows the shape of the day (overnight/morning/afternoon/evening)
      so you can see if mornings are worse (common in POTS).

    Supporting metrics:
    - HRV (SDNN): Higher = better autonomic function. Low HRV correlates with flares.
    - SpO2: Desaturation events (below 90%) flag breathing issues during sleep.
    - Respiratory rate: Baseline tracking.

    Args:
        user_id: UUID of the user. Use get_users to discover available users.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        timezone: IANA timezone for time block boundaries (default: Australia/Melbourne).
                  Time blocks are meaningless in UTC — this ensures overnight means
                  local midnight to 6am.

    Returns:
        A dictionary containing:
        - user: Information about the user (id, first_name, last_name)
        - period: The date range queried
        - records: List of daily cardiac records with time blocks
        - summary: Aggregate statistics across the period (for multi-day queries)

    Example queries this tool answers:
        - "How's my heart been this week?"
        - "Check my POTS today"
        - "Am I having a good autonomic day?"
        - "Show me my orthostatic delta trend"
        - "Were my mornings worse than evenings this week?"
        - "Any SpO2 drops overnight?"

    Notes for LLMs:
        - Call get_users first to get the user_id if you don't have it.
        - For POTS monitoring, orthostatic_hr_delta_bpm is the most important metric.
          >= 30bpm is diagnostic, >= 40bpm is severe.
        - Compare time blocks to spot patterns: POTS often shows worse mornings.
        - Use the period summary for trend analysis over multiple days.
        - tachycardia_minutes correlates with symptom burden — more minutes = harder day.
        - HRV trending down over days may predict a flare before symptoms appear.
        - Use the present_health_data prompt for formatting guidelines.
    """
    try:
        # Fetch user details
        try:
            user_data = await client.get_user(user_id)
            user = {
                "id": str(user_data.get("id")),
                "first_name": user_data.get("first_name"),
                "last_name": user_data.get("last_name"),
            }
        except ValueError as e:
            return {"error": f"User not found: {user_id}", "details": str(e)}

        # Fetch cardiac data
        cardiac_response = await client.get_cardiac_summaries(
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
        )

        records_data = cardiac_response.get("data", [])

        # Collect period-level aggregates
        resting_hrs = []
        orthostatic_deltas = []
        hrv_avgs = []
        spo2_avgs = []
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

        # Build period summary
        summary = {
            "total_days": len(records_data),
            "avg_resting_hr_bpm": (
                round(sum(resting_hrs) / len(resting_hrs)) if resting_hrs else None
            ),
            "avg_orthostatic_delta_bpm": (
                round(sum(orthostatic_deltas) / len(orthostatic_deltas))
                if orthostatic_deltas
                else None
            ),
            "avg_hrv_sdnn_ms": (
                round(sum(hrv_avgs) / len(hrv_avgs), 1) if hrv_avgs else None
            ),
            "avg_spo2_percent": (
                round(sum(spo2_avgs) / len(spo2_avgs), 1) if spo2_avgs else None
            ),
            "worst_spo2_day": worst_spo2_day,
            "highest_orthostatic_delta_day": highest_delta_day,
        }

        return {
            "user": user,
            "period": {"start": start_date, "end": end_date},
            "records": records_data,
            "summary": summary,
        }

    except ValueError as e:
        logger.error(f"API error in get_cardiac_summary: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.exception(f"Unexpected error in get_cardiac_summary: {e}")
        return {"error": f"Failed to fetch cardiac summary: {e}"}
