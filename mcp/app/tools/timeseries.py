"""MCP tools for querying granular time series health data."""

import logging

from fastmcp import FastMCP

from app.services.api_client import client

logger = logging.getLogger(__name__)

# Create router for timeseries tools
timeseries_router = FastMCP(name="Timeseries Tools")


@timeseries_router.tool
async def get_timeseries(
    user_id: str,
    start_time: str,
    end_time: str,
    types: list[str] | None = None,
    resolution: str = "raw",
    limit: int = 50,
    cursor: str | None = None,
) -> dict:
    """
    Get granular time series health data for a user within a time range.

    Returns individual data point samples (e.g. every heart rate reading from
    an Apple Watch) rather than daily summaries. Essential for tracking
    intra-day patterns and autonomic conditions like POTS.

    Args:
        user_id: UUID of the user. Use get_users to discover available users.
        start_time: Start of time range in ISO 8601 format.
                    Example: "2026-03-05T00:00:00+11:00" or "2026-03-05T00:00:00"
        end_time: End of time range in ISO 8601 format.
                  Example: "2026-03-05T23:59:59+11:00" or "2026-03-05T23:59:59"
        types: Optional list of series types to filter. If omitted, returns all
               available types in the time range. Common POTS-relevant types:
               - "heart_rate" (bpm) - continuous HR from wearable
               - "resting_heart_rate" (bpm) - daily resting HR
               - "heart_rate_variability_sdnn" (ms) - HRV readings
               - "walking_heart_rate_average" (bpm) - average HR while walking
               - "oxygen_saturation" (%) - SpO2 readings
               - "respiratory_rate" (brpm) - breathing rate
               - "steps" (count) - step count samples
               Other available types include: heart_rate_variability_rmssd,
               heart_rate_recovery_one_minute, recovery_score, blood_pressure_systolic,
               blood_pressure_diastolic, body_temperature, skin_temperature,
               weight, vo2_max, energy, basal_energy, garmin_stress_level,
               garmin_body_battery, and many more.
        resolution: Data granularity. One of:
                    - "raw" (default) - every individual sample
                    - "1min" - 1-minute averages
                    - "5min" - 5-minute averages
                    - "15min" - 15-minute averages
                    - "1hour" - hourly averages
                    Use coarser resolutions for longer time ranges to reduce data volume.
        limit: Maximum number of samples to return per page (1-100, default 50).
        cursor: Pagination cursor from a previous response's next_cursor field.
                Pass this to retrieve the next page of results.

    Returns:
        A dictionary containing:
        - data: List of time series samples, each with:
            - timestamp: ISO 8601 timestamp of the reading
            - type: The series type (e.g. "heart_rate")
            - value: The numeric value
            - unit: The unit of measurement (e.g. "bpm", "ms", "percent")
            - source: Optional metadata about the data source/device
        - next_cursor: Pagination cursor for the next page (null if no more data)
        - previous_cursor: Pagination cursor for the previous page

    Notes for LLMs:
        - Call get_users first to get the user_id if you don't have it.
        - For POTS monitoring, the most useful queries are:
          * Heart rate throughout a day: types=["heart_rate"], resolution="1min"
          * HRV trends over a week: types=["heart_rate_variability_sdnn"], resolution="1hour"
          * SpO2 readings: types=["oxygen_saturation"]
          * Walking HR (orthostatic indicator): types=["walking_heart_rate_average"]
        - Use resolution="raw" for short ranges (hours), coarser for longer ranges.
        - Paginate with cursor for large result sets. Keep calling with next_cursor
          until it returns null.
        - Timestamps include timezone info. Data from Apple Watch is typically in
          the user's local timezone.
        - Use the present_health_data prompt for formatting guidelines.
    """
    try:
        response = await client.get_timeseries(
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            types=types,
            resolution=resolution,
            limit=limit,
            cursor=cursor,
        )
        return response

    except ValueError as e:
        logger.error(f"API error in get_timeseries: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.exception(f"Unexpected error in get_timeseries: {e}")
        return {"error": f"Failed to fetch timeseries data: {e}"}
