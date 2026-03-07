"""Open Wearables MCP Server - Main entry point."""

import logging
from datetime import date

from fastmcp import FastMCP

from app.config import settings
from app.prompts import prompts_router
from app.tools.activity import activity_router
from app.tools.sleep import sleep_router
from app.tools.users import users_router
from app.tools.timeseries import timeseries_router
from app.tools.workouts import workouts_router
from app.tools.cardiac import cardiac_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Create FastMCP server instance
mcp = FastMCP(
    "open-wearables",
    instructions=f"""
    Today's date is {date.today().isoformat()}.

    Enables the model to query data describing user health states and general wellness metrics.
    Data is acquired from users' wearable devices (Garmin, Whoop, Polar, Suunto, etc.),
    covering all user-connected devices and providers, aggregated into a single unified format.

    Available tools:
    - get_users: Discover users accessible via your API key
    - get_activity_summary: Get daily activity data (steps, calories, heart rate, intensity minutes)
    - get_sleep_summary: Get sleep data for a user over a specified time period
    - get_workout_events: Get workout/exercise data for a user over a specified time period
    - get_timeseries: Get granular time series data (heart rate samples, HRV, SpO2, etc.)
    - get_cardiac_summary: Get daily cardiac summaries with POTS-relevant metrics
      (orthostatic HR delta, tachycardia minutes, HRV, SpO2, time blocks).
      Use this instead of get_timeseries when you need a compact cardiac overview
      rather than raw samples. Ideal for POTS monitoring and autonomic health checks.
      Use this for intra-day patterns, POTS monitoring, and any query needing individual
      data points rather than daily summaries. Supports type filtering, resolution
      downsampling, and cursor pagination.

    Available prompts:
    - present_health_data: Guidelines for formatting health data for human readability

    Workflow:
    1. If you don't know the user's ID, call get_users first to discover available users
    2. Select the appropriate user:
       - If only ONE user is returned: use that user automatically (personal API key)
       - If MULTIPLE users and query says "my" or "me": ask which user they mean
       - If MULTIPLE users with a name hint (e.g., "John's workouts"): match by name
    3. Determine the date range:
       - If user specifies a time period: calculate the appropriate start_date and end_date
       - If NO time period specified: default to the last 2 weeks (start_date = 14 days ago, end_date = today)
    4. Use the user's ID to query their health data with the appropriate tool
    5. Present the data in a human-friendly format, highlighting key insights

    Example interaction:
    User: "How many steps did I take this week?"
    Assistant actions:
      1. Call get_users() to find the user's ID
      2. Calculate dates: start_date = 7 days ago, end_date = today
      3. Call get_activity_summary(user_id="{{user_id}}", start_date="2026-01-28", end_date="2026-02-04")
      4. Respond with: "This week you walked 58,500 steps total, averaging 8,357 steps per day.
         Your best day was Saturday (12,432 steps), and you burned 2,450 active calories.
         You accumulated 90 minutes of vigorous activity across the week."

    Example interaction:
    User: "Fetch workouts for John"
    Assistant actions:
      1. Call get_users() to find John's user_id
      2. No time period specified, so default to last 2 weeks: start_date = 14 days ago, end_date = today
      3. Call get_workout_events(user_id="{{user_id}}", start_date="2026-01-21", end_date="2026-02-04")
      4. Respond with a summary of John's recent workouts

    Example interaction:
    User: "How did I sleep last week?"
    Assistant actions:
      1. Call get_users() to find the user's ID
      2. Calculate dates: start_date = 7 days ago, end_date = today
      3. Call get_sleep_summary(user_id="{{user_id}}", start_date="2026-01-28", end_date="2026-02-04")
      4. Respond with: "Over the past week, you averaged 7.2 hours of sleep per night.
         Your best night was Tuesday (8.1 hours), and your shortest was Friday (5.9 hours).
         Your sleep efficiency averaged 89%, which is good."

    Example interaction:
    User: "Compare my sleep this week vs last week"
    Assistant actions:
      1. Calculate dates for two-week period: start_date = 14 days ago, end_date = today
      2. Call get_sleep_summary(user_id="{{user_id}}", start_date="2026-01-21", end_date="2026-02-04")
      3. Analyze the data, splitting into two 7-day periods
      4. Respond with a comparison highlighting trends and changes

    Example interaction:
    User: "Show me my workouts this week"
    Assistant actions:
      1. Call get_users() to find the user's ID
      2. Calculate dates: start_date = 7 days ago, end_date = today
      3. Call get_workout_events(user_id="{{user_id}}", start_date="2026-01-28", end_date="2026-02-04")
      4. Respond with: "This week you completed 5 workouts totaling 3.5 hours.
         You ran 28.5 km across 3 running sessions and did 2 strength workouts.
         Your total calories burned was 2,100 kcal."

    Example interaction:
    User: "How many miles did I run last month?"
    Assistant actions:
      1. Calculate dates for last month: start_date = first of last month, end_date = last of last month
      2. Call get_workout_events(user_id="{{user_id}}", start_date="2026-01-01",
         end_date="2026-01-31", workout_type="running")
      3. Convert distance from km to miles and respond with the total

    Example interaction:
    User: "How's my POTS today?"
    Assistant actions:
      1. Call get_users() to find the user's ID
      2. Call get_cardiac_summary(user_id="{{user_id}}", start_date="2026-03-05", end_date="2026-03-05")
      3. Respond with: "Your orthostatic delta today is 42bpm (resting 75, walking 117) —
         that's above the POTS threshold. You had 12 minutes of tachycardia.
         Mornings were rougher (avg HR 82) compared to overnight (68).
         HRV averaged 35.8ms across 10 readings. One SpO2 dip below 90% overnight."

    Example interaction:
    User: "What was my heart rate doing this morning?"
    Assistant actions:
      1. Call get_users() to find the user's ID
      2. Calculate times: start_time = today 00:00:00, end_time = today 12:00:00
      3. Call get_timeseries(user_id="{{user_id}}", start_time="2026-03-05T00:00:00",
         end_time="2026-03-05T12:00:00", types=["heart_rate"], resolution="5min")
      4. Respond with patterns: resting rate overnight, any spikes, morning rise

    Example interaction:
    User: "Show my HRV trend this week"
    Assistant actions:
      1. Calculate times for the past 7 days
      2. Call get_timeseries(user_id="{{user_id}}", start_time="2026-02-26T00:00:00",
         end_time="2026-03-05T23:59:59", types=["heart_rate_variability_sdnn"], resolution="1hour")
      3. Respond with daily HRV averages, trends, and any notable changes

    The API key determines which users you can access (personal, team, or enterprise scope).
    All data is returned in a normalized format regardless of the original wearable provider.
    """,
)

# Mount tool routers
mcp.mount(users_router)
mcp.mount(activity_router)
mcp.mount(sleep_router)
mcp.mount(workouts_router)
mcp.mount(timeseries_router)
mcp.mount(cardiac_router)

# Mount prompts
mcp.mount(prompts_router)

logger.info(f"Open Wearables MCP server initialized. API URL: {settings.open_wearables_api_url}")


def main() -> None:
    """Entry point for the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
