"""Open Wearables MCP Server - Main entry point."""

import logging
from datetime import date

from fastmcp import FastMCP

from app.config import settings
from app.tools.activity import activity_router
from app.tools.cardiac import cardiac_router
from app.tools.sleep import sleep_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "open-wearables",
    instructions=f"""Today is {date.today().isoformat()}.

Health data from Shane's Apple Watch via Auto Export.
Three tools: activity (steps/calories/distance/HR), sleep (duration/stages/physio), cardiac (POTS/HRV/orthostatic).
All take start_date and end_date in YYYY-MM-DD format. Default to last 7 days if unspecified.
""",
)

mcp.mount(activity_router)
mcp.mount(sleep_router)
mcp.mount(cardiac_router)

logger.info(f"Open Wearables MCP server initialized. API URL: {settings.open_wearables_api_url}")


def main() -> None:
    """Entry point for the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
