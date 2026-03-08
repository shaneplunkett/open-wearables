"""HTTP client for Open Wearables backend API."""

import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class OpenWearablesClient:
    """Client for interacting with Open Wearables REST API."""

    def __init__(self) -> None:
        self.base_url = settings.open_wearables_api_url.rstrip("/")
        self.timeout = settings.request_timeout
        self._api_key = settings.open_wearables_api_key.get_secret_value()

    @property
    def headers(self) -> dict[str, str]:
        return {
            "X-Open-Wearables-API-Key": self._api_key,
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as http_client:
            response = await http_client.request(
                method=method,
                url=url,
                headers=self.headers,
                **kwargs,
            )
            response.raise_for_status()
            return response.json()

    async def get_activity_summaries(
        self, user_id: str, start_date: str, end_date: str, limit: int = 100
    ) -> dict[str, Any]:
        params = {"start_date": start_date, "end_date": end_date, "limit": limit}
        return await self._request("GET", f"/api/v1/users/{user_id}/summaries/activity", params=params)

    async def get_sleep_summaries(
        self, user_id: str, start_date: str, end_date: str, limit: int = 100
    ) -> dict[str, Any]:
        params = {"start_date": start_date, "end_date": end_date, "limit": limit}
        return await self._request("GET", f"/api/v1/users/{user_id}/summaries/sleep", params=params)

    async def get_cardiac_summaries(
        self,
        user_id: str,
        start_date: str,
        end_date: str,
        timezone: str = "Australia/Melbourne",
        limit: int = 100,
    ) -> dict[str, Any]:
        params = {"start_date": start_date, "end_date": end_date, "timezone": timezone, "limit": limit}
        return await self._request("GET", f"/api/v1/users/{user_id}/summaries/cardiac", params=params)


client = OpenWearablesClient()
