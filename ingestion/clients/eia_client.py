"""EIA API v2 client."""

import logging
from typing import Any

from ingestion.clients.base_client import BaseHttpClient
from ingestion.config import settings

logger = logging.getLogger(__name__)

_EIA_BASE = "https://api.eia.gov/v2"

# EIA v2 endpoint paths
_GENERATION_PATH = "electricity/electric-power-operational-data/data/"
_PRICES_PATH = "electricity/retail-sales/data/"


class EIAClient(BaseHttpClient):
    """Client for EIA API v2 electricity data endpoints."""

    def __init__(self) -> None:
        super().__init__(
            base_url=_EIA_BASE,
            timeout_s=settings.eia_request_timeout_s,
            max_retries=settings.eia_max_retries,
        )
        self._api_key = settings.eia_api_key

    def _base_params(self) -> dict[str, Any]:
        return {"api_key": self._api_key}

    async def fetch_electricity_generation(
        self,
        frequency: str = "monthly",
        start_period: str | None = None,
        end_period: str | None = None,
        states: list[str] | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Fetch electricity generation by fuel type and state.

        Args:
            frequency: Data frequency — "monthly" or "annual".
            start_period: ISO period start e.g. "2023-01".
            end_period: ISO period end e.g. "2024-12".
            states: List of US state codes to filter by.
            offset: Pagination offset.

        Returns:
            Raw EIA API response dict.
        """
        params: dict[str, Any] = {
            **self._base_params(),
            "frequency": frequency,
            "data[0]": "generation",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": settings.eia_page_size,
            "offset": offset,
        }

        if start_period:
            params["start"] = start_period
        if end_period:
            params["end"] = end_period
        if states:
            for i, state in enumerate(states):
                params[f"facets[location][{i}]"] = state

        logger.info(
            "Fetching EIA generation data frequency=%s start=%s end=%s states=%s offset=%d",
            frequency, start_period, end_period, states, offset,
        )
        return await self._get(_GENERATION_PATH, params=params)

    async def fetch_electricity_prices(
        self,
        frequency: str = "monthly",
        start_period: str | None = None,
        end_period: str | None = None,
        states: list[str] | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Fetch retail electricity prices by sector and state.

        Args:
            frequency: Data frequency — "monthly" or "annual".
            start_period: ISO period start e.g. "2023-01".
            end_period: ISO period end e.g. "2024-12".
            states: List of US state codes to filter by.
            offset: Pagination offset.

        Returns:
            Raw EIA API response dict.
        """
        params: dict[str, Any] = {
            **self._base_params(),
            "frequency": frequency,
            "data[0]": "price",
            "data[1]": "revenue",
            "data[2]": "sales",
            "data[3]": "customers",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": settings.eia_page_size,
            "offset": offset,
        }

        if start_period:
            params["start"] = start_period
        if end_period:
            params["end"] = end_period
        if states:
            for i, state in enumerate(states):
                params[f"facets[stateid][{i}]"] = state

        logger.info(
            "Fetching EIA prices data frequency=%s start=%s end=%s states=%s offset=%d",
            frequency, start_period, end_period, states, offset,
        )
        return await self._get(_PRICES_PATH, params=params)

    async def fetch_all_generation_pages(
        self,
        frequency: str = "monthly",
        start_period: str | None = None,
        end_period: str | None = None,
        states: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all generation records and return a flat list of rows.

        EIA paginates with `offset` + `length`. This method handles pagination
        transparently and returns the combined data rows.
        """
        all_rows: list[dict[str, Any]] = []
        offset = 0
        page = 0

        while True:
            page += 1
            response = await self.fetch_electricity_generation(
                frequency=frequency,
                start_period=start_period,
                end_period=end_period,
                states=states,
                offset=offset,
            )

            data = response.get("response", {})
            rows = data.get("data", [])
            total = data.get("total", 0)

            if not rows:
                logger.info("EIA generation pagination complete: %d total rows across %d pages", len(all_rows), page - 1)
                break

            all_rows.extend(rows)
            logger.info("EIA generation page=%d rows=%d cumulative=%d total=%s", page, len(rows), len(all_rows), total)

            if len(all_rows) >= int(total or 0):
                break
            offset += settings.eia_page_size

        return all_rows

    async def fetch_all_prices_pages(
        self,
        frequency: str = "monthly",
        start_period: str | None = None,
        end_period: str | None = None,
        states: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all price records and return a flat list of rows."""
        all_rows: list[dict[str, Any]] = []
        offset = 0
        page = 0

        while True:
            page += 1
            response = await self.fetch_electricity_prices(
                frequency=frequency,
                start_period=start_period,
                end_period=end_period,
                states=states,
                offset=offset,
            )

            data = response.get("response", {})
            rows = data.get("data", [])
            total = data.get("total", 0)

            if not rows:
                logger.info("EIA prices pagination complete: %d total rows across %d pages", len(all_rows), page - 1)
                break

            all_rows.extend(rows)
            logger.info("EIA prices page=%d rows=%d cumulative=%d total=%s", page, len(rows), len(all_rows), total)

            if len(all_rows) >= int(total or 0):
                break
            offset += settings.eia_page_size

        return all_rows
