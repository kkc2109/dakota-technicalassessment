"""Internal enrichment API client."""

import logging
from typing import Any

from ingestion.clients.base_client import BaseHttpClient
from ingestion.config import settings

logger = logging.getLogger(__name__)


class EnrichmentClient(BaseHttpClient):
    """Client for the Dakota enrichment FastAPI service."""

    def __init__(self) -> None:
        super().__init__(
            base_url=settings.enrichment_api_url,
            timeout_s=settings.enrichment_request_timeout_s,
            max_retries=settings.enrichment_max_retries,
        )


    async def fetch_current_weather(self, region: str) -> dict[str, Any]:
        """Fetch current weather conditions for a grid region."""
        logger.info("Fetching current weather for region=%s", region)
        return await self._get("/weather/current", params={"region": region})

    async def fetch_weather_forecast(self, region: str, hours: int = 24) -> dict[str, Any]:
        """Fetch hourly weather forecast for a grid region."""
        logger.info("Fetching %d-hour weather forecast for region=%s", hours, region)
        return await self._get("/weather/forecast", params={"region": region, "hours": hours})


    async def fetch_carbon_intensity(self, fuel_type: str, region: str) -> dict[str, Any]:
        """Fetch carbon intensity for a given fuel type and region."""
        logger.info("Fetching carbon intensity region=%s fuel=%s", region, fuel_type)
        return await self._get("/carbon/intensity", params={"fuel_type": fuel_type, "region": region})

    async def fetch_carbon_factors(self) -> dict[str, Any]:
        """Fetch the full emission factor reference table."""
        logger.info("Fetching carbon emission factors table")
        return await self._get("/carbon/factors")

    async def fetch_market_prices(self, region: str) -> dict[str, Any]:
        """Fetch current electricity market prices for a grid region."""
        logger.info("Fetching market prices for region=%s", region)
        return await self._get("/market/prices", params={"region": region})

    async def fetch_demand_forecast(self, region: str, hours: int = 24) -> dict[str, Any]:
        """Fetch hourly demand forecast for a grid region."""
        logger.info("Fetching %d-hour demand forecast for region=%s", hours, region)
        return await self._get("/market/demand-forecast", params={"region": region, "hours": hours})

    async def fetch_all_regions_weather(
        self, regions: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch current weather for all configured grid regions."""
        regions = regions or settings.enrichment_regions
        results: list[dict[str, Any]] = []
        for region in regions:
            data = await self.fetch_current_weather(region)
            results.append(data)
        logger.info("Fetched weather for %d regions", len(results))
        return results

    async def fetch_all_regions_carbon(
        self, regions: list[str] | None = None, fuel_types: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch carbon intensity for all regions × fuel type combinations."""
        regions = regions or settings.enrichment_regions
        fuel_types = fuel_types or [
            "coal", "natural_gas", "nuclear", "wind", "solar_pv", "hydro", "petroleum"
        ]
        results: list[dict[str, Any]] = []
        for region in regions:
            for fuel in fuel_types:
                data = await self.fetch_carbon_intensity(fuel_type=fuel, region=region)
                results.append(data)
        logger.info("Fetched carbon intensity for %d region×fuel combinations", len(results))
        return results

    async def fetch_all_regions_market(
        self, regions: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch market prices for all configured grid regions."""
        regions = regions or settings.enrichment_regions
        results: list[dict[str, Any]] = []
        for region in regions:
            data = await self.fetch_market_prices(region)
            results.append(data)
        logger.info("Fetched market prices for %d regions", len(results))
        return results

    async def fetch_all_regions_demand_forecast(
        self, regions: list[str] | None = None, hours: int = 24
    ) -> list[dict[str, Any]]:
        """Fetch demand forecasts for all configured grid regions."""
        regions = regions or settings.enrichment_regions
        results: list[dict[str, Any]] = []
        for region in regions:
            data = await self.fetch_demand_forecast(region=region, hours=hours)
            results.append(data)
        logger.info("Fetched demand forecasts for %d regions", len(results))
        return results
