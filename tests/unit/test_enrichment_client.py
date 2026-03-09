"""Unit tests for the enrichment API client."""

import logging
from datetime import datetime, timezone

import pytest
import respx
from httpx import Response

logger = logging.getLogger(__name__)

_WEATHER_RESP = {
    "region": "ERCOT",
    "temperature_c": 28.5,
    "wind_speed_ms": 6.2,
    "solar_irradiance_wm2": 750.0,
    "humidity_pct": 55.0,
    "cloud_cover_pct": 20.0,
    "weather_condition": "Clear",
    "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
}

_CARBON_RESP = {
    "region": "ERCOT",
    "fuel_type": "natural_gas",
    "direct_co2_per_mwh": 490.0,
    "lifecycle_co2_per_mwh": 490.0,
    "carbon_intensity_category": "medium",
    "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
}

_MARKET_RESP = {
    "region": "ERCOT",
    "spot_price_usd_mwh": 45.50,
    "peak_price_usd_mwh": 98.20,
    "off_peak_price_usd_mwh": 28.10,
    "demand_mw": 42000.0,
    "demand_forecast_mw": 43500.0,
    "price_signal": "normal",
    "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
}


class TestEnrichmentClientWeather:
    @respx.mock
    async def test_fetch_current_weather(self):
        from ingestion.clients.enrichment_client import EnrichmentClient

        respx.get("http://localhost:8000/weather/current").mock(
            return_value=Response(200, json=_WEATHER_RESP)
        )

        async with EnrichmentClient() as client:
            result = await client.fetch_current_weather("ERCOT")

        assert result["region"] == "ERCOT"
        assert result["temperature_c"] == 28.5
        logger.info("Enrichment weather fetch: region=%s temp=%.1f", result["region"], result["temperature_c"])

    @respx.mock
    async def test_fetch_all_regions_weather(self):
        from ingestion.clients.enrichment_client import EnrichmentClient

        respx.get("http://localhost:8000/weather/current").mock(
            return_value=Response(200, json=_WEATHER_RESP)
        )

        async with EnrichmentClient() as client:
            results = await client.fetch_all_regions_weather(regions=["ERCOT", "CAISO"])

        assert len(results) == 2
        logger.info("Bulk weather fetch: %d regions", len(results))

    @respx.mock
    async def test_retries_on_500(self):
        from ingestion.clients.enrichment_client import EnrichmentClient

        respx.get("http://localhost:8000/weather/current").mock(
            side_effect=[Response(500), Response(200, json=_WEATHER_RESP)]
        )

        async with EnrichmentClient() as client:
            result = await client.fetch_current_weather("ERCOT")

        assert result["region"] == "ERCOT"
        logger.info("Retry on 500 succeeded")


class TestEnrichmentClientCarbon:
    @respx.mock
    async def test_fetch_carbon_intensity(self):
        from ingestion.clients.enrichment_client import EnrichmentClient

        respx.get("http://localhost:8000/carbon/intensity").mock(
            return_value=Response(200, json=_CARBON_RESP)
        )

        async with EnrichmentClient() as client:
            result = await client.fetch_carbon_intensity("natural_gas", "ERCOT")

        assert result["fuel_type"] == "natural_gas"
        assert result["direct_co2_per_mwh"] == 490.0
        logger.info("Carbon intensity: fuel=%s direct=%.0f", result["fuel_type"], result["direct_co2_per_mwh"])


class TestEnrichmentClientMarket:
    @respx.mock
    async def test_fetch_market_prices(self):
        from ingestion.clients.enrichment_client import EnrichmentClient

        respx.get("http://localhost:8000/market/prices").mock(
            return_value=Response(200, json=_MARKET_RESP)
        )

        async with EnrichmentClient() as client:
            result = await client.fetch_market_prices("ERCOT")

        assert result["spot_price_usd_mwh"] == 45.50
        assert result["price_signal"] == "normal"
        logger.info("Market prices: region=%s spot=%.2f", result["region"], result["spot_price_usd_mwh"])
