"""Unit tests for the EIA ingestion client.

Uses respx to mock HTTP responses — no real API calls made.
"""

import logging

import pytest
import respx
from httpx import Response

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_eia_generation_response():
    return {
        "response": {
            "total": 2,
            "data": [
                {
                    "period": "2024-01",
                    "location": "TX",
                    "stateDescription": "Texas",
                    "sectorid": "99",
                    "sectorDescription": "All Sectors",
                    "fueltypeid": "NG",
                    "fuelTypeDescription": "Natural Gas",
                    "generation": "45678.9",
                    "generation-units": "thousand megawatthours",
                },
                {
                    "period": "2024-01",
                    "location": "TX",
                    "stateDescription": "Texas",
                    "sectorid": "99",
                    "sectorDescription": "All Sectors",
                    "fueltypeid": "SUN",
                    "fuelTypeDescription": "Solar",
                    "generation": "1234.5",
                    "generation-units": "thousand megawatthours",
                },
            ],
        }
    }


@pytest.fixture
def mock_eia_prices_response():
    return {
        "response": {
            "total": 1,
            "data": [
                {
                    "period": "2024-01",
                    "stateid": "TX",
                    "stateDescription": "Texas",
                    "sectorid": "RES",
                    "sectorName": "Residential",
                    "price": "12.45",
                    "revenue": "987.6",
                    "sales": "45678.9",
                    "customers": "8765432",
                }
            ],
        }
    }


class TestEIAClientGeneration:
    @respx.mock
    async def test_fetch_generation_success(self, mock_eia_generation_response):
        from ingestion.clients.eia_client import EIAClient

        respx.get("https://api.eia.gov/v2/electricity/electric-power-operational-data/data/").mock(
            return_value=Response(200, json=mock_eia_generation_response)
        )

        async with EIAClient() as client:
            result = await client.fetch_electricity_generation()

        assert "response" in result
        assert len(result["response"]["data"]) == 2
        logger.info("EIA generation mock returned %d rows", len(result["response"]["data"]))

    @respx.mock
    async def test_fetch_generation_pagination(self, mock_eia_generation_response):
        from ingestion.clients.eia_client import EIAClient

        # First page has data, second page is empty
        empty_response = {"response": {"total": 2, "data": []}}
        respx.get("https://api.eia.gov/v2/electricity/electric-power-operational-data/data/").mock(
            side_effect=[
                Response(200, json=mock_eia_generation_response),
                Response(200, json=empty_response),
            ]
        )

        async with EIAClient() as client:
            rows = await client.fetch_all_generation_pages()

        assert len(rows) == 2
        logger.info("Pagination test: collected %d rows", len(rows))

    @respx.mock
    async def test_fetch_generation_retries_on_503(self, mock_eia_generation_response):
        from ingestion.clients.eia_client import EIAClient

        respx.get("https://api.eia.gov/v2/electricity/electric-power-operational-data/data/").mock(
            side_effect=[
                Response(503),
                Response(503),
                Response(200, json=mock_eia_generation_response),
            ]
        )

        async with EIAClient() as client:
            result = await client.fetch_electricity_generation()

        assert "response" in result
        logger.info("Retry test: succeeded after 2 failures")

    @respx.mock
    async def test_fetch_generation_raises_on_404(self):
        from ingestion.clients.base_client import PermanentApiError
        from ingestion.clients.eia_client import EIAClient

        respx.get("https://api.eia.gov/v2/electricity/electric-power-operational-data/data/").mock(
            return_value=Response(404, text="Not found")
        )

        with pytest.raises(PermanentApiError):
            async with EIAClient() as client:
                await client.fetch_electricity_generation()

        logger.info("Correctly raised PermanentApiError on HTTP 404")


class TestEIAClientPrices:
    @respx.mock
    async def test_fetch_prices_success(self, mock_eia_prices_response):
        from ingestion.clients.eia_client import EIAClient

        respx.get("https://api.eia.gov/v2/electricity/retail-sales/data/").mock(
            return_value=Response(200, json=mock_eia_prices_response)
        )

        async with EIAClient() as client:
            result = await client.fetch_electricity_prices()

        assert len(result["response"]["data"]) == 1
        logger.info("EIA prices mock returned %d rows", len(result["response"]["data"]))
