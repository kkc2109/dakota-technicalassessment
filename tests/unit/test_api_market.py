"""Unit tests for the FastAPI market endpoints."""

import logging

import pytest
from fastapi.testclient import TestClient

logger = logging.getLogger(__name__)


@pytest.fixture
def client():
    from api.app.main import app
    return TestClient(app)


REGIONS = ["ERCOT", "CAISO", "PJM", "MISO", "NYISO", "NEISO", "SPP", "WECC", "SERC"]


class TestMarketPricesEndpoint:
    def test_default_returns_200(self, client):
        logger.info("Testing GET /market/prices (default region)")
        response = client.get("/market/prices")
        assert response.status_code == 200

    def test_response_schema(self, client):
        response = client.get("/market/prices?region=ERCOT")
        assert response.status_code == 200
        data = response.json()
        logger.info("Market prices response: %s", data)

        required_fields = [
            "region", "spot_price_usd_mwh", "peak_price_usd_mwh",
            "off_peak_price_usd_mwh", "demand_mw", "demand_forecast_mw",
            "price_signal", "recorded_at",
        ]
        for field in required_fields:
            assert field in data, f"Missing field: {field}"

    def test_all_regions(self, client):
        for region in REGIONS:
            response = client.get(f"/market/prices?region={region}")
            assert response.status_code == 200
            data = response.json()
            assert data["spot_price_usd_mwh"] > 0
            logger.info("Region %-8s spot=%.2f demand=%.0f MW", region, data["spot_price_usd_mwh"], data["demand_mw"])

    def test_prices_positive(self, client):
        for region in REGIONS:
            data = client.get(f"/market/prices?region={region}").json()
            assert data["spot_price_usd_mwh"] > 0
            assert data["peak_price_usd_mwh"] > 0
            assert data["off_peak_price_usd_mwh"] > 0

    def test_demand_non_negative(self, client):
        data = client.get("/market/prices?region=ERCOT").json()
        assert data["demand_mw"] >= 0
        assert data["demand_forecast_mw"] >= 0

    def test_price_signal_valid_values(self, client):
        valid_signals = {"low", "normal", "high", "critical"}
        for region in REGIONS:
            data = client.get(f"/market/prices?region={region}").json()
            assert data["price_signal"] in valid_signals

    def test_peak_price_greater_than_off_peak(self, client):
        """Peak electricity prices should always exceed off-peak prices."""
        for _ in range(5):
            data = client.get("/market/prices?region=PJM").json()
            assert data["peak_price_usd_mwh"] > data["off_peak_price_usd_mwh"]


class TestDemandForecastEndpoint:
    def test_default_24h_forecast(self, client):
        logger.info("Testing GET /market/demand-forecast (24h)")
        response = client.get("/market/demand-forecast?region=ERCOT")
        assert response.status_code == 200
        data = response.json()
        assert data["forecast_hours"] == 24
        assert len(data["forecast"]) == 24

    def test_custom_hours(self, client):
        response = client.get("/market/demand-forecast?region=CAISO&hours=72")
        assert response.status_code == 200
        assert len(response.json()["forecast"]) == 72

    def test_forecast_points_schema(self, client):
        response = client.get("/market/demand-forecast?region=PJM&hours=5")
        data = response.json()
        for pt in data["forecast"]:
            assert "forecast_hour" in pt
            assert "forecast_demand_mw" in pt
            assert "confidence_pct" in pt
            assert pt["forecast_demand_mw"] >= 0
            assert 0 <= pt["confidence_pct"] <= 100

    def test_confidence_decreases_with_horizon(self, client):
        """Forecast confidence should degrade over longer time horizons."""
        response = client.get("/market/demand-forecast?region=MISO&hours=48")
        forecast = response.json()["forecast"]
        # First hour confidence should be higher than last hour
        assert forecast[0]["confidence_pct"] > forecast[-1]["confidence_pct"]
        logger.info(
            "Confidence: hour=1 %.1f%% → hour=48 %.1f%%",
            forecast[0]["confidence_pct"], forecast[-1]["confidence_pct"]
        )
