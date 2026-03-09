"""Unit tests for the FastAPI weather endpoints."""

import logging

import pytest
from fastapi.testclient import TestClient

logger = logging.getLogger(__name__)


@pytest.fixture
def client():
    from api.app.main import app
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        logger.info("Testing /health endpoint")
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        logger.info("Health check passed: %s", data)

    def test_health_contains_service_name(self, client):
        response = client.get("/health")
        assert "service" in response.json()
        assert "version" in response.json()


class TestWeatherCurrentEndpoint:
    def test_default_region_returns_200(self, client):
        logger.info("Testing GET /weather/current (default region)")
        response = client.get("/weather/current")
        assert response.status_code == 200

    def test_response_schema(self, client):
        response = client.get("/weather/current?region=ERCOT")
        assert response.status_code == 200
        data = response.json()
        logger.info("Weather response: %s", data)

        assert "region" in data
        assert data["region"] == "ERCOT"
        assert "temperature_c" in data
        assert "wind_speed_ms" in data
        assert "solar_irradiance_wm2" in data
        assert "humidity_pct" in data
        assert "cloud_cover_pct" in data
        assert "weather_condition" in data
        assert "recorded_at" in data

    def test_all_regions_return_200(self, client):
        regions = ["ERCOT", "CAISO", "PJM", "MISO", "NYISO", "NEISO", "SPP", "WECC", "SERC"]
        for region in regions:
            response = client.get(f"/weather/current?region={region}")
            assert response.status_code == 200, f"Region {region} failed"
            logger.info("Region %s: temp=%.1f°C", region, response.json()["temperature_c"])

    def test_unknown_region_falls_back(self, client):
        response = client.get("/weather/current?region=UNKNOWN_GRID")
        assert response.status_code == 200  # falls back to default

    def test_wind_speed_non_negative(self, client):
        for _ in range(5):
            response = client.get("/weather/current?region=SPP")
            assert response.json()["wind_speed_ms"] >= 0

    def test_humidity_in_range(self, client):
        response = client.get("/weather/current?region=CAISO")
        data = response.json()
        assert 0 <= data["humidity_pct"] <= 100
        assert 0 <= data["cloud_cover_pct"] <= 100

    def test_solar_irradiance_non_negative(self, client):
        response = client.get("/weather/current?region=ERCOT")
        assert response.json()["solar_irradiance_wm2"] >= 0


class TestWeatherForecastEndpoint:
    def test_default_forecast_returns_24_hours(self, client):
        logger.info("Testing GET /weather/forecast (24h default)")
        response = client.get("/weather/forecast?region=ERCOT")
        assert response.status_code == 200
        data = response.json()
        assert data["forecast_hours"] == 24
        assert len(data["forecast"]) == 24

    def test_custom_hours(self, client):
        response = client.get("/weather/forecast?region=PJM&hours=48")
        assert response.status_code == 200
        data = response.json()
        assert data["forecast_hours"] == 48
        assert len(data["forecast"]) == 48

    def test_max_hours_168(self, client):
        response = client.get("/weather/forecast?region=MISO&hours=168")
        assert response.status_code == 200
        assert len(response.json()["forecast"]) == 168

    def test_exceeding_max_returns_422(self, client):
        response = client.get("/weather/forecast?region=MISO&hours=200")
        assert response.status_code == 422

    def test_forecast_points_schema(self, client):
        response = client.get("/weather/forecast?region=CAISO&hours=3")
        data = response.json()
        for pt in data["forecast"]:
            assert "forecast_hour" in pt
            assert "temperature_c" in pt
            assert "precipitation_mm" in pt
            assert pt["precipitation_mm"] >= 0
