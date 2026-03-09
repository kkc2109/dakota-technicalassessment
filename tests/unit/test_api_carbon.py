"""Unit tests for the FastAPI carbon intensity endpoints."""

import logging

import pytest
from fastapi.testclient import TestClient

logger = logging.getLogger(__name__)


@pytest.fixture
def client():
    from api.app.main import app
    return TestClient(app)


VALID_FUEL_TYPES = [
    "coal", "natural_gas", "petroleum", "nuclear",
    "hydro", "wind", "solar_pv", "biomass", "geothermal", "other",
]


class TestCarbonIntensityEndpoint:
    def test_default_params_return_200(self, client):
        logger.info("Testing GET /carbon/intensity (default params)")
        response = client.get("/carbon/intensity")
        assert response.status_code == 200

    def test_response_schema(self, client):
        response = client.get("/carbon/intensity?fuel_type=coal&region=ERCOT")
        assert response.status_code == 200
        data = response.json()
        logger.info("Carbon intensity response: %s", data)

        assert "region" in data
        assert "fuel_type" in data
        assert "direct_co2_per_mwh" in data
        assert "lifecycle_co2_per_mwh" in data
        assert "carbon_intensity_category" in data
        assert "recorded_at" in data

    def test_all_fuel_types(self, client):
        for fuel in VALID_FUEL_TYPES:
            response = client.get(f"/carbon/intensity?fuel_type={fuel}&region=PJM")
            assert response.status_code == 200
            data = response.json()
            assert data["fuel_type"] == fuel
            assert data["direct_co2_per_mwh"] >= 0
            logger.info("Fuel %-15s direct_co2=%.1f lifecycle_co2=%.1f", fuel, data["direct_co2_per_mwh"], data["lifecycle_co2_per_mwh"])

    def test_direct_co2_non_negative(self, client):
        for fuel in VALID_FUEL_TYPES:
            data = client.get(f"/carbon/intensity?fuel_type={fuel}").json()
            assert data["direct_co2_per_mwh"] >= 0
            assert data["lifecycle_co2_per_mwh"] >= 0

    def test_renewable_fuels_have_zero_direct_co2(self, client):
        zero_direct = ["nuclear", "hydro", "wind", "solar_pv"]
        for fuel in zero_direct:
            data = client.get(f"/carbon/intensity?fuel_type={fuel}").json()
            assert data["direct_co2_per_mwh"] == 0.0, f"{fuel} should have zero direct CO2"
            logger.info("Confirmed zero direct CO2 for %s", fuel)

    def test_fossil_fuels_have_high_category(self, client):
        for fuel in ["coal", "petroleum"]:
            data = client.get(f"/carbon/intensity?fuel_type={fuel}").json()
            assert data["carbon_intensity_category"] == "high"

    def test_renewables_have_low_category(self, client):
        for fuel in ["wind", "solar_pv", "nuclear", "hydro"]:
            data = client.get(f"/carbon/intensity?fuel_type={fuel}").json()
            assert data["carbon_intensity_category"] in ("low", "medium")

    def test_unknown_fuel_type_falls_back(self, client):
        response = client.get("/carbon/intensity?fuel_type=magic_fuel")
        assert response.status_code == 200
        assert response.json()["fuel_type"] == "other"

    def test_region_is_uppercased(self, client):
        response = client.get("/carbon/intensity?fuel_type=coal&region=ercot")
        assert response.json()["region"] == "ERCOT"


class TestCarbonFactorsEndpoint:
    def test_returns_200(self, client):
        logger.info("Testing GET /carbon/factors")
        response = client.get("/carbon/factors")
        assert response.status_code == 200

    def test_returns_all_fuel_types(self, client):
        data = client.get("/carbon/factors").json()
        returned_fuels = {f["fuel_type"] for f in data["factors"]}
        for fuel in VALID_FUEL_TYPES:
            assert fuel in returned_fuels, f"Missing fuel type: {fuel}"
        logger.info("All %d fuel types present in factors table", len(returned_fuels))

    def test_factors_schema(self, client):
        data = client.get("/carbon/factors").json()
        assert "source" in data
        assert "generated_at" in data
        assert "factors" in data
        for factor in data["factors"]:
            assert "fuel_type" in factor
            assert "direct_co2_gco2_per_kwh" in factor
            assert "lifecycle_co2_gco2_per_kwh" in factor
            assert "category" in factor
            assert "description" in factor
