"""Integration tests for the full ingestion pipeline.

Tests the complete path: API client → loader → database.
Requires live PostgreSQL and a running enrichment API service.

Mark: pytest -m integration
"""

import logging
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration


class TestEIALoaderIntegration:
    """Tests EIALoader writing to the real database."""

    def test_write_generation_rows(self, db_engine):
        from ingestion.loaders.eia_loader import EIALoader

        test_rows = [
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
            }
        ]

        loader = EIALoader()
        inserted = loader.write_generation(test_rows, run_id="integration_test_001")
        loader.dispose()

        assert inserted >= 0  # 0 if already exists (ON CONFLICT DO NOTHING), 1 if new
        logger.info("EIA generation write integration test: %d rows inserted", inserted)

        # Verify data exists in DB
        with db_engine.connect() as conn:
            result = conn.execute(
                text("SELECT count(*) FROM raw.eia_electricity_generation WHERE period='2024-01' AND state_code='TX'")
            )
            count = result.scalar()
        assert count >= 1
        logger.info("Verified row exists in raw.eia_electricity_generation")

    def test_idempotent_write(self, db_engine):
        """Writing the same generation data twice should not create duplicates."""
        from ingestion.loaders.eia_loader import EIALoader

        test_rows = [
            {
                "period": "2024-02",
                "location": "CA",
                "stateDescription": "California",
                "sectorid": "99",
                "sectorDescription": "All Sectors",
                "fueltypeid": "SUN",
                "fuelTypeDescription": "Solar",
                "generation": "9876.5",
                "generation-units": "thousand megawatthours",
            }
        ]

        loader = EIALoader()
        first = loader.write_generation(test_rows, run_id="integration_idempotent_1")
        second = loader.write_generation(test_rows, run_id="integration_idempotent_2")
        loader.dispose()

        logger.info("Idempotency test: first_insert=%d second_insert=%d", first, second)
        assert second == 0  # second write should insert 0 rows (ON CONFLICT DO NOTHING)


class TestEnrichmentLoaderIntegration:
    """Tests EnrichmentLoader writing to the real database."""

    def test_write_weather_records(self, db_engine):
        from ingestion.loaders.enrichment_loader import EnrichmentLoader

        records = [
            {
                "region": "ERCOT",
                "temperature_c": 28.5,
                "wind_speed_ms": 6.2,
                "solar_irradiance_wm2": 750.0,
                "humidity_pct": 55.0,
                "cloud_cover_pct": 20.0,
                "weather_condition": "Clear",
                "recorded_at": datetime.now(tz=timezone.utc),
            }
        ]

        loader = EnrichmentLoader()
        inserted = loader.write_weather(records, run_id="integration_weather_001")
        loader.dispose()

        assert inserted == 1
        logger.info("Enrichment weather write integration test: %d rows inserted", inserted)

    def test_write_carbon_intensity(self, db_engine):
        from ingestion.loaders.enrichment_loader import EnrichmentLoader

        records = [
            {
                "region": "ERCOT",
                "fuel_type": "natural_gas",
                "direct_co2_per_mwh": 490.0,
                "lifecycle_co2_per_mwh": 490.0,
                "carbon_intensity_category": "medium",
                "recorded_at": datetime.now(tz=timezone.utc),
            }
        ]

        loader = EnrichmentLoader()
        inserted = loader.write_carbon_intensity(records, run_id="integration_carbon_001")
        loader.dispose()

        assert inserted == 1
        logger.info("Enrichment carbon intensity write integration test: %d rows inserted", inserted)

    def test_audit_log_tracks_writes(self, db_engine):
        """Verify that each write creates an audit log entry."""
        from ingestion.loaders.enrichment_loader import EnrichmentLoader

        run_id = "integration_audit_test_001"
        records = [
            {
                "region": "PJM",
                "spot_price_usd_mwh": 45.0,
                "peak_price_usd_mwh": 90.0,
                "off_peak_price_usd_mwh": 25.0,
                "demand_mw": 80000.0,
                "demand_forecast_mw": 82000.0,
                "price_signal": "normal",
                "recorded_at": datetime.now(tz=timezone.utc),
            }
        ]

        loader = EnrichmentLoader()
        loader.write_market_prices(records, run_id=run_id)
        loader.dispose()

        with db_engine.connect() as conn:
            result = conn.execute(
                text("SELECT status, records_written FROM raw.ingestion_audit_log WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            row = result.fetchone()

        assert row is not None, "No audit log entry created"
        assert row[0] == "success"
        assert row[1] == 1
        logger.info("Audit log entry: status=%s records_written=%d", row[0], row[1])
