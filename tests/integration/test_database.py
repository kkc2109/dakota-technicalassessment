"""Integration tests for database connectivity and schema correctness.

These tests run against a live PostgreSQL instance.
Mark: pytest -m integration
"""

import logging

import pytest
from sqlalchemy import inspect, text

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration


class TestDatabaseConnectivity:
    def test_connection(self, db_engine):
        logger.info("Testing database connection")
        with db_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS ok"))
            row = result.fetchone()
        assert row[0] == 1
        logger.info("Database connection: OK")

    def test_schemas_exist(self, db_engine):
        inspector = inspect(db_engine)
        schemas = inspector.get_schema_names()
        logger.info("Available schemas: %s", schemas)
        for schema in ("raw", "staging", "analytics"):
            assert schema in schemas, f"Schema '{schema}' not found"
            logger.info("Schema '%s': present", schema)


class TestRawTablesExist:
    EXPECTED_TABLES = [
        ("raw", "eia_electricity_generation"),
        ("raw", "eia_electricity_prices"),
        ("raw", "enrichment_weather"),
        ("raw", "enrichment_carbon_intensity"),
        ("raw", "enrichment_market_prices"),
        ("raw", "enrichment_demand_forecast"),
        ("raw", "ingestion_audit_log"),
    ]

    def test_all_raw_tables_exist(self, db_engine):
        inspector = inspect(db_engine)
        for schema, table in self.EXPECTED_TABLES:
            tables = inspector.get_table_names(schema=schema)
            assert table in tables, f"Table {schema}.{table} not found"
            logger.info("Table %s.%s: present", schema, table)


class TestRawTableColumns:
    def test_eia_generation_columns(self, db_engine):
        inspector = inspect(db_engine)
        cols = {c["name"] for c in inspector.get_columns("eia_electricity_generation", schema="raw")}
        required = {"id", "ingested_at", "period", "state_code", "fuel_type_code", "generation_mwh"}
        missing = required - cols
        assert not missing, f"Missing columns in raw.eia_electricity_generation: {missing}"
        logger.info("raw.eia_electricity_generation columns: %s", cols)

    def test_enrichment_weather_columns(self, db_engine):
        inspector = inspect(db_engine)
        cols = {c["name"] for c in inspector.get_columns("enrichment_weather", schema="raw")}
        required = {"id", "ingested_at", "region", "temperature_c", "wind_speed_ms", "recorded_at"}
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"
        logger.info("raw.enrichment_weather columns: %s", cols)


class TestRawTableConstraints:
    def test_unique_index_on_eia_generation(self, db_engine):
        inspector = inspect(db_engine)
        indexes = inspector.get_indexes("eia_electricity_generation", schema="raw")
        unique_indexes = [i for i in indexes if i.get("unique")]
        assert len(unique_indexes) > 0, "No unique index found on eia_electricity_generation"
        logger.info("Unique indexes on eia_electricity_generation: %s", unique_indexes)


class TestAuditLog:
    def test_audit_log_insert(self, db_engine):
        logger.info("Testing audit log insert")
        with db_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw.ingestion_audit_log
                        (run_id, asset_name, source, status)
                    VALUES ('test_run_001', 'test_asset', 'test', 'running')
                """)
            )
            result = conn.execute(
                text("SELECT status FROM raw.ingestion_audit_log WHERE run_id = 'test_run_001'")
            )
            row = result.fetchone()
        assert row[0] == "running"
        logger.info("Audit log insert: OK")

    def test_audit_log_status_constraint(self, db_engine):
        logger.info("Testing audit log status constraint")
        with pytest.raises(Exception):
            with db_engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO raw.ingestion_audit_log
                            (run_id, asset_name, source, status)
                        VALUES ('bad_run', 'test', 'test', 'invalid_status')
                    """)
                )
        logger.info("Status constraint correctly rejected invalid value")
