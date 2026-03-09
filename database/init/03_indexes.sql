-- 03_indexes.sql
-- raw.eia_electricity_generation

-- Primary access pattern: incremental loads look for new records by ingested_at
CREATE INDEX IF NOT EXISTS idx_eia_gen_ingested_at
    ON raw.eia_electricity_generation (ingested_at DESC);

-- dbt Silver model joins and filters on period + state
CREATE INDEX IF NOT EXISTS idx_eia_gen_period_state
    ON raw.eia_electricity_generation (period, state_code);

-- Analytical queries group by fuel type
CREATE INDEX IF NOT EXISTS idx_eia_gen_fuel_type
    ON raw.eia_electricity_generation (fuel_type_code);

-- Deduplication: prevent double-loading the same EIA record
CREATE UNIQUE INDEX IF NOT EXISTS uq_eia_gen_period_state_sector_fuel
    ON raw.eia_electricity_generation (period, state_code, sector_id, fuel_type_code);

-- raw.eia_electricity_prices

CREATE INDEX IF NOT EXISTS idx_eia_prices_ingested_at
    ON raw.eia_electricity_prices (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_eia_prices_period_state
    ON raw.eia_electricity_prices (period, state_code);

CREATE INDEX IF NOT EXISTS idx_eia_prices_sector
    ON raw.eia_electricity_prices (sector_id);

-- Deduplication: prevent double-loading the same EIA price record
CREATE UNIQUE INDEX IF NOT EXISTS uq_eia_prices_period_state_sector
    ON raw.eia_electricity_prices (period, state_code, sector_id);

-- raw.enrichment_weather
-- Primary time-series access pattern
CREATE INDEX IF NOT EXISTS idx_enrich_weather_recorded_at
    ON raw.enrichment_weather (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrich_weather_ingested_at
    ON raw.enrichment_weather (ingested_at DESC);

-- Regional queries
CREATE INDEX IF NOT EXISTS idx_enrich_weather_region
    ON raw.enrichment_weather (region, recorded_at DESC);

-- raw.enrichment_carbon_intensity
CREATE INDEX IF NOT EXISTS idx_enrich_carbon_recorded_at
    ON raw.enrichment_carbon_intensity (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrich_carbon_ingested_at
    ON raw.enrichment_carbon_intensity (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrich_carbon_region_fuel
    ON raw.enrichment_carbon_intensity (region, fuel_type);


-- raw.enrichment_market_prices
CREATE INDEX IF NOT EXISTS idx_enrich_market_recorded_at
    ON raw.enrichment_market_prices (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrich_market_ingested_at
    ON raw.enrichment_market_prices (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrich_market_region
    ON raw.enrichment_market_prices (region, recorded_at DESC);

-- raw.enrichment_demand_forecast
CREATE INDEX IF NOT EXISTS idx_enrich_demand_forecast_hour
    ON raw.enrichment_demand_forecast (forecast_hour);

CREATE INDEX IF NOT EXISTS idx_enrich_demand_recorded_at
    ON raw.enrichment_demand_forecast (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_enrich_demand_region
    ON raw.enrichment_demand_forecast (region, forecast_hour);

-- raw.ingestion_audit_log
CREATE INDEX IF NOT EXISTS idx_audit_run_id
    ON raw.ingestion_audit_log (run_id);

CREATE INDEX IF NOT EXISTS idx_audit_asset_started
    ON raw.ingestion_audit_log (asset_name, started_at DESC);
