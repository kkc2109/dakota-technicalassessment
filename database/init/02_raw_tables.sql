-- 02_raw_tables.sql

-- EIA: Electricity Generation by Fuel Source and State
CREATE TABLE IF NOT EXISTS raw.eia_electricity_generation (
    id                      BIGSERIAL       PRIMARY KEY,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- EIA API response fields
    period                  VARCHAR(10)     NOT NULL,
    state_code              VARCHAR(5)      NOT NULL,
    state_description       VARCHAR(100),
    sector_id               VARCHAR(10)     NOT NULL,
    sector_description      VARCHAR(100),
    fuel_type_code          VARCHAR(10)     NOT NULL,
    fuel_type_description   VARCHAR(100),
    generation_mwh          NUMERIC(18, 4),
    generation_units        VARCHAR(50),

    -- Pipeline metadata
    source                  VARCHAR(50)     NOT NULL DEFAULT 'eia_api',
    api_request_id          VARCHAR(100),

    CONSTRAINT chk_eia_generation_period
        CHECK (period ~ '^\d{4}-\d{2}$')
);

COMMENT ON TABLE raw.eia_electricity_generation IS
    'Bronze: Raw EIA electricity generation data by fuel type and state. Append-only.';

-- EIA: Retail Electricity Prices by Sector and State
CREATE TABLE IF NOT EXISTS raw.eia_electricity_prices (
    id                      BIGSERIAL       PRIMARY KEY,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- EIA API response fields
    period                  VARCHAR(10)     NOT NULL,
    state_code              VARCHAR(5)      NOT NULL,
    state_description       VARCHAR(100),
    sector_id               VARCHAR(10)     NOT NULL,
    sector_name             VARCHAR(100),
    price_cents_per_kwh     NUMERIC(10, 4),
    revenue_million_dollars NUMERIC(18, 4),
    sales_million_kwh       NUMERIC(18, 4),
    customers               BIGINT,

    -- Pipeline metadata
    source                  VARCHAR(50)     NOT NULL DEFAULT 'eia_api',
    api_request_id          VARCHAR(100),

    CONSTRAINT chk_eia_prices_period
        CHECK (period ~ '^\d{4}-\d{2}$')
);

COMMENT ON TABLE raw.eia_electricity_prices IS
    'Bronze: Raw EIA retail electricity price data by sector and state. Append-only.';

-- Enrichment: Weather Conditions per Region
CREATE TABLE IF NOT EXISTS raw.enrichment_weather (
    id                      BIGSERIAL       PRIMARY KEY,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Enrichment API response fields
    region                  VARCHAR(50)     NOT NULL,
    temperature_c           NUMERIC(6, 2),
    wind_speed_ms           NUMERIC(6, 2),
    solar_irradiance_wm2    NUMERIC(8, 2),
    humidity_pct            NUMERIC(5, 2),
    cloud_cover_pct         NUMERIC(5, 2),
    weather_condition       VARCHAR(50),
    recorded_at             TIMESTAMPTZ     NOT NULL,

    -- Pipeline metadata
    source                  VARCHAR(50)     NOT NULL DEFAULT 'enrichment_api'
);

COMMENT ON TABLE raw.enrichment_weather IS
    'Bronze: Raw synthetic weather enrichment data per grid region. Append-only.';

-- Enrichment: Carbon Intensity by Fuel Type and Region
CREATE TABLE IF NOT EXISTS raw.enrichment_carbon_intensity (
    id                              BIGSERIAL       PRIMARY KEY,
    ingested_at                     TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Enrichment API response fields
    region                          VARCHAR(50)     NOT NULL,
    fuel_type                       VARCHAR(50)     NOT NULL,
    direct_co2_per_mwh              NUMERIC(10, 4),
    lifecycle_co2_per_mwh           NUMERIC(10, 4),
    carbon_intensity_category       VARCHAR(20),
    recorded_at                     TIMESTAMPTZ     NOT NULL,

    -- Pipeline metadata
    source                          VARCHAR(50)     NOT NULL DEFAULT 'enrichment_api'
);

COMMENT ON TABLE raw.enrichment_carbon_intensity IS
    'Bronze: Raw synthetic carbon intensity data by fuel type and region. Append-only.';

-- Enrichment: Electricity Market Prices per Region
CREATE TABLE IF NOT EXISTS raw.enrichment_market_prices (
    id                      BIGSERIAL       PRIMARY KEY,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Enrichment API response fields
    region                  VARCHAR(50)     NOT NULL,
    spot_price_usd_mwh      NUMERIC(10, 4),
    peak_price_usd_mwh      NUMERIC(10, 4),
    off_peak_price_usd_mwh  NUMERIC(10, 4),
    demand_mw               NUMERIC(12, 2),
    demand_forecast_mw      NUMERIC(12, 2),
    price_signal            VARCHAR(20),
    recorded_at             TIMESTAMPTZ     NOT NULL,

    -- Pipeline metadata
    source                  VARCHAR(50)     NOT NULL DEFAULT 'enrichment_api'
);

COMMENT ON TABLE raw.enrichment_market_prices IS
    'Bronze: Raw synthetic electricity market price data per grid region. Append-only.';

-- Enrichment: Hourly Demand Forecasts per Region
CREATE TABLE IF NOT EXISTS raw.enrichment_demand_forecast (
    id                      BIGSERIAL       PRIMARY KEY,
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Enrichment API response fields
    region                  VARCHAR(50)     NOT NULL,
    forecast_hour           TIMESTAMPTZ     NOT NULL,
    forecast_demand_mw      NUMERIC(12, 2)  NOT NULL,
    temperature_c           NUMERIC(6, 2),
    confidence_pct          NUMERIC(5, 2),
    recorded_at             TIMESTAMPTZ     NOT NULL,

    -- Pipeline metadata
    source                  VARCHAR(50)     NOT NULL DEFAULT 'enrichment_api'
);

COMMENT ON TABLE raw.enrichment_demand_forecast IS
    'Bronze: Raw synthetic hourly demand forecast data per grid region. Append-only.';

-- Pipeline audit log — track every ingestion run
CREATE TABLE IF NOT EXISTS raw.ingestion_audit_log (
    id              BIGSERIAL       PRIMARY KEY,
    run_id          VARCHAR(100)    NOT NULL,
    asset_name      VARCHAR(100)    NOT NULL,
    source          VARCHAR(50)     NOT NULL,
    records_written INTEGER         NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    status          VARCHAR(20)     NOT NULL DEFAULT 'running',
    error_message   TEXT,

    CONSTRAINT chk_audit_status
        CHECK (status IN ('running', 'success', 'failed'))
);

COMMENT ON TABLE raw.ingestion_audit_log IS
    'Tracks every ingestion run for observability and debugging.';
