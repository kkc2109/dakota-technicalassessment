# Architecture — Dakota Analytics Energy Pipeline

## Overview

This pipeline ingests US electricity data from two sources, transforms it through three layers, and produces executive-ready reports. Everything runs in Docker. No local Python setup needed.

---

## Data Flow

```
EIA API v2 (last 3 months)
    ├── electricity generation by fuel type and state
    └── retail electricity prices by sector and state
                        │
FastAPI Enrichment Service (per run)
    ├── weather conditions per grid region
    ├── carbon intensity per fuel type and region
    ├── market prices per grid region
    └── demand forecasts per grid region
                        │
                        ▼
              PostgreSQL — raw schema (Bronze)
                        │
                        ▼
              dbt Silver — cleaned and normalised
                        │
                        ▼
              dbt Gold — KPI aggregations
                        │
                        ▼
              HTML Report (Jupyter + Plotly)
```

---

## Components

### 1. FastAPI Enrichment Service

A containerised service that generates synthetic enrichment data across three domains:

| Domain  | Endpoints | Data |
|---------|-----------|------|
| Weather | `/weather/current`, `/weather/forecast` | Temperature, wind speed, solar irradiance, humidity |
| Carbon  | `/carbon/intensity`, `/carbon/factors`  | CO₂ per MWh by fuel type and grid region |
| Market  | `/market/prices`, `/market/demand-forecast` | Spot prices, peak prices, demand forecasts |

### 2. Data Ingestion

Async HTTP clients built with `httpx` and `tenacity`. Both clients handle pagination, retry on failures with exponential backoff, and log every run to an audit table.

- **EIA client** — fetches the last 90 days of generation and price data
- **Enrichment client** — fetches all nine US grid regions per run

### 3. Orchestration (Dagster)

Dagster manages the full asset graph and scheduling. Each pipeline stage is a software-defined asset, so Dagster tracks lineage from raw ingestion through to the final report.

- **Daily job** — EIA ingestion followed by dbt transformations
- **Enrichment job** — runs on demand or via sensor to pull the latest market and weather data
- **Report job** — generates the HTML report from the current Gold layer data

### 4. Database (PostgreSQL 15)

Three schemas following the Medallion pattern:

| Schema     | Layer  | Purpose |
|------------|--------|---------|
| `raw`      | Bronze | Raw ingested data, append-only, never modified |
| `staging`  | Silver | Cleaned, validated, and normalised by dbt |
| `analytics`| Gold   | Aggregated KPIs ready for reporting |

### 5. dbt Transformations

**Bronze** — thin views over the raw tables. No logic, just a stable reference point.

**Silver** — incremental models that clean and standardise the data. Handles null filtering, unit conversion, and code normalisation. Only processes new rows on each run.

**Gold** — four table models built on every run:
- `gold_generation_by_fuel_monthly` — generation mix with market share by fuel type
- `gold_price_trends` — retail prices with year-over-year and rolling averages
- `gold_carbon_footprint` — state-level CO₂ and renewable share analysis
- `gold_executive_summary` — single flat table combining all KPIs for reporting

### 6. Reporting

An HTML report generated from a Jupyter notebook via `nbconvert`. The report has six sections: executive KPI summary, generation mix, price analysis, carbon footprint, regional market performance, and pipeline audit log. Charts are interactive Plotly figures. The output is a single self-contained HTML file that requires no runtime to open.

---

## Services

| Service           | Port | Purpose |
|-------------------|------|---------|
| PostgreSQL        | 5432 | Primary data store |
| FastAPI (enrichment) | 8000 | Enrichment data API |
| Dagster UI        | 3000 | Pipeline monitoring and run history |
| Adminer           | 8081 | PostgreSQL web interface |
| dbt Docs          | 8082 | dbt model lineage and documentation |
