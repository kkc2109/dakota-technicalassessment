# Dakota Analytics — Energy Analytics Pipeline

An end-to-end data pipeline that ingests US electricity data from the EIA API, enriches it with weather, carbon, and market signals, transforms it through a Bronze → Silver → Gold architecture using dbt, and produces an interactive HTML report, orchestrated by Dagster in Docker.

## Code Owner

Krishna Chaitanya Koganti - krishnachaitanya2109@gmail.com

---

## Prerequisites

Docker Desktop (Windows/Mac) or Docker Engine (Linux) is the only requirement. No local Python setup needed.

---

## Getting Started

**Step 1 — Clone the repository**

```bash
git clone <your-repo-url>
cd dakota-technicalassessment
```

**Step 2 — Set up your environment file**

The `.env` file is not committed to the repository as it contains sensitive credentials. For this assessment, all required values, including the EIA API key, are already filled in `.env.example`. Simply copy it across, and everything will work out of the box:

```bash
cp .env.example .env
```

**Step 3 — Build and start the services**

```bash
make setup
```

This builds all Docker images and starts every service. The first run takes around 5 minutes while images are built. Subsequent runs are much faster.

**Step 4 — Run the pipeline**

```bash
make run
```

This ingests EIA electricity data for the last 3 months and runs the dbt transformations from Bronze through to Gold.

**Step 5 — Generate the report**

```bash
make report
```

The report is saved to `reports/output/energy_analytics_report.html`. Open it in any browser.

---

## Available Commands

```bash
make setup    # Build images and start all services
make run      # Ingest EIA data and run dbt transformations
make report   # Generate the HTML report from current data
make test     # Run unit, integration, and dbt tests
make down     # Stop all services (your data is kept intact)
make clean    # Stop all services and wipe all data
```

On Windows, use `run.bat <command>` instead of `make <command>`.

---

## Services

Once `make setup` finishes, the following are available in your browser:

- **http://localhost:3000**      — Dagster UI — view the pipeline graph, run history, and asset lineage
- **http://localhost:8000/docs** — Enrichment API — Swagger documentation for all endpoints
- **http://localhost:8081**      — Adminer — PostgreSQL web interface for querying the database
- **http://localhost:8082**      — dbt Docs — model lineage and column-level documentation

To log into Adminer, use these credentials:

| Field    | Value              |
|----------|--------------------|
| System   | PostgreSQL         |
| Server   | postgres           |
| Username | dakota_user        |
| Password | Password           |
| Database | energy_analytics   |
