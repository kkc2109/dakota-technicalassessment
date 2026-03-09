"""Dagster pipeline job definitions."""

from dagster import AssetSelection, define_asset_job

# Full daily pipeline: EIA ingestion → dbt (Bronze → Silver → Gold)
daily_eia_pipeline_job = define_asset_job(
    name="daily_eia_pipeline",
    selection=AssetSelection.groups("eia_bronze", "bronze", "silver", "gold"),
    description=(
        "Full daily pipeline: ingest EIA data, then run dbt transformations "
        "(Bronze → Silver → Gold)."
    ),
)

# Enrichment-only job: all four enrichment assets
enrichment_pipeline_job = define_asset_job(
    name="enrichment_pipeline",
    selection=AssetSelection.groups("enrichment_bronze"),
    description="Ingest all enrichment data (weather, carbon, market prices, demand forecasts).",
)

# dbt-only job: useful for re-running transformations without re-ingesting
dbt_transformation_job = define_asset_job(
    name="dbt_transformation",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description="Run dbt build (Bronze → Silver → Gold) without re-ingesting source data.",
)

# Report-only job: re-generate reports from existing Gold data
report_generation_job = define_asset_job(
    name="report_generation",
    selection=AssetSelection.groups("reports"),
    description="Re-generate all reports from existing Gold layer data.",
)
