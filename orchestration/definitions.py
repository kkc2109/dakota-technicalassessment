"""Dagster Definitions — the single entry point for the Dakota pipeline."""

import os
from pathlib import Path

from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource

from orchestration.assets import dbt_assets, ingestion_assets, report_assets
from orchestration.jobs.pipeline_jobs import (
    daily_eia_pipeline_job,
    dbt_transformation_job,
    enrichment_pipeline_job,
    report_generation_job,
)
from orchestration.resources.database import PostgresResource
from orchestration.schedules.schedules import daily_eia_schedule, enrichment_frequent_schedule
from orchestration.sensors.enrichment_sensor import enrichment_api_sensor

_DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt" / "energy_analytics"


all_assets = load_assets_from_modules([ingestion_assets, dbt_assets, report_assets])

resources = {
    "dbt": DbtCliResource(
        project_dir=str(_DBT_PROJECT_DIR),
        profiles_dir=str(_DBT_PROJECT_DIR),
    ),
    "database": PostgresResource(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "energy_analytics"),
        username=os.getenv("POSTGRES_USER", "dakota_user"),
        password=os.getenv("POSTGRES_PASSWORD", "Password"),
    ),
}

defs = Definitions(
    assets=all_assets,
    jobs=[
        daily_eia_pipeline_job,
        enrichment_pipeline_job,
        dbt_transformation_job,
        report_generation_job,
    ],
    schedules=[
        daily_eia_schedule,
        enrichment_frequent_schedule,
    ],
    sensors=[enrichment_api_sensor],
    resources=resources,
)
