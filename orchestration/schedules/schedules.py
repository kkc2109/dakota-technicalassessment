"""Dagster schedules."""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from orchestration.jobs.pipeline_jobs import daily_eia_pipeline_job, enrichment_pipeline_job

daily_eia_schedule = ScheduleDefinition(
    name="daily_eia_pipeline",
    job=daily_eia_pipeline_job,
    cron_schedule="0 6 * * *",           # 06:00 UTC daily
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily EIA batch ingestion, dbt transformation, and report generation.",
)

enrichment_frequent_schedule = ScheduleDefinition(
    name="enrichment_frequent",
    job=enrichment_pipeline_job,
    cron_schedule="*/15 * * * *",        # every 15 minutes
    default_status=DefaultScheduleStatus.RUNNING,
    description="Frequent ingestion of synthetic enrichment data (weather, carbon, market).",
)
