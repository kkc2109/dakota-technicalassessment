"""Dagster sensor for enrichment API availability."""

import logging

import httpx
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor

from orchestration.jobs.pipeline_jobs import enrichment_pipeline_job

logger = logging.getLogger(__name__)

import os

_ENRICHMENT_API_URL = os.getenv("ENRICHMENT_API_URL", "http://api:8000")


@sensor(
    job=enrichment_pipeline_job,
    minimum_interval_seconds=900,   # 15 minutes between sensor evaluations
    description="Triggers enrichment ingestion when the API is healthy.",
)
def enrichment_api_sensor(context: SensorEvaluationContext) -> SensorResult:
    """Check enrichment API health and trigger ingestion if healthy."""
    try:
        response = httpx.get(f"{_ENRICHMENT_API_URL}/health", timeout=10)
        api_healthy = response.status_code == 200
    except httpx.RequestError as exc:
        context.log.warning("Enrichment API unreachable: %s — skipping run", exc)
        return SensorResult(run_requests=[], skip_reason=f"API unreachable: {exc}")

    if not api_healthy:
        context.log.warning("Enrichment API returned HTTP %d — skipping run", response.status_code)
        return SensorResult(
            run_requests=[],
            skip_reason=f"API returned HTTP {response.status_code}",
        )

    context.log.info("Enrichment API healthy — triggering enrichment pipeline")
    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"enrichment_{context.cursor or 'initial'}",
                tags={"triggered_by": "enrichment_api_sensor"},
            )
        ]
    )
