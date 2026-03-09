"""Dagster software-defined assets for data ingestion."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    MetadataValue,
    Output,
    asset,
)

from ingestion.clients.eia_client import EIAClient
from ingestion.clients.enrichment_client import EnrichmentClient
from ingestion.loaders.eia_loader import EIALoader
from ingestion.loaders.enrichment_loader import EnrichmentLoader

logger = logging.getLogger(__name__)


@asset(
    group_name="eia_bronze",
    compute_kind="python",
    description="Raw EIA electricity generation by fuel source and state (monthly data, daily batch).",
)
def eia_electricity_generation(context: AssetExecutionContext) -> Output:
    """Ingest EIA electricity generation data for the past 24 months."""
    
    run_id = context.run_id
    end_period = datetime.now(tz=timezone.utc).strftime("%Y-%m")
    start_period = (datetime.now(tz=timezone.utc) - timedelta(days=90)).strftime("%Y-%m")

    context.log.info(
        "Ingesting EIA generation data: %s → %s run_id=%s",
        start_period, end_period, run_id,
    )

    async def _run() -> int:
        async with EIAClient() as client:
            rows = await client.fetch_all_generation_pages(
                start_period=start_period, end_period=end_period
            )
        loader = EIALoader()
        inserted = loader.write_generation(rows, run_id=run_id, api_request_id=run_id)
        loader.dispose()
        return inserted

    inserted = asyncio.run(_run())
    context.log.info("EIA generation ingestion complete: %d rows inserted", inserted)

    return Output(
        value=inserted,
        metadata={
            "rows_inserted":  MetadataValue.int(inserted),
            "start_period":   MetadataValue.text(start_period),
            "end_period":     MetadataValue.text(end_period),
            "source":         MetadataValue.text("EIA API v2"),
            "table":          MetadataValue.text("raw.eia_electricity_generation"),
        },
    )


@asset(
    group_name="eia_bronze",
    compute_kind="python",
    description="Raw EIA retail electricity prices by sector and state (monthly data, daily batch).",
)
def eia_electricity_prices(context: AssetExecutionContext) -> Output:
    """Ingest EIA retail electricity price data for the past 24 months."""
    run_id = context.run_id
    end_period = datetime.now(tz=timezone.utc).strftime("%Y-%m")
    start_period = (datetime.now(tz=timezone.utc) - timedelta(days=90)).strftime("%Y-%m")

    context.log.info(
        "Ingesting EIA prices data: %s → %s run_id=%s",
        start_period, end_period, run_id,
    )

    async def _run() -> int:
        async with EIAClient() as client:
            rows = await client.fetch_all_prices_pages(
                start_period=start_period, end_period=end_period
            )
        loader = EIALoader()
        inserted = loader.write_prices(rows, run_id=run_id, api_request_id=run_id)
        loader.dispose()
        return inserted

    inserted = asyncio.run(_run())
    context.log.info("EIA prices ingestion complete: %d rows inserted", inserted)

    return Output(
        value=inserted,
        metadata={
            "rows_inserted": MetadataValue.int(inserted),
            "start_period":  MetadataValue.text(start_period),
            "end_period":    MetadataValue.text(end_period),
            "source":        MetadataValue.text("EIA API v2"),
            "table":         MetadataValue.text("raw.eia_electricity_prices"),
        },
    )


@asset(
    group_name="enrichment_bronze",
    compute_kind="python",
    description="Synthetic weather conditions for all US grid regions.",
)
def enrichment_weather(context: AssetExecutionContext) -> Output:
    """Ingest current weather readings for all configured grid regions."""
    run_id = context.run_id

    async def _run() -> list[dict]:
        async with EnrichmentClient() as client:
            return await client.fetch_all_regions_weather()

    responses = asyncio.run(_run())
    loader = EnrichmentLoader()
    inserted = loader.write_weather_from_api_responses(responses, run_id=run_id)
    loader.dispose()

    context.log.info("Enrichment weather ingestion complete: %d rows inserted", inserted)
    return Output(
        value=inserted,
        metadata={
            "rows_inserted": MetadataValue.int(inserted),
            "regions":       MetadataValue.int(len(responses)),
            "table":         MetadataValue.text("raw.enrichment_weather"),
        },
    )


@asset(
    group_name="enrichment_bronze",
    compute_kind="python",
    description="Synthetic carbon intensity data for all regions × fuel type combinations.",
)
def enrichment_carbon_intensity(context: AssetExecutionContext) -> Output:
    """Ingest carbon intensity readings for all regions and fuel types."""
    run_id = context.run_id

    async def _run() -> list[dict]:
        async with EnrichmentClient() as client:
            return await client.fetch_all_regions_carbon()

    responses = asyncio.run(_run())
    loader = EnrichmentLoader()
    inserted = loader.write_carbon_from_api_responses(responses, run_id=run_id)
    loader.dispose()

    context.log.info("Enrichment carbon intensity ingestion complete: %d rows inserted", inserted)
    return Output(
        value=inserted,
        metadata={
            "rows_inserted":       MetadataValue.int(inserted),
            "region_fuel_combos":  MetadataValue.int(len(responses)),
            "table":               MetadataValue.text("raw.enrichment_carbon_intensity"),
        },
    )


@asset(
    group_name="enrichment_bronze",
    compute_kind="python",
    description="Synthetic electricity market prices for all US grid regions.",
)
def enrichment_market_prices(context: AssetExecutionContext) -> Output:
    """Ingest market price readings for all configured grid regions."""
    run_id = context.run_id

    async def _run() -> list[dict]:
        async with EnrichmentClient() as client:
            return await client.fetch_all_regions_market()

    responses = asyncio.run(_run())
    loader = EnrichmentLoader()
    inserted = loader.write_market_from_api_responses(responses, run_id=run_id)
    loader.dispose()

    context.log.info("Enrichment market prices ingestion complete: %d rows inserted", inserted)
    return Output(
        value=inserted,
        metadata={
            "rows_inserted": MetadataValue.int(inserted),
            "regions":       MetadataValue.int(len(responses)),
            "table":         MetadataValue.text("raw.enrichment_market_prices"),
        },
    )


@asset(
    group_name="enrichment_bronze",
    compute_kind="python",
    description="Synthetic 24-hour demand forecasts for all US grid regions.",
)
def enrichment_demand_forecast(context: AssetExecutionContext) -> Output:
    """Ingest 24-hour demand forecasts for all configured grid regions."""
    run_id = context.run_id
    total_inserted = 0

    async def _run() -> list[dict]:
        async with EnrichmentClient() as client:
            return await client.fetch_all_regions_demand_forecast(hours=24)

    forecast_responses = asyncio.run(_run())
    loader = EnrichmentLoader()
    for response in forecast_responses:
        total_inserted += loader.write_demand_forecast_from_api_response(
            response, run_id=run_id
        )
    loader.dispose()

    context.log.info("Enrichment demand forecast ingestion complete: %d rows inserted", total_inserted)
    return Output(
        value=total_inserted,
        metadata={
            "rows_inserted":  MetadataValue.int(total_inserted),
            "regions":        MetadataValue.int(len(forecast_responses)),
            "forecast_hours": MetadataValue.int(24),
            "table":          MetadataValue.text("raw.enrichment_demand_forecast"),
        },
    )
