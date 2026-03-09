"""Electricity market enrichment endpoints.

Synthetic market prices are modelled on realistic US wholesale electricity
price ranges (LMP). Demand is correlated with temperature and time of day.
"""

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Query

from app.models.market import DemandForecast, DemandForecastPoint, MarketPrices

logger = logging.getLogger(__name__)

router = APIRouter()

# Realistic LMP price baselines (USD/MWh) per region based on 2023 market data
REGION_MARKET_PROFILES: dict[str, dict] = {
    "ERCOT":  {"base_price": 45, "peak_mult": 2.2, "demand_gw_base": 45, "volatility": 0.30},
    "CAISO":  {"base_price": 55, "peak_mult": 2.5, "demand_gw_base": 28, "volatility": 0.25},
    "PJM":    {"base_price": 40, "peak_mult": 2.0, "demand_gw_base": 90, "volatility": 0.20},
    "MISO":   {"base_price": 32, "peak_mult": 1.8, "demand_gw_base": 75, "volatility": 0.18},
    "NYISO":  {"base_price": 50, "peak_mult": 2.3, "demand_gw_base": 18, "volatility": 0.28},
    "NEISO":  {"base_price": 48, "peak_mult": 2.1, "demand_gw_base": 14, "volatility": 0.25},
    "SPP":    {"base_price": 30, "peak_mult": 1.9, "demand_gw_base": 40, "volatility": 0.22},
    "WECC":   {"base_price": 42, "peak_mult": 2.0, "demand_gw_base": 30, "volatility": 0.20},
    "SERC":   {"base_price": 38, "peak_mult": 1.9, "demand_gw_base": 50, "volatility": 0.18},
}

DEFAULT_REGION = "ERCOT"


def _price_signal(spot: float, base: float) -> str:
    ratio = spot / base
    if ratio < 0.6:
        return "low"
    if ratio < 1.4:
        return "normal"
    if ratio < 2.5:
        return "high"
    return "critical"


def _demand_for_hour(demand_base_gw: float, hour: int, temperature_c: float) -> float:
    """Demand is higher during business hours and when temperatures are extreme."""
    hour_factor = 1.0 + 0.3 * (1 if 8 <= hour <= 21 else -0.2)
    temp_factor = 1.0 + 0.01 * max(0, temperature_c - 20)  # AC load above 20°C
    noise = random.uniform(0.95, 1.05)
    return round(demand_base_gw * 1000 * hour_factor * temp_factor * noise, 2)


@router.get(
    "/prices",
    response_model=MarketPrices,
    summary="Current electricity market prices for a grid region",
)
async def get_market_prices(
    region: Annotated[
        str,
        Query(description="Grid region identifier. One of: " + ", ".join(REGION_MARKET_PROFILES)),
    ] = DEFAULT_REGION,
) -> MarketPrices:
    """Return synthetic real-time electricity market prices for the requested grid region.

    Spot prices use log-normal variation around regional LMP baselines.
    Peak/off-peak prices reflect typical day-ahead spread patterns.
    """
    region = region.upper()
    profile = REGION_MARKET_PROFILES.get(region, REGION_MARKET_PROFILES[DEFAULT_REGION])
    if region not in REGION_MARKET_PROFILES:
        logger.warning("Unknown region '%s', using %s profile", region, DEFAULT_REGION)
        region = DEFAULT_REGION

    base = profile["base_price"]
    volatility = profile["volatility"]

    spot = round(base * (1 + random.gauss(0, volatility)), 2)
    spot = max(0.01, spot)  # floor at near-zero (can go negative in ERCOT but rare)
    peak = round(spot * profile["peak_mult"] * random.uniform(0.9, 1.1), 2)
    off_peak = round(spot * random.uniform(0.4, 0.7), 2)

    now = datetime.now(tz=timezone.utc)
    temperature_c = random.uniform(10, 35)
    demand = _demand_for_hour(profile["demand_gw_base"], now.hour, temperature_c)
    forecast_demand = round(demand * random.uniform(0.95, 1.05), 2)

    logger.info(
        "Generated market prices region=%s spot=%.2f USD/MWh demand=%.0f MW",
        region, spot, demand,
    )

    return MarketPrices(
        region=region,
        spot_price_usd_mwh=spot,
        peak_price_usd_mwh=peak,
        off_peak_price_usd_mwh=off_peak,
        demand_mw=demand,
        demand_forecast_mw=forecast_demand,
        price_signal=_price_signal(spot, base),
        recorded_at=now,
    )


@router.get(
    "/demand-forecast",
    response_model=DemandForecast,
    summary="Hourly demand forecast for a grid region",
)
async def get_demand_forecast(
    region: Annotated[str, Query(description="Grid region identifier")] = DEFAULT_REGION,
    hours: Annotated[int, Query(ge=1, le=168, description="Forecast horizon in hours (max 168)")] = 24,
) -> DemandForecast:
    """Return a synthetic hourly demand forecast for the requested grid region."""
    region = region.upper()
    profile = REGION_MARKET_PROFILES.get(region, REGION_MARKET_PROFILES[DEFAULT_REGION])
    if region not in REGION_MARKET_PROFILES:
        region = DEFAULT_REGION

    now = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    forecast_points: list[DemandForecastPoint] = []

    for h in range(hours):
        dt = now + timedelta(hours=h)
        temperature_c = round(random.uniform(10, 38), 2)
        demand = _demand_for_hour(profile["demand_gw_base"], dt.hour, temperature_c)
        confidence = round(max(50.0, 99.0 - h * 0.3), 2)  # confidence degrades with horizon

        forecast_points.append(
            DemandForecastPoint(
                forecast_hour=dt,
                forecast_demand_mw=demand,
                temperature_c=temperature_c,
                confidence_pct=confidence,
            )
        )

    logger.info("Generated %d-hour demand forecast for region=%s", hours, region)
    return DemandForecast(region=region, generated_at=now, forecast_hours=hours, forecast=forecast_points)
