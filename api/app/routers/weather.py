"""Weather enrichment endpoints.

Generates realistic synthetic weather data for US grid regions.
Temperature and solar irradiance are modelled with seasonal variation;
wind speed follows a Weibull-like distribution.
"""

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Query

from app.models.weather import WeatherForecast, WeatherForecastPoint, WeatherReading

logger = logging.getLogger(__name__)

router = APIRouter()

# US grid regions with realistic baseline temperature ranges (°C) and solar profiles
REGION_PROFILES: dict[str, dict] = {
    "ERCOT":  {"temp_base": 22, "temp_range": 18, "solar_peak": 900, "wind_mean": 7.5},
    "CAISO":  {"temp_base": 18, "temp_range": 12, "solar_peak": 950, "wind_mean": 5.0},
    "PJM":    {"temp_base": 12, "temp_range": 20, "solar_peak": 750, "wind_mean": 5.5},
    "MISO":   {"temp_base": 10, "temp_range": 22, "solar_peak": 700, "wind_mean": 8.0},
    "NYISO":  {"temp_base": 10, "temp_range": 20, "solar_peak": 680, "wind_mean": 5.0},
    "NEISO":  {"temp_base": 8,  "temp_range": 20, "solar_peak": 650, "wind_mean": 5.5},
    "SPP":    {"temp_base": 16, "temp_range": 22, "solar_peak": 800, "wind_mean": 9.5},
    "WECC":   {"temp_base": 15, "temp_range": 18, "solar_peak": 870, "wind_mean": 6.0},
    "SERC":   {"temp_base": 20, "temp_range": 16, "solar_peak": 780, "wind_mean": 4.5},
}

WEATHER_CONDITIONS = [
    "Clear", "Partly Cloudy", "Mostly Cloudy", "Overcast",
    "Light Rain", "Heavy Rain", "Thunderstorm", "Fog", "Snow", "Windy",
]

DEFAULT_REGION = "ERCOT"


def _solar_factor(hour: int) -> float:
    """Return a 0–1 multiplier for solar irradiance based on hour of day."""
    if hour < 6 or hour >= 20:
        return 0.0
    peak_hour = 13
    return max(0.0, 1.0 - abs(hour - peak_hour) / 7.0)


def _generate_weather(region: str, dt: datetime) -> WeatherReading:
    profile = REGION_PROFILES.get(region, REGION_PROFILES[DEFAULT_REGION])
    month_offset = (dt.month - 6) / 6.0  # peaks in June/July
    temperature_c = round(
        profile["temp_base"] + profile["temp_range"] * month_offset + random.uniform(-3, 3), 2
    )
    cloud_cover = round(random.uniform(0, 100), 2)
    solar_irradiance = round(
        profile["solar_peak"] * _solar_factor(dt.hour) * (1 - cloud_cover / 100) + random.uniform(-20, 20),
        2,
    )
    wind_speed = round(abs(random.gauss(profile["wind_mean"], 2.5)), 2)
    humidity = round(random.uniform(30, 95), 2)

    if cloud_cover > 80:
        condition = random.choice(["Overcast", "Heavy Rain", "Light Rain", "Thunderstorm"])
    elif cloud_cover > 50:
        condition = random.choice(["Mostly Cloudy", "Partly Cloudy", "Light Rain"])
    elif wind_speed > 12:
        condition = "Windy"
    else:
        condition = "Clear" if cloud_cover < 20 else "Partly Cloudy"

    return WeatherReading(
        region=region,
        temperature_c=temperature_c,
        wind_speed_ms=wind_speed,
        solar_irradiance_wm2=max(0.0, solar_irradiance),
        humidity_pct=humidity,
        cloud_cover_pct=cloud_cover,
        weather_condition=condition,
        recorded_at=dt,
    )


@router.get(
    "/current",
    response_model=WeatherReading,
    summary="Current weather conditions for a grid region",
)
async def get_current_weather(
    region: Annotated[
        str,
        Query(description="Grid region identifier. One of: " + ", ".join(REGION_PROFILES)),
    ] = DEFAULT_REGION,
) -> WeatherReading:
    """Return synthetic current weather conditions for the requested grid region."""
    region = region.upper()
    if region not in REGION_PROFILES:
        logger.warning("Unknown region '%s', falling back to %s", region, DEFAULT_REGION)
        region = DEFAULT_REGION

    now = datetime.now(tz=timezone.utc)
    reading = _generate_weather(region, now)
    logger.info("Generated current weather for region=%s temp=%.1f°C", region, reading.temperature_c)
    return reading


@router.get(
    "/forecast",
    response_model=WeatherForecast,
    summary="Hourly weather forecast for a grid region",
)
async def get_weather_forecast(
    region: Annotated[str, Query(description="Grid region identifier")] = DEFAULT_REGION,
    hours: Annotated[int, Query(ge=1, le=168, description="Forecast horizon in hours (max 168)")] = 24,
) -> WeatherForecast:
    """Return a synthetic hourly weather forecast for the requested grid region."""
    region = region.upper()
    if region not in REGION_PROFILES:
        logger.warning("Unknown region '%s', falling back to %s", region, DEFAULT_REGION)
        region = DEFAULT_REGION

    now = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    forecast_points: list[WeatherForecastPoint] = []

    for h in range(hours):
        dt = now + timedelta(hours=h)
        reading = _generate_weather(region, dt)
        forecast_points.append(
            WeatherForecastPoint(
                forecast_hour=dt,
                temperature_c=reading.temperature_c,
                wind_speed_ms=reading.wind_speed_ms,
                solar_irradiance_wm2=reading.solar_irradiance_wm2,
                humidity_pct=reading.humidity_pct,
                cloud_cover_pct=reading.cloud_cover_pct,
                weather_condition=reading.weather_condition,
                precipitation_mm=round(random.uniform(0, 5) if "Rain" in reading.weather_condition else 0.0, 2),
            )
        )

    logger.info("Generated %d-hour weather forecast for region=%s", hours, region)
    return WeatherForecast(region=region, generated_at=now, forecast_hours=hours, forecast=forecast_points)
