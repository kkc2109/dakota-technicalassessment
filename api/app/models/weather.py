"""Pydantic models for weather enrichment data."""

from datetime import datetime
from pydantic import BaseModel, Field


class WeatherReading(BaseModel):
    region: str = Field(..., description="Grid region identifier (e.g. ERCOT, CAISO)")
    temperature_c: float = Field(..., description="Ambient temperature in degrees Celsius")
    wind_speed_ms: float = Field(..., ge=0, description="Wind speed in metres per second")
    solar_irradiance_wm2: float = Field(..., ge=0, description="Solar irradiance in W/m²")
    humidity_pct: float = Field(..., ge=0, le=100, description="Relative humidity percentage")
    cloud_cover_pct: float = Field(..., ge=0, le=100, description="Cloud cover percentage")
    weather_condition: str = Field(..., description="Human-readable weather condition")
    recorded_at: datetime = Field(..., description="Timestamp of this reading (UTC)")


class WeatherForecastPoint(BaseModel):
    forecast_hour: datetime = Field(..., description="Forecasted hour (UTC)")
    temperature_c: float
    wind_speed_ms: float = Field(..., ge=0)
    solar_irradiance_wm2: float = Field(..., ge=0)
    humidity_pct: float = Field(..., ge=0, le=100)
    cloud_cover_pct: float = Field(..., ge=0, le=100)
    weather_condition: str
    precipitation_mm: float = Field(..., ge=0, description="Expected precipitation in mm")


class WeatherForecast(BaseModel):
    region: str
    generated_at: datetime = Field(..., description="When this forecast was generated (UTC)")
    forecast_hours: int = Field(..., description="Number of hours in the forecast")
    forecast: list[WeatherForecastPoint]
