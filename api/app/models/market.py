"""Pydantic models for electricity market enrichment data."""

from datetime import datetime
from pydantic import BaseModel, Field


class MarketPrices(BaseModel):
    region: str = Field(..., description="Grid region identifier (e.g. ERCOT, CAISO)")
    spot_price_usd_mwh: float = Field(..., description="Real-time spot price in USD/MWh")
    peak_price_usd_mwh: float = Field(..., description="On-peak period price in USD/MWh")
    off_peak_price_usd_mwh: float = Field(..., description="Off-peak period price in USD/MWh")
    demand_mw: float = Field(..., ge=0, description="Current grid demand in MW")
    demand_forecast_mw: float = Field(..., ge=0, description="24-hour ahead demand forecast in MW")
    price_signal: str = Field(
        ..., description="Price signal level: low | normal | high | critical"
    )
    recorded_at: datetime = Field(..., description="Timestamp of this reading (UTC)")


class DemandForecastPoint(BaseModel):
    forecast_hour: datetime = Field(..., description="Hour being forecast (UTC)")
    forecast_demand_mw: float = Field(..., ge=0, description="Forecast load in MW")
    temperature_c: float = Field(..., description="Driving temperature for demand model")
    confidence_pct: float = Field(..., ge=0, le=100, description="Model confidence 0–100")


class DemandForecast(BaseModel):
    region: str
    generated_at: datetime = Field(..., description="When this forecast was generated (UTC)")
    forecast_hours: int
    forecast: list[DemandForecastPoint]
