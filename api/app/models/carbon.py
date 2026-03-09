"""Pydantic models for carbon intensity enrichment data."""

from datetime import datetime
from pydantic import BaseModel, Field


# Reference emission factors (gCO2eq/kWh) based on IPCC and EPA data
CARBON_INTENSITY_FACTORS: dict[str, dict[str, float]] = {
    "coal": {"direct": 820.0, "lifecycle": 820.0},
    "natural_gas": {"direct": 490.0, "lifecycle": 490.0},
    "petroleum": {"direct": 650.0, "lifecycle": 650.0},
    "nuclear": {"direct": 0.0, "lifecycle": 12.0},
    "hydro": {"direct": 0.0, "lifecycle": 4.0},
    "wind": {"direct": 0.0, "lifecycle": 7.0},
    "solar_pv": {"direct": 0.0, "lifecycle": 20.0},
    "biomass": {"direct": 230.0, "lifecycle": 230.0},
    "geothermal": {"direct": 38.0, "lifecycle": 38.0},
    "other": {"direct": 400.0, "lifecycle": 400.0},
}

VALID_FUEL_TYPES = list(CARBON_INTENSITY_FACTORS.keys())


class CarbonIntensityReading(BaseModel):
    region: str = Field(..., description="Grid region identifier")
    fuel_type: str = Field(..., description="Fuel type for electricity generation")
    direct_co2_per_mwh: float = Field(
        ..., ge=0, description="Direct CO2 emissions in gCO2eq/kWh"
    )
    lifecycle_co2_per_mwh: float = Field(
        ..., ge=0, description="Lifecycle CO2 emissions in gCO2eq/kWh (includes manufacturing)"
    )
    carbon_intensity_category: str = Field(
        ..., description="Intensity category: low | medium | high"
    )
    recorded_at: datetime = Field(..., description="Timestamp of this reading (UTC)")


class CarbonEmissionFactor(BaseModel):
    fuel_type: str
    direct_co2_gco2_per_kwh: float = Field(..., description="Direct emissions gCO2eq/kWh")
    lifecycle_co2_gco2_per_kwh: float = Field(..., description="Lifecycle emissions gCO2eq/kWh")
    category: str = Field(..., description="low | medium | high")
    description: str


class CarbonFactorsResponse(BaseModel):
    source: str = Field(default="IPCC AR6 / EPA eGRID 2023")
    generated_at: datetime
    factors: list[CarbonEmissionFactor]
