"""Carbon intensity enrichment endpoints.

Emission factors are based on IPCC AR6 and EPA eGRID 2023 reference data.
Regional variance is applied as ±5% to simulate real-world grid mix variation.
"""

import logging
import random
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Query

from app.models.carbon import (
    CARBON_INTENSITY_FACTORS,
    VALID_FUEL_TYPES,
    CarbonEmissionFactor,
    CarbonFactorsResponse,
    CarbonIntensityReading,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_CATEGORY_THRESHOLDS = {
    "low":    (0, 100),
    "medium": (100, 500),
    "high":   (500, float("inf")),
}


def _categorise(lifecycle_co2: float) -> str:
    for category, (lo, hi) in _CATEGORY_THRESHOLDS.items():
        if lo <= lifecycle_co2 < hi:
            return category
    return "high"


def _fuel_description(fuel_type: str) -> str:
    descriptions = {
        "coal":        "Conventional coal combustion — highest carbon intensity fuel",
        "natural_gas": "Combined-cycle natural gas — approximately 40% cleaner than coal",
        "petroleum":   "Oil-based generation — high carbon intensity, typically peakers",
        "nuclear":     "Zero direct emissions; lifecycle includes construction and fuel processing",
        "hydro":       "Near-zero direct emissions; lifecycle includes reservoir methane",
        "wind":        "Zero direct emissions; very low lifecycle (manufacturing and install)",
        "solar_pv":    "Zero direct emissions; lifecycle includes panel manufacturing",
        "biomass":     "Carbon-neutral in theory; direct combustion emissions are significant",
        "geothermal":  "Low but non-zero direct emissions from geothermal fluid",
        "other":       "Mixed/unclassified generation sources",
    }
    return descriptions.get(fuel_type, "Energy generation source")


@router.get(
    "/intensity",
    response_model=CarbonIntensityReading,
    summary="Carbon intensity for a specific fuel type and region",
)
async def get_carbon_intensity(
    fuel_type: Annotated[
        str,
        Query(description="Fuel type. One of: " + ", ".join(VALID_FUEL_TYPES)),
    ] = "natural_gas",
    region: Annotated[str, Query(description="Grid region identifier")] = "ERCOT",
) -> CarbonIntensityReading:
    """Return synthetic carbon intensity data for a given fuel type and region.

    A ±5% regional variance is applied to base IPCC/EPA emission factors to
    simulate real-world grid mix differences.
    """
    fuel_type = fuel_type.lower()
    if fuel_type not in CARBON_INTENSITY_FACTORS:
        logger.warning("Unknown fuel_type '%s', falling back to 'other'", fuel_type)
        fuel_type = "other"

    base = CARBON_INTENSITY_FACTORS[fuel_type]
    variance = random.uniform(-0.05, 0.05)
    direct = round(base["direct"] * (1 + variance), 4)
    lifecycle = round(base["lifecycle"] * (1 + variance), 4)
    category = _categorise(lifecycle)

    logger.info(
        "Generated carbon intensity region=%s fuel=%s direct=%.1f lifecycle=%.1f category=%s",
        region, fuel_type, direct, lifecycle, category,
    )

    return CarbonIntensityReading(
        region=region.upper(),
        fuel_type=fuel_type,
        direct_co2_per_mwh=direct,
        lifecycle_co2_per_mwh=lifecycle,
        carbon_intensity_category=category,
        recorded_at=datetime.now(tz=timezone.utc),
    )


@router.get(
    "/factors",
    response_model=CarbonFactorsResponse,
    summary="Full emission factor reference table",
)
async def get_carbon_factors() -> CarbonFactorsResponse:
    """Return the complete emission factor reference table for all supported fuel types.

    Values are based on IPCC AR6 Table 7.SM.7 and EPA eGRID 2023.
    """
    factors: list[CarbonEmissionFactor] = []
    for fuel_type, values in CARBON_INTENSITY_FACTORS.items():
        factors.append(
            CarbonEmissionFactor(
                fuel_type=fuel_type,
                direct_co2_gco2_per_kwh=values["direct"],
                lifecycle_co2_gco2_per_kwh=values["lifecycle"],
                category=_categorise(values["lifecycle"]),
                description=_fuel_description(fuel_type),
            )
        )

    logger.info("Serving carbon emission factors table (%d entries)", len(factors))
    return CarbonFactorsResponse(
        generated_at=datetime.now(tz=timezone.utc),
        factors=factors,
    )
