"""Enrichment data loader."""

import logging
from typing import Any

from sqlalchemy import text

from ingestion.loaders.base_loader import BaseLoader

logger = logging.getLogger(__name__)


class EnrichmentLoader(BaseLoader):
    """Writes enrichment data into the raw schema enrichment tables."""

    def write_weather(
        self, records: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Write weather readings. Returns rows inserted."""
        if not records:
            return 0
        audit_id = self._start_audit(run_id, "enrichment_weather", "enrichment_api")
        try:
            with self._connection() as conn:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO raw.enrichment_weather
                            (region, temperature_c, wind_speed_ms, solar_irradiance_wm2,
                             humidity_pct, cloud_cover_pct, weather_condition, recorded_at)
                        VALUES
                            (:region, :temperature_c, :wind_speed_ms, :solar_irradiance_wm2,
                             :humidity_pct, :cloud_cover_pct, :weather_condition, :recorded_at)
                        """
                    ),
                    records,
                )
                inserted = result.rowcount
            logger.info("Enrichment weather written=%d", inserted)
            self._complete_audit(audit_id, inserted)
            return inserted
        except Exception as exc:
            self._fail_audit(audit_id, str(exc))
            raise

    def write_carbon_intensity(
        self, records: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Write carbon intensity readings. Returns rows inserted."""
        if not records:
            return 0
        audit_id = self._start_audit(run_id, "enrichment_carbon_intensity", "enrichment_api")
        try:
            with self._connection() as conn:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO raw.enrichment_carbon_intensity
                            (region, fuel_type, direct_co2_per_mwh, lifecycle_co2_per_mwh,
                             carbon_intensity_category, recorded_at)
                        VALUES
                            (:region, :fuel_type, :direct_co2_per_mwh, :lifecycle_co2_per_mwh,
                             :carbon_intensity_category, :recorded_at)
                        """
                    ),
                    records,
                )
                inserted = result.rowcount
            logger.info("Enrichment carbon intensity written=%d", inserted)
            self._complete_audit(audit_id, inserted)
            return inserted
        except Exception as exc:
            self._fail_audit(audit_id, str(exc))
            raise

    def write_market_prices(
        self, records: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Write market price readings. Returns rows inserted."""
        if not records:
            return 0
        audit_id = self._start_audit(run_id, "enrichment_market_prices", "enrichment_api")
        try:
            with self._connection() as conn:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO raw.enrichment_market_prices
                            (region, spot_price_usd_mwh, peak_price_usd_mwh,
                             off_peak_price_usd_mwh, demand_mw, demand_forecast_mw,
                             price_signal, recorded_at)
                        VALUES
                            (:region, :spot_price_usd_mwh, :peak_price_usd_mwh,
                             :off_peak_price_usd_mwh, :demand_mw, :demand_forecast_mw,
                             :price_signal, :recorded_at)
                        """
                    ),
                    records,
                )
                inserted = result.rowcount
            logger.info("Enrichment market prices written=%d", inserted)
            self._complete_audit(audit_id, inserted)
            return inserted
        except Exception as exc:
            self._fail_audit(audit_id, str(exc))
            raise

    def write_demand_forecast(
        self, records: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Write demand forecast points. Returns rows inserted."""
        if not records:
            return 0
        audit_id = self._start_audit(run_id, "enrichment_demand_forecast", "enrichment_api")
        try:
            with self._connection() as conn:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO raw.enrichment_demand_forecast
                            (region, forecast_hour, forecast_demand_mw,
                             temperature_c, confidence_pct, recorded_at)
                        VALUES
                            (:region, :forecast_hour, :forecast_demand_mw,
                             :temperature_c, :confidence_pct, :recorded_at)
                        """
                    ),
                    records,
                )
                inserted = result.rowcount
            logger.info("Enrichment demand forecast written=%d", inserted)
            self._complete_audit(audit_id, inserted)
            return inserted
        except Exception as exc:
            self._fail_audit(audit_id, str(exc))
            raise


    def write_weather_from_api_responses(
        self, api_responses: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Parse API response dicts and bulk-insert weather records."""
        records = [
            {
                "region":               r["region"],
                "temperature_c":        r["temperature_c"],
                "wind_speed_ms":        r["wind_speed_ms"],
                "solar_irradiance_wm2": r["solar_irradiance_wm2"],
                "humidity_pct":         r["humidity_pct"],
                "cloud_cover_pct":      r["cloud_cover_pct"],
                "weather_condition":    r.get("weather_condition"),
                "recorded_at":          r["recorded_at"],
            }
            for r in api_responses
        ]
        return self.write_weather(records, run_id=run_id)

    def write_carbon_from_api_responses(
        self, api_responses: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Parse API response dicts and bulk-insert carbon intensity records."""
        records = [
            {
                "region":                       r["region"],
                "fuel_type":                    r["fuel_type"],
                "direct_co2_per_mwh":           r["direct_co2_per_mwh"],
                "lifecycle_co2_per_mwh":        r["lifecycle_co2_per_mwh"],
                "carbon_intensity_category":    r.get("carbon_intensity_category"),
                "recorded_at":                  r["recorded_at"],
            }
            for r in api_responses
        ]
        return self.write_carbon_intensity(records, run_id=run_id)

    def write_market_from_api_responses(
        self, api_responses: list[dict[str, Any]], run_id: str = "manual"
    ) -> int:
        """Parse API response dicts and bulk-insert market price records."""
        records = [
            {
                "region":                   r["region"],
                "spot_price_usd_mwh":       r["spot_price_usd_mwh"],
                "peak_price_usd_mwh":       r["peak_price_usd_mwh"],
                "off_peak_price_usd_mwh":   r["off_peak_price_usd_mwh"],
                "demand_mw":                r["demand_mw"],
                "demand_forecast_mw":       r["demand_forecast_mw"],
                "price_signal":             r.get("price_signal"),
                "recorded_at":              r["recorded_at"],
            }
            for r in api_responses
        ]
        return self.write_market_prices(records, run_id=run_id)

    def write_demand_forecast_from_api_response(
        self, api_response: dict[str, Any], run_id: str = "manual"
    ) -> int:
        """Parse a forecast API response dict and bulk-insert demand forecast points."""
        region = api_response["region"]
        recorded_at = api_response["generated_at"]
        records = [
            {
                "region":               region,
                "forecast_hour":        pt["forecast_hour"],
                "forecast_demand_mw":   pt["forecast_demand_mw"],
                "temperature_c":        pt.get("temperature_c"),
                "confidence_pct":       pt.get("confidence_pct"),
                "recorded_at":          recorded_at,
            }
            for pt in api_response.get("forecast", [])
        ]
        return self.write_demand_forecast(records, run_id=run_id)
