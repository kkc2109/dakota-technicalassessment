"""EIA data loader."""

import logging
from typing import Any

from sqlalchemy import text

from ingestion.loaders.base_loader import BaseLoader

logger = logging.getLogger(__name__)


class EIALoader(BaseLoader):
    """Writes EIA electricity data into raw.eia_electricity_generation and raw.eia_electricity_prices."""


    def write_generation(
        self,
        rows: list[dict[str, Any]],
        run_id: str = "manual",
        api_request_id: str | None = None,
    ) -> int:
        """Write EIA generation rows. Returns the number of rows actually inserted."""
        if not rows:
            logger.info("No generation rows to write")
            return 0

        audit_id = self._start_audit(run_id, "eia_electricity_generation", "eia_api")
        inserted = 0

        try:
            batch = [self._parse_generation_row(r, api_request_id) for r in rows]
            # Filter out rows that failed parsing
            batch = [r for r in batch if r is not None]

            with self._connection() as conn:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO raw.eia_electricity_generation
                            (period, state_code, state_description,
                             sector_id, sector_description,
                             fuel_type_code, fuel_type_description,
                             generation_mwh, generation_units,
                             source, api_request_id)
                        VALUES
                            (:period, :state_code, :state_description,
                             :sector_id, :sector_description,
                             :fuel_type_code, :fuel_type_description,
                             :generation_mwh, :generation_units,
                             :source, :api_request_id)
                        ON CONFLICT (period, state_code, sector_id, fuel_type_code)
                        DO NOTHING
                        """
                    ),
                    batch,
                )
                inserted = result.rowcount

            logger.info("EIA generation written=%d skipped=%d", inserted, len(batch) - inserted)
            self._complete_audit(audit_id, inserted)
        except Exception as exc:
            self._fail_audit(audit_id, str(exc))
            raise

        return inserted

    def _parse_generation_row(
        self, row: dict[str, Any], api_request_id: str | None
    ) -> dict[str, Any] | None:
        """Map EIA API response fields to DB column names."""
        try:
            raw_gen = row.get("generation")
            return {
                "period":               str(row["period"]),
                "state_code":           str(row.get("location") or row.get("stateid") or ""),
                "state_description":    row.get("stateDescription") or row.get("locationDescription"),
                "sector_id":            str(row.get("sectorid") or row.get("sectorId") or ""),
                "sector_description":   row.get("sectorDescription"),
                "fuel_type_code":       str(row.get("fueltypeid") or row.get("fuelTypeId") or ""),
                "fuel_type_description": row.get("fuelTypeDescription"),
                "generation_mwh":       float(raw_gen) if raw_gen is not None else None,
                "generation_units":     row.get("generation-units"),
                "source":               "eia_api",
                "api_request_id":       api_request_id,
            }
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed generation row %s: %s", row, exc)
            return None


    def write_prices(
        self,
        rows: list[dict[str, Any]],
        run_id: str = "manual",
        api_request_id: str | None = None,
    ) -> int:
        """Write EIA price rows. Returns the number of rows actually inserted."""
        if not rows:
            logger.info("No price rows to write")
            return 0

        audit_id = self._start_audit(run_id, "eia_electricity_prices", "eia_api")
        inserted = 0

        try:
            batch = [self._parse_price_row(r, api_request_id) for r in rows]
            batch = [r for r in batch if r is not None]

            with self._connection() as conn:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO raw.eia_electricity_prices
                            (period, state_code, state_description,
                             sector_id, sector_name,
                             price_cents_per_kwh, revenue_million_dollars,
                             sales_million_kwh, customers,
                             source, api_request_id)
                        VALUES
                            (:period, :state_code, :state_description,
                             :sector_id, :sector_name,
                             :price_cents_per_kwh, :revenue_million_dollars,
                             :sales_million_kwh, :customers,
                             :source, :api_request_id)
                        ON CONFLICT (period, state_code, sector_id)
                        DO NOTHING
                        """
                    ),
                    batch,
                )
                inserted = result.rowcount

            logger.info("EIA prices written=%d skipped=%d", inserted, len(batch) - inserted)
            self._complete_audit(audit_id, inserted)
        except Exception as exc:
            self._fail_audit(audit_id, str(exc))
            raise

        return inserted

    def _parse_price_row(
        self, row: dict[str, Any], api_request_id: str | None
    ) -> dict[str, Any] | None:
        try:
            def _float(v: Any) -> float | None:
                return float(v) if v is not None else None

            def _int(v: Any) -> int | None:
                return int(float(v)) if v is not None else None

            return {
                "period":                   str(row["period"]),
                "state_code":               str(row.get("stateid") or row.get("stateId") or ""),
                "state_description":        row.get("stateDescription"),
                "sector_id":                str(row.get("sectorid") or row.get("sectorId") or ""),
                "sector_name":              row.get("sectorName"),
                "price_cents_per_kwh":      _float(row.get("price")),
                "revenue_million_dollars":  _float(row.get("revenue")),
                "sales_million_kwh":        _float(row.get("sales")),
                "customers":                _int(row.get("customers")),
                "source":                   "eia_api",
                "api_request_id":           api_request_id,
            }
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed price row %s: %s", row, exc)
            return None
