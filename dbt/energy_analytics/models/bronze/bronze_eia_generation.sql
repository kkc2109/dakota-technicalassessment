{{
    config(
        materialized = 'view',
        description  = 'Bronze: view over raw EIA electricity generation table.'
    )
}}

/*
  Bronze layer model — thin view over the raw source table.
  No transformation is performed here. The purpose is to:
  1. Provide a stable dbt-managed reference that Silver models can depend on.
  2. Surface source lineage in the Dagster asset graph.
  3. Allow source tests (defined in sources.yml) to run as part of dbt build.
*/

select
    id,
    ingested_at,
    period,
    state_code,
    state_description,
    sector_id,
    sector_description,
    fuel_type_code,
    fuel_type_description,
    generation_mwh,
    generation_units,
    source,
    api_request_id
from {{ source('raw', 'eia_electricity_generation') }}
