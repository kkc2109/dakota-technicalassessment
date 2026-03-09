{{
    config(
        materialized = 'view',
        description  = 'Bronze: view over raw enrichment carbon intensity table.'
    )
}}

select
    id,
    ingested_at,
    region,
    fuel_type,
    direct_co2_per_mwh,
    lifecycle_co2_per_mwh,
    carbon_intensity_category,
    recorded_at,
    source
from {{ source('raw', 'enrichment_carbon_intensity') }}
