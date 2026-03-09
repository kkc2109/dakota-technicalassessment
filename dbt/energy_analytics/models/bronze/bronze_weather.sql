{{
    config(
        materialized = 'view',
        description  = 'Bronze: view over raw enrichment weather table.'
    )
}}

select
    id,
    ingested_at,
    region,
    temperature_c,
    wind_speed_ms,
    solar_irradiance_wm2,
    humidity_pct,
    cloud_cover_pct,
    weather_condition,
    recorded_at,
    source
from {{ source('raw', 'enrichment_weather') }}
