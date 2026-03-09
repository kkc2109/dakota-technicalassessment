{{
    config(
        materialized = 'view',
        description  = 'Bronze: view over raw enrichment market prices table.'
    )
}}

select
    id,
    ingested_at,
    region,
    spot_price_usd_mwh,
    peak_price_usd_mwh,
    off_peak_price_usd_mwh,
    demand_mw,
    demand_forecast_mw,
    price_signal,
    recorded_at,
    source
from {{ source('raw', 'enrichment_market_prices') }}
