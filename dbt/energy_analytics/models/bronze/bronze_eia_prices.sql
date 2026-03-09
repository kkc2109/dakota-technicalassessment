{{
    config(
        materialized = 'view',
        description  = 'Bronze: view over raw EIA retail electricity prices table.'
    )
}}

select
    id,
    ingested_at,
    period,
    state_code,
    state_description,
    sector_id,
    sector_name,
    price_cents_per_kwh,
    revenue_million_dollars,
    sales_million_kwh,
    customers,
    source,
    api_request_id
from {{ source('raw', 'eia_electricity_prices') }}
