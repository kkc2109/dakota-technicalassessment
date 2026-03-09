{{
    config(
        materialized        = 'incremental',
        unique_key          = ['period', 'state_code', 'sector_id'],
        incremental_strategy = 'merge',
        on_schema_change    = 'append_new_columns',
        description         = 'Silver: cleaned and standardised EIA retail price data.'
    )
}}

with source as (
    select * from {{ ref('bronze_eia_prices') }}

    {% if is_incremental() %}
    where ingested_at > (
        select coalesce(max(ingested_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
),

cleaned as (
    select
        id,
        ingested_at,

        period,
        split_part(period, '-', 1)::integer                 as period_year,
        split_part(period, '-', 2)::integer                 as period_month,
        to_date(period || '-01', 'YYYY-MM-DD')              as period_date,

        upper(trim(state_code))                             as state_code,
        trim(state_description)                             as state_description,
        upper(trim(sector_id))                              as sector_id,
        trim(sector_name)                                   as sector_name,

        -- Convert cents/kWh → USD/MWh for consistent units across the pipeline
        -- 1 USD/MWh = 0.1 cents/kWh  →  multiply by 10
        coalesce(price_cents_per_kwh, 0) * 10               as price_usd_per_mwh,
        coalesce(price_cents_per_kwh, 0)                    as price_cents_per_kwh,

        coalesce(revenue_million_dollars, 0)                as revenue_million_dollars,
        coalesce(sales_million_kwh, 0)                      as sales_million_kwh,
        coalesce(customers, 0)                              as customers,

        source
    from source
    where price_cents_per_kwh is not null
      and price_cents_per_kwh > 0
      and state_code is not null
      and trim(state_code) != ''
)

select * from cleaned
