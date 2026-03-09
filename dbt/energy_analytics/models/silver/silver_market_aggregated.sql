{{
    config(
        materialized        = 'incremental',
        unique_key          = ['region', 'reading_date'],
        incremental_strategy = 'merge',
        on_schema_change    = 'append_new_columns',
        description         = 'Silver: daily aggregated electricity market prices per grid region.'
    )
}}

with source as (
    select * from {{ ref('bronze_market_prices') }}

    {% if is_incremental() %}
    where ingested_at > (
        select coalesce(max(ingested_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
),

daily_agg as (
    select
        region,
        recorded_at::date                                       as reading_date,

        round(avg(spot_price_usd_mwh)::numeric, 4)             as avg_spot_price_usd_mwh,
        round(min(spot_price_usd_mwh)::numeric, 4)             as min_spot_price_usd_mwh,
        round(max(spot_price_usd_mwh)::numeric, 4)             as max_spot_price_usd_mwh,
        round(avg(peak_price_usd_mwh)::numeric, 4)             as avg_peak_price_usd_mwh,
        round(avg(off_peak_price_usd_mwh)::numeric, 4)         as avg_off_peak_price_usd_mwh,
        round(avg(demand_mw)::numeric, 2)                      as avg_demand_mw,
        round(max(demand_mw)::numeric, 2)                      as peak_demand_mw,
        round(avg(demand_forecast_mw)::numeric, 2)             as avg_demand_forecast_mw,

        -- Price volatility (coefficient of variation)
        case
            when avg(spot_price_usd_mwh) > 0
            then round(
                (stddev(spot_price_usd_mwh) / nullif(avg(spot_price_usd_mwh), 0) * 100)::numeric,
                2
            )
            else 0
        end                                                     as price_volatility_pct,

        count(*)                                                as reading_count,
        max(ingested_at)                                        as ingested_at
    from source
    where spot_price_usd_mwh is not null
      and region is not null
    group by region, recorded_at::date
)

select * from daily_agg
