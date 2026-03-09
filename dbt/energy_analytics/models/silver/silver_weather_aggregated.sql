{{
    config(
        materialized        = 'incremental',
        unique_key          = ['region', 'reading_date'],
        incremental_strategy = 'merge',
        on_schema_change    = 'append_new_columns',
        description         = 'Silver: daily aggregated weather metrics per grid region.'
    )
}}

/*
  Aggregates sub-daily weather readings into daily summaries per region.
  This aligns the enrichment data granularity with the monthly EIA data
  by providing daily averages that can be further aggregated in Gold.
*/

with source as (
    select * from {{ ref('bronze_weather') }}

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
        recorded_at::date                       as reading_date,

        -- Temperature statistics
        round(avg(temperature_c)::numeric, 2)   as avg_temperature_c,
        round(min(temperature_c)::numeric, 2)   as min_temperature_c,
        round(max(temperature_c)::numeric, 2)   as max_temperature_c,

        -- Wind statistics
        round(avg(wind_speed_ms)::numeric, 2)   as avg_wind_speed_ms,
        round(max(wind_speed_ms)::numeric, 2)   as max_wind_speed_ms,

        -- Solar statistics
        round(avg(solar_irradiance_wm2)::numeric, 2) as avg_solar_irradiance_wm2,
        round(max(solar_irradiance_wm2)::numeric, 2) as peak_solar_irradiance_wm2,

        -- Atmospheric
        round(avg(humidity_pct)::numeric, 2)    as avg_humidity_pct,
        round(avg(cloud_cover_pct)::numeric, 2) as avg_cloud_cover_pct,

        count(*)                                as reading_count,
        max(ingested_at)                        as ingested_at
    from source
    where temperature_c is not null
      and region is not null
    group by region, recorded_at::date
)

select * from daily_agg
