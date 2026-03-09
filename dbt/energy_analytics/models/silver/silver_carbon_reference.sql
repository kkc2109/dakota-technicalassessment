{{
    config(
        materialized        = 'incremental',
        unique_key          = ['region', 'fuel_type', 'reading_date'],
        incremental_strategy = 'merge',
        on_schema_change    = 'append_new_columns',
        description         = 'Silver: deduplicated daily carbon intensity reference by region and fuel type.'
    )
}}

/*
  Carbon intensity values are relatively stable (based on fixed reference factors
  with small regional variance). This model deduplicates to daily averages per
  region × fuel_type for joining with monthly EIA generation data in Gold.
*/

with source as (
    select * from {{ ref('bronze_carbon_intensity') }}

    {% if is_incremental() %}
    where ingested_at > (
        select coalesce(max(ingested_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
),

daily_avg as (
    select
        region,
        lower(trim(fuel_type))                              as fuel_type,
        recorded_at::date                                   as reading_date,

        round(avg(direct_co2_per_mwh)::numeric, 4)         as avg_direct_co2_per_mwh,
        round(avg(lifecycle_co2_per_mwh)::numeric, 4)      as avg_lifecycle_co2_per_mwh,

        -- Take the most recent category for the day
        (array_agg(carbon_intensity_category order by recorded_at desc))[1] as carbon_intensity_category,

        count(*)                                            as reading_count,
        max(ingested_at)                                    as ingested_at
    from source
    where fuel_type is not null
      and region is not null
    group by region, lower(trim(fuel_type)), recorded_at::date
)

select * from daily_avg
