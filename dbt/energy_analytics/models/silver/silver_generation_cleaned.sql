{{
    config(
        materialized        = 'incremental',
        unique_key          = ['period', 'state_code', 'sector_id', 'fuel_type_code'],
        incremental_strategy = 'merge',
        on_schema_change    = 'append_new_columns',
        description         = 'Silver: cleaned and standardised EIA generation data.'
    )
}}

/*
  Silver transformation for EIA generation data.

  Changes from Bronze:
  - Null generation_mwh rows filtered out
  - fuel_type_code standardised to uppercase
  - state_code standardised to uppercase
  - generation converted from thousand MWh to MWh for consistency
  - year and month extracted for easier aggregation downstream
  - Incremental: only processes new records since last run
*/

with source as (
    select * from {{ ref('bronze_eia_generation') }}

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

        -- Standardise period fields
        period,
        split_part(period, '-', 1)::integer                 as period_year,
        split_part(period, '-', 2)::integer                 as period_month,
        to_date(period || '-01', 'YYYY-MM-DD')              as period_date,

        -- Standardise codes
        upper(trim(state_code))                             as state_code,
        trim(state_description)                             as state_description,
        upper(trim(sector_id))                              as sector_id,
        trim(sector_description)                            as sector_description,
        upper(trim(fuel_type_code))                         as fuel_type_code,
        trim(fuel_type_description)                         as fuel_type_description,

        -- Convert thousand MWh → MWh for consistent units
        coalesce(generation_mwh, 0) * 1000                  as generation_mwh,
        'MWh'                                               as generation_units_normalised,

        source
    from source
    where generation_mwh is not null
      and state_code is not null
      and trim(state_code) != ''
)

select * from cleaned
