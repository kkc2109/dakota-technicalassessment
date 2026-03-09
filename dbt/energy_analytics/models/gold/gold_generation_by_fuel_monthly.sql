{{
    config(
        materialized = 'table',
        description  = 'Gold: monthly electricity generation by fuel type with market share and enrichment context.'
    )
}}

/*
  Gold mart: monthly electricity generation breakdown by fuel type.

  Business questions answered:
  - What is the US electricity generation mix by fuel type, month, and state?
  - What is each fuel type's share of total generation?
  - How does generation correlate with carbon intensity?
*/

with generation as (
    select * from {{ ref('silver_generation_cleaned') }}
    where sector_id = '99'   -- All Sectors (avoids double-counting sub-sectors)
),

carbon as (
    select * from {{ ref('silver_carbon_reference') }}
),

-- Map EIA fuel type codes to enrichment API fuel types
fuel_type_mapping as (
    select *
    from (values
        ('NG',  'natural_gas'),
        ('COL', 'coal'),
        ('NUC', 'nuclear'),
        ('HYC', 'hydro'),
        ('WWW', 'wind'),
        ('SUN', 'solar_pv'),
        ('PET', 'petroleum'),
        ('GEO', 'geothermal'),
        ('OTH', 'other'),
        ('BIO', 'biomass'),
        ('WND', 'wind'),
        ('SOL', 'solar_pv')
    ) as t(eia_fuel_code, enrichment_fuel_type)
),

generation_with_carbon as (
    select
        g.period,
        g.period_year,
        g.period_month,
        g.period_date,
        g.state_code,
        g.state_description,
        g.fuel_type_code,
        g.fuel_type_description,
        sum(g.generation_mwh)                                               as total_generation_mwh,

        -- Carbon intensity (most recent available for this fuel type)
        avg(c.avg_direct_co2_per_mwh)                                       as avg_direct_co2_per_mwh,
        avg(c.avg_lifecycle_co2_per_mwh)                                    as avg_lifecycle_co2_per_mwh,

        -- Estimated carbon emissions (tonnes CO2)
        -- generation_mwh * gCO2/kWh / 1000 / 1000 → tonnes
        round(
            (sum(g.generation_mwh) * avg(coalesce(c.avg_direct_co2_per_mwh, 0)) / 1000000)::numeric,
            2
        )                                                                   as estimated_co2_tonnes
    from generation g
    left join fuel_type_mapping ftm
        on g.fuel_type_code = ftm.eia_fuel_code
    left join carbon c
        on ftm.enrichment_fuel_type = c.fuel_type
        -- Match on most recent carbon reading available (approximate)
        and c.reading_date = (
            select max(reading_date)
            from {{ ref('silver_carbon_reference') }}
            where fuel_type = ftm.enrichment_fuel_type
        )
    group by
        g.period, g.period_year, g.period_month, g.period_date,
        g.state_code, g.state_description,
        g.fuel_type_code, g.fuel_type_description
),

with_market_share as (
    select
        *,
        -- Market share within the same period and state
        round(
            (total_generation_mwh / nullif(
                sum(total_generation_mwh) over (partition by period, state_code),
                0
            ) * 100)::numeric,
            2
        )                                                                   as pct_of_state_generation,

        -- Classify as renewable vs non-renewable
        case
            when fuel_type_code in ('WWW', 'WND', 'SUN', 'SOL', 'HYC', 'GEO', 'BIO')
            then 'renewable'
            when fuel_type_code in ('NUC')
            then 'low_carbon'
            else 'fossil'
        end                                                                 as fuel_category
    from generation_with_carbon
)

select * from with_market_share
order by period desc, state_code, total_generation_mwh desc
