{{
    config(
        materialized = 'table',
        description  = 'Gold: executive KPI summary — the single table powering CEO-level reports.'
    )
}}

/*
  Gold mart: executive summary KPIs.

  This is the primary source for the CEO report. It provides the latest
  available metrics across all key dimensions in a single, flat table
  that is easy to query and visualise.

  One row per period (month) per state.
*/

with generation as (
    select * from {{ ref('gold_carbon_footprint') }}
),

prices as (
    select *
    from {{ ref('gold_price_trends') }}
    where sector_id = 'RES'   -- Residential retail price as the headline metric
),

market as (
    select * from {{ ref('silver_market_aggregated') }}
),

weather as (
    select * from {{ ref('silver_weather_aggregated') }}
),

state_region_map as (
    select * from (values
        ('TX', 'ERCOT'), ('CA', 'CAISO'), ('NY', 'NYISO'),
        ('PA', 'PJM'),   ('OH', 'PJM'),   ('IL', 'MISO'),
        ('MI', 'MISO'),  ('MN', 'MISO'),  ('FL', 'SERC'),
        ('GA', 'SERC'),  ('NC', 'SERC'),  ('VA', 'PJM'),
        ('MA', 'NEISO'), ('CT', 'NEISO'), ('TX', 'ERCOT')
    ) as t(state_code, region)
),

combined as (
    select
        -- Identifiers
        g.period,
        g.period_year,
        g.period_month,
        g.period_date,
        g.state_code,
        g.state_description,
        coalesce(srm.region, 'OTHER')                               as grid_region,

        -- Generation KPIs
        g.total_generation_mwh,
        g.renewable_generation_mwh,
        g.low_carbon_generation_mwh,
        g.fossil_generation_mwh,
        g.renewable_share_pct,
        g.clean_energy_share_pct,
        g.natural_gas_mwh,
        g.coal_mwh,
        g.nuclear_mwh,
        g.wind_mwh,
        g.solar_mwh,
        g.hydro_mwh,

        -- Carbon KPIs
        g.total_co2_tonnes,
        g.grid_carbon_intensity_gco2_kwh,
        g.carbon_intensity_yoy_change_pct,

        -- Price KPIs
        p.price_usd_per_mwh                                         as residential_price_usd_mwh,
        p.price_cents_per_kwh                                       as residential_price_cents_kwh,
        p.price_yoy_change_pct                                      as residential_price_yoy_change_pct,
        p.rolling_12m_avg_price_usd_mwh,

        -- Market KPIs (wholesale)
        m.avg_spot_price_usd_mwh                                    as wholesale_spot_price_usd_mwh,
        m.avg_peak_price_usd_mwh                                    as wholesale_peak_price_usd_mwh,
        m.avg_demand_mw,
        m.peak_demand_mw,
        m.price_volatility_pct,

        -- Weather context
        w.avg_temperature_c,
        w.avg_wind_speed_ms,
        w.avg_solar_irradiance_wm2,

        -- Derived: revenue from renewables at average retail price
        round(
            (g.renewable_generation_mwh * coalesce(p.price_usd_per_mwh, 0) / 1000000)::numeric,
            4
        )                                                           as renewable_revenue_million_usd
    from generation g
    left join prices p
        on g.state_code = p.state_code
        and g.period    = p.period
    left join state_region_map srm
        on g.state_code = srm.state_code
    left join market m
        on coalesce(srm.region, 'OTHER') = m.region
        and g.period_date = m.reading_date
    left join weather w
        on coalesce(srm.region, 'OTHER') = w.region
        and g.period_date = w.reading_date
)

select * from combined
order by period desc, total_generation_mwh desc nulls last
