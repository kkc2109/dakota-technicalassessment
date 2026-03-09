{{
    config(
        materialized = 'table',
        description  = 'Gold: estimated carbon footprint by state, fuel type, and month with renewable share.'
    )
}}

/*
  Gold mart: carbon footprint analysis.

  Business questions answered:
  - What is the estimated CO2 footprint of electricity generation by state?
  - What is each state's renewable energy share?
  - Which states have the cleanest / dirtiest grid?
  - How is the carbon footprint trending over time?
*/

with generation as (
    select * from {{ ref('gold_generation_by_fuel_monthly') }}
),

state_monthly_totals as (
    select
        period,
        period_year,
        period_month,
        period_date,
        state_code,
        state_description,

        -- Total generation
        sum(total_generation_mwh)                                               as total_generation_mwh,

        -- Total estimated CO2 emissions
        sum(estimated_co2_tonnes)                                               as total_co2_tonnes,

        -- Generation by category
        sum(case when fuel_category = 'renewable' then total_generation_mwh else 0 end)
                                                                                as renewable_generation_mwh,
        sum(case when fuel_category = 'low_carbon' then total_generation_mwh else 0 end)
                                                                                as low_carbon_generation_mwh,
        sum(case when fuel_category = 'fossil' then total_generation_mwh else 0 end)
                                                                                as fossil_generation_mwh,

        -- Top fuel types by generation
        max(case when fuel_type_code = 'NG'  then total_generation_mwh end)    as natural_gas_mwh,
        max(case when fuel_type_code = 'COL' then total_generation_mwh end)    as coal_mwh,
        max(case when fuel_type_code = 'NUC' then total_generation_mwh end)    as nuclear_mwh,
        max(case when fuel_type_code in ('WWW','WND') then total_generation_mwh end) as wind_mwh,
        max(case when fuel_type_code in ('SUN','SOL') then total_generation_mwh end) as solar_mwh,
        max(case when fuel_type_code = 'HYC' then total_generation_mwh end)    as hydro_mwh
    from generation
    group by period, period_year, period_month, period_date, state_code, state_description
),

with_metrics as (
    select
        *,

        -- Renewable share (renewable + low_carbon / total)
        round(
            (renewable_generation_mwh / nullif(total_generation_mwh, 0) * 100)::numeric,
            2
        )                                                                       as renewable_share_pct,

        round(
            ((renewable_generation_mwh + low_carbon_generation_mwh) / nullif(total_generation_mwh, 0) * 100)::numeric,
            2
        )                                                                       as clean_energy_share_pct,

        -- Grid carbon intensity (gCO2/kWh)
        -- tonnes CO2 / MWh * 1,000,000 (tonnes→grams) / 1000 (MWh→kWh) = gCO2/kWh
        round(
            (total_co2_tonnes * 1000 / nullif(total_generation_mwh, 0))::numeric,
            2
        )                                                                       as grid_carbon_intensity_gco2_kwh,

        -- YoY change in CO2 intensity
        round(
            (
                (total_co2_tonnes / nullif(total_generation_mwh, 0))
                - lag(total_co2_tonnes / nullif(total_generation_mwh, 0), 12) over (
                    partition by state_code
                    order by period_date
                )
            ) / nullif(
                lag(total_co2_tonnes / nullif(total_generation_mwh, 0), 12) over (
                    partition by state_code
                    order by period_date
                ),
            0) * 100,
        2)                                                                      as carbon_intensity_yoy_change_pct
    from state_monthly_totals
)

select * from with_metrics
order by period desc, total_co2_tonnes desc
