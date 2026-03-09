{{
    config(
        materialized = 'table',
        description  = 'Gold: monthly retail electricity price trends by state and sector with YoY change.'
    )
}}

/*
  Gold mart: electricity price trends.

  Business questions answered:
  - How are retail electricity prices trending by state and sector?
  - What is the year-over-year price change?
  - How do residential prices compare with commercial and industrial?
  - How do retail prices correlate with wholesale spot prices?
*/

with prices as (
    select * from {{ ref('silver_prices_cleaned') }}
),

market as (
    select * from {{ ref('silver_market_aggregated') }}
),

-- State-to-region mapping for joining with enrichment market data
state_region_map as (
    select * from (values
        ('TX', 'ERCOT'), ('CA', 'CAISO'), ('NY', 'NYISO'),
        ('PA', 'PJM'),   ('OH', 'PJM'),   ('IL', 'MISO'),
        ('MI', 'MISO'),  ('MN', 'MISO'),  ('FL', 'SERC'),
        ('GA', 'SERC'),  ('NC', 'SERC'),  ('VA', 'PJM'),
        ('MA', 'NEISO'), ('CT', 'NEISO'), ('RI', 'NEISO'),
        ('ME', 'NEISO'), ('NH', 'NEISO'), ('VT', 'NEISO'),
        ('KS', 'SPP'),   ('OK', 'SPP'),   ('NE', 'SPP'),
        ('CO', 'WECC'),  ('AZ', 'WECC'),  ('NM', 'WECC'),
        ('WA', 'WECC'),  ('OR', 'WECC'),  ('NV', 'WECC'),
        ('ID', 'WECC'),  ('MT', 'WECC'),  ('WY', 'WECC'),
        ('UT', 'WECC')
    ) as t(state_code, region)
),

price_with_yoy as (
    select
        p.period,
        p.period_year,
        p.period_month,
        p.period_date,
        p.state_code,
        p.state_description,
        p.sector_id,
        p.sector_name,
        p.price_usd_per_mwh,
        p.price_cents_per_kwh,
        p.revenue_million_dollars,
        p.sales_million_kwh,
        p.customers,

        -- Year-over-year price change (%)
        round(
            (p.price_usd_per_mwh - lag(p.price_usd_per_mwh, 12) over (
                partition by p.state_code, p.sector_id
                order by p.period_date
            )) / nullif(lag(p.price_usd_per_mwh, 12) over (
                partition by p.state_code, p.sector_id
                order by p.period_date
            ), 0) * 100,
        2)                                                          as price_yoy_change_pct,

        -- Month-over-month change
        round(
            (p.price_usd_per_mwh - lag(p.price_usd_per_mwh, 1) over (
                partition by p.state_code, p.sector_id
                order by p.period_date
            )) / nullif(lag(p.price_usd_per_mwh, 1) over (
                partition by p.state_code, p.sector_id
                order by p.period_date
            ), 0) * 100,
        2)                                                          as price_mom_change_pct,

        -- Rolling 12-month average
        round(
            avg(p.price_usd_per_mwh) over (
                partition by p.state_code, p.sector_id
                order by p.period_date
                rows between 11 preceding and current row
            )::numeric,
        2)                                                          as rolling_12m_avg_price_usd_mwh,

        sr.region
    from prices p
    left join state_region_map sr on p.state_code = sr.state_code
),

with_wholesale as (
    select
        pwyw.*,
        m.avg_spot_price_usd_mwh                                    as wholesale_spot_price_usd_mwh,
        m.avg_peak_price_usd_mwh                                    as wholesale_peak_price_usd_mwh,

        -- Retail-to-wholesale spread (approximation)
        round(
            (pwyw.price_usd_per_mwh - coalesce(m.avg_spot_price_usd_mwh, 0))::numeric,
            2
        )                                                           as retail_wholesale_spread_usd_mwh
    from price_with_yoy pwyw
    left join market m
        on pwyw.region = m.region
        and pwyw.period_date = m.reading_date
)

select * from with_wholesale
order by period desc, state_code, sector_id
