"""Jupyter Notebook Report Generator.

Programmatically creates and executes a Jupyter notebook using nbformat,
then renders it to a standalone HTML file via nbconvert.

The notebook contains:
  - Section 1: Executive KPI Summary (scorecard metrics)
  - Section 2: Electricity Generation Mix (interactive Plotly charts)
  - Section 3: Price Analysis (trends, YoY, sector comparison)
  - Section 4: Carbon Footprint & Sustainability
  - Section 5: Regional Market Performance
  - Section 6: Data Quality & Pipeline Summary

"""

import logging
import os
import textwrap
from pathlib import Path

import nbformat
from nbconvert import HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor

logger = logging.getLogger(__name__)

_DB_SETUP_CODE = textwrap.dedent("""
import os, warnings
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')

# ---------- Database connection ----------
def get_engine():
    url = (
        f"postgresql://{os.getenv('POSTGRES_USER','dakota_user')}"
        f":{os.getenv('POSTGRES_PASSWORD','change_me')}"
        f"@{os.getenv('POSTGRES_HOST','localhost')}"
        f":{os.getenv('POSTGRES_PORT','5432')}"
        f"/{os.getenv('POSTGRES_DB','energy_analytics')}"
    )
    return create_engine(url, pool_pre_ping=True)

def qdf(sql):
    engine = get_engine()
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

COLORS = {
    'coal': '#2C3E50', 'natural_gas': '#2980B9', 'nuclear': '#8E44AD',
    'wind': '#27AE60', 'solar_pv': '#F39C12', 'hydro': '#1ABC9C',
    'petroleum': '#C0392B', 'biomass': '#D35400', 'geothermal': '#16A085',
    'other': '#95A5A6',
}
BRAND_BLUE = '#1B3A5C'
BRAND_GREEN = '#27AE60'
BRAND_RED = '#C0392B'

PLOTLY_LAYOUT = dict(
    font=dict(family='Calibri, Arial', size=12),
    paper_bgcolor='white',
    plot_bgcolor='#FAFAFA',
    margin=dict(l=60, r=40, t=70, b=60),
)
print("Environment ready ✓")
""")

_SECTION1_CODE = textwrap.dedent("""
# ──────────────────────────────────────────────────────────────────────────────
# SECTION 1 | Executive KPI Summary
# ──────────────────────────────────────────────────────────────────────────────
from IPython.display import display, HTML
from datetime import datetime

kpis = qdf('''
    select
        round(sum(total_generation_mwh) / 1e6, 2)          as total_gen_twh,
        round(avg(renewable_share_pct), 1)                  as avg_renewable_pct,
        round(avg(clean_energy_share_pct), 1)               as avg_clean_pct,
        round(sum(total_co2_tonnes) / 1e6, 2)               as total_co2_mt,
        round(avg(grid_carbon_intensity_gco2_kwh), 1)       as avg_carbon_intensity,
        round(avg(residential_price_cents_kwh), 2)          as avg_residential_price,
        round(avg(wholesale_spot_price_usd_mwh), 2)         as avg_wholesale_price,
        count(distinct state_code)                          as states_covered,
        max(period)                                         as latest_period
    from analytics.gold_executive_summary
    where period_year >= extract(year from now()) - 1
''')

if not kpis.empty:
    row = kpis.iloc[0]
    html = f'''
    <div style='font-family:Calibri,Arial;'>
    <div style='background:#1B3A5C;color:white;padding:20px 30px;border-radius:8px 8px 0 0;'>
      <h1 style='margin:0;font-size:24px;'>Dakota Analytics | Energy Market Executive Report</h1>
      <p style='margin:5px 0 0;opacity:0.8;font-size:13px;'>
        Generated {datetime.now().strftime("%B %d, %Y")} &middot;
        Data through {row.get("latest_period","N/A")} &middot;
        {int(row.get("states_covered",0))} US states
      </p>
    </div>
    <div style='display:flex;flex-wrap:wrap;background:#F4F6F9;padding:20px;border-radius:0 0 8px 8px;gap:12px;'>
    '''
    metrics = [
        ("Total Generation",        f"{row.get('total_gen_twh','N/A')} TWh",        "#2E6DA4"),
        ("Renewable Share",          f"{row.get('avg_renewable_pct','N/A')}%",        "#27AE60"),
        ("Clean Energy Share",       f"{row.get('avg_clean_pct','N/A')}%",            "#27AE60"),
        ("Total CO₂ Emissions",      f"{row.get('total_co2_mt','N/A')} Mt",           "#C0392B"),
        ("Grid Carbon Intensity",    f"{row.get('avg_carbon_intensity','N/A')} gCO₂/kWh", "#F39C12"),
        ("Avg Residential Price",    f"{row.get('avg_residential_price','N/A')} ¢/kWh",  "#2E6DA4"),
        ("Avg Wholesale Price",      f"${row.get('avg_wholesale_price','N/A')}/MWh",   "#2E6DA4"),
    ]
    for label, value, color in metrics:
        html += f'''
        <div style='background:white;border-radius:6px;padding:14px 20px;min-width:160px;
                    box-shadow:0 2px 6px rgba(0,0,0,.08);flex:1;'>
          <div style='color:#777;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;'>{label}</div>
          <div style='color:{color};font-size:22px;font-weight:bold;margin-top:4px;'>{value}</div>
        </div>'''
    html += "</div></div>"
    display(HTML(html))
else:
    print("No data available — pipeline may not have run yet.")
""")

_SECTION2_CODE = textwrap.dedent("""
# ──────────────────────────────────────────────────────────────────────────────
# SECTION 2 | Electricity Generation Mix
# ──────────────────────────────────────────────────────────────────────────────
gen = qdf('''
    select period_year, fuel_type_description, fuel_category,
           round(sum(total_generation_mwh)/1e6,2) as generation_twh,
           round(avg(pct_of_state_generation),2) as avg_share_pct
    from analytics.gold_generation_by_fuel_monthly
    where fuel_type_description is not null and period_year >= 2020
    group by period_year, fuel_type_description, fuel_category
    order by period_year, generation_twh desc
''')

if not gen.empty:
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Annual Generation (TWh)", "Generation Share by Category"),
                        specs=[[{"type": "xy"}, {"type": "domain"}]])

    # Stacked bar by fuel type
    for fuel in gen['fuel_type_description'].unique():
        fd = gen[gen['fuel_type_description'] == fuel]
        color = COLORS.get(fuel.lower().replace(' ','_'), '#95A5A6')
        fig.add_trace(
            go.Bar(name=fuel, x=fd['period_year'], y=fd['generation_twh'],
                   marker_color=color, legendgroup=fuel),
            row=1, col=1
        )

    # Pie by category (latest year)
    latest = gen[gen['period_year'] == gen['period_year'].max()]
    cat_df = latest.groupby('fuel_category')['generation_twh'].sum().reset_index()
    cat_colors = {'renewable':'#27AE60','low_carbon':'#8E44AD','fossil':'#C0392B'}
    fig.add_trace(
        go.Pie(labels=cat_df['fuel_category'],
               values=cat_df['generation_twh'],
               marker_colors=[cat_colors.get(c,'#95A5A6') for c in cat_df['fuel_category']],
               hole=0.4, name=''),
        row=1, col=2
    )

    fig.update_layout(
        title=dict(text='<b>US Electricity Generation Mix</b>', font=dict(size=18,color=BRAND_BLUE)),
        barmode='stack', height=500, showlegend=True, **PLOTLY_LAYOUT
    )
    display(HTML(fig.to_html(include_plotlyjs='cdn', full_html=False)))
else:
    print("Generation data not yet available.")
""")

_SECTION3_CODE = textwrap.dedent("""
# ──────────────────────────────────────────────────────────────────────────────
# SECTION 3 | Price Analysis
# ──────────────────────────────────────────────────────────────────────────────
prices = qdf('''
    select period, period_year, state_code,
           sector_id, sector_name,
           round(price_cents_per_kwh,2) as price_cents_kwh,
           round(price_yoy_change_pct,1) as yoy_change_pct,
           round(rolling_12m_avg_price_usd_mwh,2) as rolling_avg
    from analytics.gold_price_trends
    where sector_id in (\'RES\',\'COM\',\'IND\') and period_year >= 2021
    order by period desc, state_code
    limit 2000
''')

if not prices.empty:
    # Avg price by sector over time
    sector_trend = prices.groupby(['period','sector_id','sector_name'])['price_cents_kwh'].mean().reset_index()

    fig = go.Figure()
    sector_colors = {'RES':'#2E6DA4','COM':'#F39C12','IND':'#27AE60'}
    for sector in sector_trend['sector_id'].unique():
        sd = sector_trend[sector_trend['sector_id'] == sector].sort_values('period')
        name = sd['sector_name'].iloc[0] if not sd.empty else sector
        fig.add_trace(go.Scatter(
            x=sd['period'], y=sd['price_cents_kwh'],
            mode='lines+markers', name=name,
            line=dict(color=sector_colors.get(sector,'#555'), width=2.5),
            marker=dict(size=5),
        ))

    fig.update_layout(
        title=dict(text='<b>Average Retail Electricity Price by Sector (¢/kWh)</b>',
                   font=dict(size=16,color=BRAND_BLUE)),
        xaxis_title='Period', yaxis_title='Price (¢/kWh)',
        height=420, **PLOTLY_LAYOUT
    )
    display(HTML(fig.to_html(include_plotlyjs='cdn', full_html=False)))

    # YoY change distribution
    yoy = prices[prices['yoy_change_pct'].notna()][['state_code','sector_id','yoy_change_pct']].copy()
    if not yoy.empty:
        fig2 = px.box(yoy, x='sector_id', y='yoy_change_pct', color='sector_id',
                      title='<b>Year-over-Year Price Change Distribution by Sector</b>',
                      labels={'yoy_change_pct':'YoY Change (%)', 'sector_id':'Sector'},
                      color_discrete_map=sector_colors)
        fig2.update_layout(height=380, showlegend=False, **PLOTLY_LAYOUT)
        display(HTML(fig2.to_html(include_plotlyjs=False, full_html=False)))
else:
    print("Price data not yet available.")
""")

_SECTION4_CODE = textwrap.dedent("""
# ──────────────────────────────────────────────────────────────────────────────
# SECTION 4 | Carbon Footprint & Sustainability
# ──────────────────────────────────────────────────────────────────────────────
carbon = qdf('''
    select period, period_year, state_code, state_description,
           round(total_co2_tonnes/1e6,3) as co2_mt,
           round(renewable_share_pct,1) as renewable_pct,
           round(clean_energy_share_pct,1) as clean_pct,
           round(grid_carbon_intensity_gco2_kwh,1) as carbon_intensity,
           round(carbon_intensity_yoy_change_pct,1) as intensity_yoy_change
    from analytics.gold_carbon_footprint
    where period_year >= 2021
    order by period desc, co2_mt desc nulls last
    limit 2000
''')

if not carbon.empty:
    # State renewable share — latest period
    latest = carbon[carbon['period'] == carbon['period'].max()]
    top20 = latest.dropna(subset=['renewable_pct']).nlargest(20, 'renewable_pct')

    fig = px.bar(top20, x='state_code', y='renewable_pct', color='renewable_pct',
                 color_continuous_scale=[(0,'#C0392B'),(0.4,'#F39C12'),(1,'#27AE60')],
                 title=f'<b>Top 20 States by Renewable Energy Share ({carbon["period"].max()})</b>',
                 labels={'renewable_pct':'Renewable Share (%)', 'state_code':'State'})
    fig.add_hline(y=top20['renewable_pct'].mean(), line_dash='dash',
                  annotation_text=f"Average: {top20['renewable_pct'].mean():.1f}%",
                  line_color='#2C3E50')
    fig.update_layout(height=430, coloraxis_showscale=False, **PLOTLY_LAYOUT)
    display(HTML(fig.to_html(include_plotlyjs='cdn', full_html=False)))

    # Carbon intensity trend (national average)
    nat_trend = carbon.groupby('period')['carbon_intensity'].mean().reset_index().sort_values('period')
    fig2 = go.Figure(go.Scatter(
        x=nat_trend['period'], y=nat_trend['carbon_intensity'],
        mode='lines+markers', fill='tozeroy',
        line=dict(color=BRAND_RED, width=2.5),
        fillcolor='rgba(192,57,43,0.1)',
        name='Avg Grid Intensity'
    ))
    fig2.update_layout(
        title='<b>National Average Grid Carbon Intensity Over Time (gCO₂/kWh)</b>',
        xaxis_title='Period', yaxis_title='gCO₂/kWh', height=380, **PLOTLY_LAYOUT
    )
    display(HTML(fig2.to_html(include_plotlyjs=False, full_html=False)))
else:
    print("Carbon footprint data not yet available.")
""")

_SECTION5_CODE = textwrap.dedent("""
# ──────────────────────────────────────────────────────────────────────────────
# SECTION 5 | Regional Market Performance
# ──────────────────────────────────────────────────────────────────────────────
market = qdf('''
    select reading_date, region,
           round(avg_spot_price_usd_mwh,2) as spot_price,
           round(avg_peak_price_usd_mwh,2) as peak_price,
           round(avg_demand_mw/1000,1) as demand_gw,
           round(price_volatility_pct,1) as volatility_pct
    from staging.silver_market_aggregated
    order by reading_date desc, region
    limit 1000
''')

if not market.empty:
    region_colors = {
        'ERCOT':'#E74C3C','CAISO':'#F39C12','PJM':'#2E6DA4','MISO':'#27AE60',
        'NYISO':'#8E44AD','NEISO':'#1ABC9C','SPP':'#D35400','WECC':'#2980B9','SERC':'#16A085'
    }

    fig = go.Figure()
    for region in market['region'].unique():
        rd = market[market['region'] == region].sort_values('reading_date')
        fig.add_trace(go.Scatter(
            x=rd['reading_date'], y=rd['spot_price'],
            mode='lines', name=region,
            line=dict(color=region_colors.get(region,'#777'), width=2),
        ))

    fig.update_layout(
        title='<b>Wholesale Spot Electricity Price by Grid Region ($/MWh)</b>',
        xaxis_title='Date', yaxis_title='Spot Price ($/MWh)',
        height=430, **PLOTLY_LAYOUT
    )
    display(HTML(fig.to_html(include_plotlyjs='cdn', full_html=False)))

    # Latest price snapshot table
    latest_market = market[market['reading_date'] == market['reading_date'].max()]
    display(HTML(f"<h3 style='color:{BRAND_BLUE};font-family:Calibri'>Latest Market Snapshot</h3>"))
    display(latest_market[['region','spot_price','peak_price','demand_gw','volatility_pct']]
            .rename(columns={
                'spot_price':'Spot Price ($/MWh)',
                'peak_price':'Peak Price ($/MWh)',
                'demand_gw':'Current Demand (GW)',
                'volatility_pct':'Price Volatility (%)'
            }).reset_index(drop=True))
else:
    print("Market data not yet available.")
""")

_SECTION6_CODE = textwrap.dedent("""
# ──────────────────────────────────────────────────────────────────────────────
# SECTION 6 | Pipeline Data Quality Summary
# ──────────────────────────────────────────────────────────────────────────────
from IPython.display import display, HTML

audit = qdf('''
    select
        asset_name,
        status,
        sum(records_written)    as total_records,
        count(*)                as run_count,
        max(completed_at)       as last_run
    from raw.ingestion_audit_log
    group by asset_name, status
    order by asset_name, status
''')

if not audit.empty:
    display(HTML("<h3 style='color:#1B3A5C;font-family:Calibri'>Ingestion Audit Log Summary</h3>"))
    display(audit.rename(columns={
        'asset_name':'Asset','status':'Status',
        'total_records':'Total Records','run_count':'Runs','last_run':'Last Run'
    }).reset_index(drop=True))
else:
    print("Audit log is empty — pipeline may not have run yet.")
""")


class JupyterReportGenerator:
    """Builds and executes a Jupyter notebook then renders it to HTML."""

    def generate(self, output_path: str) -> None:
        logger.info("Building Jupyter notebook...")
        nb = nbformat.v4.new_notebook()

        sections = [
            ("markdown", "# Dakota Analytics — Energy Market Report\n*Auto-generated executive report*"),
            ("code", _DB_SETUP_CODE),
            ("markdown", "## 1. Executive KPI Summary"),
            ("code", _SECTION1_CODE),
            ("markdown", "## 2. Electricity Generation Mix"),
            ("code", _SECTION2_CODE),
            ("markdown", "## 3. Retail Price Analysis"),
            ("code", _SECTION3_CODE),
            ("markdown", "## 4. Carbon Footprint & Sustainability"),
            ("code", _SECTION4_CODE),
            ("markdown", "## 5. Regional Market Performance"),
            ("code", _SECTION5_CODE),
            ("markdown", "## 6. Pipeline Data Quality"),
            ("code", _SECTION6_CODE),
        ]

        for cell_type, source in sections:
            if cell_type == "markdown":
                nb.cells.append(nbformat.v4.new_markdown_cell(source))
            else:
                nb.cells.append(nbformat.v4.new_code_cell(source))

        # Execute the notebook
        logger.info("Executing notebook (this may take a minute)...")
        ep = ExecutePreprocessor(timeout=300, kernel_name="python3")
        try:
            ep.preprocess(nb, {"metadata": {"path": str(Path(output_path).parent)}})
        except Exception as exc:
            logger.warning("Notebook execution error (continuing with partial output): %s", exc)

        # Export to HTML
        logger.info("Rendering notebook to HTML...")
        html_exporter = HTMLExporter(theme="light")
        html_exporter.exclude_input = True   # clean output — no code visible to CEO
        (html_body, _) = html_exporter.from_notebook_node(nb)

        Path(output_path).write_text(html_body, encoding="utf-8")
        logger.info("Jupyter HTML report saved to %s", output_path)
