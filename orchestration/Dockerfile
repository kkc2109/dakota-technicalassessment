
FROM python:3.11-slim AS base

WORKDIR /app

# Install system dependencies required by psycopg2 and dbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

COPY orchestration/pyproject.toml ./orchestration/pyproject.toml
RUN uv pip install --system --no-cache -r orchestration/pyproject.toml 2>/dev/null || \
    uv pip install --system --no-cache \
        dagster>=1.7.0 \
        dagster-postgres>=0.23.0 \
        dagster-dbt>=0.23.0 \
        dagster-webserver>=1.7.0 \
        dbt-core>=1.8.0 \
        dbt-postgres>=1.8.0 \
        httpx>=0.27.0 \
        tenacity>=8.3.0 \
        sqlalchemy>=2.0.0 \
        psycopg2-binary>=2.9.0 \
        pydantic>=2.7.0 \
        pydantic-settings>=2.3.0 \
        python-dotenv>=1.0.0 \
        pandas>=2.2.0 \
        openpyxl>=3.1.0 \
        plotly>=5.22.0 \
        kaleido>=0.2.1 \
        nbformat>=5.10.0 \
        nbconvert>=7.16.0 \
        ipykernel>=6.29.0 \
        fastapi>=0.111.0 \
        respx>=0.21.0 \
        pytest>=8.0.0 \
        pytest-asyncio>=0.23.0 \
        httpx>=0.27.0


ENV PYTHONPATH=/app

COPY ingestion/ ./ingestion/
COPY orchestration/ ./orchestration/
COPY reports/ ./reports/
COPY dbt/ ./dbt/
COPY tests/ ./tests/
COPY api/ ./api/


ENV DBT_PROFILES_DIR=/app/dbt/energy_analytics
RUN cd /app/dbt/energy_analytics && dbt deps && dbt parse --profiles-dir .


ENV DAGSTER_HOME=/app/orchestration

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:3000/health')"
