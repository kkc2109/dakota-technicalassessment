# =============================================================================
# Dakota Enrichment API — Dockerfile
# Uses uv for fast, reproducible dependency installation.
# =============================================================================
FROM python:3.11-slim AS base

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency manifest first (layer cache optimisation)
COPY pyproject.toml ./

# Install production dependencies only
RUN uv pip install --system --no-cache .

# Copy application source
COPY app/ ./app/

# Non-root user for security
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
