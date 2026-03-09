"""Shared pytest fixtures and test configuration."""

import logging
import os

import pytest

# Configure logging so test output is clearly readable
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def log_test_name(request):
    """Log each test name before and after execution for traceability."""
    logger.info("▶ START  %s", request.node.nodeid)
    yield
    logger.info("✓ FINISH %s", request.node.nodeid)

@pytest.fixture
def api_base_url() -> str:
    return os.getenv("ENRICHMENT_API_URL", "http://localhost:8000")

@pytest.fixture
def eia_api_key() -> str:
    return os.getenv("EIA_API_KEY", "test_key")

@pytest.fixture
def db_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "energy_analytics")
    user = os.getenv("POSTGRES_USER", "dakota_user")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

@pytest.fixture
def db_engine(db_url):
    """SQLAlchemy engine connected to the test database."""
    from sqlalchemy import create_engine
    engine = create_engine(db_url, pool_pre_ping=True)
    logger.info("Test DB engine created: %s", db_url.split("@")[-1])
    yield engine
    engine.dispose()
    logger.info("Test DB engine disposed")
