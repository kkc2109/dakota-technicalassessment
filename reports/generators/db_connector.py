"""Shared database connector for report generators."""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "energy_analytics")
    user = os.getenv("POSTGRES_USER", "dakota_user")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
        logger.info("Query returned %d rows", len(df))
        return df
    finally:
        engine.dispose()
