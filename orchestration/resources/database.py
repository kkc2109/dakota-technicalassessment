"""Dagster database resource."""

import logging
from contextlib import contextmanager
from typing import Generator

from dagster import ConfigurableResource
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class PostgresResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 5432
    database: str = "energy_analytics"
    username: str = "dakota_user"
    password: str = "change_me"

    @property
    def url(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self) -> Engine:
        return create_engine(self.url, pool_pre_ping=True)

    @contextmanager
    def get_connection(self) -> Generator:
        engine = self.get_engine()
        try:
            with engine.begin() as conn:
                yield conn
        finally:
            engine.dispose()

    def health_check(self) -> bool:
        """Returns True if the database is reachable."""
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            logger.error("Database health check failed: %s", exc)
            return False
