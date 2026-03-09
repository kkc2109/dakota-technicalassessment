"""Base database loader."""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ingestion.config import settings

logger = logging.getLogger(__name__)


class BaseLoader:
    """Handles database connection lifecycle and audit logging."""

    def __init__(self) -> None:
        self._engine: Engine = create_engine(
            settings.database_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  
        )
        logger.info("Database engine created for %s:%d/%s", settings.postgres_host, settings.postgres_port, settings.postgres_db)

    @contextmanager
    def _connection(self) -> Generator:
        with self._engine.begin() as conn:
            yield conn

    def _start_audit(self, run_id: str, asset_name: str, source: str) -> int:
        """Insert an audit log entry and return its id."""
        with self._connection() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO raw.ingestion_audit_log
                        (run_id, asset_name, source, started_at, status)
                    VALUES (:run_id, :asset_name, :source, :started_at, 'running')
                    RETURNING id
                    """
                ),
                {
                    "run_id": run_id,
                    "asset_name": asset_name,
                    "source": source,
                    "started_at": datetime.now(tz=timezone.utc),
                },
            )
            audit_id: int = result.scalar_one()
        logger.info("Audit log started id=%d run_id=%s asset=%s", audit_id, run_id, asset_name)
        return audit_id

    def _complete_audit(self, audit_id: int, records_written: int) -> None:
        """Mark an audit log entry as successful."""
        with self._connection() as conn:
            conn.execute(
                text(
                    """
                    UPDATE raw.ingestion_audit_log
                    SET status = 'success',
                        records_written = :records,
                        completed_at = :completed_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": audit_id,
                    "records": records_written,
                    "completed_at": datetime.now(tz=timezone.utc),
                },
            )
        logger.info("Audit log completed id=%d records_written=%d", audit_id, records_written)

    def _fail_audit(self, audit_id: int, error_message: str) -> None:
        """Mark an audit log entry as failed."""
        with self._connection() as conn:
            conn.execute(
                text(
                    """
                    UPDATE raw.ingestion_audit_log
                    SET status = 'failed',
                        completed_at = :completed_at,
                        error_message = :error_message
                    WHERE id = :id
                    """
                ),
                {
                    "id": audit_id,
                    "completed_at": datetime.now(tz=timezone.utc),
                    "error_message": error_message[:1000],
                },
            )
        logger.error("Audit log failed id=%d error=%s", audit_id, error_message[:200])

    def dispose(self) -> None:
        """Release all database connections in the pool."""
        self._engine.dispose()
        logger.debug("Database engine disposed")
