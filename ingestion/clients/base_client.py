"""Base HTTP client with retry, backoff, and structured logging."""

import logging
from typing import Any

import httpx
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Retry on transient network errors and 5xx server errors
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class TransientApiError(Exception):
    """Raised for retryable API errors (5xx, rate limits)."""


class PermanentApiError(Exception):
    """Raised for non-retryable API errors (4xx excluding 429)."""


class BaseHttpClient:
    """Async HTTP client with exponential backoff retry and structured logging."""

    def __init__(
        self,
        base_url: str,
        timeout_s: int = 30,
        max_retries: int = 3,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = httpx.Timeout(timeout_s)
        self._max_retries = max_retries
        self._headers = headers or {}
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "BaseHttpClient":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._headers,
            follow_redirects=True,
        )
        logger.debug("HTTP client opened for base_url=%s", self._base_url)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            logger.debug("HTTP client closed for base_url=%s", self._base_url)

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GET request with retry logic and structured logging."""
        if self._client is None:
            raise RuntimeError("Client must be used as an async context manager.")

        attempt = 0

        @retry(
            retry=retry_if_exception_type(TransientApiError),
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        async def _execute() -> dict[str, Any]:
            nonlocal attempt
            attempt += 1
            url = f"{self._base_url}/{path.lstrip('/')}"
            logger.info("GET %s attempt=%d params=%s", url, attempt, params)

            try:
                response = await self._client.get(path, params=params)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as exc:
                logger.warning("Network error on GET %s: %s", path, exc)
                raise TransientApiError(str(exc)) from exc

            if response.status_code in _RETRYABLE_STATUS_CODES:
                logger.warning(
                    "Retryable HTTP %d on GET %s body=%s",
                    response.status_code, path, response.text[:200],
                )
                raise TransientApiError(f"HTTP {response.status_code}")

            if response.status_code >= 400:
                logger.error(
                    "Permanent HTTP %d on GET %s body=%s",
                    response.status_code, path, response.text[:500],
                )
                raise PermanentApiError(f"HTTP {response.status_code}: {response.text[:200]}")

            logger.info(
                "GET %s -> HTTP %d (%.0f ms)",
                path, response.status_code,
                response.elapsed.total_seconds() * 1000,
            )
            return response.json()

        try:
            return await _execute()
        except RetryError as exc:
            logger.error("All %d retries exhausted for GET %s", self._max_retries, path)
            raise TransientApiError(f"Max retries exceeded for {path}") from exc
