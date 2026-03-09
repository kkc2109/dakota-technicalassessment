"""Ingestion layer configuration loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # EIA API
    eia_api_key: str = "your_key_here"
    eia_base_url: str = "https://api.eia.gov/v2"
    eia_request_timeout_s: int = 30
    eia_max_retries: int = 3
    eia_page_size: int = 5000          # EIA max per request

    # Enrichment API
    enrichment_api_url: str = "http://localhost:8000"
    enrichment_request_timeout_s: int = 15
    enrichment_max_retries: int = 3

    # Database
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "energy_analytics"
    postgres_user: str = "dakota_user"
    postgres_password: str = "change_me"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # EIA states to ingest (lower 48 US states + DC)
    eia_states: list[str] = [
        "AL", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
        "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
        "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
        "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
        "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    ]

    # Grid regions for enrichment data
    enrichment_regions: list[str] = [
        "ERCOT", "CAISO", "PJM", "MISO", "NYISO", "NEISO", "SPP", "WECC", "SERC",
    ]


settings = IngestionSettings()
