"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Dakota Enrichment API"
    app_version: str = "1.0.0"
    log_level: str = "INFO"

    # Seed for reproducible synthetic data in tests
    random_seed: int | None = None


settings = Settings()
