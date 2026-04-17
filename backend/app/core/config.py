from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    """Runtime settings for the backend service."""

    app_name: str = "Governed Commercial Banking Analytics Assistant"
    app_version: str = "0.1.0"
    duckdb_path: Path = PROJECT_ROOT / "data" / "commercial_banking.duckdb"
    bedrock_enabled: bool = False
    aws_region: str = "us-east-1"
    aws_profile: Optional[str] = None
    bedrock_model_id: str = "amazon.titan-text-express-v1"
    bedrock_temperature: float = 0.0
    bedrock_max_tokens: int = 1800

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_prefix="BANKING_ASSISTANT_",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
