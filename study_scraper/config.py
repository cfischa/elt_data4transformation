"""Runtime configuration for the study scraper.

Loads from environment variables (and a `.env` file via pydantic-settings
when available). Designed so unit tests and the CLI run without any
external services configured — only `python -m study_scraper run` against
live sources requires real credentials.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


REPO_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Process-wide settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Topics
    topics_csv_path: Path = Field(
        default=REPO_ROOT / "config" / "topics" / "topics.csv",
        description="CSV of topics edited by the maintainer.",
    )

    # Storage — Supabase primary (per DECISIONS.md A7). When unset, the
    # scraper falls back to a plain Postgres URL for local development.
    supabase_url: Optional[str] = Field(default=None)
    supabase_service_key: Optional[str] = Field(default=None)
    postgres_url: Optional[str] = Field(
        default=None,
        description="Local-dev Postgres URL used when Supabase env vars "
        "are not set. Example: postgresql://postgres:postgres@"
        "localhost:5432/study_scraper",
    )

    # Raw artifact storage. Bucket name in Supabase Storage when
    # supabase_url is set; otherwise a local filesystem directory.
    artifact_bucket: str = Field(default="study-scraper-artifacts")
    artifact_local_dir: Path = Field(
        default=REPO_ROOT / "data" / "study_scraper" / "raw",
    )

    # HTTP
    http_user_agent: str = Field(
        default="study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)"
    )
    http_timeout_seconds: float = Field(default=30.0)
    http_max_retries: int = Field(default=3)
    respect_robots_txt: bool = Field(default=True)

    @property
    def has_supabase(self) -> bool:
        return bool(self.supabase_url) and bool(self.supabase_service_key)


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Return the cached process-wide settings, creating on first call."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
