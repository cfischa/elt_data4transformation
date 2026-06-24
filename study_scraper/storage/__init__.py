"""Storage adapter for the study scraper.

Per DECISIONS.md A7, the default target is Supabase (Postgres). The
adapter targets a plain Postgres URL so it works against:

- a hosted Supabase project (`SUPABASE_URL` + `SUPABASE_SERVICE_KEY`
  resolved into a Postgres URL),
- a `supabase start` local instance,
- a local Postgres container (the recommended v1 setup; see
  `study_scraper/docker-compose.yml`).

SQLite is intentionally **not** supported (A7).
"""

from study_scraper.storage.postgres import (
    PostgresStorage,
    StorageError,
    resolve_database_url,
)

__all__ = [
    "PostgresStorage",
    "StorageError",
    "resolve_database_url",
]
