"""Synchronous Postgres storage adapter.

Why sync, not async: the scraper's hot path is HTTP fetching (which is
async). DB writes happen at the end of each candidate's processing.
Sync `psycopg` keeps the storage layer trivial; the discovery/extraction
layers can still use asyncio and run DB calls via `to_thread` if they
care.

The adapter targets a plain Postgres URL so the same code path works
against (a) a hosted Supabase project, (b) `supabase start` locally,
(c) a local Postgres container. SQLite is intentionally not supported
(see DECISIONS.md A7).
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence
from urllib.parse import urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - dependency gate
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]

from study_scraper.models import CrawlRun, Study


LOGGER = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
SCHEMA = "study_scraper"


class StorageError(RuntimeError):
    """Raised for unrecoverable storage problems."""


def resolve_database_url(
    *,
    postgres_url: Optional[str],
    supabase_url: Optional[str],
    supabase_service_key: Optional[str],
) -> str:
    """Return a Postgres URL for the current configuration.

    Precedence:
        1. explicit `postgres_url` (development, docker-compose).
        2. derived from `supabase_url` + `supabase_service_key`
           (Supabase Postgres connection string).

    Raises StorageError if neither is configured.
    """
    if postgres_url:
        return postgres_url

    if supabase_url and supabase_service_key:
        # Supabase project URL looks like https://<ref>.supabase.co
        # Postgres host is db.<ref>.supabase.co on port 5432, user
        # `postgres`, database `postgres`, password = service key
        # (or anon key for limited access). The service key is the
        # intended choice for backend ingestion.
        parsed = urlparse(supabase_url)
        if not parsed.hostname or not parsed.hostname.endswith(".supabase.co"):
            raise StorageError(
                f"SUPABASE_URL {supabase_url!r} is not a supabase.co host"
            )
        ref = parsed.hostname.split(".")[0]
        return (
            f"postgresql://postgres:{supabase_service_key}"
            f"@db.{ref}.supabase.co:5432/postgres?sslmode=require"
        )

    raise StorageError(
        "No database configured. Set POSTGRES_URL for local development "
        "(see study_scraper/docker-compose.yml) or SUPABASE_URL + "
        "SUPABASE_SERVICE_KEY for a hosted Supabase project. "
        "See docs/study_scraper/DECISIONS.md A7."
    )


def _migration_files() -> List[Path]:
    return sorted(MIGRATIONS_DIR.glob("[0-9]*.sql"))


def _migration_version(path: Path) -> int:
    return int(path.name.split("_", 1)[0])


class PostgresStorage:
    """Thin synchronous wrapper around psycopg for the scraper's needs."""

    def __init__(self, database_url: str) -> None:
        if psycopg is None:
            raise StorageError(
                "psycopg is not installed; add it to your environment "
                "(`pip install 'psycopg[binary]'`) to use PostgresStorage."
            )
        self._database_url = database_url

    @contextmanager
    def connection(self) -> Iterator["psycopg.Connection[Any]"]:
        assert psycopg is not None  # for type checkers
        with psycopg.connect(self._database_url, row_factory=dict_row) as conn:
            yield conn

    # ------------------------------------------------------------------
    # Migrations
    # ------------------------------------------------------------------

    def migrate(self) -> List[int]:
        """Apply pending migrations. Returns the list of applied versions."""
        applied: List[int] = []
        files = _migration_files()
        if not files:
            return applied

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_versions (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        description TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    f"SELECT version FROM {SCHEMA}.schema_versions"
                )
                already: set[int] = {row["version"] for row in cur.fetchall()}

            for path in files:
                version = _migration_version(path)
                if version in already:
                    continue
                sql = path.read_text(encoding="utf-8")
                LOGGER.info("applying migration %s", path.name)
                with conn.cursor() as cur:
                    cur.execute(sql)
                applied.append(version)
            conn.commit()
        return applied

    # ------------------------------------------------------------------
    # Studies
    # ------------------------------------------------------------------

    def upsert_study(self, study: Study) -> bool:
        """Insert or update a study row.

        Returns True if the row was newly inserted, False if it already
        existed (the caller can use this to populate `is_new` on the
        crawl_run_studies junction).
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {SCHEMA}.studies WHERE id = %s",
                    (study.id,),
                )
                existed = cur.fetchone() is not None

                payload = self._study_to_row(study)
                placeholders = ", ".join(["%s"] * len(payload))
                columns = ", ".join(payload.keys())
                updates = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in payload
                    if col not in {"id", "created_at"}
                )

                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.studies ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {updates},
                        updated_at = now()
                    """,
                    list(payload.values()),
                )
            conn.commit()
        return not existed

    def get_study(self, study_id: str) -> Optional[Dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {SCHEMA}.studies WHERE id = %s",
                    (study_id,),
                )
                return cur.fetchone()

    def list_studies(
        self,
        *,
        topic_id: Optional[str] = None,
        source_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if topic_id is not None:
            clauses.append("%s = ANY(topic_ids)")
            params.append(topic_id)
        if source_id is not None:
            clauses.append("source_id = %s")
            params.append(source_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT * FROM {SCHEMA}.studies
                    {where}
                    ORDER BY fetched_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    # ------------------------------------------------------------------
    # Crawl runs
    # ------------------------------------------------------------------

    def record_crawl_run(self, run: CrawlRun) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.crawl_runs (
                        id, source_id, topic_id, started_at, finished_at,
                        candidates_seen, candidates_kept, errors,
                        parameters, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        finished_at = EXCLUDED.finished_at,
                        candidates_seen = EXCLUDED.candidates_seen,
                        candidates_kept = EXCLUDED.candidates_kept,
                        errors = EXCLUDED.errors,
                        parameters = EXCLUDED.parameters,
                        notes = EXCLUDED.notes
                    """,
                    (
                        run.id,
                        run.source_id,
                        run.topic_id,
                        run.started_at,
                        run.finished_at,
                        run.candidates_seen,
                        run.candidates_kept,
                        run.errors,
                        json.dumps(run.parameters),
                        run.notes,
                    ),
                )
            conn.commit()

    def attach_studies_to_run(
        self,
        run_id: str,
        entries: Iterable[tuple[str, bool]],
    ) -> None:
        """Attach studies (study_id, is_new) to a crawl run."""
        rows: Sequence[tuple[str, str, bool]] = [
            (run_id, study_id, is_new) for study_id, is_new in entries
        ]
        if not rows:
            return
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"""
                    INSERT INTO {SCHEMA}.crawl_run_studies (
                        run_id, study_id, is_new
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, study_id) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _study_to_row(study: Study) -> Dict[str, Any]:
        survey_metadata = (
            json.dumps(study.survey_metadata.model_dump(mode="json"))
            if study.survey_metadata is not None
            else None
        )
        return {
            "id": study.id,
            "canonical_url": study.canonical_url,
            "source_urls": study.source_urls,
            "title": study.title,
            "authors": study.authors,
            "publisher": study.publisher,
            "publication_date": study.publication_date,
            "language": study.language,
            "topic_ids": study.topic_ids,
            "topic_scores": json.dumps(study.topic_scores),
            "has_quantitative_data": study.has_quantitative_data,
            "abstract": study.abstract,
            "key_findings": study.key_findings,
            "survey_metadata": survey_metadata,
            "raw_artifact_ref": study.raw_artifact_ref,
            "fetched_at": study.fetched_at,
            "source_id": study.source_id,
            "provenance": json.dumps(study.provenance.model_dump(mode="json")),
        }
