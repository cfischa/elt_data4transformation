"""Integration tests for the Postgres storage adapter.

These tests need a real Postgres reachable via `STUDY_SCRAPER_TEST_DSN`.
When the env var is absent, the tests are skipped (CI without docker
still passes the rest of the suite).

Locally:

    POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper \\
    STUDY_SCRAPER_TEST_DSN=$POSTGRES_URL \\
    pytest tests/study_scraper/test_storage.py
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from typing import Iterator

import pytest

from study_scraper.models import CrawlRun, Provenance, Study, SurveyMetadata
from study_scraper.storage import PostgresStorage, StorageError, resolve_database_url


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping storage tests"
)


@pytest.fixture(scope="module")
def storage() -> PostgresStorage:
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytest.fixture(autouse=True)
def _clean_tables(storage: PostgresStorage) -> Iterator[None]:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


def _study(url: str = "https://example.org/study/1", **overrides: object) -> Study:
    base = dict(
        canonical_url=url,
        title="An example study",
        fetched_at=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
        source_id="ssoar",
        provenance=Provenance(discovery_source="ssoar", discovery_query="klima"),
        topic_ids=["klima"],
        topic_scores={"klima": 0.9},
        has_quantitative_data=True,
        authors=["Mustermann, M."],
        publisher="GESIS",
        publication_date=date(2025, 1, 1),
        language="de",
        abstract="Eine Untersuchung zu Klimaschutzmaßnahmen.",
        key_findings=["X stieg um 5%"],
        survey_metadata=SurveyMetadata(sample_size=1500, methodology="CATI"),
    )
    base.update(overrides)
    return Study.build(**base)  # type: ignore[arg-type]


class TestMigrations:
    def test_migrate_is_idempotent(self, storage: PostgresStorage) -> None:
        # The fixture already migrated; a second call must be a no-op.
        applied = storage.migrate()
        assert applied == []

    def test_schema_version_row_exists(self, storage: PostgresStorage) -> None:
        with storage.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT version FROM study_scraper.schema_versions ORDER BY version"
                )
                versions = [row["version"] for row in cur.fetchall()]
        assert 1 in versions


class TestUpsertStudy:
    def test_first_insert_returns_true(self, storage: PostgresStorage) -> None:
        is_new = storage.upsert_study(_study())
        assert is_new is True

    def test_second_upsert_returns_false_and_updates(
        self, storage: PostgresStorage
    ) -> None:
        first = _study(title="v1")
        storage.upsert_study(first)
        second = _study(title="v2 — corrected")
        is_new = storage.upsert_study(second)
        assert is_new is False
        fetched = storage.get_study(first.id)
        assert fetched is not None
        assert fetched["title"] == "v2 — corrected"

    def test_array_and_jsonb_round_trip(self, storage: PostgresStorage) -> None:
        study = _study()
        storage.upsert_study(study)
        row = storage.get_study(study.id)
        assert row is not None
        assert row["topic_ids"] == ["klima"]
        assert row["topic_scores"] == {"klima": 0.9}
        assert row["authors"] == ["Mustermann, M."]
        assert row["survey_metadata"]["sample_size"] == 1500
        assert row["provenance"]["discovery_source"] == "ssoar"


class TestListStudies:
    def test_filter_by_topic(self, storage: PostgresStorage) -> None:
        storage.upsert_study(
            _study(url="https://example.org/a", topic_ids=["klima"])
        )
        storage.upsert_study(
            _study(url="https://example.org/b", topic_ids=["steuern"])
        )
        klima = storage.list_studies(topic_id="klima")
        assert len(klima) == 1
        assert klima[0]["canonical_url"] == "https://example.org/a"

    def test_filter_by_source(self, storage: PostgresStorage) -> None:
        storage.upsert_study(
            _study(url="https://example.org/a", source_id="ssoar")
        )
        storage.upsert_study(
            _study(url="https://example.org/b", source_id="openalex")
        )
        rows = storage.list_studies(source_id="openalex")
        assert len(rows) == 1
        assert rows[0]["canonical_url"] == "https://example.org/b"


class TestCrawlRuns:
    def test_record_and_attach(self, storage: PostgresStorage) -> None:
        study = _study()
        storage.upsert_study(study)

        run = CrawlRun(
            id=str(uuid.uuid4()),
            source_id="ssoar",
            topic_id="klima",
            started_at=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 5, 5, 12, 1, tzinfo=timezone.utc),
            candidates_seen=10,
            candidates_kept=1,
            errors=0,
            parameters={"limit": 50},
            notes="phase-4 first run",
        )
        storage.record_crawl_run(run)
        storage.attach_studies_to_run(run.id, [(study.id, True)])

        with storage.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT r.candidates_kept, j.is_new, s.canonical_url
                    FROM study_scraper.crawl_runs r
                    JOIN study_scraper.crawl_run_studies j ON j.run_id = r.id
                    JOIN study_scraper.studies s ON s.id = j.study_id
                    WHERE r.id = %s
                    """,
                    (run.id,),
                )
                row = cur.fetchone()
        assert row is not None
        assert row["candidates_kept"] == 1
        assert row["is_new"] is True
        assert row["canonical_url"] == study.canonical_url


class TestResolveDatabaseUrl:
    def test_postgres_url_takes_precedence(self) -> None:
        url = resolve_database_url(
            postgres_url="postgresql://x/y",
            supabase_url="https://abc.supabase.co",
            supabase_service_key="key",
        )
        assert url == "postgresql://x/y"

    def test_supabase_url_is_derived(self) -> None:
        url = resolve_database_url(
            postgres_url=None,
            supabase_url="https://abcd.supabase.co",
            supabase_service_key="svc-key",
        )
        assert url == (
            "postgresql://postgres:svc-key@db.abcd.supabase.co:5432/"
            "postgres?sslmode=require"
        )

    def test_no_config_raises(self) -> None:
        with pytest.raises(StorageError, match="No database configured"):
            resolve_database_url(
                postgres_url=None, supabase_url=None, supabase_service_key=None
            )

    def test_non_supabase_host_raises(self) -> None:
        with pytest.raises(StorageError, match="not a supabase.co host"):
            resolve_database_url(
                postgres_url=None,
                supabase_url="https://example.com",
                supabase_service_key="key",
            )
