"""Smoke + correctness tests for the Streamlit control dock.

We deliberately don't render the Streamlit pages -- that would need
`streamlit.testing.v1.AppTest` which is heavyweight. Instead we test:

  * the page files compile (catches typos and import errors),
  * the CSV-writer helper round-trips topics correctly,
  * the topic-filter preview logic (which the page reuses verbatim
    via score_text) behaves on a known input.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import os
import pkgutil
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.topics import Topic, TopicLocale, load_topics


CONSOLE_DIR = Path(__file__).resolve().parents[2] / "study_scraper" / "console"
TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
PAGE_FILES = [
    CONSOLE_DIR / "Home.py",
    CONSOLE_DIR / "pages" / "0_Questions.py",
    CONSOLE_DIR / "pages" / "1_Topics.py",
    CONSOLE_DIR / "pages" / "2_Review.py",
    CONSOLE_DIR / "pages" / "3_Lake.py",
    CONSOLE_DIR / "pages" / "4_Attributions.py",
    CONSOLE_DIR / "pages" / "5_Sources.py",
    CONSOLE_DIR / "pages" / "6_Candidate_Sources.py",
]


@pytest.mark.parametrize("page_file", PAGE_FILES, ids=lambda p: p.name)
def test_console_page_compiles(page_file: Path) -> None:
    """`py_compile`-equivalent: each console module parses without error."""
    source = page_file.read_text(encoding="utf-8")
    compile(source, str(page_file), "exec")


def test_topic_csv_writer_round_trips(tmp_path: Path) -> None:
    """`write_csv` must produce a file the topics loader reads back identically."""
    from study_scraper.console._csv import write_csv

    out = tmp_path / "topics.csv"
    topic = Topic(
        id="testtopic",
        locales={
            "de": TopicLocale(
                name="Test DE",
                description="desc",
                include_keywords=["a", "b"],
                exclude_keywords=["x"],
                synonyms=["y", "z"],
            ),
            "en": TopicLocale(
                name="Test EN",
                description="desc en",
                include_keywords=["a", "c"],
                exclude_keywords=[],
                synonyms=["y"],
            ),
        },
    )
    write_csv(out, [topic])

    reloaded = load_topics(out)
    assert len(reloaded) == 1
    t = reloaded[0]
    assert t.id == "testtopic"
    assert set(t.locales) == {"de", "en"}
    assert t.locales["de"].include_keywords == ["a", "b"]
    assert t.locales["de"].exclude_keywords == ["x"]
    assert t.locales["de"].synonyms == ["y", "z"]
    assert t.locales["en"].include_keywords == ["a", "c"]
    assert t.locales["en"].name == "Test EN"


def _source_ids_in_package(package_name: str) -> set[str]:
    """Collect every class-level `source_id` string defined in a package.

    Used to keep `source_kind()` (issue #123) honest: a new
    discovery/*.py or sources/*.py module that forgets to register its
    source_id there would otherwise silently render as kind '?' in the
    dock instead of failing a test.
    """
    package = importlib.import_module(package_name)
    ids: set[str] = set()
    for module_info in pkgutil.iter_modules(package.__path__):
        if module_info.name in {"base", "__init__"}:
            continue
        module = importlib.import_module(f"{package_name}.{module_info.name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue  # skip re-exported/imported classes
            source_id = obj.__dict__.get("source_id")
            if isinstance(source_id, str):
                ids.add(source_id)
    return ids


def test_source_kind_covers_all_sources() -> None:
    """Every discovery/*.py source_id classifies as 'catalog', every
    sources/*.py source_id classifies as 'lake' -- catches the #123
    staleness bug (new sources silently falling through to '?')."""
    from study_scraper.console._sources import source_kind

    catalog_ids = _source_ids_in_package("study_scraper.discovery")
    lake_ids = _source_ids_in_package("study_scraper.sources")

    assert catalog_ids, "expected at least one discovery source_id"
    assert lake_ids, "expected at least one lake source_id"
    assert not (catalog_ids & lake_ids)

    for source_id in catalog_ids:
        assert source_kind(source_id) == "catalog", source_id
    for source_id in lake_ids:
        assert source_kind(source_id) == "lake", source_id


def test_pending_reference_rows_shapes_ids_for_display() -> None:
    from study_scraper.console._candidates import pending_reference_rows

    rows = pending_reference_rows(
        ["https://openalex.org/W123", "https://openalex.org/W456"]
    )
    assert rows == [
        {"openalex_id": "W123", "url": "https://openalex.org/W123"},
        {"openalex_id": "W456", "url": "https://openalex.org/W456"},
    ]


def test_days_since_computes_elapsed_days() -> None:
    from study_scraper.console._sources import days_since

    now = datetime(2026, 9, 21, tzinfo=timezone.utc)
    then = now - timedelta(days=3, hours=12)
    assert days_since(then, now=now) == pytest.approx(3.5)


def test_days_since_returns_none_for_never_run() -> None:
    from study_scraper.console._sources import days_since

    assert days_since(None) is None


def test_pending_reference_rows_handles_empty() -> None:
    from study_scraper.console._candidates import pending_reference_rows

    assert pending_reference_rows([]) == []


def test_topic_csv_writer_handles_empty_keyword_lists(tmp_path: Path) -> None:
    from study_scraper.console._csv import write_csv

    topic = Topic(
        id="t",
        locales={
            "de": TopicLocale(name="n", description="d"),
        },
    )
    out = tmp_path / "topics.csv"
    write_csv(out, [topic])
    reloaded = load_topics(out)
    assert reloaded[0].locales["de"].include_keywords == []
    assert reloaded[0].locales["de"].exclude_keywords == []


# --------------------------------------------------------------------------
# Integration: per_source_run_stats' "clean run" definition (#178)
#
# `pytestmark` is scoped to this *class*, not module-level -- a bare
# module-level marker would skip this file's pure unit tests above whenever
# STUDY_SCRAPER_TEST_DSN is unset (i.e. normal CI), same fix as
# test_lake.py's/test_eurostat.py's `TestLakeIngestIntegration` (#153/#167).
# --------------------------------------------------------------------------


class TestPerSourceRunStatsIntegration:
    pytestmark = pytest.mark.skipif(
        not TEST_DSN,
        reason="STUDY_SCRAPER_TEST_DSN not set; skipping per_source_run_stats integration",
    )

    @pytest.fixture()
    def storage(self):
        from study_scraper.storage import PostgresStorage

        assert TEST_DSN is not None
        store = PostgresStorage(TEST_DSN)
        store.migrate()
        return store

    @pytest.fixture(autouse=True)
    def _clean(self, storage) -> Iterator[None]:
        with storage.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
            conn.commit()
        yield

    def test_aborted_run_is_not_last_ok(self, storage) -> None:
        """A source whose only run aborted (errors=0, finished_at NULL,
        notes starting `aborted:` -- the #48/#106 401 shape) must not show
        that run's timestamp as `last_ok`, matching
        `status.py::build_status`'s `source_days_since_last_success`."""
        from study_scraper.console._sources import per_source_run_stats
        from study_scraper.models import CrawlRun

        run = CrawlRun(
            id=str(uuid.uuid4()),
            source_id="bundestag_dip",
            topic_id="klima",
            started_at=datetime.now(timezone.utc),
            finished_at=None,
            errors=0,
            notes="aborted: 401 Unauthorized",
        )
        storage.record_crawl_run(run)

        stats = per_source_run_stats(storage)
        assert stats["bundestag_dip"]["last_ok"] is None
        assert stats["bundestag_dip"]["last_run"] is not None
        assert stats["bundestag_dip"]["total_errors"] == 0

    def test_clean_run_is_last_ok(self, storage) -> None:
        from study_scraper.console._sources import per_source_run_stats
        from study_scraper.models import CrawlRun

        started = datetime.now(timezone.utc)
        run = CrawlRun(
            id=str(uuid.uuid4()),
            source_id="ssoar",
            topic_id="klima",
            started_at=started,
            finished_at=started,
            errors=0,
        )
        storage.record_crawl_run(run)

        stats = per_source_run_stats(storage)
        assert stats["ssoar"]["last_ok"] == started
        assert stats["ssoar"]["total_errors"] == 0
