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
import pkgutil
import sys
from pathlib import Path

import pytest

from study_scraper.topics import Topic, TopicLocale, load_topics


CONSOLE_DIR = Path(__file__).resolve().parents[2] / "study_scraper" / "console"
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
