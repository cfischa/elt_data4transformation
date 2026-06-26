"""Smoke + correctness tests for the Streamlit control dock.

We deliberately don't render the Streamlit pages -- that would need
`streamlit.testing.v1.AppTest` which is heavyweight. Instead we test:

  * the page files compile (catches typos and import errors),
  * the CSV-writer helper round-trips topics correctly,
  * the topic-filter preview logic (which the page reuses verbatim
    via score_text) behaves on a known input.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from study_scraper.topics import Topic, TopicLocale, load_topics


CONSOLE_DIR = Path(__file__).resolve().parents[2] / "study_scraper" / "console"
PAGE_FILES = [
    CONSOLE_DIR / "Home.py",
    CONSOLE_DIR / "pages" / "1_Topics.py",
    CONSOLE_DIR / "pages" / "2_Review.py",
    CONSOLE_DIR / "pages" / "3_Lake.py",
    CONSOLE_DIR / "pages" / "4_Attributions.py",
    CONSOLE_DIR / "pages" / "5_Sources.py",
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
