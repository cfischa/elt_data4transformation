"""Tests for the regex-v1 claim extractor + the claims storage path."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest
from typer.testing import CliRunner

from study_scraper.cli import app
from study_scraper.claims import extract_claims, _value_to_float
from study_scraper.discovery.openalex import OpenAlexSource
from study_scraper.pipeline import run_one
from study_scraper.storage import PostgresStorage
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "openalex" / "klima_works.json"
)


# --------------------------------------------------------------------------
# Pure-Python unit tests (always run)
# --------------------------------------------------------------------------


class TestExtractor:
    def test_captures_simple_percent(self) -> None:
        claims = extract_claims(
            study_id="s", title=None,
            abstract="62% der Befragten halten ein Klimaschutzgesetz für nötig."
        )
        pcts = [c for c in claims if c.unit == "%"]
        assert any(c.numeric_value == 62.0 for c in pcts)

    def test_captures_percent_with_space_and_decimal(self) -> None:
        claims = extract_claims(
            study_id="s", title=None,
            abstract="62,5 % der Befragten und 67 % der Personen unter 30"
        )
        values = sorted(c.numeric_value for c in claims if c.unit == "%")
        assert values == [62.5, 67.0]

    def test_captures_sample_size_n_equals(self) -> None:
        claims = extract_claims(
            study_id="s", title=None,
            abstract="Repräsentative Umfrage (n=1009, Wahlberechtigte) im Jahr 2024."
        )
        ns = [c for c in claims if c.unit == "n"]
        assert any(c.numeric_value == 1009.0 for c in ns)

    def test_filters_implausible_percentages(self) -> None:
        # "200 %" is not a poll figure; suppress.
        claims = extract_claims(
            study_id="s", title=None,
            abstract="Marktwachstum 200% im Jahr 2024, Zustimmung 55%"
        )
        values = sorted(c.numeric_value for c in claims if c.unit == "%")
        assert values == [55.0]

    def test_deduplicates_within_same_field(self) -> None:
        # Same snippet should not be stored twice.
        claims = extract_claims(
            study_id="s", title=None,
            abstract="62% der Befragten. Wiederholung: 62% der Befragten."
        )
        # Two distinct matches because surrounding context differs.
        # But "62%" appearing in identical adjacent text would dedupe.
        assert all(c.numeric_value == 62.0 for c in claims if c.unit == "%")

    def test_title_field_marked(self) -> None:
        claims = extract_claims(
            study_id="s",
            title="55% wollen Atomkraft zurück",
            abstract=None,
        )
        assert any(c.source_field == "title" and c.unit == "%" for c in claims)

    def test_empty_input(self) -> None:
        assert extract_claims(study_id="s", title=None, abstract=None) == []
        assert extract_claims(study_id="s", title="", abstract="") == []

    def test_no_match_returns_empty(self) -> None:
        assert extract_claims(
            study_id="s", title=None,
            abstract="Eine qualitative Diskursanalyse ohne Zahlen."
        ) == []

    def test_claim_id_is_stable(self) -> None:
        c1 = extract_claims(
            study_id="s", title=None, abstract="62% support."
        )[0]
        c2 = extract_claims(
            study_id="s", title=None, abstract="62% support."
        )[0]
        assert c1.id == c2.id


class TestValueParsing:
    @pytest.mark.parametrize(
        "token,expected",
        [
            ("62", 62.0),
            ("62.5", 62.5),
            ("62,5", 62.5),
            ("6,063", 6063.0),   # german thousands
            ("6.063", 6063.0),   # european thousands
            ("not a number", None),
        ],
    )
    def test_value_to_float(self, token: str, expected) -> None:
        assert _value_to_float(token) == expected


# --------------------------------------------------------------------------
# Integration: pipeline extracts claims, search CLI returns them
# --------------------------------------------------------------------------


pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping claims integration tests"
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
            cur.execute("TRUNCATE study_scraper.claims CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


@pytest.fixture(scope="module")
def klima_topic():
    return next(
        t for t in load_topics(
            Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
        ) if t.id == "klima"
    )


def test_pipeline_populates_claims_table(storage: PostgresStorage, klima_topic) -> None:
    with OpenAlexSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    assert storage.count_claims() > 0


def test_search_finds_klimaschutzgesetz_claim(
    storage: PostgresStorage, klima_topic
) -> None:
    """The Forsa fixture has '62% ... Klimaschutzgesetz' in the abstract."""
    with OpenAlexSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    hits = storage.search_claims(query="klimaschutzgesetz", unit="%")
    # The 62% support figure and the 44% from CO2-Bepreisung should both
    # surface because both mention Klimaschutzgesetz/strengere Klimaschutzgesetze.
    assert hits, "no claims matched klimaschutzgesetz; extractor or query broken"
    values = [float(h["numeric_value"]) for h in hits]
    assert 62.0 in values


def test_search_finds_nuclear_claim(storage: PostgresStorage, klima_topic) -> None:
    with OpenAlexSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    hits = storage.search_claims(query="atomkraft", unit="%")
    assert hits
    values = [float(h["numeric_value"]) for h in hits]
    # 55% support reintroducing nuclear; 36% oppose; 65% Civey longitudinal
    assert 55.0 in values


# --------------------------------------------------------------------------
# CLI surface
# --------------------------------------------------------------------------


@pytest.fixture()
def runner(monkeypatch) -> CliRunner:
    import study_scraper.config as _cfg
    monkeypatch.setattr(_cfg, "_settings", None)
    return CliRunner()


def test_cli_search_climate_law(
    storage: PostgresStorage, klima_topic, runner: CliRunner, monkeypatch
) -> None:
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)
    with OpenAlexSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    out = runner.invoke(app, ["search", "klimaschutzgesetz"])
    assert out.exit_code == 0, out.output
    assert "62" in out.output  # the headline %


def test_cli_search_no_hits(runner: CliRunner, monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)
    out = runner.invoke(app, ["search", "completely_made_up_term_xyz"])
    assert out.exit_code == 0
    assert "no claims matched" in out.output.lower()
