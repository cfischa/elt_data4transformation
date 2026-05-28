"""Tests for the OpenAlex discovery source.

The from_file fixture and the live HTTP path share the same parser, so
testing the fixture path validates production parsing.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from study_scraper.discovery.openalex import (
    OpenAlexSource,
    _build_search_query,
    _reconstruct_abstract,
)
from study_scraper.topics import load_topics


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "openalex" / "klima_works.json"


@pytest.fixture(scope="module")
def topics():
    return load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )


@pytest.fixture(scope="module")
def klima(topics):
    return next(t for t in topics if t.id == "klima")


class TestOpenAlexFromFile:
    def test_parses_all_results(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        # 4 academic climate + 1 cross-source dedup test +
        # 2 polling-release records (Forsa climate-law, nuclear poll) +
        # 1 off-topic negative control = 8.
        assert len(cands) == 8

    def test_canonical_url_prefers_doi_over_openalex_id(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        co2 = next(c for c in cands if "CO₂-Bepreisung" in c.title)
        assert co2.canonical_url == "https://doi.org/10.1515/pwp-2023-0031"
        # external_id keeps the OpenAlex work id for cross-reference
        assert co2.external_id == "https://openalex.org/W4391234567"

    def test_canonical_url_falls_back_to_openalex_id_when_no_doi(
        self, klima
    ) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        # The dedup-test record has empty DOI; canonical_url must fall
        # back to the OpenAlex id (not the empty string).
        dedup = next(c for c in cands if "Erdgas" in c.title)
        assert dedup.canonical_url == "https://openalex.org/W3145167890"

    def test_reconstructs_abstract_from_inverted_index(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        co2 = next(c for c in cands if "CO₂-Bepreisung" in c.title)
        assert co2.abstract is not None
        assert "repräsentative Befragung" in co2.abstract
        assert "6063" in co2.abstract

    def test_extracts_authors(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        co2 = next(c for c in cands if "CO₂-Bepreisung" in c.title)
        assert "Sommer, Stephan" in co2.authors
        assert "Mattauch, Linus" in co2.authors

    def test_extracts_publisher_from_primary_location(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        co2 = next(c for c in cands if "CO₂-Bepreisung" in c.title)
        assert co2.publisher == "Perspektiven der Wirtschaftspolitik"

    def test_extracts_publication_date(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        co2 = next(c for c in cands if "CO₂-Bepreisung" in c.title)
        assert co2.publication_date == date(2023, 12, 1)

    def test_language_passed_through(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        de_count = sum(1 for c in cands if c.language == "de")
        en_count = sum(1 for c in cands if c.language == "en")
        assert de_count >= 3
        assert en_count >= 1

    def test_raw_includes_doi_topics_keywords(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        co2 = next(c for c in cands if "CO₂-Bepreisung" in c.title)
        assert co2.raw["doi"] == "https://doi.org/10.1515/pwp-2023-0031"
        assert any("Climate policy" in t["display_name"] for t in co2.raw["topics"])
        assert "CO2 pricing" in co2.raw["keywords"]

    def test_limit_truncates(self, klima) -> None:
        with OpenAlexSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima, limit=2))
        assert len(cands) == 2


class TestHelpers:
    def test_build_search_query_uses_topic_keywords(self, klima) -> None:
        q = _build_search_query(klima)
        assert q  # not empty
        # Should contain at least one of the include keywords
        assert any(
            kw.lower() in q.lower()
            for kw in ["Klimaschutz", "Energiewende", "Klimawandel", "CO2"]
        )

    def test_reconstruct_abstract_handles_none(self) -> None:
        assert _reconstruct_abstract(None) is None
        assert _reconstruct_abstract({}) is None

    def test_reconstruct_abstract_orders_by_position(self) -> None:
        idx = {"world": [1], "hello": [0]}
        assert _reconstruct_abstract(idx) == "hello world"

    def test_reconstruct_abstract_handles_multiple_positions(self) -> None:
        idx = {"the": [0, 2], "cat": [1, 3]}
        assert _reconstruct_abstract(idx) == "the cat the cat"
