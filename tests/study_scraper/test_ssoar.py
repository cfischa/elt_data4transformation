"""Tests for the SSOAR OAI-PMH discovery source.

Both the from-file fixture path and the live HTTP path go through the
same parser, so testing the fixture path validates production parsing.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from study_scraper.discovery.ssoar import SSOARSource, _normalize_language, _parse_date
from study_scraper.topics import load_topics


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "ssoar" / "klima_records.xml"


@pytest.fixture(scope="module")
def klima_topic():
    topics = load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )
    return next(t for t in topics if t.id == "klima")


class TestSSOARFromFile:
    def test_parses_all_records(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        assert len(cands) == 7  # 6 klima + 1 negative control

    def test_canonical_url_is_https_identifier(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        for cand in cands:
            assert cand.canonical_url.startswith("https://www.ssoar.info/")

    def test_extracts_titles_and_authors(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        kommunal = next(c for c in cands if "Kommunaler Klimaschutz" in c.title)
        assert "Kern, Kristine" in kommunal.authors
        assert "Niederhafner, Stefan" in kommunal.authors
        assert kommunal.publisher == (
            "Wissenschaftszentrum Berlin für Sozialforschung gGmbH"
        )

    def test_extracts_dates_in_year_only_format(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        kommunal = next(c for c in cands if "Kommunaler Klimaschutz" in c.title)
        assert kommunal.publication_date == date(2005, 1, 1)

    def test_normalizes_language_deu_to_de(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        for cand in cands:
            assert cand.language == "de"

    def test_extracts_subject_into_raw(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        kommunal = next(c for c in cands if "Kommunaler Klimaschutz" in c.title)
        assert "Klimaschutz" in kommunal.raw["subjects"]

    def test_external_id_uses_oai_identifier(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic))
        kommunal = next(c for c in cands if "Kommunaler Klimaschutz" in c.title)
        assert kommunal.external_id == "oai:gesis.izsoz.de:document/19672"

    def test_limit_truncates(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima_topic, limit=2))
        assert len(cands) == 2

    def test_discovery_query_is_topic_id(self, klima_topic) -> None:
        with SSOARSource(from_file=FIXTURE) as src:
            cand = next(src.iter_candidates(klima_topic))
        assert cand.discovery_query == "klima"


class TestHelpers:
    def test_parse_date_handles_year_only(self) -> None:
        assert _parse_date(["2014"]) == date(2014, 1, 1)
        assert _parse_date(["2021-09"]) == date(2021, 9, 1)
        assert _parse_date(["2005-12-01"]) == date(2005, 12, 1)

    def test_parse_date_returns_none_for_garbage(self) -> None:
        assert _parse_date([]) is None
        assert _parse_date(["not a date"]) is None

    def test_normalize_language(self) -> None:
        assert _normalize_language(["deu"]) == "de"
        assert _normalize_language(["ger"]) == "de"
        assert _normalize_language(["eng"]) == "en"
        assert _normalize_language(["de"]) == "de"
        assert _normalize_language(["xx"]) is None
        assert _normalize_language([]) is None
