"""Tests for the Bundestag DIP discovery source.

The from_file fixture and the live HTTP path share the same parser, so
testing the fixture path validates production parsing.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from study_scraper.discovery.bundestag_dip import (
    PUBLIC_API_KEY,
    BundestagDIPSource,
    _search_terms,
    resolve_api_key,
)
from study_scraper.topics import load_topics

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "dip_drucksache.json"


@pytest.fixture(scope="module")
def klima():
    topics = load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )
    return next(t for t in topics if t.id == "klima")


class TestFromFile:
    def test_parses_all_documents(self, klima) -> None:
        with BundestagDIPSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        assert len(cands) == 3
        assert all(c.source_id == "bundestag_dip" for c in cands)

    def test_candidate_fields(self, klima) -> None:
        with BundestagDIPSource(from_file=FIXTURE) as src:
            cand = next(src.iter_candidates(klima))
        assert cand.external_id == "300754"
        assert cand.title.startswith("Klimaschutzgesetz konsequent umsetzen")
        assert cand.canonical_url == (
            "https://dserver.bundestag.de/btd/21/045/2104521.pdf"
        )
        assert cand.publisher == "Fraktion BÜNDNIS 90/DIE GRÜNEN"
        assert cand.publication_date == date(2025, 11, 12)
        assert cand.language == "de"
        assert cand.raw["drucksachetyp"] == "Antrag"
        assert cand.raw["dokumentnummer"] == "21/4521"
        assert cand.raw["vorgangsbezug"][0]["vorgangstyp"] == "Antrag"

    def test_missing_pdf_falls_back_to_dip_page(self, klima) -> None:
        with BundestagDIPSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        gesetzentwurf = next(c for c in cands if c.external_id == "300901")
        assert gesetzentwurf.canonical_url == (
            "https://dip.bundestag.de/drucksache/300901"
        )

    def test_limit_respected(self, klima) -> None:
        with BundestagDIPSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima, limit=1))
        assert len(cands) == 1


class TestSearchTerms:
    def test_german_terms_first_and_capped(self, klima) -> None:
        terms = _search_terms(klima)
        assert 0 < len(terms) <= 8
        # klima topic's German vocabulary should lead.
        assert any("klima" in t.lower() for t in terms)
        # Cap is 8 so specific include_keywords survive the umbrella
        # synonyms (at 4 they were dead config; audit 2026-07-11).
        from study_scraper.discovery.bundestag_dip import _MAX_TERMS
        assert _MAX_TERMS == 8


class TestApiKey:
    def test_resolution_order(self, monkeypatch) -> None:
        monkeypatch.delenv("DIP_API_KEY", raising=False)
        assert resolve_api_key() == PUBLIC_API_KEY
        monkeypatch.setenv("DIP_API_KEY", "personal-key")
        assert resolve_api_key() == "personal-key"
        assert resolve_api_key("explicit") == "explicit"
