"""Tests for the CORE (core.ac.uk) discovery source.

The from_file fixture and the live HTTP path share the same parser, so
testing the fixture path validates production parsing.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Callable, List

import httpx
import pytest

from study_scraper.discovery.core_search import (
    CoreSearchSource,
    DEFAULT_BASE_URL,
    _build_search_query,
)
from study_scraper.topics import load_topics

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "core_search.json"


@pytest.fixture(scope="module")
def klima():
    topics = load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )
    return next(t for t in topics if t.id == "klima")


class TestFromFile:
    def test_parses_all_results(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        assert len(cands) == 3
        assert all(c.source_id == "core" for c in cands)

    def test_canonical_url_prefers_doi(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        eco = next(c for c in cands if "Ökosystembasierte" in c.title)
        assert eco.canonical_url == "https://doi.org/10.1007/978-3-658-05578-3_12"
        assert eco.doi == "10.1007/978-3-658-05578-3_12"

    def test_canonical_url_falls_back_to_download_url(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        trump = next(c for c in cands if "Trump" in c.title)
        assert trump.doi is None
        assert trump.canonical_url == "https://core.ac.uk/download/553024823.pdf"

    def test_canonical_url_falls_back_to_works_page(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        gerechtigkeit = next(c for c in cands if "Gerechtigkeit" in c.title)
        assert gerechtigkeit.doi is None
        assert gerechtigkeit.canonical_url == "https://core.ac.uk/works/158306103"

    def test_extracts_authors(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        eco = next(c for c in cands if "Ökosystembasierte" in c.title)
        assert "Röder, Norbert" in eco.authors
        assert "Schröter-Schlaack, Christoph" in eco.authors

    def test_extracts_publication_date_from_published_date(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        eco = next(c for c in cands if "Ökosystembasierte" in c.title)
        assert eco.publication_date == date(2016, 10, 24)

    def test_falls_back_to_year_when_published_date_empty(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        gerechtigkeit = next(c for c in cands if "Gerechtigkeit" in c.title)
        assert gerechtigkeit.publication_date == date(2015, 1, 1)

    def test_missing_language_is_none(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        gerechtigkeit = next(c for c in cands if "Gerechtigkeit" in c.title)
        assert gerechtigkeit.language is None

    def test_language_passed_through(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        eco = next(c for c in cands if "Ökosystembasierte" in c.title)
        assert eco.language == "de"

    def test_raw_includes_core_id_and_document_type(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima))
        eco = next(c for c in cands if "Ökosystembasierte" in c.title)
        assert eco.raw["core_id"] == "266729884"
        assert eco.raw["document_type"] == "chapter"
        assert "EconStor (ZBW Kiel)" in eco.raw["data_providers"]

    def test_limit_respected(self, klima) -> None:
        with CoreSearchSource(from_file=FIXTURE) as src:
            cands = list(src.iter_candidates(klima, limit=1))
        assert len(cands) == 1


def _recording_sleeper() -> "tuple[Callable[[float], None], List[float]]":
    slept: List[float] = []
    return (lambda s: slept.append(s)), slept


class TestLivePagination:
    def test_paginates_by_offset_until_exhausted(self, klima) -> None:
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            offset = int(dict(request.url.params).get("offset", "0"))
            if offset == 0:
                return httpx.Response(
                    200,
                    json={
                        "totalHits": 1,
                        "limit": 25,
                        "offset": 0,
                        "results": [
                            {
                                "id": 1,
                                "title": "Klimapolitik",
                                "abstract": "",
                                "authors": [],
                                "doi": None,
                                "downloadUrl": "",
                                "language": None,
                                "publishedDate": "",
                                "yearPublished": 2020,
                                "dataProviders": [],
                                "links": [],
                            }
                        ],
                    },
                )
            return httpx.Response(200, json={"totalHits": 1, "results": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with CoreSearchSource(client=client) as src:
            cands = list(src.iter_candidates(klima))

        assert len(cands) == 1
        assert calls["n"] == 1  # stops once offset >= totalHits, no extra page

    def test_stops_on_empty_results_page(self, klima) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"totalHits": 500, "results": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with CoreSearchSource(client=client) as src:
            cands = list(src.iter_candidates(klima))

        assert cands == []

    def test_uses_default_base_url(self, klima) -> None:
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(200, json={"totalHits": 0, "results": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with CoreSearchSource(client=client) as src:
            list(src.iter_candidates(klima, limit=1))

        assert seen["url"].startswith(DEFAULT_BASE_URL)


class TestHelpers:
    def test_build_search_query_uses_topic_keywords(self, klima) -> None:
        q = _build_search_query(klima)
        assert q
        assert any(
            kw.lower() in q.lower()
            for kw in ["Klimaschutz", "Energiewende", "Klimawandel", "CO2"]
        )

    def test_build_search_query_or_joins_terms(self, klima) -> None:
        q = _build_search_query(klima)
        assert " OR " in q
        for chunk in q.split(" OR "):
            assert " " not in chunk or chunk.startswith('"'), chunk
