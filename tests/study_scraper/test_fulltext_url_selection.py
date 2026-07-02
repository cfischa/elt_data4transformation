"""Fix #25: fulltext must not fetch openalex.org work pages (bot-protected
metadata, guaranteed 403). Pure tests for select_fetch_url + the provenance
capture that feeds it."""

from __future__ import annotations

from study_scraper.fulltext import select_fetch_url


OA = "https://openalex.org/W123"


def test_non_openalex_canonical_passes_through() -> None:
    url, why = select_fetch_url(
        canonical_url="https://doi.org/10.1515/x", doi="10.1515/x",
    )
    assert url == "https://doi.org/10.1515/x"
    assert why == "canonical"


def test_openalex_prefers_pdf_url() -> None:
    url, why = select_fetch_url(
        canonical_url=OA,
        doi="10.1/x",
        provenance={"pdf_url": "https://host.org/paper.pdf",
                    "landing_page_url": "https://host.org/page"},
    )
    assert url == "https://host.org/paper.pdf"
    assert why == "pdf_url"


def test_openalex_falls_back_to_landing_page() -> None:
    url, why = select_fetch_url(
        canonical_url=OA,
        provenance={"pdf_url": None,
                    "landing_page_url": "https://host.org/page"},
    )
    assert url == "https://host.org/page"
    assert why == "landing_page_url"


def test_openalex_falls_back_to_bare_doi() -> None:
    url, why = select_fetch_url(canonical_url=OA, doi="10.1515/pwp-2023-0031")
    assert url == "https://doi.org/10.1515/pwp-2023-0031"
    assert why == "doi"


def test_openalex_doi_already_a_url() -> None:
    url, why = select_fetch_url(
        canonical_url=OA, doi="https://doi.org/10.1/x",
    )
    assert url == "https://doi.org/10.1/x"
    assert why == "doi"


def test_openalex_nothing_fetchable() -> None:
    url, why = select_fetch_url(canonical_url=OA)
    assert url is None
    assert why == "no_fetchable_url"


def test_openalex_alt_url_pointing_back_at_openalex_is_ignored() -> None:
    url, why = select_fetch_url(
        canonical_url=OA,
        provenance={"landing_page_url": "https://openalex.org/W123"},
    )
    assert url is None and why == "no_fetchable_url"


def test_none_canonical_is_passed_through() -> None:
    url, why = select_fetch_url(canonical_url=None)
    assert url is None and why == "canonical"


def test_openalex_source_captures_location_urls() -> None:
    """The OpenAlex source must surface primary_location URLs in raw so
    the pipeline can put them into provenance."""
    from study_scraper.discovery.openalex import OpenAlexSource

    work = {
        "id": "https://openalex.org/W999",
        "title": "Umfrage zur Kernenergie",
        "doi": "",
        "language": "de",
        "primary_location": {
            "landing_page_url": "https://journal.example.org/a/999",
            "pdf_url": "https://journal.example.org/a/999.pdf",
            "source": {"display_name": "Journal"},
        },
    }
    src = OpenAlexSource()

    class _T:
        id = "atomkraft"

    cand = src._work_to_candidate(work, topic=_T())  # noqa: SLF001 - unit test
    assert cand is not None
    assert cand.raw["landing_page_url"] == "https://journal.example.org/a/999"
    assert cand.raw["pdf_url"] == "https://journal.example.org/a/999.pdf"
