"""Tests for the EconStor OAI-PMH discovery source (shares SSOAR's parser)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from study_scraper.discovery.econstor import EconStorSource
from study_scraper.topic_filter import score_text
from study_scraper.topics import load_topics

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "econstor" / "steuern_records.xml"


@pytest.fixture(scope="module")
def steuern_topic():
    topics = load_topics(ROOT / "config" / "topics" / "topics.csv")
    return next(t for t in topics if t.id == "steuern")


def test_parses_records_and_skips_deleted(steuern_topic) -> None:
    with EconStorSource(from_file=FIXTURE) as src:
        cands = list(src.iter_candidates(steuern_topic))
    assert [c.external_id for c in cands] == [
        "oai:econstor.eu:10419/100001",
        "oai:econstor.eu:10419/100002",
    ]


def test_candidate_fields(steuern_topic) -> None:
    with EconStorSource(from_file=FIXTURE) as src:
        cand = next(iter(src.iter_candidates(steuern_topic)))
    assert cand.source_id == "econstor"
    assert cand.canonical_url == "https://hdl.handle.net/10419/100001"
    assert cand.language == "de"
    assert cand.publication_date == date(2024, 3, 1)
    assert cand.discovery_query == "steuern"


def test_default_base_url_and_resumption_token() -> None:
    src = EconStorSource(from_file=FIXTURE)
    assert src._base_url == "https://www.econstor.eu/oai/request"
    assert src._extract_resumption_token(FIXTURE.read_text(encoding="utf-8")) == "tok123"


def test_topic_filter_keeps_only_on_topic(steuern_topic) -> None:
    with EconStorSource(from_file=FIXTURE) as src:
        cands = list(src.iter_candidates(steuern_topic))
    kept = [
        c.external_id
        for c in cands
        if score_text(f"{c.title}\n{c.abstract or ''}", steuern_topic).passes
    ]
    assert kept == ["oai:econstor.eu:10419/100001"]
