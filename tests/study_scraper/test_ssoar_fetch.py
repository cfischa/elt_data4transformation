"""Live-fetch behaviour for the SSOAR OAI-PMH source (issue #34).

Mocked transport — no network, no DB — so these always run. Covers the
incremental `from=` window: full harvest when `since` is unset (first
run), OAI `from=YYYY-MM-DD` when a prior successful run's timestamp is
known.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest

from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.topics import load_topics


_EMPTY_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords></ListRecords>
</OAI-PMH>"""


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.fixture(scope="module")
def klima_topic():
    topics = load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )
    return next(t for t in topics if t.id == "klima")


def test_no_since_omits_from_param(klima_topic) -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["from"] = request.url.params.get("from")
        return httpx.Response(200, text=_EMPTY_RESPONSE)

    with SSOARSource(client=_client(handler)) as src:
        list(src.iter_candidates(klima_topic))

    assert seen["from"] is None


def test_since_sets_from_param_as_iso_date(klima_topic) -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["from"] = request.url.params.get("from")
        return httpx.Response(200, text=_EMPTY_RESPONSE)

    with SSOARSource(since=date(2026, 6, 20), client=_client(handler)) as src:
        list(src.iter_candidates(klima_topic))

    assert seen["from"] == "2026-06-20"


def test_from_file_mode_ignores_since(klima_topic) -> None:
    """Fixture mode never hits the network, so `since` (a live-mode
    concern) has nothing to do -- no crash, no param to check."""
    fixture = (
        Path(__file__).resolve().parent / "fixtures" / "ssoar" / "klima_records.xml"
    )
    with SSOARSource(from_file=fixture, since=date(2026, 6, 20)) as src:
        cands = list(src.iter_candidates(klima_topic))
    assert len(cands) == 7
