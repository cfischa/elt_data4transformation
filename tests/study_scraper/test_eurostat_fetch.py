"""Live-fetch behaviour for the Eurostat source (A14.1).

These exercise the filtered, size-guarded HTTP path with a mocked
transport — no network, no DB — so they always run. They cover the
fixes for the `nrg_bal_s` MemoryError seen on 2026-06-24:

  - geo=DE is applied by default (shrinks payloads ~30x);
  - an over-size response is skipped (warning) instead of crashing;
  - a 400 on the filtered request retries unfiltered.
"""

from __future__ import annotations

import json

import httpx

from study_scraper.sources.eurostat import EurostatSource


RUN_ID = "00000000-0000-0000-0000-000000000000"

_MINIMAL_DATASET = {
    "class": "dataset",
    "label": "Test table",
    "updated": "2026-01-01",
    "id": ["geo"],
    "size": [1],
    "dimension": {"geo": {"category": {"index": {"DE": 0}}}},
    "value": {"0": 1.23},
}


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _records(src: EurostatSource):
    with src as s:
        return list(s.iter_records(run_id=RUN_ID))


def test_default_filter_is_geo_de() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["geo"] = request.url.params.get("geo")
        seen["format"] = request.url.params.get("format")
        return httpx.Response(200, json=_MINIMAL_DATASET)

    recs = _records(EurostatSource(codes=["nrg_bal_s"], client=_client(handler)))
    assert len(recs) == 1
    assert seen["geo"] == "DE"           # German scraper default
    assert seen["format"] == "json"
    # ...and the filter is recorded in provenance.
    assert recs[0].provenance["filters"] == {"geo": "DE"}


def test_custom_geo_filter() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["geo"] = request.url.params.get("geo")
        return httpx.Response(200, json=_MINIMAL_DATASET)

    _records(EurostatSource(
        codes=["nrg_bal_s"], filters={"geo": "FR"}, client=_client(handler)))
    assert seen["geo"] == "FR"


def test_oversize_payload_is_skipped_not_fatal() -> None:
    big = ("x" * 200).encode()  # bytes; we only need to exceed max_bytes

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=big)

    # max_bytes below the response size => skipped, no MemoryError, no raise.
    recs = _records(EurostatSource(
        codes=["nrg_bal_s"], max_bytes=100, client=_client(handler)))
    assert recs == []


def test_400_on_filtered_request_retries_unfiltered() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        has_geo = "geo" in request.url.params
        calls.append(has_geo)
        if has_geo:
            return httpx.Response(400, text="wrong dimension geo")
        return httpx.Response(200, json=_MINIMAL_DATASET)

    recs = _records(EurostatSource(codes=["some_table"], client=_client(handler)))
    assert len(recs) == 1
    assert calls == [True, False]  # filtered first, then unfiltered retry


def test_no_filters_when_explicitly_empty() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["geo"] = request.url.params.get("geo")
        return httpx.Response(200, json=_MINIMAL_DATASET)

    _records(EurostatSource(
        codes=["nrg_bal_s"], filters={}, client=_client(handler)))
    assert seen["geo"] is None  # opted out of filtering


def test_payload_round_trips_through_record() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_MINIMAL_DATASET)

    recs = _records(EurostatSource(codes=["t"], client=_client(handler)))
    assert recs[0].payload["class"] == "dataset"
    assert json.loads(json.dumps(recs[0].payload))["value"]["0"] == 1.23
