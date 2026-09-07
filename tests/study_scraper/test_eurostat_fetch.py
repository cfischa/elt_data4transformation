"""Live-fetch behaviour for the Eurostat source (A14.1).

These exercise the filtered, size-guarded HTTP path with a mocked
transport — no network, no DB — so they always run. They cover the
fixes for the `nrg_bal_s` MemoryError seen on 2026-06-24:

  - geo=DE is applied by default (shrinks payloads ~30x);
  - an over-size response records a `payload_uri` pointer instead of
    crashing *or* silently vanishing (#153 -- it used to just be
    dropped, with nothing in `source_records` and no trace past a log
    line, see the module docstring in `sources/eurostat.py`);
  - a 400 on the filtered request retries unfiltered.
"""

from __future__ import annotations

import hashlib
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


def test_oversize_payload_records_pointer_not_fatal() -> None:
    big = ("x" * 200).encode()  # bytes; we only need to exceed max_bytes

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=big)

    # max_bytes below the response size => no MemoryError, no raise, and
    # (per #153) not silently dropped either -- a pointer record instead.
    recs = _records(EurostatSource(
        codes=["nrg_bal_s"], max_bytes=100, client=_client(handler)))
    assert len(recs) == 1
    record = recs[0]
    assert record.payload is None
    assert record.payload_uri == (
        "https://ec.europa.eu/eurostat/databrowser/view/nrg_bal_s"
    )
    assert record.provenance["oversized"] is True
    assert record.provenance["byte_size"] == 200
    assert record.provenance["max_bytes"] == 100
    # Real bytes' hash, not fabricated -- a later re-fetch under a
    # narrower filter that actually changes still registers as a change.
    assert record.content_hash == hashlib.sha256(big).hexdigest()


def test_under_limit_payload_is_unaffected_by_the_size_guard() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_MINIMAL_DATASET)

    recs = _records(EurostatSource(
        codes=["nrg_bal_s"], max_bytes=1_000_000, client=_client(handler)))
    assert len(recs) == 1
    assert recs[0].payload is not None
    assert recs[0].payload_uri is None
    assert recs[0].provenance.get("oversized") is not True


def test_oversized_code_does_not_abort_other_codes_in_the_same_run() -> None:
    small_body = json.dumps(_MINIMAL_DATASET).encode()
    big_body = ("x" * (len(small_body) * 10)).encode()
    bodies = {"big_code": big_body, "small_code": small_body}

    def handler(request: httpx.Request) -> httpx.Response:
        code = str(request.url).rsplit("/", 1)[-1].split("?", 1)[0]
        return httpx.Response(200, content=bodies[code])

    recs = _records(EurostatSource(
        codes=["big_code", "small_code"],
        max_bytes=len(small_body) + 1,
        client=_client(handler),
    ))
    by_code = {r.source_record_id: r for r in recs}
    assert set(by_code) == {"big_code", "small_code"}
    assert by_code["big_code"].payload is None
    assert by_code["big_code"].payload_uri is not None
    assert by_code["small_code"].payload is not None
    assert by_code["small_code"].payload_uri is None


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
