"""Tests for Option A / A21: the llm-v1 attribution extractor.

Pure-function tests (prompt build, response parse, schema, id) always
run with no SDK and no API key. Integration tests (storage round-trip,
queue, search, CLI offline path) gate on STUDY_SCRAPER_TEST_DSN.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.extractors import llm_v1
from study_scraper.extractors.llm_v1 import (
    Attribution,
    OUTPUT_SCHEMA,
    build_user_prompt,
    parse_response,
)


# --------------------------------------------------------------------------
# Pure: schema + prompt + parse + id  (no SDK, no network)
# --------------------------------------------------------------------------


class TestSchema:
    def test_schema_shape(self) -> None:
        assert OUTPUT_SCHEMA["type"] == "object"
        assert OUTPUT_SCHEMA["additionalProperties"] is False
        item = OUTPUT_SCHEMA["properties"]["attributions"]["items"]
        assert item["additionalProperties"] is False
        assert set(item["required"]) == {
            "question", "position", "percentage", "population",
            "confidence", "source_span"
        }
        assert item["properties"]["position"]["enum"] == [
            "support", "oppose", "neutral", "unspecified"
        ]


class TestBuildPrompt:
    def test_includes_title_abstract_and_claims(self) -> None:
        p = build_user_prompt(
            title="Klima-Umfrage",
            abstract="62% befürworten ein Klimaschutzgesetz.",
            claim_snippets=["62% befürworten ein Klimaschutzgesetz"],
        )
        assert "Klima-Umfrage" in p
        assert "62%" in p
        assert "REGEX CLAIMS" in p

    def test_handles_missing_fields(self) -> None:
        p = build_user_prompt(title=None, abstract=None, claim_snippets=[])
        assert "(none)" in p
        assert "REGEX CLAIMS" not in p  # no claims section when empty


class TestParseResponse:
    def _resp(self, items) -> str:
        return json.dumps({"attributions": items})

    def test_parses_well_formed(self) -> None:
        text = self._resp([
            {"question": "Re-enter nuclear energy", "position": "support",
             "percentage": 55, "population": "Befragte", "confidence": 0.9},
            {"question": "Re-enter nuclear energy", "position": "oppose",
             "percentage": 36, "population": None, "confidence": 0.8},
        ])
        attrs = parse_response(text, study_id="s" * 64)
        assert len(attrs) == 2
        assert attrs[0].position == "support" and attrs[0].percentage == 55.0
        assert attrs[1].position == "oppose" and attrs[1].population is None

    def test_strips_code_fence(self) -> None:
        inner = self._resp([
            {"question": "Q", "position": "support",
             "percentage": 50, "population": None, "confidence": 1},
        ])
        attrs = parse_response("```json\n" + inner + "\n```", study_id="s" * 64)
        assert len(attrs) == 1

    def test_invalid_position_falls_back_to_unspecified(self) -> None:
        attrs = parse_response(self._resp([
            {"question": "Q", "position": "favoured",
             "percentage": 10, "population": None, "confidence": 0.5},
        ]), study_id="s" * 64)
        assert attrs[0].position == "unspecified"

    def test_out_of_range_percentage_nulled(self) -> None:
        attrs = parse_response(self._resp([
            {"question": "Q", "position": "support",
             "percentage": 250, "population": None, "confidence": 0.5},
        ]), study_id="s" * 64)
        assert attrs[0].percentage is None

    def test_null_percentage_kept(self) -> None:
        attrs = parse_response(self._resp([
            {"question": "Q", "position": "neutral",
             "percentage": None, "population": None, "confidence": 0.4},
        ]), study_id="s" * 64)
        assert attrs[0].percentage is None
        assert attrs[0].position == "neutral"

    def test_confidence_clamped(self) -> None:
        attrs = parse_response(self._resp([
            {"question": "Q", "position": "support",
             "percentage": 1, "population": None, "confidence": 5},
        ]), study_id="s" * 64)
        assert attrs[0].confidence == 1.0

    def test_blank_question_skipped(self) -> None:
        attrs = parse_response(self._resp([
            {"question": "  ", "position": "support",
             "percentage": 1, "population": None, "confidence": 1},
        ]), study_id="s" * 64)
        assert attrs == []

    def test_garbage_returns_empty(self) -> None:
        assert parse_response("not json at all", study_id="s" * 64) == []
        assert parse_response("", study_id="s" * 64) == []
        assert parse_response('{"attributions": "nope"}', study_id="s" * 64) == []

    def test_recovers_object_from_surrounding_prose(self) -> None:
        text = 'Here is the result: {"attributions": []} hope that helps'
        assert parse_response(text, study_id="s" * 64) == []


class TestAttributionId:
    def test_id_is_deterministic_and_64_hex(self) -> None:
        a = Attribution(study_id="s" * 64, question="Q", position="support",
                        percentage=55.0, population=None, confidence=0.9)
        b = Attribution(study_id="s" * 64, question="Q", position="support",
                        percentage=55.0, population="other", confidence=0.1)
        assert a.id == b.id  # id keys on study/question/position/pct/model
        assert len(a.id) == 64

    def test_id_differs_on_position(self) -> None:
        a = Attribution(study_id="s" * 64, question="Q", position="support",
                        percentage=55.0, population=None, confidence=0.9)
        b = Attribution(study_id="s" * 64, question="Q", position="oppose",
                        percentage=55.0, population=None, confidence=0.9)
        assert a.id != b.id


# --------------------------------------------------------------------------
# extract_live with an injected fake client (no real SDK / network)
# --------------------------------------------------------------------------


class _FakeBlock:
    type = "text"

    def __init__(self, text: str) -> None:
        self.text = text


class _FakeResponse:
    model = "claude-opus-4-8"

    def __init__(self, text: str) -> None:
        self.content = [_FakeBlock(text)]


class _FakeMessages:
    def __init__(self, text: str) -> None:
        self._text = text
        self.last_kwargs = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        return _FakeResponse(self._text)


class _FakeClient:
    def __init__(self, text: str) -> None:
        self.messages = _FakeMessages(text)


def test_extract_live_with_injected_client_uses_prompt_cache() -> None:
    payload = json.dumps({"attributions": [
        {"question": "Stricter climate laws", "position": "support",
         "percentage": 62, "population": "n=1009", "confidence": 0.95},
    ]})
    client = _FakeClient(payload)
    attrs = llm_v1.extract_live(
        study_id="s" * 64,
        title="Forsa", abstract="62% ...",
        claim_snippets=["62% ..."],
        client=client,
    )
    assert len(attrs) == 1 and attrs[0].percentage == 62.0
    # System prompt sent as a cache_control ephemeral block.
    sys_blocks = client.messages.last_kwargs["system"]
    assert sys_blocks[0]["cache_control"] == {"type": "ephemeral"}
    # Structured output requested.
    assert client.messages.last_kwargs["output_config"]["format"]["type"] == "json_schema"
    # Served model recorded on the attribution.
    assert attrs[0].model == "claude-opus-4-8"


# --------------------------------------------------------------------------
# Integration: storage + queue + search + offline apply
# --------------------------------------------------------------------------


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration"
)


@pytest.fixture(scope="module")
def storage():
    from study_scraper.storage import PostgresStorage
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytest.fixture(autouse=True)
def _clean(storage) -> Iterator[None]:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.attributions CASCADE")
            cur.execute("TRUNCATE study_scraper.claims CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


def _seed_study_with_claim(storage, *, title: str, abstract: str,
                           publication_date=None):
    from study_scraper.claims import extract_claims
    from study_scraper.models import Provenance, Study
    study = Study.build(
        canonical_url=f"https://example.org/{abs(hash(title))}",
        title=title,
        abstract=abstract,
        publication_date=publication_date,
        fetched_at=datetime(2026, 6, 15, tzinfo=timezone.utc),
        source_id="openalex",
        provenance=Provenance(discovery_source="openalex"),
        topic_ids=["klima"],
        topic_scores={"klima": 0.5},
    )
    storage.upsert_study(study)
    claims = extract_claims(study_id=study.id, title=title, abstract=abstract)
    storage.upsert_claims(study.id, claims, extractor="regex-v1")
    return study


def test_attribution_queue_lists_studies_with_claims_no_attribution(storage) -> None:
    s = _seed_study_with_claim(
        storage, title="Klima", abstract="62% befürworten ein Klimaschutzgesetz.")
    rows = storage.query_view("attribution_queue", limit=10)
    assert s.id in {r["id"] for r in rows}


def test_offline_apply_and_search(storage) -> None:
    from study_scraper.attribute import apply_responses

    s = _seed_study_with_claim(
        storage, title="Kernenergie",
        abstract="55% befürworten den Wiedereinstieg in die Atomkraft.")
    response = json.dumps({"attributions": [
        {"question": "Re-enter nuclear energy", "position": "support",
         "percentage": 55, "population": "Befragte", "confidence": 0.9},
    ]})
    results = apply_responses(storage=storage, responses={s.id: response})
    assert results[0]["attributions"] == 1
    assert storage.count_attributions() == 1

    hits = storage.search_attributions(query="nuclear")
    assert hits and float(hits[0]["percentage"]) == 55.0
    assert hits[0]["position"] == "support"

    # Once attributed, the study leaves the queue.
    rows = storage.query_view("attribution_queue", limit=10)
    assert s.id not in {r["id"] for r in rows}


def test_upsert_attributions_idempotent_per_model(storage) -> None:
    from study_scraper.attribute import apply_responses
    s = _seed_study_with_claim(storage, title="X", abstract="50% support Y.")
    resp = json.dumps({"attributions": [
        {"question": "Y", "position": "support",
         "percentage": 50, "population": None, "confidence": 0.7},
    ]})
    apply_responses(storage=storage, responses={s.id: resp})
    apply_responses(storage=storage, responses={s.id: resp})  # re-run
    assert storage.count_attributions() == 1  # not 2


def test_search_attributions_since_filters_old_and_undated(storage) -> None:
    from datetime import date

    from study_scraper.attribute import apply_responses

    old = _seed_study_with_claim(
        storage, title="Alt", abstract="44% befürworten strengere Klimagesetze.",
        publication_date=date(2021, 3, 1))
    new = _seed_study_with_claim(
        storage, title="Neu", abstract="62% befürworten strengere Klimagesetze.",
        publication_date=date(2025, 5, 1))
    undated = _seed_study_with_claim(
        storage, title="Ohne Datum",
        abstract="50% befürworten strengere Klimagesetze.")

    def resp(pct):
        return json.dumps({"attributions": [
            {"question": "Stricter climate laws", "position": "support",
             "percentage": pct, "population": None, "confidence": 0.9},
        ]})

    apply_responses(storage=storage, responses={
        old.id: resp(44), new.id: resp(62), undated.id: resp(50)})

    all_hits = storage.search_attributions(query="climate")
    assert len(all_hits) == 3
    recent = storage.search_attributions(query="climate", since=2024)
    assert [float(r["percentage"]) for r in recent] == [62.0]


def test_search_attributions_carries_sample_size(storage) -> None:
    from study_scraper.attribute import apply_responses

    s = _seed_study_with_claim(
        storage, title="Tempolimit",
        abstract="60% der Befragten (n=1009) befürworten ein Tempolimit.")
    resp = json.dumps({"attributions": [
        {"question": "General speed limit", "position": "support",
         "percentage": 60, "population": None, "confidence": 0.9},
    ]})
    apply_responses(storage=storage, responses={s.id: resp})
    hits = storage.search_attributions(query="speed limit")
    assert hits and int(hits[0]["sample_size"]) == 1009


def test_search_attributions_sample_size_null_without_n_claim(storage) -> None:
    from study_scraper.attribute import apply_responses

    s = _seed_study_with_claim(
        storage, title="Rente", abstract="70% wollen ein höheres Rentenniveau.")
    resp = json.dumps({"attributions": [
        {"question": "Higher pension level", "position": "support",
         "percentage": 70, "population": None, "confidence": 0.9},
    ]})
    apply_responses(storage=storage, responses={s.id: resp})
    hits = storage.search_attributions(query="pension")
    assert hits and hits[0]["sample_size"] is None


def test_dump_prompts_emits_queue(storage) -> None:
    from study_scraper.attribute import dump_prompts
    s = _seed_study_with_claim(
        storage, title="Klima", abstract="62% befürworten ein Klimaschutzgesetz.")
    items = dump_prompts(storage=storage, limit=10)
    assert any(it["study_id"] == s.id for it in items)
    assert any("62%" in it["prompt"] for it in items)
