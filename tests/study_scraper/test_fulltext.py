"""Tests for Phase 6-full (A20): full-document statistics extraction,
the reading-list view, and the reference follower's pending queue.

Pure-Python parts always run; DB-touching parts gate on
STUDY_SCRAPER_TEST_DSN like the rest of the integration suite.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.claims import extract_claims_from_text
from study_scraper.config import get_settings
from study_scraper.fulltext import (
    PROCESSED_MARKER_PREFIX,
    extract_text,
    extract_text_from_html,
    extract_text_from_pdf,
    process_document,
    sniff_kind,
)
from study_scraper.models import Provenance, Study
from study_scraper.storage import PostgresStorage


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "fulltext"
PDF_BYTES = (FIXTURES / "klima_report.pdf").read_bytes()
HTML_BYTES = (FIXTURES / "energiewende_report.html").read_bytes()


# --------------------------------------------------------------------------
# Pure: type sniffing + text extraction
# --------------------------------------------------------------------------


class TestSniffKind:
    def test_pdf_magic_bytes(self) -> None:
        assert sniff_kind(PDF_BYTES, None) == "pdf"

    def test_pdf_content_type_without_magic(self) -> None:
        assert sniff_kind(b"garbage", "application/pdf") == "pdf"

    def test_html_doctype(self) -> None:
        assert sniff_kind(HTML_BYTES, None) == "html"

    def test_html_content_type(self) -> None:
        assert sniff_kind(b"<div>x</div>", "text/html; charset=utf-8") == "html"

    def test_unknown(self) -> None:
        assert sniff_kind(b"\x00\x01binary", "application/octet-stream") == "unknown"


class TestTextExtraction:
    def test_pdf_text_contains_statistics(self) -> None:
        text = extract_text_from_pdf(PDF_BYTES)
        assert "62%" in text
        assert "Klimaschutzgesetz" in text
        assert "n=2034" in text

    def test_html_text_contains_statistics_and_drops_script(self) -> None:
        text = extract_text_from_html(HTML_BYTES)
        assert "68%" in text
        assert "Windkraft" in text
        assert "52%" in text          # table cell
        assert "tracking noise" not in text  # <script> stripped

    def test_extract_text_dispatch(self) -> None:
        kind, text = extract_text(PDF_BYTES, None)
        assert kind == "pdf" and "62%" in text
        kind, text = extract_text(HTML_BYTES, "text/html")
        assert kind == "html" and "68%" in text
        kind, text = extract_text(b"\x00\x01", None)
        assert kind == "unknown" and text == ""


class TestPersistArtifactsSetting:
    """Pure: no DB needed to check the config knob itself."""

    def test_defaults_true(self) -> None:
        from study_scraper.config import Settings

        assert Settings().persist_artifacts is True

    def test_env_var_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from study_scraper.config import Settings

        monkeypatch.setenv("STUDY_SCRAPER_PERSIST_ARTIFACTS", "false")
        assert Settings().persist_artifacts is False


class TestFulltextClaims:
    def test_extracts_all_percentages_from_pdf_text(self) -> None:
        text = extract_text_from_pdf(PDF_BYTES)
        claims = extract_claims_from_text(study_id="s" * 64, text=text)
        pct = sorted(
            c.numeric_value for c in claims if c.unit == "%"
        )
        assert pct == [28.0, 55.0, 62.0, 73.0]
        n = [c for c in claims if c.unit == "n"]
        assert any(c.numeric_value == 2034.0 for c in n)
        assert all(c.source_field == "fulltext" for c in claims)

    def test_extracts_from_html_including_table_cells(self) -> None:
        text = extract_text_from_html(HTML_BYTES)
        claims = extract_claims_from_text(study_id="s" * 64, text=text)
        pct = sorted(c.numeric_value for c in claims if c.unit == "%")
        assert pct == [41.0, 49.0, 52.0, 57.0, 68.0]


# --------------------------------------------------------------------------
# Integration: process_document + reading_list + pending_references
# --------------------------------------------------------------------------


pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration"
)


@pytest.fixture(scope="module")
def storage() -> PostgresStorage:
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytest.fixture(autouse=True)
def _clean(storage: PostgresStorage) -> Iterator[None]:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.claims CASCADE")
            cur.execute("TRUNCATE study_scraper.source_records CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


def _mk_study(url: str, title: str, **extras) -> Study:
    return Study.build(
        canonical_url=url,
        title=title,
        fetched_at=datetime(2026, 5, 31, tzinfo=timezone.utc),
        source_id="ssoar",
        provenance=Provenance(discovery_source="ssoar", **extras.pop("prov", {})),
        topic_ids=["klima"],
        topic_scores={"klima": 0.5},
        **extras,
    )


def test_process_document_pdf_end_to_end(
    storage: PostgresStorage, tmp_path: Path
) -> None:
    study = _mk_study("https://example.org/report.pdf", "Klima report PDF")
    storage.upsert_study(study)

    summary = process_document(
        storage=storage, study_id=study.id,
        content=PDF_BYTES, content_type="application/pdf",
        artifact_dir=tmp_path,
    )
    assert summary["status"] == "ok"
    assert summary["kind"] == "pdf"
    assert summary["claims"] >= 5  # 4 percentages + n=2034

    # Artifact saved + ref recorded.
    artifact = tmp_path / f"{study.id}.pdf"
    assert artifact.exists()
    row = storage.get_study(study.id)
    assert row["raw_artifact_ref"] == str(artifact)

    # Claims are queryable via search and carry the right extractor.
    hits = storage.search_claims(query="klimaschutzgesetz", unit="%")
    assert any(float(h["numeric_value"]) == 62.0 for h in hits)
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT extractor FROM study_scraper.claims "
                "WHERE study_id = %s", (study.id,),
            )
            extractors = {r["extractor"] for r in cur.fetchall()}
    assert extractors == {"regex-v2"}


def test_fulltext_claims_coexist_with_abstract_claims(
    storage: PostgresStorage, tmp_path: Path
) -> None:
    """regex-v1 (abstract) and regex-v2 (fulltext) claims coexist;
    re-running fulltext replaces only regex-v2 rows."""
    from study_scraper.claims import extract_claims

    study = _mk_study(
        "https://example.org/r2.pdf", "Report v2",
        abstract="Vorab: 99% Zustimmung in der Pilotstudie.",
    )
    storage.upsert_study(study)
    abstract_claims = extract_claims(
        study_id=study.id, title=study.title, abstract=study.abstract
    )
    storage.upsert_claims(study.id, abstract_claims, extractor="regex-v1")

    process_document(
        storage=storage, study_id=study.id,
        content=PDF_BYTES, artifact_dir=tmp_path,
    )
    process_document(  # idempotent re-run
        storage=storage, study_id=study.id,
        content=PDF_BYTES, artifact_dir=tmp_path,
    )

    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT extractor, COUNT(*) AS c FROM study_scraper.claims "
                "WHERE study_id = %s GROUP BY extractor ORDER BY extractor",
                (study.id,),
            )
            counts = {r["extractor"]: r["c"] for r in cur.fetchall()}
    assert counts.get("regex-v1") == 1   # the 99% abstract claim survives
    assert counts.get("regex-v2", 0) >= 5


def test_process_document_unusable_payload(
    storage: PostgresStorage, tmp_path: Path
) -> None:
    study = _mk_study("https://example.org/bin", "Binary blob")
    storage.upsert_study(study)
    summary = process_document(
        storage=storage, study_id=study.id,
        content=b"\x00\x01\x02", artifact_dir=tmp_path,
    )
    assert summary["status"].startswith("no_text")
    assert storage.get_study(study.id)["raw_artifact_ref"] is None


def test_reading_list_view_reasons(
    storage: PostgresStorage, tmp_path: Path
) -> None:
    """no_artifact before fulltext; gone from list once claims exist;
    no_claims when fulltext found nothing quantitative."""
    quant = _mk_study("https://example.org/quant.pdf", "Quantitative Studie")
    quali = _mk_study("https://example.org/quali", "Qualitative Diskursanalyse")
    fresh = _mk_study("https://example.org/fresh", "Frisch ingestiert")
    for s in (quant, quali, fresh):
        storage.upsert_study(s)

    # quant gets fulltext with numbers -> leaves the list.
    process_document(
        storage=storage, study_id=quant.id,
        content=PDF_BYTES, artifact_dir=tmp_path,
    )
    # quali gets an artifact but its text has no numbers.
    no_numbers_html = b"<html><body><p>Eine rein qualitative Analyse ohne Zahlen.</p></body></html>"
    process_document(
        storage=storage, study_id=quali.id,
        content=no_numbers_html, content_type="text/html",
        artifact_dir=tmp_path,
    )

    rows = storage.query_view("reading_list", limit=50)
    by_id = {r["id"]: r for r in rows}
    assert quant.id not in by_id                      # has claims
    assert by_id[quali.id]["reason"] == "no_claims"   # read it yourself
    assert by_id[fresh.id]["reason"] == "no_artifact" # fetch pending


def test_process_document_no_persist_leaves_marker_not_path(
    storage: PostgresStorage, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """STUDY_SCRAPER_PERSIST_ARTIFACTS=false (scheduled CI, disk vanishes
    after the job): no file is written, but raw_artifact_ref still gets a
    processed:<sha> marker so the fulltext queue excludes the study."""
    monkeypatch.setattr(get_settings(), "persist_artifacts", False)
    study = _mk_study("https://example.org/ci-run.pdf", "Report processed in CI")
    storage.upsert_study(study)

    summary = process_document(
        storage=storage, study_id=study.id,
        content=PDF_BYTES, content_type="application/pdf",
        artifact_dir=tmp_path,
    )
    assert summary["status"] == "ok"
    expected_ref = PROCESSED_MARKER_PREFIX + hashlib.sha256(PDF_BYTES).hexdigest()
    assert summary["artifact"] == expected_ref
    assert list(tmp_path.iterdir()) == []  # nothing written to disk

    row = storage.get_study(study.id)
    assert row["raw_artifact_ref"] == expected_ref

    queue = storage.list_studies_for_fulltext(limit=10)
    assert study.id not in {r["id"] for r in queue}  # still leaves the queue


def test_list_studies_for_fulltext_queue(
    storage: PostgresStorage, tmp_path: Path
) -> None:
    a = _mk_study("https://example.org/a", "Queued A")
    b = _mk_study("https://example.org/b", "Queued B")
    storage.upsert_study(a)
    storage.upsert_study(b)
    queue = storage.list_studies_for_fulltext(limit=10)
    assert {r["id"] for r in queue} == {a.id, b.id}

    process_document(
        storage=storage, study_id=a.id,
        content=PDF_BYTES, artifact_dir=tmp_path,
    )
    queue = storage.list_studies_for_fulltext(limit=10)
    assert {r["id"] for r in queue} == {b.id}
    # include_done re-queues processed studies too.
    queue = storage.list_studies_for_fulltext(limit=10, include_done=True)
    assert {r["id"] for r in queue} == {a.id, b.id}


def test_set_fetch_conditional_merges_into_provenance(
    storage: PostgresStorage,
) -> None:
    """Issue #34: ETag/Last-Modified from a fulltext fetch persist onto
    the study's provenance so the next run can send a conditional GET."""
    study = _mk_study("https://example.org/cond.pdf", "Conditional GET study")
    storage.upsert_study(study)

    changed = storage.set_fetch_conditional(
        study.id, etag='"abc123"', last_modified="Wed, 01 Jul 2026 00:00:00 GMT",
    )
    assert changed is True

    row = storage.get_study(study.id)
    assert row["provenance"]["fetch_etag"] == '"abc123"'
    assert row["provenance"]["fetch_last_modified"] == "Wed, 01 Jul 2026 00:00:00 GMT"

    # Existing provenance keys survive the merge.
    assert row["provenance"]["discovery_source"] == "ssoar"


def test_set_fetch_conditional_noop_without_headers(
    storage: PostgresStorage,
) -> None:
    study = _mk_study("https://example.org/nocond.pdf", "No headers study")
    storage.upsert_study(study)

    changed = storage.set_fetch_conditional(study.id)
    assert changed is False
    row = storage.get_study(study.id)
    assert "fetch_etag" not in row["provenance"]


def test_pending_references_queue(storage: PostgresStorage) -> None:
    """The follower lists cited works we don't have, skips ones we do."""
    from study_scraper.follow import pending_references

    citing = _mk_study(
        "https://doi.org/10.1/citing", "Citing study",
        prov={
            "openalex_id": "https://openalex.org/W100",
            "referenced_works": [
                "https://openalex.org/W200",   # unknown -> pending
                "https://openalex.org/W300",   # known via canonical_url
                "https://openalex.org/W100",   # self -> known
            ],
            "related_works": ["https://openalex.org/W400"],  # unknown
        },
    )
    known = _mk_study("https://openalex.org/W300", "Already ingested")
    storage.upsert_study(citing)
    storage.upsert_study(known)

    pending = pending_references(storage, limit=50)
    assert "https://openalex.org/W200" in pending
    assert "https://openalex.org/W400" in pending
    assert "https://openalex.org/W300" not in pending
    assert "https://openalex.org/W100" not in pending
