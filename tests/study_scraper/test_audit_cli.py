"""Agent task #20: `audit --sample-size N` accuracy spot-check CLI."""

from __future__ import annotations

import os
import re
from typing import Iterator

import pytest
from typer.testing import CliRunner

from study_scraper.cli import app


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")


def _plain(text: str) -> str:
    """Strip ANSI escapes: on CI runners rich forces terminal mode and
    styles help output, splitting option names with color codes (seen
    with rich 15 on GitHub Actions, 2026-07-05)."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_cli_registers_audit_command() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["audit", "--help"])
    assert result.exit_code == 0
    assert "--sample-size" in _plain(result.output)
    assert "spot-check" in _plain(result.output).lower()


# ---------------------------------------------------------------------------
# Integration (real Postgres)
# ---------------------------------------------------------------------------

pytestmark_integration = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration"
)


@pytest.fixture(scope="module")
def storage():
    from study_scraper.storage import PostgresStorage
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytestmark_integration
def test_sample_attributions_shape(storage) -> None:
    from datetime import datetime, timezone
    import json as _json

    from study_scraper.attribute import apply_responses
    from study_scraper.claims import extract_claims
    from study_scraper.models import Provenance, Study

    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.attributions CASCADE")
            cur.execute("TRUNCATE study_scraper.claims CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
        conn.commit()

    study = Study.build(
        canonical_url="https://example.org/audit-test",
        title="Audit test",
        abstract="62% befürworten ein Klimaschutzgesetz.",
        fetched_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        source_id="openalex",
        provenance=Provenance(discovery_source="openalex"),
        topic_ids=["klima"],
        topic_scores={"klima": 0.5},
    )
    storage.upsert_study(study)
    claims = extract_claims(
        study_id=study.id, title=study.title, abstract=study.abstract
    )
    storage.upsert_claims(study.id, claims, extractor="regex-v1")
    response = _json.dumps({"attributions": [
        {"question": "Stricter climate law", "position": "support",
         "percentage": 62, "population": None, "confidence": 0.9,
         "source_span": "62% befürworten ein Klimaschutzgesetz"},
    ]})
    apply_responses(storage=storage, responses={study.id: response})

    rows = storage.sample_attributions(limit=5)
    assert len(rows) == 1
    row = rows[0]
    for field in ("question", "position", "percentage", "confidence",
                  "raw", "title", "canonical_url", "source_id"):
        assert field in row
    assert row["raw"].get("source_span")
    assert row["raw"].get("grounded") is True
