"""Domain-audit source discovery (issue #38): `sources-audit` CLI +
the pure domain-normalization helper it's built on."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone

import pytest
from typer.testing import CliRunner

from study_scraper.cli import app
from study_scraper.domain_audit import KNOWN_DOMAINS, audit_domains, normalize_domain


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")


def _plain(text: str) -> str:
    """Strip ANSI escapes (rich forces terminal styling in CI)."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


# ---------------------------------------------------------------------------
# normalize_domain — pure, no DB
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://www.destatis.de/DE/Themen/foo.pdf", "destatis.de"),
        ("https://destatis.de/some/path", "destatis.de"),
        ("http://search.dip.bundestag.de/api/v1/drucksache", "bundestag.de"),
        ("https://doi.org/10.1234/abcd", "doi.org"),
        ("https://EXAMPLE.COM/path", "example.com"),
        ("https://user:pass@sub.example.org:8443/x", "example.org"),
        ("not a url", None),
        ("", None),
        (None, None),
        ("https://192.168.0.1/foo", None),
    ],
)
def test_normalize_domain(url, expected) -> None:
    assert normalize_domain(url) == expected


def test_known_domains_cover_existing_sources() -> None:
    for domain in ("openalex.org", "ssoar.info", "dawum.de", "gesis.org",
                    "govdata.de", "doi.org"):
        assert domain in KNOWN_DOMAINS


# ---------------------------------------------------------------------------
# CLI registration
# ---------------------------------------------------------------------------


def test_cli_registers_sources_audit_command() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["sources-audit", "--help"])
    assert result.exit_code == 0
    assert "--limit" in _plain(result.output)
    assert "domain" in _plain(result.output).lower()


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


def _clean_studies(storage) -> None:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
        conn.commit()


@pytestmark_integration
def test_audit_domains_surfaces_unknown_domain_ranked_by_frequency(storage) -> None:
    from study_scraper.models import Provenance, Study

    _clean_studies(storage)

    # Two studies landing on an unrecognized domain, one on a known
    # (already-sourced) domain — only the former should surface.
    for i in range(2):
        study = Study.build(
            canonical_url=f"https://doi.org/10.9999/unseen-{i}",
            title=f"Unseen study {i}",
            fetched_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            source_id="openalex",
            provenance=Provenance(
                discovery_source="openalex",
                landing_page_url=f"https://www.bundesbank.de/paper-{i}",
            ),
        )
        storage.upsert_study(study)

    known = Study.build(
        canonical_url="https://www.ssoar.info/ssoar/handle/x",
        title="Known-source study",
        fetched_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        source_id="ssoar",
        provenance=Provenance(discovery_source="ssoar"),
    )
    storage.upsert_study(known)

    stats = audit_domains(storage, limit=10)
    domains = {s.domain: s.hits for s in stats}
    assert domains.get("bundesbank.de") == 2
    assert "ssoar.info" not in domains
    assert "doi.org" not in domains


@pytestmark_integration
def test_sources_audit_cli_prints_unknown_domains(storage, monkeypatch) -> None:
    from study_scraper.models import Provenance, Study

    _clean_studies(storage)
    study = Study.build(
        canonical_url="https://www.example-gov-agency.de/report",
        title="Agency report",
        fetched_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        source_id="openalex",
        provenance=Provenance(discovery_source="openalex"),
    )
    storage.upsert_study(study)

    # study_scraper.config caches Settings() process-wide; reset it so
    # monkeypatch.setenv("POSTGRES_URL", ...) actually takes effect.
    import study_scraper.config as _cfg
    monkeypatch.setattr(_cfg, "_settings", None)
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)

    runner = CliRunner()
    result = runner.invoke(app, ["sources-audit"])
    assert result.exit_code == 0, result.output
    assert "example-gov-agency.de" in result.output
