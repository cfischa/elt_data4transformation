"""Unit tests for domain models."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from study_scraper.models import Provenance, Study, study_id_for


def make_provenance() -> Provenance:
    return Provenance(discovery_source="ssoar", discovery_query="klima")


def test_study_id_is_sha256_of_canonical_url() -> None:
    sid = study_id_for("https://example.org/study/42")
    assert len(sid) == 64
    assert sid.islower()
    # Deterministic
    assert sid == study_id_for("https://example.org/study/42")
    assert sid != study_id_for("https://example.org/study/43")


def test_study_id_for_empty_url_raises() -> None:
    with pytest.raises(ValueError):
        study_id_for("")


class TestStudy:
    def test_build_factory_derives_id_from_url(self) -> None:
        study = Study.build(
            canonical_url="https://example.org/x",
            title="A study",
            fetched_at=datetime(2026, 5, 5, tzinfo=timezone.utc),
            source_id="ssoar",
            provenance=make_provenance(),
        )
        assert study.id == study_id_for("https://example.org/x")
        assert study.canonical_url == "https://example.org/x"

    def test_naive_fetched_at_is_made_utc_aware(self) -> None:
        study = Study.build(
            canonical_url="https://example.org/x",
            title="t",
            fetched_at=datetime(2026, 5, 5),  # naive
            source_id="ssoar",
            provenance=make_provenance(),
        )
        assert study.fetched_at.tzinfo is timezone.utc

    def test_topic_scores_must_be_in_unit_interval(self) -> None:
        with pytest.raises(ValueError, match=r"outside \[0, 1\]"):
            Study.build(
                canonical_url="https://example.org/x",
                title="t",
                fetched_at=datetime(2026, 5, 5, tzinfo=timezone.utc),
                source_id="ssoar",
                provenance=make_provenance(),
                topic_scores={"klima": 1.5},
            )

    def test_canonical_url_whitespace_is_stripped(self) -> None:
        study = Study.build(
            canonical_url="  https://example.org/x  ",
            title="t",
            fetched_at=datetime(2026, 5, 5, tzinfo=timezone.utc),
            source_id="ssoar",
            provenance=make_provenance(),
        )
        assert study.canonical_url == "https://example.org/x"

    def test_optional_fields_default_safely(self) -> None:
        study = Study.build(
            canonical_url="https://example.org/x",
            title="t",
            fetched_at=datetime(2026, 5, 5, tzinfo=timezone.utc),
            source_id="ssoar",
            provenance=make_provenance(),
        )
        assert study.authors == []
        assert study.topic_ids == []
        assert study.topic_scores == {}
        assert study.has_quantitative_data is False
        assert study.publication_date is None
        assert study.survey_metadata is None
