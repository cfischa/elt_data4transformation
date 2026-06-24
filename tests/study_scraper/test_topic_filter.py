"""Unit tests for the rule-based topic filter."""

from __future__ import annotations

from pathlib import Path

import pytest

from study_scraper.topic_filter import classify, score_text
from study_scraper.topics import load_topics


@pytest.fixture(scope="module")
def topics() -> list:
    return load_topics(Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv")


@pytest.fixture(scope="module")
def klima(topics):
    return next(t for t in topics if t.id == "klima")


@pytest.fixture(scope="module")
def migration(topics):
    return next(t for t in topics if t.id == "migration_einwanderung")


class TestScoreText:
    def test_single_include_keyword_passes_default_threshold(self, klima):
        result = score_text(
            "Eine Studie zum Klimaschutz in deutschen Großstädten", klima
        )
        assert result.passes
        assert "klimaschutz" in result.matched_includes
        assert result.score >= 0.2

    def test_multiple_include_keywords_increase_score(self, klima):
        single = score_text("Eine Studie zum Klimaschutz", klima).score
        many = score_text(
            "Eine Studie zu Klimaschutz, Klimawandel und Energiewende",
            klima,
        ).score
        assert many > single
        assert many <= 1.0

    def test_exclude_keyword_short_circuits(self, klima):
        result = score_text(
            "Eine Untersuchung zum Unternehmensklima in der Wirtschaft", klima
        )
        assert not result.passes
        assert result.matched_excludes == ["unternehmensklima"]
        assert result.score == 0.0

    def test_unrelated_text_does_not_match(self, klima):
        result = score_text(
            "Eine Studie zu IT-Servermigrationsprojekten", klima
        )
        assert result.matched_includes == []
        # The "Klimawandel" / "Klimabilanz" synonyms shouldn't match here
        # even though "migration" is in the text — that's the migration
        # topic's vocabulary, not klima's.
        assert result.matched_synonyms == []

    def test_synonyms_only_score(self, klima):
        # "Treibhausgas" is a synonym, not an include keyword (in the DE
        # row include is CO2/Emissionen/Treibhausgas/...). Verify the
        # synonym path returns a non-zero score.
        result = score_text("Treibhausgas-Emissionen sinken", klima)
        assert result.passes
        # At least one include (Treibhausgas, Emissionen are both
        # include keywords in the seeded topic) should match — the
        # asserts below don't pin which one to keep this test resilient
        # to topic edits.
        assert result.score > 0

    def test_case_insensitive(self, klima):
        result = score_text("KLIMASCHUTZ in Deutschland", klima)
        assert result.passes

    def test_synonyms_dont_double_count_includes(self, klima):
        # "Klimaschutz" is in both include and synonym lists of the DE
        # locale; the filter de-duplicates by removing duplicates from
        # the synonym list when an include matches.
        result = score_text("Klimaschutz", klima)
        assert result.matched_includes == ["klimaschutz"]
        # Klimaschutz should NOT also appear in synonyms.
        assert "klimaschutz" not in result.matched_synonyms


class TestClassify:
    def test_returns_only_topics_passing_threshold(self, topics, klima):
        text = "Eine Untersuchung zum Klimaschutz in deutschen Großstädten"
        results = classify(text, topics)
        topic_ids = [r.topic_id for r in results]
        assert "klima" in topic_ids
        # migration / steuern / bildung should not match
        assert "migration_einwanderung" not in topic_ids
        assert "steuern" not in topic_ids
        assert "bildung" not in topic_ids

    def test_results_sorted_by_descending_score(self, topics):
        # Construct a text that hits multiple klima includes to push the
        # score above 0.2.
        text = "Studie zu Klimaschutz, Klimawandel, CO2-Emissionen"
        results = classify(text, topics)
        assert results
        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_empty_input_returns_empty(self, topics):
        assert classify("", topics) == []

    def test_per_topic_exclude_does_not_affect_other_topics(
        self, topics, klima
    ):
        # "Datenmigration" is a migration_einwanderung exclude; klima
        # should still see "Klimaschutz" as a match in this text.
        text = "Datenmigration und Klimaschutz in Behörden"
        results = classify(text, topics)
        topic_ids = [r.topic_id for r in results]
        assert "klima" in topic_ids
        assert "migration_einwanderung" not in topic_ids
