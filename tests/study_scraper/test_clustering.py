"""Tests for semantic question clustering (ROADMAP E, pure; no DB)."""

from __future__ import annotations

from study_scraper.clustering import (
    DEFAULT_THRESHOLD,
    cluster_attributions,
    cluster_questions,
    question_similarity,
)


class TestSimilarity:
    def test_roadmap_example_de_en_nuclear(self) -> None:
        # The literal ROADMAP item E example.
        sim = question_similarity(
            "Atomausstieg rückgängig machen", "Return to nuclear power"
        )
        assert sim >= DEFAULT_THRESHOLD

    def test_same_topic_different_question_stays_apart(self) -> None:
        # Both climate — but 'stricter laws' and 'EU priority' are
        # different survey questions and must NOT merge.
        sim = question_similarity(
            "Stricter climate laws", "EU climate priority"
        )
        assert sim < DEFAULT_THRESHOLD

    def test_plural_and_phrasing_variants_cluster(self) -> None:
        sim = question_similarity(
            "Stricter climate laws", "Germany should tighten its climate law"
        )
        assert sim >= DEFAULT_THRESHOLD

    def test_umlaut_folding(self) -> None:
        assert question_similarity("Rückkehr zur Atomkraft", "ruckkehr zur atomkraft") == 1.0

    def test_unrelated_questions_zero(self) -> None:
        sim = question_similarity(
            "Higher pension level", "General speed limit on the Autobahn"
        )
        assert sim < 0.2

    def test_empty_question_no_crash(self) -> None:
        assert question_similarity("", "anything") == 0.0


class TestClusterQuestions:
    def test_deterministic_ids_in_first_appearance_order(self) -> None:
        qs = [
            "Return to nuclear power",
            "Stricter climate laws",
            "Atomausstieg rückgängig machen",   # joins cluster 0
            "Tighten the climate law",           # joins cluster 1
        ]
        assert cluster_questions(qs) == [0, 1, 0, 1]

    def test_singletons_get_own_cluster(self) -> None:
        qs = ["A pension question", "A housing question", "A defense question"]
        assert cluster_questions(qs) == [0, 1, 2]

    def test_empty_input(self) -> None:
        assert cluster_questions([]) == []

    def test_custom_embedder_backend(self) -> None:
        # Dense-vector path: everything identical → one cluster.
        def embedder(texts):
            return [[1.0, 0.0] for _ in texts]

        assert cluster_questions(["a", "b", "c"], embedder=embedder) == [0, 0, 0]


class TestClusterAttributions:
    def test_annotates_rows_with_cluster_and_label(self) -> None:
        rows = [
            {"question": "Return to nuclear power", "percentage": 55},
            {"question": "Atomausstieg rückgängig machen", "percentage": 61},
            {"question": "Return to nuclear power", "percentage": 65},
        ]
        out = cluster_attributions(rows)
        assert [r["cluster_id"] for r in out] == [0, 0, 0]
        # Most common phrasing wins the label.
        assert out[0]["cluster_label"] == "Return to nuclear power"
        # Raw rows not mutated.
        assert "cluster_id" not in rows[0]

    def test_label_tie_prefers_shortest(self) -> None:
        rows = [
            {"question": "Tighten the existing climate law"},
            {"question": "Stricter climate laws"},
        ]
        out = cluster_attributions(rows)
        assert out[0]["cluster_id"] == out[1]["cluster_id"]
        assert out[0]["cluster_label"] == "Stricter climate laws"
