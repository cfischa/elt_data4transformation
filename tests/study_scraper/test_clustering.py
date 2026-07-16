"""Tests for semantic question clustering (ROADMAP E, pure; no DB)."""

from __future__ import annotations

from study_scraper.clustering import (
    DEFAULT_THRESHOLD,
    cluster_attributions,
    cluster_questions,
    question_similarity,
    semantic_filter,
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
        sim = question_similarity("Rückkehr zur Atomkraft", "ruckkehr zur atomkraft")
        assert abs(sim - 1.0) < 1e-9

    def test_unrelated_questions_zero(self) -> None:
        sim = question_similarity(
            "Higher pension level", "General speed limit on the Autobahn"
        )
        assert sim < 0.2

    def test_empty_question_no_crash(self) -> None:
        assert question_similarity("", "anything") == 0.0


class TestOverMergeRegressions:
    def test_build_new_plants_distinct_from_general_use(self) -> None:
        # Real-data regression (2026-07-05): 32% (build new plants) and
        # 65% (use nuclear power) merged into a meaningless 48.5%.
        sim = question_similarity(
            "Use nuclear power", "Build new nuclear power plants"
        )
        assert sim < DEFAULT_THRESHOLD

    def test_kraftwerk_maps_to_plant_cross_language(self) -> None:
        sim = question_similarity(
            "Neubau neuer Kernkraftwerke", "Build new nuclear power plants"
        )
        assert sim >= DEFAULT_THRESHOLD

    def test_distinct_climate_questions_stay_apart(self) -> None:
        # Real-data regression (2026-07-05): at concept weight 3, nine
        # different climate questions merged into one mushy cluster.
        pairs = [
            ("Ambitious climate protection policy",
             "Climate protection is an important political task"),
            ("Ambitious climate protection policy",
             "Politics must implement more climate protection measures"),
            ("Willingness to reduce household energy consumption",
             "Ambitious climate protection policy"),
        ]
        for a, b in pairs:
            assert question_similarity(a, b) < DEFAULT_THRESHOLD, (a, b)


class TestSemanticFilter:
    def test_german_query_finds_english_questions(self) -> None:
        # `ask klimaschutzgesetz` returned nothing under ILIKE because
        # llm-v1 normalizes questions to English (found 2026-07-05).
        rows = [
            {"question": "Stricter climate protection laws", "percentage": 44},
            {"question": "Higher pension level", "percentage": 70},
        ]
        out = semantic_filter("klimaschutzgesetz", rows)
        assert [r["question"] for r in out] == ["Stricter climate protection laws"]

    def test_ranked_best_first(self) -> None:
        rows = [
            {"question": "Ambitious climate protection policy"},
            {"question": "Stricter climate protection laws"},
        ]
        out = semantic_filter("klimagesetz", rows)
        assert out[0]["question"] == "Stricter climate protection laws"

    def test_unrelated_rows_dropped(self) -> None:
        rows = [{"question": "General speed limit"}]
        assert semantic_filter("klimaschutzgesetz", rows) == []


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


class TestSemanticFilterAliases:
    """Registry recall aliases: 'a|b' queries score rows by their BEST
    alternative. The founding case: 'conscription' shares zero tokens
    with 'reintroduce compulsory military service' — without the alias
    the registered question reported a coverage gap over real data."""

    ROWS = [
        {"question": "Reintroduce compulsory military service", "percentage": 58},
        {"question": "Introduce a speed limit on the Autobahn", "percentage": 52},
    ]

    def test_seed_alone_misses_the_paraphrase(self) -> None:
        # Documents the failure mode the aliases exist to fix.
        assert semantic_filter("conscription", self.ROWS) == []

    def test_alias_recovers_it(self) -> None:
        out = semantic_filter("conscription|military service", self.ROWS)
        assert [r["question"] for r in out] == [
            "Reintroduce compulsory military service"
        ]

    def test_unrelated_rows_stay_out(self) -> None:
        out = semantic_filter("rent control|rent cap|mietpreisbremse", self.ROWS)
        assert out == []

    def test_blank_query_matches_nothing(self) -> None:
        assert semantic_filter("|", self.ROWS) == []

    def test_single_alt_unchanged(self) -> None:
        rows = [{"question": "Stricter climate laws"}]
        assert semantic_filter("klimagesetz", rows) == rows


class TestPolarityGuards:
    """Opposite propositions must NOT share a cluster (their 'support'
    means would average into nonsense), while REVERSAL phrasings of the
    same proposition must stay together."""

    def test_keep_vs_phase_out_split(self) -> None:
        rows = [
            {"question": "Keep nuclear power", "percentage": 55},
            {"question": "Phase out nuclear power", "percentage": 61},
        ]
        out = cluster_attributions(rows)
        assert out[0]["cluster_id"] != out[1]["cluster_id"]

    def test_reversal_phrasing_stays_together(self) -> None:
        rows = [
            {"question": "Return to nuclear power", "percentage": 55},
            {"question": "Atomausstieg rückgängig machen", "percentage": 61},
        ]
        out = cluster_attributions(rows)
        assert out[0]["cluster_id"] == out[1]["cluster_id"]
