"""Unit tests for the topics CSV loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from study_scraper.topics import (
    Topic,
    TopicCSVError,
    load_topics,
)


CSV_HEADER = (
    "id,name,description,language,include_keywords,exclude_keywords,synonyms\n"
)


def write_csv(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "topics.csv"
    path.write_text(CSV_HEADER + body, encoding="utf-8")
    return path


class TestLoadTopics:
    def test_loads_seeded_repo_csv(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        topics = load_topics(repo_root / "config" / "topics" / "topics.csv")
        ids = {t.id for t in topics}
        # Core topics must be present; new topics (e.g. atomkraft,
        # added 2026-06-28) may join over time.
        assert {"steuern", "klima", "migration_einwanderung", "bildung",
                "atomkraft"} <= ids
        for topic in topics:
            assert "de" in topic.locales
            assert "en" in topic.locales

    def test_groups_rows_by_id(self, tmp_path: Path) -> None:
        path = write_csv(
            tmp_path,
            'foo,Foo DE,Beschreibung,de,a|b,x,y\n'
            'foo,Foo EN,description,en,a|c,,z\n',
        )
        topics = load_topics(path)
        assert len(topics) == 1
        assert topics[0].id == "foo"
        assert set(topics[0].locales) == {"de", "en"}
        assert topics[0].locales["de"].include_keywords == ["a", "b"]
        assert topics[0].locales["en"].include_keywords == ["a", "c"]

    def test_pipe_separated_lists_strip_whitespace(self, tmp_path: Path) -> None:
        path = write_csv(
            tmp_path, 'foo,Name,Desc,de, a | b |  ,,\n'
        )
        topics = load_topics(path)
        assert topics[0].locales["de"].include_keywords == ["a", "b"]

    def test_empty_keyword_columns_are_empty_lists(self, tmp_path: Path) -> None:
        path = write_csv(tmp_path, "foo,Name,Desc,de,,,\n")
        topics = load_topics(path)
        loc = topics[0].locales["de"]
        assert loc.include_keywords == []
        assert loc.exclude_keywords == []
        assert loc.synonyms == []

    def test_duplicate_locale_for_same_topic_is_an_error(
        self, tmp_path: Path
    ) -> None:
        path = write_csv(
            tmp_path,
            "foo,Name,Desc,de,a,,\n"
            "foo,Name2,Desc2,de,b,,\n",
        )
        with pytest.raises(TopicCSVError, match="duplicate locale"):
            load_topics(path)

    def test_missing_columns_is_an_error(self, tmp_path: Path) -> None:
        bad = tmp_path / "topics.csv"
        bad.write_text("id,name\nfoo,Bar\n", encoding="utf-8")
        with pytest.raises(TopicCSVError, match="missing required columns"):
            load_topics(bad)

    def test_missing_file_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_topics(tmp_path / "does-not-exist.csv")

    def test_empty_topic_id_is_an_error(self, tmp_path: Path) -> None:
        path = write_csv(tmp_path, ",Name,Desc,de,a,,\n")
        with pytest.raises(TopicCSVError, match="empty topic id"):
            load_topics(path)

    def test_empty_language_is_an_error(self, tmp_path: Path) -> None:
        path = write_csv(tmp_path, "foo,Name,Desc,,a,,\n")
        with pytest.raises(TopicCSVError, match="empty language"):
            load_topics(path)


class TestLoadTopicsRotation:
    """Issue #71: scrape.yml's per-topic crawl loop takes its order
    straight from load_topics(), so a fixed CSV order always puts the
    same topics last and a rate-limited source always starves them.
    Rotating by GITHUB_RUN_NUMBER (always set by Actions) fixes this
    without touching the workflow file, which is out of agent scope --
    and, unlike an earlier day-based revision of this fix, never fires
    outside Actions, so it can't corrupt the console's Topics editor
    (which reads via load_topics() and writes back in that same order,
    see TestLoadTopicsRotationConsoleSafety below)."""

    def _four_topic_csv(self, tmp_path: Path) -> Path:
        return write_csv(
            tmp_path,
            "a,A,d,de,,,\n"
            "b,B,d,de,,,\n"
            "c,C,d,de,,,\n"
            "d,D,d,de,,,\n",
        )

    def test_no_rotation_outside_github_actions(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GITHUB_RUN_NUMBER", raising=False)
        path = self._four_topic_csv(tmp_path)
        topics = load_topics(path)
        assert [t.id for t in topics] == ["a", "b", "c", "d"]

    def test_rotates_by_run_number_inside_github_actions(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = self._four_topic_csv(tmp_path)

        monkeypatch.setenv("GITHUB_RUN_NUMBER", "0")
        assert [t.id for t in load_topics(path)] == ["a", "b", "c", "d"]

        monkeypatch.setenv("GITHUB_RUN_NUMBER", "1")
        assert [t.id for t in load_topics(path)] == ["b", "c", "d", "a"]

        monkeypatch.setenv("GITHUB_RUN_NUMBER", "3")
        assert [t.id for t in load_topics(path)] == ["d", "a", "b", "c"]

        # wraps around
        monkeypatch.setenv("GITHUB_RUN_NUMBER", "5")
        assert [t.id for t in load_topics(path)] == ["b", "c", "d", "a"]

    def test_rotation_never_drops_or_duplicates_a_topic(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = self._four_topic_csv(tmp_path)
        monkeypatch.setenv("GITHUB_RUN_NUMBER", "137")
        topics = load_topics(path)
        assert sorted(t.id for t in topics) == ["a", "b", "c", "d"]

    def test_single_topic_csv_is_unaffected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GITHUB_RUN_NUMBER", "42")
        path = write_csv(tmp_path, "foo,Name,Desc,de,a,,\n")
        topics = load_topics(path)
        assert [t.id for t in topics] == ["foo"]


class TestLoadTopicsRotationConsoleSafety:
    """Regression test for the bug an adversarial review caught in an
    earlier (day-based, unconditional) revision of this fix: the
    console's Topics editor reads via load_topics() and writes topics
    back to disk in that exact order on save. Since Streamlit is a
    human-launched process, GITHUB_RUN_NUMBER is never set there, so
    load_topics() must return the literal CSV order in that context."""

    def test_load_topics_matches_csv_order_when_not_in_actions(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GITHUB_RUN_NUMBER", raising=False)
        path = write_csv(
            tmp_path,
            "a,A,d,de,,,\n"
            "b,B,d,de,,,\n"
            "c,C,d,de,,,\n",
        )
        # Simulates console/pages/1_Topics.py's save path: read, then
        # write back in the same order load_topics() returned.
        topics = load_topics(path)
        ordered_ids = [t.id for t in topics]
        assert ordered_ids == ["a", "b", "c"]


class TestTopicHelpers:
    def test_primary_name_prefers_de(self) -> None:
        topic = Topic.model_validate(
            {
                "id": "foo",
                "locales": {
                    "en": {"name": "EN", "description": "d"},
                    "de": {"name": "DE", "description": "d"},
                },
            }
        )
        assert topic.primary_name == "DE"

    def test_primary_name_falls_back_to_first_locale(self) -> None:
        topic = Topic.model_validate(
            {
                "id": "foo",
                "locales": {
                    "fr": {"name": "FR", "description": "d"},
                },
            }
        )
        assert topic.primary_name == "FR"

    def test_all_keywords_dedupes_across_locales(self) -> None:
        topic = Topic.model_validate(
            {
                "id": "foo",
                "locales": {
                    "de": {
                        "name": "DE",
                        "description": "d",
                        "include_keywords": ["A", "b"],
                        "synonyms": ["c"],
                    },
                    "en": {
                        "name": "EN",
                        "description": "d",
                        "include_keywords": ["a", "d"],
                    },
                },
            }
        )
        # case-folded, deduped, order preserved across locales
        assert topic.all_keywords() == ["a", "b", "c", "d"]
