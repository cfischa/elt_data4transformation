"""Accuracy fix F6: the topic taxonomy must catch common German inflected /
stem forms (recall). Pure-function tests over the real topics.csv.

NOTE: substring keyword matching is a sandbox-safe partial. Full
morphological coverage (lemmatization) needs spaCy + a gold set — flagged
for the maintainer. These tests lock in the variants we added and guard
that exclude keywords still short-circuit."""

from __future__ import annotations

from pathlib import Path

import pytest

from study_scraper.topic_filter import score_text
from study_scraper.topics import load_topics


TOPICS_CSV = Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
TOPICS = {t.id: t for t in load_topics(TOPICS_CSV)}


@pytest.mark.parametrize(
    "topic_id, text",
    [
        ("migration_einwanderung", "Eine Studie über Zuwanderer in Deutschland."),
        ("migration_einwanderung", "Die Zahl der Migranten ist gestiegen."),
        ("migration_einwanderung", "Geflüchtete und Flüchtlinge im Fokus."),
        ("klima", "Die Emission von CO2 sinkt leicht."),
        ("klima", "Photovoltaik und Windkraft im Ausbau."),
        ("bildung", "Studierende an der Hochschule befragt."),
        ("bildung", "Die Ausbildung junger Menschen."),
        ("steuern", "Die Gewerbesteuer und der Steuersatz steigen."),
        ("atomkraft", "Umfrage zum Atomausstieg und zur Kernenergie."),
        ("atomkraft", "Mehrheit lehnt neue Atomkraftwerke ab."),
        ("atomkraft", "Streit ums Endlager für Atommüll."),
    ],
)
def test_inflected_form_now_matches(topic_id: str, text: str) -> None:
    result = score_text(text, TOPICS[topic_id])
    assert result.passes, f"{topic_id!r} should match {text!r} ({result.rationale})"
    assert result.score > 0.0


@pytest.mark.parametrize(
    "topic_id, text",
    [
        ("migration_einwanderung", "Servermigration im Rechenzentrum."),
        ("migration_einwanderung", "Datenmigration auf neue Hardware."),
        ("steuern", "Die Steuerung des Prozesses."),
        ("klima", "Das Investitionsklima der Firma."),
        ("atomkraft", "Geschichte der Atomwaffe im Kalten Krieg."),
    ],
)
def test_exclude_keywords_still_short_circuit(topic_id: str, text: str) -> None:
    result = score_text(text, TOPICS[topic_id])
    assert not result.passes
    assert result.matched_excludes


def test_all_four_topics_present() -> None:
    assert {"klima", "steuern", "bildung", "migration_einwanderung"} <= set(TOPICS)
