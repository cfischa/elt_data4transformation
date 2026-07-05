"""Tests for the research dossier + evidence-gap report (pure; no DB)."""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from study_scraper.dossier import (
    build_dossier,
    build_gap_report,
    build_policy_gap_report,
)

TODAY = datetime.date(2026, 7, 1)


def _finding(q, pos, pct, *, date=None, n=None, publisher="Forsa",
             url="https://example.org/a", title="Study A", topic="klima"):
    return {
        "question": q, "position": pos, "percentage": pct,
        "publication_date": date, "sample_size": n, "confidence": 0.9,
        "publisher": publisher, "canonical_url": url, "title": title,
        "source_id": "openalex", "topic_ids": [topic], "raw": {},
    }


class FakeStorage:
    def __init__(self, findings: List[Dict[str, Any]],
                 dip_docs: Optional[List[Dict[str, Any]]] = None) -> None:
        self.findings = findings
        self.dip_docs = dip_docs or []

    def list_studies(self, *, topic_id=None, source_id=None, status="kept",
                     limit=100):
        assert source_id == "bundestag_dip"
        return list(self.dip_docs)

    def search_attributions_deduped(self, *, query: str, limit: int = 500,
                                    since: Optional[int] = None):
        rows = [f for f in self.findings if query.lower() in f["question"].lower()]
        if since is not None:
            rows = [r for r in rows
                    if r.get("publication_date")
                    and r["publication_date"].year >= since]
        return rows[:limit]

    def filter_attributions(self, *, topic=None, limit=1000, **kw):
        return [f for f in self.findings if topic in (f.get("topic_ids") or [])]

    def list_distinct_attribution_topics(self):
        out = sorted({t for f in self.findings for t in f.get("topic_ids") or []})
        return out


class TestDossier:
    def test_full_dossier_structure(self) -> None:
        store = FakeStorage([
            _finding("Stricter climate laws", "support", 62,
                     date=datetime.date(2021, 6, 1), n=1009,
                     publisher="Forsa", url="https://example.org/forsa",
                     title="Forsa Klima"),
            _finding("Stricter climate laws", "support", 44,
                     date=datetime.date(2022, 3, 1), n=6063,
                     publisher="Ariadne", url="https://example.org/ariadne",
                     title="Ariadne Panel"),
        ])
        md = build_dossier(store, "climate", today=TODAY)
        assert "# Research dossier: “climate”" in md
        assert "## Summary — what the polls say" in md
        assert "2 polls" in md and "spread 44–62%" in md
        assert "## Findings in detail" in md
        assert "| 44.0% | support | 2022 | 6,063 |" in md
        assert "## Method & caveats" in md
        assert "## Sources" in md
        assert "<https://example.org/forsa>" in md
        # newest-first in the detail table
        assert md.index("44.0%") < md.index("62.0%")

    def test_empty_dossier_says_so(self) -> None:
        md = build_dossier(FakeStorage([]), "nothing", today=TODAY)
        assert "No findings matched" in md

    def test_since_filter_propagates(self) -> None:
        store = FakeStorage([
            _finding("Stricter climate laws", "support", 44,
                     date=datetime.date(2020, 1, 1)),
        ])
        md = build_dossier(store, "climate", since=2024, today=TODAY)
        assert "No findings matched" in md

    def test_citation_dedup_across_findings(self) -> None:
        # Two findings from the SAME study → one Sources entry.
        store = FakeStorage([
            _finding("Re-enter nuclear energy", "support", 55,
                     date=datetime.date(2025, 3, 1)),
            _finding("Re-enter nuclear energy", "oppose", 36,
                     date=datetime.date(2025, 3, 1)),
        ])
        md = build_dossier(store, "nuclear", today=TODAY)
        assert md.count("<https://example.org/a>") == 1


class TestGapReport:
    def test_flags_stale_and_single_source(self) -> None:
        store = FakeStorage([
            _finding("Old lonely question", "support", 50,
                     date=datetime.date(2020, 1, 1)),
        ])
        md = build_gap_report(store, topic="klima", today=TODAY)
        assert "## Topic: `klima`" in md
        assert "stale (last 2020)" in md
        assert "single source" in md

    def test_fresh_multi_source_cluster_has_no_gaps(self) -> None:
        store = FakeStorage([
            _finding("Stricter climate laws", "support", 62,
                     date=datetime.date(2026, 1, 1), publisher="Forsa",
                     url="https://example.org/1"),
            _finding("Stricter climate laws", "support", 58,
                     date=datetime.date(2025, 6, 1), publisher="Civey",
                     url="https://example.org/2"),
        ])
        md = build_gap_report(store, topic="klima", today=TODAY)
        row = next(l for l in md.splitlines() if "Stricter climate laws" in l)
        assert row.rstrip().endswith("| — |")

    def test_topic_without_findings_is_a_gap(self) -> None:
        md = build_gap_report(FakeStorage([]), topic="rente", today=TODAY)
        assert "no attributed findings at all" in md

    def test_all_topics_mode_lists_each(self) -> None:
        store = FakeStorage([
            _finding("Q1 about climate", "support", 60, topic="klima",
                     date=datetime.date(2026, 1, 1)),
            _finding("Q2 about pension", "support", 70, topic="rente",
                     date=datetime.date(2026, 1, 1)),
        ])
        md = build_gap_report(store, today=TODAY)
        assert "## Topic: `klima`" in md and "## Topic: `rente`" in md


class TestPolicyGap:
    def _dip_doc(self, title, *, typ="Antrag", nr="21/4521",
                 date_=datetime.date(2025, 11, 12)):
        return {
            "title": title, "publisher": "Fraktion X",
            "publication_date": date_,
            "canonical_url": "https://dserver.bundestag.de/btd/x.pdf",
            "provenance": {"drucksachetyp": typ, "dokumentnummer": nr},
        }

    def test_juxtaposes_opinion_and_parliament(self) -> None:
        store = FakeStorage(
            [_finding("Stricter climate laws", "support", 62,
                      date=datetime.date(2025, 6, 1))],
            dip_docs=[self._dip_doc("Klimaschutzgesetz konsequent umsetzen")],
        )
        md = build_policy_gap_report(store, topic="klima", today=TODAY)
        assert "# Opinion–policy gap: `klima`" in md
        assert "## What the polls say" in md
        assert "support: **62.0%**" in md
        assert "## What parliament is doing" in md
        assert "**Antrag 21/4521** (2025, Fraktion X)" in md
        assert "dserver.bundestag.de" in md

    def test_empty_halves_have_hints(self) -> None:
        md = build_policy_gap_report(
            FakeStorage([], dip_docs=[]), topic="rente", today=TODAY
        )
        assert "no aggregated findings" in md
        assert "no Bundestag DIP documents" in md
        assert "run `run --source bundestag_dip --topic rente`" in md
