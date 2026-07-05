"""Monitoring v1 digest — the reader-facing artifact of each crawl.

For every active watch (a standing question registered with `watch
add`), each `digest` run:

  1. pulls the watch's current deduped findings,
  2. aggregates them (poll-of-polls, `aggregate.py`),
  3. compares against the watch's PREVIOUS snapshot:
       - **shift**: same (question-cluster, position) moved by at least
         `SHIFT_POINTS` percentage points,
       - **new question**: a cluster with no counterpart last time,
       - **new findings**: the finding count grew,
  4. stores the new snapshot and renders a Markdown section.

Cluster matching between runs is fuzzy (`question_similarity`) because
a cluster's label is its most common phrasing and can drift as new
polls arrive.

Delivery v1 is a Markdown file (committed / uploaded as a CI artifact
by the scheduled workflow) — zero new infrastructure, per the
product-expansion note. The digest never prints a lone number: shifts
carry both endpoints, aggregates carry poll counts.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from study_scraper.aggregate import ClusterAnswer, aggregate_findings
from study_scraper.clustering import DEFAULT_THRESHOLD, question_similarity

SHIFT_POINTS = 5.0


@dataclass
class WatchDigest:
    watch_id: int
    query: str
    label: str
    findings_count: int
    previous_count: int
    answers: List[ClusterAnswer] = field(repr=False)
    shifts: List[Dict[str, Any]] = field(default_factory=list)
    new_questions: List[str] = field(default_factory=list)
    first_run: bool = False

    @property
    def has_news(self) -> bool:
        return bool(
            self.shifts
            or self.new_questions
            or self.findings_count > self.previous_count
        )


def _flatten(answers: List[ClusterAnswer]) -> List[Dict[str, Any]]:
    """Snapshot payload: one row per (cluster, position)."""
    rows: List[Dict[str, Any]] = []
    for a in answers:
        for p in a.positions:
            rows.append(
                {
                    "cluster_label": a.label,
                    "position": p.position,
                    "weighted_pct": round(p.weighted_pct, 1),
                    "n_findings": p.n_findings,
                }
            )
    return rows


def _match_prev(
    label: str, position: str, prev_rows: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Find last run's row for this (cluster, position), label-fuzzy."""
    best: Tuple[float, Optional[Dict[str, Any]]] = (0.0, None)
    for row in prev_rows:
        if row.get("position") != position:
            continue
        sim = question_similarity(label, row.get("cluster_label") or "")
        if sim > best[0]:
            best = (sim, row)
    return best[1] if best[0] >= DEFAULT_THRESHOLD else None


def digest_watch(
    storage: Any,
    watch: Dict[str, Any],
    *,
    today: Optional[_dt.date] = None,
    save: bool = True,
) -> WatchDigest:
    """Compute one watch's digest and (by default) persist the snapshot."""
    rows = storage.search_attributions_deduped(
        query=watch["query"], limit=500, since=watch.get("since_year")
    )
    answers = aggregate_findings(rows, today=today)
    current = _flatten(answers)

    prev_snapshot = storage.latest_watch_snapshot(watch["id"])
    prev_rows = list(prev_snapshot["payload"]) if prev_snapshot else []
    prev_count = int(prev_snapshot["findings_count"]) if prev_snapshot else 0

    shifts: List[Dict[str, Any]] = []
    new_questions: List[str] = []
    seen_labels: set = set()
    for row in current:
        label = row["cluster_label"]
        prev = _match_prev(label, row["position"], prev_rows)
        if prev is None:
            if prev_snapshot is not None and label not in seen_labels:
                new_questions.append(label)
        else:
            delta = row["weighted_pct"] - float(prev["weighted_pct"])
            if abs(delta) >= SHIFT_POINTS:
                shifts.append(
                    {
                        "cluster_label": label,
                        "position": row["position"],
                        "from_pct": float(prev["weighted_pct"]),
                        "to_pct": row["weighted_pct"],
                        "delta": round(delta, 1),
                    }
                )
        seen_labels.add(label)

    if save:
        storage.save_watch_snapshot(
            watch["id"], findings_count=len(rows), payload=current
        )

    return WatchDigest(
        watch_id=watch["id"],
        query=watch["query"],
        label=watch.get("label") or watch["query"],
        findings_count=len(rows),
        previous_count=prev_count,
        answers=answers,
        shifts=shifts,
        new_questions=sorted(set(new_questions)),
        first_run=prev_snapshot is None,
    )


def run_digest(
    storage: Any, *, today: Optional[_dt.date] = None, save: bool = True
) -> List[WatchDigest]:
    """Digest every active watch, in watch-id order."""
    return [
        digest_watch(storage, watch, today=today, save=save)
        for watch in storage.list_watches()
    ]


def format_digest_markdown(
    digests: List[WatchDigest], *, generated_at: Optional[_dt.datetime] = None
) -> str:
    """The full digest as Markdown — the deliverable of a digest run."""
    from study_scraper.aggregate import format_answer

    ts = (generated_at or _dt.datetime.now(_dt.timezone.utc)).strftime(
        "%Y-%m-%d %H:%M UTC"
    )
    lines: List[str] = [f"# Opinion digest — {ts}", ""]
    if not digests:
        lines.append("_No active watches. Register one with `watch add`._")
        return "\n".join(lines) + "\n"

    for d in digests:
        lines.append(f"## {d.label}")
        lines.append("")
        if d.first_run:
            lines.append(
                f"_First digest for this watch — baseline snapshot of "
                f"{d.findings_count} finding(s) stored; shifts appear from "
                f"the next run._"
            )
        else:
            delta = d.findings_count - d.previous_count
            lines.append(
                f"{d.findings_count} finding(s) "
                f"({'+' if delta >= 0 else ''}{delta} since last digest)."
            )
        lines.append("")
        for s in d.shifts:
            arrow = "▲" if s["delta"] > 0 else "▼"
            lines.append(
                f"- **{arrow} shift**: *{s['cluster_label']}* ({s['position']}) "
                f"moved {s['from_pct']:.1f}% → {s['to_pct']:.1f}% "
                f"({s['delta']:+.1f} pts)"
            )
        for q in d.new_questions:
            lines.append(f"- **new question tracked**: *{q}*")
        if d.shifts or d.new_questions:
            lines.append("")
        if d.answers:
            lines.append("```")
            for a in d.answers[:10]:
                lines.append(format_answer(a))
                lines.append("")
            if lines[-1] == "":
                lines.pop()
            lines.append("```")
        else:
            lines.append("_(no findings with percentages yet)_")
        lines.append("")

    lines.append("---")
    lines.append(
        "_Method: weighted mean per question cluster; weights = recency "
        "(3-year half-life) × sqrt(n/1000) clamped to [0.3, 3]. Shifts "
        f"reported at ≥ {SHIFT_POINTS:.0f} points._"
    )
    return "\n".join(lines) + "\n"
