"""Research dossier + evidence-gap report (product-expansion Axis 2).

A dossier turns one policy question into what a researcher would
assemble by hand: the poll-of-polls aggregate per question cluster with
its spread, every underlying finding with year / n / population /
institute, honest methodology notes, and a provenance appendix linking
each cited study. Output is Markdown — citable by a journalist or NGO.

The gap report is the inverse product, unique to a coverage-first
store: per topic, which question clusters exist, how fresh and how
broadly sourced they are — and where the holes are (stale, single
source, no quantitative findings). It tells civil society what poll to
commission next, and the operator which sources to wire up.

Both are read-time renderers over the attribution layer; no migrations.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, List, Optional

from study_scraper.aggregate import aggregate_findings
from study_scraper.clustering import cluster_attributions
from study_scraper.findings import dedupe_attributions

_STALE_YEARS = 3
_METHOD_NOTE = (
    "Aggregates are weighted means per question cluster: weight = "
    "recency (3-year half-life on publication date) × sqrt(n/1000) "
    "clamped to [0.3, 3]; undated studies are heavily discounted. "
    "Question clustering is lexical-semantic (v1) and can over- or "
    "under-group; every underlying finding is listed so the grouping "
    "is checkable. Findings are machine-extracted (llm-v1) from study "
    "text with a verbatim evidence span; extraction errors are "
    "possible — follow the links before citing."
)


def _fmt_year(d: Optional[_dt.date]) -> str:
    return str(d.year) if d else "—"


def _fmt_n(n: Optional[float]) -> str:
    return f"{int(n):,}" if n is not None else "—"


def _institute(row: Dict[str, Any]) -> str:
    return row.get("publisher") or row.get("source_id") or "—"


def build_dossier(
    storage: Any,
    query: str,
    *,
    since: Optional[int] = None,
    today: Optional[_dt.date] = None,
) -> str:
    """Render the Markdown dossier for one query. Empty result → a
    short 'no findings' document (still valid Markdown)."""
    today = today or _dt.date.today()
    rows = storage.search_attributions_deduped(query=query, limit=500, since=since)
    lines: List[str] = [f"# Research dossier: “{query}”", ""]
    scope = f"findings from {since} onward" if since else "all ingested findings"
    lines.append(
        f"_Generated from the study-scraper attribution layer; {scope}._"
    )
    lines.append("")

    if not rows:
        lines.append("**No findings matched.** Either no ingested study answers")
        lines.append("this question, or the attribution pass hasn't covered it")
        lines.append("yet (`attribute` / see the evidence-gap report).")
        return "\n".join(lines) + "\n"

    answers = aggregate_findings(rows, today=today)

    lines.append("## Summary — what the polls say")
    lines.append("")
    for a in answers:
        lines.append(f"**{a.label}**")
        lines.append("")
        for p in a.positions:
            bits = [f"{p.n_findings} poll{'s' if p.n_findings != 1 else ''}"]
            if p.n_findings > 1:
                bits.append(f"spread {p.min_pct:.0f}–{p.max_pct:.0f}%")
            if p.year_min is not None:
                bits.append(
                    str(p.year_max) if p.year_min == p.year_max
                    else f"{p.year_min}–{p.year_max}"
                )
            if p.total_sample is not None:
                bits.append(f"Σn={p.total_sample:,}")
            lines.append(
                f"- {p.position}: **{p.weighted_pct:.1f}%** ({', '.join(bits)})"
            )
        lines.append("")

    lines.append("## Findings in detail")
    lines.append("")
    citations: List[Dict[str, Any]] = []
    seen_urls: set = set()
    for a in answers:
        lines.append(f"### {a.label}")
        lines.append("")
        lines.append("| % | position | year | n | population | institute / source | conf |")
        lines.append("|---|----------|------|---|------------|--------------------|------|")
        members = [m for p in a.positions for m in p.findings]
        members.sort(
            key=lambda m: (m.get("publication_date") or _dt.date.min),
            reverse=True,
        )
        for m in members:
            url = m.get("canonical_url")
            if url and url not in seen_urls:
                seen_urls.add(url)
                citations.append(m)
            ref = f"[{len(citations)}]" if url else "—"
            conf = m.get("confidence")
            conf_str = f"{float(conf):.2f}" if conf is not None else "—"
            lines.append(
                f"| {float(m['percentage']):.1f}% | {m.get('position')} "
                f"| {_fmt_year(m.get('publication_date'))} "
                f"| {_fmt_n(m.get('sample_size'))} "
                f"| {m.get('population') or '—'} "
                f"| {_institute(m)} {ref} "
                f"| {conf_str} |"
            )
        lines.append("")

    lines.append("## Method & caveats")
    lines.append("")
    lines.append(_METHOD_NOTE)
    lines.append("")

    lines.append("## Sources")
    lines.append("")
    for i, c in enumerate(citations, start=1):
        title = (c.get("title") or "(untitled)").replace("\n", " ")
        lines.append(
            f"{i}. {title} — {_institute(c)}, "
            f"{_fmt_year(c.get('publication_date'))}. "
            f"<{c.get('canonical_url')}>"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def build_policy_gap_report(
    storage: Any,
    *,
    topic: str,
    today: Optional[_dt.date] = None,
) -> str:
    """Opinion–policy gap (flagship, product Axis 4): juxtapose what
    the polls say on a topic with what parliament is doing on it.

    Opinion half: the topic's aggregated question clusters. Parliament
    half: ingested Bundestag DIP Drucksachen on the topic, newest
    first. v1 is a juxtaposition, not a per-question join — question →
    bill matching needs richer DIP coverage first; the renderer makes
    the pairing a human judgement with both halves on one page.
    """
    today = today or _dt.date.today()
    lines: List[str] = [f"# Opinion–policy gap: `{topic}`", ""]

    raw = storage.filter_attributions(topic=topic, limit=1000)
    rows = dedupe_attributions(raw)
    answers = aggregate_findings(rows, today=today)

    lines.append("## What the polls say")
    lines.append("")
    if not answers:
        lines.append("_(no aggregated findings for this topic yet — run the")
        lines.append("pipeline through `attribute`)_")
    else:
        for a in answers[:15]:
            lines.append(f"**{a.label}**")
            for p in a.positions:
                lines.append(
                    f"- {p.position}: **{p.weighted_pct:.1f}%** "
                    f"({p.n_findings} poll{'s' if p.n_findings != 1 else ''}"
                    + (
                        f", {p.year_min}–{p.year_max}"
                        if p.year_min and p.year_min != p.year_max
                        else (f", {p.year_max}" if p.year_max else "")
                    )
                    + ")"
                )
            lines.append("")

    lines.append("## What parliament is doing")
    lines.append("")
    docs = storage.list_studies(
        topic_id=topic, source_id="bundestag_dip", limit=30
    )
    if not docs:
        lines.append("_(no Bundestag DIP documents ingested for this topic —")
        lines.append("run `run --source bundestag_dip --topic " + topic + "`)_")
    else:
        docs.sort(
            key=lambda d: d.get("publication_date") or _dt.date.min,
            reverse=True,
        )
        for d in docs:
            prov = d.get("provenance") or {}
            typ = prov.get("drucksachetyp") or "Drucksache"
            nr = prov.get("dokumentnummer")
            nr_str = f" {nr}" if nr else ""
            title = (d.get("title") or "").replace("\n", " ")
            lines.append(
                f"- **{typ}{nr_str}** ({_fmt_year(d.get('publication_date'))}, "
                f"{d.get('publisher') or 'Deutscher Bundestag'}): {title}  "
            )
            lines.append(f"  <{d.get('canonical_url')}>")
        lines.append("")

    lines.append("---")
    lines.append(
        "_Read the two halves together: strong majorities with no "
        "parliamentary activity, or activity against the majority, are "
        "the gaps. Aggregation method as in the dossier; both halves "
        "carry provenance links._"
    )
    return "\n".join(lines) + "\n"


def build_gap_report(
    storage: Any,
    *,
    topic: Optional[str] = None,
    today: Optional[_dt.date] = None,
) -> str:
    """Render the Markdown evidence-gap report for one topic (or all)."""
    today = today or _dt.date.today()
    topics = [topic] if topic else storage.list_distinct_attribution_topics()
    lines: List[str] = ["# Evidence-gap report", ""]
    lines.append(
        "_Which questions have polling data, how fresh, how broadly "
        "sourced — and where the holes are. A gap is a reason to find "
        "another source or commission a poll, not a claim that no data "
        "exists anywhere._"
    )
    lines.append("")
    if not topics:
        lines.append("**No attributed topics yet** — run the pipeline through")
        lines.append("`attribute` first.")
        return "\n".join(lines) + "\n"

    stale_cut = today.year - _STALE_YEARS
    for t in topics:
        raw = storage.filter_attributions(topic=t, limit=1000)
        rows = dedupe_attributions(raw)
        lines.append(f"## Topic: `{t}`")
        lines.append("")
        if not rows:
            lines.append("- **gap: no attributed findings at all** — studies may")
            lines.append("  be ingested but nothing has reached the answer layer.")
            lines.append("")
            continue
        clustered = cluster_attributions(rows)
        by_cluster: Dict[int, List[Dict[str, Any]]] = {}
        for r in clustered:
            by_cluster.setdefault(r["cluster_id"], []).append(r)

        lines.append("| question cluster | findings | institutes | latest | gaps |")
        lines.append("|------------------|-----------|------------|--------|------|")
        for cid, members in sorted(
            by_cluster.items(), key=lambda kv: -len(kv[1])
        ):
            label = members[0]["cluster_label"]
            institutes = {_institute(m) for m in members}
            years = [
                m["publication_date"].year
                for m in members
                if m.get("publication_date")
            ]
            latest = max(years) if years else None
            with_pct = [m for m in members if m.get("percentage") is not None]
            gaps: List[str] = []
            if latest is None:
                gaps.append("undated")
            elif latest < stale_cut:
                gaps.append(f"stale (last {latest})")
            if len(institutes) == 1:
                gaps.append("single source")
            if not with_pct:
                gaps.append("no percentages")
            lines.append(
                f"| {label} | {len(members)} | {len(institutes)} "
                f"| {latest or '—'} | {', '.join(gaps) or '—'} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"
