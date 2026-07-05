"""Poll-of-polls aggregation (ROADMAP item D).

A real "what does Germany think about X" answer is not a list of single
findings — it aggregates across institutes: a recency- and sample-size-
weighted average per (question-cluster, position), with the spread shown
honestly. Non-partisanship rule from the product-expansion note: always
show spread and poll count, never a lone number.

Pure functions over deduped attribution rows (the same rows `ask`
displays); clustering comes from `clustering.py`. Read-time, no
migration — the raw audit trail is untouched.

Weights:
  - recency: half-life of 3 years on the study's publication date
    (a 2022 poll counts half a 2025 poll). Undated studies are treated
    as `_UNDATED_AGE_YEARS` old — included, but nearly voiceless.
  - sample size: sqrt(n / 1000) clamped to [0.3, 3.0]; unknown n = 1.0.
    sqrt because information grows sub-linearly in n; the clamp stops a
    single mega-panel from drowning every other institute.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from study_scraper.clustering import DEFAULT_THRESHOLD, cluster_attributions

_HALF_LIFE_YEARS = 3.0
_UNDATED_AGE_YEARS = 8.0
_REFERENCE_N = 1000.0


def _age_years(pub: Optional[_dt.date], today: _dt.date) -> float:
    if pub is None:
        return _UNDATED_AGE_YEARS
    if isinstance(pub, _dt.datetime):
        pub = pub.date()
    return max(0.0, (today - pub).days / 365.25)


def recency_weight(pub: Optional[_dt.date], today: _dt.date) -> float:
    return 0.5 ** (_age_years(pub, today) / _HALF_LIFE_YEARS)


def sample_weight(n: Optional[float]) -> float:
    if n is None or n <= 0:
        return 1.0
    return min(3.0, max(0.3, (float(n) / _REFERENCE_N) ** 0.5))


@dataclass
class PositionAggregate:
    """One (question-cluster, position) cell of the answer."""

    position: str
    weighted_pct: float
    min_pct: float
    max_pct: float
    n_findings: int
    year_min: Optional[int]
    year_max: Optional[int]
    total_sample: Optional[int]   # sum of known n; None if none known
    findings: List[Dict[str, Any]] = field(repr=False, default_factory=list)

    @property
    def spread(self) -> float:
        return self.max_pct - self.min_pct


@dataclass
class ClusterAnswer:
    """The aggregate answer for one question cluster."""

    label: str
    positions: List[PositionAggregate]
    n_findings: int

    def get(self, position: str) -> Optional[PositionAggregate]:
        for p in self.positions:
            if p.position == position:
                return p
        return None


def aggregate_findings(
    rows: List[Dict[str, Any]],
    *,
    today: Optional[_dt.date] = None,
    threshold: float = DEFAULT_THRESHOLD,
) -> List[ClusterAnswer]:
    """Cluster deduped attribution rows and aggregate per position.

    Rows without a percentage are skipped (they can't average). Clusters
    come back largest-first; positions within a cluster in the fixed
    support/oppose/neutral/unspecified order.
    """
    today = today or _dt.date.today()
    usable = [r for r in rows if r.get("percentage") is not None]
    if not usable:
        return []

    clustered = cluster_attributions(usable, threshold=threshold)
    groups: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
    labels: Dict[int, str] = {}
    for row in clustered:
        cid = row["cluster_id"]
        labels[cid] = row["cluster_label"]
        key = (cid, row.get("position") or "unspecified")
        groups.setdefault(key, []).append(row)

    by_cluster: Dict[int, List[PositionAggregate]] = {}
    for (cid, position), members in groups.items():
        w_sum = 0.0
        wx_sum = 0.0
        pcts: List[float] = []
        years: List[int] = []
        samples: List[int] = []
        for m in members:
            pct = float(m["percentage"])
            w = recency_weight(m.get("publication_date"), today) * sample_weight(
                m.get("sample_size")
            )
            w_sum += w
            wx_sum += w * pct
            pcts.append(pct)
            pub = m.get("publication_date")
            if pub is not None:
                years.append(pub.year)
            n = m.get("sample_size")
            if n is not None:
                samples.append(int(n))
        by_cluster.setdefault(cid, []).append(
            PositionAggregate(
                position=position,
                weighted_pct=wx_sum / w_sum if w_sum else 0.0,
                min_pct=min(pcts),
                max_pct=max(pcts),
                n_findings=len(members),
                year_min=min(years) if years else None,
                year_max=max(years) if years else None,
                total_sample=sum(samples) if samples else None,
                findings=members,
            )
        )

    position_order = {"support": 0, "oppose": 1, "neutral": 2, "unspecified": 3}
    answers: List[ClusterAnswer] = []
    for cid, positions in by_cluster.items():
        positions.sort(key=lambda p: position_order.get(p.position, 9))
        answers.append(
            ClusterAnswer(
                label=labels[cid],
                positions=positions,
                n_findings=sum(p.n_findings for p in positions),
            )
        )
    answers.sort(key=lambda a: -a.n_findings)
    return answers


def format_answer(answer: ClusterAnswer) -> str:
    """One cluster as human-readable CLI text."""
    lines = [f"Q: {answer.label}"]
    for p in answer.positions:
        bits = [f"{p.n_findings} poll{'s' if p.n_findings != 1 else ''}"]
        if p.n_findings > 1:
            bits.append(f"spread {p.min_pct:.0f}–{p.max_pct:.0f}%")
        if p.year_min is not None:
            years = (
                str(p.year_max)
                if p.year_min == p.year_max
                else f"{p.year_min}–{p.year_max}"
            )
            bits.append(years)
        if p.total_sample is not None:
            bits.append(f"Σn={p.total_sample:,}")
        lines.append(
            f"   {p.position:<11} {p.weighted_pct:5.1f}%   ({', '.join(bits)})"
        )
    return "\n".join(lines)
