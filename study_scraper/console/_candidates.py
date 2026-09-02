"""Row-shaping for the reference-follower candidate-studies queue, used by
`pages/6_Candidate_Sources.py`.

Extracted so unit tests can exercise it without importing the Streamlit
page or `_shared.py` (which imports streamlit -- see `_sources.py` for the
same pattern).
"""

from __future__ import annotations

from typing import Iterable, List, TypedDict


class PendingReferenceRow(TypedDict):
    openalex_id: str
    url: str


def pending_reference_rows(work_ids: Iterable[str]) -> List[PendingReferenceRow]:
    """Shape `follow.pending_references()` output for a dataframe: the
    full OpenAlex URL (used as the id everywhere else in the DB) plus a
    short id (`W123...`) for a readable column."""
    return [
        {"openalex_id": work_id.rsplit("/", 1)[-1], "url": work_id}
        for work_id in work_ids
    ]
