"""Source kind classification + per-source run stats used by
`pages/5_Sources.py`.

Extracted so unit/integration tests can exercise this logic without
importing the Streamlit page or `_shared.py` (which imports streamlit --
see `_csv.py` for the same pattern).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from study_scraper.status import CATALOG_SOURCE_IDS, LAKE_SOURCE_IDS, SCHEMA, run_is_clean
from study_scraper.storage import PostgresStorage

# Known source kinds: "catalog" sources (study_scraper/discovery/*.py) write
# topic-filtered candidates to `studies`; "lake" sources
# (study_scraper/sources/*.py) write raw payloads to `source_records`
# (issue #123 -- this dict previously only covered the first 5 sources and
# silently misclassified everything added after it as "?"). The registry
# itself lives in `status.py` (single source of truth, also used to compute
# `never_run_sources`); kept in sync by
# tests/study_scraper/test_console.py::test_source_kind_covers_all_sources,
# which walks both packages and fails if a new source_id isn't listed there.
CATALOG_SOURCES = CATALOG_SOURCE_IDS
LAKE_SOURCES = LAKE_SOURCE_IDS


def source_kind(source_id: str) -> str:
    """Classify a source_id as 'catalog' (-> studies) or 'lake' (-> source_records).

    Returns '?' for an unrecognized source_id.
    """
    if source_id in CATALOG_SOURCES:
        return "catalog"
    if source_id in LAKE_SOURCES:
        return "lake"
    return "?"


def days_since(timestamp: Optional[datetime], *, now: Optional[datetime] = None) -> Optional[float]:
    """Days elapsed since `timestamp`, or None if `timestamp` is None.

    Same "days since last clean run" computation `status.py::build_status`
    does for the Home page's staleness table -- the Sources page's per-
    source table (`5_Sources.py`) previously only showed the raw "last
    successful run" timestamp, leaving an operator to do the date math by
    hand on the one page purpose-built for per-source health, while the
    Home page already surfaced the same signal as a sorted "days since"
    number. `now` is injectable for tests; defaults to the real current time.
    """
    if timestamp is None:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    return (now - timestamp).total_seconds() / 86400.0


def per_source_run_stats(storage: PostgresStorage) -> Dict[str, Dict[str, Any]]:
    """Per-source last-clean-run timestamp, last-attempted-run timestamp,
    and cumulative error count.

    Uses the same "clean run" definition as `status.py::build_status`'s
    `source_days_since_last_success` (errors = 0 AND not aborted) --
    a bare `errors = 0` filter (this page's query before #178) misreads a
    run that raised out of `iter_candidates` (e.g. an unhandled 401,
    `finished_at` NULL, `notes` starting `aborted:`, per #48/#106) as
    successful, showing a stale/aborted run's timestamp as the source's
    "last successful run" instead of the actual last clean run (or
    "never").
    """
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT source_id,
                       MAX(started_at) FILTER (
                           WHERE errors = 0
                             AND NOT (finished_at IS NULL
                                       AND COALESCE(notes, '') LIKE 'aborted:%')
                       )                         AS last_ok,
                       MAX(started_at)            AS last_run,
                       COALESCE(SUM(errors), 0)   AS total_errors
                FROM   {SCHEMA}.crawl_runs
                GROUP  BY source_id
                """
            )
            return {row["source_id"]: dict(row) for row in cur.fetchall()}


def recent_run_rows(recent_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Shape `StatusReport.recent_runs` for `pages/5_Sources.py`'s
    "recent runs" table, using `status.run_is_clean` for the `ok`/`ERR`
    flag -- that table previously computed it with a bare `errors > 0`
    check, so a run that aborted out of `iter_candidates` (errors=0,
    finished_at NULL, notes starting `aborted:`, the #48/#106 401 shape)
    rendered as `ok` here even after #178 fixed the identical bug in this
    same page's per-source summary table above it (see #180).
    """
    return [
        {
            "ok": "ok" if run_is_clean(r) else "ERR",
            "source": r.get("source_id"),
            "topic": r.get("topic_id"),
            "seen": r.get("candidates_seen"),
            "kept": r.get("candidates_kept"),
            "errors": r.get("errors"),
            "started": (
                r["started_at"].isoformat(timespec="seconds")
                if r.get("started_at")
                else ""
            ),
        }
        for r in recent_runs
    ]
