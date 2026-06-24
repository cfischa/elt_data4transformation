"""Reference follower — Phase 5d step 2 (builds on A18's capture).

Every OpenAlex study carries `referenced_works[]` / `related_works[]`
IDs in its provenance (A18). This module walks that citation graph one
hop: collect the IDs we have NOT ingested yet, then (live mode) fetch
them through the existing OpenAlex source and run them through the
normal topic-filtered pipeline.

Coverage rationale (A12): each relevant study's bibliography is a
curated list of more candidate studies — a recall booster we get for
free from data we already store.

Over-fetching is safe: ingest is idempotent, and DOI/title dedup
(migrations 0004/0006) collapse anything we already had under another
URL. "Already ingested" matching is best-effort by design.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from study_scraper.models import CrawlRun
from study_scraper.storage import PostgresStorage
from study_scraper.topics import Topic


LOGGER = logging.getLogger(__name__)

# OpenAlex allows up to ~50 OR-joined values in one filter expression.
FETCH_BATCH = 50


def pending_references(
    storage: PostgresStorage, *, limit: int = 200
) -> List[str]:
    """OpenAlex work IDs referenced by our studies but not yet ingested.

    "Not yet ingested" = the ID is neither any study's canonical_url
    nor any study's provenance openalex_id. Rejected studies still
    count as ingested (a human said no; don't re-suggest).
    """
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH cited AS (
                    SELECT DISTINCT jsonb_array_elements_text(
                               provenance->'referenced_works'
                           ) AS work_id
                    FROM   study_scraper.studies
                    WHERE  provenance ? 'referenced_works'
                    UNION
                    SELECT DISTINCT jsonb_array_elements_text(
                               provenance->'related_works'
                           ) AS work_id
                    FROM   study_scraper.studies
                    WHERE  provenance ? 'related_works'
                ),
                known AS (
                    SELECT canonical_url AS work_id
                    FROM   study_scraper.studies
                    UNION
                    SELECT provenance->>'openalex_id'
                    FROM   study_scraper.studies
                    WHERE  provenance ? 'openalex_id'
                )
                SELECT c.work_id
                FROM   cited c
                WHERE  c.work_id LIKE 'https://openalex.org/%%'
                  AND  NOT EXISTS (
                           SELECT 1 FROM known k
                           WHERE  k.work_id = c.work_id
                       )
                ORDER  BY c.work_id
                LIMIT  %s
                """,
                (limit,),
            )
            return [row["work_id"] for row in cur.fetchall()]


def fetch_references(
    *,
    storage: PostgresStorage,
    topic: Topic,
    limit: int = 100,
    min_score: float = 0.2,
) -> List[CrawlRun]:
    """Live mode: fetch pending referenced works via the OpenAlex API
    and run them through the normal topic-filtered pipeline.

    Each batch becomes one crawl run. Network required — runs on the
    maintainer's machine, not the sandbox.
    """
    from study_scraper.discovery.openalex import OpenAlexSource
    from study_scraper.pipeline import run_one

    ids = pending_references(storage, limit=limit)
    if not ids:
        LOGGER.info("reference follower: nothing pending")
        return []

    runs: List[CrawlRun] = []
    for start in range(0, len(ids), FETCH_BATCH):
        batch = ids[start:start + FETCH_BATCH]
        with OpenAlexSource(work_ids=batch) as src:
            run = run_one(
                source=src, topic=topic, storage=storage,
                min_score=min_score,
            )
        runs.append(run)
        LOGGER.info(
            "reference follower batch %d: seen=%d kept=%d",
            start // FETCH_BATCH + 1, run.candidates_seen,
            run.candidates_kept,
        )
    return runs
