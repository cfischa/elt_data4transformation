"""Synchronous Postgres storage adapter.

Why sync, not async: the scraper's hot path is HTTP fetching (which is
async). DB writes happen at the end of each candidate's processing.
Sync `psycopg` keeps the storage layer trivial; the discovery/extraction
layers can still use asyncio and run DB calls via `to_thread` if they
care.

The adapter targets a plain Postgres URL so the same code path works
against (a) a hosted Supabase project, (b) `supabase start` locally,
(c) a local Postgres container. SQLite is intentionally not supported
(see DECISIONS.md A7).
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence
from urllib.parse import urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - dependency gate
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]

from study_scraper.models import CrawlRun, SourceRecord, Study


LOGGER = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
SCHEMA = "study_scraper"


class StorageError(RuntimeError):
    """Raised for unrecoverable storage problems."""


def resolve_database_url(
    *,
    postgres_url: Optional[str],
    supabase_url: Optional[str],
    supabase_service_key: Optional[str],
) -> str:
    """Return a Postgres URL for the current configuration.

    Precedence:
        1. explicit `postgres_url` (development, docker-compose).
        2. derived from `supabase_url` + `supabase_service_key`
           (Supabase Postgres connection string).

    Raises StorageError if neither is configured.
    """
    if postgres_url:
        return postgres_url

    if supabase_url and supabase_service_key:
        # NOTE (2026-06-24): For a hosted Supabase project the RELIABLE
        # path is to set POSTGRES_URL to the connection string from
        # Project Settings -> Database -> Connection string -> "Session
        # pooler" (it already contains the DB password). Prefer that.
        #
        # This derived path is kept for backwards compatibility but is
        # fragile: (a) the password is the DB password you chose at
        # project creation, NOT the API/service key; (b) the direct host
        # db.<ref>.supabase.co is IPv6-only on new projects and often
        # unreachable from GitHub Actions. So treat supabase_service_key
        # here as "the DB password" and expect to fall back to
        # POSTGRES_URL if the direct host won't resolve.
        parsed = urlparse(supabase_url)
        if not parsed.hostname or not parsed.hostname.endswith(".supabase.co"):
            raise StorageError(
                f"SUPABASE_URL {supabase_url!r} is not a supabase.co host"
            )
        ref = parsed.hostname.split(".")[0]
        return (
            f"postgresql://postgres:{supabase_service_key}"
            f"@db.{ref}.supabase.co:5432/postgres?sslmode=require"
        )

    raise StorageError(
        "No database configured. Set POSTGRES_URL — for local dev see "
        "study_scraper/docker-compose.yml; for hosted Supabase use the "
        "Session-pooler connection string (Project Settings -> Database "
        "-> Connection string -> Session pooler), e.g. postgresql://"
        "postgres.<ref>:<db-password>@aws-0-<region>.pooler.supabase.com"
        ":5432/postgres . The publishable/anon API key will NOT work — "
        "this connects directly to Postgres. See docs/study_scraper/"
        "DECISIONS.md A7."
    )


def _migration_files() -> List[Path]:
    return sorted(MIGRATIONS_DIR.glob("[0-9]*.sql"))


def _migration_version(path: Path) -> int:
    return int(path.name.split("_", 1)[0])


class PostgresStorage:
    """Thin synchronous wrapper around psycopg for the scraper's needs."""

    def __init__(self, database_url: str) -> None:
        if psycopg is None:
            raise StorageError(
                "psycopg is not installed; add it to your environment "
                "(`pip install 'psycopg[binary]'`) to use PostgresStorage."
            )
        self._database_url = database_url

    @contextmanager
    def connection(self) -> Iterator["psycopg.Connection[Any]"]:
        assert psycopg is not None  # for type checkers
        with psycopg.connect(self._database_url, row_factory=dict_row) as conn:
            yield conn

    # ------------------------------------------------------------------
    # Migrations
    # ------------------------------------------------------------------

    def migrate(self) -> List[int]:
        """Apply pending migrations. Returns the list of applied versions."""
        applied: List[int] = []
        files = _migration_files()
        if not files:
            return applied

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_versions (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        description TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    f"SELECT version FROM {SCHEMA}.schema_versions"
                )
                already: set[int] = {row["version"] for row in cur.fetchall()}

            for path in files:
                version = _migration_version(path)
                if version in already:
                    continue
                sql = path.read_text(encoding="utf-8")
                LOGGER.info("applying migration %s", path.name)
                with conn.cursor() as cur:
                    cur.execute(sql)
                applied.append(version)
            conn.commit()
        return applied

    # ------------------------------------------------------------------
    # Studies
    # ------------------------------------------------------------------

    def upsert_study(self, study: Study, *, status: str = "kept") -> bool:
        """Insert or update a study row.

        Returns True if the row was newly inserted, False if it already
        existed (the caller can use this to populate `is_new` on the
        crawl_run_studies junction).

        Status semantics (Q12):
          - "kept"     — passed the topic threshold; default.
          - "pending"  — recorded for human review; below threshold.
          - "rejected" — should only come from `reject_study`, not here.

        Re-discovery rules (sticky human decisions):
          - existing "kept"     stays "kept" (we don't downgrade).
          - existing "rejected" stays "rejected" (human said no).
          - existing "pending" can be promoted to "kept" if the new
            run brings a `kept` upsert.
        """
        if status not in {"pending", "kept"}:
            raise ValueError(
                f"upsert_study() status must be 'pending' or 'kept'; got {status!r}"
            )

        with self.connection() as conn:
            with conn.cursor() as cur:
                # DOI dedup short-circuit: if a row with the same DOI
                # exists under a different id (same study, different
                # source), the existing row is the canonical one. We
                # only append the new URL to `source_urls` and the new
                # topic to `topic_ids`; all other fields stay frozen so
                # the first-recorded version of the metadata wins.
                # Claims, status, and audit fields are owned by the
                # existing row.
                if study.doi:
                    cur.execute(
                        f"SELECT id FROM {SCHEMA}.studies "
                        f"WHERE doi = %s AND id <> %s",
                        (study.doi, study.id),
                    )
                    dup = cur.fetchone()
                    if dup is not None:
                        target_id = dup["id"]
                        LOGGER.info(
                            "doi dedup: %s -> %s (doi=%s)",
                            study.id[:12],
                            target_id[:12],
                            study.doi,
                        )
                        cur.execute(
                            f"""
                            UPDATE {SCHEMA}.studies
                               SET source_urls = (
                                       SELECT ARRAY(SELECT DISTINCT unnest(
                                           source_urls || %s::text[]))
                                   ),
                                   topic_ids   = (
                                       SELECT ARRAY(SELECT DISTINCT unnest(
                                           topic_ids   || %s::text[]))
                                   ),
                                   topic_scores = topic_scores || %s::jsonb,
                                   updated_at   = now()
                             WHERE id = %s
                            """,
                            (
                                study.source_urls or [study.canonical_url],
                                study.topic_ids,
                                json.dumps(study.topic_scores),
                                target_id,
                            ),
                        )
                        conn.commit()
                        # Reflect the rename so the caller can use
                        # `study.id` consistently downstream (claim
                        # extraction, crawl_run_studies attach).
                        study.id = target_id  # pydantic v2: validate_assignment defaults False
                        return False  # not new

                # Title-near-duplicate dedup (Phase 5b follow-on,
                # migration 0006). Fallback when DOI dedup didn't fire
                # -- same study published under different URLs, often
                # without a DOI on at least one side. pg_trgm similarity
                # >= 0.85 with matching publication_year.
                if study.title:
                    cand_year = (
                        study.publication_date.year
                        if study.publication_date else None
                    )
                    cur.execute(
                        f"SELECT * FROM {SCHEMA}.find_title_dup("
                        f"%s, %s, %s)",
                        (study.id, study.title, cand_year),
                    )
                    title_dup = cur.fetchone()
                    if title_dup is not None and title_dup.get("id"):
                        target_id = title_dup["id"]
                        LOGGER.info(
                            "title dedup: %s -> %s (sim=%.2f, '%s')",
                            study.id[:12],
                            target_id[:12],
                            float(title_dup.get("sim") or 0.0),
                            (study.title or "")[:60],
                        )
                        cur.execute(
                            f"""
                            UPDATE {SCHEMA}.studies
                               SET source_urls = (
                                       SELECT ARRAY(SELECT DISTINCT unnest(
                                           source_urls || %s::text[]))
                                   ),
                                   topic_ids   = (
                                       SELECT ARRAY(SELECT DISTINCT unnest(
                                           topic_ids   || %s::text[]))
                                   ),
                                   topic_scores = topic_scores || %s::jsonb,
                                   doi = COALESCE(doi, %s),
                                   updated_at  = now()
                             WHERE id = %s
                            """,
                            (
                                study.source_urls or [study.canonical_url],
                                study.topic_ids,
                                json.dumps(study.topic_scores),
                                study.doi,
                                target_id,
                            ),
                        )
                        conn.commit()
                        study.id = target_id
                        return False

                cur.execute(
                    f"SELECT status FROM {SCHEMA}.studies WHERE id = %s",
                    (study.id,),
                )
                existing = cur.fetchone()
                existed = existing is not None

                payload = self._study_to_row(study)
                payload["status"] = status

                placeholders = ", ".join(["%s"] * len(payload))
                columns = ", ".join(payload.keys())
                # Most columns overwrite on conflict, but:
                #   - source_urls / topic_ids accumulate (different
                #     sources may surface the same canonical URL).
                #   - status is decided by a CASE that respects human
                #     decisions (see comment above).
                #   - reviewed_by / reviewed_at / rejected_reason
                #     are preserved when set; ingest never overrides
                #     a human review.
                _MERGE_ARRAY_COLS = {"source_urls", "topic_ids"}
                _PRESERVE_COLS = {"id", "created_at"}
                _STICKY_REVIEW_COLS = {
                    "reviewed_by",
                    "reviewed_at",
                    "rejected_reason",
                }
                update_parts: List[str] = []
                for col in payload:
                    if col in _PRESERVE_COLS:
                        continue
                    if col == "status":
                        # rejected stays rejected; kept stays kept;
                        # pending may be promoted by a kept run.
                        update_parts.append(
                            f"status = CASE "
                            f"WHEN {SCHEMA}.studies.status = 'rejected' THEN 'rejected' "
                            f"WHEN {SCHEMA}.studies.status = 'kept' THEN 'kept' "
                            f"ELSE EXCLUDED.status END"
                        )
                        continue
                    if col in _STICKY_REVIEW_COLS:
                        update_parts.append(
                            f"{col} = COALESCE({SCHEMA}.studies.{col}, EXCLUDED.{col})"
                        )
                        continue
                    if col in _MERGE_ARRAY_COLS:
                        update_parts.append(
                            f"{col} = (SELECT ARRAY(SELECT DISTINCT unnest("
                            f"{SCHEMA}.studies.{col} || EXCLUDED.{col})))"
                        )
                    else:
                        update_parts.append(f"{col} = EXCLUDED.{col}")
                updates = ", ".join(update_parts)

                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.studies ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {updates},
                        updated_at = now()
                    """,
                    list(payload.values()),
                )
            conn.commit()
        return not existed

    def get_study(self, study_id: str) -> Optional[Dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {SCHEMA}.studies WHERE id = %s",
                    (study_id,),
                )
                return cur.fetchone()

    def list_studies(
        self,
        *,
        topic_id: Optional[str] = None,
        source_id: Optional[str] = None,
        status: Optional[str] = "kept",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List studies. By default returns only `status='kept'` rows;
        pass `status='pending'` for the review queue, `status='rejected'`
        for the audit trail, or `status=None` for everything.
        """
        clauses: List[str] = []
        params: List[Any] = []
        if topic_id is not None:
            clauses.append("%s = ANY(topic_ids)")
            params.append(topic_id)
        if source_id is not None:
            clauses.append("source_id = %s")
            params.append(source_id)
        if status is not None:
            clauses.append("status = %s")
            params.append(status)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT * FROM {SCHEMA}.studies
                    {where}
                    ORDER BY fetched_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    # ------------------------------------------------------------------
    # Review queue (Q12)
    # ------------------------------------------------------------------

    def promote_study(self, study_id: str, *, reviewed_by: str) -> bool:
        """Move a study from pending → kept. Returns True if a row changed."""
        return self._set_review_status(
            study_id,
            new_status="kept",
            reviewed_by=reviewed_by,
            rejected_reason=None,
            allowed_from={"pending"},
        )

    def reject_study(
        self, study_id: str, *, reviewed_by: str, reason: Optional[str] = None
    ) -> bool:
        """Move a study to rejected. Returns True if a row changed."""
        return self._set_review_status(
            study_id,
            new_status="rejected",
            reviewed_by=reviewed_by,
            rejected_reason=reason,
            allowed_from={"pending", "kept"},
        )

    def _set_review_status(
        self,
        study_id: str,
        *,
        new_status: str,
        reviewed_by: str,
        rejected_reason: Optional[str],
        allowed_from: set,
    ) -> bool:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {SCHEMA}.studies
                       SET status          = %s,
                           reviewed_by     = %s,
                           reviewed_at     = now(),
                           rejected_reason = %s,
                           updated_at      = now()
                     WHERE id = %s
                       AND status = ANY(%s)
                    """,
                    (
                        new_status,
                        reviewed_by,
                        rejected_reason,
                        study_id,
                        list(allowed_from),
                    ),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    def count_studies_by_status(self) -> Dict[str, int]:
        """{ status -> n }. Used by status report + dock."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT status, COUNT(*) AS c "
                    f"FROM {SCHEMA}.studies GROUP BY status"
                )
                return {row["status"]: int(row["c"]) for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Full-text processing support (Phase 6-full, A20)
    # ------------------------------------------------------------------

    def set_artifact_ref(self, study_id: str, ref: str) -> bool:
        """Record where the raw fetched document lives. True if updated."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.studies "
                    f"   SET raw_artifact_ref = %s, updated_at = now() "
                    f" WHERE id = %s",
                    (ref, study_id),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    def set_review_rationale(self, study_id: str, rationale: str) -> bool:
        """Record why a study was auto-reviewed, merged into provenance jsonb
        (audit trail for the auto-reviewer; no schema change). True if updated."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.studies "
                    f"   SET provenance = COALESCE(provenance, '{{}}'::jsonb) "
                    f"       || jsonb_build_object('review_rationale', %s::text), "
                    f"       updated_at = now() "
                    f" WHERE id = %s",
                    (rationale, study_id),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    def set_resolved_pdf_url(self, study_id: str, pdf_url: str) -> bool:
        """Record the PDF URL resolved from a landing page, merged into the
        study's provenance jsonb (no schema change). Keeps the audit trail:
        which document we actually read for full-text claims. True if updated."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.studies "
                    f"   SET provenance = COALESCE(provenance, '{{}}'::jsonb) "
                    f"       || jsonb_build_object('resolved_pdf_url', %s::text), "
                    f"       updated_at = now() "
                    f" WHERE id = %s",
                    (pdf_url, study_id),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    def set_fetch_conditional(
        self,
        study_id: str,
        *,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> bool:
        """Record ETag/Last-Modified from the last successful fulltext
        fetch, merged into provenance jsonb (no schema change), so the
        next run can send a conditional GET (If-None-Match /
        If-Modified-Since) and skip unchanged documents (issue #34).
        No-op (returns False) when neither header was present. True if
        updated."""
        patch: Dict[str, Any] = {}
        if etag:
            patch["fetch_etag"] = etag
        if last_modified:
            patch["fetch_last_modified"] = last_modified
        if not patch:
            return False
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.studies "
                    f"   SET provenance = COALESCE(provenance, '{{}}'::jsonb) "
                    f"       || %s::jsonb, "
                    f"       updated_at = now() "
                    f" WHERE id = %s",
                    (json.dumps(patch), study_id),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    def list_studies_for_fulltext(
        self, *, limit: int = 20, include_done: bool = False
    ) -> List[Dict[str, Any]]:
        """Kept studies queued for document fetching.

        Default: only those without a raw artifact yet. With
        `include_done=True`, also re-queue already-processed ones
        (e.g. after an extractor upgrade).
        """
        artifact_clause = (
            "" if include_done else "AND raw_artifact_ref IS NULL"
        )
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, canonical_url, title, raw_artifact_ref,
                           doi, provenance
                    FROM   {SCHEMA}.studies
                    WHERE  status = 'kept'
                      {artifact_clause}
                      AND  NOT COALESCE(
                               (provenance->>'no_fetchable_url')::boolean,
                               false)
                    ORDER  BY fetched_at DESC
                    LIMIT  %s
                    """,
                    (limit,),
                )
                return list(cur.fetchall())

    def mark_unfetchable(self, study_id: str) -> bool:
        """Flag a study as having no fetchable document URL (e.g. an
        openalex-only work with no DOI/location). Merged into provenance
        jsonb; `list_studies_for_fulltext` then skips it, so the queue
        can't 403 on the same study forever. True if updated."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.studies "
                    f"   SET provenance = COALESCE(provenance, '{{}}'::jsonb) "
                    f"       || jsonb_build_object('no_fetchable_url', true), "
                    f"       updated_at = now() "
                    f" WHERE id = %s",
                    (study_id,),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    # ------------------------------------------------------------------
    # Claims (Phase 6-mini)
    # ------------------------------------------------------------------

    def upsert_claims(
        self,
        study_id: str,
        claims: Iterable[Any],
        *,
        extractor: str = "regex-v1",
    ) -> int:
        """Replace the claims for one study; return the count written.

        Idempotent: deletes existing claims from this `extractor` for the
        study and re-inserts. Other extractors' claims (when we add LLM-
        based extraction) are untouched.
        """
        rows = [
            (
                claim.id,
                claim.study_id,
                claim.claim_text,
                claim.numeric_value,
                claim.unit,
                claim.source_field,
                extractor,
            )
            for claim in claims
        ]
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {SCHEMA}.claims "
                    f"WHERE study_id = %s AND extractor = %s",
                    (study_id, extractor),
                )
                if rows:
                    cur.executemany(
                        f"""
                        INSERT INTO {SCHEMA}.claims (
                            id, study_id, claim_text, numeric_value,
                            unit, source_field, extractor
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        rows,
                    )
            conn.commit()
        return len(rows)

    def search_claims(
        self,
        *,
        query: str,
        unit: Optional[str] = "%",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """ILIKE-search over claim_text. Joins back to studies for context."""
        params: List[Any] = []
        clauses: List[str] = []
        if unit is not None:
            clauses.append("c.unit = %s")
            params.append(unit)
        clauses.append("lower(c.claim_text) LIKE %s")
        params.append(f"%{query.lower()}%")
        where = " AND ".join(clauses)
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT  c.id            AS claim_id,
                            c.claim_text,
                            c.numeric_value,
                            c.unit,
                            c.source_field,
                            s.id            AS study_id,
                            s.title,
                            s.canonical_url,
                            s.source_id,
                            s.publication_date,
                            s.topic_ids,
                            s.status
                    FROM    {SCHEMA}.claims  c
                    JOIN    {SCHEMA}.studies s ON s.id = c.study_id
                    WHERE   {where}
                      AND   s.status <> 'rejected'
                    ORDER   BY s.publication_date DESC NULLS LAST,
                               c.numeric_value DESC NULLS LAST
                    LIMIT   %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    def claims_for_study(self, study_id: str) -> List[Dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {SCHEMA}.claims WHERE study_id = %s "
                    f"ORDER BY source_field, numeric_value DESC",
                    (study_id,),
                )
                return list(cur.fetchall())

    def count_claims(self) -> int:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS c FROM {SCHEMA}.claims")
                return int(cur.fetchone()["c"])

    # ------------------------------------------------------------------
    # Attributions (Option A / A21: llm-v1 structured triples)
    # ------------------------------------------------------------------

    def upsert_attributions(
        self,
        study_id: str,
        attributions: Iterable[Any],
        *,
        model: str = "llm-v1",
    ) -> int:
        """Replace this study's attributions for the given `model`.

        Idempotent per (study, model): delete existing rows from this
        model, re-insert. Other models' attributions are untouched, so
        an llm-v1 re-run doesn't disturb a future llm-v2 pass.
        """
        rows = [
            (
                a.id,
                a.study_id,
                a.question,
                a.position,
                a.percentage,
                a.population,
                a.confidence,
                model,
                json.dumps(a.raw, ensure_ascii=False),
            )
            for a in attributions
        ]
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {SCHEMA}.attributions "
                    f"WHERE study_id = %s AND model = %s",
                    (study_id, model),
                )
                if rows:
                    cur.executemany(
                        f"""
                        INSERT INTO {SCHEMA}.attributions (
                            id, study_id, question, position, percentage,
                            population, confidence, model, raw
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        rows,
                    )
            conn.commit()
        return len(rows)

    def search_attributions(
        self, *, query: str, limit: int = 50, since: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """ILIKE search over attribution questions; joins study context.

        Each row carries `sample_size` — the study's representative n=
        claim (ROADMAP item C): the most frequent plausible sample-size
        value extracted from that study, largest on ties. NULL when the
        study has no n= claim.

        `since` (ROADMAP item B) keeps only findings whose study was
        published in that year or later; studies with an unknown
        publication date are excluded when the filter is active (an
        undatable poll can't be shown as "recent").
        """
        since_clause = ""
        params: List[Any] = [f"%{query.lower()}%"]
        if since is not None:
            since_clause = (
                "AND s.publication_date IS NOT NULL "
                "AND EXTRACT(YEAR FROM s.publication_date) >= %s"
            )
            params.append(since)
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT a.question, a.position, a.percentage, a.population,
                           a.confidence, a.model, a.raw,
                           s.title, s.canonical_url, s.source_id,
                           s.publisher, s.publication_date, n.sample_size
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    LEFT JOIN LATERAL (
                        SELECT c.numeric_value AS sample_size
                        FROM   {SCHEMA}.claims c
                        WHERE  c.study_id = s.id
                          AND  c.unit = 'n'
                          AND  c.numeric_value BETWEEN 30 AND 10000000
                        GROUP  BY c.numeric_value
                        ORDER  BY COUNT(*) DESC, c.numeric_value DESC
                        LIMIT  1
                    ) n ON TRUE
                    WHERE  lower(a.question) LIKE %s
                      AND  s.status <> 'rejected'
                      {since_clause}
                    -- Recency first: these windows feed dedup/aggregation,
                    -- and truncation under percentage-DESC systematically
                    -- inflated the mean (drops low findings first). Under
                    -- recency-DESC truncation prefers what the weights
                    -- prefer anyway (audit 2026-07-11).
                    ORDER  BY s.publication_date DESC NULLS LAST,
                              a.percentage DESC NULLS LAST
                    LIMIT  %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    def search_attributions_deduped(
        self, *, query: str, limit: int = 50, since: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Like `search_attributions` but collapses the same finding seen
        across multiple studies to one confidence-weighted representative
        (carries `dup_count`). Dedup is read-time in Python; raw rows are
        untouched. Fetches a wider window first so dedup has material.

        `query` may carry pipe-separated recall alternatives from the
        question registry ('conscription|military service'): the lexical
        pass runs per alternative and unions the rows, so a finding
        phrased under ANY alias is found. Duplicates across alternatives
        collapse in dedupe (identical rows share a dedup key)."""
        from study_scraper.findings import dedupe_attributions

        alternatives = [a.strip() for a in query.split("|") if a.strip()] or [""]
        window = max(limit * 5, 200)
        raw_rows: List[Dict[str, Any]] = []
        seen: set = set()
        for alt in alternatives:
            for row in self.search_attributions(
                query=alt, limit=window, since=since
            ):
                # Two aliases can match the SAME physical row — drop the
                # re-find, or dup_count (a trust signal: independently
                # seen across studies) would inflate.
                ident = (
                    row.get("question"), row.get("position"),
                    row.get("percentage"), row.get("population"),
                    row.get("canonical_url"),
                )
                if ident in seen:
                    continue
                seen.add(ident)
                raw_rows.append(row)
        return dedupe_attributions(raw_rows)[:limit]

    def search_attributions_semantic(
        self, *, query: str, limit: int = 50, since: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Deduped attribution search: lexical AND semantic, unioned.

        Attribution questions are normalized to English (llm-v1) but
        phrased freely, so neither pass alone answers "from ALL relevant
        data": ILIKE misses paraphrases ('klimaschutzgesetz', 'compulsory
        military service' vs seed 'conscription'), and the old
        semantic-only-when-lexical-is-empty short circuit meant three
        lexical rows suppressed thirty semantically matching ones.

        Both passes always run. Lexical rows lead (exact evidence); the
        semantic sweep ranks the DISTINCT question texts by bilingual
        concept similarity (clustering.py) — scaling with phrasing
        variety, not row count (the previous top-N-by-percentage window
        went blind as the corpus grew) — and rows for matching texts not
        already found lexically are appended best-match-first."""
        from study_scraper.clustering import semantic_filter
        from study_scraper.findings import dedupe_attributions

        lexical = self.search_attributions_deduped(
            query=query, limit=limit, since=since
        )
        have = {(r.get("question") or "").lower() for r in lexical}
        candidates = [
            {"question": q}
            for q in self.list_distinct_attribution_questions(since=since)
            if q.lower() not in have
        ]
        matched = semantic_filter(query, candidates)
        if not matched:
            return lexical[:limit]
        sem_rows = self.search_attributions_for_questions(
            [m["question"] for m in matched],
            since=since,
            limit=max(limit * 5, 200),
        )
        return (lexical + dedupe_attributions(sem_rows))[:limit]

    def list_distinct_attribution_questions(
        self, *, since: Optional[int] = None
    ) -> List[str]:
        """Distinct attribution question texts (non-rejected studies) —
        the candidate pool for the semantic sweep. A few hundred distinct
        phrasings stand in for arbitrarily many rows."""
        since_clause = ""
        params: List[Any] = []
        if since is not None:
            since_clause = (
                "AND s.publication_date IS NOT NULL "
                "AND EXTRACT(YEAR FROM s.publication_date) >= %s"
            )
            params.append(since)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT a.question
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    WHERE  s.status <> 'rejected'
                      {since_clause}
                    ORDER  BY a.question
                    """,
                    params,
                )
                return [row["question"] for row in cur.fetchall()]

    def search_attributions_for_questions(
        self,
        questions: List[str],
        *,
        limit: int = 200,
        since: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """All attribution rows whose question text matches one of
        `questions` exactly (case-insensitive) — the fetch stage after the
        semantic sweep picked which phrasings are relevant."""
        if not questions:
            return []
        since_clause = ""
        params: List[Any] = [[q.lower() for q in questions]]
        if since is not None:
            since_clause = (
                "AND s.publication_date IS NOT NULL "
                "AND EXTRACT(YEAR FROM s.publication_date) >= %s"
            )
            params.append(since)
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT a.question, a.position, a.percentage, a.population,
                           a.confidence, a.model, a.raw,
                           s.title, s.canonical_url, s.source_id,
                           s.publisher, s.publication_date, n.sample_size
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    LEFT JOIN LATERAL (
                        SELECT c.numeric_value AS sample_size
                        FROM   {SCHEMA}.claims c
                        WHERE  c.study_id = s.id
                          AND  c.unit = 'n'
                          AND  c.numeric_value BETWEEN 30 AND 10000000
                        GROUP  BY c.numeric_value
                        ORDER  BY COUNT(*) DESC, c.numeric_value DESC
                        LIMIT  1
                    ) n ON TRUE
                    WHERE  lower(a.question) = ANY(%s)
                      AND  s.status <> 'rejected'
                      {since_clause}
                    -- Recency first: these windows feed dedup/aggregation,
                    -- and truncation under percentage-DESC systematically
                    -- inflated the mean (drops low findings first). Under
                    -- recency-DESC truncation prefers what the weights
                    -- prefer anyway (audit 2026-07-11).
                    ORDER  BY s.publication_date DESC NULLS LAST,
                              a.percentage DESC NULLS LAST
                    LIMIT  %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    def count_attributions(self) -> int:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS c FROM {SCHEMA}.attributions")
                return int(cur.fetchone()["c"])

    def sample_attributions(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        """Random sample of stored attributions with study context, for
        the `audit` accuracy spot-check (ACCURACY.md measurement C).
        Excludes rejected studies. Includes `raw` (source_span, grounded,
        distribution_check live there)."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT a.question, a.position, a.percentage,
                           a.population, a.confidence, a.model, a.raw,
                           s.title, s.canonical_url, s.source_id
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    WHERE  s.status <> 'rejected'
                    ORDER  BY random()
                    LIMIT  %s
                    """,
                    (limit,),
                )
                return list(cur.fetchall())

    def filter_attributions(
        self,
        *,
        query: Optional[str] = None,
        position: Optional[str] = None,
        topic: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Browse attribution triples with optional text/position/topic
        filters (powers the dock Attributions page). Joins study context;
        excludes rejected studies. Ordered by percentage then confidence."""
        clauses: List[str] = ["s.status <> 'rejected'"]
        params: List[Any] = []
        if query:
            clauses.append("lower(a.question) LIKE %s")
            params.append(f"%{query.lower()}%")
        if position:
            clauses.append("a.position = %s")
            params.append(position)
        if topic:
            clauses.append("%s = ANY(s.topic_ids)")
            params.append(topic)
        where = " AND ".join(clauses)
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT a.question, a.position, a.percentage, a.population,
                           a.confidence, a.model, a.raw,
                           s.title, s.canonical_url, s.source_id,
                           s.publisher, s.publication_date, s.topic_ids
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    WHERE  {where}
                    ORDER  BY a.percentage DESC NULLS LAST,
                              a.confidence DESC NULLS LAST
                    LIMIT  %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    def list_distinct_attribution_topics(self) -> List[str]:
        """Distinct topic_ids appearing on studies that have attributions —
        populates the dock's topic filter dropdown."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT unnest(s.topic_ids) AS t
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    WHERE  s.status <> 'rejected'
                      AND  array_length(s.topic_ids, 1) > 0
                    ORDER  BY t
                    """
                )
                return [r["t"] for r in cur.fetchall()]

    def get_study_for_attribution(self, study_id: str) -> Optional[Dict[str, Any]]:
        """Study + its claim snippets, for feeding the llm-v1 extractor."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, title, abstract FROM {SCHEMA}.studies "
                    f"WHERE id = %s",
                    (study_id,),
                )
                study = cur.fetchone()
                if study is None:
                    return None
                cur.execute(
                    f"SELECT claim_text FROM {SCHEMA}.claims "
                    f"WHERE study_id = %s ORDER BY source_field, numeric_value DESC",
                    (study_id,),
                )
                study = dict(study)
                study["claim_snippets"] = [r["claim_text"] for r in cur.fetchall()]
                return study

    # ------------------------------------------------------------------
    # Source records (Q16-v2 lake; structured-data sources land here)
    # ------------------------------------------------------------------

    def upsert_source_record(
        self, record: SourceRecord, *, status: str = "kept"
    ) -> bool:
        """Insert or update a `source_records` row. Same status rules as
        `upsert_study` (Q12 still applies): existing 'rejected' and
        'kept' are sticky; only 'pending' can be promoted by a 'kept'
        upsert."""
        if status not in {"pending", "kept"}:
            raise ValueError(
                f"upsert_source_record(): status must be 'pending' or 'kept'; got {status!r}"
            )
        payload_json: Optional[str] = (
            json.dumps(record.payload, ensure_ascii=False)
            if record.payload is not None else None
        )
        provenance_json = json.dumps(record.provenance, ensure_ascii=False)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {SCHEMA}.source_records WHERE id = %s",
                    (record.id,),
                )
                existed = cur.fetchone() is not None
                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.source_records (
                        id, source_id, source_record_id, canonical_url,
                        format, content_type, content_hash,
                        payload, payload_uri,
                        topic_ids, doi, license,
                        fetched_at, discovery_run_id,
                        status, provenance
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            %s::jsonb, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s::jsonb)
                    ON CONFLICT (id) DO UPDATE SET
                        source_record_id = EXCLUDED.source_record_id,
                        format          = EXCLUDED.format,
                        content_type    = EXCLUDED.content_type,
                        content_hash    = EXCLUDED.content_hash,
                        payload         = EXCLUDED.payload,
                        payload_uri     = EXCLUDED.payload_uri,
                        topic_ids = (SELECT ARRAY(SELECT DISTINCT unnest(
                                {SCHEMA}.source_records.topic_ids || EXCLUDED.topic_ids))),
                        doi             = COALESCE({SCHEMA}.source_records.doi, EXCLUDED.doi),
                        license         = COALESCE({SCHEMA}.source_records.license, EXCLUDED.license),
                        fetched_at      = EXCLUDED.fetched_at,
                        discovery_run_id = EXCLUDED.discovery_run_id,
                        status = CASE
                            WHEN {SCHEMA}.source_records.status = 'rejected' THEN 'rejected'
                            WHEN {SCHEMA}.source_records.status = 'kept'     THEN 'kept'
                            ELSE EXCLUDED.status
                        END,
                        provenance      = EXCLUDED.provenance,
                        updated_at      = now()
                    """,
                    (
                        record.id,
                        record.source_id,
                        record.source_record_id,
                        record.canonical_url,
                        record.format,
                        record.content_type,
                        record.content_hash,
                        payload_json,
                        record.payload_uri,
                        record.topic_ids,
                        record.doi,
                        record.license,
                        record.fetched_at,
                        record.discovery_run_id,
                        status,
                        provenance_json,
                    ),
                )
            conn.commit()
        return not existed

    def count_source_records(self, *, source_id: Optional[str] = None) -> int:
        clauses: List[str] = []
        params: List[Any] = []
        if source_id is not None:
            clauses.append("source_id = %s")
            params.append(source_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS c FROM {SCHEMA}.source_records {where}",
                    params,
                )
                return int(cur.fetchone()["c"])

    def list_source_records(
        self,
        *,
        source_id: Optional[str] = None,
        topic_id: Optional[str] = None,
        format: Optional[str] = None,
        status: Optional[str] = "kept",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if source_id is not None:
            clauses.append("source_id = %s")
            params.append(source_id)
        if topic_id is not None:
            clauses.append("%s = ANY(topic_ids)")
            params.append(topic_id)
        if format is not None:
            clauses.append("format = %s")
            params.append(format)
        if status is not None:
            clauses.append("status = %s")
            params.append(status)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, source_id, source_record_id, canonical_url,
                           format, content_hash, topic_ids, license,
                           fetched_at, status
                    FROM   {SCHEMA}.source_records
                    {where}
                    ORDER  BY fetched_at DESC
                    LIMIT  %s
                    """,
                    params,
                )
                return list(cur.fetchall())

    def query_view(
        self, view_name: str, *, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Read N rows from a SQL view inside the schema.

        Limited to view names that look like identifiers so a caller
        typo can't smuggle SQL through; views live in the
        `study_scraper` schema only.
        """
        if not view_name.replace("_", "").isalnum():
            raise ValueError(f"invalid view name: {view_name!r}")
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {SCHEMA}.{view_name} LIMIT %s", (limit,)
                )
                return list(cur.fetchall())

    # ------------------------------------------------------------------
    # Open dataset export
    # ------------------------------------------------------------------

    def list_attributions_for_export(self) -> List[Dict[str, Any]]:
        """Every attribution on a non-rejected study, with study context
        and the representative sample size — the findings.csv shape."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT a.question, a.position, a.percentage, a.population,
                           a.confidence, a.model, n.sample_size,
                           s.publication_date, s.publisher, s.title,
                           s.source_id, s.canonical_url, s.topic_ids
                    FROM   {SCHEMA}.attributions a
                    JOIN   {SCHEMA}.studies s ON s.id = a.study_id
                    LEFT JOIN LATERAL (
                        SELECT c.numeric_value AS sample_size
                        FROM   {SCHEMA}.claims c
                        WHERE  c.study_id = s.id
                          AND  c.unit = 'n'
                          AND  c.numeric_value BETWEEN 30 AND 10000000
                        GROUP  BY c.numeric_value
                        ORDER  BY COUNT(*) DESC, c.numeric_value DESC
                        LIMIT  1
                    ) n ON TRUE
                    WHERE  s.status <> 'rejected'
                    ORDER  BY s.publication_date DESC NULLS LAST, a.question
                    """
                )
                return list(cur.fetchall())

    def list_studies_for_export(self) -> List[Dict[str, Any]]:
        """Kept studies' bibliographic metadata — the studies.csv shape.
        Deliberately excludes abstract / key_findings / raw artifacts."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, title, publisher, publication_date, language,
                           topic_ids, has_quantitative_data, source_id,
                           canonical_url
                    FROM   {SCHEMA}.studies
                    WHERE  status = 'kept'
                    ORDER  BY publication_date DESC NULLS LAST, id
                    """
                )
                return list(cur.fetchall())

    # ------------------------------------------------------------------
    # Monitoring v1: watches + snapshots (migration 0009)
    # ------------------------------------------------------------------

    def add_watch(
        self,
        *,
        query: str,
        label: Optional[str] = None,
        since_year: Optional[int] = None,
        source: Optional[str] = None,
    ) -> int:
        """Register a standing question; returns the watch id.
        Re-adding the same query re-activates it and updates label/since.
        `source` marks who manages the row ('registry' for questions.yml
        sync; NULL for manual `watch add`) — sync prunes only its own."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.watches (query, label, since_year, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (query) DO UPDATE SET
                        label = EXCLUDED.label,
                        since_year = EXCLUDED.since_year,
                        source = EXCLUDED.source,
                        active = TRUE
                    RETURNING id
                    """,
                    (query, label, since_year, source),
                )
                watch_id = int(cur.fetchone()["id"])
            conn.commit()
        return watch_id

    def prune_watches(
        self, *, source: str, keep_queries: List[str]
    ) -> List[Dict[str, Any]]:
        """Deactivate active watches managed by `source` whose query is
        not in `keep_queries` (case-insensitive). Returns the pruned
        rows. A registry edit changes the watch query, which upserts a
        NEW row and would otherwise strand the old one active — silently
        double-answering the same polls in every digest."""
        keep = [q.lower() for q in keep_queries]
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {SCHEMA}.watches
                    SET    active = FALSE
                    WHERE  active
                      AND  source = %s
                      AND  NOT (lower(query) = ANY(%s))
                    RETURNING id, query, label
                    """,
                    (source, keep),
                )
                pruned = list(cur.fetchall())
            conn.commit()
        return pruned

    def list_watches(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        where = "WHERE active" if active_only else ""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, query, label, since_year, active, source, created_at "
                    f"FROM {SCHEMA}.watches {where} ORDER BY id"
                )
                return list(cur.fetchall())

    def remove_watch(self, watch_id: int) -> bool:
        """Deactivate (not delete) so snapshot history survives."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.watches SET active = FALSE "
                    f"WHERE id = %s AND active",
                    (watch_id,),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    def latest_watch_snapshot(self, watch_id: int) -> Optional[Dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, watch_id, taken_at, findings_count, payload
                    FROM   {SCHEMA}.watch_snapshots
                    WHERE  watch_id = %s
                    ORDER  BY taken_at DESC, id DESC
                    LIMIT  1
                    """,
                    (watch_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def save_watch_snapshot(
        self, watch_id: int, *, findings_count: int, payload: List[Dict[str, Any]]
    ) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.watch_snapshots
                        (watch_id, findings_count, payload)
                    VALUES (%s, %s, %s::jsonb)
                    """,
                    (watch_id, findings_count, json.dumps(payload, ensure_ascii=False)),
                )
            conn.commit()

    # ------------------------------------------------------------------
    # Crawl runs
    # ------------------------------------------------------------------

    def record_crawl_run(self, run: CrawlRun) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.crawl_runs (
                        id, source_id, topic_id, started_at, finished_at,
                        candidates_seen, candidates_kept, errors,
                        parameters, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        finished_at = EXCLUDED.finished_at,
                        candidates_seen = EXCLUDED.candidates_seen,
                        candidates_kept = EXCLUDED.candidates_kept,
                        errors = EXCLUDED.errors,
                        parameters = EXCLUDED.parameters,
                        notes = EXCLUDED.notes
                    """,
                    (
                        run.id,
                        run.source_id,
                        run.topic_id,
                        run.started_at,
                        run.finished_at,
                        run.candidates_seen,
                        run.candidates_kept,
                        run.errors,
                        json.dumps(run.parameters),
                        run.notes,
                    ),
                )
            conn.commit()

    def last_crawl_finished_at(
        self, *, source_id: str, topic_id: str
    ) -> Optional[datetime]:
        """Timestamp of the most recently completed crawl_runs row for this
        (source, topic) pair, or None if no prior run exists. Feeds the
        SSOAR OAI `from=` incremental window (issue #34): full harvest on
        the first run, incremental thereafter."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX(finished_at) AS latest
                    FROM   {SCHEMA}.crawl_runs
                    WHERE  source_id = %s AND topic_id = %s
                      AND  finished_at IS NOT NULL
                    """,
                    (source_id, topic_id),
                )
                row = cur.fetchone()
        return row["latest"] if row else None

    def attach_studies_to_run(
        self,
        run_id: str,
        entries: Iterable[tuple[str, bool]],
    ) -> None:
        """Attach studies (study_id, is_new) to a crawl run."""
        rows: Sequence[tuple[str, str, bool]] = [
            (run_id, study_id, is_new) for study_id, is_new in entries
        ]
        if not rows:
            return
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"""
                    INSERT INTO {SCHEMA}.crawl_run_studies (
                        run_id, study_id, is_new
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, study_id) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _study_to_row(study: Study) -> Dict[str, Any]:
        survey_metadata = (
            json.dumps(study.survey_metadata.model_dump(mode="json"))
            if study.survey_metadata is not None
            else None
        )
        return {
            "id": study.id,
            "canonical_url": study.canonical_url,
            "source_urls": study.source_urls,
            "title": study.title,
            "authors": study.authors,
            "publisher": study.publisher,
            "publication_date": study.publication_date,
            "language": study.language,
            "doi": study.doi,
            "topic_ids": study.topic_ids,
            "topic_scores": json.dumps(study.topic_scores),
            "has_quantitative_data": study.has_quantitative_data,
            "abstract": study.abstract,
            "key_findings": study.key_findings,
            "survey_metadata": survey_metadata,
            "raw_artifact_ref": study.raw_artifact_ref,
            "fetched_at": study.fetched_at,
            "source_id": study.source_id,
            "provenance": json.dumps(study.provenance.model_dump(mode="json")),
        }
