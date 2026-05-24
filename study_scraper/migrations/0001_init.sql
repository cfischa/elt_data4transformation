-- Study scraper schema, v0001.
--
-- Postgres-compatible (and therefore Supabase-compatible). The scraper
-- never assumes Supabase-only features in SQL; any Supabase-specific
-- policy (RLS, Storage RLS) is created separately if/when needed.
--
-- Apply with:
--   psql "$DATABASE_URL" -f study_scraper/migrations/0001_init.sql
-- or via the storage adapter (study_scraper.storage.migrate).

CREATE SCHEMA IF NOT EXISTS study_scraper;

SET search_path TO study_scraper, public;

-- A row per discovered + accepted study.
--
-- `id` is a deterministic sha256 hex of the canonical_url; this makes
-- re-runs idempotent.
CREATE TABLE IF NOT EXISTS studies (
    id                     CHAR(64)        PRIMARY KEY,
    canonical_url          TEXT            NOT NULL UNIQUE,
    source_urls            TEXT[]          NOT NULL DEFAULT '{}',
    title                  TEXT            NOT NULL,
    authors                TEXT[]          NOT NULL DEFAULT '{}',
    publisher              TEXT,
    publication_date       DATE,
    language               TEXT,
    topic_ids              TEXT[]          NOT NULL DEFAULT '{}',
    topic_scores           JSONB           NOT NULL DEFAULT '{}'::jsonb,
    has_quantitative_data  BOOLEAN         NOT NULL DEFAULT FALSE,
    abstract               TEXT,
    key_findings           TEXT[]          NOT NULL DEFAULT '{}',
    survey_metadata        JSONB,
    raw_artifact_ref       TEXT,                  -- storage URI for the PDF/HTML
    fetched_at             TIMESTAMPTZ     NOT NULL,
    source_id              TEXT            NOT NULL,
    provenance             JSONB           NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Hot paths: filter by topic, filter by source, recency.
CREATE INDEX IF NOT EXISTS studies_topic_ids_gin   ON studies USING GIN (topic_ids);
CREATE INDEX IF NOT EXISTS studies_source_id_idx   ON studies (source_id);
CREATE INDEX IF NOT EXISTS studies_pub_date_idx    ON studies (publication_date DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS studies_fetched_at_idx  ON studies (fetched_at DESC);

-- Bookkeeping for crawl runs. One row per CLI invocation; lets the eval
-- harness later diff record sets across runs and track stability.
CREATE TABLE IF NOT EXISTS crawl_runs (
    id                 UUID            PRIMARY KEY,
    source_id          TEXT            NOT NULL,
    topic_id           TEXT            NOT NULL,
    started_at         TIMESTAMPTZ     NOT NULL,
    finished_at        TIMESTAMPTZ,
    candidates_seen    INTEGER         NOT NULL DEFAULT 0,
    candidates_kept    INTEGER         NOT NULL DEFAULT 0,
    errors             INTEGER         NOT NULL DEFAULT 0,
    parameters         JSONB           NOT NULL DEFAULT '{}'::jsonb,
    notes              TEXT
);

CREATE INDEX IF NOT EXISTS crawl_runs_source_topic_idx
    ON crawl_runs (source_id, topic_id, started_at DESC);

-- Junction: which run discovered/re-discovered which study. A study can
-- be re-discovered by many runs; we keep one row per (run, study).
CREATE TABLE IF NOT EXISTS crawl_run_studies (
    run_id     UUID         NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
    study_id   CHAR(64)     NOT NULL REFERENCES studies(id)    ON DELETE CASCADE,
    is_new     BOOLEAN      NOT NULL DEFAULT FALSE,
    PRIMARY KEY (run_id, study_id)
);

-- Schema version tracking (lightweight; no Alembic yet).
CREATE TABLE IF NOT EXISTS schema_versions (
    version       INTEGER       PRIMARY KEY,
    applied_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    description   TEXT          NOT NULL
);

INSERT INTO schema_versions (version, description)
    VALUES (1, 'initial schema: studies, crawl_runs, crawl_run_studies')
    ON CONFLICT (version) DO NOTHING;
