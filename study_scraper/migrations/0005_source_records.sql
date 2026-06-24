-- Migration 0005 — `source_records` lake table for structured-data sources.
--
-- Per maintainer Q16-v2 (2026-05-28): "We need to collect the data as
-- they are first. We can later think about how to process them."
--
-- Design: ONE universal table for every structured-data source (DAWUM,
-- Destatis, Eurostat, GESIS, ...). No table per source. The payload is
-- captured as-is; only what's needed to find / filter / dedup gets
-- typed columns. Per-source structured queries become views on top of
-- this table -- added when the access pattern needs them, not before.
--
-- Coexists with `studies`. Academic sources keep writing to studies;
-- structured-data sources write here. Claims may reference either via
-- the discriminator added below.

SET search_path TO study_scraper, public;

CREATE TABLE IF NOT EXISTS source_records (
    id                CHAR(64)        PRIMARY KEY,
                       -- sha256(source_id || '|' || canonical_url)
                       -- so re-fetching the same record is idempotent
                       -- and the same record from a different source
                       -- gets a different id.
    source_id         TEXT            NOT NULL,
                       -- 'dawum' | 'destatis' | 'eurostat' | 'gesis' | ...
    source_record_id  TEXT,
                       -- the source's own id for the record
                       -- (poll id, table code, study ZA-number).
    canonical_url     TEXT            NOT NULL,
                       -- where the bytes came from. UNIQUE per source.
    format            TEXT            NOT NULL,
                       -- 'dawum_survey_json' | 'destatis_table_csv' |
                       -- 'eurostat_jsonstat' | 'gesis_sav' | ...
                       -- The shape *inside* `payload` is implied by
                       -- this label; views per format do the parsing.
    content_type      TEXT,
                       -- HTTP MIME when known.
    content_hash      TEXT            NOT NULL,
                       -- sha256 of the original bytes. Change-detection
                       -- + idempotency.
    payload           JSONB,
                       -- inline for JSON / CSV / small structured.
                       -- NULL when payload lives in Supabase Storage
                       -- (binaries, large files) -- see `payload_uri`.
    payload_uri       TEXT,
                       -- Storage URI for binary / oversized payloads.
                       -- Exactly one of `payload` / `payload_uri` is set.
    topic_ids         TEXT[]          NOT NULL DEFAULT '{}',
                       -- topic association; empty when the source
                       -- doesn't have a natural topic discriminator
                       -- (DAWUM, for instance).
    doi               TEXT,
                       -- when the source provides one; reused for
                       -- cross-source dedup the same way studies.doi is.
    license           TEXT,
                       -- captured per ingest. Trumps source-level
                       -- default. Important for redistribution rules.
    fetched_at        TIMESTAMPTZ     NOT NULL,
    discovery_run_id  UUID            NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
    status            TEXT            NOT NULL DEFAULT 'kept',
                       -- Q12 review queue still applies: pending /
                       -- kept / rejected.
    rejected_reason   TEXT,
    reviewed_by       TEXT,
    reviewed_at       TIMESTAMPTZ,
    provenance        JSONB           NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- One row per (source_id, canonical_url).
CREATE UNIQUE INDEX IF NOT EXISTS source_records_source_url_uq
    ON source_records (source_id, canonical_url);

CREATE INDEX IF NOT EXISTS source_records_source_id_idx
    ON source_records (source_id);
CREATE INDEX IF NOT EXISTS source_records_format_idx
    ON source_records (format);
CREATE INDEX IF NOT EXISTS source_records_topic_ids_gin
    ON source_records USING GIN (topic_ids);
CREATE INDEX IF NOT EXISTS source_records_doi_idx
    ON source_records (doi) WHERE doi IS NOT NULL;
CREATE INDEX IF NOT EXISTS source_records_status_idx
    ON source_records (status);
CREATE INDEX IF NOT EXISTS source_records_fetched_at_idx
    ON source_records (fetched_at DESC);

-- Sanity: exactly one storage cell populated.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'source_records_one_payload_chk'
    ) THEN
        ALTER TABLE source_records
            ADD CONSTRAINT source_records_one_payload_chk
            CHECK (
                (payload IS NOT NULL AND payload_uri IS NULL) OR
                (payload IS NULL AND payload_uri IS NOT NULL)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'source_records_status_chk'
    ) THEN
        ALTER TABLE source_records
            ADD CONSTRAINT source_records_status_chk
            CHECK (status IN ('pending', 'kept', 'rejected'));
    END IF;
END$$;

-- claims gains an optional FK to source_records, so a claim may be
-- backed by either a `study` (existing) or a `source_record` (new).
-- A check constraint guarantees exactly one is set.
ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS source_record_id CHAR(64)
        REFERENCES source_records(id) ON DELETE CASCADE,
    ALTER COLUMN study_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'claims_target_one_chk'
    ) THEN
        ALTER TABLE claims
            ADD CONSTRAINT claims_target_one_chk
            CHECK (
                (study_id IS NOT NULL AND source_record_id IS NULL) OR
                (study_id IS NULL AND source_record_id IS NOT NULL)
            );
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS claims_source_record_idx
    ON claims (source_record_id);

-- ---------------------------------------------------------------------------
-- FIRST VIEW: dawum_polls
--
-- Demonstrates the lake-then-view pattern. Source-specific projections
-- are added as VIEWS over `source_records`; no per-source table needed.
-- Schema changes from DAWUM upstream require updating this view, not a
-- table migration.
--
-- payload shape per DAWUM survey row (denormalised at ingest time so
-- the row is self-contained):
--   {
--     "survey_id": "1234",
--     "date": "2026-05-28",
--     "survey_period": {"start": "...", "end": "..."},
--     "sample_size": 1234,
--     "institute":  {"id": "...", "name": "Forsa"},
--     "tasker":     {"id": "...", "name": "RTL"},
--     "parliament": {"id": "0",   "name": "Bundestag", "shortcut": "BTW"},
--     "method":     {"id": "...", "name": "Online"},
--     "results":    [{"party_id": "1", "party_name": "CDU/CSU",
--                     "shortcut": "Union", "percentage": 30.0}, ...]
--   }
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW dawum_polls AS
SELECT
    id                                                AS source_record_id,
    canonical_url,
    fetched_at,
    payload->>'survey_id'                             AS survey_id,
    (payload->>'date')::date                          AS poll_date,
    (payload->'survey_period'->>'start')::date        AS period_start,
    (payload->'survey_period'->>'end')::date          AS period_end,
    NULLIF(payload->>'sample_size', '')::int          AS sample_size,
    payload->'parliament'->>'name'                    AS parliament_name,
    payload->'parliament'->>'shortcut'                AS parliament_shortcut,
    payload->'institute'->>'name'                     AS institute_name,
    payload->'tasker'->>'name'                        AS tasker_name,
    payload->'method'->>'name'                        AS method_name,
    payload->'results'                                AS results,  -- jsonb array
    status
FROM   source_records
WHERE  source_id = 'dawum'
  AND  format = 'dawum_survey_json'
  AND  status = 'kept';

-- Convenience view: one row per (poll, party) — fully flattened.
CREATE OR REPLACE VIEW dawum_poll_results AS
SELECT
    p.source_record_id,
    p.canonical_url,
    p.poll_date,
    p.parliament_name,
    p.parliament_shortcut,
    p.institute_name,
    p.tasker_name,
    p.sample_size,
    r->>'party_id'                                    AS party_id,
    r->>'party_name'                                  AS party_name,
    r->>'shortcut'                                    AS party_shortcut,
    (r->>'percentage')::numeric                       AS percentage
FROM   dawum_polls p,
       jsonb_array_elements(p.results) AS r;

INSERT INTO schema_versions (version, description)
    VALUES (5, 'source_records lake + dawum_polls / dawum_poll_results views')
    ON CONFLICT (version) DO NOTHING;
