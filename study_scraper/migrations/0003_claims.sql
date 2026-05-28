-- Migration 0003 — claims table for extracted numerical findings.
--
-- Per the 2026-05-28 measurement against example questions ("how many
-- Germans want stricter climate laws / nuclear back?"): current ingest
-- captures a study catalog, but not the headline numerical findings
-- those studies contain. Phase 6-mini adds a small extractor: for each
-- study, find numeric tokens (mostly "X%") and a short context window
-- around them, store as `claims`.
--
-- This is intentionally narrow: regex over title + abstract only,
-- english + german units, no LLM call yet. Better extraction (full
-- text, LLM-based) lands later. The schema is shaped for both.
--
-- See docs/study_scraper/notes/example-questions-2026-05-28.md.

SET search_path TO study_scraper, public;

CREATE TABLE IF NOT EXISTS claims (
    id             CHAR(64)        PRIMARY KEY,
    study_id       CHAR(64)        NOT NULL
                                   REFERENCES studies(id) ON DELETE CASCADE,
    claim_text     TEXT            NOT NULL,     -- the matched snippet incl. context
    numeric_value  NUMERIC,                       -- 62.0 for "62 %"
    unit           TEXT,                          -- '%', 'Mio.', 'Personen', ...
    source_field   TEXT            NOT NULL,     -- 'title' | 'abstract'
    extractor      TEXT            NOT NULL,     -- 'regex-v1' for now
    extracted_at   TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS claims_study_idx       ON claims (study_id);
CREATE INDEX IF NOT EXISTS claims_unit_idx        ON claims (unit);
CREATE INDEX IF NOT EXISTS claims_value_idx       ON claims (numeric_value);
-- Trigram or full-text would be nicer; keep it simple for v1 since
-- volumes are small. The CLI search command uses ILIKE for now.
CREATE INDEX IF NOT EXISTS claims_text_lower_idx
    ON claims (lower(claim_text) text_pattern_ops);

INSERT INTO schema_versions (version, description)
    VALUES (3, 'claims table: extracted numerical findings from study abstracts')
    ON CONFLICT (version) DO NOTHING;
