-- Migration 0008 — attributions table (Option A: llm-v1 extractor).
--
-- Per maintainer 2026-06-15: an LLM extractor turns regex claims +
-- study text into structured (question, position, percentage) triples
-- — the real shape of an issue-polling system. The regex passes
-- (regex-v1 abstract, regex-v2 full text) find numbers-with-context;
-- the LLM pass adds WHAT each number answers.
--
-- An attribution can be backed by a `study` (academic catalog) or a
-- `source_record` (lake) — same dual-target shape as `claims`.

SET search_path TO study_scraper, public;

CREATE TABLE IF NOT EXISTS attributions (
    id               CHAR(64)      PRIMARY KEY,
                     -- sha256(target_id|question|position|percentage|model)
    study_id         CHAR(64)      REFERENCES studies(id)        ON DELETE CASCADE,
    source_record_id CHAR(64)      REFERENCES source_records(id) ON DELETE CASCADE,
    question         TEXT          NOT NULL,   -- the survey question, normalized
    position         TEXT          NOT NULL,   -- support|oppose|neutral|unspecified
    percentage       NUMERIC,                  -- the figure, 0..100 (NULL if none)
    population       TEXT,                      -- who was asked (best effort)
    confidence       NUMERIC,                   -- model self-report, 0..1
    model            TEXT          NOT NULL,   -- 'llm-v1' or the model id used
    raw              JSONB,                     -- the model's full item, audit
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Exactly one target (mirrors claims_target_one_chk).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'attributions_target_one_chk'
    ) THEN
        ALTER TABLE attributions
            ADD CONSTRAINT attributions_target_one_chk
            CHECK (
                (study_id IS NOT NULL AND source_record_id IS NULL) OR
                (study_id IS NULL AND source_record_id IS NOT NULL)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'attributions_position_chk'
    ) THEN
        ALTER TABLE attributions
            ADD CONSTRAINT attributions_position_chk
            CHECK (position IN ('support', 'oppose', 'neutral', 'unspecified'));
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS attributions_study_idx        ON attributions (study_id);
CREATE INDEX IF NOT EXISTS attributions_source_rec_idx   ON attributions (source_record_id);
CREATE INDEX IF NOT EXISTS attributions_model_idx        ON attributions (model);
CREATE INDEX IF NOT EXISTS attributions_question_trgm_idx
    ON attributions USING GIN (lower(question) public.gin_trgm_ops);

-- Studies still missing an LLM attribution pass — the `attribute`
-- queue. A study qualifies once it has at least one claim (something
-- to attribute) but no attribution yet.
CREATE OR REPLACE VIEW attribution_queue AS
SELECT s.id, s.title, s.canonical_url, s.source_id, s.topic_ids
FROM   studies s
WHERE  s.status = 'kept'
  AND  EXISTS (SELECT 1 FROM claims c WHERE c.study_id = s.id)
  AND  NOT EXISTS (SELECT 1 FROM attributions a WHERE a.study_id = s.id)
ORDER  BY s.fetched_at DESC;

INSERT INTO schema_versions (version, description)
    VALUES (8, 'attributions table (llm-v1) + attribution_queue view')
    ON CONFLICT (version) DO NOTHING;
