-- Migration 0002 — Q12 candidates review queue.
--
-- Per DECISIONS.md A12 + Q12 (yes): we store everything the source
-- yields and survives the exclude short-circuit, regardless of score.
-- A `status` column distinguishes the cases:
--
--   pending  — recorded but below the topic-filter threshold; awaits
--              human review in the dock.
--   kept     — passed the threshold OR a human promoted it. Default.
--   rejected — a human dismissed it via the dock / review CLI.
--              Sticky: re-discovery does not overturn the decision.
--
-- This avoids a second table: candidates and kept studies share the
-- same shape and lifecycle; only `status` differs.

SET search_path TO study_scraper, public;

ALTER TABLE studies
    ADD COLUMN IF NOT EXISTS status           TEXT        NOT NULL DEFAULT 'kept',
    ADD COLUMN IF NOT EXISTS rejected_reason  TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_by      TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_at      TIMESTAMPTZ;

-- Constraint added after the columns exist (idempotent).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'studies_status_check'
    ) THEN
        ALTER TABLE studies
            ADD CONSTRAINT studies_status_check
            CHECK (status IN ('pending', 'kept', 'rejected'));
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS studies_status_idx
    ON studies (status);

-- Composite index for the dock's "pending review per topic" query.
CREATE INDEX IF NOT EXISTS studies_status_topic_idx
    ON studies (status)
    INCLUDE (topic_ids);

INSERT INTO schema_versions (version, description)
    VALUES (2, 'studies.status: pending/kept/rejected for the Q12 review queue')
    ON CONFLICT (version) DO NOTHING;
