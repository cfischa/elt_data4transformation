-- Migration 0009 — monitoring v1: standing questions ("watches") and
-- per-digest snapshots.
--
-- Product-expansion note (2026-07-04), Axis 1: the scheduled crawl
-- should produce meaning, not just rows. A watch is a standing query
-- against the attribution layer; each `digest` run aggregates the
-- watch's findings (poll-of-polls, ROADMAP D), compares against the
-- previous snapshot, and reports shifts and novelties. Snapshots keep
-- the comparison state in the DB so digests work from cron/CI.

SET search_path TO study_scraper, public;

CREATE TABLE IF NOT EXISTS watches (
    id          SERIAL        PRIMARY KEY,
    query       TEXT          NOT NULL UNIQUE,  -- keyword match against attribution questions
    label       TEXT,                           -- human name for the digest heading
    since_year  INT,                            -- optional recency floor (ask --since)
    active      BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS watch_snapshots (
    id              SERIAL       PRIMARY KEY,
    watch_id        INT          NOT NULL REFERENCES watches(id) ON DELETE CASCADE,
    taken_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    findings_count  INT          NOT NULL,
    -- list of {cluster_label, position, weighted_pct, n_findings}
    payload         JSONB        NOT NULL
);

CREATE INDEX IF NOT EXISTS watch_snapshots_watch_idx
    ON watch_snapshots (watch_id, taken_at DESC);

INSERT INTO schema_versions (version, description)
    VALUES (9, 'monitoring v1: watches + watch_snapshots')
    ON CONFLICT (version) DO NOTHING;
