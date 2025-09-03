-- ClickHouse DDL for Destatis metadata table
-- Creates a ReplacingMergeTree table partitioned by month with proper indexing

CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.destatis_metadata (
    cube_code String,
    content String DEFAULT '',
    state String DEFAULT '',
    time_coverage String DEFAULT '',
    latest_update Nullable(DateTime),
    information Nullable(Bool),
    fetched_at DateTime,
    source String DEFAULT 'destatis_catalogue',
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    event_date Date MATERIALIZED toDate(fetched_at)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (cube_code, fetched_at)
SETTINGS index_granularity = 8192;

-- Create indexes for common query patterns (only after table creation)
-- Index for code search
ALTER TABLE raw.destatis_metadata ADD INDEX IF NOT EXISTS idx_cube_code cube_code TYPE minmax GRANULARITY 1;

-- Index for content search (only on non-nullable String columns)
ALTER TABLE raw.destatis_metadata ADD INDEX IF NOT EXISTS idx_content content TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1;

-- Index for time-based queries
ALTER TABLE raw.destatis_metadata ADD INDEX IF NOT EXISTS idx_latest_update latest_update TYPE minmax GRANULARITY 1;
