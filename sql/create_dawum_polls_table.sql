-- ClickHouse DDL for DAWUM polls table
-- Creates a ReplacingMergeTree table partitioned by month with proper indexing

CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.dawum_polls (
    poll_id String,
    institute_id Nullable(String),
    institute_name Nullable(String),
    tasker_id Nullable(String),
    tasker_name Nullable(String),
    parliament_id Nullable(String),
    parliament_name Nullable(String),
    method_id Nullable(String),
    method_name Nullable(String),
    survey_period_start Nullable(DateTime),
    survey_period_end Nullable(DateTime),
    publication_date Nullable(DateTime),
    sample_size Nullable(Int32),
    results String,
    source_url Nullable(String),
    source_loaded_at DateTime,
    created_at DateTime,
    updated_at DateTime,
    event_date Date MATERIALIZED toDate(publication_date)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (poll_id, publication_date)
SETTINGS index_granularity = 8192;

-- Create indexes for better query performance
CREATE INDEX idx_institute_id ON raw.dawum_polls (institute_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX idx_parliament_id ON raw.dawum_polls (parliament_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX idx_publication_date ON raw.dawum_polls (publication_date) TYPE minmax GRANULARITY 3;
