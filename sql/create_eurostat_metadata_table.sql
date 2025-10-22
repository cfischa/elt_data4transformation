CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.eurostat_metadata (
    dataset_code String,
    title String,
    description String,
    last_update Nullable(DateTime64(3)),
    keywords Array(String),
    themes Array(String),
    dimensions Array(String),
    values_count UInt64,
    fetched_at DateTime64(3),
    raw_metadata String,
    event_date Date MATERIALIZED toDate(fetched_at)
) ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY dataset_code
SETTINGS index_granularity = 8192;
