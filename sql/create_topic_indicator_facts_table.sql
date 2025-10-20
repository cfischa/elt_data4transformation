CREATE DATABASE IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.topic_indicator_facts (
    topic_id String,
    indicator_id String,
    indicator_name String,
    source String,
    dataset_id String,
    geo_code String,
    time DateTime,
    value Float64,
    unit String,
    frequency String,
    metadata String,
    ingested_at DateTime64(3),
    event_month Date MATERIALIZED toDate(time)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_month)
ORDER BY (topic_id, indicator_id, geo_code, time)
SETTINGS index_granularity = 8192;
