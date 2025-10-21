CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.topic_selected_payloads (
    run_id UUID,
    dataset_id String,
    source LowCardinality(String),
    topic_id LowCardinality(String),
    classifier_version String,
    taxonomy_version String,
    metadata_hash FixedString(32),
    decided_at DateTime64(3),
    payload_format LowCardinality(String),
    payload String,
    records_count UInt64,
    ingestion_status LowCardinality(String),
    ingestion_started_at DateTime64(3),
    ingestion_completed_at DateTime64(3),
    event_date Date MATERIALIZED toDate(ingestion_started_at)
) ENGINE = ReplacingMergeTree(ingestion_completed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (source, dataset_id, topic_id, ingestion_started_at)
SETTINGS index_granularity = 8192;
