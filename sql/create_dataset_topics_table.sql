CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dataset_topics (
    dataset_id String,
    source LowCardinality(String),
    topic_id LowCardinality(String),
    score Float32,
    classifier_version String,
    matched_terms String,
    rationale String,
    metadata_hash FixedString(32),
    decided_at DateTime64(3),
    event_date Date MATERIALIZED toDate(decided_at)
) ENGINE = ReplacingMergeTree(decided_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (source, dataset_id, topic_id, decided_at)
SETTINGS index_granularity = 8192;
