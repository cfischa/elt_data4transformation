CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.topic_classifier_runs (
    run_id UUID,
    started_at DateTime64(3),
    completed_at DateTime64(3),
    datasets_scanned UInt64,
    datasets_classified UInt64,
    topics_assigned UInt64,
    taxonomy_version String,
    classifier_version String,
    status String,
    error_message String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY (started_at, run_id)
SETTINGS index_granularity = 8192;
