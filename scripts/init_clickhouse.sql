-- ClickHouse initialization script
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS political_data;

-- Create raw schema tables
USE political_data;

CREATE TABLE IF NOT EXISTS raw.dawum_polls (
    id UInt32,
    parliament_id UInt32,
    institute_id UInt32,
    tasker_id UInt32,
    method_id UInt32,
    poll_date Date,
    start_date Date,
    end_date Date,
    survey_period String,
    sample_size UInt32,
    results String,
    source_url String,
    created_at DateTime,
    updated_at DateTime,
    _extracted_at DateTime DEFAULT now(),
    _batch_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(poll_date)
ORDER BY (poll_date, id);
