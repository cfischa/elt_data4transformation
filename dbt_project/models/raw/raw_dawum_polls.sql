-- Raw DAWUM polls data
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='(poll_date, id)',
    partition_by='toYYYYMM(poll_date)',
    tags=['raw', 'dawum', 'polls']
) }}

SELECT 
    id,
    parliament_id,
    institute_id,
    tasker_id,
    method_id,
    toDate(parseDateTimeBestEffort(poll_date)) as poll_date,
    toDate(parseDateTimeBestEffort(start_date)) as start_date,
    toDate(parseDateTimeBestEffort(end_date)) as end_date,
    survey_period,
    sample_size,
    results,
    source_url,
    toDateTime(parseDateTimeBestEffort(created_at)) as created_at,
    toDateTime(parseDateTimeBestEffort(updated_at)) as updated_at,
    _extracted_at,
    _batch_id
FROM {{ source('dawum', 'raw_polls') }}
WHERE id IS NOT NULL
  AND poll_date IS NOT NULL
  AND results IS NOT NULL
