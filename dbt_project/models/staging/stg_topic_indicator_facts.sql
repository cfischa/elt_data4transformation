{{ config(materialized='view') }}

select
    topic_id,
    indicator_id,
    indicator_name,
    source,
    dataset_id,
    geo_code,
    time,
    value,
    unit,
    frequency,
    metadata,
    ingested_at
from {{ source('topics', 'topic_indicator_facts') }}
