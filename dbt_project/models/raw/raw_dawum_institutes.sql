-- Raw DAWUM institutes data
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='id',
    tags=['raw', 'dawum', 'reference']
) }}

SELECT 
    id,
    name,
    description,
    website,
    _extracted_at,
    _batch_id
FROM {{ source('dawum', 'raw_institutes') }}
WHERE id IS NOT NULL
