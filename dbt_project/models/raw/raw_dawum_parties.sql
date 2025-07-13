-- Raw DAWUM parties data
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='id',
    tags=['raw', 'dawum', 'reference']
) }}

SELECT 
    id,
    name,
    shortcut,
    _extracted_at,
    _batch_id
FROM {{ source('dawum', 'raw_parties') }}
WHERE id IS NOT NULL
