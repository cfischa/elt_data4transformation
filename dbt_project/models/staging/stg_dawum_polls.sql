-- Staging model for cleaned and standardized poll data
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='(poll_date, institute_id, poll_id)',
    partition_by='toYYYYMM(poll_date)',
    tags=['staging', 'polls']
) }}

WITH poll_results_parsed AS (
    SELECT 
        p.id as poll_id,
        p.parliament_id,
        p.institute_id,
        p.tasker_id,
        p.method_id,
        p.poll_date,
        p.start_date,
        p.end_date,
        p.survey_period,
        p.sample_size,
        p.source_url,
        p.created_at,
        p.updated_at,
        p._extracted_at,
        p._batch_id,
        
        -- Parse JSON results into individual party percentages
        JSONExtract(p.results, 'Map(String, Float64)') as results_map
    FROM {{ ref('raw_dawum_polls') }} p
    WHERE p.results != ''
      AND isValidJSON(p.results)
),

poll_results_flattened AS (
    SELECT 
        poll_id,
        parliament_id,
        institute_id,
        tasker_id,
        method_id,
        poll_date,
        start_date,
        end_date,
        survey_period,
        sample_size,
        source_url,
        created_at,
        updated_at,
        _extracted_at,
        _batch_id,
        
        -- Flatten the results map
        arrayJoin(
            arrayMap(
                (k, v) -> tuple(k, v), 
                mapKeys(results_map), 
                mapValues(results_map)
            )
        ) as party_result,
        
        party_result.1 as party_id,
        party_result.2 as poll_value
    FROM poll_results_parsed
    WHERE length(mapKeys(results_map)) > 0
)

SELECT 
    poll_id,
    parliament_id,
    institute_id,
    tasker_id,
    method_id,
    poll_date,
    start_date,
    end_date,
    survey_period,
    sample_size,
    source_url,
    created_at,
    updated_at,
    _extracted_at,
    _batch_id,
    
    -- Clean party data
    toString(party_id) as party_id,
    poll_value,
    
    -- Data quality flags
    CASE 
        WHEN sample_size < 100 THEN 'LOW_SAMPLE_SIZE'
        WHEN sample_size < 500 THEN 'MEDIUM_SAMPLE_SIZE'
        ELSE 'HIGH_SAMPLE_SIZE'
    END as sample_size_quality,
    
    CASE 
        WHEN poll_value < 0 OR poll_value > 100 THEN TRUE
        ELSE FALSE
    END as has_invalid_percentage,
    
    CASE 
        WHEN dateDiff('day', poll_date, today()) > 365 THEN TRUE
        ELSE FALSE
    END as is_outdated,
    
    -- Calculate poll age
    dateDiff('day', poll_date, today()) as poll_age_days,
    
    -- Generate row hash for change detection
    cityHash64(
        toString(poll_id) || 
        toString(party_id) || 
        toString(poll_value) || 
        toString(sample_size)
    ) as row_hash

FROM poll_results_flattened
WHERE poll_value IS NOT NULL
  AND party_id IS NOT NULL
  AND party_id != ''
