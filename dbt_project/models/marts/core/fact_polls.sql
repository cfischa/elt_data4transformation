-- Core mart for enriched poll data with all dimensions
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='(poll_date, institute_id, party_id)',
    partition_by='toYYYYMM(poll_date)',
    tags=['marts', 'core', 'polls']
) }}

WITH poll_base AS (
    SELECT 
        p.poll_id,
        p.parliament_id,
        p.institute_id,
        p.party_id,
        p.poll_date,
        p.start_date,
        p.end_date,
        p.survey_period,
        p.sample_size,
        p.poll_value,
        p.source_url,
        p.created_at,
        p.updated_at,
        p._extracted_at,
        p._batch_id,
        p.sample_size_quality,
        p.has_invalid_percentage,
        p.is_outdated,
        p.poll_age_days,
        
        -- Institute information
        i.institute_name,
        i.institute_description,
        i.institute_website,
        i.institute_quality_rating,
        i.institute_type,
        i.institute_country,
        
        -- Party information
        pt.party_name_original,
        pt.party_shortcut_original,
        pt.party_name_clean,
        pt.party_shortcut_clean,
        pt.party_color,
        pt.party_spectrum,
        pt.is_major_party,
        pt.is_government_capable
        
    FROM {{ ref('stg_dawum_polls') }} p
    LEFT JOIN {{ ref('stg_dawum_institutes') }} i 
        ON p.institute_id = i.institute_id
    LEFT JOIN {{ ref('stg_dawum_parties') }} pt 
        ON p.party_id = pt.party_id
    WHERE NOT p.has_invalid_percentage
),

poll_with_trends AS (
    SELECT 
        *,
        -- Calculate poll quality score using macro
        {{ generate_poll_quality_score(
            'sample_size', 
            'institute_type', 
            'institute_quality_rating'
        ) }} as poll_quality_score,
        
        -- Calculate trends
        LAG(poll_value, 1) OVER (
            PARTITION BY party_id, institute_id 
            ORDER BY poll_date
        ) as previous_poll_value,
        
        LAG(poll_date, 1) OVER (
            PARTITION BY party_id, institute_id 
            ORDER BY poll_date
        ) as previous_poll_date,
        
        -- Calculate moving averages
        avg(poll_value) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as poll_value_ma3,
        
        avg(poll_value) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as poll_value_ma7,
        
        -- Rank polls by date within party
        row_number() OVER (
            PARTITION BY party_id 
            ORDER BY poll_date DESC
        ) as poll_recency_rank,
        
        -- Rank polls by quality within party and date
        row_number() OVER (
            PARTITION BY party_id, poll_date 
            ORDER BY poll_quality_score DESC
        ) as poll_quality_rank
        
    FROM poll_base
)

SELECT 
    poll_id,
    parliament_id,
    institute_id,
    party_id,
    poll_date,
    start_date,
    end_date,
    survey_period,
    sample_size,
    poll_value,
    source_url,
    created_at,
    updated_at,
    _extracted_at,
    _batch_id,
    
    -- Quality metrics
    sample_size_quality,
    poll_quality_score,
    poll_recency_rank,
    poll_quality_rank,
    
    -- Trend analysis
    previous_poll_value,
    previous_poll_date,
    CASE 
        WHEN previous_poll_value IS NULL THEN 'NEW'
        WHEN poll_value > previous_poll_value THEN 'UP'
        WHEN poll_value < previous_poll_value THEN 'DOWN'
        ELSE 'STABLE'
    END as trend_direction,
    
    COALESCE(poll_value - previous_poll_value, 0) as trend_change_pp,
    
    CASE 
        WHEN previous_poll_value IS NOT NULL AND previous_poll_value > 0 THEN
            round((poll_value - previous_poll_value) / previous_poll_value * 100, 2)
        ELSE 0
    END as trend_change_pct,
    
    COALESCE(dateDiff('day', previous_poll_date, poll_date), 0) as days_since_last_poll,
    
    -- Moving averages
    round(poll_value_ma3, 2) as poll_value_ma3,
    round(poll_value_ma7, 2) as poll_value_ma7,
    
    -- Deviation from moving average
    round(poll_value - poll_value_ma3, 2) as deviation_from_ma3,
    round(poll_value - poll_value_ma7, 2) as deviation_from_ma7,
    
    -- Institute dimensions
    institute_name,
    institute_description,
    institute_website,
    institute_quality_rating,
    institute_type,
    institute_country,
    
    -- Party dimensions
    party_name_original,
    party_shortcut_original,
    party_name_clean,
    party_shortcut_clean,
    party_color,
    party_spectrum,
    is_major_party,
    is_government_capable,
    
    -- Flags
    CASE 
        WHEN poll_recency_rank = 1 THEN TRUE
        ELSE FALSE
    END as is_latest_poll,
    
    CASE 
        WHEN poll_quality_rank = 1 THEN TRUE
        ELSE FALSE
    END as is_highest_quality_poll,
    
    CASE 
        WHEN poll_date >= today() - INTERVAL 30 DAY THEN TRUE
        ELSE FALSE
    END as is_recent_poll,
    
    -- Time dimensions
    toYear(poll_date) as poll_year,
    toMonth(poll_date) as poll_month,
    toQuarter(poll_date) as poll_quarter,
    toDayOfWeek(poll_date) as poll_day_of_week,
    toWeek(poll_date) as poll_week,
    
    -- Data lineage
    now() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id

FROM poll_with_trends
WHERE poll_value IS NOT NULL
  AND party_id IS NOT NULL
  AND institute_id IS NOT NULL
