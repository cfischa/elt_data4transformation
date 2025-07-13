-- Aggregated daily poll averages by party
{{ config(
    materialized='view',
    tags=['marts', 'aggregates', 'daily']
) }}

WITH daily_polls AS (
    SELECT 
        poll_date,
        party_id,
        party_name_clean,
        party_shortcut_clean,
        party_color,
        party_spectrum,
        is_major_party,
        
        -- Basic aggregations
        count(*) as poll_count,
        avg(poll_value) as avg_poll_value,
        median(poll_value) as median_poll_value,
        min(poll_value) as min_poll_value,
        max(poll_value) as max_poll_value,
        stddevPop(poll_value) as stddev_poll_value,
        
        -- Quality-weighted average
        sum(poll_value * poll_quality_score) / sum(poll_quality_score) as weighted_avg_poll_value,
        
        -- Sample size information
        sum(sample_size) as total_sample_size,
        avg(sample_size) as avg_sample_size,
        max(sample_size) as max_sample_size,
        
        -- Quality metrics
        avg(poll_quality_score) as avg_quality_score,
        max(poll_quality_score) as max_quality_score,
        
        -- Institute diversity
        uniq(institute_id) as unique_institutes,
        
        -- Data quality flags
        sum(CASE WHEN sample_size_quality = 'HIGH_SAMPLE_SIZE' THEN 1 ELSE 0 END) as high_quality_polls,
        sum(CASE WHEN sample_size_quality = 'LOW_SAMPLE_SIZE' THEN 1 ELSE 0 END) as low_quality_polls
        
    FROM {{ ref('fact_polls') }}
    WHERE poll_date >= today() - INTERVAL 2 YEAR
    GROUP BY 
        poll_date,
        party_id,
        party_name_clean,
        party_shortcut_clean,
        party_color,
        party_spectrum,
        is_major_party
),

daily_with_trends AS (
    SELECT 
        *,
        -- Calculate daily trends
        LAG(weighted_avg_poll_value, 1) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date
        ) as previous_day_value,
        
        LAG(poll_date, 1) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date
        ) as previous_poll_date,
        
        -- Rolling averages
        avg(weighted_avg_poll_value) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7day_avg,
        
        avg(weighted_avg_poll_value) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30day_avg,
        
        -- Volatility measures
        stddevPop(weighted_avg_poll_value) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30day_volatility
        
    FROM daily_polls
)

SELECT 
    poll_date,
    party_id,
    party_name_clean,
    party_shortcut_clean,
    party_color,
    party_spectrum,
    is_major_party,
    
    -- Poll counts and basic stats
    poll_count,
    round(avg_poll_value, 2) as avg_poll_value,
    round(median_poll_value, 2) as median_poll_value,
    round(min_poll_value, 2) as min_poll_value,
    round(max_poll_value, 2) as max_poll_value,
    round(stddev_poll_value, 2) as stddev_poll_value,
    
    -- Quality-weighted metrics
    round(weighted_avg_poll_value, 2) as weighted_avg_poll_value,
    round(avg_quality_score, 1) as avg_quality_score,
    max_quality_score,
    
    -- Sample size information
    total_sample_size,
    round(avg_sample_size, 0) as avg_sample_size,
    max_sample_size,
    
    -- Institute diversity
    unique_institutes,
    
    -- Data quality
    high_quality_polls,
    low_quality_polls,
    round(high_quality_polls / poll_count * 100, 1) as high_quality_pct,
    
    -- Trend analysis
    previous_day_value,
    CASE 
        WHEN previous_day_value IS NULL THEN 'NEW'
        WHEN weighted_avg_poll_value > previous_day_value THEN 'UP'
        WHEN weighted_avg_poll_value < previous_day_value THEN 'DOWN'
        ELSE 'STABLE'
    END as trend_direction,
    
    COALESCE(weighted_avg_poll_value - previous_day_value, 0) as trend_change_pp,
    
    -- Rolling averages
    round(rolling_7day_avg, 2) as rolling_7day_avg,
    round(rolling_30day_avg, 2) as rolling_30day_avg,
    round(rolling_30day_volatility, 2) as rolling_30day_volatility,
    
    -- Deviation from rolling averages
    round(weighted_avg_poll_value - rolling_7day_avg, 2) as deviation_from_7day_avg,
    round(weighted_avg_poll_value - rolling_30day_avg, 2) as deviation_from_30day_avg,
    
    -- Time dimensions
    toYear(poll_date) as poll_year,
    toMonth(poll_date) as poll_month,
    toQuarter(poll_date) as poll_quarter,
    toDayOfWeek(poll_date) as poll_day_of_week,
    
    -- Flags
    CASE 
        WHEN poll_date = today() - INTERVAL 1 DAY THEN TRUE
        ELSE FALSE
    END as is_yesterday,
    
    CASE 
        WHEN poll_date >= today() - INTERVAL 7 DAY THEN TRUE
        ELSE FALSE
    END as is_last_week,
    
    CASE 
        WHEN poll_count >= 3 THEN TRUE
        ELSE FALSE
    END as has_sufficient_polls,
    
    -- Data lineage
    now() as dbt_updated_at

FROM daily_with_trends
ORDER BY poll_date DESC, party_shortcut_clean
