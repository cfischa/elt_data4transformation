-- Latest poll standings for each party
{{ config(
    materialized='view',
    tags=['marts', 'aggregates', 'current']
) }}

WITH latest_polls AS (
    SELECT 
        party_id,
        party_name_clean,
        party_shortcut_clean,
        party_color,
        party_spectrum,
        is_major_party,
        is_government_capable,
        
        -- Get the most recent poll for each party
        argMax(poll_value, poll_date) as latest_poll_value,
        argMax(poll_date, poll_date) as latest_poll_date,
        argMax(institute_name, poll_date) as latest_poll_institute,
        argMax(sample_size, poll_date) as latest_poll_sample_size,
        argMax(poll_quality_score, poll_date) as latest_poll_quality,
        
        -- Get the previous poll for trend calculation
        nth_value(poll_value, 2) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date DESC
        ) as previous_poll_value,
        
        nth_value(poll_date, 2) OVER (
            PARTITION BY party_id 
            ORDER BY poll_date DESC
        ) as previous_poll_date,
        
        -- Calculate recent averages
        avg(poll_value) FILTER (
            WHERE poll_date >= today() - INTERVAL 7 DAY
        ) as avg_7day,
        
        avg(poll_value) FILTER (
            WHERE poll_date >= today() - INTERVAL 30 DAY
        ) as avg_30day,
        
        avg(poll_value) FILTER (
            WHERE poll_date >= today() - INTERVAL 90 DAY
        ) as avg_90day,
        
        -- Count recent polls
        count(*) FILTER (
            WHERE poll_date >= today() - INTERVAL 7 DAY
        ) as polls_7day,
        
        count(*) FILTER (
            WHERE poll_date >= today() - INTERVAL 30 DAY
        ) as polls_30day,
        
        -- Calculate volatility
        stddevPop(poll_value) FILTER (
            WHERE poll_date >= today() - INTERVAL 30 DAY
        ) as volatility_30day,
        
        -- Get highest and lowest recent values
        max(poll_value) FILTER (
            WHERE poll_date >= today() - INTERVAL 90 DAY
        ) as high_90day,
        
        min(poll_value) FILTER (
            WHERE poll_date >= today() - INTERVAL 90 DAY
        ) as low_90day
        
    FROM {{ ref('fact_polls') }}
    WHERE poll_date >= today() - INTERVAL 1 YEAR
    GROUP BY 
        party_id,
        party_name_clean,
        party_shortcut_clean,
        party_color,
        party_spectrum,
        is_major_party,
        is_government_capable
),

latest_with_rankings AS (
    SELECT 
        *,
        -- Rank parties by latest poll value
        row_number() OVER (ORDER BY latest_poll_value DESC) as current_rank,
        
        -- Rank parties by 30-day average
        row_number() OVER (ORDER BY avg_30day DESC) as rank_30day_avg,
        
        -- Calculate market share among major parties
        latest_poll_value / sum(latest_poll_value) FILTER (
            WHERE is_major_party
        ) OVER () * 100 as market_share_major_parties
        
    FROM latest_polls
    WHERE latest_poll_value IS NOT NULL
)

SELECT 
    party_id,
    party_name_clean,
    party_shortcut_clean,
    party_color,
    party_spectrum,
    is_major_party,
    is_government_capable,
    
    -- Current standings
    current_rank,
    round(latest_poll_value, 1) as latest_poll_value,
    latest_poll_date,
    latest_poll_institute,
    latest_poll_sample_size,
    latest_poll_quality,
    
    -- Trend analysis
    round(previous_poll_value, 1) as previous_poll_value,
    previous_poll_date,
    
    CASE 
        WHEN previous_poll_value IS NULL THEN 'NEW'
        WHEN latest_poll_value > previous_poll_value THEN 'UP'
        WHEN latest_poll_value < previous_poll_value THEN 'DOWN'
        ELSE 'STABLE'
    END as trend_direction,
    
    round(latest_poll_value - COALESCE(previous_poll_value, 0), 1) as trend_change_pp,
    
    dateDiff('day', previous_poll_date, latest_poll_date) as days_since_previous,
    
    -- Recent averages
    round(avg_7day, 1) as avg_7day,
    round(avg_30day, 1) as avg_30day,
    round(avg_90day, 1) as avg_90day,
    
    -- Poll frequency
    polls_7day,
    polls_30day,
    
    -- Volatility and ranges
    round(volatility_30day, 1) as volatility_30day,
    round(high_90day, 1) as high_90day,
    round(low_90day, 1) as low_90day,
    round(high_90day - low_90day, 1) as range_90day,
    
    -- Rankings
    rank_30day_avg,
    current_rank - rank_30day_avg as rank_change_vs_30day,
    
    -- Market share
    round(market_share_major_parties, 1) as market_share_major_parties,
    
    -- Performance indicators
    CASE 
        WHEN latest_poll_value >= 5.0 THEN 'LIKELY_IN_PARLIAMENT'
        WHEN latest_poll_value >= 3.0 THEN 'MARGINAL'
        ELSE 'UNLIKELY'
    END as parliament_likelihood,
    
    CASE 
        WHEN latest_poll_value >= 30.0 THEN 'LEADING'
        WHEN latest_poll_value >= 20.0 THEN 'STRONG'
        WHEN latest_poll_value >= 10.0 THEN 'MODERATE'
        WHEN latest_poll_value >= 5.0 THEN 'WEAK'
        ELSE 'MARGINAL'
    END as strength_category,
    
    -- Coalition potential
    CASE 
        WHEN is_government_capable AND latest_poll_value >= 15.0 THEN 'SENIOR_PARTNER'
        WHEN is_government_capable AND latest_poll_value >= 5.0 THEN 'JUNIOR_PARTNER'
        WHEN latest_poll_value >= 5.0 THEN 'OPPOSITION'
        ELSE 'MARGINAL'
    END as coalition_potential,
    
    -- Data quality flags
    CASE 
        WHEN dateDiff('day', latest_poll_date, today()) <= 7 THEN 'FRESH'
        WHEN dateDiff('day', latest_poll_date, today()) <= 30 THEN 'RECENT'
        ELSE 'STALE'
    END as data_freshness,
    
    CASE 
        WHEN polls_30day >= 10 THEN 'HIGH_FREQUENCY'
        WHEN polls_30day >= 5 THEN 'MEDIUM_FREQUENCY'
        WHEN polls_30day >= 1 THEN 'LOW_FREQUENCY'
        ELSE 'NO_RECENT_POLLS'
    END as polling_frequency,
    
    -- Data lineage
    now() as dbt_updated_at

FROM latest_with_rankings
ORDER BY current_rank
