-- Staging model for parties with standardized names and metadata
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='party_id',
    tags=['staging', 'reference']
) }}

SELECT 
    id as party_id,
    trim(name) as party_name_original,
    trim(shortcut) as party_shortcut_original,
    _extracted_at,
    _batch_id,
    
    -- Standardize party names using macro
    {{ clean_party_name('name') }} as party_name_clean,
    
    -- Standardize party shortcuts
    CASE 
        WHEN shortcut ILIKE '%cdu%' THEN 'CDU'
        WHEN shortcut ILIKE '%spd%' THEN 'SPD'
        WHEN shortcut ILIKE '%grün%' OR shortcut ILIKE '%bündnis%' THEN 'GRÜNE'
        WHEN shortcut ILIKE '%fdp%' THEN 'FDP'
        WHEN shortcut ILIKE '%linke%' THEN 'DIE LINKE'
        WHEN shortcut ILIKE '%afd%' THEN 'AFD'
        WHEN shortcut ILIKE '%bsw%' OR shortcut ILIKE '%wagenknecht%' THEN 'BSW'
        WHEN shortcut ILIKE '%csu%' THEN 'CSU'
        WHEN shortcut ILIKE '%piraten%' THEN 'PIRATEN'
        WHEN shortcut ILIKE '%npd%' THEN 'NPD'
        WHEN shortcut ILIKE '%tierschutz%' THEN 'TIERSCHUTZ'
        WHEN shortcut ILIKE '%freie%' THEN 'FREIE WÄHLER'
        ELSE upper(trim(shortcut))
    END as party_shortcut_clean,
    
    -- Assign party colors using macro
    {{ get_political_party_color('shortcut') }} as party_color,
    
    -- Categorize party by political spectrum
    CASE 
        WHEN shortcut ILIKE '%cdu%' OR shortcut ILIKE '%csu%' THEN 'CONSERVATIVE'
        WHEN shortcut ILIKE '%spd%' THEN 'SOCIAL_DEMOCRATIC'
        WHEN shortcut ILIKE '%grün%' OR shortcut ILIKE '%bündnis%' THEN 'GREEN'
        WHEN shortcut ILIKE '%fdp%' THEN 'LIBERAL'
        WHEN shortcut ILIKE '%linke%' THEN 'LEFT'
        WHEN shortcut ILIKE '%afd%' THEN 'RIGHT_POPULIST'
        WHEN shortcut ILIKE '%bsw%' THEN 'LEFT_POPULIST'
        WHEN shortcut ILIKE '%piraten%' THEN 'DIGITAL_RIGHTS'
        WHEN shortcut ILIKE '%npd%' THEN 'EXTREME_RIGHT'
        WHEN shortcut ILIKE '%tierschutz%' THEN 'SINGLE_ISSUE'
        WHEN shortcut ILIKE '%freie%' THEN 'REGIONAL'
        ELSE 'OTHER'
    END as party_spectrum,
    
    -- Major party flag (parties typically in parliament)
    CASE 
        WHEN shortcut ILIKE '%cdu%' OR shortcut ILIKE '%csu%' OR 
             shortcut ILIKE '%spd%' OR shortcut ILIKE '%grün%' OR 
             shortcut ILIKE '%fdp%' OR shortcut ILIKE '%linke%' OR 
             shortcut ILIKE '%afd%' OR shortcut ILIKE '%bsw%' THEN TRUE
        ELSE FALSE
    END as is_major_party,
    
    -- Government potential (parties that could realistically govern)
    CASE 
        WHEN shortcut ILIKE '%cdu%' OR shortcut ILIKE '%csu%' OR 
             shortcut ILIKE '%spd%' OR shortcut ILIKE '%grün%' OR 
             shortcut ILIKE '%fdp%' THEN TRUE
        ELSE FALSE
    END as is_government_capable,
    
    -- Data quality flags
    CASE 
        WHEN name IS NULL OR trim(name) = '' THEN TRUE
        ELSE FALSE
    END as has_missing_name,
    
    CASE 
        WHEN shortcut IS NULL OR trim(shortcut) = '' THEN TRUE
        ELSE FALSE
    END as has_missing_shortcut,
    
    -- Row hash for change detection
    cityHash64(
        toString(id) || 
        COALESCE(name, '') || 
        COALESCE(shortcut, '')
    ) as row_hash

FROM {{ ref('raw_dawum_parties') }}
WHERE id IS NOT NULL
