-- Staging model for institutes with enhanced metadata
{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='institute_id',
    tags=['staging', 'reference']
) }}

SELECT 
    id as institute_id,
    trim(name) as institute_name,
    trim(description) as institute_description,
    trim(website) as institute_website,
    _extracted_at,
    _batch_id,
    
    -- Add institute quality rating based on known criteria
    CASE 
        WHEN name ILIKE '%forsa%' THEN 85
        WHEN name ILIKE '%infratest%' THEN 90
        WHEN name ILIKE '%emnid%' THEN 85
        WHEN name ILIKE '%gms%' THEN 80
        WHEN name ILIKE '%insa%' THEN 75
        WHEN name ILIKE '%allensbach%' THEN 90
        WHEN name ILIKE '%ipsos%' THEN 85
        WHEN name ILIKE '%yougov%' THEN 80
        WHEN name ILIKE '%kantar%' THEN 85
        ELSE 70
    END as institute_quality_rating,
    
    -- Categorize institute type
    CASE 
        WHEN name ILIKE '%forsa%' OR name ILIKE '%infratest%' OR name ILIKE '%emnid%' THEN 'TRADITIONAL'
        WHEN name ILIKE '%yougov%' OR name ILIKE '%civey%' THEN 'ONLINE'
        WHEN name ILIKE '%allensbach%' THEN 'ACADEMIC'
        ELSE 'OTHER'
    END as institute_type,
    
    -- Extract country from website or institute name
    CASE 
        WHEN website ILIKE '%.de%' OR name ILIKE '%deutsch%' THEN 'DE'
        WHEN website ILIKE '%.at%' OR name ILIKE '%österreich%' THEN 'AT'
        WHEN website ILIKE '%.ch%' OR name ILIKE '%schweiz%' THEN 'CH'
        ELSE 'DE'
    END as institute_country,
    
    -- Data quality flags
    CASE 
        WHEN name IS NULL OR trim(name) = '' THEN TRUE
        ELSE FALSE
    END as has_missing_name,
    
    CASE 
        WHEN website IS NULL OR trim(website) = '' THEN TRUE
        ELSE FALSE
    END as has_missing_website,
    
    -- Row hash for change detection
    cityHash64(
        toString(id) || 
        COALESCE(name, '') || 
        COALESCE(description, '') || 
        COALESCE(website, '')
    ) as row_hash

FROM {{ ref('raw_dawum_institutes') }}
WHERE id IS NOT NULL
