CREATE DATABASE IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.datasets_unified AS
SELECT
    'destatis' AS source,
    cube_code AS dataset_id,
    ifNull(content, cube_code) AS title,
    multiIf(
        length(trim(state)) > 0 AND length(ifNull(time_coverage, '')) > 0,
        concat(state, ' | Zeitabdeckung: ', time_coverage),
        length(trim(state)) > 0,
        state,
        length(ifNull(time_coverage, '')) > 0,
        concat('Zeitabdeckung: ', time_coverage),
        ifNull(content, cube_code)
    ) AS description,
    latest_update,
    arrayFilter(x -> length(trim(x)) > 0, [
        ifNull(state, ''),
        ifNull(time_coverage, '')
    ]) AS keywords,
    [] AS variables,
    toJSONString(map(
        'time_coverage', ifNull(time_coverage, ''),
        'information', ifNull(toString(information), ''),
        'state', ifNull(state, ''),
        'fetched_at', toString(fetched_at)
    )) AS raw_metadata
FROM raw.destatis_metadata

UNION ALL

SELECT
    'eurostat' AS source,
    dataset_code AS dataset_id,
    ifNull(title, dataset_code) AS title,
    ifNull(description, ifNull(title, dataset_code)) AS description,
    last_update,
    ifNull(keywords, []) AS keywords,
    ifNull(dimensions, []) AS variables,
    ifNull(raw_metadata, '') AS raw_metadata
FROM raw.eurostat_metadata

UNION ALL

SELECT
    'gesis' AS source,
    resource_id AS dataset_id,
    ifNull(title, resource_id) AS title,
    ifNull(description, ifNull(title, resource_id)) AS description,
    parseDateTimeBestEffortOrNull(issued) AS latest_update,
    ifNull(JSONExtract(raw_data, 'keywords', 'Array(String)'), []) AS keywords,
    ifNull(JSONExtract(raw_data, 'variables', 'Array(String)'), []) AS variables,
    ifNull(raw_data, '') AS raw_metadata
FROM raw.gesis_metadata

UNION ALL

SELECT
    'soep' AS source,
    slug AS dataset_id,
    ifNull(title, slug) AS title,
    multiIf(
        length(trim(description)) > 0,
        description,
        ifNull(title, slug)
    ) AS description,
    parseDateTimeBestEffortOrNull(last_updated_at) AS latest_update,
    arrayFilter(x -> length(trim(x)) > 0, ifNull(topics, [])) AS keywords,
    arrayFilter(
        x -> length(trim(x)) > 0,
        JSONExtractKeys(ifNull(dimensions, '{}'))
    ) AS variables,
    if(length(trim(raw_detail)) > 0, raw_detail, raw_summary) AS raw_metadata
FROM raw.soep_metadata;
