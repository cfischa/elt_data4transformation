-- View helper query for topic-selected datasets.
-- Returns distinct dataset/topic pairs that cleared the high confidence threshold,
-- enriched with canonical metadata for downstream extraction.

SELECT
    dt.source,
    dt.dataset_id,
    dt.topic_id,
    argMax(dt.score, dt.decided_at) AS score,
    argMax(dt.decided_at, dt.decided_at) AS decided_at,
    argMax(dt.classifier_version, dt.decided_at) AS classifier_version,
    argMax(dt.metadata_hash, dt.decided_at) AS metadata_hash,
    argMax(dm.title, dt.decided_at) AS title,
    argMax(dm.description, dt.decided_at) AS description,
    argMax(dm.latest_update, dt.decided_at) AS latest_update,
    argMax(dm.keywords, dt.decided_at) AS keywords,
    argMax(dm.variables, dt.decided_at) AS variables,
    argMax(dm.raw_metadata, dt.decided_at) AS raw_metadata
FROM analytics.dataset_topics AS dt
LEFT JOIN analytics.datasets_unified AS dm
    ON dt.source = dm.source
   AND dt.dataset_id = dm.dataset_id
GROUP BY
    dt.source,
    dt.dataset_id,
    dt.topic_id;
