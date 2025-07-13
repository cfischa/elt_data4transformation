-- Additional DDL operations for DAWUM polls table management

-- View to check table statistics
CREATE VIEW IF NOT EXISTS raw.dawum_polls_stats AS
SELECT 
    COUNT(*) as total_polls,
    COUNT(DISTINCT poll_id) as unique_polls,
    COUNT(DISTINCT institute_name) as unique_institutes,
    COUNT(DISTINCT parliament_name) as unique_parliaments,
    MIN(publication_date) as earliest_poll,
    MAX(publication_date) as latest_poll,
    MIN(source_loaded_at) as first_load,
    MAX(source_loaded_at) as last_load,
    formatReadableSize(sum(bytes)) as table_size
FROM raw.dawum_polls
LEFT JOIN system.parts ON table = 'dawum_polls';

-- Maintenance queries

-- Force optimization to merge ReplacingMergeTree duplicates
-- ALTER TABLE raw.dawum_polls OPTIMIZE FINAL;

-- Check partition information
-- SELECT partition, count() as rows, formatReadableSize(sum(bytes)) as size
-- FROM system.parts 
-- WHERE table = 'dawum_polls' 
-- GROUP BY partition 
-- ORDER BY partition;

-- Drop old partitions (example - adjust date range as needed)
-- ALTER TABLE raw.dawum_polls DROP PARTITION '202312';

-- Truncate table (for testing)
-- TRUNCATE TABLE raw.dawum_polls;
