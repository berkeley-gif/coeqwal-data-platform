-- LOAD CHANNEL ENTITY AND VARIABLE SEED DATA
-- Sources (relative to repo root):
--   database/seed_tables/04_calsim_data/channel_entity.csv    (~670 rows)
--   database/seed_tables/04_variable/channel_variable.csv     (~1352 rows)
--
-- Seed data is loaded via \copy from the local repo — no S3 upload needed.
-- The path 'database/seed_tables/...' is relative to wherever psql is invoked.
--
-- Prerequisites:
--   1. Run 01_create_channel_entity_variable_tables.sql first
--   2. Run from repo root:
--        psql $SUPERUSER_URL -f database/scripts/sql/14_channel_entity/02_load_channel_entity_variable_from_s3.sql

\echo ''
\echo '================================================'
\echo 'LOADING CHANNEL ENTITY AND VARIABLE SEED DATA'
\echo '================================================'


-- ============================================
-- CLEAR EXISTING DATA (safe re-run)
-- ============================================
\echo ''
\echo 'Clearing existing data for clean load...'

TRUNCATE TABLE channel_variable CASCADE;
TRUNCATE TABLE channel_entity  CASCADE;

\echo '✅ Existing data cleared'


-- ============================================
-- 1. LOAD CHANNEL_ENTITY
-- ============================================
-- CSV has no id column — SERIAL assigns ids automatically.
-- Columns: network_arc_id, short_code, name, description, subtype,
--   entity_type_id, schematic_type_id, hydrologic_region_id,
--   boundary_condition, from_node, to_node, length_m,
--   has_tiers, is_main, has_gis_data, entity_version_id, source_ids,
--   watershed_short_code, unimp_sv_variable, has_mif, has_eflows, channel_class

\echo ''
\echo 'Loading channel_entity from repo CSV...'

\copy channel_entity (network_arc_id, short_code, name, description, subtype, entity_type_id, schematic_type_id, hydrologic_region_id, boundary_condition, from_node, to_node, length_m, has_tiers, is_main, has_gis_data, entity_version_id, source_ids, watershed_short_code, unimp_sv_variable, has_mif, has_eflows, channel_class) FROM 'database/seed_tables/04_calsim_data/channel_entity.csv' WITH (FORMAT csv, HEADER true, NULL '')

\echo '✅ channel_entity loaded'


-- ============================================
-- 2. LOAD CHANNEL_VARIABLE
-- ============================================
-- CSV has explicit id column.
-- Columns: id, calsim_id, name, description, channel_entity_id,
--   variable_type, unit_id, temporal_scale_id, variable_version_id,
--   is_regulatory, regulatory_authority, is_aggregate,
--   aggregated_variable_ids, variable_id, source_ids, created_by, updated_by

\echo ''
\echo 'Loading channel_variable from repo CSV...'

\copy channel_variable (id, calsim_id, name, description, channel_entity_id, variable_type, unit_id, temporal_scale_id, variable_version_id, is_regulatory, regulatory_authority, is_aggregate, aggregated_variable_ids, variable_id, source_ids, created_by, updated_by) FROM 'database/seed_tables/04_variable/channel_variable.csv' WITH (FORMAT csv, HEADER true, NULL '')

-- Advance the id sequence past the highest loaded id so future INSERTs don't collide
SELECT setval(
    pg_get_serial_sequence('channel_variable', 'id'),
    (SELECT MAX(id) FROM channel_variable)
);

\echo '✅ channel_variable loaded'


-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Record counts:'
SELECT 'channel_entity'   AS table_name, COUNT(*) AS records FROM channel_entity
UNION ALL
SELECT 'channel_variable' AS table_name, COUNT(*) AS records FROM channel_variable;

\echo ''
\echo 'channel_entity: env-flow attribute summary:'
SELECT
    channel_class,
    COUNT(*)                                                   AS total,
    COUNT(*) FILTER (WHERE watershed_short_code IS NOT NULL)   AS with_watershed,
    COUNT(*) FILTER (WHERE has_mif = TRUE)                     AS with_mif,
    COUNT(*) FILTER (WHERE has_eflows = TRUE)                  AS with_eflows
FROM channel_entity
GROUP BY channel_class
ORDER BY channel_class NULLS LAST;

\echo ''
\echo 'channel_variable: type and regulatory breakdown:'
SELECT
    variable_type,
    COUNT(*)                                        AS total,
    COUNT(*) FILTER (WHERE is_regulatory = TRUE)    AS regulatory
FROM channel_variable
GROUP BY variable_type
ORDER BY total DESC;

\echo ''
\echo '✅ Channel entity and variable data loaded successfully'
