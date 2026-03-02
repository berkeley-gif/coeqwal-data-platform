-- LOAD CHANNEL ENTITY AND VARIABLE SEED DATA
-- Sources (relative to repo root):
--   database/seed_tables/04_calsim_data/channel_entity.csv    (~669 rows)
--   database/seed_tables/04_variable/channel_variable.csv     (~1352 rows)
--
-- Seed data is loaded via \copy from the local repo — no S3 upload needed.
-- The path 'database/seed_tables/...' is relative to wherever psql is invoked.
--
-- Prerequisites:
--   1. Run 01_create_channel_entity_variable_tables.sql first
--   2. Run from repo root:
--        psql $SUPERUSER_URL -f database/scripts/sql/14_channel_entity/02_load_channel_entity_variable.sql

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
-- The CSV has channel_entity_id values referencing old integer IDs that no
-- longer match the SERIAL ids assigned above.  Strategy:
--   a) Drop the FK constraint so the old IDs load without error
--   b) Load all columns from CSV (id column is ignored — SERIAL assigns new ids)
--   c) Repopulate channel_entity_id via a natural-key join on network_arc_id
--   d) Restore the FK constraint
--
-- CSV columns: id(ignored), calsim_id, name, description, channel_entity_id(stale),
--   variable_type, unit_id, temporal_scale_id, variable_version_id,
--   is_regulatory, regulatory_authority, is_aggregate,
--   aggregated_variable_ids, variable_id, source_ids, created_by, updated_by

\echo ''
\echo 'Dropping channel_entity_id FK for clean load...'

ALTER TABLE channel_variable
    DROP CONSTRAINT IF EXISTS channel_variable_channel_entity_id_fkey;

\echo 'Loading channel_variable from repo CSV...'

-- Skip the stale `id` and `channel_entity_id` columns from the CSV.
-- Load into a temp staging table that matches the CSV column order exactly,
-- then insert only the columns we want.

DROP TABLE IF EXISTS cv_stage;
CREATE TEMP TABLE cv_stage (
    _id                     INTEGER,
    calsim_id               VARCHAR(40),
    name                    VARCHAR(200),
    description             TEXT,
    _channel_entity_id      INTEGER,   -- stale; will be resolved via join
    variable_type           VARCHAR(50),
    unit_id                 INTEGER,
    temporal_scale_id       INTEGER,
    variable_version_id     INTEGER,
    is_regulatory           BOOLEAN,
    regulatory_authority    VARCHAR(100),
    is_aggregate            BOOLEAN,
    aggregated_variable_ids TEXT,
    variable_id             UUID,
    source_ids              TEXT,
    created_by              INTEGER,
    updated_by              INTEGER
);

\copy cv_stage FROM 'database/seed_tables/04_variable/channel_variable.csv' WITH (FORMAT csv, HEADER true, NULL '')

INSERT INTO channel_variable
    (calsim_id, name, description,
     variable_type, unit_id, temporal_scale_id, variable_version_id,
     is_regulatory, regulatory_authority,
     is_aggregate, aggregated_variable_ids, variable_id,
     source_ids, created_by, updated_by)
SELECT
    calsim_id, name, description,
    variable_type, unit_id, temporal_scale_id, variable_version_id,
    is_regulatory, regulatory_authority,
    is_aggregate, aggregated_variable_ids, variable_id,
    source_ids, created_by, updated_by
FROM cv_stage;

\echo 'Linking channel_entity_id via network_arc_id...'

-- Standard flow variables: calsim_id == network_arc_id exactly
UPDATE channel_variable cv
SET channel_entity_id = ce.id
FROM channel_entity ce
WHERE cv.calsim_id = ce.network_arc_id;

\echo '✅ channel_variable loaded and linked'


-- Restore FK now that channel_entity_id is populated
ALTER TABLE channel_variable
    ADD CONSTRAINT channel_variable_channel_entity_id_fkey
    FOREIGN KEY (channel_entity_id) REFERENCES channel_entity(id);

\echo '✅ FK constraint restored'


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
\echo 'channel_variable: link and regulatory summary:'
SELECT
    variable_type,
    COUNT(*)                                                   AS total,
    COUNT(*) FILTER (WHERE channel_entity_id IS NOT NULL)      AS linked,
    COUNT(*) FILTER (WHERE is_regulatory = TRUE)               AS regulatory
FROM channel_variable
GROUP BY variable_type
ORDER BY total DESC;

\echo ''
\echo '✅ Channel entity and variable data loaded successfully'
