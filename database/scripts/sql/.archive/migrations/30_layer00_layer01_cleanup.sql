-- Migration 30: Layer 00 + Layer 01 cleanup
-- Run from Cloud9: psql $DATABASE_URL -f database/scripts/sql/migrations/30_layer00_layer01_cleanup.sql
--
-- Changes:
--    1.  developer: update id=2 affiliation
--    2.  version_family: rename "infrastructure" → "audit"
--    3.  version: drop unused manifest column
--    4.  domain_family_map: fix attribution (created_by/updated_by → 2)
--    5.  domain_family_map: add database_level column and populate
--    6.  model_source: drop version_family_id column
--    7.  source: fix record 35 attribution
--    8.  source: resequence IDs (32→9, 33→10, 34→11, 35→12)
--    9.  network_subtype: drop redundant network_entity_type_id column
--   10.  wba: drop legacy hydrologic_region text column
--   11.  wba: set record 1 (DETAW) hydrologic_region_id = DELTA (id=3)
--   12.  statistic_category: create lookup table (summary, percentile_band, exceedance)
--   13.  statistic_type: resequence IDs and add exceedance percentiles
--   14.  statistic_type: replace is_percentile with statistic_category_id FK
--
-- NOTE: DDL statements (ALTER TABLE, CREATE TABLE) require table ownership.
--       SET ROLE postgres is used only around DDL; DML runs as your own role
--       so audit triggers record the correct developer.

BEGIN;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. developer: update affiliation for id=2
-- ═══════════════════════════════════════════════════════════════════════════
UPDATE developer
SET    affiliation = 'Berkeley Geospatial Innovation Facility',
       updated_at  = NOW()
WHERE  id = 2;

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. version_family: rename "infrastructure" → "audit"
-- ═══════════════════════════════════════════════════════════════════════════
UPDATE version_family
SET    short_code   = 'audit',
       label        = 'Audit',
       description  = 'Layer 00 system tables: versioning, developer registry, domain mapping, audit log',
       updated_at   = NOW()
WHERE  id = 14;

-- ═══════════════════════════════════════════════════════════════════════════
-- 3. version: drop unused manifest column  [DDL]
-- ═══════════════════════════════════════════════════════════════════════════
SET ROLE postgres;
ALTER TABLE version DROP COLUMN IF EXISTS manifest;
RESET ROLE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 4. domain_family_map: fix attribution
-- ═══════════════════════════════════════════════════════════════════════════
UPDATE domain_family_map
SET    created_by = 2,
       updated_by = 2,
       updated_at = NOW()
WHERE  created_by = 1 OR updated_by = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- 5. domain_family_map: add database_level column and populate
-- ═══════════════════════════════════════════════════════════════════════════
SET ROLE postgres;
ALTER TABLE domain_family_map
    ADD COLUMN IF NOT EXISTS database_level TEXT;
RESET ROLE;

UPDATE domain_family_map SET database_level = CASE table_name
    -- Layer 00: versioning
    WHEN 'developer'                        THEN '00'
    WHEN 'version_family'                   THEN '00'
    WHEN 'version'                          THEN '00'
    WHEN 'domain_family_map'                THEN '00'
    WHEN 'audit_log'                        THEN '00'
    -- Layer 01: lookups
    WHEN 'hydrologic_region'                THEN '01'
    WHEN 'source'                           THEN '01'
    WHEN 'model_source'                     THEN '01'
    WHEN 'unit'                             THEN '01'
    WHEN 'spatial_scale'                    THEN '01'
    WHEN 'temporal_scale'                   THEN '01'
    WHEN 'statistic_category'               THEN '01'
    WHEN 'statistic_type'                   THEN '01'
    WHEN 'geometry_type'                    THEN '01'
    WHEN 'network_type'                     THEN '01'
    WHEN 'network_subtype'                  THEN '01'
    WHEN 'network_entity_type'              THEN '01'
    WHEN 'watershed'                        THEN '01'
    -- Layer 02: network
    WHEN 'network'                          THEN '02'
    WHEN 'network_arc'                      THEN '02'
    WHEN 'network_node'                     THEN '02'
    WHEN 'network_gis'                      THEN '02'
    -- Layer 03: entity
    WHEN 'reservoir'                        THEN '03'
    WHEN 'reservoir_entity'                 THEN '03'
    WHEN 'reservoir_group'                  THEN '03'
    WHEN 'reservoir_group_member'           THEN '03'
    WHEN 'compliance_station'               THEN '03'
    WHEN 'du_agriculture_entity'            THEN '03'
    WHEN 'du_urban_entity'                  THEN '03'
    WHEN 'du_urban_group'                   THEN '03'
    WHEN 'du_urban_group_member'            THEN '03'
    WHEN 'du_refuge_entity'                 THEN '03'
    WHEN 'mi_contractor'                    THEN '03'
    WHEN 'mi_contractor_group'              THEN '03'
    WHEN 'mi_contractor_group_member'       THEN '03'
    WHEN 'ag_aggregate_entity'              THEN '03'
    WHEN 'cws_aggregate_entity'             THEN '03'
    WHEN 'channel_entity'                   THEN '03'
    WHEN 'wba'                              THEN '03'
    -- Layer 04: variable
    WHEN 'calsim_model_variable_type'       THEN '04'
    WHEN 'derived_variable_type'            THEN '04'
    WHEN 'variable_type'                    THEN '04'
    WHEN 'channel_variable'                 THEN '04'
    WHEN 'du_urban_variable'                THEN '04'
    -- Layer 05: assumptions + operations
    WHEN 'assumption_category'              THEN '05'
    WHEN 'assumption_definition'            THEN '05'
    WHEN 'operation_category'               THEN '05'
    WHEN 'operation_definition'             THEN '05'
    WHEN 'scenario_key_assumption_link'     THEN '05'
    WHEN 'scenario_key_operation_link'      THEN '05'
    -- Layer 06: scenario
    WHEN 'scenario'                         THEN '06'
    WHEN 'scenario_author'                  THEN '06'
    -- Layer 07: hydroclimate
    WHEN 'hydroclimate'                     THEN '07'
    WHEN 'slr'                              THEN '07'
    -- Layer 08: theme
    WHEN 'theme'                            THEN '08'
    WHEN 'theme_scenario_link'              THEN '08'
    -- Layer 09: geospatial infrastructure
    WHEN 'spatial_ref_sys'                  THEN '09'
    -- Layer 10+: delivery arcs + results
    WHEN 'du_urban_delivery_arc'            THEN '10'
    WHEN 'mi_contractor_delivery_arc'       THEN '10'
    WHEN 'tier_definition'                  THEN '10'
    WHEN 'tier_result'                      THEN '10'
    WHEN 'tier_location_result'             THEN '10'
    WHEN 'reservoir_storage_monthly'        THEN '11'
    WHEN 'reservoir_spill_monthly'          THEN '11'
    WHEN 'reservoir_period_summary'         THEN '11'
    WHEN 'reservoir_monthly_percentile'     THEN '11'
    WHEN 'du_delivery_monthly'              THEN '12'
    WHEN 'du_shortage_monthly'              THEN '12'
    WHEN 'du_period_summary'                THEN '12'
    WHEN 'mi_delivery_monthly'              THEN '12'
    WHEN 'mi_shortage_monthly'              THEN '12'
    WHEN 'mi_contractor_period_summary'     THEN '12'
    WHEN 'cws_aggregate_monthly'            THEN '12'
    WHEN 'cws_aggregate_period_summary'     THEN '12'
    WHEN 'ag_du_delivery_monthly'           THEN '13'
    WHEN 'ag_du_shortage_monthly'           THEN '13'
    WHEN 'ag_du_period_summary'             THEN '13'
    WHEN 'ag_du_demand_monthly'             THEN '13'
    WHEN 'ag_du_gw_pumping_monthly'         THEN '13'
    WHEN 'ag_du_sw_delivery_monthly'        THEN '13'
    WHEN 'ag_aggregate_monthly'             THEN '13'
    WHEN 'ag_aggregate_period_summary'      THEN '13'
    WHEN 'refuge_du_delivery_monthly'       THEN '13'
    WHEN 'refuge_du_shortage_monthly'       THEN '13'
    WHEN 'refuge_du_period_summary'         THEN '13'
    WHEN 'env_flow_season'                  THEN '14'
    WHEN 'env_flow_channel_monthly'         THEN '14'
    WHEN 'env_flow_channel_seasonal'        THEN '14'
    WHEN 'env_flow_channel_period_summary'  THEN '14'
    WHEN 'delta_monthly'                    THEN '15'
    WHEN 'delta_period_summary'             THEN '15'
    ELSE NULL
END;

-- ═══════════════════════════════════════════════════════════════════════════
-- 6. model_source: drop version_family_id (no other lookup table has it)  [DDL]
-- ═══════════════════════════════════════════════════════════════════════════
SET ROLE postgres;
ALTER TABLE model_source DROP COLUMN IF EXISTS version_family_id;
RESET ROLE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 7. source: fix record 35 attribution (was inserted as postgres/system)
-- ═══════════════════════════════════════════════════════════════════════════
UPDATE source
SET    created_by = 2,
       updated_by = 2,
       updated_at = NOW()
WHERE  id = 35;

-- ═══════════════════════════════════════════════════════════════════════════
-- 8. source: resequence IDs (32→9, 33→10, 34→11, 35→12)
--    Strategy: insert copies with new IDs, update children, delete originals.
-- ═══════════════════════════════════════════════════════════════════════════

-- 8a. Insert new rows with correct IDs (copy from old rows)
INSERT INTO source (id, source, description, is_active, created_at, created_by, updated_at, updated_by)
SELECT 9,  source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 32
UNION ALL
SELECT 10, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 33
UNION ALL
SELECT 11, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 34
UNION ALL
SELECT 12, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 35;

-- 8b. Update all child tables (FK → source.id)
UPDATE network_type     SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_subtype  SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_arc      SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_node     SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_gis      SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE reservoir        SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE compliance_station SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE wba              SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE hydroclimate     SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);

-- 8c. Delete old source rows
DELETE FROM source WHERE id IN (32, 33, 34, 35);

-- 8d. Reset sequence to max+1
SELECT setval('source_id_seq', (SELECT MAX(id) FROM source));

-- ═══════════════════════════════════════════════════════════════════════════
-- 9. network_subtype: drop redundant network_entity_type_id  [DDL]
--    (derivable via type_id → network_type.network_entity_type_id)
-- ═══════════════════════════════════════════════════════════════════════════
SET ROLE postgres;
ALTER TABLE network_subtype DROP COLUMN IF EXISTS network_entity_type_id;
RESET ROLE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 10. wba: drop legacy hydrologic_region text column  [DDL]
--     (hydrologic_region_id FK is the authoritative reference)
-- ═══════════════════════════════════════════════════════════════════════════
SET ROLE postgres;
ALTER TABLE wba DROP COLUMN IF EXISTS hydrologic_region;
RESET ROLE;

-- ═══════════════════════════════════════════════════════════════════════════
-- 11. wba: set DETAW (id=1) to DELTA hydrologic region (id=3)
-- ═══════════════════════════════════════════════════════════════════════════
UPDATE wba
SET    hydrologic_region_id = 3,
       updated_at = NOW()
WHERE  id = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- 12. statistic_category: create lookup table  [DDL]
-- ═══════════════════════════════════════════════════════════════════════════
SET ROLE postgres;
CREATE TABLE IF NOT EXISTS statistic_category (
    id          SERIAL PRIMARY KEY,
    short_code  TEXT UNIQUE NOT NULL,
    label       TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    created_by  INTEGER NOT NULL REFERENCES developer(id),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_by  INTEGER NOT NULL REFERENCES developer(id)
);
RESET ROLE;

INSERT INTO statistic_category (id, short_code, label, description, created_by, updated_by)
VALUES
    (1, 'summary',         'Summary',         'Aggregate summary statistics (mean, median, min, max, cv, stdev)', 2, 2),
    (2, 'percentile_band', 'Percentile Band', 'Standard quantile bands aligned with DWR water year types',       2, 2),
    (3, 'exceedance',      'Exceedance',      'Exceedance percentiles for flow duration and reliability analysis', 2, 2);

SELECT setval('statistic_category_id_seq', (SELECT MAX(id) FROM statistic_category));

-- Register in domain_family_map
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, database_level, created_by, updated_by)
VALUES ('public', 'statistic_category', 11, 'Statistic category lookup', '01', 2, 2)
ON CONFLICT (schema_name, table_name) DO UPDATE
SET note = EXCLUDED.note, database_level = EXCLUDED.database_level;

-- ═══════════════════════════════════════════════════════════════════════════
-- 13. statistic_type: resequence IDs and add exceedance percentiles
--     No FK references statistic_type.id, so resequencing is safe.
-- ═══════════════════════════════════════════════════════════════════════════

-- Current IDs: 1(MEAN),2(MEDIAN),3(MIN),4(MAX),7(Q0),8(Q10),9(Q30),10(Q50),
--              11(Q70),12(Q90),13(Q100),14(CV),15(STDEV)
-- Target:  1(MEAN),2(MEDIAN),3(MIN),4(MAX),5(CV),6(STDEV),
--          7(Q0),8(Q10),9(Q30),10(Q50),11(Q70),12(Q90),13(Q100),
--          14(EXC_P5),15(EXC_P10),16(EXC_P25),17(EXC_P50),
--          18(EXC_P75),19(EXC_P90),20(EXC_P95)

-- 13a. Resequence existing rows (use negative temp IDs to avoid collisions)
UPDATE statistic_type SET id = -14 WHERE id = 14;  -- CV
UPDATE statistic_type SET id = -15 WHERE id = 15;  -- STDEV
UPDATE statistic_type SET id = -7  WHERE id = 7;   -- Q0
UPDATE statistic_type SET id = -8  WHERE id = 8;   -- Q10
UPDATE statistic_type SET id = -9  WHERE id = 9;   -- Q30
UPDATE statistic_type SET id = -10 WHERE id = 10;  -- Q50
UPDATE statistic_type SET id = -11 WHERE id = 11;  -- Q70
UPDATE statistic_type SET id = -12 WHERE id = 12;  -- Q90
UPDATE statistic_type SET id = -13 WHERE id = 13;  -- Q100

-- Now assign final IDs
UPDATE statistic_type SET id = 5  WHERE id = -14;  -- CV → 5
UPDATE statistic_type SET id = 6  WHERE id = -15;  -- STDEV → 6
UPDATE statistic_type SET id = 7  WHERE id = -7;   -- Q0 stays 7
UPDATE statistic_type SET id = 8  WHERE id = -8;   -- Q10 stays 8
UPDATE statistic_type SET id = 9  WHERE id = -9;   -- Q30 stays 9
UPDATE statistic_type SET id = 10 WHERE id = -10;  -- Q50 stays 10
UPDATE statistic_type SET id = 11 WHERE id = -11;  -- Q70 stays 11
UPDATE statistic_type SET id = 12 WHERE id = -12;  -- Q90 stays 12
UPDATE statistic_type SET id = 13 WHERE id = -13;  -- Q100 stays 13

-- 13b. Insert exceedance percentiles (still with is_percentile for now)
INSERT INTO statistic_type (id, short_code, label, description, is_percentile, created_by, updated_by)
VALUES
    (14, 'EXC_P5',  '5th exceedance',  'Value exceeded 5% of time (very wet)',   true, 2, 2),
    (15, 'EXC_P10', '10th exceedance', 'Value exceeded 10% of time (wet)',       true, 2, 2),
    (16, 'EXC_P25', '25th exceedance', 'Value exceeded 25% of time (above avg)', true, 2, 2),
    (17, 'EXC_P50', '50th exceedance', 'Value exceeded 50% of time (median)',    true, 2, 2),
    (18, 'EXC_P75', '75th exceedance', 'Value exceeded 75% of time (below avg)', true, 2, 2),
    (19, 'EXC_P90', '90th exceedance', 'Value exceeded 90% of time (dry)',       true, 2, 2),
    (20, 'EXC_P95', '95th exceedance', 'Value exceeded 95% of time (very dry)',  true, 2, 2);

-- 13c. Reset sequence
SELECT setval('statistic_type_id_seq', (SELECT MAX(id) FROM statistic_type));

-- ═══════════════════════════════════════════════════════════════════════════
-- 14. statistic_type: replace is_percentile with statistic_category_id FK
-- ═══════════════════════════════════════════════════════════════════════════

-- 14a. Add statistic_category_id column  [DDL]
SET ROLE postgres;
ALTER TABLE statistic_type ADD COLUMN statistic_category_id INTEGER REFERENCES statistic_category(id);
RESET ROLE;

-- 14b. Populate from existing data
UPDATE statistic_type SET statistic_category_id = 1 WHERE short_code IN ('MEAN','MEDIAN','MIN','MAX','CV','STDEV');
UPDATE statistic_type SET statistic_category_id = 2 WHERE short_code LIKE 'Q%';
UPDATE statistic_type SET statistic_category_id = 3 WHERE short_code LIKE 'EXC_%';

-- 14c. Make NOT NULL now that all rows are populated  [DDL]
SET ROLE postgres;
ALTER TABLE statistic_type ALTER COLUMN statistic_category_id SET NOT NULL;

-- 14d. Drop the old boolean column  [DDL]
ALTER TABLE statistic_type DROP COLUMN IF EXISTS is_percentile;
RESET ROLE;

COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- Verify
-- ═══════════════════════════════════════════════════════════════════════════
\echo 'VERIFICATION'
\echo '============'

SELECT 'developer id=2' AS check, affiliation FROM developer WHERE id = 2;
SELECT 'version_family id=14' AS check, short_code, label FROM version_family WHERE id = 14;
SELECT 'version has no manifest' AS check, COUNT(*) AS manifest_cols FROM information_schema.columns WHERE table_name = 'version' AND column_name = 'manifest';
SELECT 'domain_family_map attribution' AS check, COUNT(*) AS rows_with_system FROM domain_family_map WHERE created_by = 1 OR updated_by = 1;
SELECT 'domain_family_map levels' AS check, COUNT(*) AS rows_without_level FROM domain_family_map WHERE database_level IS NULL;
SELECT 'model_source no version_family_id' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'model_source' AND column_name = 'version_family_id';
SELECT 'source IDs sequential' AS check, array_agg(id ORDER BY id) AS ids FROM source;
SELECT 'source no gaps > 12' AS check, COUNT(*) AS high_ids FROM source WHERE id > 12;
SELECT 'network_subtype no entity_type_id' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'network_subtype' AND column_name = 'network_entity_type_id';
SELECT 'wba no text region col' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'wba' AND column_name = 'hydrologic_region' AND data_type = 'character varying';
SELECT 'wba id=1 region' AS check, w.id, hr.short_code FROM wba w JOIN hydrologic_region hr ON w.hydrologic_region_id = hr.id WHERE w.id = 1;
SELECT 'statistic_category' AS check, id, short_code, label FROM statistic_category ORDER BY id;
SELECT 'statistic_type' AS check, st.id, st.short_code, sc.short_code AS category
FROM statistic_type st JOIN statistic_category sc ON st.statistic_category_id = sc.id ORDER BY st.id;
SELECT 'statistic_type no is_percentile' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'statistic_type' AND column_name = 'is_percentile';
