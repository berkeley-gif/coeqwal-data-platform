-- =============================================================================
-- 05_layer01_cleanup.sql
-- Layer 01 cleanup:
--   1. Fix false updated_at timestamps on tables seeded in migration 03
--   2. Rename hydrologic_region EXTERNAL to EXPORT
--   3. Remap du_urban_variable.variable_type_id to calsim_model_variable_type
--   4. Drop calsim_variable_type (superseded by calsim_model_variable_type)
-- =============================================================================

\echo '============================================================================'
\echo 'MIGRATION 05: LAYER 01 CLEANUP'
\echo '============================================================================'


-- =============================================================================
-- PART 1: Fix false updated_at on tables seeded by migration 03 and 04
--   Correct state: created_by=2, updated_by=2, updated_at = created_at.
-- =============================================================================
\echo ''
\echo 'PART 1: Fixing false updated_at timestamps on migration 03 tables...'

ALTER TABLE watershed              DISABLE TRIGGER USER;
ALTER TABLE calsim_model_variable_type DISABLE TRIGGER USER;
ALTER TABLE derived_variable_type  DISABLE TRIGGER USER;

UPDATE watershed
SET created_by = 2, updated_by = 2, updated_at = created_at;

UPDATE calsim_model_variable_type
SET created_by = 2, updated_by = 2, updated_at = created_at;

UPDATE derived_variable_type
SET created_by = 2, updated_by = 2, updated_at = created_at;

ALTER TABLE watershed              ENABLE TRIGGER USER;
ALTER TABLE calsim_model_variable_type ENABLE TRIGGER USER;
ALTER TABLE derived_variable_type  ENABLE TRIGGER USER;

\echo '  Verify (created_at should equal updated_at for all rows):'
SELECT 'watershed' AS tbl, short_code, created_by, updated_by,
       created_at = updated_at AS timestamps_match
FROM watershed LIMIT 3
UNION ALL
SELECT 'calsim_model_variable_type', short_code, created_by, updated_by,
       created_at = updated_at
FROM calsim_model_variable_type LIMIT 2
UNION ALL
SELECT 'derived_variable_type', short_code, created_by, updated_by,
       created_at = updated_at
FROM derived_variable_type LIMIT 2;


-- =============================================================================
-- PART 2: Rename EXTERNAL to EXPORT in hydrologic_region
--   But updated_by must be 2 (not 1/postgres).
-- =============================================================================
\echo ''
\echo 'PART 2: Renaming EXTERNAL to EXPORT in hydrologic_region...'

ALTER TABLE hydrologic_region DISABLE TRIGGER USER;

UPDATE hydrologic_region
SET short_code = 'EXPORT',
    label      = 'Export region',
    updated_by = 2,
    updated_at = NOW()
WHERE short_code = 'EXTERNAL';

ALTER TABLE hydrologic_region ENABLE TRIGGER USER;

SELECT id, short_code, label, created_by, updated_by FROM hydrologic_region ORDER BY id;


-- =============================================================================
-- PART 3: Remap du_urban_variable.variable_type_id to calsim_model_variable_type
--   3 = decision                      3 = decision (same)
-- =============================================================================
\echo ''
\echo 'PART 3: Remapping du_urban_variable.variable_type_id...'

\echo '  Before:'
SELECT variable_type_id,
       (SELECT short_code FROM calsim_variable_type WHERE id = variable_type_id) AS old_type,
       COUNT(*) AS rows
FROM du_urban_variable
WHERE variable_type_id IS NOT NULL
GROUP BY variable_type_id ORDER BY variable_type_id;

ALTER TABLE du_urban_variable DISABLE TRIGGER USER;

UPDATE du_urban_variable
SET variable_type_id = 4,
    updated_by = 2,
    updated_at = NOW()
WHERE variable_type_id = 2;

ALTER TABLE du_urban_variable ENABLE TRIGGER USER;

ALTER TABLE du_urban_variable
    DROP CONSTRAINT IF EXISTS du_urban_variable_variable_type_id_fkey;

ALTER TABLE du_urban_variable
    ADD CONSTRAINT du_urban_variable_variable_type_id_fkey
    FOREIGN KEY (variable_type_id) REFERENCES calsim_model_variable_type(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

\echo '  After:'
SELECT variable_type_id,
       (SELECT short_code FROM calsim_model_variable_type WHERE id = variable_type_id) AS new_type,
       COUNT(*) AS rows
FROM du_urban_variable
WHERE variable_type_id IS NOT NULL
GROUP BY variable_type_id ORDER BY variable_type_id;


-- =============================================================================
-- PART 4: Drop calsim_variable_type
-- =============================================================================
\echo ''
\echo 'PART 4: Dropping calsim_variable_type...'

DELETE FROM domain_family_map WHERE table_name = 'calsim_variable_type';
DROP TABLE IF EXISTS calsim_variable_type;

SELECT COUNT(*) AS calsim_variable_type_still_exists
FROM pg_tables
WHERE schemaname = 'public' AND tablename = 'calsim_variable_type';


-- =============================================================================
-- SUMMARY
-- =============================================================================
\echo ''
\echo 'domain_family_map count (was 73, should be 72):'
SELECT COUNT(*) FROM domain_family_map;

\echo ''
\echo '============================================================================'
\echo 'MIGRATION 05 COMPLETE'
\echo '============================================================================'
