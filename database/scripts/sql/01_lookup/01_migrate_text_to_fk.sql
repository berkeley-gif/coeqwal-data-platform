-- =============================================================================
-- 01_migrate_text_to_fk.sql
-- Migrate text lookup columns to proper FK relationships
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/01_lookup/01_migrate_text_to_fk.sql
-- =============================================================================
-- 
-- This script converts text columns storing lookup values to proper FK columns.
-- For each table:
--   1. Add new FK column
--   2. Populate FK from text values
--   3. Report any unmapped values (need new lookup entries)
--   4. Keep old text column for reference (can drop later)
-- =============================================================================

\echo '============================================================================'
\echo 'MIGRATING TEXT COLUMNS TO FK RELATIONSHIPS'
\echo '============================================================================'

-- =============================================================================
-- 1. du_agriculture_entity
-- =============================================================================
\echo ''
\echo '1. du_agriculture_entity'
\echo '------------------------'

-- Check current text values
\echo 'Current hydrologic_region values:'
SELECT DISTINCT hydrologic_region FROM du_agriculture_entity ORDER BY 1;

\echo 'Current model_source values:'
SELECT DISTINCT model_source FROM du_agriculture_entity ORDER BY 1;

-- Add FK columns if they don't exist
ALTER TABLE du_agriculture_entity 
    ADD COLUMN IF NOT EXISTS hydrologic_region_id INTEGER REFERENCES hydrologic_region(id);
ALTER TABLE du_agriculture_entity 
    ADD COLUMN IF NOT EXISTS model_source_id INTEGER REFERENCES model_source(id);

-- Populate FK from text values
UPDATE du_agriculture_entity dae
SET hydrologic_region_id = hr.id
FROM hydrologic_region hr
WHERE UPPER(dae.hydrologic_region) = UPPER(hr.short_code)
AND dae.hydrologic_region_id IS NULL;

UPDATE du_agriculture_entity dae
SET model_source_id = ms.id
FROM model_source ms
WHERE LOWER(dae.model_source) = LOWER(ms.short_code)
AND dae.model_source_id IS NULL;

-- Report unmapped values
\echo 'Unmapped hydrologic_region values (need lookup entries):'
SELECT DISTINCT hydrologic_region 
FROM du_agriculture_entity 
WHERE hydrologic_region IS NOT NULL 
AND hydrologic_region_id IS NULL;

\echo 'Unmapped model_source values (need lookup entries):'
SELECT DISTINCT model_source 
FROM du_agriculture_entity 
WHERE model_source IS NOT NULL 
AND model_source_id IS NULL;

-- =============================================================================
-- 2. du_urban_entity
-- =============================================================================
\echo ''
\echo '2. du_urban_entity'
\echo '------------------'

-- Check current text values
\echo 'Current hydrologic_region values:'
SELECT DISTINCT hydrologic_region FROM du_urban_entity ORDER BY 1;

\echo 'Current model_source values:'
SELECT DISTINCT model_source FROM du_urban_entity ORDER BY 1;

-- Add FK columns if they don't exist
ALTER TABLE du_urban_entity 
    ADD COLUMN IF NOT EXISTS hydrologic_region_id INTEGER REFERENCES hydrologic_region(id);
ALTER TABLE du_urban_entity 
    ADD COLUMN IF NOT EXISTS model_source_id INTEGER REFERENCES model_source(id);

-- Populate FK from text values
UPDATE du_urban_entity due
SET hydrologic_region_id = hr.id
FROM hydrologic_region hr
WHERE UPPER(due.hydrologic_region) = UPPER(hr.short_code)
AND due.hydrologic_region_id IS NULL;

UPDATE du_urban_entity due
SET model_source_id = ms.id
FROM model_source ms
WHERE LOWER(due.model_source) = LOWER(ms.short_code)
AND due.model_source_id IS NULL;

-- Report unmapped values
\echo 'Unmapped hydrologic_region values:'
SELECT DISTINCT hydrologic_region 
FROM du_urban_entity 
WHERE hydrologic_region IS NOT NULL 
AND hydrologic_region_id IS NULL;

\echo 'Unmapped model_source values:'
SELECT DISTINCT model_source 
FROM du_urban_entity 
WHERE model_source IS NOT NULL 
AND model_source_id IS NULL;

-- =============================================================================
-- 3. du_urban_variable
-- =============================================================================
\echo ''
\echo '3. du_urban_variable'
\echo '--------------------'

-- Check current text values
\echo 'Current variable_type values:'
SELECT DISTINCT variable_type FROM du_urban_variable ORDER BY 1;

-- Add FK column if it doesn't exist
ALTER TABLE du_urban_variable 
    ADD COLUMN IF NOT EXISTS variable_type_id INTEGER REFERENCES variable_type(id);

-- Populate FK from text values
UPDATE du_urban_variable duv
SET variable_type_id = vt.id
FROM variable_type vt
WHERE LOWER(duv.variable_type) = LOWER(vt.short_code)
AND duv.variable_type_id IS NULL;

-- Report unmapped values
\echo 'Unmapped variable_type values:'
SELECT DISTINCT variable_type 
FROM du_urban_variable 
WHERE variable_type IS NOT NULL 
AND variable_type_id IS NULL;

-- =============================================================================
-- 4. wba (Water Budget Area)
-- =============================================================================
\echo ''
\echo '4. wba'
\echo '------'

-- Check current text values
\echo 'Current hydrologic_region values:'
SELECT DISTINCT hydrologic_region FROM wba ORDER BY 1;

\echo 'Current data_source values:'
SELECT DISTINCT data_source FROM wba ORDER BY 1;

-- Add FK columns if they don't exist
ALTER TABLE wba 
    ADD COLUMN IF NOT EXISTS hydrologic_region_id INTEGER REFERENCES hydrologic_region(id);
ALTER TABLE wba 
    ADD COLUMN IF NOT EXISTS source_id INTEGER REFERENCES source(id);

-- Populate FK from text values
UPDATE wba w
SET hydrologic_region_id = hr.id
FROM hydrologic_region hr
WHERE UPPER(w.hydrologic_region) = UPPER(hr.short_code)
AND w.hydrologic_region_id IS NULL;

UPDATE wba w
SET source_id = s.id
FROM source s
WHERE LOWER(w.data_source) = LOWER(s.source)
AND w.source_id IS NULL;

-- Report unmapped values
\echo 'Unmapped hydrologic_region values:'
SELECT DISTINCT hydrologic_region 
FROM wba 
WHERE hydrologic_region IS NOT NULL 
AND hydrologic_region_id IS NULL;

\echo 'Unmapped data_source values:'
SELECT DISTINCT data_source 
FROM wba 
WHERE data_source IS NOT NULL 
AND source_id IS NULL;

-- =============================================================================
-- 5. reservoirs
-- =============================================================================
\echo ''
\echo '5. reservoirs'
\echo '-------------'

-- Check current text values
\echo 'Current data_source values:'
SELECT DISTINCT data_source FROM reservoirs ORDER BY 1;

-- Add FK column if it doesn't exist
ALTER TABLE reservoirs 
    ADD COLUMN IF NOT EXISTS source_id INTEGER REFERENCES source(id);

-- Populate FK from text values
UPDATE reservoirs r
SET source_id = s.id
FROM source s
WHERE LOWER(r.data_source) = LOWER(s.source)
AND r.source_id IS NULL;

-- Report unmapped values
\echo 'Unmapped data_source values:'
SELECT DISTINCT data_source 
FROM reservoirs 
WHERE data_source IS NOT NULL 
AND source_id IS NULL;

-- =============================================================================
-- 6. compliance_stations
-- =============================================================================
\echo ''
\echo '6. compliance_stations'
\echo '----------------------'

-- Check current text values
\echo 'Current data_source values:'
SELECT DISTINCT data_source FROM compliance_stations ORDER BY 1;

-- Add FK column if it doesn't exist
ALTER TABLE compliance_stations 
    ADD COLUMN IF NOT EXISTS source_id INTEGER REFERENCES source(id);

-- Populate FK from text values
UPDATE compliance_stations cs
SET source_id = s.id
FROM source s
WHERE LOWER(cs.data_source) = LOWER(s.source)
AND cs.source_id IS NULL;

-- Report unmapped values
\echo 'Unmapped data_source values:'
SELECT DISTINCT data_source 
FROM compliance_stations 
WHERE data_source IS NOT NULL 
AND source_id IS NULL;

-- =============================================================================
-- SUMMARY
-- =============================================================================
\echo ''
\echo '============================================================================'
\echo 'MIGRATION SUMMARY'
\echo '============================================================================'

\echo ''
\echo 'New FK columns added:'
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
AND column_name IN ('hydrologic_region_id', 'model_source_id', 'variable_type_id', 'source_id')
AND table_name IN ('du_agriculture_entity', 'du_urban_entity', 'du_urban_variable', 
                   'wba', 'reservoirs', 'compliance_stations')
ORDER BY table_name, column_name;

\echo ''
\echo 'Migration statistics:'
SELECT 'du_agriculture_entity' as table_name,
       COUNT(*) as total_rows,
       COUNT(hydrologic_region_id) as mapped_region,
       COUNT(model_source_id) as mapped_source
FROM du_agriculture_entity
UNION ALL
SELECT 'du_urban_entity', COUNT(*), COUNT(hydrologic_region_id), COUNT(model_source_id)
FROM du_urban_entity
UNION ALL
SELECT 'du_urban_variable', COUNT(*), COUNT(variable_type_id), NULL
FROM du_urban_variable
UNION ALL
SELECT 'wba', COUNT(*), COUNT(hydrologic_region_id), COUNT(source_id)
FROM wba
UNION ALL
SELECT 'reservoirs', COUNT(*), COUNT(source_id), NULL
FROM reservoirs
UNION ALL
SELECT 'compliance_stations', COUNT(*), COUNT(source_id), NULL
FROM compliance_stations;

\echo ''
\echo '============================================================================'
\echo 'NEXT STEPS:'
\echo '1. Review unmapped values above'
\echo '2. Add missing lookup entries if needed'
\echo '3. Re-run migration for any new mappings'
\echo '4. Once verified, drop old text columns (optional)'
\echo '============================================================================'
