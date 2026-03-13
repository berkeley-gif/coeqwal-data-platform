-- Migration 30c: Finalize schema — run as postgres
--
-- Run from Cloud9 (after 30b_data_changes.sql):
--   psql "postgresql://postgres:<password>@<host>:5432/coeqwal_scenario" \
--        -f database/scripts/sql/migrations/30c_finalize_schema.sql

BEGIN;

-- Re-enable audit triggers that 30a disabled for created_by corrections
ALTER TABLE domain_family_map ENABLE TRIGGER audit_fields_domain_family_map;
ALTER TABLE source ENABLE TRIGGER audit_fields_source;

-- statistic_type: make category NOT NULL now that all rows are populated
ALTER TABLE statistic_type ALTER COLUMN statistic_category_id SET NOT NULL;

-- statistic_type: drop the old boolean column
ALTER TABLE statistic_type DROP COLUMN IF EXISTS is_percentile;

COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- Verification (runs outside the transaction so we see committed state)
-- ═══════════════════════════════════════════════════════════════════════════
\echo ''
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
SELECT 'source id=35 attribution' AS check, created_by, updated_by FROM source WHERE id = 12;
SELECT 'network_subtype no entity_type_id' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'network_subtype' AND column_name = 'network_entity_type_id';
SELECT 'wba no text region col' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'wba' AND column_name = 'hydrologic_region';
SELECT 'wba id=1 region' AS check, w.id, hr.short_code FROM wba w JOIN hydrologic_region hr ON w.hydrologic_region_id = hr.id WHERE w.id = 1;
SELECT 'statistic_category' AS check, id, short_code, label FROM statistic_category ORDER BY id;
SELECT 'statistic_type' AS check, st.id, st.short_code, sc.short_code AS category
FROM statistic_type st JOIN statistic_category sc ON st.statistic_category_id = sc.id ORDER BY st.id;
SELECT 'statistic_type no is_percentile' AS check, COUNT(*) AS cols FROM information_schema.columns WHERE table_name = 'statistic_type' AND column_name = 'is_percentile';
SELECT 'audit trigger domain_family_map' AS check, COUNT(*) AS triggers FROM information_schema.triggers WHERE event_object_table = 'domain_family_map' AND trigger_name = 'audit_fields_domain_family_map';
SELECT 'audit trigger source' AS check, COUNT(*) AS triggers FROM information_schema.triggers WHERE event_object_table = 'source' AND trigger_name = 'audit_fields_source';
SELECT 'audit trigger statistic_category' AS check, COUNT(*) AS triggers FROM information_schema.triggers WHERE event_object_table = 'statistic_category' AND trigger_name = 'audit_fields_statistic_category';
