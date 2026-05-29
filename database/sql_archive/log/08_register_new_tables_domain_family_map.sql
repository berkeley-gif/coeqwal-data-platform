-- =============================================================================
-- Migration 08: Register new tables in domain_family_map + fix provenance
-- =============================================================================
-- 1. Registers slr, assumption_category, operation_category in domain_family_map
--    (these were created by migrations 06 -07 but never registered).
-- 2. Fixes created_by/updated_by on assumption_category and operation_category
--    seed rows that were inserted outside a DISABLE TRIGGER USER block in
--    migration 07, causing the audit trigger to record postgres (not jfantauzza).
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/08_register_new_tables_domain_family_map.sql
-- =============================================================================


ALTER TABLE assumption_category DISABLE TRIGGER USER;

UPDATE assumption_category
SET created_by = 2, updated_by = 2
WHERE short_code IN ('land_use', 'gw_model');

ALTER TABLE assumption_category ENABLE TRIGGER USER;

ALTER TABLE operation_category DISABLE TRIGGER USER;

UPDATE operation_category
SET created_by = 2, updated_by = 2
WHERE short_code IN ('comm_delivery', 'delta_outflow', 'carryover', 'regulatory_salinity');

ALTER TABLE operation_category ENABLE TRIGGER USER;


ALTER TABLE domain_family_map DISABLE TRIGGER USER;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
VALUES
    ('public', 'slr',                 5, 'Sea-level rise lookup (hydroclimate layer)', 2, 2),
    ('public', 'assumption_category', 3, 'Assumption category lookup',                 2, 2),
    ('public', 'operation_category',  4, 'Operation category lookup',                  2, 2)
ON CONFLICT (schema_name, table_name) DO NOTHING;

ALTER TABLE domain_family_map ENABLE TRIGGER USER;

SELECT table_name, version_family_id, note
FROM domain_family_map
WHERE table_name IN ('slr', 'assumption_category', 'operation_category')
ORDER BY version_family_id;
