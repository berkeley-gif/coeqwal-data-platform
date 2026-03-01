-- =============================================================================
-- Migration 08: Register new tables in domain_family_map
-- =============================================================================
-- Three tables created by migrations 06 and 07 were not registered in
-- domain_family_map and are flagged by the post-migration audit.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/08_register_new_tables_domain_family_map.sql
-- =============================================================================

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
VALUES
    ('public', 'slr',                5, 'Sea-level rise lookup (hydroclimate layer)', 2, 2),
    ('public', 'assumption_category', 3, 'Assumption category lookup',                2, 2),
    ('public', 'operation_category',  4, 'Operation category lookup',                 2, 2)
ON CONFLICT (schema_name, table_name) DO NOTHING;

-- Verify
SELECT table_name, version_family_id, note
FROM domain_family_map
WHERE table_name IN ('slr', 'assumption_category', 'operation_category')
ORDER BY version_family_id;
