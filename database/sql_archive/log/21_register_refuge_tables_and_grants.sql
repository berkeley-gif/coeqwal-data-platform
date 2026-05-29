-- =============================================================================
-- 21_register_refuge_tables_and_grants.sql
-- Patches migration 19 (refuge statistics tables) and 20 (refuge entity table)
-- to satisfy the new-table rubric items that were omitted.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/21_register_refuge_tables_and_grants.sql
--
-- What this migration does:
--   1. Grants SELECT/INSERT/UPDATE/DELETE + sequence access on the three
--      statistics tables from migration 19.
--   2. Registers all four refuge tables in domain_family_map:
--        refuge_du_delivery_monthly  to statistics family
--        refuge_du_shortage_monthly  to statistics family
--        refuge_du_period_summary    to statistics family
--        du_refuge_entity            to entity family  (also done in migration 20,
--                                       but ON CONFLICT DO UPDATE handles re-runs)
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================='
\echo 'MIGRATION 21  - Refuge table grants + domain_family_map registration'
\echo '============================================='


-- =============================================================================
-- 1. Grants for statistics tables (migration 19 omission)
-- =============================================================================

\echo ''
\echo 'Granting permissions on refuge statistics tables...'

GRANT SELECT, INSERT, UPDATE, DELETE ON refuge_du_delivery_monthly         TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE      refuge_du_delivery_monthly_id_seq     TO jfantauzza;

GRANT SELECT, INSERT, UPDATE, DELETE ON refuge_du_shortage_monthly         TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE      refuge_du_shortage_monthly_id_seq     TO jfantauzza;

GRANT SELECT, INSERT, UPDATE, DELETE ON refuge_du_period_summary           TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE      refuge_du_period_summary_id_seq       TO jfantauzza;

\echo 'Grants applied.'


-- =============================================================================
-- 2. Register all four refuge tables in domain_family_map
-- =============================================================================

\echo ''
\echo 'Registering refuge tables in domain_family_map...'

ALTER TABLE domain_family_map DISABLE TRIGGER USER;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
SELECT 'public', t.table_name, vf.id, t.note, 2, 2
FROM (VALUES
    ('refuge_du_delivery_monthly', 'Wildlife refuge demand unit monthly delivery statistics'),
    ('refuge_du_shortage_monthly', 'Wildlife refuge demand unit monthly shortage statistics'),
    ('refuge_du_period_summary',   'Wildlife refuge demand unit period-of-record summary statistics')
) AS t(table_name, note)
CROSS JOIN version_family vf
WHERE vf.short_code = 'statistics'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id,
        note              = EXCLUDED.note,
        updated_at        = NOW(),
        updated_by        = 2;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
SELECT 'public', 'du_refuge_entity', vf.id, 'Wildlife refuge demand unit entities (Layer 03)', 2, 2
FROM version_family vf
WHERE vf.short_code = 'entity'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id,
        note              = EXCLUDED.note,
        updated_at        = NOW(),
        updated_by        = 2;

ALTER TABLE domain_family_map ENABLE TRIGGER USER;

\echo 'Registered in domain_family_map.'


-- =============================================================================
-- 3. Verification
-- =============================================================================

\echo ''
\echo '===== VERIFICATION ====='

\echo ''
\echo 'Refuge tables in domain_family_map:'
SELECT
    dfm.table_name,
    vf.short_code AS version_family,
    dfm.note
FROM domain_family_map dfm
JOIN version_family vf ON vf.id = dfm.version_family_id
WHERE dfm.table_name IN (
    'du_refuge_entity',
    'refuge_du_delivery_monthly',
    'refuge_du_shortage_monthly',
    'refuge_du_period_summary'
)
ORDER BY vf.short_code, dfm.table_name;

\echo ''
\echo '=== Migration 21 complete ==='
