-- Migration 44: Register scenario_tag and scenario_tag_link in domain_family_map
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/44_register_tag_tables_in_domain_family_map.sql

BEGIN;

ALTER TABLE domain_family_map DISABLE TRIGGER USER;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, database_level, is_active, created_by, updated_by)
VALUES
    ('public', 'scenario_tag',      2, 'Scenario classification tags',      '06', TRUE, 2, 2),
    ('public', 'scenario_tag_link', 2, 'Scenario-tag crosswalk',            '06', TRUE, 2, 2);

ALTER TABLE domain_family_map ENABLE TRIGGER USER;

COMMIT;

\echo ''
\echo 'New domain_family_map entries:'
SELECT table_name, version_family_id, database_level, note
FROM domain_family_map
WHERE table_name IN ('scenario_tag', 'scenario_tag_link')
ORDER BY table_name;

\echo ''
\echo 'Total domain_family_map rows:'
SELECT count(*) AS total FROM domain_family_map;

\echo ''
\echo '44 REGISTER TAG TABLES COMPLETE'
\echo '================================'
