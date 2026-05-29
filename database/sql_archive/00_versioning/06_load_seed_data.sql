-- =============================================================================
-- 06_load_seed_data.sql
-- Seeds the versioning tables with bootstrap data.
-- =============================================================================

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'LOADING VERSIONING SEED DATA'
\echo '============================================================================'


-- =============================================================================
-- 1. developer
-- =============================================================================

\echo 'Seeding developer...'

INSERT INTO developer (
    email, name, display_name, role,
    aws_sso_username,
    is_bootstrap, sync_source, is_active,
    created_by, updated_by
) VALUES (
    'jfantauzza@berkeley.edu', 'Jill', 'Jill Fantauzza', 'admin',
    'jfantauzza',
    true, 'seed', true,
    1, 1
)
ON CONFLICT (email) DO NOTHING;

SELECT id, email, name, display_name, role, aws_sso_username FROM developer ORDER BY id;


-- =============================================================================
-- 2. version_family
-- =============================================================================

\echo ''
\echo 'Seeding version_family...'

CREATE TEMP TABLE tmp_version_family (
    short_code  TEXT,
    label       TEXT,
    description TEXT,
    is_active   BOOLEAN
) ON COMMIT DROP;

\copy tmp_version_family FROM 'database/seed_tables/00_versioning/version_family.csv' CSV HEADER

INSERT INTO version_family (short_code, label, description, is_active, created_by, updated_by)
SELECT short_code, label, description, is_active, 1, 1
FROM tmp_version_family
ON CONFLICT (short_code) DO NOTHING;

SELECT id, short_code, label FROM version_family ORDER BY id;


-- =============================================================================
-- 3. version
-- =============================================================================

\echo ''
\echo 'Seeding version...'

CREATE TEMP TABLE tmp_version (
    version_family_short_code TEXT,
    version_number            TEXT,
    changelog                 TEXT,
    is_active                 BOOLEAN
) ON COMMIT DROP;

\copy tmp_version FROM 'database/seed_tables/00_versioning/version.csv' CSV HEADER

INSERT INTO version (version_family_id, version_number, changelog, is_active, created_by, updated_by)
SELECT
    vf.id,
    tv.version_number,
    tv.changelog,
    tv.is_active,
    1, 1
FROM tmp_version tv
JOIN version_family vf ON vf.short_code = tv.version_family_short_code
ON CONFLICT (version_family_id, version_number) DO NOTHING;

SELECT v.id, vf.short_code, v.version_number, v.is_active
FROM version v JOIN version_family vf ON v.version_family_id = vf.id
ORDER BY vf.short_code;


-- =============================================================================
-- 4. domain_family_map
-- =============================================================================
-- NOT loaded here. The seed CSV (seed_tables/00_versioning/domain_family_map.csv)
-- is out of date
-- =============================================================================

\echo ''
\echo 'domain_family_map: skipped here  - populated by 05_populate_domain_family_map.sql'
\echo '(see script header for CSV regeneration instructions)'


-- =============================================================================
-- Summary
-- =============================================================================

\echo ''
\echo 'Seed data loaded:'
SELECT 'developer'       AS "table", COUNT(*) AS rows FROM developer
UNION ALL
SELECT 'version_family',             COUNT(*)         FROM version_family
UNION ALL
SELECT 'version',                    COUNT(*)         FROM version
UNION ALL
SELECT 'domain_family_map',          COUNT(*)         FROM domain_family_map
ORDER BY "table";

\echo ''
\echo '============================================================================'
\echo 'SEED DATA LOADED SUCCESSFULLY'
\echo 'Next: run 05_populate_domain_family_map.sql'
\echo '============================================================================'
