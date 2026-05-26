-- =============================================================================
-- 06_load_seed_data.sql
-- Seeds the versioning tables with bootstrap data.
-- =============================================================================
-- Run AFTER 00_create_versioning_tables.sql and BEFORE 05_populate_domain_family_map.sql
-- (which seeds domain_family_map separately — see note below).
--
-- Run in Cloud9 (from the repo root):
--   \i database/scripts/sql/00_versioning/06_load_seed_data.sql
--
-- All INSERTs use ON CONFLICT DO NOTHING, so this is safe to re-run.
-- =============================================================================

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'LOADING VERSIONING SEED DATA'
\echo '============================================================================'


-- =============================================================================
-- 1. developer
-- =============================================================================
-- The system user (id=1) was already inserted by 00_create_versioning_tables.sql.
-- Here we seed the admin developer(s).
--
-- NOTE: The seed CSV (seed_tables/00_versioning/developer.csv) is missing the
-- `name` and `aws_sso_username` columns, so this uses inline INSERT instead of
-- \copy. Update the VALUES below when adding new bootstrap developers.
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
-- Loaded from CSV using a temp table to resolve audit fields.
-- CSV columns: short_code, label, description, is_active
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
-- CSV uses version_family short_code (not id), so we join to resolve the FK.
-- CSV columns: version_family_short_code, version_number, changelog, is_active
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
-- is out of date (34 rows vs 70 in production) and has a column
-- (target_version_column) that does not exist in the database table.
--
-- domain_family_map is populated by 05_populate_domain_family_map.sql, which
-- has the current full set of 70 table mappings.
--
-- If you need to regenerate the CSV to match production, run:
--   psql $DATABASE_URL -c "\copy (SELECT schema_name, table_name,
--       (SELECT short_code FROM version_family WHERE id = version_family_id),
--       note, is_active FROM domain_family_map ORDER BY table_name)
--   TO 'database/seed_tables/00_versioning/domain_family_map.csv' CSV HEADER"
-- =============================================================================

\echo ''
\echo 'domain_family_map: skipped here — populated by 05_populate_domain_family_map.sql'
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
