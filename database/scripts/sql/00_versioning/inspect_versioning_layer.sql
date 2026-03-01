-- ============================================================================
-- INSPECT: 00 Versioning Layer Tables
-- ============================================================================
-- Prints all four core versioning tables in human-readable format.
-- Run from the repo root:
--   psql $DATABASE_URL -f database/scripts/sql/inspect_versioning_layer.sql
-- ============================================================================

\x auto
\pset null '(null)'

\echo ''
\echo '============================================================'
\echo ' DEVELOPER'
\echo '============================================================'
SELECT
    id,
    name,
    display_name,
    email,
    aws_sso_username,
    is_active
FROM developer
ORDER BY id;

\echo ''
\echo '============================================================'
\echo ' VERSION FAMILY'
\echo '============================================================'
SELECT
    id,
    short_code,
    label,
    is_active
FROM version_family
ORDER BY id;

\echo ''
\echo '============================================================'
\echo ' VERSION'
\echo '============================================================'
SELECT
    v.id,
    vf.short_code AS family,
    v.version_number,
    v.is_active
FROM version v
JOIN version_family vf ON vf.id = v.version_family_id
ORDER BY vf.short_code, v.id;

\echo ''
\echo '============================================================'
\echo ' DOMAIN FAMILY MAP  (table → version family)'
\echo '============================================================'
SELECT
    dfm.table_name,
    vf.short_code AS version_family
FROM domain_family_map dfm
JOIN version_family vf ON vf.id = dfm.version_family_id
ORDER BY vf.short_code, dfm.table_name;
