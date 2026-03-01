-- =============================================================================
-- inspect_erd_gaps.sql
-- One-time inspection: tables present in erd_old.txt but absent from the
-- current ERD documentation, plus index gaps in Layer 00.
-- =============================================================================
-- Run with either user:
--   psql $DATABASE_URL   -f database/scripts/sql/inspect_erd_gaps.sql
--   psql $SUPERUSER_URL  -f database/scripts/sql/inspect_erd_gaps.sql
-- =============================================================================

\echo '============================================================================'
\echo 'ERD GAP INSPECTION'
\echo '============================================================================'


-- =============================================================================
-- 1. ZOMBIE TABLE CHECK
--    Tables in erd_old.txt that were expected to be dropped during redesign.
--    If any of these show "exists = true" they are live and need a decision.
-- =============================================================================
\echo ''
\echo '1. ZOMBIE TABLE CHECK (old DBML tables, expected to be absent)'
\echo '--------------------------------------------------------------'

SELECT
    e.tablename                          AS table_name,
    CASE WHEN pt.tablename IS NOT NULL
         THEN 'EXISTS in DB'
         ELSE 'not found'
    END                                  AS status,
    (SELECT reltuples::bigint
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = e.tablename)      AS approx_row_count
FROM (VALUES
    ('flow_regime'),
    ('season'),
    ('region'),
    ('analysis_type'),
    ('hydroclimate_variable_type')
) AS e(tablename)
LEFT JOIN pg_tables pt
       ON pt.schemaname = 'public'
      AND pt.tablename  = e.tablename
ORDER BY e.tablename;


-- =============================================================================
-- 2. COLUMN LISTING FOR ANY ZOMBIE TABLES THAT EXIST
--    (Empty result set = all were dropped cleanly)
-- =============================================================================
\echo ''
\echo '2. COLUMNS ON ANY SURVIVING OLD-DBML TABLES'
\echo '--------------------------------------------'

SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
WHERE c.table_schema = 'public'
  AND c.table_name IN (
        'flow_regime',
        'season',
        'region',
        'analysis_type',
        'hydroclimate_variable_type'
      )
ORDER BY c.table_name, c.ordinal_position;


-- =============================================================================
-- 3. VERSION TABLE: MANIFEST INDEX CHECK
--    erd_old.txt documented a (manifest) GIN/JSONB index on version.
--    Was it ever created?
-- =============================================================================
\echo ''
\echo '3. VERSION.MANIFEST INDEX CHECK'
\echo '--------------------------------'

SELECT
    i.relname                            AS index_name,
    pg_get_indexdef(ix.indexrelid)       AS index_definition
FROM pg_index ix
JOIN pg_class t  ON t.oid  = ix.indrelid
JOIN pg_class i  ON i.oid  = ix.indexrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'public'
  AND t.relname = 'version'
ORDER BY i.relname;


-- =============================================================================
-- 4. ALL LAYER 00 INDEXES (sanity cross-check against ERD)
--    Expected after migration 02:
--      version_family_short_code_key        (short_code)
--      version_version_family_id_version... (version_family_id, version_number)
--      idx_domain_family_map_version_family (version_family_id)
--      idx_audit_log_changed_at             (changed_at)
--      idx_audit_log_changed_by             (changed_by)
--      idx_audit_log_record                 (table_name, record_id)
--    NOT expected (were dropped):
--      idx_version_family
--      idx_audit_log_table_name
--      idx_audit_log_operation
-- =============================================================================
\echo ''
\echo '4. ALL LAYER 00 INDEXES (full list)'
\echo '-------------------------------------'

SELECT
    t.relname                            AS table_name,
    i.relname                            AS index_name,
    ix.indisunique                       AS is_unique,
    ix.indisprimary                      AS is_primary,
    pg_get_indexdef(ix.indexrelid)       AS definition
FROM pg_index ix
JOIN pg_class t  ON t.oid  = ix.indrelid
JOIN pg_class i  ON i.oid  = ix.indexrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'public'
  AND t.relname IN (
        'version_family',
        'version',
        'developer',
        'audit_log',
        'domain_family_map'
      )
ORDER BY t.relname, i.relname;


\echo ''
\echo '============================================================================'
\echo 'INSPECTION COMPLETE'
\echo '============================================================================'
