-- ============================================================================
-- MIGRATION: Fix layer 00 FK rules and indexes
-- ============================================================================
-- Discovered during layer 00 audit (Feb 2026):
--
--   1. idx_domain_family_map_version_family was defined in
--      00_create_versioning_tables.sql but never applied to the live DB.
--
--   2. All layer 00 FK constraints were created with ON DELETE NO ACTION /
--      ON UPDATE NO ACTION. The create script intent is ON DELETE RESTRICT /
--      ON UPDATE CASCADE. Aligning the live DB to match.
--
--   3. idx_audit_log_table_name is redundant: idx_audit_log_record already
--      covers (table_name, record_id), and PostgreSQL uses it for
--      WHERE table_name = ? prefix queries. Dropped to reduce write overhead.
--
-- Safe to run multiple times (IF NOT EXISTS / DO $$ guards throughout).
-- Run as:
--   psql $DATABASE_URL -f database/scripts/sql/migrations/02_fix_layer00_fk_rules_and_indexes.sql
-- ============================================================================

\echo ''
\echo '============================================================'
\echo ' MIGRATION 02: Layer 00 FK rules and indexes'
\echo '============================================================'
\echo ''

-- ============================================================================
-- PART 1: Add missing index on domain_family_map
-- ============================================================================
\echo 'Part 1: Adding missing idx_domain_family_map_version_family...'

CREATE INDEX IF NOT EXISTS idx_domain_family_map_version_family
    ON domain_family_map(version_family_id);

\echo 'Part 1: Done.'
\echo ''

-- ============================================================================
-- PART 2: Drop redundant indexes
-- ============================================================================
-- idx_audit_log_table_name(table_name): redundant with idx_audit_log_record
--   (table_name, record_id). PostgreSQL uses the composite index for prefix
--   queries. Pure write overhead on the highest-write table in the schema.
--
-- idx_version_family(version_family_id): redundant with the composite unique
--   index version_version_family_id_version_number_key(version_family_id,
--   version_number). B-tree indexes support left-prefix scanning, so the
--   composite index already handles any WHERE version_family_id = ? query.
-- ============================================================================
\echo 'Part 2: Dropping redundant indexes...'

DROP INDEX IF EXISTS idx_audit_log_table_name;
DROP INDEX IF EXISTS idx_version_family;

\echo 'Part 2: Done.'
\echo ''

-- ============================================================================
-- PART 3: Fix FK rules — ON DELETE RESTRICT / ON UPDATE CASCADE
-- ============================================================================
-- Each constraint is dropped and re-added. DO $$ guards check existence before
-- dropping so the script is safe to re-run.
--
-- Tables affected: developer (self-ref), version_family, version,
--                  domain_family_map, audit_log
-- ============================================================================
\echo 'Part 3: Fixing FK rules (NO ACTION → RESTRICT / CASCADE)...'

-- developer self-references
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'developer_created_by_fkey' AND table_name = 'developer') THEN
        ALTER TABLE developer DROP CONSTRAINT developer_created_by_fkey;
    END IF;
    ALTER TABLE developer
        ADD CONSTRAINT developer_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'developer_updated_by_fkey' AND table_name = 'developer') THEN
        ALTER TABLE developer DROP CONSTRAINT developer_updated_by_fkey;
    END IF;
    ALTER TABLE developer
        ADD CONSTRAINT developer_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

-- version_family → developer
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'version_family_created_by_fkey' AND table_name = 'version_family') THEN
        ALTER TABLE version_family DROP CONSTRAINT version_family_created_by_fkey;
    END IF;
    ALTER TABLE version_family
        ADD CONSTRAINT version_family_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'version_family_updated_by_fkey' AND table_name = 'version_family') THEN
        ALTER TABLE version_family DROP CONSTRAINT version_family_updated_by_fkey;
    END IF;
    ALTER TABLE version_family
        ADD CONSTRAINT version_family_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

-- version → version_family and → developer
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'version_version_family_id_fkey' AND table_name = 'version') THEN
        ALTER TABLE version DROP CONSTRAINT version_version_family_id_fkey;
    END IF;
    ALTER TABLE version
        ADD CONSTRAINT version_version_family_id_fkey
        FOREIGN KEY (version_family_id) REFERENCES version_family(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'version_created_by_fkey' AND table_name = 'version') THEN
        ALTER TABLE version DROP CONSTRAINT version_created_by_fkey;
    END IF;
    ALTER TABLE version
        ADD CONSTRAINT version_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'version_updated_by_fkey' AND table_name = 'version') THEN
        ALTER TABLE version DROP CONSTRAINT version_updated_by_fkey;
    END IF;
    ALTER TABLE version
        ADD CONSTRAINT version_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

-- domain_family_map → version_family and → developer
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'domain_family_map_version_family_id_fkey' AND table_name = 'domain_family_map') THEN
        ALTER TABLE domain_family_map DROP CONSTRAINT domain_family_map_version_family_id_fkey;
    END IF;
    ALTER TABLE domain_family_map
        ADD CONSTRAINT domain_family_map_version_family_id_fkey
        FOREIGN KEY (version_family_id) REFERENCES version_family(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'domain_family_map_created_by_fkey' AND table_name = 'domain_family_map') THEN
        ALTER TABLE domain_family_map DROP CONSTRAINT domain_family_map_created_by_fkey;
    END IF;
    ALTER TABLE domain_family_map
        ADD CONSTRAINT domain_family_map_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'domain_family_map_updated_by_fkey' AND table_name = 'domain_family_map') THEN
        ALTER TABLE domain_family_map DROP CONSTRAINT domain_family_map_updated_by_fkey;
    END IF;
    ALTER TABLE domain_family_map
        ADD CONSTRAINT domain_family_map_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

-- audit_log → developer
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_name = 'audit_log_changed_by_fkey' AND table_name = 'audit_log') THEN
        ALTER TABLE audit_log DROP CONSTRAINT audit_log_changed_by_fkey;
    END IF;
    ALTER TABLE audit_log
        ADD CONSTRAINT audit_log_changed_by_fkey
        FOREIGN KEY (changed_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
END $$;

\echo 'Part 3: Done.'
\echo ''

-- ============================================================================
-- VERIFY: confirm the migration landed correctly
-- ============================================================================
\echo 'Verification:'

SELECT 'FK rules (all should be RESTRICT / CASCADE):' AS check;
SELECT
    c.conrelid::regclass::text                    AS table_name,
    a.attname                                     AS column_name,
    c.confrelid::regclass::text                   AS ref_table,
    f.attname                                     AS ref_column,
    CASE c.confdeltype
        WHEN 'a' THEN 'NO ACTION'  WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'    WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT' END            AS delete_rule,
    CASE c.confupdtype
        WHEN 'a' THEN 'NO ACTION'  WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'    WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT' END            AS update_rule
FROM pg_constraint     c
JOIN pg_attribute      a ON a.attrelid = c.conrelid  AND a.attnum = ANY(c.conkey)
JOIN pg_attribute      f ON f.attrelid = c.confrelid AND f.attnum = ANY(c.confkey)
WHERE c.contype = 'f'
  AND c.conrelid::regclass::text IN
      ('developer', 'version_family', 'version', 'domain_family_map', 'audit_log')
ORDER BY table_name, column_name;

SELECT 'domain_family_map indexes (should include idx_domain_family_map_version_family):' AS check;
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'domain_family_map'
ORDER BY indexname;

SELECT 'audit_log indexes (idx_audit_log_table_name should NOT appear):' AS check;
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'audit_log'
ORDER BY indexname;

\echo ''
\echo '============================================================'
\echo ' MIGRATION 02 COMPLETE'
\echo ' Re-run the audit to capture the updated state:'
\echo '   bash database/run_audit.sh'
\echo '============================================================'
