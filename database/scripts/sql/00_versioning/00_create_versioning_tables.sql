-- =============================================================================
-- 00_create_versioning_tables.sql
-- Creates the foundational versioning tables: developer, version_family,
-- version, and domain_family_map.
-- =============================================================================
-- Run BEFORE any other 00_versioning scripts (they depend on these tables).
-- Run in Cloud9: \i database/scripts/sql/00_versioning/00_create_versioning_tables.sql
-- =============================================================================

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'CREATING VERSIONING TABLES'
\echo '============================================================================'


-- =============================================================================
-- 1. developer
-- =============================================================================
-- Created first because all other tables have FK references to developer.id.
-- The created_by / updated_by columns are nullable here to allow the bootstrap
-- system user (id=1) to be its own creator (chicken-and-egg). The self-
-- referencing FKs are added after insertion of the system user.
-- =============================================================================

CREATE TABLE IF NOT EXISTS developer (
    id               SERIAL PRIMARY KEY,
    email            TEXT UNIQUE,
    name             TEXT,
    display_name     TEXT NOT NULL,
    affiliation      TEXT,
    role             TEXT,
    aws_sso_user_id  TEXT,
    aws_sso_username TEXT UNIQUE,
    is_bootstrap     BOOLEAN DEFAULT FALSE,
    sync_source      TEXT DEFAULT 'manual',
    is_active        BOOLEAN DEFAULT TRUE,
    last_login       TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by       INTEGER,   -- nullable: required for bootstrap self-reference
    updated_by       INTEGER    -- nullable: required for bootstrap self-reference
);

-- Insert system bootstrap user (id=1) before adding FK constraints.
-- Uses INSERT ... ON CONFLICT so this is safe to re-run.
INSERT INTO developer (id, email, name, display_name, role, is_bootstrap, sync_source, is_active)
VALUES (1, 'system@coeqwal.local', 'System', 'System', 'system', true, 'seed', true)
ON CONFLICT (email) DO NOTHING;

-- Now self-referencing FK: system user is its own creator
UPDATE developer SET created_by = 1, updated_by = 1 WHERE id = 1 AND created_by IS NULL;

-- Add self-referencing FK constraints (safe to add after bootstrap row exists)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'developer_created_by_fkey' AND table_name = 'developer'
    ) THEN
        ALTER TABLE developer
            ADD CONSTRAINT developer_created_by_fkey
            FOREIGN KEY (created_by) REFERENCES developer(id)
            ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'developer_updated_by_fkey' AND table_name = 'developer'
    ) THEN
        ALTER TABLE developer
            ADD CONSTRAINT developer_updated_by_fkey
            FOREIGN KEY (updated_by) REFERENCES developer(id)
            ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

COMMENT ON TABLE developer IS
'Registered database users. Audit fields (created_by, updated_by) on all other tables
reference this table. The system user (id=1) is the bootstrap account used for
administrative operations and as the fallback when postgres connects.
created_by/updated_by are nullable only to allow id=1 to be self-referencing.';

\echo '  developer table ready'


-- =============================================================================
-- 2. version_family
-- =============================================================================

CREATE TABLE IF NOT EXISTS version_family (
    id          SERIAL PRIMARY KEY,
    short_code  TEXT UNIQUE NOT NULL,
    label       TEXT,
    description TEXT,
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by  INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_by  INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE
);

COMMENT ON TABLE version_family IS
'Logical versioning domains (theme, scenario, network, entity, etc.). Each domain
tracks its own version lineage independently. One active version per family at a time.';

\echo '  version_family table ready'


-- =============================================================================
-- 3. version
-- =============================================================================

CREATE TABLE IF NOT EXISTS version (
    id                SERIAL PRIMARY KEY,
    version_family_id INTEGER NOT NULL
        REFERENCES version_family(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    version_number    TEXT,
    manifest          JSONB,
    changelog         TEXT,
    is_active         BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by        INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_by        INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS version_version_family_id_version_number_key
    ON version(version_family_id, version_number);

CREATE INDEX IF NOT EXISTS idx_version_family
    ON version(version_family_id);

COMMENT ON TABLE version IS
'Specific version instances within a version_family. Versions are immutable once
created — never update an existing version record, create a new one instead.
Only one version per family should have is_active = true at a time.';

\echo '  version table ready'


-- =============================================================================
-- 4. domain_family_map
-- =============================================================================

CREATE TABLE IF NOT EXISTS domain_family_map (
    schema_name       TEXT NOT NULL DEFAULT 'public',
    table_name        TEXT NOT NULL,
    version_family_id INTEGER NOT NULL
        REFERENCES version_family(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    note              TEXT,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by        INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_by        INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    PRIMARY KEY (schema_name, table_name)
);

CREATE INDEX IF NOT EXISTS idx_domain_family_map_version_family
    ON domain_family_map(version_family_id);

COMMENT ON TABLE domain_family_map IS
'Maps each database table to its version_family. Used by the versioning system to
determine which version governs a table''s data. Populated by
05_populate_domain_family_map.sql. The target_version_column is documented in the
ERD but not stored here — it is the FK column on the target table (e.g. scenario_version_id).';

\echo '  domain_family_map table ready'


-- =============================================================================
-- Verify
-- =============================================================================

\echo ''
\echo 'Versioning tables created:'
SELECT table_name, obj_description(c.oid) AS description
FROM information_schema.tables t
JOIN pg_class c ON c.relname = t.table_name
WHERE t.table_schema = 'public'
  AND t.table_name IN ('developer', 'version_family', 'version', 'domain_family_map')
ORDER BY t.table_name;

\echo ''
\echo '============================================================================'
\echo 'VERSIONING TABLES CREATED SUCCESSFULLY'
\echo 'Next: run 01_create_audit_trigger_function.sql'
\echo '============================================================================'
