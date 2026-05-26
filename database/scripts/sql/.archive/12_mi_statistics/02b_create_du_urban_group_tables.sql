-- CREATE DU_URBAN_GROUP TABLES
-- Creates group tables for organizing urban demand units into logical groups
-- Follows the reservoir_group pattern for consistency
--
-- Tables created:
--   - du_urban_group: Group definitions (tier, nod, sod, swp_served, etc.)
--   - du_urban_group_member: Many-to-many linking DUs to groups
--
-- Run with: psql -f 02b_create_du_urban_group_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING DU_URBAN_GROUP TABLES'
\echo '========================================='

-- ============================================
-- 1. DU_URBAN_GROUP (like reservoir_group)
-- ============================================
\echo ''
\echo 'Creating du_urban_group table...'

CREATE TABLE IF NOT EXISTS du_urban_group (
    id INTEGER PRIMARY KEY,
    short_code VARCHAR(50) UNIQUE NOT NULL,
    label VARCHAR(100) NOT NULL,
    description TEXT,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

COMMENT ON TABLE du_urban_group IS 'Groups for organizing urban demand units (e.g., tier, nod, sod, swp_served)';
COMMENT ON COLUMN du_urban_group.short_code IS 'Unique identifier for the group (e.g., tier, nod, sod)';
COMMENT ON COLUMN du_urban_group.label IS 'Display name for the group';

\echo '✅ du_urban_group table created'

-- ============================================
-- 2. DU_URBAN_GROUP_MEMBER (like reservoir_group_member)
-- ============================================
\echo ''
\echo 'Creating du_urban_group_member table...'

CREATE TABLE IF NOT EXISTS du_urban_group_member (
    id SERIAL PRIMARY KEY,
    du_urban_group_id INTEGER NOT NULL,
    du_id VARCHAR(20) NOT NULL,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT fk_du_group_member_group
        FOREIGN KEY (du_urban_group_id) REFERENCES du_urban_group(id) ON DELETE CASCADE,
    CONSTRAINT fk_du_group_member_du
        FOREIGN KEY (du_id) REFERENCES du_urban_entity(du_id) ON DELETE CASCADE,
    CONSTRAINT uq_du_urban_group_member
        UNIQUE(du_urban_group_id, du_id)
);

COMMENT ON TABLE du_urban_group_member IS 'Many-to-many relationship between urban demand units and groups';
COMMENT ON COLUMN du_urban_group_member.du_id IS 'Reference to du_urban_entity.du_id';

\echo '✅ du_urban_group_member table created'

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

CREATE INDEX IF NOT EXISTS idx_du_urban_group_short_code
    ON du_urban_group(short_code);

CREATE INDEX IF NOT EXISTS idx_du_urban_group_member_group
    ON du_urban_group_member(du_urban_group_id);

CREATE INDEX IF NOT EXISTS idx_du_urban_group_member_du
    ON du_urban_group_member(du_id);

\echo '✅ Indexes created'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Tables created:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('du_urban_group', 'du_urban_group_member')
ORDER BY table_name;

\echo ''
\echo '✅ DU_URBAN_GROUP tables created successfully'
