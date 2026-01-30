-- CREATE RESERVOIR ENTITY TABLES
-- Part of: ENTITY LAYER
-- Tables: reservoir_entity, reservoir_group, reservoir_group_member
--
-- Run with: psql -f 01_create_reservoir_entity_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING RESERVOIR ENTITY TABLES'
\echo '========================================='

-- ============================================
-- 1. RESERVOIR_ENTITY TABLE
-- ============================================
\echo ''
\echo 'Creating reservoir_entity table...'

CREATE TABLE IF NOT EXISTS reservoir_entity (
    id INTEGER PRIMARY KEY,
    network_node_id VARCHAR(20) NOT NULL,
    short_code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100),
    description TEXT,
    associated_river VARCHAR(100),
    entity_type_id INTEGER NOT NULL DEFAULT 1,
    schematic_type_id INTEGER,
    hydrologic_region_id INTEGER,
    capacity_taf NUMERIC(10,2),
    dead_pool_taf NUMERIC(10,2),
    surface_area_acres NUMERIC(12,2),
    operational_purpose VARCHAR(50),
    has_tiers BOOLEAN DEFAULT FALSE,
    is_main BOOLEAN DEFAULT FALSE,
    has_gis_data INTEGER DEFAULT 1,
    entity_version_id INTEGER NOT NULL DEFAULT 1,
    source_ids TEXT,

    -- Audit fields
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_reservoir_entity_short_code ON reservoir_entity(short_code);
CREATE INDEX IF NOT EXISTS idx_reservoir_entity_region ON reservoir_entity(hydrologic_region_id);
CREATE INDEX IF NOT EXISTS idx_reservoir_entity_has_tiers ON reservoir_entity(has_tiers) WHERE has_tiers = TRUE;
CREATE INDEX IF NOT EXISTS idx_reservoir_entity_is_main ON reservoir_entity(is_main) WHERE is_main = TRUE;

-- Comments
COMMENT ON TABLE reservoir_entity IS 'Reservoir management entities with capacity and operational attributes. Part of ENTITY LAYER.';
COMMENT ON COLUMN reservoir_entity.short_code IS 'Short identifier (SHSTA, OROVL, etc.) - matches network.short_code';
COMMENT ON COLUMN reservoir_entity.capacity_taf IS 'Maximum reservoir capacity in thousand acre-feet (TAF)';
COMMENT ON COLUMN reservoir_entity.dead_pool_taf IS 'Dead pool storage in TAF - unusable storage at bottom';
COMMENT ON COLUMN reservoir_entity.has_tiers IS 'TRUE if reservoir is included in tier analysis';
COMMENT ON COLUMN reservoir_entity.is_main IS 'TRUE if reservoir is a major system reservoir';
COMMENT ON COLUMN reservoir_entity.hydrologic_region_id IS 'FK to hydrologic_region: 1=SAC(NOD), 2=SJR(SOD), 4=Tulare(SOD)';

\echo '✅ reservoir_entity table created'

-- ============================================
-- 2. RESERVOIR_GROUP TABLE
-- ============================================
\echo ''
\echo 'Creating reservoir_group table...'

CREATE TABLE IF NOT EXISTS reservoir_group (
    id INTEGER PRIMARY KEY,
    short_code VARCHAR(50) UNIQUE NOT NULL,
    label VARCHAR(100) NOT NULL,
    description TEXT,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_reservoir_group_short_code ON reservoir_group(short_code);

-- Comments
COMMENT ON TABLE reservoir_group IS 'Reservoir grouping definitions (major, cvp, swp, tier). Part of ENTITY LAYER.';
COMMENT ON COLUMN reservoir_group.short_code IS 'Group identifier: major, cvp, swp, tier';

\echo '✅ reservoir_group table created'

-- ============================================
-- 3. RESERVOIR_GROUP_MEMBER TABLE
-- ============================================
\echo ''
\echo 'Creating reservoir_group_member table...'

CREATE TABLE IF NOT EXISTS reservoir_group_member (
    id INTEGER PRIMARY KEY,
    reservoir_group_id INTEGER NOT NULL REFERENCES reservoir_group(id) ON DELETE CASCADE,
    reservoir_entity_id INTEGER NOT NULL REFERENCES reservoir_entity(id) ON DELETE CASCADE,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Unique constraint
    CONSTRAINT uq_reservoir_group_member UNIQUE(reservoir_group_id, reservoir_entity_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_reservoir_group_member_group ON reservoir_group_member(reservoir_group_id);
CREATE INDEX IF NOT EXISTS idx_reservoir_group_member_reservoir ON reservoir_group_member(reservoir_entity_id);

-- Comments
COMMENT ON TABLE reservoir_group_member IS 'Junction table linking reservoirs to groups. A reservoir can belong to multiple groups.';

\echo '✅ reservoir_group_member table created'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('reservoir_entity', 'reservoir_group', 'reservoir_group_member')
ORDER BY table_name;

\echo ''
\echo '✅ All reservoir entity tables created successfully'
