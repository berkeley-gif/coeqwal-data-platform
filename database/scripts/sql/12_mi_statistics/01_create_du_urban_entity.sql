-- CREATE DU_URBAN_ENTITY TABLE
-- Urban demand units for M&I (Municipal & Industrial) statistics
--
-- Prerequisites:
--   hydrologic_region table exists (for FK)
--
-- Run with: psql -f 01_create_du_urban_entity.sql

\echo ''
\echo '========================================='
\echo 'CREATING DU_URBAN_ENTITY TABLE'
\echo '========================================='

-- ============================================
-- DROP IF EXISTS (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS du_urban_entity CASCADE;

-- ============================================
-- CREATE DU_URBAN_ENTITY TABLE
-- ============================================
\echo ''
\echo 'Creating du_urban_entity table...'

CREATE TABLE du_urban_entity (
    id SERIAL PRIMARY KEY,

    -- Core identifiers (from CSV)
    du_id VARCHAR(20) UNIQUE NOT NULL,          -- Demand unit ID (e.g., "02_NU", "02_PU")
    wba_id VARCHAR(10),                          -- Water Budget Area ID
    hydrologic_region VARCHAR(10),               -- SAC, SJR, TULARE, etc.

    -- Classification
    dups INTEGER DEFAULT 0,                      -- Duplicate flag
    du_class VARCHAR(20) DEFAULT 'Urban',        -- Class (Urban)
    cs3_type VARCHAR(10),                        -- CalSim3 type (NU, PU, SU)

    -- Physical attributes
    total_acres NUMERIC(15,7),                   -- Total area in acres
    polygon_count INTEGER DEFAULT 1,             -- Number of polygons

    -- Urban-specific attributes
    community_agency TEXT,                       -- Community/agency served
    gw VARCHAR(5),                               -- Groundwater (0/1)
    sw VARCHAR(5),                               -- Surface water (0/1)
    point_of_diversion TEXT,                     -- Point of diversion description

    -- Source tracking
    source VARCHAR(100),                         -- Data source(s)
    model_source VARCHAR(20),                    -- Model source (calsim3)
    has_gis_data BOOLEAN DEFAULT TRUE,           -- Whether GIS data exists

    -- Contractor relationship (NEW)
    primary_contractor_short_code VARCHAR(50),   -- FK to mi_contractor.short_code (to be populated)

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,       -- FK → developer.id (1 = system)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1        -- FK → developer.id
);

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

CREATE INDEX idx_du_urban_entity_du_id ON du_urban_entity(du_id);
CREATE INDEX idx_du_urban_entity_wba_id ON du_urban_entity(wba_id);
CREATE INDEX idx_du_urban_entity_region ON du_urban_entity(hydrologic_region);
CREATE INDEX idx_du_urban_entity_type ON du_urban_entity(cs3_type);
CREATE INDEX idx_du_urban_entity_contractor ON du_urban_entity(primary_contractor_short_code);

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE du_urban_entity IS 'Urban demand units for M&I (Municipal & Industrial) water delivery statistics. Maps to UD_* columns in DEMANDS files.';
COMMENT ON COLUMN du_urban_entity.du_id IS 'Unique demand unit identifier (e.g., 02_NU, 02_PU). Maps to UD_{du_id} columns in DEMANDS files.';
COMMENT ON COLUMN du_urban_entity.cs3_type IS 'CalSim3 demand type: NU=Non-project Urban, PU=Project Urban, SU=Settlement Urban';
COMMENT ON COLUMN du_urban_entity.primary_contractor_short_code IS 'FK to mi_contractor.short_code - the primary water contractor serving this demand unit';

\echo ''
\echo '✅ du_urban_entity table created successfully'
\echo ''

-- ============================================
-- VERIFICATION
-- ============================================
\echo 'Table structure:'
\d du_urban_entity
