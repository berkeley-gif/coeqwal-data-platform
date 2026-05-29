-- CREATE DU_URBAN_ENTITY TABLE
-- Urban demand units for M&I (Municipal & Industrial) statistics
--
-- Prerequisites:
--   hydrologic_region table exists (for FK)
--

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

    du_id VARCHAR(20) UNIQUE NOT NULL,
    wba_id VARCHAR(10),
    hydrologic_region VARCHAR(10),

    dups INTEGER DEFAULT 0,
    du_class VARCHAR(20) DEFAULT 'Urban',        -- Class (Urban)
    cs3_type VARCHAR(10),

    total_acres NUMERIC(15,7),
    polygon_count INTEGER DEFAULT 1,

    community_agency TEXT,
    gw VARCHAR(5),
    sw VARCHAR(5),
    point_of_diversion TEXT,

    source VARCHAR(100),
    model_source VARCHAR(20),
    has_gis_data BOOLEAN DEFAULT TRUE,

    primary_contractor_short_code VARCHAR(50),

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
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
