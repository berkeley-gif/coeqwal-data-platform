-- CREATE AGRICULTURAL ENTITY TABLES
-- Entity definitions for agricultural demand units and aggregates
--
-- Tables created:
--   1. du_agriculture_entity - Agricultural demand units (132+)
--   2. ag_aggregate_entity - Pre-computed aggregates (SWP/CVP PAG)
--
-- Run with: psql -f 01_create_ag_entity_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING AGRICULTURAL ENTITY TABLES'
\echo '========================================='

-- ============================================
-- DROP EXISTING TABLES (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS ag_aggregate_entity CASCADE;
DROP TABLE IF EXISTS du_agriculture_entity CASCADE;

-- ============================================
-- 1. DU_AGRICULTURE_ENTITY
-- Agricultural demand units from CalSim3
-- ============================================
\echo ''
\echo 'Creating du_agriculture_entity table...'

CREATE TABLE du_agriculture_entity (
    id SERIAL PRIMARY KEY,
    du_id VARCHAR(20) UNIQUE NOT NULL,       -- 02_NA, 64_PA1, etc.
    wba_id VARCHAR(10),                       -- Water Budget Area ID
    hydrologic_region VARCHAR(20) NOT NULL,   -- SAC, SJR, TULARE
    dups INTEGER,                             -- Duplicate indicator
    du_class VARCHAR(50),                     -- Class (e.g., Agriculture)
    cs3_type VARCHAR(10),                     -- PA, SA, XA, PR, NR, or blank for NA
    total_acres NUMERIC(12,2),                -- Total irrigated acres
    polygon_count INTEGER,                    -- Number of GIS polygons
    source VARCHAR(100),                      -- Data source (geopackage, calsim_report)
    model_source VARCHAR(50),                 -- Model source (calsim3)
    agency VARCHAR(200),                      -- Agency/district name
    provider VARCHAR(50),                     -- CVP, SWP, Reclamation, or blank
    gw BOOLEAN DEFAULT TRUE,                  -- Has groundwater source
    sw BOOLEAN DEFAULT TRUE,                  -- Has surface water source
    point_of_diversion TEXT,                  -- Point of diversion description
    diversion_arc VARCHAR(50),                -- CalSim diversion arc variable
    river_reach VARCHAR(200),                 -- River reach description
    river_mile_start NUMERIC(10,2),           -- River mile start
    river_mile_end NUMERIC(10,2),             -- River mile end
    bank VARCHAR(20),                         -- River bank (Left, Right)
    area_acres NUMERIC(12,2),                 -- Area in acres
    annual_diversion_taf NUMERIC(10,2),       -- Annual diversion in TAF
    demand_unit VARCHAR(20),                  -- Demand unit reference
    table_id VARCHAR(50),                     -- Table ID reference
    has_gis_data BOOLEAN DEFAULT TRUE,        -- Has GIS polygon data

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

-- Comments
COMMENT ON TABLE du_agriculture_entity IS 'Agricultural demand units from CalSim3 model. Source: du_agriculture_entity.csv';
COMMENT ON COLUMN du_agriculture_entity.du_id IS 'Demand unit identifier (e.g., 02_NA, 64_PA1, 72_XA1)';
COMMENT ON COLUMN du_agriculture_entity.wba_id IS 'Water Budget Area ID for groundwater accounting';
COMMENT ON COLUMN du_agriculture_entity.hydrologic_region IS 'Hydrologic region: SAC (Sacramento), SJR (San Joaquin), TULARE';
COMMENT ON COLUMN du_agriculture_entity.dups IS 'Duplicate indicator from source data';
COMMENT ON COLUMN du_agriculture_entity.du_class IS 'Demand unit class (e.g., Agriculture)';
COMMENT ON COLUMN du_agriculture_entity.cs3_type IS 'CalSim3 contract type: PA=Project AG, SA=Settlement AG, XA=Exchange AG, PR=Project Refuge, NR=Non-project Refuge, blank=Non-project';
COMMENT ON COLUMN du_agriculture_entity.polygon_count IS 'Number of GIS polygons for this demand unit';
COMMENT ON COLUMN du_agriculture_entity.source IS 'Data source (geopackage, calsim_report)';
COMMENT ON COLUMN du_agriculture_entity.model_source IS 'Model source (calsim3)';
COMMENT ON COLUMN du_agriculture_entity.provider IS 'Water provider: CVP, SWP, Reclamation, or blank for non-project';
COMMENT ON COLUMN du_agriculture_entity.point_of_diversion IS 'Description of water diversion point';
COMMENT ON COLUMN du_agriculture_entity.river_reach IS 'River reach description';
COMMENT ON COLUMN du_agriculture_entity.river_mile_start IS 'River mile at start of reach';
COMMENT ON COLUMN du_agriculture_entity.river_mile_end IS 'River mile at end of reach';
COMMENT ON COLUMN du_agriculture_entity.bank IS 'River bank (Left or Right)';
COMMENT ON COLUMN du_agriculture_entity.area_acres IS 'Area in acres';
COMMENT ON COLUMN du_agriculture_entity.annual_diversion_taf IS 'Annual diversion in thousand acre-feet';

-- Indexes
CREATE INDEX idx_du_ag_region ON du_agriculture_entity(hydrologic_region);
CREATE INDEX idx_du_ag_type ON du_agriculture_entity(cs3_type);
CREATE INDEX idx_du_ag_provider ON du_agriculture_entity(provider);
CREATE INDEX idx_du_ag_wba ON du_agriculture_entity(wba_id);

-- ============================================
-- 2. AG_AGGREGATE_ENTITY
-- Pre-computed aggregate delivery variables
-- ============================================
\echo ''
\echo 'Creating ag_aggregate_entity table...'

CREATE TABLE ag_aggregate_entity (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR(50) UNIQUE NOT NULL,   -- swp_pag, cvp_pag_n, etc.
    label VARCHAR(100) NOT NULL,              -- Display label
    project VARCHAR(10),                      -- SWP, CVP
    region VARCHAR(10),                       -- NOD, SOD, TOTAL
    delivery_variable VARCHAR(100) NOT NULL,  -- CalSim variable: DEL_SWP_PAG, etc.
    description TEXT,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

-- Comments
COMMENT ON TABLE ag_aggregate_entity IS 'Agricultural delivery aggregates from CalSim3. Pre-computed project-level totals.';
COMMENT ON COLUMN ag_aggregate_entity.short_code IS 'Short identifier for API use';
COMMENT ON COLUMN ag_aggregate_entity.delivery_variable IS 'CalSim variable name for delivery aggregate';

-- Index
CREATE INDEX idx_ag_agg_project ON ag_aggregate_entity(project);

-- ============================================
-- SEED DATA: AG_AGGREGATE_ENTITY
-- ============================================
\echo ''
\echo 'Inserting ag_aggregate_entity seed data...'

INSERT INTO ag_aggregate_entity (short_code, label, project, region, delivery_variable, description, display_order) VALUES
    ('swp_pag', 'SWP Project AG', 'SWP', 'TOTAL', 'DEL_SWP_PAG', 'State Water Project agricultural deliveries - Total', 1),
    ('swp_pag_n', 'SWP Project AG North', 'SWP', 'NOD', 'DEL_SWP_PAG_N', 'State Water Project agricultural deliveries - North of Delta', 2),
    ('swp_pag_s', 'SWP Project AG South', 'SWP', 'SOD', 'DEL_SWP_PAG_S', 'State Water Project agricultural deliveries - South of Delta', 3),
    ('cvp_pag_n', 'CVP Project AG North', 'CVP', 'NOD', 'DEL_CVP_PAG_N', 'Central Valley Project agricultural deliveries - North of Delta', 4),
    ('cvp_pag_s', 'CVP Project AG South', 'CVP', 'SOD', 'DEL_CVP_PAG_S', 'Central Valley Project agricultural deliveries - South of Delta', 5);

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo '✅ Agricultural entity tables created successfully'
\echo ''
\echo 'Tables created:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('du_agriculture_entity', 'ag_aggregate_entity')
ORDER BY table_name;

\echo ''
\echo 'ag_aggregate_entity seed data:'
SELECT short_code, label, project, region, delivery_variable FROM ag_aggregate_entity ORDER BY display_order;

\echo ''
\echo 'Next steps:'
\echo '  1. Load du_agriculture_entity from S3:'
\echo '     psql -f 01b_load_du_agriculture_entity_from_s3.sql'
\echo '  2. Run 02_create_ag_statistics_tables.sql'
