-- CREATE DU_URBAN_VARIABLE TABLE
-- Maps urban demand units to their CalSim delivery and shortage variables
--
-- This table follows the same pattern as mi_contractor_delivery_arc,
-- providing the variable mappings needed for ETL processing.
--
-- Variable types:
--   - DL_*    : Total delivery to demand unit (primary delivery variable)
--   - D_*     : Arc delivery from specific source (for units with multiple sources)
--   - SHRTG_* : Surface water shortage
--   - GW_SHORT_*: Groundwater restriction shortage
--
-- Prerequisites:
--   1. Run 01_create_du_urban_entity.sql first
--
-- Run with: psql -f 01c_create_du_urban_variable.sql

\echo ''
\echo '========================================='
\echo 'CREATING DU_URBAN_VARIABLE TABLE'
\echo '========================================='

-- ============================================
-- DROP IF EXISTS (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS du_urban_variable CASCADE;

-- ============================================
-- CREATE DU_URBAN_VARIABLE TABLE
-- ============================================
\echo ''
\echo 'Creating du_urban_variable table...'

CREATE TABLE du_urban_variable (
    id SERIAL PRIMARY KEY,
    
    -- Reference to demand unit
    du_id VARCHAR(20) NOT NULL,                   -- FK → du_urban_entity.du_id
    
    -- Variable mappings
    delivery_variable VARCHAR(100) NOT NULL,      -- CalSim variable for delivery (DL_*, D_*)
    shortage_variable VARCHAR(100),               -- CalSim variable for shortage (SHRTG_*, GW_SHORT_*)
    
    -- Variable metadata
    variable_type VARCHAR(20) NOT NULL DEFAULT 'delivery',  -- 'delivery', 'gw_pumping', 'diversion', etc.
    requires_sum BOOLEAN DEFAULT FALSE,           -- TRUE if multiple D_* arcs need summing
    
    -- Notes
    notes TEXT,                                   -- Additional context about the mapping
    
    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,
    
    -- Constraints
    CONSTRAINT fk_du_urban_variable_du
        FOREIGN KEY (du_id) REFERENCES du_urban_entity(du_id) ON DELETE CASCADE,
    CONSTRAINT uq_du_urban_variable_du_id
        UNIQUE(du_id)
);

-- ============================================
-- CREATE DU_URBAN_DELIVERY_ARC TABLE
-- ============================================
-- For demand units that have multiple delivery arcs that need summing
-- (similar to mi_contractor_delivery_arc)
\echo ''
\echo 'Creating du_urban_delivery_arc table...'

CREATE TABLE du_urban_delivery_arc (
    id SERIAL PRIMARY KEY,
    
    -- Reference to demand unit
    du_id VARCHAR(20) NOT NULL,                   -- FK → du_urban_entity.du_id
    
    -- Arc information
    delivery_arc VARCHAR(100) NOT NULL,           -- CalSim arc variable (D_WTPNBR_FRFLD, etc.)
    arc_order INTEGER DEFAULT 1,                  -- Order for summing (if relevant)
    
    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,
    
    -- Constraints
    CONSTRAINT fk_du_urban_delivery_arc_du
        FOREIGN KEY (du_id) REFERENCES du_urban_entity(du_id) ON DELETE CASCADE,
    CONSTRAINT uq_du_urban_delivery_arc
        UNIQUE(du_id, delivery_arc)
);

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

CREATE INDEX idx_du_urban_variable_du_id ON du_urban_variable(du_id);
CREATE INDEX idx_du_urban_variable_type ON du_urban_variable(variable_type);
CREATE INDEX idx_du_urban_delivery_arc_du_id ON du_urban_delivery_arc(du_id);

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE du_urban_variable IS 'Maps urban demand units to their CalSim delivery and shortage variables. Used by ETL to extract correct data for each demand unit.';
COMMENT ON COLUMN du_urban_variable.du_id IS 'Reference to du_urban_entity.du_id (e.g., 02_PU, FRFLD)';
COMMENT ON COLUMN du_urban_variable.delivery_variable IS 'Primary CalSim delivery variable. For DL type: DL_{du_id}. For D type: D_*_{du_id} or specific arc.';
COMMENT ON COLUMN du_urban_variable.shortage_variable IS 'CalSim shortage variable. Can be SHRTG_{du_id} or GW_SHORT_{du_id} depending on unit type.';
COMMENT ON COLUMN du_urban_variable.variable_type IS 'Type of water supply measurement: delivery (surface water), gw_pumping (groundwater), diversion, etc.';
COMMENT ON COLUMN du_urban_variable.requires_sum IS 'TRUE if delivery is sum of multiple D_* arcs (e.g., FRFLD = D_WTPNBR_FRFLD + D_WTPWMN_FRFLD)';

COMMENT ON TABLE du_urban_delivery_arc IS 'Delivery arcs for demand units with multiple sources. Sum all arcs for total delivery.';
COMMENT ON COLUMN du_urban_delivery_arc.du_id IS 'Reference to du_urban_entity.du_id';
COMMENT ON COLUMN du_urban_delivery_arc.delivery_arc IS 'CalSim arc variable name (D_*)';

\echo ''
\echo '✅ du_urban_variable table created successfully'
\echo ''

-- ============================================
-- VERIFICATION
-- ============================================
\echo 'Table structure:'
\d du_urban_variable
