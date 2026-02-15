-- ============================================================================
-- 04_CREATE_RESERVOIR_STORAGE_MONTHLY.SQL
-- ============================================================================
-- Creates the reservoir_storage_monthly table for storing pre-computed
-- monthly storage statistics for all 92 reservoirs.
--
-- Part of: 09_STATISTICS LAYER
-- Related: reservoir_entity (FK), reservoir_variable
-- ============================================================================

\echo '============================================================================'
\echo 'CREATING RESERVOIR_STORAGE_MONTHLY TABLE'
\echo '============================================================================'

-- Drop existing objects if recreating
DROP TABLE IF EXISTS reservoir_storage_monthly CASCADE;

-- ============================================================================
-- TABLE: reservoir_storage_monthly
-- ============================================================================
-- Monthly storage statistics (12 months × 92 reservoirs × 8 scenarios = 8,832 rows)

CREATE TABLE reservoir_storage_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    reservoir_entity_id INTEGER NOT NULL,         -- FK → reservoir_entity.id
    water_month INTEGER NOT NULL,                 -- 1-12 (Oct=1, Sep=12)

    -- Storage statistics (TAF)
    storage_avg_taf NUMERIC(10,2),                -- Mean storage
    storage_cv NUMERIC(6,4),                      -- Coefficient of variation
    storage_pct_capacity NUMERIC(6,2),            -- Mean as % of capacity

    -- Storage percentiles (% of capacity)
    q0 NUMERIC(6,2),                              -- min (0th percentile)
    q10 NUMERIC(6,2),
    q30 NUMERIC(6,2),
    q50 NUMERIC(6,2),                             -- median
    q70 NUMERIC(6,2),
    q90 NUMERIC(6,2),
    q100 NUMERIC(6,2),                            -- max

    -- Metadata
    capacity_taf NUMERIC(10,2),                   -- Denormalized for convenience
    sample_count INTEGER,                         -- Number of months in sample

    -- Audit fields (ERD standard)
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,        -- FK → developer.id
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,        -- FK → developer.id

    -- Foreign key constraints
    CONSTRAINT fk_storage_monthly_reservoir_entity
        FOREIGN KEY (reservoir_entity_id) REFERENCES reservoir_entity(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    -- Unique constraint
    CONSTRAINT uq_storage_monthly
        UNIQUE(scenario_short_code, reservoir_entity_id, water_month),
    CONSTRAINT chk_storage_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

\echo 'Created table: reservoir_storage_monthly'

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX idx_storage_monthly_scenario
    ON reservoir_storage_monthly(scenario_short_code);

CREATE INDEX idx_storage_monthly_entity
    ON reservoir_storage_monthly(reservoir_entity_id);

CREATE INDEX idx_storage_monthly_combined
    ON reservoir_storage_monthly(scenario_short_code, reservoir_entity_id);

CREATE INDEX idx_storage_monthly_active
    ON reservoir_storage_monthly(is_active) WHERE is_active = TRUE;

\echo 'Created indexes'

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE reservoir_storage_monthly IS
    'Monthly storage statistics by water month for all 92 reservoirs. Part of 09_STATISTICS layer. References reservoir_entity via FK.';

COMMENT ON COLUMN reservoir_storage_monthly.scenario_short_code IS
    'Scenario identifier (e.g., s0020). Logical reference for ETL flexibility.';

COMMENT ON COLUMN reservoir_storage_monthly.reservoir_entity_id IS
    'FK to reservoir_entity.id. Join to get short_code (SHSTA), capacity, dead_pool, etc.';

COMMENT ON COLUMN reservoir_storage_monthly.water_month IS
    'Water year month: 1=October, 2=November, ..., 12=September';

COMMENT ON COLUMN reservoir_storage_monthly.storage_avg_taf IS
    'Mean storage volume in TAF for this month';

COMMENT ON COLUMN reservoir_storage_monthly.storage_cv IS
    'Coefficient of variation (std/mean) of storage';

COMMENT ON COLUMN reservoir_storage_monthly.storage_pct_capacity IS
    'Mean storage as percent of physical capacity';

COMMENT ON COLUMN reservoir_storage_monthly.q0 IS
    'Minimum storage as percent of capacity (0th percentile)';

COMMENT ON COLUMN reservoir_storage_monthly.q50 IS
    'Median storage as percent of capacity (50th percentile)';

COMMENT ON COLUMN reservoir_storage_monthly.q100 IS
    'Maximum storage as percent of capacity (100th percentile)';

COMMENT ON COLUMN reservoir_storage_monthly.capacity_taf IS
    'Reservoir physical capacity in TAF (denormalized from reservoir_entity)';

COMMENT ON COLUMN reservoir_storage_monthly.sample_count IS
    'Number of monthly observations in the sample';

\echo 'Added comments'

-- ============================================================================
-- VERIFICATION
-- ============================================================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_name = 'reservoir_storage_monthly') as column_count
FROM information_schema.tables
WHERE table_name = 'reservoir_storage_monthly';

\echo ''
\echo 'Columns:'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'reservoir_storage_monthly'
ORDER BY ordinal_position;

\echo ''
\echo '============================================================================'
\echo 'RESERVOIR_STORAGE_MONTHLY TABLE CREATED SUCCESSFULLY'
\echo '============================================================================'
\echo ''
