-- ============================================================================
-- 05_CREATE_RESERVOIR_SPILL_MONTHLY.SQL
-- ============================================================================
-- Creates the reservoir_spill_monthly table for storing pre-computed
-- monthly spill (flood release) statistics for all 92 reservoirs.
--
-- Spill = C_*_FLOOD variable (flood release above release capacity)
-- From constraints-FloodSpill.wresl: C_{res}_NCF + C_{res}_Flood = C_{res}
--
-- Part of: 09_STATISTICS LAYER
-- Related: reservoir_entity, reservoir_variable
-- ============================================================================

\echo '============================================================================'
\echo 'CREATING RESERVOIR_SPILL_MONTHLY TABLE'
\echo '============================================================================'

-- Drop existing objects if recreating
DROP TABLE IF EXISTS reservoir_spill_monthly CASCADE;

-- ============================================================================
-- TABLE: reservoir_spill_monthly
-- ============================================================================
-- Monthly spill statistics (12 months × 92 reservoirs × 8 scenarios = 8,832 rows)

CREATE TABLE reservoir_spill_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    reservoir_code VARCHAR(20) NOT NULL,       -- S_SHSTA, S_OROVL, etc.
    water_month INTEGER NOT NULL,              -- 1-12 (Oct=1, Sep=12)

    -- Spill frequency this month
    spill_months_count INTEGER,                -- Count of months with spill > 0
    total_months INTEGER,                      -- Total months in sample
    spill_frequency_pct NUMERIC(5,2),          -- % of months with spill

    -- Spill magnitude when spilling (CFS)
    spill_avg_cfs NUMERIC(10,2),               -- Mean spill when > 0
    spill_max_cfs NUMERIC(10,2),               -- Max spill this month

    -- Spill exceedance percentiles (CFS) - of non-zero values
    spill_q50 NUMERIC(10,2),                   -- Median when spilling
    spill_q90 NUMERIC(10,2),                   -- 90th percentile
    spill_q100 NUMERIC(10,2),                  -- Max (same as spill_max_cfs)

    -- Storage threshold for spill context
    storage_at_spill_avg_pct NUMERIC(6,2),     -- Avg storage % when spilling

    -- Audit fields (ERD standard)
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_spill_monthly
        UNIQUE(scenario_short_code, reservoir_code, water_month),
    CONSTRAINT chk_spill_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

\echo 'Created table: reservoir_spill_monthly'

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX idx_spill_monthly_scenario
    ON reservoir_spill_monthly(scenario_short_code);

CREATE INDEX idx_spill_monthly_reservoir
    ON reservoir_spill_monthly(reservoir_code);

CREATE INDEX idx_spill_monthly_combined
    ON reservoir_spill_monthly(scenario_short_code, reservoir_code);

CREATE INDEX idx_spill_monthly_frequency
    ON reservoir_spill_monthly(spill_frequency_pct DESC);

CREATE INDEX idx_spill_monthly_active
    ON reservoir_spill_monthly(is_active) WHERE is_active = TRUE;

\echo 'Created indexes'

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE reservoir_spill_monthly IS
    'Monthly spill (flood release) statistics by water month. Spill = C_*_FLOOD variable. Part of 09_STATISTICS layer.';

COMMENT ON COLUMN reservoir_spill_monthly.scenario_short_code IS
    'Scenario identifier (e.g., s0020)';

COMMENT ON COLUMN reservoir_spill_monthly.reservoir_code IS
    'Reservoir code (e.g., S_SHSTA). Corresponds to C_{code}_FLOOD spill variable.';

COMMENT ON COLUMN reservoir_spill_monthly.water_month IS
    'Water year month: 1=October, 2=November, ..., 12=September';

COMMENT ON COLUMN reservoir_spill_monthly.spill_months_count IS
    'Number of months in the sample where spill > 0';

COMMENT ON COLUMN reservoir_spill_monthly.total_months IS
    'Total number of months in the sample for this water month';

COMMENT ON COLUMN reservoir_spill_monthly.spill_frequency_pct IS
    'Percentage of months with flood spill > 0 for this water month';

COMMENT ON COLUMN reservoir_spill_monthly.spill_avg_cfs IS
    'Mean spill magnitude (CFS) when spilling (spill > 0 only)';

COMMENT ON COLUMN reservoir_spill_monthly.spill_max_cfs IS
    'Maximum spill magnitude (CFS) observed in this water month';

COMMENT ON COLUMN reservoir_spill_monthly.spill_q50 IS
    'Median spill (CFS) when spilling (50th percentile of non-zero values)';

COMMENT ON COLUMN reservoir_spill_monthly.spill_q90 IS
    '90th percentile of spill (CFS) when spilling';

COMMENT ON COLUMN reservoir_spill_monthly.spill_q100 IS
    'Maximum spill (CFS) when spilling (same as spill_max_cfs)';

COMMENT ON COLUMN reservoir_spill_monthly.storage_at_spill_avg_pct IS
    'Average storage (% capacity) when spill occurs - indicates the spill threshold';

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
     WHERE table_name = 'reservoir_spill_monthly') as column_count
FROM information_schema.tables
WHERE table_name = 'reservoir_spill_monthly';

\echo ''
\echo 'Columns:'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'reservoir_spill_monthly'
ORDER BY ordinal_position;

\echo ''
\echo '============================================================================'
\echo 'RESERVOIR_SPILL_MONTHLY TABLE CREATED SUCCESSFULLY'
\echo '============================================================================'
\echo ''
