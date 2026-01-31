-- ============================================================================
-- 03_CREATE_RESERVOIR_PERCENTILE_TABLE.SQL
-- ============================================================================
-- Creates the reservoir_monthly_percentile table for storing pre-computed
-- monthly percentile statistics for reservoir storage (as % of capacity).
-- Used by frontend for percentile band charts.
--
-- Part of: 09_STATISTICS LAYER
-- Related: reservoir_entity, reservoir_group
-- ============================================================================

\echo '============================================================================'
\echo 'CREATING RESERVOIR_MONTHLY_PERCENTILE TABLE'
\echo '============================================================================'

-- Drop existing objects if recreating
DROP FUNCTION IF EXISTS upsert_reservoir_percentile CASCADE;
DROP TABLE IF EXISTS reservoir_monthly_percentile CASCADE;

-- ============================================================================
-- TABLE: reservoir_monthly_percentile
-- ============================================================================
CREATE TABLE reservoir_monthly_percentile (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    reservoir_code VARCHAR(20) NOT NULL,      -- S_SHSTA, S_OROVL, etc.
    water_month INTEGER NOT NULL,             -- 1-12 (Oct=1, Sep=12)

    -- Percentiles (% of capacity): q0=min, q50=median, q100=max
    q0 NUMERIC(6,2),     -- minimum (0th percentile)
    q10 NUMERIC(6,2),
    q30 NUMERIC(6,2),
    q50 NUMERIC(6,2),    -- median
    q70 NUMERIC(6,2),
    q90 NUMERIC(6,2),
    q100 NUMERIC(6,2),   -- maximum (100th percentile)

    -- Additional statistics
    mean_value NUMERIC(6,2),

    -- Reference data
    max_capacity_taf NUMERIC(10,2),           -- reservoir capacity in TAF

    -- Audit fields (matching ERD standard)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,    -- FK → developer.id (1 = system)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,    -- FK → developer.id

    -- Unique constraint: one row per scenario/reservoir/month combination
    CONSTRAINT uq_reservoir_percentile
        UNIQUE(scenario_short_code, reservoir_code, water_month),

    -- Check constraint for water_month range
    CONSTRAINT chk_water_month
        CHECK (water_month >= 1 AND water_month <= 12)
);

\echo 'Created table: reservoir_monthly_percentile'

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX idx_reservoir_percentile_scenario
    ON reservoir_monthly_percentile(scenario_short_code);

CREATE INDEX idx_reservoir_percentile_reservoir
    ON reservoir_monthly_percentile(reservoir_code);

CREATE INDEX idx_reservoir_percentile_scenario_reservoir
    ON reservoir_monthly_percentile(scenario_short_code, reservoir_code);

CREATE INDEX idx_reservoir_percentile_active
    ON reservoir_monthly_percentile(is_active) WHERE is_active = TRUE;

\echo 'Created indexes'

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE reservoir_monthly_percentile IS
    'Monthly percentile statistics for reservoir storage (% of capacity), used for band charts. Part of 09_STATISTICS layer.';

COMMENT ON COLUMN reservoir_monthly_percentile.scenario_short_code IS
    'Scenario identifier (e.g., s0020)';

COMMENT ON COLUMN reservoir_monthly_percentile.reservoir_code IS
    'Reservoir variable code from reservoir_entity (e.g., S_SHSTA, S_OROVL)';

COMMENT ON COLUMN reservoir_monthly_percentile.water_month IS
    'Water year month: 1=October, 2=November, ..., 12=September';

COMMENT ON COLUMN reservoir_monthly_percentile.q0 IS
    'Minimum storage as percent of reservoir capacity (0th percentile)';

COMMENT ON COLUMN reservoir_monthly_percentile.q50 IS
    'Median storage as percent of reservoir capacity (50th percentile)';

COMMENT ON COLUMN reservoir_monthly_percentile.q100 IS
    'Maximum storage as percent of reservoir capacity (100th percentile)';

COMMENT ON COLUMN reservoir_monthly_percentile.mean_value IS
    'Mean storage as percent of reservoir capacity';

COMMENT ON COLUMN reservoir_monthly_percentile.max_capacity_taf IS
    'Reservoir maximum capacity in thousand acre-feet (TAF), from reservoir_entity';

\echo 'Added comments'

-- ============================================================================
-- UPSERT FUNCTION
-- ============================================================================
CREATE OR REPLACE FUNCTION upsert_reservoir_percentile(
    p_scenario_short_code VARCHAR(20),
    p_reservoir_code VARCHAR(20),
    p_water_month INTEGER,
    p_q0 NUMERIC(6,2),
    p_q10 NUMERIC(6,2),
    p_q30 NUMERIC(6,2),
    p_q50 NUMERIC(6,2),
    p_q70 NUMERIC(6,2),
    p_q90 NUMERIC(6,2),
    p_q100 NUMERIC(6,2),
    p_mean_value NUMERIC(6,2),
    p_max_capacity_taf NUMERIC(10,2),
    p_updated_by INTEGER DEFAULT 1
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO reservoir_monthly_percentile (
        scenario_short_code, reservoir_code, water_month,
        q0, q10, q30, q50, q70, q90, q100,
        mean_value, max_capacity_taf,
        is_active, created_by, updated_by
    ) VALUES (
        p_scenario_short_code, p_reservoir_code, p_water_month,
        p_q0, p_q10, p_q30, p_q50, p_q70, p_q90, p_q100,
        p_mean_value, p_max_capacity_taf,
        TRUE, p_updated_by, p_updated_by
    )
    ON CONFLICT (scenario_short_code, reservoir_code, water_month)
    DO UPDATE SET
        q0 = EXCLUDED.q0,
        q10 = EXCLUDED.q10,
        q30 = EXCLUDED.q30,
        q50 = EXCLUDED.q50,
        q70 = EXCLUDED.q70,
        q90 = EXCLUDED.q90,
        q100 = EXCLUDED.q100,
        mean_value = EXCLUDED.mean_value,
        max_capacity_taf = EXCLUDED.max_capacity_taf,
        is_active = TRUE,
        updated_at = NOW(),
        updated_by = p_updated_by
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION upsert_reservoir_percentile IS
    'Insert or update reservoir percentile data with proper audit tracking';

\echo 'Created upsert function'

-- ============================================================================
-- VERIFICATION
-- ============================================================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_name = 'reservoir_monthly_percentile') as column_count
FROM information_schema.tables
WHERE table_name = 'reservoir_monthly_percentile';

\echo ''
\echo 'Columns:'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'reservoir_monthly_percentile'
ORDER BY ordinal_position;

\echo ''
\echo '============================================================================'
\echo 'RESERVOIR_MONTHLY_PERCENTILE TABLE CREATED SUCCESSFULLY'
\echo '============================================================================'
\echo ''