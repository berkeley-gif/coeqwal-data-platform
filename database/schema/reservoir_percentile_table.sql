-- ============================================================================
-- Reservoir Monthly Percentile Table Schema
-- ============================================================================
-- Stores monthly percentile statistics for reservoir storage (% of capacity)
-- Used for percentile band charts in the frontend
--
-- Part of: 09_STATISTICS LAYER
-- Related: reservoir_entity, reservoir_group
-- Deployment script: database/scripts/sql/09_statistics/03_create_reservoir_percentile_table.sql
-- ============================================================================

CREATE TABLE IF NOT EXISTS reservoir_monthly_percentile (
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

    -- Constraints
    CONSTRAINT uq_reservoir_percentile
        UNIQUE(scenario_short_code, reservoir_code, water_month),
    CONSTRAINT chk_water_month
        CHECK (water_month >= 1 AND water_month <= 12)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_scenario
    ON reservoir_monthly_percentile(scenario_short_code);

CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_reservoir
    ON reservoir_monthly_percentile(reservoir_code);

CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_scenario_reservoir
    ON reservoir_monthly_percentile(scenario_short_code, reservoir_code);

CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_active
    ON reservoir_monthly_percentile(is_active) WHERE is_active = TRUE;
