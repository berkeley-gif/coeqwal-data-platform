-- ============================================================================
-- 06_CREATE_RESERVOIR_PERIOD_SUMMARY.SQL
-- ============================================================================
-- Creates the reservoir_period_summary table for storing period-of-record
-- summary statistics for all 92 reservoirs.
--
-- Includes comprehensive spill metrics:
-- - Spill frequency (% of years)
-- - Spill magnitude (mean, peak)
-- - Annual spill volume (TAF)
-- - Annual max spill distribution
--
-- Part of: 09_STATISTICS LAYER
-- Related: reservoir_entity, reservoir_variable
-- ============================================================================

\echo '============================================================================'
\echo 'CREATING RESERVOIR_PERIOD_SUMMARY TABLE'
\echo '============================================================================'

-- Drop existing objects if recreating
DROP TABLE IF EXISTS reservoir_period_summary CASCADE;

-- ============================================================================
-- TABLE: reservoir_period_summary
-- ============================================================================
-- Period-of-record summary (92 reservoirs × 8 scenarios = 736 rows)

CREATE TABLE reservoir_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    reservoir_entity_id INTEGER NOT NULL,      -- FK to reservoir_entity.id

    -- Simulation period
    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    -- ============================================================
    -- STORAGE EXCEEDANCE (% capacity exceeded X% of time)
    -- For full-period exceedance curves (not by month)
    -- ============================================================
    storage_exc_p5 NUMERIC(6,2),               -- Storage exceeded 95% of time
    storage_exc_p10 NUMERIC(6,2),              -- Storage exceeded 90% of time
    storage_exc_p25 NUMERIC(6,2),              -- Storage exceeded 75% of time
    storage_exc_p50 NUMERIC(6,2),              -- Storage exceeded 50% of time (median)
    storage_exc_p75 NUMERIC(6,2),              -- Storage exceeded 25% of time
    storage_exc_p90 NUMERIC(6,2),              -- Storage exceeded 10% of time
    storage_exc_p95 NUMERIC(6,2),              -- Storage exceeded 5% of time

    -- ============================================================
    -- THRESHOLD MARKERS (for horizontal lines on charts)
    -- ============================================================
    dead_pool_taf NUMERIC(10,2),               -- Dead pool volume (from reservoir_entity)
    dead_pool_pct NUMERIC(6,2),                -- Dead pool as % of capacity
    spill_threshold_pct NUMERIC(6,2),          -- Avg storage % when spill begins

    -- ============================================================
    -- SPILL FREQUENCY AND MAGNITUDE
    -- ============================================================
    -- Annual spill frequency
    spill_years_count INTEGER,                 -- Years with any spill
    spill_frequency_pct NUMERIC(5,2),          -- % of years with spill

    -- Spill magnitude summary (CFS)
    spill_mean_cfs NUMERIC(10,2),              -- Mean when spilling (all events)
    spill_peak_cfs NUMERIC(10,2),              -- Maximum ever observed

    -- Annual spill volume (TAF)
    annual_spill_avg_taf NUMERIC(10,2),        -- Mean annual volume
    annual_spill_cv NUMERIC(6,4),              -- CV of annual volume
    annual_spill_max_taf NUMERIC(10,2),        -- Max annual volume

    -- Annual max spill distribution (worst event each year)
    annual_max_spill_q50 NUMERIC(10,2),        -- Median of annual peaks
    annual_max_spill_q90 NUMERIC(10,2),        -- 90th percentile of annual peaks
    annual_max_spill_q100 NUMERIC(10,2),       -- Max (same as spill_peak_cfs)

    -- Metadata
    capacity_taf NUMERIC(10,2),

    -- Audit fields (ERD standard)
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT fk_period_summary_reservoir_entity
        FOREIGN KEY (reservoir_entity_id) REFERENCES reservoir_entity(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT uq_period_summary
        UNIQUE(scenario_short_code, reservoir_entity_id)
);

\echo 'Created table: reservoir_period_summary'

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX idx_period_summary_scenario
    ON reservoir_period_summary(scenario_short_code);

CREATE INDEX idx_period_summary_entity
    ON reservoir_period_summary(reservoir_entity_id);

CREATE INDEX idx_period_summary_spill_freq
    ON reservoir_period_summary(spill_frequency_pct DESC);

CREATE INDEX idx_period_summary_active
    ON reservoir_period_summary(is_active) WHERE is_active = TRUE;

\echo 'Created indexes'

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE reservoir_period_summary IS
    'Period-of-record summary statistics for each reservoir. Comprehensive spill metrics. Part of 09_STATISTICS layer.';

COMMENT ON COLUMN reservoir_period_summary.scenario_short_code IS
    'Scenario identifier (e.g., s0020)';

COMMENT ON COLUMN reservoir_period_summary.reservoir_entity_id IS
    'FK to reservoir_entity.id. Storage from S_{short_code}, spill from C_{short_code}_FLOOD.';

COMMENT ON COLUMN reservoir_period_summary.simulation_start_year IS
    'First water year in the simulation period';

COMMENT ON COLUMN reservoir_period_summary.simulation_end_year IS
    'Last water year in the simulation period';

COMMENT ON COLUMN reservoir_period_summary.total_years IS
    'Number of water years in the simulation';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p5 IS
    'Storage (% capacity) exceeded 95% of the time - 5th percentile of full period';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p10 IS
    'Storage (% capacity) exceeded 90% of the time - 10th percentile of full period';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p25 IS
    'Storage (% capacity) exceeded 75% of the time - 25th percentile of full period';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p50 IS
    'Storage (% capacity) exceeded 50% of the time - median of full period';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p75 IS
    'Storage (% capacity) exceeded 25% of the time - 75th percentile of full period';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p90 IS
    'Storage (% capacity) exceeded 10% of the time - 90th percentile of full period';

COMMENT ON COLUMN reservoir_period_summary.storage_exc_p95 IS
    'Storage (% capacity) exceeded 5% of the time - 95th percentile of full period';

COMMENT ON COLUMN reservoir_period_summary.dead_pool_taf IS
    'Dead pool volume in TAF (denormalized from reservoir_entity)';

COMMENT ON COLUMN reservoir_period_summary.dead_pool_pct IS
    'Dead pool as percent of capacity - horizontal threshold for charts';

COMMENT ON COLUMN reservoir_period_summary.spill_threshold_pct IS
    'Average storage (% capacity) when spill begins - horizontal threshold for charts';

COMMENT ON COLUMN reservoir_period_summary.spill_years_count IS
    'Number of water years with at least one spill event';

COMMENT ON COLUMN reservoir_period_summary.spill_frequency_pct IS
    'Percentage of simulation years with at least one spill event';

COMMENT ON COLUMN reservoir_period_summary.spill_mean_cfs IS
    'Mean spill magnitude (CFS) across all spill events when spilling';

COMMENT ON COLUMN reservoir_period_summary.spill_peak_cfs IS
    'Maximum spill ever observed (CFS)';

COMMENT ON COLUMN reservoir_period_summary.annual_spill_avg_taf IS
    'Mean annual spill volume (TAF) - total volume spilled each year, averaged';

COMMENT ON COLUMN reservoir_period_summary.annual_spill_cv IS
    'Coefficient of variation of annual spill volume';

COMMENT ON COLUMN reservoir_period_summary.annual_spill_max_taf IS
    'Maximum annual spill volume (TAF) in the simulation';

COMMENT ON COLUMN reservoir_period_summary.annual_max_spill_q50 IS
    'Median of annual peak spill values (CFS) - typical worst spill each year';

COMMENT ON COLUMN reservoir_period_summary.annual_max_spill_q90 IS
    '90th percentile of annual peak spill values (CFS) - useful for risk assessment';

COMMENT ON COLUMN reservoir_period_summary.annual_max_spill_q100 IS
    'Maximum annual peak spill (CFS) - same as spill_peak_cfs';

COMMENT ON COLUMN reservoir_period_summary.capacity_taf IS
    'Reservoir physical capacity in TAF (denormalized from reservoir_entity)';

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
     WHERE table_name = 'reservoir_period_summary') as column_count
FROM information_schema.tables
WHERE table_name = 'reservoir_period_summary';

\echo ''
\echo 'Columns:'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'reservoir_period_summary'
ORDER BY ordinal_position;

\echo ''
\echo '============================================================================'
\echo 'RESERVOIR_PERIOD_SUMMARY TABLE CREATED SUCCESSFULLY'
\echo '============================================================================'
\echo ''
