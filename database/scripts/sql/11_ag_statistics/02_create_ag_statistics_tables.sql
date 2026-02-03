-- CREATE AGRICULTURAL STATISTICS TABLES
-- Monthly and period statistics for agricultural demand units and aggregates
--
-- Tables created:
--   1. ag_du_delivery_monthly - Monthly delivery statistics by demand unit
--   2. ag_du_shortage_monthly - Monthly shortage statistics by demand unit (SJR/Tulare only)
--   3. ag_du_period_summary - Period-of-record summary by demand unit
--   4. ag_aggregate_monthly - Monthly delivery statistics by aggregate
--   5. ag_aggregate_period_summary - Period-of-record summary by aggregate
--
-- Prerequisites:
--   1. Run 01_create_ag_entity_tables.sql first
--   2. Load du_agriculture_entity data from CSV
--
-- Run with: psql -f 02_create_ag_statistics_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING AGRICULTURAL STATISTICS TABLES'
\echo '========================================='

-- ============================================
-- DROP EXISTING TABLES (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS ag_aggregate_period_summary CASCADE;
DROP TABLE IF EXISTS ag_aggregate_monthly CASCADE;
DROP TABLE IF EXISTS ag_du_period_summary CASCADE;
DROP TABLE IF EXISTS ag_du_shortage_monthly CASCADE;
DROP TABLE IF EXISTS ag_du_delivery_monthly CASCADE;

-- ============================================
-- 1. AG_DU_DELIVERY_MONTHLY
-- Monthly delivery statistics for agricultural demand units
-- Source: AW_{DU_ID} columns in CalSim output
-- ============================================
\echo ''
\echo 'Creating ag_du_delivery_monthly table...'

CREATE TABLE ag_du_delivery_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,               -- References du_agriculture_entity.du_id
    water_month INTEGER NOT NULL,             -- 1-12 (Oct=1, Sep=12)

    -- Delivery statistics (TAF)
    delivery_avg_taf NUMERIC(10,2),
    delivery_cv NUMERIC(10,4),                -- Coefficient of variation (can exceed 100)

    -- Delivery percentiles (TAF) - for box plots
    q0 NUMERIC(10,2),                         -- Minimum
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),                        -- Median
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),                       -- Maximum

    -- Exceedance percentiles (TAF) - for exceedance plots
    exc_p5 NUMERIC(10,2),                     -- Value exceeded 5% of time
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),                    -- Median
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),                    -- Value exceeded 95% of time

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_du_delivery_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_ag_du_delivery_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- Comments
COMMENT ON TABLE ag_du_delivery_monthly IS 'Monthly delivery statistics for agricultural demand units. Source: AW_* variables in CalSim output.';
COMMENT ON COLUMN ag_du_delivery_monthly.du_id IS 'Agricultural demand unit ID, references du_agriculture_entity.du_id';
COMMENT ON COLUMN ag_du_delivery_monthly.water_month IS 'Water month: 1=October, 2=November, ..., 12=September';
COMMENT ON COLUMN ag_du_delivery_monthly.delivery_avg_taf IS 'Average monthly delivery in thousand acre-feet';

-- ============================================
-- 2. AG_DU_SHORTAGE_MONTHLY
-- Monthly groundwater RESTRICTION shortage statistics (SJR/Tulare regions only)
-- Source: GW_SHORT_{DU_ID} columns in CalSim output
-- IMPORTANT: These are GROUNDWATER RESTRICTION shortages, not total delivery shortages.
--            For aggregate delivery shortages, see ag_aggregate_period_summary which uses
--            SHORT_CVP_PAG_* and SHORT_SWP_PAG_* variables.
-- Note: Sacramento region DUs have no shortage data, and not all scenarios include GW_SHORT.
-- ============================================
\echo ''
\echo 'Creating ag_du_shortage_monthly table...'

CREATE TABLE ag_du_shortage_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,
    water_month INTEGER NOT NULL,

    -- Shortage statistics (TAF)
    shortage_avg_taf NUMERIC(10,2),
    shortage_cv NUMERIC(10,4),
    shortage_frequency_pct NUMERIC(5,2),      -- % months with shortage > 0
    shortage_pct_of_demand_avg NUMERIC(6,2),  -- Average shortage as % of demand

    -- Shortage percentiles (TAF)
    q0 NUMERIC(10,2),
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_du_shortage_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_ag_du_shortage_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- Comments
COMMENT ON TABLE ag_du_shortage_monthly IS 'Monthly groundwater RESTRICTION shortage statistics for agricultural demand units. This is NOT total delivery shortage - it represents shortage due to groundwater pumping restrictions. Only SJR/Tulare regions have data. Source: GW_SHORT_* variables (COEQWAL-added for testing gw restrictions).';
COMMENT ON COLUMN ag_du_shortage_monthly.shortage_pct_of_demand_avg IS 'Average shortage as percentage of total demand: shortage / (delivery + shortage) * 100';

-- ============================================
-- 3. AG_DU_PERIOD_SUMMARY
-- Period-of-record summary for agricultural demand units
-- ============================================
\echo ''
\echo 'Creating ag_du_period_summary table...'

CREATE TABLE ag_du_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,

    -- Simulation period
    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    -- Annual delivery statistics
    annual_delivery_avg_taf NUMERIC(10,2),
    annual_delivery_cv NUMERIC(10,4),

    -- Annual delivery exceedance percentiles
    delivery_exc_p5 NUMERIC(10,2),
    delivery_exc_p10 NUMERIC(10,2),
    delivery_exc_p25 NUMERIC(10,2),
    delivery_exc_p50 NUMERIC(10,2),
    delivery_exc_p75 NUMERIC(10,2),
    delivery_exc_p90 NUMERIC(10,2),
    delivery_exc_p95 NUMERIC(10,2),

    -- Annual shortage statistics (NULL for Sacramento region)
    annual_shortage_avg_taf NUMERIC(10,2),
    shortage_years_count INTEGER,
    shortage_frequency_pct NUMERIC(5,2),
    annual_shortage_pct_of_demand NUMERIC(6,2),  -- Average annual shortage as % of demand

    -- Reliability metrics
    reliability_pct NUMERIC(5,2),             -- % of months meeting full demand
    avg_pct_demand_met NUMERIC(5,2),          -- Average delivery/demand ratio
    annual_demand_avg_taf NUMERIC(10,2),      -- Back-calculated annual demand

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_du_period_summary
        UNIQUE(scenario_short_code, du_id)
);

-- Comments
COMMENT ON TABLE ag_du_period_summary IS 'Period-of-record summary statistics for agricultural demand units.';
COMMENT ON COLUMN ag_du_period_summary.annual_shortage_pct_of_demand IS 'Average annual shortage as percentage of demand. NULL for Sacramento region DUs.';

-- ============================================
-- 4. AG_AGGREGATE_MONTHLY
-- Monthly delivery statistics for project aggregates
-- Source: DEL_SWP_PAG, DEL_CVP_PAG_N, etc. in CalSim output
-- ============================================
\echo ''
\echo 'Creating ag_aggregate_monthly table...'

CREATE TABLE ag_aggregate_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    aggregate_code VARCHAR(50) NOT NULL,      -- References ag_aggregate_entity.short_code
    water_month INTEGER NOT NULL,

    -- Delivery statistics (TAF)
    delivery_avg_taf NUMERIC(10,2),
    delivery_cv NUMERIC(10,4),

    -- Delivery percentiles (TAF)
    q0 NUMERIC(10,2),
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),

    -- Exceedance percentiles (TAF)
    exc_p5 NUMERIC(10,2),
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),

    -- Shortage statistics (from SHORT_CVP_PAG_*, SHORT_SWP_PAG_*)
    shortage_avg_taf NUMERIC(10,2),
    shortage_cv NUMERIC(10,4),                 -- CV can exceed 100 when mean is small
    shortage_frequency_pct NUMERIC(5,2),       -- % of months with shortage > 0.1 TAF

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_aggregate_monthly
        UNIQUE(scenario_short_code, aggregate_code, water_month),
    CONSTRAINT chk_ag_aggregate_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- Comments
COMMENT ON TABLE ag_aggregate_monthly IS 'Monthly delivery and shortage statistics for agricultural project aggregates. Source: DEL_*_PAG and SHORT_*_PAG variables in CalSim output.';

-- ============================================
-- 5. AG_AGGREGATE_PERIOD_SUMMARY
-- Period-of-record summary for project aggregates
-- ============================================
\echo ''
\echo 'Creating ag_aggregate_period_summary table...'

CREATE TABLE ag_aggregate_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    aggregate_code VARCHAR(50) NOT NULL,

    -- Simulation period
    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    -- Annual delivery statistics
    annual_delivery_avg_taf NUMERIC(10,2),
    annual_delivery_cv NUMERIC(10,4),

    -- Annual delivery exceedance percentiles
    delivery_exc_p5 NUMERIC(10,2),
    delivery_exc_p10 NUMERIC(10,2),
    delivery_exc_p25 NUMERIC(10,2),
    delivery_exc_p50 NUMERIC(10,2),
    delivery_exc_p75 NUMERIC(10,2),
    delivery_exc_p90 NUMERIC(10,2),
    delivery_exc_p95 NUMERIC(10,2),

    -- Annual shortage statistics (from SHORT_CVP_PAG_*, SHORT_SWP_PAG_*)
    annual_shortage_avg_taf NUMERIC(10,2),
    shortage_years_count INTEGER,               -- Years with shortage > 0.1 TAF
    shortage_frequency_pct NUMERIC(5,2),        -- % years with meaningful shortage

    -- Shortage exceedance percentiles
    shortage_exc_p5 NUMERIC(10,2),
    shortage_exc_p10 NUMERIC(10,2),
    shortage_exc_p25 NUMERIC(10,2),
    shortage_exc_p50 NUMERIC(10,2),
    shortage_exc_p75 NUMERIC(10,2),
    shortage_exc_p90 NUMERIC(10,2),
    shortage_exc_p95 NUMERIC(10,2),

    -- Reliability metric
    reliability_pct NUMERIC(5,2),               -- 1 - (avg shortage / avg delivery)

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_aggregate_period_summary
        UNIQUE(scenario_short_code, aggregate_code)
);

-- Comments
COMMENT ON TABLE ag_aggregate_period_summary IS 'Period-of-record summary statistics for agricultural project aggregates. Shortage from SHORT_CVP_PAG_*, SHORT_SWP_PAG_* variables.';
COMMENT ON COLUMN ag_aggregate_period_summary.shortage_years_count IS 'Number of years with annual shortage > 0.1 TAF (threshold to filter floating-point noise)';
COMMENT ON COLUMN ag_aggregate_period_summary.reliability_pct IS 'Reliability = 1 - (avg shortage / avg delivery) * 100';

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

-- ag_du_delivery_monthly indexes
CREATE INDEX idx_ag_du_delivery_monthly_scenario ON ag_du_delivery_monthly(scenario_short_code);
CREATE INDEX idx_ag_du_delivery_monthly_du ON ag_du_delivery_monthly(du_id);
CREATE INDEX idx_ag_du_delivery_monthly_combined ON ag_du_delivery_monthly(scenario_short_code, du_id);

-- ag_du_shortage_monthly indexes
CREATE INDEX idx_ag_du_shortage_monthly_scenario ON ag_du_shortage_monthly(scenario_short_code);
CREATE INDEX idx_ag_du_shortage_monthly_du ON ag_du_shortage_monthly(du_id);
CREATE INDEX idx_ag_du_shortage_monthly_combined ON ag_du_shortage_monthly(scenario_short_code, du_id);

-- ag_du_period_summary indexes
CREATE INDEX idx_ag_du_period_summary_scenario ON ag_du_period_summary(scenario_short_code);
CREATE INDEX idx_ag_du_period_summary_du ON ag_du_period_summary(du_id);

-- ag_aggregate_monthly indexes
CREATE INDEX idx_ag_aggregate_monthly_scenario ON ag_aggregate_monthly(scenario_short_code);
CREATE INDEX idx_ag_aggregate_monthly_code ON ag_aggregate_monthly(aggregate_code);
CREATE INDEX idx_ag_aggregate_monthly_combined ON ag_aggregate_monthly(scenario_short_code, aggregate_code);

-- ag_aggregate_period_summary indexes
CREATE INDEX idx_ag_aggregate_period_summary_scenario ON ag_aggregate_period_summary(scenario_short_code);
CREATE INDEX idx_ag_aggregate_period_summary_code ON ag_aggregate_period_summary(aggregate_code);

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo '✅ Agricultural statistics tables created successfully'
\echo ''
\echo 'Tables created:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name LIKE 'ag_%'
ORDER BY table_name;

\echo ''
\echo 'Indexes created:'
SELECT indexname, tablename
FROM pg_indexes
WHERE tablename LIKE 'ag_%'
ORDER BY tablename, indexname;
