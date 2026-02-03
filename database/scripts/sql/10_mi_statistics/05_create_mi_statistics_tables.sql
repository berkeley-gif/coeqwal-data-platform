-- CREATE MI_CONTRACTOR STATISTICS TABLES
-- Monthly delivery and shortage statistics for SWP/CVP contractors
--
-- Tables created:
--   1. mi_delivery_monthly - Monthly delivery statistics by contractor
--   2. mi_shortage_monthly - Monthly shortage statistics by contractor
--   3. mi_contractor_period_summary - Period-of-record summary with reliability metrics
--
-- Prerequisites:
--   1. Run 03_create_mi_contractor_entity_tables.sql first
--   2. Load mi_contractor data
--
-- Run with: psql -f 05_create_mi_statistics_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING MI_CONTRACTOR STATISTICS TABLES'
\echo '========================================='

-- ============================================
-- DROP EXISTING TABLES (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS mi_contractor_period_summary CASCADE;
DROP TABLE IF EXISTS mi_shortage_monthly CASCADE;
DROP TABLE IF EXISTS mi_delivery_monthly CASCADE;

-- ============================================
-- 1. MI_DELIVERY_MONTHLY
-- ============================================
\echo ''
\echo 'Creating mi_delivery_monthly table...'

CREATE TABLE mi_delivery_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    mi_contractor_code VARCHAR(50) NOT NULL,    -- References mi_contractor.short_code
    water_month INTEGER NOT NULL,               -- 1-12 (Oct=1, Sep=12)

    -- Delivery statistics (TAF)
    delivery_avg_taf NUMERIC(10,2),
    delivery_cv NUMERIC(6,4),                   -- Coefficient of variation

    -- Delivery percentiles (TAF) - for box plots
    q0 NUMERIC(10,2),                           -- Minimum
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),                          -- Median
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),                         -- Maximum

    -- Exceedance percentiles (TAF) - for exceedance plots
    exc_p5 NUMERIC(10,2),                       -- Value exceeded 5% of time
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),                      -- Median
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),                      -- Value exceeded 95% of time

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_mi_delivery_monthly
        UNIQUE(scenario_short_code, mi_contractor_code, water_month),
    CONSTRAINT chk_mi_delivery_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- ============================================
-- 2. MI_SHORTAGE_MONTHLY
-- ============================================
\echo ''
\echo 'Creating mi_shortage_monthly table...'

CREATE TABLE mi_shortage_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    mi_contractor_code VARCHAR(50) NOT NULL,    -- References mi_contractor.short_code
    water_month INTEGER NOT NULL,

    -- Shortage statistics (TAF)
    shortage_avg_taf NUMERIC(10,2),
    shortage_cv NUMERIC(6,4),
    shortage_frequency_pct NUMERIC(5,2),        -- % months with shortage > 0

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
    CONSTRAINT uq_mi_shortage_monthly
        UNIQUE(scenario_short_code, mi_contractor_code, water_month),
    CONSTRAINT chk_mi_shortage_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- ============================================
-- 3. MI_CONTRACTOR_PERIOD_SUMMARY
-- ============================================
\echo ''
\echo 'Creating mi_contractor_period_summary table...'

CREATE TABLE mi_contractor_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    mi_contractor_code VARCHAR(50) NOT NULL,    -- References mi_contractor.short_code

    -- Simulation period
    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    -- Annual delivery statistics
    annual_delivery_avg_taf NUMERIC(10,2),
    annual_delivery_cv NUMERIC(6,4),

    -- Annual delivery exceedance percentiles (for exceedance plots)
    delivery_exc_p5 NUMERIC(10,2),
    delivery_exc_p10 NUMERIC(10,2),
    delivery_exc_p25 NUMERIC(10,2),
    delivery_exc_p50 NUMERIC(10,2),
    delivery_exc_p75 NUMERIC(10,2),
    delivery_exc_p90 NUMERIC(10,2),
    delivery_exc_p95 NUMERIC(10,2),

    -- Annual shortage statistics
    annual_shortage_avg_taf NUMERIC(10,2),
    shortage_years_count INTEGER,
    shortage_frequency_pct NUMERIC(5,2),        -- % of years with any shortage

    -- Annual shortage exceedance percentiles
    shortage_exc_p5 NUMERIC(10,2),
    shortage_exc_p10 NUMERIC(10,2),
    shortage_exc_p25 NUMERIC(10,2),
    shortage_exc_p50 NUMERIC(10,2),
    shortage_exc_p75 NUMERIC(10,2),
    shortage_exc_p90 NUMERIC(10,2),
    shortage_exc_p95 NUMERIC(10,2),

    -- Reliability metrics
    reliability_pct NUMERIC(5,2),               -- % of months meeting full demand
    avg_pct_demand_met NUMERIC(5,2),            -- Average delivery/demand ratio

    -- Demand reference
    contract_amount_taf NUMERIC(10,2),          -- Table A (SWP) or contract amount
    annual_demand_avg_taf NUMERIC(10,2),        -- Back-calculated demand

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_mi_period_summary
        UNIQUE(scenario_short_code, mi_contractor_code)
);

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

-- mi_delivery_monthly indexes
CREATE INDEX idx_mi_delivery_monthly_scenario ON mi_delivery_monthly(scenario_short_code);
CREATE INDEX idx_mi_delivery_monthly_contractor ON mi_delivery_monthly(mi_contractor_code);
CREATE INDEX idx_mi_delivery_monthly_combined ON mi_delivery_monthly(scenario_short_code, mi_contractor_code);

-- mi_shortage_monthly indexes
CREATE INDEX idx_mi_shortage_monthly_scenario ON mi_shortage_monthly(scenario_short_code);
CREATE INDEX idx_mi_shortage_monthly_contractor ON mi_shortage_monthly(mi_contractor_code);
CREATE INDEX idx_mi_shortage_monthly_combined ON mi_shortage_monthly(scenario_short_code, mi_contractor_code);

-- mi_contractor_period_summary indexes
CREATE INDEX idx_mi_period_summary_scenario ON mi_contractor_period_summary(scenario_short_code);
CREATE INDEX idx_mi_period_summary_contractor ON mi_contractor_period_summary(mi_contractor_code);

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE mi_delivery_monthly IS 'Monthly delivery statistics for SWP/CVP contractors. Source: D_*_PMI variables in CalSim output.';
COMMENT ON TABLE mi_shortage_monthly IS 'Monthly shortage statistics for SWP/CVP contractors. Source: SHORT_* variables in CalSim output.';
COMMENT ON TABLE mi_contractor_period_summary IS 'Period-of-record summary statistics for SWP/CVP contractors including reliability metrics.';

COMMENT ON COLUMN mi_delivery_monthly.mi_contractor_code IS 'Contractor short code, references mi_contractor.short_code';
COMMENT ON COLUMN mi_delivery_monthly.water_month IS 'Water month: 1=October, 2=November, ..., 12=September';
COMMENT ON COLUMN mi_delivery_monthly.exc_p5 IS 'Value exceeded 5% of time (high delivery conditions)';
COMMENT ON COLUMN mi_delivery_monthly.exc_p95 IS 'Value exceeded 95% of time (low delivery conditions)';

COMMENT ON COLUMN mi_contractor_period_summary.reliability_pct IS 'Percentage of months where delivery met or exceeded demand';
COMMENT ON COLUMN mi_contractor_period_summary.avg_pct_demand_met IS 'Average ratio of delivery to demand across all months';
COMMENT ON COLUMN mi_contractor_period_summary.contract_amount_taf IS 'Table A contract amount (SWP) or contract allocation (CVP) in TAF';

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo '✅ MI_CONTRACTOR statistics tables created successfully'
\echo ''
\echo 'Tables created:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('mi_delivery_monthly', 'mi_shortage_monthly', 'mi_contractor_period_summary')
ORDER BY table_name;

\echo ''
\echo 'Indexes created:'
SELECT indexname, tablename
FROM pg_indexes
WHERE tablename IN ('mi_delivery_monthly', 'mi_shortage_monthly', 'mi_contractor_period_summary')
ORDER BY tablename, indexname;
