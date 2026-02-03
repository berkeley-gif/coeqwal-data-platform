-- CREATE DU (URBAN DEMAND UNIT) STATISTICS TABLES
-- Monthly delivery and shortage statistics for 107 urban demand units
--
-- Tables created:
--   1. du_delivery_monthly - Monthly delivery statistics by demand unit
--   2. du_shortage_monthly - Monthly shortage statistics by demand unit
--   3. du_period_summary - Period-of-record summary with reliability metrics
--
-- Prerequisites:
--   1. Run 01_create_du_urban_entity.sql first
--   2. Load du_urban_entity data
--
-- Run with: psql -f 02_create_du_statistics_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING DU STATISTICS TABLES'
\echo '========================================='

-- ============================================
-- DROP EXISTING TABLES (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS du_period_summary CASCADE;
DROP TABLE IF EXISTS du_shortage_monthly CASCADE;
DROP TABLE IF EXISTS du_delivery_monthly CASCADE;

-- ============================================
-- 1. DU_DELIVERY_MONTHLY
-- ============================================
\echo ''
\echo 'Creating du_delivery_monthly table...'

CREATE TABLE du_delivery_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,                 -- FK → du_urban_entity.du_id (e.g., "02_NU")
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
    CONSTRAINT uq_du_delivery_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_du_delivery_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- ============================================
-- 2. DU_SHORTAGE_MONTHLY
-- ============================================
\echo ''
\echo 'Creating du_shortage_monthly table...'

CREATE TABLE du_shortage_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,
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
    CONSTRAINT uq_du_shortage_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_du_shortage_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- ============================================
-- 3. DU_PERIOD_SUMMARY
-- ============================================
\echo ''
\echo 'Creating du_period_summary table...'

CREATE TABLE du_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,

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
    annual_demand_avg_taf NUMERIC(10,2),        -- Back-calculated demand (DEM_* columns)

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_du_period_summary
        UNIQUE(scenario_short_code, du_id)
);

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

-- du_delivery_monthly indexes
CREATE INDEX idx_du_delivery_monthly_scenario ON du_delivery_monthly(scenario_short_code);
CREATE INDEX idx_du_delivery_monthly_du_id ON du_delivery_monthly(du_id);
CREATE INDEX idx_du_delivery_monthly_combined ON du_delivery_monthly(scenario_short_code, du_id);

-- du_shortage_monthly indexes
CREATE INDEX idx_du_shortage_monthly_scenario ON du_shortage_monthly(scenario_short_code);
CREATE INDEX idx_du_shortage_monthly_du_id ON du_shortage_monthly(du_id);
CREATE INDEX idx_du_shortage_monthly_combined ON du_shortage_monthly(scenario_short_code, du_id);

-- du_period_summary indexes
CREATE INDEX idx_du_period_summary_scenario ON du_period_summary(scenario_short_code);
CREATE INDEX idx_du_period_summary_du_id ON du_period_summary(du_id);

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE du_delivery_monthly IS 'Monthly delivery statistics for urban demand units. Source: UD_* columns in DEMANDS files.';
COMMENT ON TABLE du_shortage_monthly IS 'Monthly shortage statistics for urban demand units. Source: SHORT_* columns in DEMANDS files.';
COMMENT ON TABLE du_period_summary IS 'Period-of-record summary statistics for urban demand units including reliability metrics.';

COMMENT ON COLUMN du_delivery_monthly.water_month IS 'Water month: 1=October, 2=November, ..., 12=September';
COMMENT ON COLUMN du_delivery_monthly.exc_p5 IS 'Value exceeded 5% of time (high delivery conditions)';
COMMENT ON COLUMN du_delivery_monthly.exc_p95 IS 'Value exceeded 95% of time (low delivery conditions)';

COMMENT ON COLUMN du_period_summary.reliability_pct IS 'Percentage of months where delivery met or exceeded demand';
COMMENT ON COLUMN du_period_summary.avg_pct_demand_met IS 'Average ratio of delivery to demand across all months';

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo '✅ DU statistics tables created successfully'
\echo ''
\echo 'Tables created:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('du_delivery_monthly', 'du_shortage_monthly', 'du_period_summary')
ORDER BY table_name;

\echo ''
\echo 'Indexes created:'
SELECT indexname, tablename
FROM pg_indexes
WHERE tablename IN ('du_delivery_monthly', 'du_shortage_monthly', 'du_period_summary')
ORDER BY tablename, indexname;
