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
-- Source: AW_{DU_ID} columns in CalSim output
-- ============================================
\echo ''
\echo 'Creating ag_du_delivery_monthly table...'

CREATE TABLE ag_du_delivery_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,
    water_month INTEGER NOT NULL,

    delivery_avg_taf NUMERIC(10,2),
    delivery_cv NUMERIC(10,4),

    q0 NUMERIC(10,2),
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),

    exc_p5 NUMERIC(10,2),
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),

    sample_count INTEGER,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_ag_du_delivery_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_ag_du_delivery_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

COMMENT ON TABLE ag_du_delivery_monthly IS 'Monthly delivery statistics for agricultural demand units. Source: AW_* variables in CalSim output.';
COMMENT ON COLUMN ag_du_delivery_monthly.du_id IS 'Agricultural demand unit ID, references du_agriculture_entity.du_id';
COMMENT ON COLUMN ag_du_delivery_monthly.water_month IS 'Water month: 1=October, 2=November, ..., 12=September';
COMMENT ON COLUMN ag_du_delivery_monthly.delivery_avg_taf IS 'Average monthly delivery in thousand acre-feet';

-- ============================================
-- 2. AG_DU_SHORTAGE_MONTHLY
-- Note: Sacramento region DUs have no shortage data, and not all scenarios include GW_SHORT.
-- ============================================
\echo ''
\echo 'Creating ag_du_shortage_monthly table...'

CREATE TABLE ag_du_shortage_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,
    water_month INTEGER NOT NULL,

    shortage_avg_taf NUMERIC(10,2),
    shortage_cv NUMERIC(10,4),
    shortage_frequency_pct NUMERIC(7,2),
    shortage_pct_of_demand_avg NUMERIC(6,2),

    q0 NUMERIC(10,2),
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),

    sample_count INTEGER,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_ag_du_shortage_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_ag_du_shortage_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

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

    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    annual_demand_avg_taf NUMERIC(10,2),
    annual_demand_cv NUMERIC(10,4),

    demand_exc_p5 NUMERIC(10,2),
    demand_exc_p10 NUMERIC(10,2),
    demand_exc_p25 NUMERIC(10,2),
    demand_exc_p50 NUMERIC(10,2),
    demand_exc_p75 NUMERIC(10,2),
    demand_exc_p90 NUMERIC(10,2),
    demand_exc_p95 NUMERIC(10,2),

    annual_shortage_avg_taf NUMERIC(10,2),
    shortage_years_count INTEGER,
    shortage_frequency_pct NUMERIC(5,2),
    annual_shortage_pct_of_demand NUMERIC(6,2),

    reliability_pct NUMERIC(5,2),
    avg_pct_demand_met NUMERIC(5,2),

    annual_sw_delivery_avg_taf NUMERIC(10,2),
    annual_sw_delivery_cv NUMERIC(10,4),
    annual_gw_pumping_avg_taf NUMERIC(10,2),
    annual_gw_pumping_cv NUMERIC(10,4),
    gw_pumping_pct_of_demand NUMERIC(5,2),

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_ag_du_period_summary
        UNIQUE(scenario_short_code, du_id)
);

COMMENT ON TABLE ag_du_period_summary IS 'Period-of-record summary statistics for agricultural demand units.';
COMMENT ON COLUMN ag_du_period_summary.annual_shortage_pct_of_demand IS 'Average annual shortage as percentage of demand. NULL for Sacramento region DUs.';

-- ============================================
-- 4. AG_AGGREGATE_MONTHLY
-- Source: DEL_SWP_PAG, DEL_CVP_PAG_N, etc. in CalSim output
-- ============================================
\echo ''
\echo 'Creating ag_aggregate_monthly table...'

CREATE TABLE ag_aggregate_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    aggregate_code VARCHAR(50) NOT NULL,
    water_month INTEGER NOT NULL,

    delivery_avg_taf NUMERIC(10,2),
    delivery_cv NUMERIC(10,4),

    q0 NUMERIC(10,2),
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),

    exc_p5 NUMERIC(10,2),
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),

    shortage_avg_taf NUMERIC(10,2),
    shortage_cv NUMERIC(10,4),
    shortage_frequency_pct NUMERIC(5,2),

    sample_count INTEGER,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_ag_aggregate_monthly
        UNIQUE(scenario_short_code, aggregate_code, water_month),
    CONSTRAINT chk_ag_aggregate_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

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

    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    annual_delivery_avg_taf NUMERIC(10,2),
    annual_delivery_cv NUMERIC(10,4),

    delivery_exc_p5 NUMERIC(10,2),
    delivery_exc_p10 NUMERIC(10,2),
    delivery_exc_p25 NUMERIC(10,2),
    delivery_exc_p50 NUMERIC(10,2),
    delivery_exc_p75 NUMERIC(10,2),
    delivery_exc_p90 NUMERIC(10,2),
    delivery_exc_p95 NUMERIC(10,2),

    annual_shortage_avg_taf NUMERIC(10,2),
    shortage_years_count INTEGER,
    shortage_frequency_pct NUMERIC(5,2),

    shortage_exc_p5 NUMERIC(10,2),
    shortage_exc_p10 NUMERIC(10,2),
    shortage_exc_p25 NUMERIC(10,2),
    shortage_exc_p50 NUMERIC(10,2),
    shortage_exc_p75 NUMERIC(10,2),
    shortage_exc_p90 NUMERIC(10,2),
    shortage_exc_p95 NUMERIC(10,2),

    reliability_pct NUMERIC(5,2),

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_ag_aggregate_period_summary
        UNIQUE(scenario_short_code, aggregate_code)
);

COMMENT ON TABLE ag_aggregate_period_summary IS 'Period-of-record summary statistics for agricultural project aggregates. Shortage from SHORT_CVP_PAG_*, SHORT_SWP_PAG_* variables.';
COMMENT ON COLUMN ag_aggregate_period_summary.shortage_years_count IS 'Number of years with annual shortage > 0.1 TAF (threshold to filter floating-point noise)';
COMMENT ON COLUMN ag_aggregate_period_summary.reliability_pct IS 'Reliability = 1 - (avg shortage / avg delivery) * 100';

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

CREATE INDEX idx_ag_du_delivery_monthly_scenario ON ag_du_delivery_monthly(scenario_short_code);
CREATE INDEX idx_ag_du_delivery_monthly_du ON ag_du_delivery_monthly(du_id);
CREATE INDEX idx_ag_du_delivery_monthly_combined ON ag_du_delivery_monthly(scenario_short_code, du_id);

CREATE INDEX idx_ag_du_shortage_monthly_scenario ON ag_du_shortage_monthly(scenario_short_code);
CREATE INDEX idx_ag_du_shortage_monthly_du ON ag_du_shortage_monthly(du_id);
CREATE INDEX idx_ag_du_shortage_monthly_combined ON ag_du_shortage_monthly(scenario_short_code, du_id);

CREATE INDEX idx_ag_du_period_summary_scenario ON ag_du_period_summary(scenario_short_code);
CREATE INDEX idx_ag_du_period_summary_du ON ag_du_period_summary(du_id);

CREATE INDEX idx_ag_aggregate_monthly_scenario ON ag_aggregate_monthly(scenario_short_code);
CREATE INDEX idx_ag_aggregate_monthly_code ON ag_aggregate_monthly(aggregate_code);
CREATE INDEX idx_ag_aggregate_monthly_combined ON ag_aggregate_monthly(scenario_short_code, aggregate_code);

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
