-- CWS AGGREGATE ENTITY AND STATISTICS TABLES
-- System-level M&I delivery and shortage aggregates
--
-- Prerequisites:
--   1. Database exists
--   2. developer table exists (for audit FKs)
--
-- Run with: psql -f 06_create_cws_aggregate_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING CWS AGGREGATE TABLES'
\echo '========================================='

-- ============================================
-- 1. CWS_AGGREGATE_ENTITY
-- ============================================
-- System-level aggregate definitions (SWP Total, SWP NOD, etc.)
\echo ''
\echo 'Creating cws_aggregate_entity...'

CREATE TABLE IF NOT EXISTS cws_aggregate_entity (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR(50) UNIQUE NOT NULL,     -- swp_total, swp_nod, swp_sod, cvp_nod, cvp_sod, mwd
    label VARCHAR(100) NOT NULL,                 -- Display name
    description TEXT,

    -- Project/Region classification
    project VARCHAR(10) NOT NULL,                -- SWP, CVP, MWD
    region VARCHAR(20),                          -- total, nod, sod, null

    -- CalSim variable mappings
    delivery_variable VARCHAR(50),               -- DEL_SWP_PMI, DEL_CVP_PMI_N, etc.
    shortage_variable VARCHAR(50),               -- SHORT_SWP_PMI, etc.

    -- Ordering
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

COMMENT ON TABLE cws_aggregate_entity IS 'System-level M&I aggregate definitions for SWP/CVP deliveries';
COMMENT ON COLUMN cws_aggregate_entity.project IS 'Water project: SWP, CVP, or MWD';
COMMENT ON COLUMN cws_aggregate_entity.region IS 'Region: total, nod (North of Delta), sod (South of Delta)';
COMMENT ON COLUMN cws_aggregate_entity.delivery_variable IS 'CalSim delivery variable name (DEL_*)';
COMMENT ON COLUMN cws_aggregate_entity.shortage_variable IS 'CalSim shortage variable name (SHORT_*)';

\echo '  Created cws_aggregate_entity'

-- ============================================
-- 2. CWS_AGGREGATE_MONTHLY
-- ============================================
-- Monthly delivery and shortage statistics for system-level aggregates
\echo ''
\echo 'Creating cws_aggregate_monthly...'

CREATE TABLE IF NOT EXISTS cws_aggregate_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    cws_aggregate_id INTEGER NOT NULL,
    water_month INTEGER NOT NULL,               -- 0 = annual, 1-12 = water month (Oct=1)

    -- Delivery statistics (TAF)
    delivery_avg_taf NUMERIC(12,2),
    delivery_cv NUMERIC(6,4),

    -- Delivery percentiles (TAF) - for box plots
    delivery_q0 NUMERIC(12,2),
    delivery_q10 NUMERIC(12,2),
    delivery_q30 NUMERIC(12,2),
    delivery_q50 NUMERIC(12,2),
    delivery_q70 NUMERIC(12,2),
    delivery_q90 NUMERIC(12,2),
    delivery_q100 NUMERIC(12,2),

    -- Delivery exceedance percentiles (TAF)
    delivery_exc_p5 NUMERIC(12,2),
    delivery_exc_p10 NUMERIC(12,2),
    delivery_exc_p25 NUMERIC(12,2),
    delivery_exc_p50 NUMERIC(12,2),
    delivery_exc_p75 NUMERIC(12,2),
    delivery_exc_p90 NUMERIC(12,2),
    delivery_exc_p95 NUMERIC(12,2),

    -- Shortage statistics (TAF)
    shortage_avg_taf NUMERIC(12,2),
    shortage_cv NUMERIC(6,4),
    shortage_frequency_pct NUMERIC(5,2),        -- % of months/years with shortage > 0

    -- Shortage percentiles (TAF)
    shortage_q0 NUMERIC(12,2),
    shortage_q10 NUMERIC(12,2),
    shortage_q30 NUMERIC(12,2),
    shortage_q50 NUMERIC(12,2),
    shortage_q70 NUMERIC(12,2),
    shortage_q90 NUMERIC(12,2),
    shortage_q100 NUMERIC(12,2),

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT fk_cws_aggregate_monthly_entity
        FOREIGN KEY (cws_aggregate_id) REFERENCES cws_aggregate_entity(id),
    CONSTRAINT uq_cws_aggregate_monthly
        UNIQUE(scenario_short_code, cws_aggregate_id, water_month),
    CONSTRAINT chk_cws_aggregate_water_month
        CHECK (water_month BETWEEN 0 AND 12)
);

COMMENT ON TABLE cws_aggregate_monthly IS 'Monthly delivery and shortage statistics for system-level CWS aggregates';
COMMENT ON COLUMN cws_aggregate_monthly.water_month IS '0 = annual total, 1-12 = water month (Oct=1, Sep=12)';

\echo '  Created cws_aggregate_monthly'

-- ============================================
-- 3. CWS_AGGREGATE_PERIOD_SUMMARY
-- ============================================
-- Period-of-record summary for system-level aggregates
\echo ''
\echo 'Creating cws_aggregate_period_summary...'

CREATE TABLE IF NOT EXISTS cws_aggregate_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    cws_aggregate_id INTEGER NOT NULL,

    -- Simulation period
    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    -- Annual delivery statistics
    annual_delivery_avg_taf NUMERIC(12,2),
    annual_delivery_cv NUMERIC(6,4),
    annual_delivery_min_taf NUMERIC(12,2),
    annual_delivery_max_taf NUMERIC(12,2),

    -- Annual delivery exceedance percentiles
    delivery_exc_p5 NUMERIC(12,2),
    delivery_exc_p10 NUMERIC(12,2),
    delivery_exc_p25 NUMERIC(12,2),
    delivery_exc_p50 NUMERIC(12,2),
    delivery_exc_p75 NUMERIC(12,2),
    delivery_exc_p90 NUMERIC(12,2),
    delivery_exc_p95 NUMERIC(12,2),

    -- Annual shortage statistics
    annual_shortage_avg_taf NUMERIC(12,2),
    shortage_years_count INTEGER,
    shortage_frequency_pct NUMERIC(5,2),

    -- Annual shortage exceedance percentiles
    shortage_exc_p5 NUMERIC(12,2),
    shortage_exc_p10 NUMERIC(12,2),
    shortage_exc_p25 NUMERIC(12,2),
    shortage_exc_p50 NUMERIC(12,2),
    shortage_exc_p75 NUMERIC(12,2),
    shortage_exc_p90 NUMERIC(12,2),
    shortage_exc_p95 NUMERIC(12,2),

    -- Reliability metrics
    reliability_pct NUMERIC(5,2),               -- % of time meeting full allocation
    avg_pct_allocation_met NUMERIC(5,2),        -- avg delivery/demand across period

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT fk_cws_aggregate_period_entity
        FOREIGN KEY (cws_aggregate_id) REFERENCES cws_aggregate_entity(id),
    CONSTRAINT uq_cws_aggregate_period
        UNIQUE(scenario_short_code, cws_aggregate_id)
);

COMMENT ON TABLE cws_aggregate_period_summary IS 'Period-of-record summary for system-level CWS aggregates';
COMMENT ON COLUMN cws_aggregate_period_summary.reliability_pct IS 'Percent of time meeting full allocation';

\echo '  Created cws_aggregate_period_summary'

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

-- cws_aggregate_entity indexes
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_entity_short_code
    ON cws_aggregate_entity(short_code);
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_entity_project
    ON cws_aggregate_entity(project);

-- cws_aggregate_monthly indexes
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_monthly_scenario
    ON cws_aggregate_monthly(scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_monthly_entity
    ON cws_aggregate_monthly(cws_aggregate_id);
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_monthly_combined
    ON cws_aggregate_monthly(scenario_short_code, cws_aggregate_id);

-- cws_aggregate_period_summary indexes
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_period_scenario
    ON cws_aggregate_period_summary(scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_cws_aggregate_period_entity
    ON cws_aggregate_period_summary(cws_aggregate_id);

\echo '  Created indexes'

-- ============================================
-- SEED DATA
-- ============================================
\echo ''
\echo 'Inserting seed data for cws_aggregate_entity...'

INSERT INTO cws_aggregate_entity (id, short_code, label, description, project, region, delivery_variable, shortage_variable, display_order)
VALUES
    (1, 'swp_total', 'SWP Total M&I', 'Total State Water Project Municipal & Industrial deliveries', 'SWP', 'total', 'DEL_SWP_PMI', 'SHORT_SWP_PMI', 1),
    (2, 'cvp_nod', 'CVP North', 'Central Valley Project M&I deliveries - North of Delta', 'CVP', 'nod', 'DEL_CVP_PMI_N', 'SHORT_CVP_PMI_N', 3),
    (3, 'cvp_sod', 'CVP South', 'Central Valley Project M&I deliveries - South of Delta', 'CVP', 'sod', 'DEL_CVP_PMI_S', 'SHORT_CVP_PMI_S', 4),
    (4, 'mwd', 'Metropolitan Water District', 'MWD Southern California aggregate deliveries', 'MWD', NULL, 'DEL_SWP_MWD', 'SHORT_SWP_MWD', 6),
    (5, 'swp_nod', 'SWP North', 'State Water Project M&I deliveries - North of Delta', 'SWP', 'nod', 'DEL_SWP_PMI_N', 'SHORT_SWP_PMI_N', 2),
    (6, 'swp_sod', 'SWP South', 'State Water Project M&I deliveries - South of Delta', 'SWP', 'sod', 'DEL_SWP_PMI_S', 'SHORT_SWP_PMI_S', 5)
ON CONFLICT (short_code) DO UPDATE SET
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    project = EXCLUDED.project,
    region = EXCLUDED.region,
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    display_order = EXCLUDED.display_order,
    updated_at = NOW();

\echo '  Inserted/updated 6 aggregate entity records (SWP Total, SWP NOD, SWP SOD, CVP NOD, CVP SOD, MWD)'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'cws_aggregate_entity records:'
SELECT id, short_code, project, region, delivery_variable, shortage_variable
FROM cws_aggregate_entity
ORDER BY display_order;

\echo ''
\echo 'Table counts:'
SELECT 'cws_aggregate_entity' as table_name, COUNT(*) as records FROM cws_aggregate_entity
UNION ALL
SELECT 'cws_aggregate_monthly', COUNT(*) FROM cws_aggregate_monthly
UNION ALL
SELECT 'cws_aggregate_period_summary', COUNT(*) FROM cws_aggregate_period_summary;

\echo ''
\echo 'CWS AGGREGATE TABLES CREATED SUCCESSFULLY'
