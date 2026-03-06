-- ============================================================
-- Migration 29: Create Delta statistics tables
-- ============================================================
-- Tables for Delta outflow, X2 position, and salinity metrics.
--
-- delta_monthly: one row per (scenario, variable, water_month)
-- delta_period_summary: one row per (scenario, variable)
--   Uses JSONB for summary data because different variable
--   categories (outflow, x2, salinity) have different metrics.
-- ============================================================

\echo 'Creating delta statistics tables...'

-- ============================================
-- delta_monthly
-- ============================================

CREATE TABLE IF NOT EXISTS delta_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    variable_code VARCHAR(30) NOT NULL,
    water_month INTEGER NOT NULL CHECK (water_month BETWEEN 1 AND 12),

    -- Primary statistic (TAF for outflow, KM for X2, UMHOS/CM for salinity)
    avg NUMERIC(12,3),
    cv NUMERIC(10,4),
    unit VARCHAR(20),
    sample_count INTEGER,

    -- For CFS-native variables, also store average in CFS
    avg_cfs NUMERIC(12,2),

    -- Percentiles
    q0 NUMERIC(12,3),
    q10 NUMERIC(12,3),
    q30 NUMERIC(12,3),
    q50 NUMERIC(12,3),
    q70 NUMERIC(12,3),
    q90 NUMERIC(12,3),
    q100 NUMERIC(12,3),

    -- Exceedance percentiles
    exc_p5 NUMERIC(12,3),
    exc_p10 NUMERIC(12,3),
    exc_p25 NUMERIC(12,3),
    exc_p50 NUMERIC(12,3),
    exc_p75 NUMERIC(12,3),
    exc_p90 NUMERIC(12,3),
    exc_p95 NUMERIC(12,3),

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_delta_monthly UNIQUE (scenario_short_code, variable_code, water_month)
);

COMMENT ON TABLE delta_monthly IS 'Monthly statistics for Delta variables (outflow, X2, salinity)';

CREATE INDEX IF NOT EXISTS idx_delta_monthly_scenario ON delta_monthly(scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_delta_monthly_variable ON delta_monthly(variable_code);


-- ============================================
-- delta_period_summary
-- ============================================

CREATE TABLE IF NOT EXISTS delta_period_summary (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    variable_code VARCHAR(30) NOT NULL,

    -- Metadata
    label VARCHAR(200),
    category VARCHAR(50),
    native_unit VARCHAR(20),

    -- Simulation period
    simulation_start_year INTEGER,
    simulation_end_year INTEGER,
    total_years INTEGER,

    -- Summary data (JSONB — schema varies by category)
    --   outflow: annual_avg_taf, annual_cv, avg_cfs, sept_avg_taf, sept_cv, exc_p*
    --   x2:      avg_km, cv, april_avg_km, april_cv, sept_avg_km, sept_cv,
    --            spring_avg_km, spring_cv, fall_avg_km, fall_cv, exc_p*, april_exc_p*, sept_exc_p*
    --   salinity: avg_ec, cv, spring_avg_ec, spring_cv, fall_avg_ec, fall_cv,
    --             exceed_d1641_pct, exceed_high_pct, exceed_mid_pct, exceed_low_pct, exc_p*
    summary_data JSONB NOT NULL DEFAULT '{}',

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_delta_period_summary UNIQUE (scenario_short_code, variable_code)
);

COMMENT ON TABLE delta_period_summary IS 'Period-of-record summary for Delta variables. summary_data JSONB varies by category.';

CREATE INDEX IF NOT EXISTS idx_delta_summary_scenario ON delta_period_summary(scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_delta_summary_variable ON delta_period_summary(variable_code);
CREATE INDEX IF NOT EXISTS idx_delta_summary_category ON delta_period_summary(category);


-- ============================================
-- Register in domain/family map
-- ============================================

INSERT INTO domain_family_map (domain, family, table_name, description)
VALUES
    ('delta', 'statistics', 'delta_monthly', 'Monthly Delta statistics (outflow, X2, salinity)'),
    ('delta', 'statistics', 'delta_period_summary', 'Period summary for Delta variables')
ON CONFLICT DO NOTHING;


-- ============================================
-- Grants
-- ============================================

GRANT SELECT ON delta_monthly TO coeqwal_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON delta_monthly TO coeqwal_writer;
GRANT USAGE, SELECT ON SEQUENCE delta_monthly_id_seq TO coeqwal_writer;

GRANT SELECT ON delta_period_summary TO coeqwal_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON delta_period_summary TO coeqwal_writer;
GRANT USAGE, SELECT ON SEQUENCE delta_period_summary_id_seq TO coeqwal_writer;


-- ============================================
-- Verification
-- ============================================

\echo ''
\echo '✅ Delta statistics tables created'
\echo ''

SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_name IN ('delta_monthly', 'delta_period_summary')
ORDER BY table_name;
