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
    du_id VARCHAR(20) NOT NULL,
    water_month INTEGER NOT NULL,

    delivery_avg_taf NUMERIC(10,2),
    delivery_cv NUMERIC(6,4),

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

    demand_avg_taf NUMERIC(10,2),
    percent_of_demand_avg NUMERIC(7,2),

    sample_count INTEGER,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

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

    shortage_avg_taf NUMERIC(10,2),
    shortage_cv NUMERIC(6,4),
    shortage_frequency_pct NUMERIC(7,2),

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

    simulation_start_year INTEGER NOT NULL,
    simulation_end_year INTEGER NOT NULL,
    total_years INTEGER NOT NULL,

    annual_delivery_avg_taf NUMERIC(10,2),
    annual_delivery_cv NUMERIC(6,4),

    delivery_exc_p5 NUMERIC(10,2),
    delivery_exc_p10 NUMERIC(10,2),
    delivery_exc_p25 NUMERIC(10,2),
    delivery_exc_p50 NUMERIC(10,2),
    delivery_exc_p75 NUMERIC(10,2),
    delivery_exc_p90 NUMERIC(10,2),
    delivery_exc_p95 NUMERIC(10,2),

    annual_shortage_avg_taf NUMERIC(10,2),
    shortage_years_count INTEGER,
    shortage_frequency_pct NUMERIC(7,2),

    shortage_exc_p5 NUMERIC(10,2),
    shortage_exc_p10 NUMERIC(10,2),
    shortage_exc_p25 NUMERIC(10,2),
    shortage_exc_p50 NUMERIC(10,2),
    shortage_exc_p75 NUMERIC(10,2),
    shortage_exc_p90 NUMERIC(10,2),
    shortage_exc_p95 NUMERIC(10,2),

    reliability_pct NUMERIC(7,2),
    avg_pct_demand_met NUMERIC(7,2),

    annual_demand_avg_taf NUMERIC(10,2),

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT uq_du_period_summary
        UNIQUE(scenario_short_code, du_id)
);

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

CREATE INDEX idx_du_delivery_monthly_scenario ON du_delivery_monthly(scenario_short_code);
CREATE INDEX idx_du_delivery_monthly_du_id ON du_delivery_monthly(du_id);
CREATE INDEX idx_du_delivery_monthly_combined ON du_delivery_monthly(scenario_short_code, du_id);

CREATE INDEX idx_du_shortage_monthly_scenario ON du_shortage_monthly(scenario_short_code);
CREATE INDEX idx_du_shortage_monthly_du_id ON du_shortage_monthly(du_id);
CREATE INDEX idx_du_shortage_monthly_combined ON du_shortage_monthly(scenario_short_code, du_id);

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

COMMENT ON COLUMN du_period_summary.reliability_pct IS
    'Mean over years of (annual_delivery_taf / annual_recovered_demand_taf) * 100, clipped to [0, 100]. '
    'Denominator is the recovered annual demand, where monthly demand is computed in the ETL via the '
    'PERDV inversion (delivery + shortage) / perdv_swp_N (matches V3 DataExtraction.py demand formula, '
    'e.g. line 1242 for SBA029). NOT complementary to annual_shortage_avg_taf  - that metric uses the '
    'perdv-scaled in-month demand CalSim was solving against as its baseline, so the two metrics answer '
    'different questions and (1 - reliability_pct/100) * demand will not equal annual_shortage_avg_taf. '
    'See etl/statistics/du_urban/calculate_du_statistics_v2.py reliability_pct block for the calculation.';
COMMENT ON COLUMN du_period_summary.avg_pct_demand_met IS
    'Identical value to reliability_pct (kept as separate column for legacy / clarity). Same denominator (recovered annual demand).';
COMMENT ON COLUMN du_period_summary.annual_shortage_avg_taf IS
    '100-year mean of annual sum of CalSim SHORT_* (CFS converted to TAF via CFS_TO_TAF_PER_DAY * DaysInMonth, '
    'negatives clamped to 0 for LP-solver noise). Baseline is the perdv-scaled in-month demand CalSim was '
    'solving against  - NOT the recovered (PERDV-inverted) annual demand used by reliability_pct. See '
    'reliability_pct comment above for why the two are not complementary. CalSim emits SHORT_* directly; we do not redefine it.';

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
