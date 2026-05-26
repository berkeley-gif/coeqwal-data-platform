-- =============================================================================
-- Migration 19: Create wildlife refuge delivery statistics tables
-- =============================================================================
-- Creates three statistics tables for environmental water deliveries to
-- wildlife refuges and wetland demand units:
--
--   refuge_du_delivery_monthly  — monthly percentile bands for SW delivery
--   refuge_du_shortage_monthly  — monthly bands for shortage (TAF and %)
--   refuge_du_period_summary    — period-of-record annual stats + reliability
--
-- These tables are populated by the ETL module at etl/statistics/refuge/.
-- They mirror the ag_du_* table structure. No version_family linkage is
-- needed — statistics tables are not version-tracked.
--
-- Data source: CalSim 3 SV input (AWO_{DU_ID} demand) and deliveries CSV
-- (DN_{DU_ID} delivery), both in TAF. Shortage is derived as demand - delivery.
-- No native CalSim shortage variable exists for refuge demand units.
--
-- Developer attribution: created_by is set explicitly to 2 (jfantauzza)
-- by the ETL — the DEFAULT 1 here is a fallback only.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/19_create_refuge_statistics_tables.sql
-- =============================================================================


-- ─── 1. refuge_du_delivery_monthly ───────────────────────────────────────────
-- Monthly surface water delivery statistics.
-- One row per (scenario, du_id, water_month).
-- 18 DUs × 12 months × ~22 active scenarios ≈ 4,752 rows at full population.

CREATE TABLE IF NOT EXISTS refuge_du_delivery_monthly (
    id                      SERIAL PRIMARY KEY,
    scenario_short_code     VARCHAR(20)      NOT NULL,
    du_id                   VARCHAR(20)      NOT NULL,  -- References du_refuge_entity.du_id
    water_month             INTEGER          NOT NULL,  -- 1=Oct, 12=Sep

    -- Monthly delivery statistics (TAF)
    delivery_avg_taf        NUMERIC(10, 2),
    delivery_cv             NUMERIC(10, 4),

    -- Percentile bands (across all simulated years for this water month)
    q0                      NUMERIC(10, 2),
    q10                     NUMERIC(10, 2),
    q30                     NUMERIC(10, 2),
    q50                     NUMERIC(10, 2),
    q70                     NUMERIC(10, 2),
    q90                     NUMERIC(10, 2),
    q100                    NUMERIC(10, 2),

    -- Exceedance percentiles
    exc_p5                  NUMERIC(10, 2),
    exc_p10                 NUMERIC(10, 2),
    exc_p25                 NUMERIC(10, 2),
    exc_p50                 NUMERIC(10, 2),
    exc_p75                 NUMERIC(10, 2),
    exc_p90                 NUMERIC(10, 2),
    exc_p95                 NUMERIC(10, 2),

    sample_count            INTEGER,

    -- Audit fields
    is_active               BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by              INTEGER          NOT NULL DEFAULT 1,  -- FK → developer.id
    updated_by              INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT uq_refuge_delivery_monthly
        UNIQUE (scenario_short_code, du_id, water_month),
    CONSTRAINT chk_refuge_delivery_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

CREATE INDEX IF NOT EXISTS idx_refuge_delivery_monthly_scenario
    ON refuge_du_delivery_monthly (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_refuge_delivery_monthly_du_id
    ON refuge_du_delivery_monthly (du_id);
CREATE INDEX IF NOT EXISTS idx_refuge_delivery_monthly_scenario_du
    ON refuge_du_delivery_monthly (scenario_short_code, du_id);


-- ─── 2. refuge_du_shortage_monthly ───────────────────────────────────────────
-- Monthly delivery shortage statistics (TAF and % of demand).
-- Shortage is derived: shortage_taf = max(demand_taf - delivery_taf, 0).
-- One row per (scenario, du_id, water_month).

CREATE TABLE IF NOT EXISTS refuge_du_shortage_monthly (
    id                          SERIAL PRIMARY KEY,
    scenario_short_code         VARCHAR(20)      NOT NULL,
    du_id                       VARCHAR(20)      NOT NULL,  -- References du_refuge_entity.du_id
    water_month                 INTEGER          NOT NULL,  -- 1=Oct, 12=Sep

    -- Monthly shortage in TAF
    shortage_avg_taf            NUMERIC(10, 2),
    shortage_cv                 NUMERIC(10, 4),

    -- Monthly shortage as % of demand
    shortage_pct_avg            NUMERIC(10, 4),
    shortage_pct_cv             NUMERIC(10, 4),

    -- Fraction of months in this water_month slot with shortage > 0.1 TAF threshold
    shortage_frequency_pct      NUMERIC(10, 4),

    -- Percentile bands of monthly shortage TAF
    q0                          NUMERIC(10, 2),
    q10                         NUMERIC(10, 2),
    q30                         NUMERIC(10, 2),
    q50                         NUMERIC(10, 2),
    q70                         NUMERIC(10, 2),
    q90                         NUMERIC(10, 2),
    q100                        NUMERIC(10, 2),

    -- Exceedance percentiles of monthly shortage TAF
    exc_p5                      NUMERIC(10, 2),
    exc_p10                     NUMERIC(10, 2),
    exc_p25                     NUMERIC(10, 2),
    exc_p50                     NUMERIC(10, 2),
    exc_p75                     NUMERIC(10, 2),
    exc_p90                     NUMERIC(10, 2),
    exc_p95                     NUMERIC(10, 2),

    sample_count                INTEGER,

    -- Audit fields
    is_active                   BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at                  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by                  INTEGER          NOT NULL DEFAULT 1,
    updated_by                  INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT uq_refuge_shortage_monthly
        UNIQUE (scenario_short_code, du_id, water_month),
    CONSTRAINT chk_refuge_shortage_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

CREATE INDEX IF NOT EXISTS idx_refuge_shortage_monthly_scenario
    ON refuge_du_shortage_monthly (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_refuge_shortage_monthly_du_id
    ON refuge_du_shortage_monthly (du_id);
CREATE INDEX IF NOT EXISTS idx_refuge_shortage_monthly_scenario_du
    ON refuge_du_shortage_monthly (scenario_short_code, du_id);


-- ─── 3. refuge_du_period_summary ─────────────────────────────────────────────
-- Period-of-record annual summary statistics.
-- One row per (scenario, du_id).
-- 18 DUs × ~22 active scenarios ≈ 396 rows at full population.

CREATE TABLE IF NOT EXISTS refuge_du_period_summary (
    id                              SERIAL PRIMARY KEY,
    scenario_short_code             VARCHAR(20)      NOT NULL,
    du_id                           VARCHAR(20)      NOT NULL,  -- References du_refuge_entity.du_id

    -- Simulation period metadata
    simulation_start_year           INTEGER,
    simulation_end_year             INTEGER,
    total_years                     INTEGER,

    -- Annual delivery statistics
    annual_delivery_avg_taf         NUMERIC(10, 2),  -- Mean of annual delivery totals
    annual_delivery_cv              NUMERIC(10, 4),  -- CV of annual delivery

    -- Annual delivery exceedance curve (percentile of annual delivery distribution)
    delivery_exc_p5                 NUMERIC(10, 2),
    delivery_exc_p10                NUMERIC(10, 2),
    delivery_exc_p25                NUMERIC(10, 2),
    delivery_exc_p50                NUMERIC(10, 2),
    delivery_exc_p75                NUMERIC(10, 2),
    delivery_exc_p90                NUMERIC(10, 2),
    delivery_exc_p95                NUMERIC(10, 2),

    -- Annual shortage statistics (TAF)
    annual_shortage_avg_taf         NUMERIC(10, 2),
    annual_shortage_cv              NUMERIC(10, 4),

    -- Annual shortage statistics (% of demand)
    annual_shortage_pct_avg         NUMERIC(10, 4),  -- Mean annual shortage %
    annual_shortage_pct_cv          NUMERIC(10, 4),  -- CV of annual shortage %

    -- Reliability: 95th percentile of annual shortage %
    -- "In 95 of 100 years, annual shortage is at or below this value"
    -- 0% = perfectly reliable in 95% of years
    reliability_pct_95              NUMERIC(10, 4),

    -- Audit fields
    is_active                       BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at                      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by                      INTEGER          NOT NULL DEFAULT 1,
    updated_by                      INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT uq_refuge_period_summary
        UNIQUE (scenario_short_code, du_id)
);

CREATE INDEX IF NOT EXISTS idx_refuge_period_summary_scenario
    ON refuge_du_period_summary (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_refuge_period_summary_du_id
    ON refuge_du_period_summary (du_id);
CREATE INDEX IF NOT EXISTS idx_refuge_period_summary_scenario_du
    ON refuge_du_period_summary (scenario_short_code, du_id);


-- ─── 4. Audit trigger attachment ─────────────────────────────────────────────
-- Attach the standard audit trigger to each table.
-- The ETL sets created_by/updated_by explicitly; the trigger fires on updates
-- to non-ETL changes.

CREATE TRIGGER trg_refuge_delivery_monthly_audit
    BEFORE INSERT OR UPDATE ON refuge_du_delivery_monthly
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER trg_refuge_shortage_monthly_audit
    BEFORE INSERT OR UPDATE ON refuge_du_shortage_monthly
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER trg_refuge_period_summary_audit
    BEFORE INSERT OR UPDATE ON refuge_du_period_summary
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();


-- ─── 5. Table comments ───────────────────────────────────────────────────────

COMMENT ON TABLE refuge_du_delivery_monthly IS
    'Monthly surface water delivery statistics for 18 wildlife refuge demand units. '
    'Source: CalSim 3 DN_{DU_ID} from deliveries CSV (TAF block). '
    'Populated by ETL at etl/statistics/refuge/.';

COMMENT ON TABLE refuge_du_shortage_monthly IS
    'Monthly delivery shortage statistics for 18 wildlife refuge demand units. '
    'Shortage is derived: max(AWO_{DU_ID} - DN_{DU_ID}, 0). '
    'No native CalSim shortage variable exists for refuge DUs. '
    'Source: SV input (AWO_*, TAF) and deliveries CSV (DN_*, TAF). '
    'Populated by ETL at etl/statistics/refuge/.';

COMMENT ON TABLE refuge_du_period_summary IS
    'Period-of-record annual delivery and shortage summary for 18 wildlife refuge demand units. '
    'reliability_pct_95 = 95th percentile of annual shortage %: '
    'in 95 of 100 simulated years, annual shortage is at or below this value. '
    'Populated by ETL at etl/statistics/refuge/.';


-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'refuge_du_delivery_monthly',
      'refuge_du_shortage_monthly',
      'refuge_du_period_summary'
  )
ORDER BY table_name;

SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN (
      'refuge_du_delivery_monthly',
      'refuge_du_shortage_monthly',
      'refuge_du_period_summary'
  )
ORDER BY tablename, indexname;
