-- =============================================================================
-- Migration 24: Environmental flow statistics tables
-- =============================================================================
-- Creates four tables for environmental river flow metrics:
--
--   env_flow_season               - lookup: 5 CEFF seasonal groupings
--   env_flow_channel_monthly      - Metric 1: % unimpaired flow, monthly aggregates
--   env_flow_channel_seasonal     - Metric 2: % functional flows, by CEFF season
--   env_flow_channel_period_summary  - Metric 3: flow alteration index + period aggregates
--
-- All statistics tables:
--   - Reference reaches via network_arc_id VARCHAR (stable identifier from channel_entity)
--   - Reference scenarios via scenario_short_code VARCHAR (consistent with refuge/ag/cws pattern)
--   - Are populated by ETL at etl/statistics/env_flows/calculate_env_flow_statistics.py
--   - Cover 60 channels from the CalSim DV output (see channel_entity.has_mif,
--     channel_entity.unimp_sv_variable for per-channel capability flags)
--
-- Row count estimates (60 channels × 22 active scenarios):
--   env_flow_channel_monthly       ~15,840  (60 × 22 × 12 water months)
--   env_flow_channel_seasonal       ~1,870  (17 channels with EFLOWS × 22 × 5 seasons)
--   env_flow_channel_period_summary ~1,320  (60 × 22)
--
-- Data sources:
--   DV: {scenario}_coeqwal_calsim_output.csv  - C_{reach} (CHANNEL), C_{reach}_MIF (FLOW-MIN-INSTREAM)
--   SV: {scenario}_coeqwal_sv_input.csv        - UNIMP_{watershed} (FLOW-UNIMPAIRED),
--                                               EFLOWS_{reach} (FLOW-MIN-EFLOW)
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/24_create_env_flow_statistics_tables.sql
-- =============================================================================


\echo 'Creating env_flow_season...'

CREATE TABLE IF NOT EXISTS env_flow_season (
    id              SERIAL PRIMARY KEY,
    short_code      VARCHAR(30)  NOT NULL,
    name            VARCHAR(80)  NOT NULL,
    description     TEXT,
    wy_months       INTEGER[]    NOT NULL,
    calendar_months TEXT         NOT NULL,
    sort_order      SMALLINT     NOT NULL,

    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_by      INTEGER      NOT NULL DEFAULT 1,
    updated_by      INTEGER      NOT NULL DEFAULT 1,

    CONSTRAINT env_flow_season_short_code_key UNIQUE (short_code)
);

DROP TRIGGER IF EXISTS trg_env_flow_season_audit ON env_flow_season;
CREATE TRIGGER trg_env_flow_season_audit
    BEFORE INSERT OR UPDATE ON env_flow_season
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE env_flow_season IS
    '5-season CEFF (California Environmental Flows Framework) seasonal calendar used '
    'for environmental flow target analysis. Water year: Oct=1 … Sep=12. '
    'Used by env_flow_channel_seasonal to group monthly functional flow calculations.';

COMMENT ON COLUMN env_flow_season.wy_months IS
    'Array of water year month numbers (1=Oct…12=Sep) belonging to this season. '
    'The dry season spans the water year boundary: {10, 11, 12, 1} = Jul -Oct.';

INSERT INTO env_flow_season
    (short_code, name, description, wy_months, calendar_months, sort_order)
VALUES
    ('wet_peak',
     'Wet Season Peak',
     'High-flow wet season pulse  - December through February. '
     'Creates spawning habitat and flushes fine sediments from gravel beds.',
     ARRAY[3, 4, 5],
     'December, January, February',
     1),

    ('wet_base',
     'Wet Season Base',
     'Sustained wet-season baseflow  - March through April. '
     'Maintains inundated floodplain habitat following peak flows.',
     ARRAY[6, 7],
     'March, April',
     2),

    ('spring_recession',
     'Spring Recession',
     'Gradual snowmelt-driven recession  - May through June. '
     'Supports juvenile salmon and steelhead outmigration.',
     ARRAY[8, 9],
     'May, June',
     3),

    ('dry',
     'Dry Season',
     'Summer and early-fall low flows  - July through October. '
     'Critical thermal stress period for cold-water fish species.',
     ARRAY[10, 11, 12, 1],
     'July, August, September, October',
     4),

    ('fall_pulse',
     'Fall Pulse',
     'First-flush storm event  - November. '
     'Triggers adult salmon migration and opens sand bars at river mouths.',
     ARRAY[2],
     'November',
     5)
ON CONFLICT (short_code) DO NOTHING;

\echo '  env_flow_season: 5 rows seeded'


\echo 'Creating env_flow_channel_monthly...'

CREATE TABLE IF NOT EXISTS env_flow_channel_monthly (
    id                      SERIAL PRIMARY KEY,
    network_arc_id          VARCHAR(30)      NOT NULL,
    scenario_short_code     VARCHAR(20)      NOT NULL,
    water_month             SMALLINT         NOT NULL,

    flow_avg_cfs            NUMERIC(12, 3),
    flow_cv                 NUMERIC(8, 4),

    unimp_avg_cfs           NUMERIC(12, 3),

    pct_unimpaired_avg      NUMERIC(8, 3),
    pct_unimpaired_cv       NUMERIC(8, 4),

    q0                      NUMERIC(8, 3),
    q10                     NUMERIC(8, 3),
    q30                     NUMERIC(8, 3),
    q50                     NUMERIC(8, 3),
    q70                     NUMERIC(8, 3),
    q90                     NUMERIC(8, 3),
    q100                    NUMERIC(8, 3),

    exc_p5                  NUMERIC(8, 3),
    exc_p10                 NUMERIC(8, 3),
    exc_p25                 NUMERIC(8, 3),
    exc_p50                 NUMERIC(8, 3),
    exc_p75                 NUMERIC(8, 3),
    exc_p90                 NUMERIC(8, 3),
    exc_p95                 NUMERIC(8, 3),

    sample_count            SMALLINT,

    is_active               BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by              INTEGER          NOT NULL DEFAULT 1,
    updated_by              INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT uq_env_flow_monthly
        UNIQUE (network_arc_id, scenario_short_code, water_month),
    CONSTRAINT chk_env_flow_monthly_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

CREATE INDEX IF NOT EXISTS idx_env_flow_monthly_arc
    ON env_flow_channel_monthly (network_arc_id);
CREATE INDEX IF NOT EXISTS idx_env_flow_monthly_scenario
    ON env_flow_channel_monthly (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_env_flow_monthly_arc_scenario
    ON env_flow_channel_monthly (network_arc_id, scenario_short_code);

DROP TRIGGER IF EXISTS trg_env_flow_monthly_audit ON env_flow_channel_monthly;
CREATE TRIGGER trg_env_flow_monthly_audit
    BEFORE INSERT OR UPDATE ON env_flow_channel_monthly
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE env_flow_channel_monthly IS
    'Metric 1  - Monthly % of natural unimpaired flow for 60 CalSim channel reaches. '
    'One row per (reach, scenario, water_month); aggregated across all simulated water years. '
    'pct_unimpaired = C_{reach} / UNIMP_{watershed} × 100. NULL where no UNIMP reference exists. '
    'Source: DV (C_*) and SV (UNIMP_*). Populated by ETL at etl/statistics/env_flows/.';


\echo 'Creating env_flow_channel_seasonal...'

CREATE TABLE IF NOT EXISTS env_flow_channel_seasonal (
    id                      SERIAL PRIMARY KEY,
    network_arc_id          VARCHAR(30)      NOT NULL,
    scenario_short_code     VARCHAR(20)      NOT NULL,
    season_id               INTEGER          NOT NULL REFERENCES env_flow_season(id),

    pct_ff_avg              NUMERIC(8, 3),
    pct_ff_cv               NUMERIC(8, 4),

    deviation_avg           NUMERIC(8, 3),

    q0                      NUMERIC(8, 3),
    q10                     NUMERIC(8, 3),
    q30                     NUMERIC(8, 3),
    q50                     NUMERIC(8, 3),
    q70                     NUMERIC(8, 3),
    q90                     NUMERIC(8, 3),
    q100                    NUMERIC(8, 3),

    exc_p5                  NUMERIC(8, 3),
    exc_p10                 NUMERIC(8, 3),
    exc_p25                 NUMERIC(8, 3),
    exc_p50                 NUMERIC(8, 3),
    exc_p75                 NUMERIC(8, 3),
    exc_p90                 NUMERIC(8, 3),
    exc_p95                 NUMERIC(8, 3),

    target_met_pct          NUMERIC(6, 2),

    sample_count            SMALLINT,

    is_active               BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by              INTEGER          NOT NULL DEFAULT 1,
    updated_by              INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT uq_env_flow_seasonal
        UNIQUE (network_arc_id, scenario_short_code, season_id)
);

CREATE INDEX IF NOT EXISTS idx_env_flow_seasonal_arc
    ON env_flow_channel_seasonal (network_arc_id);
CREATE INDEX IF NOT EXISTS idx_env_flow_seasonal_scenario
    ON env_flow_channel_seasonal (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_env_flow_seasonal_season
    ON env_flow_channel_seasonal (season_id);
CREATE INDEX IF NOT EXISTS idx_env_flow_seasonal_arc_scenario
    ON env_flow_channel_seasonal (network_arc_id, scenario_short_code);

DROP TRIGGER IF EXISTS trg_env_flow_seasonal_audit ON env_flow_channel_seasonal;
CREATE TRIGGER trg_env_flow_seasonal_audit
    BEFORE INSERT OR UPDATE ON env_flow_channel_seasonal
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE env_flow_channel_seasonal IS
    'Metric 2  - Seasonal % of functional flow (EFLOWS) targets for reaches with prescribed FF targets. '
    'One row per (reach, scenario, CEFF season); aggregated across all simulated water years. '
    'pct_ff = C_{reach} / EFLOWS_{reach} × 100, grouped by the 5-season CEFF calendar. '
    'Only populated for reaches with has_eflows = true (~17 confirmed). '
    'target_met_pct = fraction of years where seasonal avg pct_ff >= 100%. '
    'Source: DV (C_*) and SV (EFLOWS_*). Populated by ETL at etl/statistics/env_flows/.';


\echo 'Creating env_flow_channel_period_summary...'

CREATE TABLE IF NOT EXISTS env_flow_channel_period_summary (
    id                          SERIAL PRIMARY KEY,
    network_arc_id              VARCHAR(30)      NOT NULL,
    scenario_short_code         VARCHAR(20)      NOT NULL,

    simulation_start_year       SMALLINT,
    simulation_end_year         SMALLINT,
    total_months                SMALLINT,

    pearson_r                   NUMERIC(6, 4),
    p_value                     NUMERIC(8, 6),

    avg_pct_unimpaired          NUMERIC(8, 3),
    annual_cv_pct_unimpaired    NUMERIC(8, 4),

    avg_pct_ff                  NUMERIC(8, 3),
    annual_cv_pct_ff            NUMERIC(8, 4),

    mif_met_pct                 NUMERIC(6, 2),

    is_active                   BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at                  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by                  INTEGER          NOT NULL DEFAULT 1,
    updated_by                  INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT uq_env_flow_period_summary
        UNIQUE (network_arc_id, scenario_short_code)
);

CREATE INDEX IF NOT EXISTS idx_env_flow_period_summary_arc
    ON env_flow_channel_period_summary (network_arc_id);
CREATE INDEX IF NOT EXISTS idx_env_flow_period_summary_scenario
    ON env_flow_channel_period_summary (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_env_flow_period_summary_arc_scenario
    ON env_flow_channel_period_summary (network_arc_id, scenario_short_code);

DROP TRIGGER IF EXISTS trg_env_flow_period_summary_audit ON env_flow_channel_period_summary;
CREATE TRIGGER trg_env_flow_period_summary_audit
    BEFORE INSERT OR UPDATE ON env_flow_channel_period_summary
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE env_flow_channel_period_summary IS
    'Metric 3  - Flow alteration index and full-period aggregate statistics for 60 CalSim channels. '
    'One row per (reach, scenario). '
    'pearson_r = Pearson correlation between monthly C_{reach} and UNIMP_{watershed} series '
    'over full 1,200-month period of record. r≈+1: natural timing preserved; r≈0: altered. '
    'mif_met_pct = fraction of months where simulated flow >= binding MIF (has_mif reaches only). '
    'Populated by ETL at etl/statistics/env_flows/.';


INSERT INTO domain_family_map (schema_name, table_name, version_family_id, is_active, created_by, updated_by)
SELECT 'public', 'env_flow_season',                 vf.id, true, 1, 1 FROM version_family vf WHERE vf.short_code = 'statistics'
ON CONFLICT (schema_name, table_name) DO NOTHING;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, is_active, created_by, updated_by)
SELECT 'public', 'env_flow_channel_monthly',        vf.id, true, 1, 1 FROM version_family vf WHERE vf.short_code = 'statistics'
ON CONFLICT (schema_name, table_name) DO NOTHING;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, is_active, created_by, updated_by)
SELECT 'public', 'env_flow_channel_seasonal',       vf.id, true, 1, 1 FROM version_family vf WHERE vf.short_code = 'statistics'
ON CONFLICT (schema_name, table_name) DO NOTHING;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, is_active, created_by, updated_by)
SELECT 'public', 'env_flow_channel_period_summary', vf.id, true, 1, 1 FROM version_family vf WHERE vf.short_code = 'statistics'
ON CONFLICT (schema_name, table_name) DO NOTHING;


SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'env_flow_season',
      'env_flow_channel_monthly',
      'env_flow_channel_seasonal',
      'env_flow_channel_period_summary'
  )
ORDER BY table_name;

SELECT short_code, name, calendar_months, wy_months
FROM env_flow_season
ORDER BY sort_order;
