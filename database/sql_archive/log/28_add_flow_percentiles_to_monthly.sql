-- =============================================================================
-- Migration 28: Add flow-volume percentile columns to env_flow_channel_monthly
-- =============================================================================
-- The original table stored only pct_unimpaired percentile bands (q0…q100).
-- This migration adds the raw flow-volume distribution in both CFS and TAF so
-- the frontend can display percentile band charts on a volume y-axis (matching
-- the reservoir, CWS, and AG sections), with a CFS / TAF toggle.
--
-- New column families:
--   flow_avg_taf           - mean monthly volume (TAF/month) across all years
--   flow_q{p}_cfs          - percentile p of per-year monthly flow (CFS)
--   flow_exc_p{p}_cfs      - exceedance percentile p (CFS); exc_p5 = value exceeded 5% of time
--   flow_q{p}_taf          - same as CFS columns but converted to TAF/month
--   flow_exc_p{p}_taf      - same, TAF
--
-- TAF conversion:  TAF/month = CFS × days_in_month × 86400 / 43,560,000
--                            = CFS × days_in_month × 0.0019835
-- The ETL applies the actual days_in_month for each simulated year before
-- computing percentiles, so leap-year Februaries are handled correctly.
--
-- Run as:
--   psql $SUPERUSER_URL \
--     -f database/scripts/sql/migrations/28_add_flow_percentiles_to_monthly.sql
-- =============================================================================

\echo 'Migration 28: adding flow-volume percentile columns to env_flow_channel_monthly...'

ALTER TABLE env_flow_channel_monthly
    ADD COLUMN IF NOT EXISTS flow_avg_taf       NUMERIC(12, 3);

ALTER TABLE env_flow_channel_monthly
    ADD COLUMN IF NOT EXISTS flow_q0_cfs        NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q10_cfs       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q30_cfs       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q50_cfs       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q70_cfs       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q90_cfs       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q100_cfs      NUMERIC(12, 3);

ALTER TABLE env_flow_channel_monthly
    ADD COLUMN IF NOT EXISTS flow_exc_p5_cfs    NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p10_cfs   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p25_cfs   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p50_cfs   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p75_cfs   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p90_cfs   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p95_cfs   NUMERIC(12, 3);

ALTER TABLE env_flow_channel_monthly
    ADD COLUMN IF NOT EXISTS flow_q0_taf        NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q10_taf       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q30_taf       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q50_taf       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q70_taf       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q90_taf       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q100_taf      NUMERIC(12, 3);

ALTER TABLE env_flow_channel_monthly
    ADD COLUMN IF NOT EXISTS flow_exc_p5_taf    NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p10_taf   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p25_taf   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p50_taf   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p75_taf   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p90_taf   NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p95_taf   NUMERIC(12, 3);

COMMENT ON COLUMN env_flow_channel_monthly.flow_avg_taf IS
    'Mean monthly flow volume in thousand acre-feet (TAF/month). '
    'Conversion: CFS × actual_days_in_month × 86400 / 43560000. '
    'Averaged across all simulated years for this water_month.';

COMMENT ON COLUMN env_flow_channel_monthly.flow_q0_cfs IS
    'Minimum (0th percentile) of per-year monthly mean flow in CFS.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_q50_cfs IS
    'Median (50th percentile) of per-year monthly mean flow in CFS.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_q100_cfs IS
    'Maximum (100th percentile) of per-year monthly mean flow in CFS.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_exc_p5_cfs IS
    'Value exceeded 5 % of simulated years (wet conditions)  - CFS.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_exc_p95_cfs IS
    'Value exceeded 95 % of simulated years (dry conditions)  - CFS.';

COMMENT ON COLUMN env_flow_channel_monthly.flow_q0_taf IS
    'Minimum (0th percentile) of per-year monthly flow volume in TAF/month.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_q50_taf IS
    'Median (50th percentile) of per-year monthly flow volume in TAF/month.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_q100_taf IS
    'Maximum (100th percentile) of per-year monthly flow volume in TAF/month.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_exc_p5_taf IS
    'Value exceeded 5 % of simulated years (wet conditions)  - TAF/month.';
COMMENT ON COLUMN env_flow_channel_monthly.flow_exc_p95_taf IS
    'Value exceeded 95 % of simulated years (dry conditions)  - TAF/month.';

COMMENT ON TABLE env_flow_channel_monthly IS
    'Metric 1  - Monthly river flow statistics for CalSim channel reaches. '
    'One row per (reach, scenario, water_month); aggregated across all simulated water years. '
    'Stores two parallel statistic families: '
    '  (1) raw flow volume bands (CFS and TAF) for percentile band charts; '
    '  (2) pct_unimpaired = C_{reach} / UNIMP_{watershed} × 100 for ecological metrics. '
    'pct_unimpaired is NULL where no UNIMP reference variable exists (e.g. Mokelumne). '
    'Source: DV (C_*) and SV (UNIMP_*). Populated by ETL at etl/statistics/env_flows/. '
    'Migration 28 added flow_q*_cfs, flow_q*_taf, flow_exc_p*_cfs, flow_exc_p*_taf columns.';

\echo 'Migration 28 complete.'
\echo '  Next step: re-run the ETL to populate the new columns.'
\echo '  python etl/statistics/env_flows/calculate_env_flow_statistics.py --all-scenarios'
