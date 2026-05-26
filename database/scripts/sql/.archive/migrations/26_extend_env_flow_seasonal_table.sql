-- =============================================================================
-- 26_extend_env_flow_seasonal_table.sql
-- Extends env_flow_channel_seasonal with raw flow volume and % unimpaired columns.
--
-- Run from the repository root:
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/26_extend_env_flow_seasonal_table.sql
--
-- Background:
--   Migration 24 created env_flow_channel_seasonal to hold only % functional flow
--   (pct_ff_*) statistics for ~17 EFLOWS channels. The expanded spec requires:
--
--   1. Raw seasonal flow volume (CFS) — for CEFF seasonal pulse diagrams.
--      Covers all 60 channels.
--
--   2. Seasonal % unimpaired — fulfils the "Monthly, seasonally" requirement
--      for Metric 1. Covers 58 channels with a unimp_sv_variable.
--
--   The existing pct_ff_* columns are unchanged. New columns are NULL for rows
--   where the relevant source variable is absent (e.g. pct_unimpaired is NULL
--   for Mokelumne reaches without a UNIMP reference).
--
--   After this migration, env_flow_channel_seasonal covers ALL 60 channels
--   (not just the 17 EFLOWS channels), one row per (reach × scenario × season).
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================================'
\echo 'MIGRATION 26 — extend env_flow_channel_seasonal'
\echo '============================================================'


-- ─── 1. Add raw flow columns ──────────────────────────────────────────────────
-- Seasonal averages and distributions of C_{reach} in CFS.
-- Covers all 60 channels; enables CEFF seasonal pulse diagrams.

\echo ''
\echo '1. Adding raw flow volume columns...'

ALTER TABLE env_flow_channel_seasonal
    ADD COLUMN IF NOT EXISTS flow_avg_cfs      NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_cv           NUMERIC(8, 4),

    -- Percentile distribution of per-year seasonal mean flow across all years
    -- (q0 = driest year seasonal mean, q100 = wettest year seasonal mean)
    ADD COLUMN IF NOT EXISTS flow_q0           NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q10          NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q30          NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q50          NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q70          NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q90          NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_q100         NUMERIC(12, 3),

    -- Exceedance percentiles of per-year seasonal mean flow
    ADD COLUMN IF NOT EXISTS flow_exc_p5       NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p10      NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p25      NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p50      NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p75      NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p90      NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS flow_exc_p95      NUMERIC(12, 3);

\echo '  ✅ Raw flow columns added'


-- ─── 2. Add natural flow reference column ────────────────────────────────────
-- Seasonal average of UNIMP_{watershed} for comparison with simulated flow.
-- NULL for reaches without a unimp_sv_variable (Mokelumne, some canals).

\echo ''
\echo '2. Adding unimpaired flow reference column...'

ALTER TABLE env_flow_channel_seasonal
    ADD COLUMN IF NOT EXISTS unimp_avg_cfs     NUMERIC(12, 3);

\echo '  ✅ Unimpaired reference column added'


-- ─── 3. Add % unimpaired seasonal columns ────────────────────────────────────
-- Seasonal aggregate of pct_unimpaired = C_{reach} / UNIMP × 100.
-- Fulfils the "Monthly, seasonally" requirement for Metric 1 (% unimpaired).
-- Covers 58 of 60 channels (NULL for MOK019, MOK028).

\echo ''
\echo '3. Adding % unimpaired seasonal columns...'

ALTER TABLE env_flow_channel_seasonal
    ADD COLUMN IF NOT EXISTS pct_unimpaired_avg  NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS pct_unimpaired_cv   NUMERIC(8, 4),

    -- Percentile distribution of per-year seasonal mean pct_unimpaired
    ADD COLUMN IF NOT EXISTS unimp_q0            NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_q10           NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_q30           NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_q50           NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_q70           NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_q90           NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_q100          NUMERIC(8, 3),

    ADD COLUMN IF NOT EXISTS unimp_exc_p5        NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_exc_p10       NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_exc_p25       NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_exc_p50       NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_exc_p75       NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_exc_p90       NUMERIC(8, 3),
    ADD COLUMN IF NOT EXISTS unimp_exc_p95       NUMERIC(8, 3);

\echo '  ✅ % unimpaired seasonal columns added'


-- ─── 4. Update column comments ───────────────────────────────────────────────

COMMENT ON TABLE env_flow_channel_seasonal IS
    'Metric 1 (seasonal % unimpaired) + Metric 2 (seasonal % functional flows) '
    'for CalSim channel reaches, aggregated by CEFF 5-season calendar. '
    'One row per (reach × scenario × CEFF season). '
    'flow_* columns: raw CFS distributions for seasonal pulse diagrams — all 60 channels. '
    'pct_unimpaired_* columns: Metric 1 seasonal — 58 channels (NULL where no UNIMP ref). '
    'pct_ff_* columns: Metric 2 — ~17 channels with has_eflows = true (NULL otherwise). '
    'Populated by ETL at etl/statistics/env_flows/calculate_env_flow_statistics.py. '
    'Extended by migration 26 from migration 24 base.';

COMMENT ON COLUMN env_flow_channel_seasonal.flow_avg_cfs IS
    'Mean of per-year seasonal mean flows (CFS) across all simulated years. '
    'Per-year mean = average of monthly C_{reach} values within the CEFF season for that year.';
COMMENT ON COLUMN env_flow_channel_seasonal.flow_q50 IS
    'Median year seasonal mean flow (CFS). Half of simulated years have higher seasonal flow.';
COMMENT ON COLUMN env_flow_channel_seasonal.unimp_avg_cfs IS
    'Mean of UNIMP_{watershed} seasonal averages across all years. '
    'Natural unimpaired flow reference for the same CEFF season. NULL where no UNIMP variable.';
COMMENT ON COLUMN env_flow_channel_seasonal.pct_unimpaired_avg IS
    'Mean of per-year seasonal pct_unimpaired values = (C_{reach} / UNIMP) × 100. '
    'Satisfies the "seasonally" dimension of Metric 1. NULL for Mokelumne and reaches without UNIMP.';

\echo '  ✅ Column comments updated'


-- ─── 5. Verification ─────────────────────────────────────────────────────────

\echo ''
\echo '===== VERIFICATION ====='

\echo ''
\echo 'New columns on env_flow_channel_seasonal:'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'env_flow_channel_seasonal'
  AND table_schema = 'public'
  AND column_name IN (
      'flow_avg_cfs', 'flow_cv', 'flow_q0', 'flow_q50', 'flow_q100',
      'unimp_avg_cfs',
      'pct_unimpaired_avg', 'pct_unimpaired_cv', 'unimp_q50'
  )
ORDER BY ordinal_position;

\echo ''
\echo 'Total columns on env_flow_channel_seasonal (expect ~50):'
SELECT COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_name = 'env_flow_channel_seasonal'
  AND table_schema = 'public';

\echo ''
\echo '=== Migration 26 complete ==='
