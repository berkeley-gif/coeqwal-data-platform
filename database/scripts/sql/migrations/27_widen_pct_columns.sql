-- =============================================================================
-- 27_widen_pct_columns.sql
-- Widens pct_unimpaired and pct_ff columns from NUMERIC(8,3) to NUMERIC(12,3).
--
-- NUMERIC(8,3) allows values up to 99,999.999. Reservoir release channels
-- (e.g. C_SHSTA, C_OROVL) can exceed 100× their UNIMP reference in wet years,
-- producing pct_unimpaired values > 100,000%. NUMERIC(12,3) supports up to
-- 999,999,999.999, which is sufficient for all realistic CalSim outputs.
--
-- Run from the repository root:
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/27_widen_pct_columns.sql
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================================'
\echo 'MIGRATION 27 — widen pct_unimpaired / pct_ff columns'
\echo '============================================================'


-- ─── env_flow_channel_monthly ─────────────────────────────────────────────────

\echo ''
\echo '1. Widening env_flow_channel_monthly pct columns...'

ALTER TABLE env_flow_channel_monthly
    ALTER COLUMN pct_unimpaired_avg  TYPE NUMERIC(12, 3),
    ALTER COLUMN pct_unimpaired_cv   TYPE NUMERIC(12, 4),
    ALTER COLUMN q0                  TYPE NUMERIC(12, 3),
    ALTER COLUMN q10                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q30                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q50                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q70                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q90                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q100                TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p5              TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p10             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p25             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p50             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p75             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p90             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p95             TYPE NUMERIC(12, 3);

\echo '  ✅ env_flow_channel_monthly widened'


-- ─── env_flow_channel_seasonal ────────────────────────────────────────────────

\echo ''
\echo '2. Widening env_flow_channel_seasonal pct columns...'

ALTER TABLE env_flow_channel_seasonal
    ALTER COLUMN pct_unimpaired_avg  TYPE NUMERIC(12, 3),
    ALTER COLUMN pct_unimpaired_cv   TYPE NUMERIC(12, 4),
    ALTER COLUMN unimp_q0            TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_q10           TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_q30           TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_q50           TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_q70           TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_q90           TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_q100          TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p5        TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p10       TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p25       TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p50       TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p75       TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p90       TYPE NUMERIC(12, 3),
    ALTER COLUMN unimp_exc_p95       TYPE NUMERIC(12, 3),
    ALTER COLUMN pct_ff_avg          TYPE NUMERIC(12, 3),
    ALTER COLUMN pct_ff_cv           TYPE NUMERIC(12, 4),
    ALTER COLUMN deviation_avg       TYPE NUMERIC(12, 3),
    ALTER COLUMN q0                  TYPE NUMERIC(12, 3),
    ALTER COLUMN q10                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q30                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q50                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q70                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q90                 TYPE NUMERIC(12, 3),
    ALTER COLUMN q100                TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p5              TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p10             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p25             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p50             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p75             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p90             TYPE NUMERIC(12, 3),
    ALTER COLUMN exc_p95             TYPE NUMERIC(12, 3);

\echo '  ✅ env_flow_channel_seasonal widened'


-- ─── env_flow_channel_period_summary ─────────────────────────────────────────

\echo ''
\echo '3. Widening env_flow_channel_period_summary pct columns...'

ALTER TABLE env_flow_channel_period_summary
    ALTER COLUMN avg_pct_unimpaired          TYPE NUMERIC(12, 3),
    ALTER COLUMN annual_cv_pct_unimpaired    TYPE NUMERIC(12, 4),
    ALTER COLUMN avg_pct_ff                  TYPE NUMERIC(12, 3),
    ALTER COLUMN annual_cv_pct_ff            TYPE NUMERIC(12, 4);

\echo '  ✅ env_flow_channel_period_summary widened'


-- ─── Verification ─────────────────────────────────────────────────────────────

\echo ''
\echo '===== VERIFICATION ====='
\echo 'pct_unimpaired_avg column type on each table (expect numeric, 12,3):'

SELECT table_name, column_name, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = 'public'
  AND column_name IN ('pct_unimpaired_avg', 'pct_ff_avg', 'avg_pct_unimpaired')
  AND table_name IN (
      'env_flow_channel_monthly',
      'env_flow_channel_seasonal',
      'env_flow_channel_period_summary'
  )
ORDER BY table_name, column_name;

\echo ''
\echo '=== Migration 27 complete ==='
