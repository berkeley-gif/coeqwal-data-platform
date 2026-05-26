-- ============================================================================
-- 08_ADD_CWS_SHORTAGE_EXCEEDANCE_COLUMNS.SQL
-- ============================================================================
-- Adds shortage exceedance percentile columns to cws_aggregate_monthly table
-- for use in percentile charts.
--
-- Part of: 10_MI_STATISTICS LAYER
-- Safe to run multiple times (uses IF NOT EXISTS)
-- ============================================================================

\echo ''
\echo '========================================='
\echo 'ADDING SHORTAGE EXCEEDANCE COLUMNS TO CWS_AGGREGATE_MONTHLY'
\echo '========================================='

-- Add shortage exceedance percentile columns
ALTER TABLE cws_aggregate_monthly
ADD COLUMN IF NOT EXISTS shortage_exc_p5 NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS shortage_exc_p10 NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS shortage_exc_p25 NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS shortage_exc_p50 NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS shortage_exc_p75 NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS shortage_exc_p90 NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS shortage_exc_p95 NUMERIC(12,2);

\echo 'cws_aggregate_monthly shortage exceedance columns added.'

-- Add comments for new columns
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p5 IS 'Shortage value exceeded 5% of the time (95th percentile)';
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p10 IS 'Shortage value exceeded 10% of the time (90th percentile)';
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p25 IS 'Shortage value exceeded 25% of the time (75th percentile)';
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p50 IS 'Shortage value exceeded 50% of the time (median)';
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p75 IS 'Shortage value exceeded 75% of the time (25th percentile)';
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p90 IS 'Shortage value exceeded 90% of the time (10th percentile)';
COMMENT ON COLUMN cws_aggregate_monthly.shortage_exc_p95 IS 'Shortage value exceeded 95% of the time (5th percentile)';

-- Verify columns exist
\echo ''
\echo 'Verifying columns exist...'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'cws_aggregate_monthly'
  AND column_name LIKE 'shortage_exc_p%'
ORDER BY column_name;

\echo ''
\echo '========================================='
\echo 'MIGRATION COMPLETE'
\echo '========================================='
