-- ============================================================================
-- 03_ADD_SHORTAGE_EXCEEDANCE_COLUMNS.SQL
-- ============================================================================
-- Adds exceedance percentile columns to ag_du_shortage_monthly table
-- for use in percentile charts.
--
-- Part of: 11_AG_STATISTICS LAYER
-- Safe to run multiple times (uses IF NOT EXISTS)
-- ============================================================================

\echo ''
\echo '========================================='
\echo 'ADDING EXCEEDANCE COLUMNS TO AG_DU_SHORTAGE_MONTHLY'
\echo '========================================='

-- Add exceedance percentile columns
ALTER TABLE ag_du_shortage_monthly
ADD COLUMN IF NOT EXISTS exc_p5 NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p10 NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p25 NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p50 NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p75 NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p90 NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p95 NUMERIC(10,2);

\echo 'ag_du_shortage_monthly exceedance columns added.'

-- Add comments for new columns
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p5 IS 'Value exceeded 5% of the time (95th percentile of shortage)';
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p10 IS 'Value exceeded 10% of the time (90th percentile of shortage)';
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p25 IS 'Value exceeded 25% of the time (75th percentile of shortage)';
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p50 IS 'Value exceeded 50% of the time (median shortage)';
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p75 IS 'Value exceeded 75% of the time (25th percentile of shortage)';
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p90 IS 'Value exceeded 90% of the time (10th percentile of shortage)';
COMMENT ON COLUMN ag_du_shortage_monthly.exc_p95 IS 'Value exceeded 95% of the time (5th percentile of shortage)';

-- Verify columns exist
\echo ''
\echo 'Verifying columns exist...'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'ag_du_shortage_monthly'
  AND column_name LIKE 'exc_p%'
ORDER BY column_name;

\echo ''
\echo '========================================='
\echo 'MIGRATION COMPLETE'
\echo '========================================='
