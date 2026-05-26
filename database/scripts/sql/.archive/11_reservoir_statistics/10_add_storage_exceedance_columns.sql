-- ============================================================================
-- 10_ADD_STORAGE_EXCEEDANCE_COLUMNS.SQL
-- ============================================================================
-- Adds exceedance percentile columns to reservoir_storage_monthly table
-- for use in percentile charts.
--
-- Part of: 09_STATISTICS LAYER
-- Safe to run multiple times (uses IF NOT EXISTS)
--
-- Note: Includes both percent-of-capacity (exc_p*) and TAF (exc_p*_taf) values
-- for consistency with existing q* and q*_taf columns.
-- ============================================================================

\echo ''
\echo '========================================='
\echo 'ADDING EXCEEDANCE COLUMNS TO RESERVOIR_STORAGE_MONTHLY'
\echo '========================================='

-- Add exceedance percentile columns (percent of capacity)
ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS exc_p5 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS exc_p10 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS exc_p25 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS exc_p50 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS exc_p75 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS exc_p90 NUMERIC(6,2),
ADD COLUMN IF NOT EXISTS exc_p95 NUMERIC(6,2);

\echo 'Percent-of-capacity exceedance columns added.'

-- Add exceedance percentile columns (TAF)
ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS exc_p5_taf NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p10_taf NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p25_taf NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p50_taf NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p75_taf NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p90_taf NUMERIC(10,2),
ADD COLUMN IF NOT EXISTS exc_p95_taf NUMERIC(10,2);

\echo 'TAF exceedance columns added.'

-- Add comments for percent-of-capacity columns
COMMENT ON COLUMN reservoir_storage_monthly.exc_p5 IS 'Storage (% of capacity) exceeded 5% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p10 IS 'Storage (% of capacity) exceeded 10% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p25 IS 'Storage (% of capacity) exceeded 25% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p50 IS 'Storage (% of capacity) exceeded 50% of the time (median)';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p75 IS 'Storage (% of capacity) exceeded 75% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p90 IS 'Storage (% of capacity) exceeded 90% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p95 IS 'Storage (% of capacity) exceeded 95% of the time';

-- Add comments for TAF columns
COMMENT ON COLUMN reservoir_storage_monthly.exc_p5_taf IS 'Storage (TAF) exceeded 5% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p10_taf IS 'Storage (TAF) exceeded 10% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p25_taf IS 'Storage (TAF) exceeded 25% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p50_taf IS 'Storage (TAF) exceeded 50% of the time (median)';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p75_taf IS 'Storage (TAF) exceeded 75% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p90_taf IS 'Storage (TAF) exceeded 90% of the time';
COMMENT ON COLUMN reservoir_storage_monthly.exc_p95_taf IS 'Storage (TAF) exceeded 95% of the time';

-- Verify columns exist
\echo ''
\echo 'Verifying exceedance columns exist...'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'reservoir_storage_monthly'
  AND column_name LIKE 'exc_p%'
ORDER BY column_name;

\echo ''
\echo '========================================='
\echo 'MIGRATION COMPLETE'
\echo '========================================='
