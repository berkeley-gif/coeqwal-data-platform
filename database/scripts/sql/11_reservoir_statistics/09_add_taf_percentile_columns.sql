-- ============================================================================
-- 09_ADD_TAF_PERCENTILE_COLUMNS.SQL
-- ============================================================================
-- Adds TAF (volume) based percentile columns to complement the existing
-- percentage-of-capacity percentile columns.
--
-- This allows the API to serve both:
--   - Percentage of capacity (q0, q10, ..., q100) - for normalized comparison
--   - Volume in TAF (q0_taf, q10_taf, ..., q100_taf) - for absolute values
--
-- Tables modified:
--   - reservoir_monthly_percentile
--   - reservoir_storage_monthly
--
-- Part of: 09_STATISTICS LAYER
-- ============================================================================

\echo '============================================================================'
\echo 'ADDING TAF PERCENTILE COLUMNS'
\echo '============================================================================'

-- ============================================================================
-- RESERVOIR_MONTHLY_PERCENTILE - Add TAF columns
-- ============================================================================
\echo 'Adding TAF columns to reservoir_monthly_percentile...'

-- Percentile values in TAF (thousand acre-feet)
ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q0_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q10_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q30_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q50_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q70_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q90_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS q100_taf NUMERIC(10,2);

ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS mean_taf NUMERIC(10,2);

-- Add capacity_taf for reference (allows client-side conversion if needed)
ALTER TABLE reservoir_monthly_percentile
ADD COLUMN IF NOT EXISTS capacity_taf NUMERIC(10,2);

-- Add comments for clarity
COMMENT ON COLUMN reservoir_monthly_percentile.q0 IS
    'Minimum storage as % of capacity (0th percentile)';
COMMENT ON COLUMN reservoir_monthly_percentile.q0_taf IS
    'Minimum storage in TAF (0th percentile)';
COMMENT ON COLUMN reservoir_monthly_percentile.q10 IS
    '10th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_monthly_percentile.q10_taf IS
    '10th percentile storage in TAF';
COMMENT ON COLUMN reservoir_monthly_percentile.q30 IS
    '30th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_monthly_percentile.q30_taf IS
    '30th percentile storage in TAF';
COMMENT ON COLUMN reservoir_monthly_percentile.q50 IS
    'Median storage as % of capacity (50th percentile)';
COMMENT ON COLUMN reservoir_monthly_percentile.q50_taf IS
    'Median storage in TAF (50th percentile)';
COMMENT ON COLUMN reservoir_monthly_percentile.q70 IS
    '70th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_monthly_percentile.q70_taf IS
    '70th percentile storage in TAF';
COMMENT ON COLUMN reservoir_monthly_percentile.q90 IS
    '90th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_monthly_percentile.q90_taf IS
    '90th percentile storage in TAF';
COMMENT ON COLUMN reservoir_monthly_percentile.q100 IS
    'Maximum storage as % of capacity (100th percentile)';
COMMENT ON COLUMN reservoir_monthly_percentile.q100_taf IS
    'Maximum storage in TAF (100th percentile)';
COMMENT ON COLUMN reservoir_monthly_percentile.mean_value IS
    'Mean storage as % of capacity';
COMMENT ON COLUMN reservoir_monthly_percentile.mean_taf IS
    'Mean storage in TAF';
COMMENT ON COLUMN reservoir_monthly_percentile.capacity_taf IS
    'Reservoir capacity in TAF (for reference/conversion)';

\echo 'Added TAF columns to reservoir_monthly_percentile'

-- ============================================================================
-- RESERVOIR_STORAGE_MONTHLY - Add TAF columns
-- ============================================================================
\echo 'Adding TAF columns to reservoir_storage_monthly...'

-- Percentile values in TAF (thousand acre-feet)
-- Note: storage_avg_taf already exists in this table
ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q0_taf NUMERIC(10,2);

ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q10_taf NUMERIC(10,2);

ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q30_taf NUMERIC(10,2);

ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q50_taf NUMERIC(10,2);

ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q70_taf NUMERIC(10,2);

ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q90_taf NUMERIC(10,2);

ALTER TABLE reservoir_storage_monthly
ADD COLUMN IF NOT EXISTS q100_taf NUMERIC(10,2);

-- Add comments for clarity
COMMENT ON COLUMN reservoir_storage_monthly.q0 IS
    'Minimum storage as % of capacity (0th percentile)';
COMMENT ON COLUMN reservoir_storage_monthly.q0_taf IS
    'Minimum storage in TAF (0th percentile)';
COMMENT ON COLUMN reservoir_storage_monthly.q10 IS
    '10th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_storage_monthly.q10_taf IS
    '10th percentile storage in TAF';
COMMENT ON COLUMN reservoir_storage_monthly.q30 IS
    '30th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_storage_monthly.q30_taf IS
    '30th percentile storage in TAF';
COMMENT ON COLUMN reservoir_storage_monthly.q50 IS
    'Median storage as % of capacity (50th percentile)';
COMMENT ON COLUMN reservoir_storage_monthly.q50_taf IS
    'Median storage in TAF (50th percentile)';
COMMENT ON COLUMN reservoir_storage_monthly.q70 IS
    '70th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_storage_monthly.q70_taf IS
    '70th percentile storage in TAF';
COMMENT ON COLUMN reservoir_storage_monthly.q90 IS
    '90th percentile storage as % of capacity';
COMMENT ON COLUMN reservoir_storage_monthly.q90_taf IS
    '90th percentile storage in TAF';
COMMENT ON COLUMN reservoir_storage_monthly.q100 IS
    'Maximum storage as % of capacity (100th percentile)';
COMMENT ON COLUMN reservoir_storage_monthly.q100_taf IS
    'Maximum storage in TAF (100th percentile)';
COMMENT ON COLUMN reservoir_storage_monthly.storage_avg_taf IS
    'Mean storage in TAF';

\echo 'Added TAF columns to reservoir_storage_monthly'

-- ============================================================================
-- VERIFICATION
-- ============================================================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'reservoir_monthly_percentile columns:'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'reservoir_monthly_percentile'
AND column_name LIKE '%taf%' OR column_name LIKE 'q%'
ORDER BY column_name;

\echo ''
\echo 'reservoir_storage_monthly columns:'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'reservoir_storage_monthly'
AND column_name LIKE '%taf%' OR column_name LIKE 'q%'
ORDER BY column_name;

\echo ''
\echo '============================================================================'
\echo 'TAF PERCENTILE COLUMNS ADDED SUCCESSFULLY'
\echo '============================================================================'
\echo ''
\echo 'New columns added to reservoir_monthly_percentile:'
\echo '  q0_taf, q10_taf, q30_taf, q50_taf, q70_taf, q90_taf, q100_taf, mean_taf, capacity_taf'
\echo ''
\echo 'New columns added to reservoir_storage_monthly:'
\echo '  q0_taf, q10_taf, q30_taf, q50_taf, q70_taf, q90_taf, q100_taf'
\echo ''
\echo 'Run ETL to populate: python main.py --all-scenarios'
\echo ''
