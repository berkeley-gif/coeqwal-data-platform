-- ============================================================================
-- 08_ADD_PROBABILITY_METRICS_TO_PERIOD_SUMMARY.SQL
-- ============================================================================
-- Adds probability metrics to reservoir_period_summary table.
-- These columns are aligned with COEQWAL research notebooks:
--   - coeqwal/notebooks/coeqwalpackage/metrics.py
--   - coeqwal/notebooks/Metrics.ipynb
--
-- New columns:
--   - Flood pool probability (all months, September, April)
--   - Dead pool probability (all months, September)
--   - Storage coefficient of variation (all, April, September)
--   - Average storage (annual, April, September)
--
-- Part of: 09_STATISTICS LAYER
-- ============================================================================

\echo '============================================================================'
\echo 'ADDING PROBABILITY METRICS TO RESERVOIR_PERIOD_SUMMARY'
\echo '============================================================================'

-- ============================================================================
-- FLOOD POOL PROBABILITY
-- Definition: P(storage >= flood control level)
-- Aligned with metrics.py:617-655 frequency_hitting_level()
-- ============================================================================

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS flood_pool_prob_all NUMERIC(6,4);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS flood_pool_prob_september NUMERIC(6,4);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS flood_pool_prob_april NUMERIC(6,4);

COMMENT ON COLUMN reservoir_period_summary.flood_pool_prob_all IS
    'Probability storage >= flood control level, all months (0.0-1.0)';

COMMENT ON COLUMN reservoir_period_summary.flood_pool_prob_september IS
    'Probability storage >= flood control level, September only (end of water year)';

COMMENT ON COLUMN reservoir_period_summary.flood_pool_prob_april IS
    'Probability storage >= flood control level, April only (spring peak)';

\echo 'Added flood_pool_prob columns'

-- ============================================================================
-- DEAD POOL PROBABILITY
-- Definition: P(storage <= dead pool level)
-- Aligned with metrics.py frequency_hitting_level() with floodzone=False
-- ============================================================================

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS dead_pool_prob_all NUMERIC(6,4);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS dead_pool_prob_september NUMERIC(6,4);

COMMENT ON COLUMN reservoir_period_summary.dead_pool_prob_all IS
    'Probability storage <= dead pool level, all months (0.0-1.0)';

COMMENT ON COLUMN reservoir_period_summary.dead_pool_prob_september IS
    'Probability storage <= dead pool level, September only (end of water year)';

\echo 'Added dead_pool_prob columns'

-- ============================================================================
-- STORAGE COEFFICIENT OF VARIATION (CV)
-- Definition: CV = standard_deviation / mean
-- Aligned with metrics.py:383-393 compute_cv()
-- ============================================================================

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS storage_cv_all NUMERIC(6,4);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS storage_cv_april NUMERIC(6,4);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS storage_cv_september NUMERIC(6,4);

COMMENT ON COLUMN reservoir_period_summary.storage_cv_all IS
    'Coefficient of variation of storage, all months (higher = more variable)';

COMMENT ON COLUMN reservoir_period_summary.storage_cv_april IS
    'Coefficient of variation of storage, April only (spring variability)';

COMMENT ON COLUMN reservoir_period_summary.storage_cv_september IS
    'Coefficient of variation of storage, September only (end of water year variability)';

\echo 'Added storage_cv columns'

-- ============================================================================
-- AVERAGE STORAGE
-- Definition: Mean of annual means (annual_avg) or monthly mean (april/september)
-- Aligned with metrics.py:526-534 ann_avg() and metrics.py:545-554 mnth_avg()
-- ============================================================================

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS annual_avg_taf NUMERIC(10,2);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS april_avg_taf NUMERIC(10,2);

ALTER TABLE reservoir_period_summary
ADD COLUMN IF NOT EXISTS september_avg_taf NUMERIC(10,2);

COMMENT ON COLUMN reservoir_period_summary.annual_avg_taf IS
    'Mean of annual mean storage (TAF) - average across water years';

COMMENT ON COLUMN reservoir_period_summary.april_avg_taf IS
    'Mean April storage (TAF) - spring peak indicator';

COMMENT ON COLUMN reservoir_period_summary.september_avg_taf IS
    'Mean September storage (TAF) - end of water year indicator';

\echo 'Added avg_taf columns'

-- ============================================================================
-- INDEXES FOR PROBABILITY QUERIES
-- ============================================================================

-- Index for flood risk queries (high flood probability reservoirs)
CREATE INDEX IF NOT EXISTS idx_period_summary_flood_prob
    ON reservoir_period_summary(flood_pool_prob_all DESC NULLS LAST)
    WHERE flood_pool_prob_all IS NOT NULL;

-- Index for drought risk queries (high dead pool probability reservoirs)
CREATE INDEX IF NOT EXISTS idx_period_summary_dead_pool_prob
    ON reservoir_period_summary(dead_pool_prob_all DESC NULLS LAST)
    WHERE dead_pool_prob_all IS NOT NULL;

-- Index for variability queries
CREATE INDEX IF NOT EXISTS idx_period_summary_cv
    ON reservoir_period_summary(storage_cv_all DESC NULLS LAST)
    WHERE storage_cv_all IS NOT NULL;

\echo 'Created indexes'

-- ============================================================================
-- VERIFICATION
-- ============================================================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'reservoir_period_summary'
AND column_name IN (
    'flood_pool_prob_all', 'flood_pool_prob_september', 'flood_pool_prob_april',
    'dead_pool_prob_all', 'dead_pool_prob_september',
    'storage_cv_all', 'storage_cv_april', 'storage_cv_september',
    'annual_avg_taf', 'april_avg_taf', 'september_avg_taf'
)
ORDER BY column_name;

\echo ''
\echo 'Total columns after migration:'
SELECT COUNT(*) as column_count
FROM information_schema.columns
WHERE table_name = 'reservoir_period_summary';

\echo ''
\echo '============================================================================'
\echo 'PROBABILITY METRICS ADDED SUCCESSFULLY'
\echo '============================================================================'
\echo ''
\echo 'New columns added:'
\echo '  - flood_pool_prob_all, flood_pool_prob_september, flood_pool_prob_april'
\echo '  - dead_pool_prob_all, dead_pool_prob_september'
\echo '  - storage_cv_all, storage_cv_april, storage_cv_september'
\echo '  - annual_avg_taf, april_avg_taf, september_avg_taf'
\echo ''
\echo 'Run ETL to populate: python main.py --all-scenarios'
\echo ''
