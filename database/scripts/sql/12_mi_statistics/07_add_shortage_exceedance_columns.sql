-- ADD EXCEEDANCE PERCENTILE COLUMNS TO SHORTAGE TABLES
-- 
-- This migration adds exc_p5 through exc_p95 columns to shortage tables
-- to match the structure of delivery tables and fix the API bug where
-- monthly_shortage returns null.
--
-- Run with: psql $DATABASE_URL -f 07_add_shortage_exceedance_columns.sql

\echo ''
\echo '========================================='
\echo 'ADDING EXCEEDANCE COLUMNS TO SHORTAGE TABLES'
\echo '========================================='

-- ============================================
-- 1. DU_SHORTAGE_MONTHLY
-- ============================================
\echo ''
\echo 'Adding exceedance columns to du_shortage_monthly...'

ALTER TABLE du_shortage_monthly
    ADD COLUMN IF NOT EXISTS exc_p5 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p10 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p25 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p50 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p75 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p90 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p95 NUMERIC(10,2);

\echo 'du_shortage_monthly updated.'

-- ============================================
-- 2. MI_SHORTAGE_MONTHLY
-- ============================================
\echo ''
\echo 'Adding exceedance columns to mi_shortage_monthly...'

ALTER TABLE mi_shortage_monthly
    ADD COLUMN IF NOT EXISTS exc_p5 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p10 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p25 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p50 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p75 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p90 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS exc_p95 NUMERIC(10,2);

\echo 'mi_shortage_monthly updated.'

-- ============================================
-- VERIFY
-- ============================================
\echo ''
\echo 'Verifying columns exist...'

SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'du_shortage_monthly' 
  AND column_name LIKE 'exc_%'
ORDER BY column_name;

SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'mi_shortage_monthly' 
  AND column_name LIKE 'exc_%'
ORDER BY column_name;

\echo ''
\echo 'Migration complete!'
