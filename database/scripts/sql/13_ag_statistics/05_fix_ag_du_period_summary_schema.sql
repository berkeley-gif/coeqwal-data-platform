-- FIX ag_du_period_summary schema conflict from migration 04
--
-- Problem: 02_create_ag_statistics_tables.sql defines both:
--   - annual_delivery_avg_taf (line 160)
--   - annual_demand_avg_taf (line 181, "Back-calculated annual demand")
--
-- Migration 04 tries: RENAME annual_delivery_avg_taf TO annual_demand_avg_taf
-- This fails because annual_demand_avg_taf already exists.
--
-- This migration fixes the schema to match what the ETL and API expect:
--   annual_delivery_* columns → annual_demand_* columns
--   delivery_exc_* columns → demand_exc_* columns
--   + new columns: annual_sw_delivery_avg_taf, annual_gw_pumping_avg_taf, etc.
--
-- Safe to run multiple times (idempotent).

\echo ''
\echo '================================================='
\echo 'FIX: ag_du_period_summary schema reconciliation'
\echo '================================================='

-- Step 1: Drop the original "back-calculated" annual_demand_avg_taf column
--         if the old annual_delivery_avg_taf column still exists (means rename failed)
DO $$
BEGIN
    -- Check if both columns exist (the conflict state)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ag_du_period_summary' AND column_name = 'annual_delivery_avg_taf'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ag_du_period_summary' AND column_name = 'annual_demand_avg_taf'
    ) THEN
        RAISE NOTICE 'Both columns exist.dropping old annual_demand_avg_taf to allow rename';
        ALTER TABLE ag_du_period_summary DROP COLUMN annual_demand_avg_taf;
    END IF;

    -- Now rename if annual_delivery_avg_taf still exists
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ag_du_period_summary' AND column_name = 'annual_delivery_avg_taf'
    ) THEN
        RAISE NOTICE 'Renaming annual_delivery_avg_taf -> annual_demand_avg_taf';
        ALTER TABLE ag_du_period_summary RENAME COLUMN annual_delivery_avg_taf TO annual_demand_avg_taf;
    END IF;

    -- Rename annual_delivery_cv -> annual_demand_cv
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ag_du_period_summary' AND column_name = 'annual_delivery_cv'
    ) THEN
        ALTER TABLE ag_du_period_summary RENAME COLUMN annual_delivery_cv TO annual_demand_cv;
    END IF;

    -- Rename delivery_exc_p* -> demand_exc_p*
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ag_du_period_summary' AND column_name = 'delivery_exc_p5'
    ) THEN
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p5 TO demand_exc_p5;
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p10 TO demand_exc_p10;
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p25 TO demand_exc_p25;
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p50 TO demand_exc_p50;
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p75 TO demand_exc_p75;
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p90 TO demand_exc_p90;
        ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p95 TO demand_exc_p95;
    END IF;
END $$;

-- Step 2: Add new columns (IF NOT EXISTS makes this idempotent)
ALTER TABLE ag_du_period_summary
    ADD COLUMN IF NOT EXISTS annual_demand_cv NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS demand_exc_p5 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS demand_exc_p10 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS demand_exc_p25 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS demand_exc_p50 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS demand_exc_p75 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS demand_exc_p90 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS demand_exc_p95 NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS annual_sw_delivery_avg_taf NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS annual_sw_delivery_cv NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS annual_gw_pumping_avg_taf NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS annual_gw_pumping_cv NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS gw_pumping_pct_of_demand NUMERIC(5,2);

-- Step 3: Verify final schema
\echo ''
\echo 'Final ag_du_period_summary columns:'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'ag_du_period_summary'
ORDER BY ordinal_position;

\echo ''
\echo '✅ ag_du_period_summary schema fix complete'
