-- 55_widen_numeric_pct_columns.sql
--
-- Widen all NUMERIC(5,2) percentage columns to NUMERIC(7,2) across statistics
-- tables. Under climate change scenarios (CC50/CC95), ratio-based metrics like
-- gw_pumping_pct_of_demand can exceed 999.99 (the NUMERIC(5,2) ceiling),
-- causing "numeric field overflow" errors in the ETL.
--
-- NUMERIC(7,2) allows values up to 99999.99, which is more than sufficient
-- for any percentage or ratio metric.
--
-- Safe to run multiple times (ALTER TYPE is idempotent when widening).

BEGIN;

-- ============================================================
-- ag_du_shortage_monthly
-- ============================================================
ALTER TABLE ag_du_shortage_monthly
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2);

-- ============================================================
-- ag_du_period_summary
-- ============================================================
ALTER TABLE ag_du_period_summary
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2),
    ALTER COLUMN reliability_pct TYPE NUMERIC(7,2),
    ALTER COLUMN avg_pct_demand_met TYPE NUMERIC(7,2),
    ALTER COLUMN gw_pumping_pct_of_demand TYPE NUMERIC(7,2);

-- ============================================================
-- ag_aggregate_monthly
-- ============================================================
ALTER TABLE ag_aggregate_monthly
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2);

-- ============================================================
-- ag_aggregate_period_summary
-- ============================================================
ALTER TABLE ag_aggregate_period_summary
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2),
    ALTER COLUMN reliability_pct TYPE NUMERIC(7,2);

-- ============================================================
-- mi_delivery_monthly
-- ============================================================
ALTER TABLE mi_delivery_monthly
    ALTER COLUMN percent_of_demand_avg TYPE NUMERIC(7,2);

-- ============================================================
-- mi_shortage_monthly
-- ============================================================
ALTER TABLE mi_shortage_monthly
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2);

-- ============================================================
-- mi_contractor_period_summary
-- ============================================================
ALTER TABLE mi_contractor_period_summary
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2),
    ALTER COLUMN reliability_pct TYPE NUMERIC(7,2),
    ALTER COLUMN avg_pct_demand_met TYPE NUMERIC(7,2);

-- ============================================================
-- cws_aggregate_monthly
-- ============================================================
ALTER TABLE cws_aggregate_monthly
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2),
    ALTER COLUMN percent_of_demand_avg TYPE NUMERIC(7,2);

-- ============================================================
-- cws_aggregate_period_summary
-- ============================================================
ALTER TABLE cws_aggregate_period_summary
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2),
    ALTER COLUMN reliability_pct TYPE NUMERIC(7,2),
    ALTER COLUMN avg_pct_allocation_met TYPE NUMERIC(7,2),
    ALTER COLUMN avg_pct_demand_met TYPE NUMERIC(7,2);

-- ============================================================
-- du_shortage_monthly
-- ============================================================
ALTER TABLE du_shortage_monthly
    ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2);

-- ============================================================
-- reservoir_period_summary
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reservoir_period_summary'
          AND column_name = 'spill_frequency_pct'
          AND numeric_precision = 5
    ) THEN
        ALTER TABLE reservoir_period_summary
            ALTER COLUMN spill_frequency_pct TYPE NUMERIC(7,2);
    END IF;
END $$;

-- ============================================================
-- du_delivery_monthly (check if percent_of_demand_avg exists)
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'du_delivery_monthly'
          AND column_name = 'percent_of_demand_avg'
          AND numeric_precision = 5
    ) THEN
        ALTER TABLE du_delivery_monthly
            ALTER COLUMN percent_of_demand_avg TYPE NUMERIC(7,2);
    END IF;
END $$;

-- ============================================================
-- du_period_summary (check if shortage_frequency_pct exists)
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'du_period_summary'
          AND column_name = 'shortage_frequency_pct'
          AND numeric_precision = 5
    ) THEN
        ALTER TABLE du_period_summary
            ALTER COLUMN shortage_frequency_pct TYPE NUMERIC(7,2);
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'du_period_summary'
          AND column_name = 'reliability_pct'
          AND numeric_precision = 5
    ) THEN
        ALTER TABLE du_period_summary
            ALTER COLUMN reliability_pct TYPE NUMERIC(7,2);
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'du_period_summary'
          AND column_name = 'avg_pct_demand_met'
          AND numeric_precision = 5
    ) THEN
        ALTER TABLE du_period_summary
            ALTER COLUMN avg_pct_demand_met TYPE NUMERIC(7,2);
    END IF;
END $$;

COMMIT;

\echo 'All NUMERIC(5,2) percentage columns widened to NUMERIC(7,2).'
