-- AG DATA MODEL CORRECTION: Add SW Delivery and GW Pumping Tables
-- 
-- This migration corrects the AG data model based on CalSim variable definitions:
--   - AW_* = Applied Water = DEMAND (not delivery)
--   - DN_* = Net Delivery = SURFACE WATER DELIVERY
--   - GP_* or (AW - DN) = GROUNDWATER PUMPING
--   - GW_SHORT_* = Groundwater Restriction Shortage (COEQWAL-specific)
--
-- Changes:
--   1. Rename ag_du_delivery_monthly -> ag_du_demand_monthly
--   2. Create ag_du_sw_delivery_monthly (Surface Water Delivery from DN_*)
--   3. Create ag_du_gw_pumping_monthly (GW Pumping, calculated as AW - DN)
--
-- Run with: psql $DATABASE_URL -f 04_add_sw_delivery_gw_pumping_tables.sql

\echo ''
\echo '=============================================='
\echo 'AG DATA MODEL CORRECTION MIGRATION'
\echo '=============================================='

-- ============================================
-- 1. RENAME ag_du_delivery_monthly to ag_du_demand_monthly
-- The existing table uses AW_* which is actually DEMAND, not delivery
-- ============================================
\echo ''
\echo 'Step 1: Renaming ag_du_delivery_monthly to ag_du_demand_monthly...'

-- Rename the table
ALTER TABLE IF EXISTS ag_du_delivery_monthly RENAME TO ag_du_demand_monthly;

-- Rename columns to reflect demand semantics
ALTER TABLE ag_du_demand_monthly RENAME COLUMN delivery_avg_taf TO demand_avg_taf;
ALTER TABLE ag_du_demand_monthly RENAME COLUMN delivery_cv TO demand_cv;

-- Rename indexes
ALTER INDEX IF EXISTS idx_ag_du_delivery_monthly_scenario RENAME TO idx_ag_du_demand_monthly_scenario;
ALTER INDEX IF EXISTS idx_ag_du_delivery_monthly_du RENAME TO idx_ag_du_demand_monthly_du;
ALTER INDEX IF EXISTS idx_ag_du_delivery_monthly_combined RENAME TO idx_ag_du_demand_monthly_combined;
ALTER INDEX IF EXISTS uq_ag_du_delivery_monthly RENAME TO uq_ag_du_demand_monthly;

-- Rename constraint
ALTER TABLE ag_du_demand_monthly RENAME CONSTRAINT chk_ag_du_delivery_water_month TO chk_ag_du_demand_water_month;

-- Update comments
COMMENT ON TABLE ag_du_demand_monthly IS 'Monthly DEMAND statistics for agricultural demand units. Source: AW_* (Applied Water) variables from CalSim SV input file. NOTE: This is DEMAND, not delivery. For actual surface water delivery, see ag_du_sw_delivery_monthly.';
COMMENT ON COLUMN ag_du_demand_monthly.demand_avg_taf IS 'Average monthly demand (applied water requirement) in thousand acre-feet';
COMMENT ON COLUMN ag_du_demand_monthly.demand_cv IS 'Coefficient of variation of monthly demand';

\echo '✅ Renamed ag_du_delivery_monthly to ag_du_demand_monthly'

-- ============================================
-- 2. CREATE ag_du_sw_delivery_monthly
-- Surface Water Delivery from DN_* variables (DV output file)
-- ============================================
\echo ''
\echo 'Step 2: Creating ag_du_sw_delivery_monthly table...'

DROP TABLE IF EXISTS ag_du_sw_delivery_monthly CASCADE;

CREATE TABLE ag_du_sw_delivery_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,               -- References du_agriculture_entity.du_id
    water_month INTEGER NOT NULL,             -- 1-12 (Oct=1, Sep=12)

    -- Surface Water Delivery statistics (TAF)
    sw_delivery_avg_taf NUMERIC(10,2),
    sw_delivery_cv NUMERIC(10,4),             -- Coefficient of variation

    -- Delivery percentiles (TAF) - for box plots
    q0 NUMERIC(10,2),                         -- Minimum
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),                        -- Median
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),                       -- Maximum

    -- Exceedance percentiles (TAF) - for exceedance plots
    -- exc_pX = value exceeded X% of the time = (100-X)th percentile
    exc_p5 NUMERIC(10,2),                     -- Value exceeded 5% of time (95th percentile)
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),                    -- Median
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),                    -- Value exceeded 95% of time (5th percentile)

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_du_sw_delivery_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_ag_du_sw_delivery_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- Comments
COMMENT ON TABLE ag_du_sw_delivery_monthly IS 'Monthly SURFACE WATER DELIVERY statistics for agricultural demand units. Source: DN_* (Net Delivery) variables from CalSim DV output file. For groundwater-only DUs, SW delivery is zero.';
COMMENT ON COLUMN ag_du_sw_delivery_monthly.du_id IS 'Agricultural demand unit ID, references du_agriculture_entity.du_id';
COMMENT ON COLUMN ag_du_sw_delivery_monthly.water_month IS 'Water month: 1=October, 2=November, ..., 12=September';
COMMENT ON COLUMN ag_du_sw_delivery_monthly.sw_delivery_avg_taf IS 'Average monthly surface water delivery in thousand acre-feet';
COMMENT ON COLUMN ag_du_sw_delivery_monthly.exc_p5 IS 'Value exceeded 5% of the time (95th percentile of SW delivery)';
COMMENT ON COLUMN ag_du_sw_delivery_monthly.exc_p95 IS 'Value exceeded 95% of the time (5th percentile of SW delivery)';

-- Indexes
CREATE INDEX idx_ag_du_sw_delivery_monthly_scenario ON ag_du_sw_delivery_monthly(scenario_short_code);
CREATE INDEX idx_ag_du_sw_delivery_monthly_du ON ag_du_sw_delivery_monthly(du_id);
CREATE INDEX idx_ag_du_sw_delivery_monthly_combined ON ag_du_sw_delivery_monthly(scenario_short_code, du_id);

\echo '✅ Created ag_du_sw_delivery_monthly table'

-- ============================================
-- 3. CREATE ag_du_gw_pumping_monthly
-- Groundwater Pumping: GP_* where available, or calculated as (AW - DN)
-- ============================================
\echo ''
\echo 'Step 3: Creating ag_du_gw_pumping_monthly table...'

DROP TABLE IF EXISTS ag_du_gw_pumping_monthly CASCADE;

CREATE TABLE ag_du_gw_pumping_monthly (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    du_id VARCHAR(20) NOT NULL,               -- References du_agriculture_entity.du_id
    water_month INTEGER NOT NULL,             -- 1-12 (Oct=1, Sep=12)

    -- Groundwater Pumping statistics (TAF)
    gw_pumping_avg_taf NUMERIC(10,2),
    gw_pumping_cv NUMERIC(10,4),              -- Coefficient of variation

    -- Pumping percentiles (TAF) - for box plots
    q0 NUMERIC(10,2),                         -- Minimum
    q10 NUMERIC(10,2),
    q30 NUMERIC(10,2),
    q50 NUMERIC(10,2),                        -- Median
    q70 NUMERIC(10,2),
    q90 NUMERIC(10,2),
    q100 NUMERIC(10,2),                       -- Maximum

    -- Exceedance percentiles (TAF) - for exceedance plots
    -- exc_pX = value exceeded X% of the time = (100-X)th percentile
    exc_p5 NUMERIC(10,2),                     -- Value exceeded 5% of time (95th percentile)
    exc_p10 NUMERIC(10,2),
    exc_p25 NUMERIC(10,2),
    exc_p50 NUMERIC(10,2),                    -- Median
    exc_p75 NUMERIC(10,2),
    exc_p90 NUMERIC(10,2),
    exc_p95 NUMERIC(10,2),                    -- Value exceeded 95% of time (5th percentile)

    -- Source tracking
    is_calculated BOOLEAN NOT NULL DEFAULT TRUE,  -- TRUE if calculated as AW-DN, FALSE if from GP_* variable

    sample_count INTEGER,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_ag_du_gw_pumping_monthly
        UNIQUE(scenario_short_code, du_id, water_month),
    CONSTRAINT chk_ag_du_gw_pumping_water_month
        CHECK (water_month BETWEEN 1 AND 12)
);

-- Comments
COMMENT ON TABLE ag_du_gw_pumping_monthly IS 'Monthly GROUNDWATER PUMPING statistics for agricultural demand units. Source: GP_* variables where available, or calculated as (AW - DN) = (Demand - SW Delivery). In CalSim, AG demand is assumed to be fully met, with any SW shortfall made up by groundwater.';
COMMENT ON COLUMN ag_du_gw_pumping_monthly.du_id IS 'Agricultural demand unit ID, references du_agriculture_entity.du_id';
COMMENT ON COLUMN ag_du_gw_pumping_monthly.water_month IS 'Water month: 1=October, 2=November, ..., 12=September';
COMMENT ON COLUMN ag_du_gw_pumping_monthly.gw_pumping_avg_taf IS 'Average monthly groundwater pumping in thousand acre-feet';
COMMENT ON COLUMN ag_du_gw_pumping_monthly.is_calculated IS 'TRUE if GW pumping was calculated as (AW - DN), FALSE if from explicit GP_* variable';
COMMENT ON COLUMN ag_du_gw_pumping_monthly.exc_p5 IS 'Value exceeded 5% of the time (95th percentile of GW pumping)';
COMMENT ON COLUMN ag_du_gw_pumping_monthly.exc_p95 IS 'Value exceeded 95% of the time (5th percentile of GW pumping)';

-- Indexes
CREATE INDEX idx_ag_du_gw_pumping_monthly_scenario ON ag_du_gw_pumping_monthly(scenario_short_code);
CREATE INDEX idx_ag_du_gw_pumping_monthly_du ON ag_du_gw_pumping_monthly(du_id);
CREATE INDEX idx_ag_du_gw_pumping_monthly_combined ON ag_du_gw_pumping_monthly(scenario_short_code, du_id);

\echo '✅ Created ag_du_gw_pumping_monthly table'

-- ============================================
-- 4. UPDATE ag_du_period_summary column names and comments
-- ============================================
\echo ''
\echo 'Step 4: Updating ag_du_period_summary to clarify semantics...'

-- Rename delivery columns to demand columns
ALTER TABLE ag_du_period_summary RENAME COLUMN annual_delivery_avg_taf TO annual_demand_avg_taf;
ALTER TABLE ag_du_period_summary RENAME COLUMN annual_delivery_cv TO annual_demand_cv;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p5 TO demand_exc_p5;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p10 TO demand_exc_p10;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p25 TO demand_exc_p25;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p50 TO demand_exc_p50;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p75 TO demand_exc_p75;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p90 TO demand_exc_p90;
ALTER TABLE ag_du_period_summary RENAME COLUMN delivery_exc_p95 TO demand_exc_p95;

-- Add new columns for SW delivery and GW pumping summaries
ALTER TABLE ag_du_period_summary 
    ADD COLUMN IF NOT EXISTS annual_sw_delivery_avg_taf NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS annual_sw_delivery_cv NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS annual_gw_pumping_avg_taf NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS annual_gw_pumping_cv NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS gw_pumping_pct_of_demand NUMERIC(5,2);

-- Update comments
COMMENT ON TABLE ag_du_period_summary IS 'Period-of-record summary statistics for agricultural demand units. Demand from AW_*, SW Delivery from DN_*, GW Pumping calculated as AW - DN.';
COMMENT ON COLUMN ag_du_period_summary.annual_demand_avg_taf IS 'Average annual demand (applied water requirement) in TAF. Source: AW_* variables.';
COMMENT ON COLUMN ag_du_period_summary.annual_sw_delivery_avg_taf IS 'Average annual surface water delivery in TAF. Source: DN_* variables.';
COMMENT ON COLUMN ag_du_period_summary.annual_gw_pumping_avg_taf IS 'Average annual groundwater pumping in TAF. Calculated as AW - DN.';
COMMENT ON COLUMN ag_du_period_summary.gw_pumping_pct_of_demand IS 'Average groundwater pumping as percentage of total demand.';

\echo '✅ Updated ag_du_period_summary table'

-- ============================================
-- 5. UPDATE ag_du_shortage_monthly comments
-- Clarify that this is GW RESTRICTION shortage, not total shortage
-- ============================================
\echo ''
\echo 'Step 5: Updating ag_du_shortage_monthly comments...'

COMMENT ON TABLE ag_du_shortage_monthly IS 'Monthly GROUNDWATER RESTRICTION shortage statistics for agricultural demand units. Source: GW_SHORT_* variables (COEQWAL-specific, added for testing groundwater pumping restrictions). This is NOT total delivery shortage - CalSim assumes AG demand is always fully met via SW + GW. Only SJR/Tulare regions have this data, and only in scenarios with GW restrictions enabled.';

\echo '✅ Updated ag_du_shortage_monthly comments'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo '=============================================='
\echo 'MIGRATION COMPLETE - VERIFICATION'
\echo '=============================================='

\echo ''
\echo 'AG tables after migration:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name LIKE 'ag_%'
ORDER BY table_name;

\echo ''
\echo 'New indexes created:'
SELECT indexname, tablename
FROM pg_indexes
WHERE tablename IN ('ag_du_sw_delivery_monthly', 'ag_du_gw_pumping_monthly', 'ag_du_demand_monthly')
ORDER BY tablename, indexname;

\echo ''
\echo '✅ AG Data Model Correction Migration Complete'
\echo ''
\echo 'Summary of changes:'
\echo '  1. Renamed ag_du_delivery_monthly -> ag_du_demand_monthly (AW_* is demand, not delivery)'
\echo '  2. Created ag_du_sw_delivery_monthly for DN_* surface water delivery data'
\echo '  3. Created ag_du_gw_pumping_monthly for calculated groundwater pumping'
\echo '  4. Updated ag_du_period_summary with new SW delivery and GW pumping columns'
\echo '  5. Clarified ag_du_shortage_monthly comments (GW restriction shortage only)'
\echo ''
\echo 'Next steps:'
\echo '  1. Update ETL code to populate new tables'
\echo '  2. Re-run ETL for all scenarios'
\echo '  3. Update API endpoints'
