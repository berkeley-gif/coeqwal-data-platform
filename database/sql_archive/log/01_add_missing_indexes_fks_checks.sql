-- ============================================================================
-- MIGRATION: Add missing indexes, FK constraints, and CHECK constraints
-- ============================================================================
-- Discovered via: python database/audit/verify_erd_against_audit.py
-- Index strategy informed by: analysis of api/coeqwal-api/routes/
-- Safe to run multiple times (CREATE INDEX IF NOT EXISTS; DO $$ guards).
-- Requires the superuser (postgres)  - DDL on tables you don't own will fail otherwise.
-- Run as:
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/01_add_missing_indexes_fks_checks.sql
-- ============================================================================

\echo ''
\echo '============================================================'
\echo ' MIGRATION: indexes, FK constraints, CHECK constraints'
\echo '============================================================'
\echo ''

-- ============================================================================
-- PART 1A: ERD-documented indexes that were never created
-- ============================================================================

\echo 'Part 1A: Creating ERD-documented indexes...'

CREATE INDEX IF NOT EXISTS idx_theme_short_code_active         ON theme (short_code, is_active);
CREATE INDEX IF NOT EXISTS idx_theme_active                    ON theme (is_active);

CREATE INDEX IF NOT EXISTS idx_theme_scenario_reverse          ON theme_scenario_link (scenario_id, theme_id);

CREATE INDEX IF NOT EXISTS idx_scenario_author_active          ON scenario_author (is_active, short_code);

CREATE INDEX IF NOT EXISTS idx_assumption_definition_category  ON assumption_definition (category, short_code);
CREATE INDEX IF NOT EXISTS idx_assumption_definition_active    ON assumption_definition (is_active);

CREATE INDEX IF NOT EXISTS idx_scenario_assumption_reverse     ON scenario_key_assumption_link (assumption_id, scenario_id);

CREATE INDEX IF NOT EXISTS idx_operation_definition_category   ON operation_definition (category, short_code);
CREATE INDEX IF NOT EXISTS idx_operation_definition_active     ON operation_definition (is_active);

CREATE INDEX IF NOT EXISTS idx_scenario_operation_reverse      ON scenario_key_operation_link (operation_id, scenario_id);

CREATE INDEX IF NOT EXISTS idx_hydroclimate_active             ON hydroclimate (is_active, short_code);
CREATE INDEX IF NOT EXISTS idx_hydroclimate_source             ON hydroclimate (source_id);

CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_reservoir  ON reservoir_monthly_percentile (reservoir_entity_id);
CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_combined   ON reservoir_monthly_percentile (scenario_short_code, reservoir_entity_id);

CREATE INDEX IF NOT EXISTS idx_period_summary_dead_prob        ON reservoir_period_summary (dead_pool_prob_all DESC);

CREATE INDEX IF NOT EXISTS idx_cws_agg_monthly_scenario        ON cws_aggregate_monthly (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_cws_agg_monthly_aggregate       ON cws_aggregate_monthly (cws_aggregate_id);
CREATE INDEX IF NOT EXISTS idx_cws_agg_monthly_combined        ON cws_aggregate_monthly (scenario_short_code, cws_aggregate_id);

CREATE INDEX IF NOT EXISTS idx_cws_agg_period_scenario         ON cws_aggregate_period_summary (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_cws_agg_period_aggregate        ON cws_aggregate_period_summary (cws_aggregate_id);

CREATE INDEX IF NOT EXISTS idx_scenario_short_code_active      ON scenario (short_code, is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_active                 ON scenario (is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_baseline               ON scenario (baseline_scenario_id);
CREATE INDEX IF NOT EXISTS idx_scenario_hydroclimate           ON scenario (hydroclimate_id);
CREATE INDEX IF NOT EXISTS idx_scenario_active_version         ON scenario (is_active, scenario_version_id);

CREATE UNIQUE INDEX IF NOT EXISTS tier_location_result_unique
    ON tier_location_result (scenario_short_code, tier_short_code, location_id, tier_version_id);

\echo 'Part 1A: Done.'
\echo ''

-- ============================================================================
-- PART 1B: High-value composite indexes from API query analysis
-- ============================================================================
-- These tables are the most-queried in the API. Every endpoint filters by
-- constantly. The tradeoff strongly favors indexing.
-- ============================================================================

\echo 'Part 1B: Creating API-informed composite indexes...'

CREATE INDEX IF NOT EXISTS idx_du_delivery_scenario_du         ON du_delivery_monthly (scenario_short_code, du_id);
CREATE INDEX IF NOT EXISTS idx_du_shortage_scenario_du         ON du_shortage_monthly (scenario_short_code, du_id);
CREATE INDEX IF NOT EXISTS idx_du_period_scenario_du           ON du_period_summary (scenario_short_code, du_id);

CREATE INDEX IF NOT EXISTS idx_ag_du_delivery_scenario_du      ON ag_du_delivery_monthly (scenario_short_code, du_id);
CREATE INDEX IF NOT EXISTS idx_ag_du_shortage_scenario_du      ON ag_du_shortage_monthly (scenario_short_code, du_id);
CREATE INDEX IF NOT EXISTS idx_ag_du_period_scenario_du        ON ag_du_period_summary (scenario_short_code, du_id);

CREATE INDEX IF NOT EXISTS idx_ag_agg_monthly_scenario         ON ag_aggregate_monthly (scenario_short_code, aggregate_code);
CREATE INDEX IF NOT EXISTS idx_ag_agg_period_scenario          ON ag_aggregate_period_summary (scenario_short_code, aggregate_code);

CREATE INDEX IF NOT EXISTS idx_mi_delivery_scenario_contractor ON mi_delivery_monthly (scenario_short_code, mi_contractor_code);
CREATE INDEX IF NOT EXISTS idx_mi_shortage_scenario_contractor ON mi_shortage_monthly (scenario_short_code, mi_contractor_code);
CREATE INDEX IF NOT EXISTS idx_mi_period_scenario_contractor   ON mi_contractor_period_summary (scenario_short_code, mi_contractor_code);

CREATE INDEX IF NOT EXISTS idx_reservoir_storage_scenario      ON reservoir_storage_monthly (scenario_short_code, reservoir_entity_id);
CREATE INDEX IF NOT EXISTS idx_reservoir_spill_scenario        ON reservoir_spill_monthly (scenario_short_code, reservoir_entity_id);

CREATE INDEX IF NOT EXISTS idx_network_arc_from_node           ON network_arc (from_node);
CREATE INDEX IF NOT EXISTS idx_network_arc_to_node             ON network_arc (to_node);

CREATE INDEX IF NOT EXISTS idx_network_gis_geom                ON network_gis USING GIST (geom);

\echo 'Part 1B: Done.'
\echo ''

-- ============================================================================
-- PART 2: mi_contractor FK  - NOT ENFORCEABLE (documented, intentional)
-- ============================================================================
-- mi_contractor_code contains both individual contractor codes AND aggregate
-- The ERD has been updated to document this mixed-use column.
-- ============================================================================

\echo 'Part 2: mi_contractor FK skipped (mixed individual+aggregate codes  - see ERD note).'
\echo ''

-- ============================================================================
-- PART 3: Missing CHECK constraints  - water_month BETWEEN 1 AND 12
-- ============================================================================

\echo 'Part 3: Adding water_month CHECK constraints...'

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ag_agg_monthly_water_month') THEN
        ALTER TABLE ag_aggregate_monthly ADD CONSTRAINT chk_ag_agg_monthly_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ag_du_delivery_water_month') THEN
        ALTER TABLE ag_du_delivery_monthly ADD CONSTRAINT chk_ag_du_delivery_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ag_du_shortage_water_month') THEN
        ALTER TABLE ag_du_shortage_monthly ADD CONSTRAINT chk_ag_du_shortage_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_cws_agg_monthly_water_month') THEN
        ALTER TABLE cws_aggregate_monthly ADD CONSTRAINT chk_cws_agg_monthly_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_du_delivery_water_month') THEN
        ALTER TABLE du_delivery_monthly ADD CONSTRAINT chk_du_delivery_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_du_shortage_water_month') THEN
        ALTER TABLE du_shortage_monthly ADD CONSTRAINT chk_du_shortage_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_mi_delivery_water_month') THEN
        ALTER TABLE mi_delivery_monthly ADD CONSTRAINT chk_mi_delivery_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_mi_shortage_water_month') THEN
        ALTER TABLE mi_shortage_monthly ADD CONSTRAINT chk_mi_shortage_water_month CHECK (water_month BETWEEN 1 AND 12);
    END IF;
    RAISE NOTICE 'water_month CHECK constraints applied';
END $$;

\echo 'Part 3: Done.'
\echo ''
\echo '============================================================'
\echo ' MIGRATION COMPLETE'
\echo ' Re-run the audit and verify:'
\echo '   DATABASE_URL="postgresql://postgres:..." bash database/run_audit.sh'
\echo '   python database/audit/verify_erd_against_audit.py database/schema/COEQWAL_SCENARIOS_DB_ERD.md audits/latest.json'
\echo '============================================================'
