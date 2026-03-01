-- ============================================================================
-- MIGRATION: Add missing indexes, FK constraints, and CHECK constraints
-- ============================================================================
-- Adds structural elements documented in the ERD but absent from the database.
-- Discovered via: python database/audit/verify_erd_against_audit.py
-- Safe to run multiple times (all statements use IF NOT EXISTS or equivalent).
-- Run as: psql $DATABASE_URL -f database/scripts/sql/migrations/01_add_missing_indexes_fks_checks.sql
-- ============================================================================

\echo ''
\echo '============================================================'
\echo ' MIGRATION: indexes, FK constraints, CHECK constraints'
\echo '============================================================'
\echo ''

-- ============================================================================
-- PART 1: Missing indexes
-- ============================================================================
-- These indexes are documented in the ERD but were never created in the DB.
-- They support anticipated API query patterns.
-- Cost: each index slows bulk INSERT/UPDATE slightly but speeds up SELECT queries.
-- ============================================================================

\echo 'Part 1: Creating missing indexes...'

-- theme
CREATE INDEX IF NOT EXISTS idx_theme_short_code_active ON theme (short_code, is_active);
CREATE INDEX IF NOT EXISTS idx_theme_active ON theme (is_active);

-- theme_scenario_link
CREATE INDEX IF NOT EXISTS idx_theme_scenario_reverse ON theme_scenario_link (scenario_id, theme_id);

-- scenario_author
CREATE INDEX IF NOT EXISTS idx_scenario_author_active ON scenario_author (is_active, short_code);

-- assumption_definition
CREATE INDEX IF NOT EXISTS idx_assumption_definition_category ON assumption_definition (category, short_code);
CREATE INDEX IF NOT EXISTS idx_assumption_definition_active ON assumption_definition (is_active);

-- scenario_key_assumption_link
CREATE INDEX IF NOT EXISTS idx_scenario_assumption_reverse ON scenario_key_assumption_link (assumption_id, scenario_id);

-- operation_definition
CREATE INDEX IF NOT EXISTS idx_operation_definition_category ON operation_definition (category, short_code);
CREATE INDEX IF NOT EXISTS idx_operation_definition_active ON operation_definition (is_active);

-- scenario_key_operation_link
CREATE INDEX IF NOT EXISTS idx_scenario_operation_reverse ON scenario_key_operation_link (operation_id, scenario_id);

-- hydroclimate
CREATE INDEX IF NOT EXISTS idx_hydroclimate_active ON hydroclimate (is_active, short_code);
CREATE INDEX IF NOT EXISTS idx_hydroclimate_source ON hydroclimate (source_id);

-- reservoir_monthly_percentile
CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_reservoir ON reservoir_monthly_percentile (reservoir_entity_id);
CREATE INDEX IF NOT EXISTS idx_reservoir_percentile_combined ON reservoir_monthly_percentile (scenario_short_code, reservoir_entity_id);

-- reservoir_period_summary
CREATE INDEX IF NOT EXISTS idx_period_summary_dead_prob ON reservoir_period_summary (dead_pool_prob_all DESC);

-- cws_aggregate_monthly
CREATE INDEX IF NOT EXISTS idx_cws_agg_monthly_scenario ON cws_aggregate_monthly (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_cws_agg_monthly_aggregate ON cws_aggregate_monthly (cws_aggregate_id);
CREATE INDEX IF NOT EXISTS idx_cws_agg_monthly_combined ON cws_aggregate_monthly (scenario_short_code, cws_aggregate_id);

-- cws_aggregate_period_summary
CREATE INDEX IF NOT EXISTS idx_cws_agg_period_scenario ON cws_aggregate_period_summary (scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_cws_agg_period_aggregate ON cws_aggregate_period_summary (cws_aggregate_id);

-- tier_location_result (unique index — doubles as a constraint)
CREATE UNIQUE INDEX IF NOT EXISTS tier_location_result_unique
    ON tier_location_result (scenario_short_code, tier_short_code, location_id, tier_version_id);

-- scenario indexes (were in the ERD but absent from DB)
CREATE INDEX IF NOT EXISTS idx_scenario_short_code_active ON scenario (short_code, is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_active ON scenario (is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_baseline ON scenario (baseline_scenario_id);
CREATE INDEX IF NOT EXISTS idx_scenario_hydroclimate ON scenario (hydroclimate_id);
CREATE INDEX IF NOT EXISTS idx_scenario_active_version ON scenario (is_active, scenario_version_id);

\echo 'Part 1: Done.'
\echo ''

-- ============================================================================
-- PART 2: Missing FK constraints — mi_contractor.short_code
-- ============================================================================
-- mi_contractor_code is a denormalized TEXT column in three tables.
-- The ERD documents it as a FK to mi_contractor.short_code (UNIQUE NOT NULL).
-- Adding enforcement here ensures referential integrity.
-- ============================================================================

\echo 'Part 2: Adding mi_contractor FK constraints...'

ALTER TABLE mi_delivery_monthly
    ADD CONSTRAINT IF NOT EXISTS fk_mi_delivery_contractor
    FOREIGN KEY (mi_contractor_code) REFERENCES mi_contractor (short_code)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE mi_shortage_monthly
    ADD CONSTRAINT IF NOT EXISTS fk_mi_shortage_contractor
    FOREIGN KEY (mi_contractor_code) REFERENCES mi_contractor (short_code)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE mi_contractor_period_summary
    ADD CONSTRAINT IF NOT EXISTS fk_mi_period_contractor
    FOREIGN KEY (mi_contractor_code) REFERENCES mi_contractor (short_code)
    ON DELETE RESTRICT ON UPDATE CASCADE;

\echo 'Part 2: Done.'
\echo ''

-- ============================================================================
-- PART 3: Missing CHECK constraints — water_month BETWEEN 1 AND 12
-- ============================================================================
-- These tables have a water_month column that should always be 1–12.
-- The constraints are documented in the ERD but were never added to the DB.
-- ============================================================================

\echo 'Part 3: Adding water_month CHECK constraints...'

ALTER TABLE ag_aggregate_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_ag_agg_monthly_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE ag_du_delivery_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_ag_du_delivery_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE ag_du_shortage_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_ag_du_shortage_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE cws_aggregate_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_cws_agg_monthly_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE du_delivery_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_du_delivery_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE du_shortage_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_du_shortage_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE mi_delivery_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_mi_delivery_water_month
    CHECK (water_month BETWEEN 1 AND 12);

ALTER TABLE mi_shortage_monthly
    ADD CONSTRAINT IF NOT EXISTS chk_mi_shortage_water_month
    CHECK (water_month BETWEEN 1 AND 12);

\echo 'Part 3: Done.'
\echo ''
\echo '============================================================'
\echo ' MIGRATION COMPLETE'
\echo ' Re-run the ERD verification to confirm:'
\echo '   python database/audit/verify_erd_against_audit.py database/schema/COEQWAL_SCENARIOS_DB_ERD.md audits/latest.json'
\echo '============================================================'
