-- =============================================================================
-- Migration 15: Fix link table provenance
-- =============================================================================
-- Migration 14 inserted rows into scenario_key_operation_link and
-- scenario_key_assumption_link without DISABLE TRIGGER USER. The audit
-- trigger (set_audit_fields) always overwrites updated_by via
-- coeqwal_current_operator(), so there is no way to override it at INSERT
-- time unless triggers are disabled. Running as superuser (postgres = id 1)
-- caused all 176 rows to be stamped created_by=1, updated_by=1.
--
-- This migration corrects both columns to 2 (jfantauzza) on all rows.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/15_fix_link_table_provenance.sql
-- =============================================================================

BEGIN;

-- ─── Fix scenario_key_operation_link ─────────────────────────────────────────

ALTER TABLE scenario_key_operation_link DISABLE TRIGGER USER;

UPDATE scenario_key_operation_link
SET created_by = 2,
    updated_by = 2;

ALTER TABLE scenario_key_operation_link ENABLE TRIGGER USER;

-- ─── Fix scenario_key_assumption_link ────────────────────────────────────────

ALTER TABLE scenario_key_assumption_link DISABLE TRIGGER USER;

UPDATE scenario_key_assumption_link
SET created_by = 2,
    updated_by = 2;

ALTER TABLE scenario_key_assumption_link ENABLE TRIGGER USER;

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT 'scenario_key_operation_link' AS table_name,
       created_by,
       count(*) AS row_count
FROM scenario_key_operation_link
GROUP BY created_by;

SELECT 'scenario_key_assumption_link' AS table_name,
       created_by,
       count(*) AS row_count
FROM scenario_key_assumption_link
GROUP BY created_by;

COMMIT;
