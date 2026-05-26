-- =============================================================================
-- Migration 09: Add source column to operation_definition and slr
-- =============================================================================
-- assumption_definition already has source TEXT (matches source lookup table).
-- operation_definition and slr were created without it.
-- All current rows sourced from james_gilbert.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/09_add_source_to_operation_definition_and_slr.sql
-- =============================================================================

-- ─── 1. Add source column ────────────────────────────────────────────────────

ALTER TABLE operation_definition ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE slr                  ADD COLUMN IF NOT EXISTS source TEXT;

-- ─── 2. Populate source = 'james_gilbert' for all current rows ───────────────
-- DISABLE TRIGGER USER prevents the audit trigger from overriding updated_by
-- with the postgres superuser role when running via $SUPERUSER_URL.

ALTER TABLE operation_definition DISABLE TRIGGER USER;

UPDATE operation_definition
SET source = 'james_gilbert',
    updated_by = 2,
    updated_at = NOW()
WHERE source IS NULL;

ALTER TABLE operation_definition ENABLE TRIGGER USER;

ALTER TABLE slr DISABLE TRIGGER USER;

UPDATE slr
SET source = 'james_gilbert',
    updated_by = 2,
    updated_at = NOW()
WHERE source IS NULL;

ALTER TABLE slr ENABLE TRIGGER USER;

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT 'operation_definition' AS table_name, short_code, source
FROM operation_definition
ORDER BY category, short_code;

SELECT 'slr' AS table_name, short_code, source
FROM slr
ORDER BY slr_value_mm;
