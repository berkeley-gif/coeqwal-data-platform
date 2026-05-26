-- =============================================================================
-- Migration 16: Fix scenario provenance + add scenario_author FK
-- =============================================================================
-- Two issues addressed:
--
-- 1. scenario and scenario_author rows were inserted without DISABLE TRIGGER
--    USER, so the audit trigger stamped created_by=1 (postgres) on all rows.
--    This migration corrects created_by and updated_by to 2 (jfantauzza).
--
-- 2. scenario.scenario_author_id has no FK constraint to scenario_author.id,
--    even though the ERD documents it as a required reference. The FK is added
--    here with ON DELETE RESTRICT / ON UPDATE CASCADE, consistent with all
--    other provenance and lookup FK rules in the schema.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/16_fix_scenario_provenance_and_author_fk.sql
-- =============================================================================

BEGIN;

-- ─── 1. Fix scenario provenance ──────────────────────────────────────────────

ALTER TABLE scenario DISABLE TRIGGER USER;

UPDATE scenario
SET created_by = 2,
    updated_by = 2,
    updated_at = NOW();

ALTER TABLE scenario ENABLE TRIGGER USER;

-- ─── 2. Fix scenario_author provenance ───────────────────────────────────────

ALTER TABLE scenario_author DISABLE TRIGGER USER;

UPDATE scenario_author
SET created_by = 2,
    updated_by = 2,
    updated_at = NOW();

ALTER TABLE scenario_author ENABLE TRIGGER USER;

-- ─── 3. Add FK: scenario.scenario_author_id → scenario_author.id ─────────────

ALTER TABLE scenario
    ADD CONSTRAINT fk_scenario_scenario_author
    FOREIGN KEY (scenario_author_id)
    REFERENCES scenario_author(id)
    ON DELETE RESTRICT
    ON UPDATE CASCADE;

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT 'scenario' AS table_name, created_by, count(*) AS row_count
FROM scenario GROUP BY created_by;

SELECT 'scenario_author' AS table_name, created_by, count(*) AS row_count
FROM scenario_author GROUP BY created_by;

SELECT conname, contype
FROM pg_constraint
WHERE conname = 'fk_scenario_scenario_author';

COMMIT;
