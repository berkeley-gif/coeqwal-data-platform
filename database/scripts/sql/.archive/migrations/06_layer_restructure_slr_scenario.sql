-- =============================================================================
-- Migration 06: Layer architecture restructure — SLR table + scenario columns
-- =============================================================================
-- Requires: superuser (for CREATE TABLE, ALTER TABLE, trigger creation)
--           Set audit context to developer id=2 (jfantauzza) before running.
--
-- What this migration does:
--   1. Creates the slr (sea level rise) lookup table in Layer 07
--   2. Attaches the audit trigger to slr
--   3. Seeds the slr table (none / 15mm / 30mm / 60mm)
--   4. Adds source_scenario_id and slr_id columns to scenario
--   5. Sets slr_id = slr.id WHERE short_code = 'none' for all existing scenarios
--   6. Drops slr_value and slr_unit_id from hydroclimate (SLR moved to slr table)
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/06_layer_restructure_slr_scenario.sql
--
-- After running, populate source_scenario_id for each scenario using the
-- seed CSV at seed_tables/06_scenario/scenario.csv (see Phase 4 seed work).
-- =============================================================================

BEGIN;

-- ─── 1. Create slr table ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS slr (
    id              SERIAL PRIMARY KEY,
    short_code      TEXT UNIQUE NOT NULL,
    label           TEXT NOT NULL,
    slr_value_mm    NUMERIC,
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by      INTEGER NOT NULL REFERENCES developer(id)
                        ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by      INTEGER NOT NULL REFERENCES developer(id)
                        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Unique and performance indexes
CREATE UNIQUE INDEX IF NOT EXISTS slr_short_code_key       ON slr (short_code);
CREATE        INDEX IF NOT EXISTS idx_slr_active           ON slr (is_active);

-- ─── 2. Attach audit trigger to slr ───────────────────────────────────────

CREATE TRIGGER set_slr_audit_fields
    BEFORE INSERT OR UPDATE ON slr
    FOR EACH ROW
    EXECUTE FUNCTION set_audit_fields();

-- ─── 3. Seed slr values ───────────────────────────────────────────────────
-- Run the INSERT as developer id=2 so audit fields are attributed correctly.
-- Use DISABLE TRIGGER USER pattern if audit trigger sets created_by automatically
-- and you need to override; otherwise INSERT directly.

INSERT INTO slr (short_code, label, slr_value_mm, description, is_active, created_by, updated_by)
VALUES
    ('none',   'No sea level rise',  0,  'Baseline — no sea level rise applied',         TRUE, 2, 2),
    ('slr_15', '15mm sea level rise', 15, '15mm sea level rise scenario',                TRUE, 2, 2),
    ('slr_30', '30mm sea level rise', 30, '30mm sea level rise scenario',                TRUE, 2, 2),
    ('slr_60', '60mm sea level rise', 60, '60mm sea level rise scenario',                TRUE, 2, 2)
ON CONFLICT (short_code) DO NOTHING;

-- ─── 4. Add columns to scenario ───────────────────────────────────────────

-- source_scenario_id: the scenario this was derived/run from
ALTER TABLE scenario
    ADD COLUMN IF NOT EXISTS source_scenario_id INTEGER
        REFERENCES scenario(id) ON DELETE SET NULL ON UPDATE CASCADE;

-- slr_id: the sea level rise condition applied to this scenario
ALTER TABLE scenario
    ADD COLUMN IF NOT EXISTS slr_id INTEGER
        REFERENCES slr(id) ON DELETE RESTRICT ON UPDATE CASCADE;

-- Indexes for the new FKs
CREATE INDEX IF NOT EXISTS idx_scenario_source_scenario ON scenario (source_scenario_id);
CREATE INDEX IF NOT EXISTS idx_scenario_slr             ON scenario (slr_id);

-- ─── 5. Set slr_id = 'none' for all existing scenarios ───────────────────
-- All scenarios run to date use no sea level rise.

UPDATE scenario
SET slr_id = (SELECT id FROM slr WHERE short_code = 'none')
WHERE slr_id IS NULL;

-- ─── 6. Grant application user access to slr ─────────────────────────────

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE slr TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE slr_id_seq TO jfantauzza;

-- ─── 7. Drop stale slr columns from hydroclimate ─────────────────────────
-- SLR is now modelled in scenario.slr_id → slr.id.
-- The hydroclimate table described hydro boundary conditions, not SLR policy.

ALTER TABLE hydroclimate
    DROP COLUMN IF EXISTS slr_value,
    DROP COLUMN IF EXISTS slr_unit_id;

COMMIT;

-- ─── Verify ───────────────────────────────────────────────────────────────

SELECT 'slr rows' AS check_name, count(*) AS count FROM slr;

SELECT 'scenario.slr_id NULLs remaining' AS check_name,
       count(*) AS count
FROM scenario
WHERE slr_id IS NULL;

SELECT 'hydroclimate columns' AS check_name,
       column_name
FROM information_schema.columns
WHERE table_name = 'hydroclimate'
  AND column_name IN ('slr_value', 'slr_unit_id');
