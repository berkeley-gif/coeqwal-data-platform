-- =============================================================================
-- MIGRATION 47: Create sibling_group table, reorder scenario columns, fix attribution
-- =============================================================================
--
-- 1. Creates sibling_group table with shared operational configuration attributes
--    (name, descriptions, baseline_group, scenario_author_id, model_source_id)
-- 2. Populates from hist_adj scenarios + inactive baselines (s0022, s0038)
-- 3. Recreates scenario table with clean column order, metadata columns last
-- 4. Removes columns moved to sibling_group (name, descriptions, baseline_scenario_id,
--    scenario_author_id, model_source_id)
-- 5. Creates scenario_backup for rollback
-- 6. Fixes created_by/updated_by attribution (requires trigger disable)
--
-- Run as: psql $SUPERUSER_URL -f <this_file>
-- Rollback: DROP TABLE scenario; ALTER TABLE scenario_backup RENAME TO scenario;
--           then re-add FKs, indexes, triggers manually
-- =============================================================================

BEGIN;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: Create sibling_group table
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE sibling_group (
    short_code          VARCHAR PRIMARY KEY,
    name                VARCHAR,
    short_description   TEXT,
    long_description    TEXT,
    baseline_group      VARCHAR,
    scenario_author_id  INTEGER,
    model_source_id     INTEGER,
    created_by          INTEGER NOT NULL DEFAULT 2,
    updated_by          INTEGER NOT NULL DEFAULT 2,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_sibling_group_baseline
        FOREIGN KEY (baseline_group) REFERENCES sibling_group(short_code)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT fk_sibling_group_author
        FOREIGN KEY (scenario_author_id) REFERENCES scenario_author(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_sibling_group_model_source
        FOREIGN KEY (model_source_id) REFERENCES model_source(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE INDEX idx_sibling_group_baseline ON sibling_group (baseline_group);

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: Populate sibling_group — insert with NULL baseline_group first
-- ═══════════════════════════════════════════════════════════════════════════════

-- Insert inactive baselines first (s0022, s0038) — they have no sibling_group
-- but are referenced as baseline_group by other groups
INSERT INTO sibling_group (short_code, name, short_description, long_description,
                           scenario_author_id, model_source_id, created_by, updated_by)
SELECT short_code, name, short_description, long_description,
       scenario_author_id, model_source_id, 2, 2
FROM scenario
WHERE short_code IN ('s0022', 's0038');

-- Insert 24 active hist_adj operational configurations
INSERT INTO sibling_group (short_code, name, short_description, long_description,
                           scenario_author_id, model_source_id, created_by, updated_by)
SELECT s.short_code, s.name, s.short_description, s.long_description,
       s.scenario_author_id, s.model_source_id, 2, 2
FROM scenario s
WHERE s.hydroclimate_id = 2
  AND s.is_active = TRUE
ORDER BY s.short_code;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: Set baseline_group values
-- ═══════════════════════════════════════════════════════════════════════════════

UPDATE sibling_group sg
SET baseline_group = base_sc.short_code
FROM scenario s
JOIN scenario base_sc ON s.baseline_scenario_id = base_sc.id
WHERE s.short_code = sg.short_code
  AND s.hydroclimate_id = 2
  AND s.baseline_scenario_id IS NOT NULL
  AND s.baseline_scenario_id != s.id;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: Apply audit trigger to sibling_group
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TRIGGER audit_fields_sibling_group
    BEFORE INSERT OR UPDATE ON sibling_group
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: Create scenario_backup (exact copy for rollback)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE scenario_backup AS SELECT * FROM scenario;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: Create scenario_new with desired column order
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE scenario_new (
    id                  SERIAL PRIMARY KEY,
    short_code          VARCHAR NOT NULL UNIQUE,
    run_name            VARCHAR,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    hydroclimate_id     INTEGER,
    sibling_group       VARCHAR,
    scenario_version_id INTEGER DEFAULT 1,
    created_by          INTEGER NOT NULL DEFAULT 2,
    updated_by          INTEGER NOT NULL DEFAULT 2,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_scenario_new_sibling_group
        FOREIGN KEY (sibling_group) REFERENCES sibling_group(short_code)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 7: Copy data (triggers disabled so we control attribution)
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO scenario_new (
    id, short_code, run_name, is_active,
    hydroclimate_id, sibling_group, scenario_version_id,
    created_by, updated_by, created_at, updated_at
)
SELECT
    id, short_code, run_name, is_active,
    hydroclimate_id, sibling_group, scenario_version_id,
    2, 2, created_at, NOW()
FROM scenario
ORDER BY short_code;

-- Inactive baselines (s0022, s0038) need sibling_group pointing to themselves
UPDATE scenario_new
SET sibling_group = short_code
WHERE short_code IN ('s0022', 's0038') AND sibling_group IS NULL;

-- Set sequence to correct value
SELECT setval('scenario_new_id_seq', (SELECT MAX(id) FROM scenario_new));

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 8: Drop dependent views and FK constraints
-- ═══════════════════════════════════════════════════════════════════════════════

-- Drop views that depend on the old scenario table
DROP VIEW IF EXISTS scenario_full;

-- Link tables → scenario.id
ALTER TABLE scenario_tag_link DROP CONSTRAINT IF EXISTS scenario_tag_link_scenario_id_fkey;
ALTER TABLE theme_scenario_link DROP CONSTRAINT IF EXISTS theme_scenario_link_scenario_id_fkey;
ALTER TABLE scenario_key_assumption_link DROP CONSTRAINT IF EXISTS scenario_key_assumption_link_scenario_id_fkey;
ALTER TABLE scenario_key_operation_link DROP CONSTRAINT IF EXISTS scenario_key_operation_link_scenario_id_fkey;

-- Self-ref and sibling_group FKs on old scenario table
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_sibling_group;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_scenario_author;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS scenario_model_source_id_fkey;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 9: Swap tables
-- ═══════════════════════════════════════════════════════════════════════════════

DROP TABLE scenario;
ALTER TABLE scenario_new RENAME TO scenario;
ALTER SEQUENCE scenario_new_id_seq RENAME TO scenario_id_seq;

-- Fix constraint names left over from scenario_new
ALTER TABLE scenario RENAME CONSTRAINT scenario_new_pkey TO scenario_pkey;
ALTER TABLE scenario RENAME CONSTRAINT scenario_new_short_code_key TO scenario_short_code_key;
ALTER TABLE scenario RENAME CONSTRAINT fk_scenario_new_sibling_group TO fk_scenario_sibling_group;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 10: Re-add FK constraints from link tables
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE scenario_tag_link
    ADD CONSTRAINT scenario_tag_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE theme_scenario_link
    ADD CONSTRAINT theme_scenario_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_assumption_link
    ADD CONSTRAINT scenario_key_assumption_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_operation_link
    ADD CONSTRAINT scenario_key_operation_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 11: Re-add indexes
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_scenario_run_name_active ON scenario (run_name, is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_active ON scenario (is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_hydroclimate ON scenario (hydroclimate_id);
CREATE INDEX IF NOT EXISTS idx_scenario_active_version ON scenario (is_active, scenario_version_id);
CREATE INDEX IF NOT EXISTS idx_scenario_sibling_group ON scenario (sibling_group);

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 12: Apply audit trigger
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TRIGGER audit_fields_scenario
    BEFORE INSERT OR UPDATE ON scenario
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 13: Recreate scenario_full view (now joins sibling_group for name/author)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW scenario_full AS
SELECT
    s.id,
    s.short_code,
    s.run_name,
    sg.name,
    s.is_active,
    sa.short_code AS author,
    h.short_code  AS hydroclimate,
    MAX(CASE WHEN oc.short_code = 'biops'              THEN od.short_code END) AS biops,
    MAX(CASE WHEN oc.short_code = 'tucp'               THEN od.short_code END) AS tucp,
    MAX(CASE WHEN oc.short_code = 'gw_restrictions'    THEN od.short_code END) AS gw_restrictions,
    MAX(CASE WHEN oc.short_code = 'infrastructure'     THEN od.short_code END) AS infrastructure,
    MAX(CASE WHEN oc.short_code = 'flow'               THEN od.short_code END) AS flow,
    MAX(CASE WHEN oc.short_code = 'delta_outflow'      THEN od.short_code END) AS delta_outflow,
    MAX(CASE WHEN oc.short_code = 'comm_delivery'      THEN od.short_code END) AS comm_delivery,
    MAX(CASE WHEN oc.short_code = 'regulatory_salinity' THEN od.short_code END) AS regulatory_salinity,
    MAX(CASE WHEN oc.short_code = 'carryover'          THEN od.short_code END) AS carryover,
    MAX(CASE WHEN ac.short_code = 'land_use'           THEN ad.short_code END) AS land_use,
    MAX(CASE WHEN ac.short_code = 'gw_model'           THEN ad.short_code END) AS gw_model
FROM scenario s
LEFT JOIN sibling_group sg ON s.sibling_group = sg.short_code
LEFT JOIN scenario_author sa ON sg.scenario_author_id = sa.id
LEFT JOIN hydroclimate h ON s.hydroclimate_id = h.id
LEFT JOIN scenario_key_operation_link skol ON s.id = skol.scenario_id
LEFT JOIN operation_definition od ON skol.operation_id = od.id
LEFT JOIN operation_category oc ON od.operation_category_id = oc.id
LEFT JOIN scenario_key_assumption_link skal ON s.id = skal.scenario_id
LEFT JOIN assumption_definition ad ON skal.assumption_id = ad.id
LEFT JOIN assumption_category ac ON ad.assumption_category_id = ac.id
WHERE s.is_active = TRUE
GROUP BY s.id, s.short_code, s.run_name, sg.name, s.is_active,
         sa.short_code, h.short_code;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 14: Register sibling_group in domain_family_map
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO domain_family_map (table_name, version_family_id, created_by, updated_by)
SELECT 'sibling_group', vf.id, 2, 2
FROM version_family vf
WHERE vf.short_code = 'scenario'
  AND NOT EXISTS (
      SELECT 1 FROM domain_family_map WHERE table_name = 'sibling_group'
  );

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. sibling_group table
SELECT 'sibling_group rows' AS check, COUNT(*) AS count FROM sibling_group;

SELECT short_code, baseline_group, scenario_author_id
FROM sibling_group
ORDER BY short_code;

-- 2. scenario table column order
SELECT column_name, ordinal_position
FROM information_schema.columns
WHERE table_name = 'scenario' AND table_schema = 'public'
ORDER BY ordinal_position;

-- 3. scenario record count
SELECT 'scenario rows' AS check, COUNT(*) AS total,
       COUNT(*) FILTER (WHERE is_active) AS active,
       COUNT(*) FILTER (WHERE NOT is_active) AS inactive
FROM scenario;

-- 4. Attribution check
SELECT 'attribution' AS check,
       COUNT(*) FILTER (WHERE created_by != 2) AS bad_created_by,
       COUNT(*) FILTER (WHERE updated_by != 2) AS bad_updated_by
FROM scenario;

-- 5. Sibling groups with member counts
SELECT sibling_group, COUNT(*) AS members
FROM scenario
WHERE sibling_group IS NOT NULL
GROUP BY sibling_group
ORDER BY sibling_group;

-- 6. FK integrity — no orphaned link table rows
SELECT 'orphaned scenario_tag_link' AS check,
       COUNT(*) AS orphans
FROM scenario_tag_link stl
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = stl.scenario_id);

SELECT 'orphaned theme_scenario_link' AS check,
       COUNT(*) AS orphans
FROM theme_scenario_link tsl
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = tsl.scenario_id);

-- 7. Backup exists
SELECT 'scenario_backup rows' AS check, COUNT(*) AS count FROM scenario_backup;

COMMIT;
