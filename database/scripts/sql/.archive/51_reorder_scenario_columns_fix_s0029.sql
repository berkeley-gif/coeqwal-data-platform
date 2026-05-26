-- =============================================================================
-- MIGRATION 51: Reorder scenario columns, fix s0029 author/model
-- =============================================================================
-- Moves scenario_author_id and model_source_id before the metadata columns.
-- Sets s0029 scenario_author_id=3, model_source_id=1.
--
-- Run as: psql $SUPERUSER_URL -f <this_file>
-- =============================================================================

BEGIN;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: Fix s0029 attributes first (on current table)
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE scenario DISABLE TRIGGER audit_fields_scenario;

UPDATE scenario
SET scenario_author_id = 3, model_source_id = 1, updated_by = 2
WHERE short_code = 's0029';

ALTER TABLE scenario ENABLE TRIGGER audit_fields_scenario;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: Drop old backup, create fresh one
-- ═══════════════════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS scenario_backup;
CREATE TABLE scenario_backup AS SELECT * FROM scenario;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: Drop dependent objects
-- ═══════════════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS scenario_full;

ALTER TABLE scenario_tag_link DROP CONSTRAINT IF EXISTS scenario_tag_link_scenario_id_fkey;
ALTER TABLE theme_scenario_link DROP CONSTRAINT IF EXISTS theme_scenario_link_scenario_id_fkey;
ALTER TABLE scenario_key_assumption_link DROP CONSTRAINT IF EXISTS scenario_key_assumption_link_scenario_id_fkey;
ALTER TABLE scenario_key_operation_link DROP CONSTRAINT IF EXISTS scenario_key_operation_link_scenario_id_fkey;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_hydro_sibling;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_scenario_author;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_model_source;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: Create scenario_new with desired column order
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE scenario_new (
    id                   SERIAL PRIMARY KEY,
    short_code           VARCHAR NOT NULL UNIQUE,
    run_name             VARCHAR,
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    hydroclimate_id      INTEGER,
    hydroclimate_sibling VARCHAR,
    scenario_version_id  INTEGER DEFAULT 1,
    scenario_author_id   INTEGER,
    model_source_id      INTEGER,
    created_by           INTEGER NOT NULL DEFAULT 2,
    updated_by           INTEGER NOT NULL DEFAULT 2,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_scenario_hydro_sibling
        FOREIGN KEY (hydroclimate_sibling) REFERENCES scenario_hydroclimate_sibling(short_code)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT fk_scenario_scenario_author
        FOREIGN KEY (scenario_author_id) REFERENCES scenario_author(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_scenario_model_source
        FOREIGN KEY (model_source_id) REFERENCES model_source(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: Copy data
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO scenario_new (
    id, short_code, run_name, is_active,
    hydroclimate_id, hydroclimate_sibling, scenario_version_id,
    scenario_author_id, model_source_id,
    created_by, updated_by, created_at, updated_at
)
SELECT
    id, short_code, run_name, is_active,
    hydroclimate_id, hydroclimate_sibling, scenario_version_id,
    scenario_author_id, model_source_id,
    created_by, updated_by, created_at, updated_at
FROM scenario
ORDER BY short_code;

SELECT setval('scenario_new_id_seq', (SELECT MAX(id) FROM scenario_new));

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: Swap tables
-- ═══════════════════════════════════════════════════════════════════════════════

DROP TABLE scenario;
ALTER TABLE scenario_new RENAME TO scenario;
ALTER SEQUENCE scenario_new_id_seq RENAME TO scenario_id_seq;

ALTER TABLE scenario RENAME CONSTRAINT scenario_new_pkey TO scenario_pkey;
ALTER TABLE scenario RENAME CONSTRAINT scenario_new_short_code_key TO scenario_short_code_key;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 7: Re-add FK constraints from link tables
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
-- STEP 8: Re-add indexes
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_scenario_run_name_active ON scenario (run_name, is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_active ON scenario (is_active);
CREATE INDEX IF NOT EXISTS idx_scenario_hydroclimate ON scenario (hydroclimate_id);
CREATE INDEX IF NOT EXISTS idx_scenario_active_version ON scenario (is_active, scenario_version_id);
CREATE INDEX IF NOT EXISTS idx_scenario_hydro_sibling ON scenario (hydroclimate_sibling);

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 9: Apply audit trigger
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TRIGGER audit_fields_scenario
    BEFORE INSERT OR UPDATE ON scenario
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 10: Recreate scenario_full view
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
LEFT JOIN scenario_hydroclimate_sibling sg ON s.hydroclimate_sibling = sg.short_code
LEFT JOIN scenario_author sa ON s.scenario_author_id = sa.id
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

-- Grant permissions
GRANT ALL ON TABLE scenario TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE scenario_id_seq TO jfantauzza;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT column_name, ordinal_position
FROM information_schema.columns
WHERE table_name = 'scenario' AND table_schema = 'public'
ORDER BY ordinal_position;

SELECT short_code, scenario_author_id, model_source_id
FROM scenario WHERE short_code = 's0029';

SELECT 'scenario rows' AS check, COUNT(*) AS total,
       COUNT(*) FILTER (WHERE is_active) AS active
FROM scenario;

SELECT 'view works' AS check, COUNT(*) AS rows FROM scenario_full;

COMMIT;
