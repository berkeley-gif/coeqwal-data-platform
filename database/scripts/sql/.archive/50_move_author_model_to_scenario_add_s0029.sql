-- =============================================================================
-- MIGRATION 50: Move scenario_author_id/model_source_id back to scenario,
--               add s0029 to scenario_hydroclimate_sibling
-- =============================================================================
-- Run as: psql $SUPERUSER_URL -f <this_file>
-- =============================================================================

BEGIN;

-- Disable triggers so we control attribution
ALTER TABLE scenario DISABLE TRIGGER audit_fields_scenario;
ALTER TABLE scenario_hydroclimate_sibling DISABLE TRIGGER audit_fields_scenario_hydroclimate_sibling;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: Add columns to scenario table
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE scenario ADD COLUMN scenario_author_id INTEGER;
ALTER TABLE scenario ADD COLUMN model_source_id INTEGER;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: Populate from scenario_hydroclimate_sibling
-- ═══════════════════════════════════════════════════════════════════════════════

UPDATE scenario s
SET scenario_author_id = sg.scenario_author_id,
    model_source_id = sg.model_source_id,
    updated_by = 2
FROM scenario_hydroclimate_sibling sg
WHERE s.hydroclimate_sibling = sg.short_code;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: Add FK constraints on scenario
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE scenario
    ADD CONSTRAINT fk_scenario_scenario_author
    FOREIGN KEY (scenario_author_id) REFERENCES scenario_author(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE scenario
    ADD CONSTRAINT fk_scenario_model_source
    FOREIGN KEY (model_source_id) REFERENCES model_source(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: Drop view (depends on columns we're about to remove), then drop columns
-- ═══════════════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS scenario_full;

ALTER TABLE scenario_hydroclimate_sibling DROP CONSTRAINT IF EXISTS fk_hydro_sibling_author;
ALTER TABLE scenario_hydroclimate_sibling DROP CONSTRAINT IF EXISTS fk_hydro_sibling_model_source;
ALTER TABLE scenario_hydroclimate_sibling DROP COLUMN scenario_author_id;
ALTER TABLE scenario_hydroclimate_sibling DROP COLUMN model_source_id;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: Add s0029 to scenario_hydroclimate_sibling
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO scenario_hydroclimate_sibling (
    short_code, name, short_description, long_description, baseline_group,
    created_by, updated_by
) VALUES (
    's0029',
    'Functional environmental flows (v1)',
    'Functional flows scenario - requirements on tribs and Delta; with 2020 land use',
    'This scenario sets new minimum flow requirements on tributaries to the Sacramento and San Joaquin rivers as well as the mainstem of those rivers plus Delta outflow. This scenario (s0029) is the same as s0018 except that the land use inputs are updated to reflect the 2020 LandIQ dataset in this study.',
    's0020',
    2, 2
);

-- Point the s0029 scenario row to its sibling group
UPDATE scenario
SET hydroclimate_sibling = 's0029', updated_by = 2
WHERE short_code = 's0029';

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: Recreate scenario_full view (author join now on scenario)
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

-- Re-enable triggers
ALTER TABLE scenario ENABLE TRIGGER audit_fields_scenario;
ALTER TABLE scenario_hydroclimate_sibling ENABLE TRIGGER audit_fields_scenario_hydroclimate_sibling;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

-- scenario_author_id populated
SELECT 'scenario author populated' AS check,
       COUNT(*) FILTER (WHERE scenario_author_id IS NULL AND is_active) AS missing_author
FROM scenario;

-- scenario_hydroclimate_sibling no longer has author/model columns
SELECT column_name FROM information_schema.columns
WHERE table_name = 'scenario_hydroclimate_sibling'
ORDER BY ordinal_position;

-- s0029 in sibling table
SELECT short_code, name, baseline_group
FROM scenario_hydroclimate_sibling
WHERE short_code = 's0029';

-- s0029 scenario row points to itself
SELECT short_code, hydroclimate_sibling, is_active
FROM scenario WHERE short_code = 's0029';

-- sibling table row count
SELECT 'sibling rows' AS check, COUNT(*) AS count
FROM scenario_hydroclimate_sibling;

-- view still works
SELECT 'view works' AS check, COUNT(*) AS rows FROM scenario_full;

COMMIT;
