-- =============================================================================
-- MIGRATION 49: Rename sibling_group → scenario_hydroclimate_sibling
-- =============================================================================
-- Run as: psql $SUPERUSER_URL -f <this_file>
-- =============================================================================

BEGIN;

-- Rename the table
ALTER TABLE sibling_group RENAME TO scenario_hydroclimate_sibling;

-- Rename the column on scenario
ALTER TABLE scenario RENAME COLUMN sibling_group TO hydroclimate_sibling;

-- Rename constraints
ALTER TABLE scenario_hydroclimate_sibling
    RENAME CONSTRAINT fk_sibling_group_baseline TO fk_hydro_sibling_baseline;
ALTER TABLE scenario_hydroclimate_sibling
    RENAME CONSTRAINT fk_sibling_group_author TO fk_hydro_sibling_author;
ALTER TABLE scenario_hydroclimate_sibling
    RENAME CONSTRAINT fk_sibling_group_model_source TO fk_hydro_sibling_model_source;
ALTER TABLE scenario
    RENAME CONSTRAINT fk_scenario_sibling_group TO fk_scenario_hydro_sibling;

-- Rename indexes
ALTER INDEX idx_sibling_group_baseline RENAME TO idx_hydro_sibling_baseline;
ALTER INDEX idx_scenario_sibling_group RENAME TO idx_scenario_hydro_sibling;

-- Rename trigger
ALTER TRIGGER audit_fields_sibling_group ON scenario_hydroclimate_sibling
    RENAME TO audit_fields_scenario_hydroclimate_sibling;

-- Update domain_family_map registration
UPDATE domain_family_map
SET table_name = 'scenario_hydroclimate_sibling'
WHERE table_name = 'sibling_group';

-- Update scenario_full view
DROP VIEW IF EXISTS scenario_full;

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

-- Grant permissions
GRANT ALL ON TABLE scenario_hydroclimate_sibling TO jfantauzza;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT 'table exists' AS check,
       COUNT(*) AS rows
FROM scenario_hydroclimate_sibling;

SELECT 'scenario column renamed' AS check,
       column_name
FROM information_schema.columns
WHERE table_name = 'scenario' AND column_name = 'hydroclimate_sibling';

SELECT 'view works' AS check, COUNT(*) AS rows FROM scenario_full;

COMMIT;
