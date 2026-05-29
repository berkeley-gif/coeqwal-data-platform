-- =============================================================================
-- Migration 17: Create scenario_full view
-- =============================================================================
-- Provides a wide, human-readable view of all scenario configurations by
-- pivoting the normalized operation and assumption link tables into columns.
-- Each row = one scenario, with all operation and assumption short_codes
-- fanned out as named columns grouped by category.
--
-- Operation columns (one per operation_category):
--   biops, tucp, gw_restrictions, infrastructure, flow,
--   delta_outflow, comm_delivery, regulatory_salinity, carryover
--
-- Assumption columns (one per assumption_category):
--   land_use, gw_model
--
-- NULL in a category column means that scenario has no link for that category
-- (e.g. s0046 has no delta_outflow; s0011 through s0042 have no gw_model).
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/17_create_scenario_full_view.sql
-- =============================================================================

DROP VIEW IF EXISTS scenario_full;

CREATE VIEW scenario_full AS
SELECT
    s.id,
    s.scenario_id,
    s.short_code,
    s.name,
    s.short_title,
    s.is_active,
    sa.short_code                                       AS author,
    hc.short_code                                       AS hydroclimate,
    slr.short_code                                      AS slr,
    MAX(CASE WHEN od.category = 'biops'               THEN od.short_code END) AS biops,
    MAX(CASE WHEN od.category = 'tucp'                THEN od.short_code END) AS tucp,
    MAX(CASE WHEN od.category = 'gw_restrictions'     THEN od.short_code END) AS gw_restrictions,
    MAX(CASE WHEN od.category = 'infrastructure'      THEN od.short_code END) AS infrastructure,
    MAX(CASE WHEN od.category = 'flow'                THEN od.short_code END) AS flow,
    MAX(CASE WHEN od.category = 'delta_outflow'       THEN od.short_code END) AS delta_outflow,
    MAX(CASE WHEN od.category = 'comm_delivery'       THEN od.short_code END) AS comm_delivery,
    MAX(CASE WHEN od.category = 'regulatory_salinity' THEN od.short_code END) AS regulatory_salinity,
    MAX(CASE WHEN od.category = 'carryover'           THEN od.short_code END) AS carryover,
    MAX(CASE WHEN ad.category = 'land_use'            THEN ad.short_code END) AS land_use,
    MAX(CASE WHEN ad.category = 'gw_model'            THEN ad.short_code END) AS gw_model
FROM scenario s
LEFT JOIN scenario_author   sa   ON sa.id  = s.scenario_author_id
LEFT JOIN hydroclimate       hc   ON hc.id  = s.hydroclimate_id
LEFT JOIN slr                     ON slr.id = s.slr_id
LEFT JOIN scenario_key_operation_link  skol ON skol.scenario_id = s.id
LEFT JOIN operation_definition         od   ON od.id  = skol.operation_id
LEFT JOIN scenario_key_assumption_link skal ON skal.scenario_id = s.id
LEFT JOIN assumption_definition        ad   ON ad.id  = skal.assumption_id
GROUP BY
    s.id, s.scenario_id, s.short_code, s.name, s.short_title, s.is_active,
    sa.short_code, hc.short_code, slr.short_code
ORDER BY s.id;

GRANT SELECT ON scenario_full TO jfantauzza;

COMMENT ON VIEW scenario_full IS
'Wide view of all scenario configurations. Pivots scenario_key_operation_link
and scenario_key_assumption_link into named columns per category. NULL in a
category column means the scenario has no link for that category.';


SELECT scenario_id, author, hydroclimate, slr,
       biops, tucp, gw_restrictions, infrastructure,
       flow, delta_outflow, comm_delivery,
       regulatory_salinity, carryover,
       land_use, gw_model
FROM scenario_full
ORDER BY id;
