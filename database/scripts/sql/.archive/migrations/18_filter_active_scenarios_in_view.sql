-- =============================================================================
-- Migration 18: Filter active scenarios in scenario_full + fix s0029 is_active
-- =============================================================================
-- Two changes:
--
-- 1. s0029 is marked is_active=1 in the DB but should be is_active=0.
--    The seed CSV already has the correct value (0). This migration corrects
--    the DB to match.
--
-- 2. scenario_full view is rebuilt to filter WHERE is_active = 1, so only
--    active scenarios are returned. Callers querying inactive scenarios should
--    query the scenario table directly.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/18_filter_active_scenarios_in_view.sql
-- =============================================================================

-- ─── 1. Fix s0029 is_active ───────────────────────────────────────────────────

ALTER TABLE scenario DISABLE TRIGGER USER;

UPDATE scenario
SET is_active  = 0,
    updated_at = NOW()
WHERE scenario_id = 's0029';

ALTER TABLE scenario ENABLE TRIGGER USER;

-- ─── 2. Rebuild scenario_full with active-only filter ─────────────────────────

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
    -- operations (pivoted by category)
    MAX(CASE WHEN od.category = 'biops'               THEN od.short_code END) AS biops,
    MAX(CASE WHEN od.category = 'tucp'                THEN od.short_code END) AS tucp,
    MAX(CASE WHEN od.category = 'gw_restrictions'     THEN od.short_code END) AS gw_restrictions,
    MAX(CASE WHEN od.category = 'infrastructure'      THEN od.short_code END) AS infrastructure,
    MAX(CASE WHEN od.category = 'flow'                THEN od.short_code END) AS flow,
    MAX(CASE WHEN od.category = 'delta_outflow'       THEN od.short_code END) AS delta_outflow,
    MAX(CASE WHEN od.category = 'comm_delivery'       THEN od.short_code END) AS comm_delivery,
    MAX(CASE WHEN od.category = 'regulatory_salinity' THEN od.short_code END) AS regulatory_salinity,
    MAX(CASE WHEN od.category = 'carryover'           THEN od.short_code END) AS carryover,
    -- assumptions (pivoted by category)
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
WHERE s.is_active = 1
GROUP BY
    s.id, s.scenario_id, s.short_code, s.name, s.short_title, s.is_active,
    sa.short_code, hc.short_code, slr.short_code
ORDER BY s.id;

GRANT SELECT ON scenario_full TO jfantauzza;

COMMENT ON VIEW scenario_full IS
'Wide view of ACTIVE scenario configurations (is_active = 1 only).
Pivots scenario_key_operation_link and scenario_key_assumption_link into named
columns per category. NULL in a category column means the scenario has no link
for that category. To query inactive scenarios query the scenario table directly.';

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT count(*) AS active_scenario_count FROM scenario_full;

SELECT scenario_id, is_active, biops, tucp, land_use
FROM scenario_full
ORDER BY id;
