-- Migration 36: Normalize TEXT category columns to integer FK columns
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/36_normalize_category_fks.sql
--
-- assumption_definition.category (TEXT) → assumption_category_id (INTEGER FK)
-- operation_definition.category (TEXT) → operation_category_id (INTEGER FK)
--
-- The scenario_full view uses od.category and ad.category in CASE expressions,
-- so it must be dropped and recreated with the new join path.

BEGIN;

-- ── 1. Drop dependent view ──────────────────────────────────────────
DROP VIEW IF EXISTS scenario_full;

-- ── 2. assumption_definition: add FK, populate, drop TEXT column ─────
ALTER TABLE assumption_definition
    ADD COLUMN assumption_category_id INTEGER
    REFERENCES assumption_category(id) ON DELETE RESTRICT ON UPDATE CASCADE;

UPDATE assumption_definition ad
SET assumption_category_id = ac.id
FROM assumption_category ac
WHERE ac.short_code = ad.category;

ALTER TABLE assumption_definition ALTER COLUMN assumption_category_id SET NOT NULL;

ALTER TABLE assumption_definition DROP COLUMN category;

CREATE INDEX idx_assumption_definition_category_id
    ON assumption_definition(assumption_category_id);

-- ── 3. operation_definition: add FK, populate, drop TEXT column ──────
ALTER TABLE operation_definition
    ADD COLUMN operation_category_id INTEGER
    REFERENCES operation_category(id) ON DELETE RESTRICT ON UPDATE CASCADE;

UPDATE operation_definition od_tbl
SET operation_category_id = oc.id
FROM operation_category oc
WHERE oc.short_code = od_tbl.category;

ALTER TABLE operation_definition ALTER COLUMN operation_category_id SET NOT NULL;

ALTER TABLE operation_definition DROP COLUMN category;

CREATE INDEX idx_operation_definition_category_id
    ON operation_definition(operation_category_id);

-- ── 4. Recreate scenario_full view with new join path ───────────────
CREATE VIEW scenario_full AS
SELECT
    s.id,
    s.short_code,
    s.run_name,
    s.name,
    s.is_active,
    sa.short_code                                       AS author,
    hc.short_code                                       AS hydroclimate,
    MAX(CASE WHEN oc.short_code = 'biops'               THEN od.short_code END) AS biops,
    MAX(CASE WHEN oc.short_code = 'tucp'                THEN od.short_code END) AS tucp,
    MAX(CASE WHEN oc.short_code = 'gw_restrictions'     THEN od.short_code END) AS gw_restrictions,
    MAX(CASE WHEN oc.short_code = 'infrastructure'      THEN od.short_code END) AS infrastructure,
    MAX(CASE WHEN oc.short_code = 'flow'                THEN od.short_code END) AS flow,
    MAX(CASE WHEN oc.short_code = 'delta_outflow'       THEN od.short_code END) AS delta_outflow,
    MAX(CASE WHEN oc.short_code = 'comm_delivery'       THEN od.short_code END) AS comm_delivery,
    MAX(CASE WHEN oc.short_code = 'regulatory_salinity' THEN od.short_code END) AS regulatory_salinity,
    MAX(CASE WHEN oc.short_code = 'carryover'           THEN od.short_code END) AS carryover,
    MAX(CASE WHEN ac.short_code = 'land_use'            THEN ad.short_code END) AS land_use,
    MAX(CASE WHEN ac.short_code = 'gw_model'            THEN ad.short_code END) AS gw_model
FROM scenario s
LEFT JOIN scenario_author                sa   ON sa.id  = s.scenario_author_id
LEFT JOIN hydroclimate                   hc   ON hc.id  = s.hydroclimate_id
LEFT JOIN scenario_key_operation_link    skol ON skol.scenario_id = s.id
LEFT JOIN operation_definition           od   ON od.id  = skol.operation_id
LEFT JOIN operation_category             oc   ON oc.id  = od.operation_category_id
LEFT JOIN scenario_key_assumption_link   skal ON skal.scenario_id = s.id
LEFT JOIN assumption_definition          ad   ON ad.id  = skal.assumption_id
LEFT JOIN assumption_category            ac   ON ac.id  = ad.assumption_category_id
WHERE s.is_active = TRUE
GROUP BY
    s.id, s.short_code, s.run_name, s.name, s.is_active,
    sa.short_code, hc.short_code
ORDER BY s.id;

GRANT SELECT ON scenario_full TO jfantauzza;

COMMENT ON VIEW scenario_full IS
    'Wide view of ACTIVE scenario configurations (is_active = TRUE only). '
    'Pivots operation and assumption links into named columns per category.';

COMMIT;

-- ── Verification ─────────────────────────────────────────────────────

SELECT 'assumption_fks' AS check, ad.short_code, ac.short_code AS category
FROM assumption_definition ad
JOIN assumption_category ac ON ac.id = ad.assumption_category_id
ORDER BY ad.id;

SELECT 'operation_fks' AS check, od.short_code, oc.short_code AS category
FROM operation_definition od
JOIN operation_category oc ON oc.id = od.operation_category_id
ORDER BY od.id;

SELECT 'view_check' AS check, count(*) AS rows FROM scenario_full;

\echo
\echo '36 CATEGORY FK NORMALIZATION COMPLETE'
\echo '======================================'
