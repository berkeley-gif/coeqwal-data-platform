-- Migration 35: Restructure scenario table
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/35_restructure_scenario_table.sql
--
-- Renames:  scenario_id to short_code, short_code to run_name, short_title to name,
--           simple_description to short_description, description to long_description
-- Drops:    name (verbose title), subtitle, narrative, source_scenario_id, slr_id
-- Adds:     model_source_id (FK to model_source, all = 1)
-- Changes:  is_active INTEGER to BOOLEAN
--
-- Dependencies handled:
--   - scenario_full view (dropped + recreated)
--   - scenario_slr_id_fkey, scenario_source_scenario_id_fkey (dropped)
--   - scenario_scenario_id_key UNIQUE constraint (renamed)
--   - Indexes on dropped columns (dropped)

BEGIN;

DROP VIEW IF EXISTS scenario_full;

ALTER TABLE scenario DROP CONSTRAINT IF EXISTS scenario_slr_id_fkey;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS scenario_source_scenario_id_fkey;

DROP INDEX IF EXISTS idx_scenario_source_scenario;
DROP INDEX IF EXISTS idx_scenario_slr;

ALTER TABLE scenario DROP COLUMN IF EXISTS subtitle;
ALTER TABLE scenario DROP COLUMN IF EXISTS narrative;
ALTER TABLE scenario DROP COLUMN IF EXISTS source_scenario_id;
ALTER TABLE scenario DROP COLUMN IF EXISTS slr_id;

ALTER TABLE scenario RENAME COLUMN name TO _name_old;
ALTER TABLE scenario RENAME COLUMN short_code TO _short_code_old;
ALTER TABLE scenario RENAME COLUMN scenario_id TO short_code;
ALTER TABLE scenario RENAME COLUMN _short_code_old TO run_name;
ALTER TABLE scenario RENAME COLUMN short_title TO name;
ALTER TABLE scenario DROP COLUMN _name_old;
ALTER TABLE scenario RENAME COLUMN simple_description TO short_description;
ALTER TABLE scenario RENAME COLUMN description TO long_description;

ALTER TABLE scenario RENAME CONSTRAINT scenario_scenario_id_key TO scenario_short_code_key;

ALTER INDEX IF EXISTS idx_scenario_short_code_active RENAME TO idx_scenario_run_name_active;

ALTER TABLE scenario ALTER COLUMN is_active DROP DEFAULT;
ALTER TABLE scenario ALTER COLUMN is_active TYPE BOOLEAN USING (is_active = 1);
ALTER TABLE scenario ALTER COLUMN is_active SET DEFAULT TRUE;

ALTER TABLE scenario ADD COLUMN model_source_id INTEGER
    REFERENCES model_source(id) ON DELETE RESTRICT ON UPDATE CASCADE;

UPDATE scenario SET model_source_id = 1;

ALTER TABLE scenario ALTER COLUMN model_source_id SET NOT NULL;

CREATE INDEX idx_scenario_model_source ON scenario(model_source_id);

CREATE VIEW scenario_full AS
SELECT
    s.id,
    s.short_code,
    s.run_name,
    s.name,
    s.is_active,
    sa.short_code                                       AS author,
    hc.short_code                                       AS hydroclimate,
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
LEFT JOIN scenario_author                sa   ON sa.id  = s.scenario_author_id
LEFT JOIN hydroclimate                   hc   ON hc.id  = s.hydroclimate_id
LEFT JOIN scenario_key_operation_link    skol ON skol.scenario_id = s.id
LEFT JOIN operation_definition           od   ON od.id  = skol.operation_id
LEFT JOIN scenario_key_assumption_link   skal ON skal.scenario_id = s.id
LEFT JOIN assumption_definition          ad   ON ad.id  = skal.assumption_id
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


SELECT 'scenario_columns' AS check,
       string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'scenario';

SELECT 'scenario_sample' AS check, id, short_code, run_name, name, is_active, model_source_id
FROM scenario ORDER BY id LIMIT 5;

SELECT 'scenario_full_count' AS check, count(*) AS active_scenarios
FROM scenario_full;

SELECT 'fk_check' AS check, conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'scenario'::regclass AND contype = 'f';

\echo
\echo '35 SCENARIO TABLE RESTRUCTURE COMPLETE'
\echo '======================================='
