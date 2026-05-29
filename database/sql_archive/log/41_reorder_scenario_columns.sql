-- Migration 41: Reorder scenario table columns
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/41_reorder_scenario_columns.sql
--
-- PostgreSQL does not support ALTER TABLE ... REORDER COLUMN, so we must
-- recreate the table. This script preserves all data, constraints, indexes,
-- triggers, foreign keys (both directions), the sequence, view, and grants.
--
-- Current order:
--   id, short_code, run_name, is_active, name, short_description, long_description,
--   baseline_scenario_id, hydroclimate_id, scenario_author_id, scenario_version_id,
--   created_by, updated_by, created_at, updated_at, model_source_id
--
-- Target order:
--   id, short_code, run_name, is_active, name, short_description, long_description,
--   baseline_scenario_id, hydroclimate_id, scenario_version_id, scenario_author_id,
--   model_source_id, created_by, updated_by, created_at, updated_at

BEGIN;

DROP VIEW IF EXISTS scenario_full;

ALTER TABLE theme_scenario_link          DROP CONSTRAINT IF EXISTS theme_scenario_link_scenario_id_fkey;
ALTER TABLE scenario_key_assumption_link DROP CONSTRAINT IF EXISTS scenario_key_assumption_link_scenario_id_fkey;
ALTER TABLE scenario_key_operation_link  DROP CONSTRAINT IF EXISTS scenario_key_operation_link_scenario_id_fkey;
ALTER TABLE scenario_tag_link            DROP CONSTRAINT IF EXISTS scenario_tag_link_scenario_id_fkey;

ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_scenario_author;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS scenario_model_source_id_fkey;

DROP INDEX IF EXISTS idx_scenario_active;
DROP INDEX IF EXISTS idx_scenario_active_version;
DROP INDEX IF EXISTS idx_scenario_baseline;
DROP INDEX IF EXISTS idx_scenario_hydroclimate;
DROP INDEX IF EXISTS idx_scenario_run_name_active;
DROP INDEX IF EXISTS idx_scenario_model_source;

DROP TRIGGER IF EXISTS audit_fields_scenario ON scenario;

CREATE TABLE scenario_reorder (
    id                    INTEGER          NOT NULL,
    short_code            VARCHAR          NOT NULL,
    run_name              VARCHAR,
    is_active             BOOLEAN          NOT NULL DEFAULT TRUE,
    name                  VARCHAR,
    short_description     TEXT,
    long_description      TEXT,
    baseline_scenario_id  INTEGER,
    hydroclimate_id       INTEGER,
    scenario_version_id   INTEGER,
    scenario_author_id    INTEGER,
    model_source_id       INTEGER          NOT NULL,
    created_by            INTEGER          NOT NULL,
    updated_by            INTEGER          NOT NULL,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO scenario_reorder (
    id, short_code, run_name, is_active, name, short_description, long_description,
    baseline_scenario_id, hydroclimate_id, scenario_version_id, scenario_author_id,
    model_source_id, created_by, updated_by, created_at, updated_at
)
SELECT
    id, short_code, run_name, is_active, name, short_description, long_description,
    baseline_scenario_id, hydroclimate_id, scenario_version_id, scenario_author_id,
    model_source_id, created_by, updated_by, created_at, updated_at
FROM scenario;

ALTER SEQUENCE scenario_id_seq OWNED BY NONE;

DROP TABLE scenario;

ALTER TABLE scenario_reorder RENAME TO scenario;

ALTER TABLE scenario ALTER COLUMN id SET DEFAULT nextval('scenario_id_seq');
ALTER SEQUENCE scenario_id_seq OWNED BY scenario.id;
SELECT setval('scenario_id_seq', (SELECT MAX(id) FROM scenario));

ALTER TABLE scenario ADD CONSTRAINT scenario_pkey PRIMARY KEY (id);
ALTER TABLE scenario ADD CONSTRAINT scenario_short_code_key UNIQUE (short_code);

ALTER TABLE scenario ADD CONSTRAINT fk_scenario_scenario_author
    FOREIGN KEY (scenario_author_id) REFERENCES scenario_author(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE scenario ADD CONSTRAINT scenario_model_source_id_fkey
    FOREIGN KEY (model_source_id) REFERENCES model_source(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX idx_scenario_active         ON scenario(is_active);
CREATE INDEX idx_scenario_active_version ON scenario(is_active, scenario_version_id);
CREATE INDEX idx_scenario_baseline       ON scenario(baseline_scenario_id);
CREATE INDEX idx_scenario_hydroclimate   ON scenario(hydroclimate_id);
CREATE INDEX idx_scenario_run_name_active ON scenario(run_name, is_active);
CREATE INDEX idx_scenario_model_source   ON scenario(model_source_id);

CREATE TRIGGER audit_fields_scenario
    BEFORE INSERT OR UPDATE ON scenario
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

ALTER TABLE theme_scenario_link ADD CONSTRAINT theme_scenario_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_assumption_link ADD CONSTRAINT scenario_key_assumption_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_operation_link ADD CONSTRAINT scenario_key_operation_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_tag_link ADD CONSTRAINT scenario_tag_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

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

COMMENT ON VIEW scenario_full IS
    'Wide view of ACTIVE scenario configurations (is_active = TRUE only). '
    'Pivots operation and assumption links into named columns per category.';

GRANT SELECT, INSERT, UPDATE, DELETE ON scenario TO jfantauzza;
GRANT SELECT ON scenario_full TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE scenario_id_seq TO jfantauzza;

COMMIT;


\echo ''
\echo 'Column order (should match target):'
SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'scenario';

\echo ''
\echo 'Row count and sample:'
SELECT count(*) AS total_scenarios FROM scenario;

SELECT id, short_code, run_name, is_active, model_source_id
FROM scenario ORDER BY id LIMIT 5;

\echo ''
\echo 'FK constraints on scenario:'
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'scenario'::regclass AND contype = 'f';

\echo ''
\echo 'FK constraints pointing TO scenario:'
SELECT conrelid::regclass AS from_table, conname
FROM pg_constraint
WHERE confrelid = 'scenario'::regclass AND contype = 'f'
ORDER BY conrelid::regclass::text;

\echo ''
\echo 'Indexes on scenario:'
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'scenario' AND schemaname = 'public'
ORDER BY indexname;

\echo ''
\echo 'Trigger check:'
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'scenario'::regclass AND NOT tgisinternal;

\echo ''
\echo 'View check:'
SELECT count(*) AS active_scenarios FROM scenario_full;

\echo ''
\echo '41 SCENARIO COLUMN REORDER COMPLETE'
\echo '====================================='
