BEGIN;

ALTER TABLE version DROP COLUMN IF EXISTS manifest;

ALTER TABLE domain_family_map ADD COLUMN IF NOT EXISTS database_level TEXT;

ALTER TABLE model_source DROP COLUMN IF EXISTS version_family_id;

ALTER TABLE network_subtype DROP COLUMN IF EXISTS network_entity_type_id;

ALTER TABLE wba DROP COLUMN IF EXISTS hydrologic_region;

CREATE TABLE IF NOT EXISTS statistic_category (
    id          SERIAL PRIMARY KEY,
    short_code  TEXT UNIQUE NOT NULL,
    label       TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    created_by  INTEGER NOT NULL REFERENCES developer(id),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_by  INTEGER NOT NULL REFERENCES developer(id)
);

CREATE TRIGGER audit_fields_statistic_category
    BEFORE INSERT OR UPDATE ON statistic_category
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

ALTER TABLE statistic_type ADD COLUMN IF NOT EXISTS statistic_category_id INTEGER REFERENCES statistic_category(id);

ALTER TABLE domain_family_map DISABLE TRIGGER audit_fields_domain_family_map;
ALTER TABLE source DISABLE TRIGGER audit_fields_source;

COMMIT;

\echo ''
\echo '30a SCHEMA CHANGES COMPLETE'
\echo '==========================='
\echo 'Audit triggers on domain_family_map and source are DISABLED.'
\echo 'Run 30b_data_changes.sql as your own role ($DATABASE_URL) next.'
