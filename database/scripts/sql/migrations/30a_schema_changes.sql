-- Migration 30a: Schema (DDL) changes — run as postgres
--
-- Run from Cloud9:
--   psql "postgresql://postgres:<password>@<host>:5432/coeqwal_scenario" \
--        -f database/scripts/sql/migrations/30a_schema_changes.sql
--
-- Then run 30b_data_changes.sql as your own role ($DATABASE_URL).

BEGIN;

-- 3. version: drop unused manifest column
ALTER TABLE version DROP COLUMN IF EXISTS manifest;

-- 5. domain_family_map: add database_level column
ALTER TABLE domain_family_map ADD COLUMN IF NOT EXISTS database_level TEXT;

-- 6. model_source: drop version_family_id
ALTER TABLE model_source DROP COLUMN IF EXISTS version_family_id;

-- 9. network_subtype: drop redundant network_entity_type_id
ALTER TABLE network_subtype DROP COLUMN IF EXISTS network_entity_type_id;

-- 10. wba: drop legacy hydrologic_region text column
ALTER TABLE wba DROP COLUMN IF EXISTS hydrologic_region;

-- 12. statistic_category: create lookup table
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

-- Apply audit trigger to the new table (03_apply_audit_triggers.sql was a
-- one-time setup; new tables need their trigger created explicitly).
CREATE TRIGGER audit_fields_statistic_category
    BEFORE INSERT OR UPDATE ON statistic_category
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- 14a. statistic_type: add category FK column (nullable for now)
ALTER TABLE statistic_type ADD COLUMN IF NOT EXISTS statistic_category_id INTEGER REFERENCES statistic_category(id);

-- Temporarily disable audit triggers on tables where 30b needs to correct
-- created_by values. The set_audit_fields() trigger unconditionally preserves
-- OLD.created_by on UPDATE, so created_by fixes would silently fail otherwise.
-- 30c re-enables these triggers after data changes are complete.
ALTER TABLE domain_family_map DISABLE TRIGGER audit_fields_domain_family_map;
ALTER TABLE source DISABLE TRIGGER audit_fields_source;

COMMIT;

\echo ''
\echo '30a SCHEMA CHANGES COMPLETE'
\echo '==========================='
\echo 'Audit triggers on domain_family_map and source are DISABLED.'
\echo 'Run 30b_data_changes.sql as your own role ($DATABASE_URL) next.'
