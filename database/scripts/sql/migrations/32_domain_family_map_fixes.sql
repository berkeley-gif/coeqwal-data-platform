-- Migration 32: domain_family_map column reorder + data fixes + version changelogs
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/32_domain_family_map_fixes.sql
--
-- Changes:
--   1. Recreate domain_family_map with desired column order
--      (database_level and is_active BEFORE audit columns)
--   2. Fix database_level for du_urban_delivery_arc and mi_contractor_delivery_arc (10 → 03)
--   3. Add 3 missing AG tables to domain_family_map
--   4. Update version changelogs for ids 13 and 14

BEGIN;

-- ══════════════════════════════════════════════════════════════════════
-- 1. RECREATE domain_family_map WITH CORRECT COLUMN ORDER
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE domain_family_map_new (
    schema_name       TEXT NOT NULL DEFAULT 'public',
    table_name        TEXT NOT NULL,
    version_family_id INTEGER NOT NULL
        REFERENCES version_family(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    note              TEXT,
    database_level    VARCHAR(2),
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by        INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by        INTEGER NOT NULL REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    PRIMARY KEY (schema_name, table_name)
);

INSERT INTO domain_family_map_new
    (schema_name, table_name, version_family_id, note, database_level, is_active,
     created_at, created_by, updated_at, updated_by)
SELECT schema_name, table_name, version_family_id, note, database_level, is_active,
       created_at, created_by, updated_at, updated_by
FROM domain_family_map;

DROP TABLE domain_family_map;

ALTER TABLE domain_family_map_new RENAME TO domain_family_map;

CREATE INDEX idx_domain_family_map_version_family
    ON domain_family_map(version_family_id);

CREATE TRIGGER audit_fields_domain_family_map
    BEFORE INSERT OR UPDATE ON domain_family_map
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE domain_family_map IS
'Maps each database table to its version_family and database layer.';

GRANT SELECT, INSERT, UPDATE, DELETE ON domain_family_map TO jfantauzza;

-- ══════════════════════════════════════════════════════════════════════
-- 2. FIX database_level FOR DELIVERY ARC TABLES (10 → 03)
-- ══════════════════════════════════════════════════════════════════════

ALTER TABLE domain_family_map DISABLE TRIGGER audit_fields_domain_family_map;

UPDATE domain_family_map
SET database_level = '03', updated_at = NOW(), updated_by = 2
WHERE table_name IN ('du_urban_delivery_arc', 'mi_contractor_delivery_arc');

-- ══════════════════════════════════════════════════════════════════════
-- 3. ADD MISSING AG TABLES
-- ══════════════════════════════════════════════════════════════════════

INSERT INTO domain_family_map
    (schema_name, table_name, version_family_id, note, database_level, is_active,
     created_at, created_by, updated_at, updated_by)
VALUES
    ('public', 'ag_du_demand_monthly', 7,
     'Agricultural DU monthly demand statistics', '13', TRUE,
     NOW(), 2, NOW(), 2),
    ('public', 'ag_du_gw_pumping_monthly', 7,
     'Agricultural DU monthly groundwater pumping statistics', '13', TRUE,
     NOW(), 2, NOW(), 2),
    ('public', 'ag_du_sw_delivery_monthly', 7,
     'Agricultural DU monthly surface water delivery statistics', '13', TRUE,
     NOW(), 2, NOW(), 2)
ON CONFLICT (schema_name, table_name) DO UPDATE
SET version_family_id = EXCLUDED.version_family_id,
    note = EXCLUDED.note,
    database_level = EXCLUDED.database_level,
    updated_at = NOW(),
    updated_by = 2;

ALTER TABLE domain_family_map ENABLE TRIGGER audit_fields_domain_family_map;

-- ══════════════════════════════════════════════════════════════════════
-- 4. UPDATE VERSION CHANGELOGS
-- ══════════════════════════════════════════════════════════════════════

ALTER TABLE version DISABLE TRIGGER audit_fields_version;

UPDATE version
SET changelog = 'Initial entity version', updated_at = NOW(), updated_by = 2
WHERE id = 13;

UPDATE version
SET changelog = 'Initial audit version', updated_at = NOW(), updated_by = 2
WHERE id = 14;

ALTER TABLE version ENABLE TRIGGER audit_fields_version;

COMMIT;

-- ── Verification ─────────────────────────────────────────────────────

SELECT 'column_order' AS check,
       string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'domain_family_map';

SELECT 'delivery_arc_levels' AS check, table_name, database_level
FROM domain_family_map
WHERE table_name IN ('du_urban_delivery_arc', 'mi_contractor_delivery_arc');

SELECT 'new_ag_tables' AS check, table_name, database_level
FROM domain_family_map
WHERE table_name IN ('ag_du_demand_monthly', 'ag_du_gw_pumping_monthly', 'ag_du_sw_delivery_monthly');

SELECT 'version_changelogs' AS check, id, changelog
FROM version WHERE id IN (13, 14);

SELECT 'trigger_state' AS check, tgname, tgenabled
FROM pg_trigger
WHERE tgname IN ('audit_fields_domain_family_map', 'audit_fields_version');

\echo
\echo '32 DOMAIN_FAMILY_MAP FIXES COMPLETE'
\echo '===================================='
