-- Migration 33: Level 01 lookup table improvements
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/33_level01_improvements.sql
--
-- Changes:
--   1. Add NC (North Coast) to hydrologic_region
--   2. Recreate watershed: replace hydrologic_region_short_code text column
--      with hydrologic_region_id FK, reorder columns (unimp_sv_variable before
--      audit fields), fix attribution to developer 2
--   3. Recreate statistic_type: reorder columns (statistic_category_id before
--      audit fields)
--   4. Remove stale ag_du_delivery_monthly from domain_family_map
--
-- The watershed and statistic_type tables are recreated because PostgreSQL
-- does not support column reordering via ALTER TABLE.

BEGIN;

-- ══════════════════════════════════════════════════════════════════════
-- 1. ADD NC (NORTH COAST) TO hydrologic_region
-- ══════════════════════════════════════════════════════════════════════

ALTER TABLE hydrologic_region DISABLE TRIGGER USER;

INSERT INTO hydrologic_region (short_code, label, is_active, created_at, created_by, updated_at, updated_by)
VALUES ('NC', 'North Coast', TRUE, NOW(), 2, NOW(), 2)
ON CONFLICT (short_code) DO NOTHING;

ALTER TABLE hydrologic_region ENABLE TRIGGER USER;

-- ══════════════════════════════════════════════════════════════════════
-- 2. RECREATE watershed WITH FK + COLUMN REORDER + ATTRIBUTION FIX
-- ══════════════════════════════════════════════════════════════════════

-- 2a. Drop the FK from channel_entity that references watershed(short_code)
ALTER TABLE channel_entity DROP CONSTRAINT IF EXISTS channel_entity_watershed_short_code_fkey;

-- 2b. Create new table with desired column order
CREATE TABLE watershed_new (
    id                    SERIAL PRIMARY KEY,
    short_code            VARCHAR NOT NULL UNIQUE,
    name                  VARCHAR NOT NULL,
    description           TEXT,
    hydrologic_region_id  INTEGER REFERENCES hydrologic_region(id)
                              ON DELETE RESTRICT ON UPDATE CASCADE,
    unimp_sv_variable     VARCHAR,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by            INTEGER NOT NULL REFERENCES developer(id)
                              ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by            INTEGER NOT NULL REFERENCES developer(id)
                              ON DELETE RESTRICT ON UPDATE CASCADE
);

-- 2c. Migrate data, resolving short_code → id via JOIN, fix attribution to dev 2
INSERT INTO watershed_new
    (id, short_code, name, description, hydrologic_region_id,
     unimp_sv_variable, is_active, created_at, created_by, updated_at, updated_by)
SELECT
    w.id, w.short_code, w.name, w.description,
    hr.id AS hydrologic_region_id,
    w.unimp_sv_variable, w.is_active,
    w.created_at, 2, NOW(), 2
FROM watershed w
LEFT JOIN hydrologic_region hr
    ON hr.short_code = w.hydrologic_region_short_code;

-- 2d. Drop old table and rename
DROP TABLE watershed;
ALTER TABLE watershed_new RENAME TO watershed;

-- 2e. Reset the sequence to continue from the max existing id
SELECT setval('watershed_new_id_seq', (SELECT MAX(id) FROM watershed));
ALTER SEQUENCE watershed_new_id_seq RENAME TO watershed_id_seq;

-- 2f. Recreate index and audit trigger
CREATE INDEX idx_watershed_hydrologic_region
    ON watershed(hydrologic_region_id);

CREATE TRIGGER audit_fields_watershed
    BEFORE INSERT OR UPDATE ON watershed
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE watershed IS
'Watersheds with FK to hydrologic_region and optional CalSim unimpaired SV variable.';

-- 2g. Re-add the FK from channel_entity
ALTER TABLE channel_entity
    ADD CONSTRAINT channel_entity_watershed_short_code_fkey
    FOREIGN KEY (watershed_short_code) REFERENCES watershed(short_code);

-- 2h. Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON watershed TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE watershed_id_seq TO jfantauzza;

-- ══════════════════════════════════════════════════════════════════════
-- 3. RECREATE statistic_type WITH COLUMN REORDER
-- ══════════════════════════════════════════════════════════════════════

-- No FKs reference statistic_type, so we can drop and recreate directly.

CREATE TABLE statistic_type_new (
    id                    SERIAL PRIMARY KEY,
    short_code            TEXT NOT NULL UNIQUE,
    label                 TEXT NOT NULL,
    description           TEXT,
    statistic_category_id INTEGER NOT NULL REFERENCES statistic_category(id)
                              ON DELETE RESTRICT ON UPDATE CASCADE,
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by            INTEGER NOT NULL REFERENCES developer(id)
                              ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by            INTEGER NOT NULL REFERENCES developer(id)
                              ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO statistic_type_new
    (id, short_code, label, description, statistic_category_id,
     created_at, created_by, updated_at, updated_by)
SELECT id, short_code, label, description, statistic_category_id,
       created_at, created_by, updated_at, updated_by
FROM statistic_type;

DROP TABLE statistic_type;
ALTER TABLE statistic_type_new RENAME TO statistic_type;

SELECT setval('statistic_type_new_id_seq', (SELECT MAX(id) FROM statistic_type));
ALTER SEQUENCE statistic_type_new_id_seq RENAME TO statistic_type_id_seq;

CREATE TRIGGER audit_fields_statistic_type
    BEFORE INSERT OR UPDATE ON statistic_type
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE statistic_type IS
'Defines each statistic (MEAN, Q50, EXC_P90, etc.) and its category.';

GRANT SELECT, INSERT, UPDATE, DELETE ON statistic_type TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE statistic_type_id_seq TO jfantauzza;

-- ══════════════════════════════════════════════════════════════════════
-- 4. REMOVE STALE ag_du_delivery_monthly FROM domain_family_map
-- ══════════════════════════════════════════════════════════════════════

ALTER TABLE domain_family_map DISABLE TRIGGER audit_fields_domain_family_map;

DELETE FROM domain_family_map
WHERE table_name = 'ag_du_delivery_monthly';

ALTER TABLE domain_family_map ENABLE TRIGGER audit_fields_domain_family_map;

COMMIT;

-- ══════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ══════════════════════════════════════════════════════════════════════

SELECT 'hydrologic_region_nc' AS check, id, short_code, label
FROM hydrologic_region WHERE short_code = 'NC';

SELECT 'watershed_columns' AS check,
       string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'watershed';

SELECT 'watershed_fk_resolved' AS check,
       w.short_code, w.hydrologic_region_id, hr.short_code AS region_code
FROM watershed w
LEFT JOIN hydrologic_region hr ON hr.id = w.hydrologic_region_id
ORDER BY w.id;

SELECT 'watershed_attribution' AS check,
       COUNT(*) FILTER (WHERE created_by = 2) AS dev2_created,
       COUNT(*) FILTER (WHERE updated_by = 2) AS dev2_updated,
       COUNT(*) FILTER (WHERE created_by != 2) AS other_created,
       COUNT(*) FILTER (WHERE updated_by != 2) AS other_updated
FROM watershed;

SELECT 'statistic_type_columns' AS check,
       string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'statistic_type';

SELECT 'stale_dfm_removed' AS check,
       COUNT(*) AS remaining
FROM domain_family_map WHERE table_name = 'ag_du_delivery_monthly';

SELECT 'trigger_state' AS check, tgrelid::regclass AS table_name, tgname, tgenabled
FROM pg_trigger
WHERE tgname IN ('audit_fields_watershed', 'audit_fields_statistic_type',
                  'audit_fields_domain_family_map', 'audit_fields_hydrologic_region')
ORDER BY tgrelid::regclass::text;

\echo
\echo '33 LEVEL 01 IMPROVEMENTS COMPLETE'
\echo '==================================='
