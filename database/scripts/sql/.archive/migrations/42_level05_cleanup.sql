-- Migration 42: Clean up layer 05 tables
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/42_level05_cleanup.sql
--
-- assumption_category:  resequence IDs (2,5)→(1,2), is_active→BOOLEAN
-- operation_category:   resequence IDs (27-31)→(5-9), is_active→BOOLEAN
-- assumption_definition: resequence IDs, drop stale cols (incl assumptions_version_id),
--                        reorder cols, source→FK, is_active→BOOLEAN, fix attribution
-- operation_definition:  drop subtitle + operation_version_id, merge
--                        simple_description+narrative→description, reorder cols,
--                        source→FK, is_active→BOOLEAN, fix attribution
-- scenario_key_assumption_link: update assumption_id refs for new IDs

BEGIN;

-- ══════════════════════════════════════════════════════════════════════
-- DROP DEPENDENT VIEW
-- ══════════════════════════════════════════════════════════════════════
DROP VIEW IF EXISTS scenario_full;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 1: RESEQUENCE CATEGORY TABLE IDS
-- ══════════════════════════════════════════════════════════════════════

-- Drop FKs from definition → category (will be re-added with table recreation)
ALTER TABLE assumption_definition DROP CONSTRAINT IF EXISTS assumption_definition_assumption_category_id_fkey;
ALTER TABLE operation_definition  DROP CONSTRAINT IF EXISTS operation_definition_operation_category_id_fkey;

-- ── 1a. assumption_category: (2,5) → (1,2) ──────────────────────────
ALTER TABLE assumption_category DISABLE TRIGGER USER;

UPDATE assumption_category SET id = 101 WHERE id = 2;
UPDATE assumption_category SET id = 102 WHERE id = 5;
UPDATE assumption_category SET id = 1   WHERE id = 101;
UPDATE assumption_category SET id = 2   WHERE id = 102;

ALTER TABLE assumption_category ALTER COLUMN is_active DROP DEFAULT;
ALTER TABLE assumption_category ALTER COLUMN is_active TYPE BOOLEAN USING (is_active = 1);
ALTER TABLE assumption_category ALTER COLUMN is_active SET DEFAULT TRUE;

SELECT setval('assumption_category_id_seq', 2);
ALTER TABLE assumption_category ENABLE TRIGGER USER;

-- ── 1b. operation_category: (27,28,29,30,31) → (5,6,7,8,9) ─────────
ALTER TABLE operation_category DISABLE TRIGGER USER;

UPDATE operation_category SET id = 5 WHERE id = 27;
UPDATE operation_category SET id = 6 WHERE id = 28;
UPDATE operation_category SET id = 7 WHERE id = 29;
UPDATE operation_category SET id = 8 WHERE id = 30;
UPDATE operation_category SET id = 9 WHERE id = 31;

ALTER TABLE operation_category ALTER COLUMN is_active DROP DEFAULT;
ALTER TABLE operation_category ALTER COLUMN is_active TYPE BOOLEAN USING (is_active = 1);
ALTER TABLE operation_category ALTER COLUMN is_active SET DEFAULT TRUE;

SELECT setval('operation_category_id_seq', 9);
ALTER TABLE operation_category ENABLE TRIGGER USER;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 2: RECREATE assumption_definition
-- ══════════════════════════════════════════════════════════════════════
-- Current cols: id, short_code, name, short_title, subtitle, simple_description,
--   description, narrative, source, source_access_date, file,
--   assumptions_version_id, is_active, notes, created_by, updated_by,
--   created_at, updated_at, assumption_category_id
--
-- Target cols:  id, short_code, name, short_title, assumption_category_id,
--   description, source_id, is_active, notes,
--   created_by, updated_by, created_at, updated_at
--
-- Dropped: subtitle, simple_description, narrative, source_access_date, file,
--          assumptions_version_id (versioning handled by Level 00 system)
-- Changed: source (TEXT) → source_id (INTEGER FK), is_active INT→BOOLEAN
-- Resequenced: (2,3,4,10,17,18) → (1,2,3,4,5,6)
-- Category IDs remapped: old 2→1 (land_use), old 5→2 (gw_model)

-- Drop FK from crosswalk → definition
ALTER TABLE scenario_key_assumption_link DROP CONSTRAINT IF EXISTS scenario_key_assumption_link_assumption_id_fkey;

-- Detach sequence before dropping table
ALTER SEQUENCE assumption_definition_id_seq OWNED BY NONE;

CREATE TABLE assumption_definition_new (
    id                      INTEGER          NOT NULL,
    short_code              VARCHAR          NOT NULL,
    name                    VARCHAR,
    short_title             VARCHAR,
    assumption_category_id  INTEGER          NOT NULL,
    description             TEXT,
    source_id               INTEGER,
    is_active               BOOLEAN          NOT NULL DEFAULT TRUE,
    notes                   TEXT,
    created_by              INTEGER          NOT NULL,
    updated_by              INTEGER          NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO assumption_definition_new (
    id, short_code, name, short_title, assumption_category_id,
    description, source_id, is_active, notes,
    created_by, updated_by, created_at, updated_at
)
SELECT
    CASE id WHEN 2 THEN 1 WHEN 3 THEN 2 WHEN 4 THEN 3
            WHEN 10 THEN 4 WHEN 17 THEN 5 WHEN 18 THEN 6 END,
    short_code,
    name,
    short_title,
    CASE assumption_category_id WHEN 2 THEN 1 WHEN 5 THEN 2 END,
    description,
    2,
    (is_active = 1),
    notes,
    2,
    2,
    created_at,
    NOW()
FROM assumption_definition
ORDER BY id;

-- Update crosswalk assumption_id refs (single CASE avoids ordering issues)
ALTER TABLE scenario_key_assumption_link DISABLE TRIGGER USER;
UPDATE scenario_key_assumption_link SET assumption_id = CASE assumption_id
    WHEN 2  THEN 1
    WHEN 3  THEN 2
    WHEN 4  THEN 3
    WHEN 10 THEN 4
    WHEN 17 THEN 5
    WHEN 18 THEN 6
END
WHERE assumption_id IN (2, 3, 4, 10, 17, 18);
ALTER TABLE scenario_key_assumption_link ENABLE TRIGGER USER;

DROP TABLE assumption_definition;
ALTER TABLE assumption_definition_new RENAME TO assumption_definition;

ALTER TABLE assumption_definition ALTER COLUMN id SET DEFAULT nextval('assumption_definition_id_seq');
ALTER SEQUENCE assumption_definition_id_seq OWNED BY assumption_definition.id;
SELECT setval('assumption_definition_id_seq', 6);

ALTER TABLE assumption_definition ADD CONSTRAINT assumption_definition_pkey PRIMARY KEY (id);
ALTER TABLE assumption_definition ADD CONSTRAINT assumption_definition_short_code_key UNIQUE (short_code);

ALTER TABLE assumption_definition ADD CONSTRAINT assumption_definition_assumption_category_id_fkey
    FOREIGN KEY (assumption_category_id) REFERENCES assumption_category(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE assumption_definition ADD CONSTRAINT assumption_definition_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES source(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX idx_assumption_definition_active      ON assumption_definition(is_active);
CREATE INDEX idx_assumption_definition_category_id ON assumption_definition(assumption_category_id);

CREATE TRIGGER audit_fields_assumption_definition
    BEFORE INSERT OR UPDATE ON assumption_definition
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

ALTER TABLE scenario_key_assumption_link ADD CONSTRAINT scenario_key_assumption_link_assumption_id_fkey
    FOREIGN KEY (assumption_id) REFERENCES assumption_definition(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 3: RECREATE operation_definition
-- ══════════════════════════════════════════════════════════════════════
-- Current cols: id, short_code, name, short_title, subtitle,
--   simple_description, description, narrative, is_active, notes,
--   operation_version_id, created_by, updated_by, created_at, updated_at,
--   source, operation_category_id
--
-- Target cols:  id, short_code, name, short_title, operation_category_id,
--   description, source_id, is_active, notes,
--   created_by, updated_by, created_at, updated_at
--
-- Dropped: subtitle, operation_version_id (versioning handled by Level 00 system)
-- Merged: simple_description + narrative → description
-- Changed: source (TEXT) → source_id (INTEGER FK), is_active INT→BOOLEAN
-- Category IDs remapped: 27→5, 28→6, 29→7, 30→8, 31→9
-- IDs: already 1-28, no resequencing needed

ALTER TABLE scenario_key_operation_link DROP CONSTRAINT IF EXISTS scenario_key_operation_link_operation_id_fkey;

ALTER SEQUENCE operation_definition_id_seq OWNED BY NONE;

CREATE TABLE operation_definition_new (
    id                      INTEGER          NOT NULL,
    short_code              VARCHAR          NOT NULL,
    name                    VARCHAR,
    short_title             VARCHAR,
    operation_category_id   INTEGER          NOT NULL,
    description             TEXT,
    source_id               INTEGER,
    is_active               BOOLEAN          NOT NULL DEFAULT TRUE,
    notes                   TEXT,
    created_by              INTEGER          NOT NULL,
    updated_by              INTEGER          NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO operation_definition_new (
    id, short_code, name, short_title, operation_category_id,
    description, source_id, is_active, notes,
    created_by, updated_by, created_at, updated_at
)
SELECT
    id,
    short_code,
    name,
    short_title,
    CASE operation_category_id
        WHEN 27 THEN 5 WHEN 28 THEN 6 WHEN 29 THEN 7
        WHEN 30 THEN 8 WHEN 31 THEN 9
        ELSE operation_category_id
    END,
    COALESCE(NULLIF(description, ''), NULLIF(simple_description, ''), NULLIF(narrative, '')),
    2,
    (is_active = 1),
    notes,
    2,
    2,
    created_at,
    NOW()
FROM operation_definition
ORDER BY id;

DROP TABLE operation_definition;
ALTER TABLE operation_definition_new RENAME TO operation_definition;

ALTER TABLE operation_definition ALTER COLUMN id SET DEFAULT nextval('operation_definition_id_seq');
ALTER SEQUENCE operation_definition_id_seq OWNED BY operation_definition.id;
SELECT setval('operation_definition_id_seq', 28);

ALTER TABLE operation_definition ADD CONSTRAINT operation_definition_pkey PRIMARY KEY (id);
ALTER TABLE operation_definition ADD CONSTRAINT operation_definition_short_code_key UNIQUE (short_code);

ALTER TABLE operation_definition ADD CONSTRAINT operation_definition_operation_category_id_fkey
    FOREIGN KEY (operation_category_id) REFERENCES operation_category(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE operation_definition ADD CONSTRAINT operation_definition_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES source(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX idx_operation_definition_active      ON operation_definition(is_active);
CREATE INDEX idx_operation_definition_category_id ON operation_definition(operation_category_id);

CREATE TRIGGER audit_fields_operation_definition
    BEFORE INSERT OR UPDATE ON operation_definition
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

ALTER TABLE scenario_key_operation_link ADD CONSTRAINT scenario_key_operation_link_operation_id_fkey
    FOREIGN KEY (operation_id) REFERENCES operation_definition(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 4: REBUILD VIEW + GRANTS
-- ══════════════════════════════════════════════════════════════════════

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

GRANT SELECT, INSERT, UPDATE, DELETE ON assumption_category TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON assumption_definition TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON operation_category TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON operation_definition TO jfantauzza;
GRANT SELECT ON scenario_full TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE assumption_definition_id_seq TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE operation_definition_id_seq TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE assumption_category_id_seq TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE operation_category_id_seq TO jfantauzza;

COMMIT;

-- ══════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ══════════════════════════════════════════════════════════════════════

\echo ''
\echo 'assumption_category (should be IDs 1-2, is_active=BOOLEAN):'
SELECT id, short_code, label, is_active FROM assumption_category ORDER BY id;

\echo ''
\echo 'operation_category (should be IDs 1-9, is_active=BOOLEAN):'
SELECT id, short_code, name, is_active FROM operation_category ORDER BY id;

\echo ''
\echo 'assumption_definition columns:'
SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'assumption_definition';

\echo ''
\echo 'assumption_definition data (should be IDs 1-6, source_id=2, created/updated_by=2):'
SELECT id, short_code, assumption_category_id, source_id, is_active, created_by, updated_by
FROM assumption_definition ORDER BY id;

\echo ''
\echo 'operation_definition columns:'
SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'operation_definition';

\echo ''
\echo 'operation_definition data (IDs 1-28, category_id 1-9, source_id=2):'
SELECT id, short_code, operation_category_id, source_id, is_active, created_by, updated_by
FROM operation_definition ORDER BY id;

\echo ''
\echo 'operation_definition description merge check (rows with description):'
SELECT id, short_code, LEFT(description, 60) AS description_preview
FROM operation_definition
WHERE description IS NOT NULL
ORDER BY id;

\echo ''
\echo 'scenario_key_assumption_link (assumption_id should be 1-6):'
SELECT scenario_id, assumption_id FROM scenario_key_assumption_link ORDER BY scenario_id;

\echo ''
\echo 'scenario_full view check:'
SELECT count(*) AS active_scenarios FROM scenario_full;

\echo ''
\echo '42 LEVEL 05 CLEANUP COMPLETE'
\echo '============================='
