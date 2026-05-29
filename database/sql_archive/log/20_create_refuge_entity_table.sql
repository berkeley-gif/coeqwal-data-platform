-- =============================================================================
-- 20_create_refuge_entity_table.sql
-- Creates du_refuge_entity table, loads seed data, creates refuge_du_full view.
--
-- Run from the repository root with SUPERUSER_URL (DDL privileges required):
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/20_create_refuge_entity_table.sql
--
-- Seed data is loaded via \copy from the local repo  - no S3 upload needed.
-- The path 'database/seed_tables/...' is relative to wherever psql is invoked
-- (the repo root when following the standard run command above).
--
-- Prerequisites:
--   1. Run from repo root (so \copy paths resolve correctly)
--   2. Migration 19_create_refuge_statistics_tables.sql already run
--
-- Rubric checklist:
--   [x] 1. Developer attributed    - created_by = 2 (jfantauzza) via DISABLE TRIGGER USER + explicit UPDATE
--   [x] 2. Data source attributed  - source column populated from CSV (calsim_report, geopackage)
--   [x] 3. Version family linked   - registered in domain_family_map (entity family)
--   [x] 4. Appropriate lookups     - cs3_type values documented; hydrologic_region VARCHAR follows existing pattern
--   [x] 5. Columns/types/FKs/indexes aligned with ERD  - explicit FK constraints + all indexes
--   [x] 6. Seed data aligned       - 18 rows from du_refuge_entity.csv
--   [x] 7. View created            - refuge_du_full denormalizes entity attributes for API consumption
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================='
\echo 'MIGRATION 20  - du_refuge_entity + refuge_du_full view'
\echo '============================================='


-- =============================================================================
-- 1. Create du_refuge_entity table
-- =============================================================================

\echo ''
\echo 'Creating du_refuge_entity...'

DROP TABLE IF EXISTS du_refuge_entity CASCADE;

CREATE TABLE du_refuge_entity (
    id                            SERIAL PRIMARY KEY,

    du_id                         VARCHAR(20)      UNIQUE NOT NULL,
    wba_id                        VARCHAR(10),
    hydrologic_region             VARCHAR(20)      NOT NULL,
    dups                          INTEGER,
    du_class                      VARCHAR(50)      DEFAULT 'Refuge',
    cs3_type                      VARCHAR(10),

    total_acres                   NUMERIC(14, 4),
    polygon_count                 INTEGER          DEFAULT 1,

    refuge_or_wildlife_area       TEXT,
    managed_by                    VARCHAR(200),
    provider                      VARCHAR(200),

    gw                            BOOLEAN          NOT NULL DEFAULT FALSE,
    sw                            BOOLEAN          NOT NULL DEFAULT TRUE,

    point_of_diversion_conveyance TEXT,
    source                        VARCHAR(100),
    model_source                  VARCHAR(50)      DEFAULT 'calsim3',
    has_gis_data                  BOOLEAN          DEFAULT TRUE,

    is_active                     BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at                    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_by                    INTEGER          NOT NULL DEFAULT 1,
    updated_at                    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_by                    INTEGER          NOT NULL DEFAULT 1,

    CONSTRAINT fk_du_refuge_entity_created_by
        FOREIGN KEY (created_by) REFERENCES developer (id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_du_refuge_entity_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer (id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_du_refuge_entity_hydrologic_region
    ON du_refuge_entity (hydrologic_region);

CREATE INDEX IF NOT EXISTS idx_du_refuge_entity_cs3_type
    ON du_refuge_entity (cs3_type);

CREATE TRIGGER trg_du_refuge_entity_audit
    BEFORE INSERT OR UPDATE ON du_refuge_entity
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE du_refuge_entity IS
    'Wildlife refuge and wetland demand units in CalSim 3. '
    'Source: database/seed_tables/04_calsim_data/du_refuge_entity.csv (18 rows). '
    'Based on CalSim 3 Main Report Tables 3-9 (SAC) and 3-10 (SJR/Tulare). '
    'Migration: 20_create_refuge_entity_table.sql.';

COMMENT ON COLUMN du_refuge_entity.du_id IS
    'CalSim 3 demand unit identifier (e.g. 08N_PR1, 91_PR). Matches AWO_{DU_ID} in SV input and DN_{DU_ID} in DV output.';
COMMENT ON COLUMN du_refuge_entity.dups IS
    '-1 = multiple sub-units or refuges combined into one CalSim DU; 0 = single unit.';
COMMENT ON COLUMN du_refuge_entity.cs3_type IS
    'PR = Project Refuge (receives CVP/Central Valley Project contract deliveries); NR = Non-project Refuge (water rights only, no CVP deliveries).';
COMMENT ON COLUMN du_refuge_entity.gw IS
    'TRUE if demand unit has access to groundwater, per CalSim 3 Main Report tables.';
COMMENT ON COLUMN du_refuge_entity.sw IS
    'TRUE if demand unit has access to surface water, per CalSim 3 Main Report tables. '
    'All 18 current refuge DUs have surface water access. '
    'Should be surfaced in the frontend (tooltip or attribute panel) alongside gw.';
COMMENT ON COLUMN du_refuge_entity.provider IS
    'Surface water provider or contractor. May be blank for drainage-supplied DUs (e.g. 63_PR1).';
COMMENT ON COLUMN du_refuge_entity.source IS
    'Comma-separated data source tags: "geopackage" = GIS polygon from COEQWAL geopackage; '
    '"calsim_report" = CalSim 3 Main Report (August 2022).';

\echo 'du_refuge_entity table created.'


-- =============================================================================
-- 2. Grants
-- =============================================================================

\echo ''
\echo 'Granting permissions...'

GRANT SELECT, INSERT, UPDATE, DELETE ON du_refuge_entity TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE du_refuge_entity_id_seq TO jfantauzza;

\echo 'Grants applied.'


-- =============================================================================
-- 3. Load seed data from repo (client-side \copy  - no S3 required)
-- =============================================================================

\echo ''
\echo 'Loading 18 refuge demand units from repo CSV...'


\copy du_refuge_entity (du_id, wba_id, hydrologic_region, dups, du_class, cs3_type, total_acres, polygon_count, refuge_or_wildlife_area, managed_by, provider, gw, sw, point_of_diversion_conveyance, source, model_source, has_gis_data) FROM 'database/seed_tables/04_calsim_data/du_refuge_entity.csv' WITH (FORMAT csv, HEADER true, NULL '');

\echo 'Seed data loaded.'


-- =============================================================================
-- 4. Set audit fields  - developer jfantauzza = id 2
-- =============================================================================

\echo ''
\echo 'Setting audit provenance (created_by = 2, jfantauzza)...'

ALTER TABLE du_refuge_entity DISABLE TRIGGER USER;

UPDATE du_refuge_entity
SET
    is_active  = TRUE,
    created_at = NOW(),
    created_by = 2,
    updated_at = NOW(),
    updated_by = 2;

ALTER TABLE du_refuge_entity ENABLE TRIGGER USER;

\echo 'Audit provenance set.'


-- =============================================================================
-- 5. Register in domain_family_map (entity version family)
-- =============================================================================

\echo ''
\echo 'Registering du_refuge_entity in domain_family_map...'

ALTER TABLE domain_family_map DISABLE TRIGGER USER;

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
SELECT
    'public',
    'du_refuge_entity',
    vf.id,
    'Wildlife refuge demand unit entities (Layer 03)',
    2,
    2
FROM version_family vf
WHERE vf.short_code = 'entity'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id,
        note              = EXCLUDED.note,
        updated_at        = NOW(),
        updated_by        = 2;

ALTER TABLE domain_family_map ENABLE TRIGGER USER;

\echo 'Registered in domain_family_map.'


-- =============================================================================
-- 6. Create refuge_du_full view
-- =============================================================================

\echo ''
\echo 'Creating refuge_du_full view...'

DROP VIEW IF EXISTS refuge_du_full;

CREATE VIEW refuge_du_full AS
SELECT
    e.du_id,
    e.wba_id,
    e.hydrologic_region,
    e.cs3_type,
    CASE e.cs3_type
        WHEN 'PR' THEN 'Project Refuge'
        WHEN 'NR' THEN 'Non-project Refuge'
        ELSE e.cs3_type
    END                                 AS cs3_type_label,
    e.refuge_or_wildlife_area,
    e.managed_by,
    e.provider,
    e.gw,
    e.sw,
    e.total_acres,
    e.polygon_count,
    e.point_of_diversion_conveyance,
    e.has_gis_data,
    e.is_active
FROM du_refuge_entity e
WHERE e.is_active = TRUE
ORDER BY e.hydrologic_region, e.du_id;

GRANT SELECT ON refuge_du_full TO jfantauzza;

COMMENT ON VIEW refuge_du_full IS
'Denormalized view of active wildlife refuge demand units with decoded cs3_type label. '
'Use for API responses and frontend attribute panels. '
'Includes gw and sw flags  - both should be surfaced in the frontend (tooltip or attribute panel). '
'Created by migration 20_create_refuge_entity_table.sql.';

\echo 'refuge_du_full view created.'


-- =============================================================================
-- 7. Verification
-- =============================================================================

\echo ''
\echo '===== VERIFICATION ====='

\echo ''
\echo 'Row count (expect 18):'
SELECT COUNT(*) AS total_rows FROM du_refuge_entity;

\echo ''
\echo 'Rows by hydrologic region:'
SELECT hydrologic_region, COUNT(*) AS n
FROM du_refuge_entity
GROUP BY hydrologic_region
ORDER BY hydrologic_region;

\echo ''
\echo 'Rows by cs3_type:'
SELECT cs3_type,
       CASE cs3_type WHEN 'PR' THEN 'Project Refuge' WHEN 'NR' THEN 'Non-project Refuge' END AS label,
       COUNT(*) AS n
FROM du_refuge_entity
GROUP BY cs3_type
ORDER BY cs3_type;

\echo ''
\echo 'GW / SW access breakdown:'
SELECT
    gw::int  AS has_gw,
    sw::int  AS has_sw,
    COUNT(*) AS n,
    STRING_AGG(du_id, ', ' ORDER BY du_id) AS du_ids
FROM du_refuge_entity
GROUP BY gw, sw
ORDER BY gw, sw;

\echo ''
\echo 'Audit provenance (all should show created_by = 2):'
SELECT DISTINCT created_by, updated_by FROM du_refuge_entity;

\echo ''
\echo 'domain_family_map registration:'
SELECT table_name, version_family_id, note
FROM domain_family_map
WHERE table_name = 'du_refuge_entity';

\echo ''
\echo 'refuge_du_full view (spot-check):'
SELECT du_id, hydrologic_region, cs3_type_label, managed_by, gw::int, sw::int
FROM refuge_du_full;

\echo ''
\echo '=== Migration 20 complete ==='
