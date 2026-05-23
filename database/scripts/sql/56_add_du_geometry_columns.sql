-- 56_add_du_geometry_columns.sql
--
-- Add demand-unit polygon geometry to the three demand-unit entity tables.
-- Mirrors the column triple used by `wba` and `reservoir`:
--     geom_wkt TEXT, srid INTEGER, geom geometry(MultiPolygon, 4326)
-- plus a standard GiST index named `idx_<table>_geom`.
--
-- Polygons are dissolved one-per-`du_id` and ship from `reference/du_4326.gpkg`
-- (layer `demandunits`, EPSG:4326). Loaded by
-- `database/scripts/data_processing/load_du_geometries.py`.
--
-- The `du_id` `26N_NA` exists in both `du_urban_entity` and `du_agriculture_entity`
-- and gets the same dissolved polygon written to both rows. See
-- `docs/du_geometry_gap.md` for the coverage scorecard and the 54 `du_id`s
-- that have no polygon in the source file.
--
-- Safe to re-run: `ADD COLUMN IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`
-- short-circuit when the columns / indexes already exist.

BEGIN;

-- ============================================================
-- du_urban_entity
-- ============================================================
ALTER TABLE du_urban_entity
    ADD COLUMN IF NOT EXISTS geom_wkt TEXT,
    ADD COLUMN IF NOT EXISTS srid     INTEGER,
    ADD COLUMN IF NOT EXISTS geom     geometry(MultiPolygon, 4326);

CREATE INDEX IF NOT EXISTS idx_du_urban_entity_geom
    ON du_urban_entity USING GIST (geom);

COMMENT ON COLUMN du_urban_entity.geom IS
    'Dissolved urban demand-unit footprint (EPSG:4326). '
    'Loaded by database/scripts/data_processing/load_du_geometries.py from reference/du_4326.gpkg.';
COMMENT ON COLUMN du_urban_entity.geom_wkt IS
    'WKT mirror of geom. Populated server-side as ST_AsText(geom) by the loader.';
COMMENT ON COLUMN du_urban_entity.srid IS
    'Spatial reference identifier for geom. Always 4326 for this table.';

-- ============================================================
-- du_agriculture_entity
-- ============================================================
ALTER TABLE du_agriculture_entity
    ADD COLUMN IF NOT EXISTS geom_wkt TEXT,
    ADD COLUMN IF NOT EXISTS srid     INTEGER,
    ADD COLUMN IF NOT EXISTS geom     geometry(MultiPolygon, 4326);

CREATE INDEX IF NOT EXISTS idx_du_agriculture_entity_geom
    ON du_agriculture_entity USING GIST (geom);

COMMENT ON COLUMN du_agriculture_entity.geom IS
    'Dissolved agricultural demand-unit footprint (EPSG:4326). '
    'Loaded by database/scripts/data_processing/load_du_geometries.py from reference/du_4326.gpkg.';
COMMENT ON COLUMN du_agriculture_entity.geom_wkt IS
    'WKT mirror of geom. Populated server-side as ST_AsText(geom) by the loader.';
COMMENT ON COLUMN du_agriculture_entity.srid IS
    'Spatial reference identifier for geom. Always 4326 for this table.';

-- ============================================================
-- du_refuge_entity
-- ============================================================
ALTER TABLE du_refuge_entity
    ADD COLUMN IF NOT EXISTS geom_wkt TEXT,
    ADD COLUMN IF NOT EXISTS srid     INTEGER,
    ADD COLUMN IF NOT EXISTS geom     geometry(MultiPolygon, 4326);

CREATE INDEX IF NOT EXISTS idx_du_refuge_entity_geom
    ON du_refuge_entity USING GIST (geom);

COMMENT ON COLUMN du_refuge_entity.geom IS
    'Dissolved refuge demand-unit footprint (EPSG:4326). '
    'Loaded by database/scripts/data_processing/load_du_geometries.py from reference/du_4326.gpkg.';
COMMENT ON COLUMN du_refuge_entity.geom_wkt IS
    'WKT mirror of geom. Populated server-side as ST_AsText(geom) by the loader.';
COMMENT ON COLUMN du_refuge_entity.srid IS
    'Spatial reference identifier for geom. Always 4326 for this table.';

-- ============================================================
-- VERIFICATION
-- ============================================================
\echo ''
\echo 'Column inventory:'
SELECT table_name, column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('du_urban_entity', 'du_agriculture_entity', 'du_refuge_entity')
  AND column_name IN ('geom', 'geom_wkt', 'srid', 'has_gis_data')
ORDER BY table_name, column_name;

\echo ''
\echo 'GiST indexes:'
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname IN (
      'idx_du_urban_entity_geom',
      'idx_du_agriculture_entity_geom',
      'idx_du_refuge_entity_geom'
  )
ORDER BY tablename;

COMMIT;
