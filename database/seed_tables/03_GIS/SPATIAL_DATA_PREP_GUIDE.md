# Spatial data (03_GIS)

Geometry seed data for the tier map visualization and for demand-unit polygons. The CSVs here were prepared from raw GIS sources (see "Source data" below) and loaded into dedicated PostGIS tables. The "files to create" task this guide originally described is done.

All geometries are in SRID 4326 (EPSG:4326).

## Files

| File | Loads into | Loader |
|------|-----------|--------|
| `reservoirs.csv` | `reservoirs` table | [`../../sql_archive/02_network_layer/load_spatial_tables.sql`](../../sql_archive/02_network_layer/load_spatial_tables.sql) |
| `wba.csv` | `wba` table | same |
| `compliance_stations.csv` | `compliance_stations` table | same |
| `du_4326.gpkg` | geometry columns on `du_urban_entity`, `du_agriculture_entity`, `du_refuge_entity` | [`../../scripts/data_processing/load_du_geometries.py`](../../scripts/data_processing/load_du_geometries.py) |

## Spatial tables

`load_spatial_tables.sql` creates `reservoirs`, `wba`, and `compliance_stations`, loads the matching CSV, then converts the `geom_wkt` column to a PostGIS `geom` with `ST_GeomFromText(geom_wkt, srid)` and builds a GIST index. These are visualization tables, distinct from the Layer 03 entity tables `reservoir` and `compliance_station`. The loader uses the older `aws_s3.table_import_from_s3` path. The rest of the repo has moved to `\copy`, so a from-scratch rebuild needs the CSVs reachable by that import or the load rewritten to `\copy`.

Columns by file:

- `reservoirs.csv`: `calsim_short_code`, `reservoir_name`, `geom_wkt`, `srid`, `area_sqkm`, `elevation_m`, `gnis_id`, `nhd_permanent_id`, `data_source`. `MULTIPOLYGON` geometries. For reservoirs with multiple NHD polygons (Shasta, Folsom) the largest was kept.
- `wba.csv`: `wba_id`, `wba_name`, `geom_wkt`, `srid`, `area_acres`, `hydrologic_region`, `comments`, `data_source`. `MULTIPOLYGON` Water Budget Area geometries.
- `compliance_stations.csv`: `station_code`, `station_name`, `latitude`, `longitude`, `srid`, `tier_use`, `geom_wkt`, `data_source`, `notes`. Jersey Point (`JP`) and Emmaton (`EM`), both `tier_use = FW_DELTA_USES`, added manually because they are not nodes in the CalSim network.

## Demand-unit geometries (du_4326.gpkg)

A GeoPackage with a `demandunits` layer of dissolved `MULTIPOLYGON` features, one per `DU_ID`. The schema migration [`../../sql_archive/04_scenario/56_add_du_geometry_columns.sql`](../../sql_archive/04_scenario/56_add_du_geometry_columns.sql) adds `geom_wkt`, `srid`, a `geometry(MultiPolygon, 4326)` column, and a GIST index to each of `du_urban_entity`, `du_agriculture_entity`, and `du_refuge_entity`. The loader strips the GeoPackage GPB header from each blob and writes the WKB to whichever entity table holds the matching `du_id` via `ST_GeomFromWKB(wkb, 4326)`. Dry-run with `--dry-run` first.

Not every demand unit has a polygon. The covered, missing, and gpkg-only IDs (and the `26N_NA` urban / agriculture overlap) are enumerated in [`geometry.md`](../../topic_docs/geometry.md#coverage).

## Tier coverage

The spatial tables back the tier map. Reservoir storage tiers (`RES_STOR`) use `reservoirs`, groundwater storage tiers (`GW_STOR`) use `wba`, and in-Delta use tiers (`FW_DELTA_USES`) use `compliance_stations`. Environmental-flow (`ENV_FLOWS`) and Delta-export (`FW_EXP`) tier locations resolve to existing CalSim network nodes in `network_gis`, so they need no separate geometry file here.

## Source data

The CSVs were derived from raw GIS exports under `data/raw/from_geopackage/`:

- Reservoir polygons: [`../../../data/raw/from_geopackage/GIS_coords_from_other_sources/reservoirs_from_nhd.csv`](../../../data/raw/from_geopackage/GIS_coords_from_other_sources/reservoirs_from_nhd.csv)
- WBA polygons: [`../../../data/raw/from_geopackage/wba_4326.csv`](../../../data/raw/from_geopackage/wba_4326.csv)
- Compliance-station coordinates were researched and entered by hand.
