# Geometry

How spatial geometry is stored across the COEQWAL database: the column convention every geometry table shares, which tables keep geometry in a dedicated table versus on the entity row, how it is consumed, and the target layout. The coverage and gap sections focus on demand units (DUs), where the gaps are. Verified against the May 2026 monthly audit snapshot and the current schema.

## How geometry is represented now

Every geometry-bearing table shares the same column triad: `geom` (the PostGIS `geometry` value, GiST-indexed), `geom_wkt` (a `text` copy of `geom` via `ST_AsText`), and `srid` (always `4326`, WGS84 lon/lat). Storing `geom_wkt` next to `geom` keeps the geometry twice, in two formats, on every row.

Where that triad lives splits the schema in two:

| Table | Geometry kind | Storage pattern |
|---|---|---|
| `network_gis` | POINT | dedicated geometry table, FK `network_id` -> `network.id` (1:1, mirrored by `network.has_gis`) |
| `reservoir` | MULTIPOLYGON Z | separate table, soft-linked to `reservoir_entity` by short_code (no FK) |
| `wba` | POLYGON | inline on the entity row |
| `compliance_station` | POINT | inline on the entity row (also keeps `latitude` / `longitude`) |
| `du_urban_entity` | MULTIPOLYGON | inline on the entity row |
| `du_agriculture_entity` | MULTIPOLYGON | inline on the entity row |
| `du_refuge_entity` | MULTIPOLYGON | inline on the entity row |

Only `network_gis` and `reservoir` keep geometry in a table of its own. The other five (`wba`, `compliance_station`, and the three `du_*_entity` tables) carry the triad inline on the entity row, so the storage pattern is the same across all five. The demand-unit tables are the ones flagged for cleanup, for two reasons specific to them: their footprints are by far the largest (dissolved MultiPolygons, the cells that overflow the audit CSV export), and their entity rows are read constantly for non-spatial data (gw/sw flags, contractor links, statistics, tiers, the API), so the heavy `geom` is dead weight on a hot table. `wba` and `compliance_station` share the inline pattern but are smaller and rarely read for non-spatial work, so they are lower priority.

The three demand-unit entity tables carry the triad plus a catalog flag (added by [`56_add_du_geometry_columns.sql`](../sql_archive/04_scenario/56_add_du_geometry_columns.sql)):

| Column | Type | Notes |
|---|---|---|
| `geom` | `geometry(MultiPolygon, 4326)` | dissolved DU footprint |
| `geom_wkt` | `text` | WKT mirror of `geom` (`ST_AsText(geom)`) |
| `srid` | `integer` | always 4326 |
| `has_gis_data` | `boolean` | catalog flag, a polygon is expected for this `du_id` |

Each DU table also has a GiST index `idx_<table>_geom`. The columns were added in anticipation of serving polygons through the API for the get-started animation, but that path was never needed: the animation reads polygon coordinates straight from the Mapbox `demand-units` vector tiles (`querySourceFeatures`) and builds the SVGs from those (coeqwal-website `apps/main/app/features/scenarioExplorer/animation/TierAnimationSection.tsx`).

**Geometry is latent at runtime.** Nothing queries it spatially (no `ST_Within` / `ST_Intersects` / `ST_Contains` anywhere) and no live API or frontend reads it. PostGIS is used for offline geometry construction in the loader (`load_du_geometries.py`, a database script, not part of the statistics ETL), for `ST_AsText` / `ST_AsGeoJSON` serialization, and for storage plus GiST indexes.

## How geometry should be represented

Demand-unit geometry should move off the entity rows into a dedicated geometry table, the pattern `network_gis` and `reservoir` already use. The target:

- One dedicated DU geometry table keyed by `(du_id, du_class)`, preferred over three per-class tables because `du_id` is not unique across classes (`26N_NA` exists in both urban and ag). Mirror the `reservoir` DDL.
- FK-enforce the link back to the entity rows, the way `network_gis.network_id` references `network.id` (the cleanest existing example), rather than the soft, unenforced link `reservoir` uses.
- Drop the redundant `geom_wkt` text mirror. Regenerate WKT on demand with `ST_AsText` if a consumer needs it.
- Apply the same rule to CWS: when `cws_entity` lands, put PWS points in a dedicated geometry table, never on the `cws_entity` attribute row.


## Coverage

Polygons are loaded by the database script [`load_du_geometries.py`](../scripts/data_processing/load_du_geometries.py), which reads the geopackage [`seed_tables/03_GIS/du_4326.gpkg`](../seed_tables/03_GIS/du_4326.gpkg) and matches on `du_id` (the column migration [`56_add_du_geometry_columns.sql`](../sql_archive/04_scenario/56_add_du_geometry_columns.sql) is already applied to RDS). This is a SQL / script load, not part of the statistics ETL. When agency-sourced polygons arrive, add them to the geopackage and rerun the loader.

Not every DU row has a polygon. From the May 2026 audit snapshot:

| Table | Rows | `geom` populated | `geom` NULL |
|---|---:|---:|---:|
| `du_urban_entity` | 145 | 86 | 59 |
| `du_agriculture_entity` | 144 | 132 | 12 |
| `du_refuge_entity` | 18 | 17 | 1 |
| **Combined (distinct)** | **306** | **234** | **72** |

Distinct counts net out `26N_NA`, the one `du_id` present in both the urban and ag tables. In this snapshot `has_gis_data` equals `geom IS NOT NULL` on every table (86, 132, 17), so the catalog flag and the actual geometry agree with no drift. To re-check current state:

```sql
SELECT 'du_urban_entity' AS table_name,
       count(*) FILTER (WHERE geom IS NOT NULL) AS geom_not_null,
       count(*) FILTER (WHERE has_gis_data)     AS has_gis_data_true
FROM du_urban_entity
UNION ALL
SELECT 'du_agriculture_entity',
       count(*) FILTER (WHERE geom IS NOT NULL),
       count(*) FILTER (WHERE has_gis_data)
FROM du_agriculture_entity
UNION ALL
SELECT 'du_refuge_entity',
       count(*) FILTER (WHERE geom IS NOT NULL),
       count(*) FILTER (WHERE has_gis_data)
FROM du_refuge_entity;
```

The `du_id`s with no polygon are in the [appendix](#appendix-du_ids-without-geometry).

## `26N_NA` (in two tables)

`26N_NA` is the only `du_id` present in both `du_urban_entity` and `du_agriculture_entity`. Both rows carry the same dissolved `MULTIPOLYGON`, so either table returns the same geometry for it.

## Referenced by tier_location but not present

`tier_location` (the tier map catalog) points at a few `du_id`s that have no row in the entity tables at all, so they have neither attributes nor geometry:

- AG_REV: `07S_PA`
- CWS_DEL: `ACFC`, `KCWA`, `MHILL_NU`, `SBCWD`, `SVWRD`, `TLMNE`, `UNION`

These appear in tier results but cannot be drawn from DB geometry until the entity rows exist.


## Roadmap

The target layout is in [How geometry should be represented](#how-geometry-should-be-represented), tracked as [`SCHEMA_BACKLOG.md` § 6f](../SCHEMA_BACKLOG.md#6f-demand-unit-geometry-denormalized-on-entity-rows). Pickup steps:

1. Decide the table layout (three tables vs one `demand_unit_geometry` with a `du_class` discriminator) and update the ERD.
2. Write the CREATE migration for the new geometry tables (mirror the `reservoir` DDL style). Draft files `58_create_du_geometry_tables.sql` and `59_migrate_du_geom_off_entity_tables.sql` (neither is on disk today).
3. Refactor [`load_du_geometries.py`](../scripts/data_processing/load_du_geometries.py) to write the dedicated tables.
4. Update the `GeometryResolver` for `demand_unit` in [`etl/common/tier_location_entities.py`](../../etl/common/tier_location_entities.py) to point at the new tables. The resolver feeds the Mapbox tile-build pipeline. The API does not consume it for geometry.
5. Data migration: copy any existing rows from `du_*_entity.geom` into the new tables.
6. Drop `geom`, `geom_wkt`, `srid`, and the GiST index from the three `du_*_entity` tables.
7. **CWS rollout follow-on:** when `cws_entity` lands, put PWS points in a dedicated geometry table, not on `cws_entity` attribute rows. Same rule as DU.

## Appendix: DU_ids without geometry

The 72 `du_id`s with `geom` NULL in the May 2026 snapshot.

### Urban (59)

`26N_NU513`, `60N_PA`, `60N_PU1`, `60S_PA`, `60S_PU`, `61_PA`, `61_PU1`, `61_PU2`, `63_PA`, `63_PR`, `64_PA`, `64_PU`, `65_PA`, `65_PU`, `70_PA`, `70_PU1`, `71_PA`, `72_PU`, `72_PU1`, `90_PU5`, `AMADR`, `AMCYN`, `ANTOC`, `BNCIA`, `CCWD`, `CCWDI`, `CLLPT`, `CSB038`, `CSB103`, `CSPSO`, `CSTIC`, `CWD`, `EBMUD`, `ELDID_NU1`, `ELDID_NU2`, `ELDID_NU3`, `ESB324`, `ESB347`, `ESB414`, `ESB415`, `ESB420`, `FRFLD`, `GDPUD_NU`, `GRSVL`, `JLIND`, `MWD`, `NAPA`, `NAPA2`, `PCWA3`, `PINES`, `PLMAS`, `SBA029`, `SBA036`, `SCVWD`, `SUISN`, `TVAFB`, `UPANG`, `VLLJO`, `WLDWD`

### Agriculture (12)

`63_NA5`, `91_PA`, `CULOS`, `CVP`, `GDPUD_NA`, `LCCWD`, `NIDBR_NA`, `NIDDC_NA1`, `NIDDC_NA2`, `NIDDC_NA3`, `PUTAH`, `SIDSH`

### Refuge (1)

`91_PR`
