# Demand-unit geometry

How demand-unit (DU) polygon geometry is stored in the COEQWAL database today: which tables hold it, the column shape, current coverage, and how it is consumed. Verified against the May 2026 monthly audit snapshot and the current schema.

## Why geometry is on the entity tables

The `geom` columns were added to the demand-unit entity tables in anticipation of serving polygons through the API for the get-started animation, which draws demand units as SVG overlays. That API path turned out to be unnecessary. The animation reads polygon coordinates straight from the Mapbox `demand-units` vector tiles with `querySourceFeatures` and builds the SVGs from those (coeqwal-website `apps/main/app/features/scenarioExplorer/animation/TierAnimationSection.tsx`, ring extraction in `extractOuterRing`). Nothing in the API or frontend now reads the DB geometry, so it no longer earns its place on the attribute tables and should move to dedicated geometry tables (see [Roadmap](#roadmap)).

## Where it lives

DU geometry is stored directly on the three demand-unit entity tables: `du_urban_entity`, `du_agriculture_entity`, and `du_refuge_entity`. Each carries the same geometry columns (defined by [`56_add_du_geometry_columns.sql`](../sql_archive/04_scenario/56_add_du_geometry_columns.sql)):

| Column | Type | Notes |
|---|---|---|
| `geom` | `geometry(MultiPolygon, 4326)` | dissolved DU footprint |
| `geom_wkt` | `text` | WKT mirror of `geom` (`ST_AsText(geom)`) |
| `srid` | `integer` | always 4326 |
| `has_gis_data` | `boolean` | catalog flag, a polygon is expected for this `du_id` |

Each table also has a GiST index `idx_<table>_geom` on its `geom` column. All DU geometry is EPSG:4326 (WGS84 lon/lat).

The other spatial layers keep geometry in standalone tables. `wba`, `compliance_station`, and `network_gis` are their own tables, and `reservoir` holds NHD-sourced polygons referenced by `reservoir_entity`. Demand-unit geometry is the exception, stored on the entity row for the reason above.

## Coverage

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

## How it is consumed

- The API does not serve GeoJSON for DU tiers (CWS_DEL, AG_REV). The tier endpoint returns tier levels keyed by `du_id`, with no geometry. See [`tier_endpoints.py`](../../api/coeqwal-api/routes/tier_endpoints.py).
- The live map draws DU polygons from a Mapbox vector tileset (`demand-units`) keyed by `DU_ID`, not from the `geom` column.
- The `geom` column is the reference for building that tileset. It is read by the tile-build pipeline, not by the running API.
- Consequence: a NULL `geom` in the DB does not by itself blank a polygon on the map (the Mapbox tile may still carry it), and populating `geom` does not change the live map until the tileset is rebuilt.

## Roadmap

Move DU geometry off the attribute tables into dedicated geometry tables (the standalone-table pattern the other layers use). The columns were added for an API path the get-started animation made unnecessary (see [Why geometry is on the entity tables](#why-geometry-is-on-the-entity-tables)), so the geometry no longer earns a place on `du_*_entity`. Tracked in [`SCHEMA_BACKLOG.md` § 6f](../SCHEMA_BACKLOG.md#6f-demand-unit-geometry-denormalized-on-entity-rows).

## Appendix: DU_ids without geometry

The 72 `du_id`s with `geom` NULL in the May 2026 snapshot.

### Urban (59)

`26N_NU513`, `60N_PA`, `60N_PU1`, `60S_PA`, `60S_PU`, `61_PA`, `61_PU1`, `61_PU2`, `63_PA`, `63_PR`, `64_PA`, `64_PU`, `65_PA`, `65_PU`, `70_PA`, `70_PU1`, `71_PA`, `72_PU`, `72_PU1`, `90_PU5`, `AMADR`, `AMCYN`, `ANTOC`, `BNCIA`, `CCWD`, `CCWDI`, `CLLPT`, `CSB038`, `CSB103`, `CSPSO`, `CSTIC`, `CWD`, `EBMUD`, `ELDID_NU1`, `ELDID_NU2`, `ELDID_NU3`, `ESB324`, `ESB347`, `ESB414`, `ESB415`, `ESB420`, `FRFLD`, `GDPUD_NU`, `GRSVL`, `JLIND`, `MWD`, `NAPA`, `NAPA2`, `PCWA3`, `PINES`, `PLMAS`, `SBA029`, `SBA036`, `SCVWD`, `SUISN`, `TVAFB`, `UPANG`, `VLLJO`, `WLDWD`

### Agriculture (12)

`63_NA5`, `91_PA`, `CULOS`, `CVP`, `GDPUD_NA`, `LCCWD`, `NIDBR_NA`, `NIDDC_NA1`, `NIDDC_NA2`, `NIDDC_NA3`, `PUTAH`, `SIDSH`

### Refuge (1)

`91_PR`
