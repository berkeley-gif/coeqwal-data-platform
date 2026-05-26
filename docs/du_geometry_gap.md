# Demand-unit geometry coverage gap

Summary counts for polygon coverage. For alias/dissolve rules (Pattern A/B)
and the Pattern C sourcing roadmap, see [`docs/du_polygon_mapping.md`](du_polygon_mapping.md).

Tracks which `DU_ID`s in `du_urban_entity`, `du_agriculture_entity`, and
`du_refuge_entity` are *not* covered by the demand-unit geopackage we ship
geometries from.

## Source of geometry

`database/seed_tables/03_GIS/du_4326.gpkg`

- Layer: `demandunits` (236 rows; one row has a NULL `DU_ID` and is ignored)
- CRS: EPSG:4326
- Geometry type: `MULTIPOLYGON`
- Columns retained: `DU_ID`, `OBJECTID`, `Shape_Leng`, `Shape_Area`, `geom`
- One polygon per `DU_ID` (the gpkg is already dissolved). The sibling
  `data/raw/from_geopackage/du_4326.csv` carries the same geometries
  pre-dissolve (1666 sub-polygons, same 235 `DU_ID`s) plus the source
  attributes (`Class`, `CS3_Type`, `WBA_ID`, `WDNAME`, etc.).

## Coverage scorecard

| Entity table             | Rows in DB | Covered by gpkg | Missing  |
| ------------------------ | ---------: | --------------: | -------: |
| `du_urban_entity`        |        145 |              86 |  **59**  |
| `du_agriculture_entity`  |        144 |             132 |  **12**  |
| `du_refuge_entity`       |         18 |              17 |   **1**  |
| **Combined (distinct)**  |    **306** |         **234** |  **72**  |

Distinct counts net out the one `du_id` that appears in two entity
tables (`26N_NA` in urban + agriculture).

The urban table is 20 rows larger than the seed CSV at
`database/seed_tables/04_calsim_data/du_urban_entity.csv` (125 rows).
Those 20 additional rows arrived in RDS through a path other than the
seed reload (suspected bulk INSERT of the `60N_*`, `60S_*`, `61_*`,
`63_*`, `64_*`, `65_*`, `70_*`, `71_*`, `72_*` family). 2 of the 20
have polygons in the gpkg and 18 do not, which accounts for the urban
"missing" count growing from 41 to 59. The seed CSV should be
regenerated from RDS, or the side-channel inserter should be tracked
down. See "Reconciling `has_gis_data`" below.

Geopackage-only IDs (in `du_4326.gpkg` but not in any DB entity table; 3):
`07S_PA`, `50_NA`, `90_NA`. These are not currently loaded as
demand-unit entities and the geometry ingest will skip them. `07S_PA`
is *also* referenced by the AG_REV tier staging CSV (see "Tier
staging IDs not in entity tables" below) - adding it to
`du_agriculture_entity` would close both gaps at once.

## Impact analysis - what users currently see

Question: do these gaps reach end users on the map and in panels?

Tier results render in the website via the Mapbox tileset path
(`outcomeLayerRegistry.ts` -> `usePolygonTooltip`). Polygons come from
the Mapbox vector tile `demand-units` keyed by `DU_ID`. Tier levels
come from the API `/api/tiers/scenarios/{scenario}/locations` endpoint
and are joined client-side by `DU_ID`. CWS_DEL, AG_REV, GW_STOR,
RES_STOR, DELTA_ECO all render this way (the registry marks each
`geometryType: "polygon"` with `requiresIdMatching: true` for the
per-feature outcomes).

The API does not serve GeoJSON for these tiers (or any tier). Geometry
is only consumed by the Mapbox tile-build pipeline at ETL time; the
gaps below therefore manifest as missing tile features at render time,
not as missing API rows.

What that means for each gap category:

| Gap                                                                          | User impact today                                                                                                                                                                                                                                                                       |
| ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Missing entity row** (7 CWS_DEL ids: `ACFC`, `KCWA`, `MHILL_NU`, `SBCWD`, `SVWRD`, `TLMNE`, `UNION`; 1 AG_REV id: `07S_PA`) | The `/locations` response still returns these rows (they exist in `tier_location_result`). Treemaps, tables, equity panels that use `useTierLocationAssignments` show them. On the map, visibility depends entirely on whether the Mapbox tileset carries a feature with that `DU_ID`. If yes, polygon colors by tier; if no, location is invisible on the map. |
| **Missing geometry (`geom IS NULL`)** (59 urban, 12 ag, 1 refuge)             | Server-side `/{scenario}/{tier}` GeoJSON drops these features (`WHERE geom IS NOT NULL`). Since CWS_DEL and AG_REV don't use that endpoint today, the gap is currently invisible to users. Same Mapbox-tile question as above for map visibility. The gap becomes user-visible only if a future feature migrates to server-side GeoJSON or fetches polygons from the API. |
| **Mapbox tileset missing `DU_ID`** (unknown count; needs a tileset cross-check) | This is the **actual user-visible gap on the CWS_DEL/AG_REV map**. A tier row exists, the polygon does not. Counted in `/locations`, absent from the map. Requires a separate audit against the deployed tileset.                                                                       |
| **Tier row uses an ID not in any source**                                     | The API returns the row in `/locations`, but with `requiresIdMatching: true` the polygon tooltip handler returns `null` for it (`tierLevel === 0 ? return null`). Polygon (if present in the tile) renders at default style with no tier color and no tooltip.                            |

Where the DB-side geometry **does** matter for users today:

- `RES_STOR` (server-side GeoJSON from `reservoir.geom`). Audit row counts show full coverage.
- `GW_STOR` (server-side GeoJSON from `wba.geom`). Audit row counts show full coverage.
- `ENV_FLOWS` server variant (`network_gis.geom`). Live frontend uses React markers instead, so the gap would only surface if a server-side variant ships.
- `compliance_station` (FW_DELTA_USES). Same React-marker story.

Bottom line: closing the 59 urban polygon gaps will not light up new polygons on the website until either (a) the Mapbox tileset is regenerated from the dataset that includes the new geometries, or (b) the frontend switches CWS_DEL/AG_REV from Mapbox tile rendering to server-side GeoJSON. The DB-side fix is still worth doing because it is the source of truth for any future re-tile or migration and because the tier-level reconciliation work requires the entity rows to exist.

Recommended next step (separate work item, not in this section's scope): audit the deployed `coeqwal.demand-units` Mapbox tileset to enumerate which `DU_ID`s it actually carries, then compare against the `tier_location_result` `location_id` set. The intersection of "in tier_location_result but not in Mapbox tile" is the real user-visible CWS_DEL/AG_REV gap.

## Tier staging IDs not in entity tables (attribute gaps)

The tier-team staging CSVs in `etl/tier_data/staging/` reference
location_ids that are not in the corresponding entity tables yet.
Distinct from the "missing geometries" lists below: those `du_id`s
exist as rows but lack polygons; the IDs here have *no row at all*.
[`etl/tier_data/scripts/sync_tier_locations_from_staging.py`](../etl/tier_data/scripts/sync_tier_locations_from_staging.py)
refuses to write them without `--allow-unresolved` because a dangling
pointer in `tier_location` would surface as a missing tile in the API.

Status: discovered 2026-05-23 via
[`etl/tier_data/scripts/audit_tier_location_geometry.py`](../etl/tier_data/scripts/audit_tier_location_geometry.py).

### AG_REV -> `du_agriculture_entity` (1)

| `location_id` | Notes |
|---|---|
| `07S_PA` | Already exists in `du_4326.gpkg` (one of the three "Geopackage-only IDs" above). Adding the entity row would auto-load geometry on the next loader run. |

### CWS_DEL -> `du_urban_entity` (7)

| `location_id` | Provisional meaning (verify with tier team) |
|---|---|
| `ACFC` | Alameda County Flood Control |
| `KCWA` | Kern County Water Agency |
| `MHILL_NU` | Morgan Hill (non-utility) |
| `SBCWD` | Santa Barbara County Water Dept (?) |
| `SVWRD` | Silicon Valley Water Recharge District (?) |
| `TLMNE` | Telmnese (?) |
| `UNION` | Union (?) |

The "provisional meaning" column is best-effort decoding from the
short codes; the tier team should confirm canonical names and
attributes (`hydrologic_region_id`, etc.) before the entity rows
are inserted. None of these have polygons in `du_4326.gpkg` either,
so adding the entity rows alone fixes the attribute gap but the
geometry gap will need separate sourcing.

## `26N_NA` (urban / agriculture overlap)

`26N_NA` is the only `DU_ID` that appears in two entity tables
(`du_urban_entity` *and* `du_agriculture_entity`). The geopackage carries
a single dissolved `MULTIPOLYGON` for `26N_NA`. The
pre-dissolve CSV shows the same polygon set is tagged with mixed `Class`
(one `Agriculture` row, two `Urban` rows) under `WBA_ID = 26N`,
so the dissolved footprint legitimately spans both flavors.

Resolution: write the same geometry to both the `du_urban_entity` and
`du_agriculture_entity` rows for `DU_ID = '26N_NA'`. The load script
routes geometry updates by which entity table contains the `DU_ID`,
which naturally handles the dual write without a special case.

## Missing `DU_ID`s (72)

These will be skipped at ingest time and logged. They need geometries
collected from the responsible agencies or a newer dissolve of the
source GIS layers before the geometry coverage of the registry can
reach 100%.

### Urban (59)

- `26N_NU513`
- `60N_PA`
- `60N_PU1`
- `60S_PA`
- `60S_PU`
- `61_PA`
- `61_PU1`
- `61_PU2`
- `63_PA`
- `63_PR`
- `64_PA`
- `64_PU`
- `65_PA`
- `65_PU`
- `70_PA`
- `70_PU1`
- `71_PA`
- `72_PU`
- `72_PU1`
- `90_PU5`
- `AMADR`
- `AMCYN`
- `ANTOC`
- `BNCIA`
- `CCWD`
- `CCWDI`
- `CLLPT`
- `CSB038`
- `CSB103`
- `CSPSO`
- `CSTIC`
- `CWD`
- `EBMUD`
- `ELDID_NU1`
- `ELDID_NU2`
- `ELDID_NU3`
- `ESB324`
- `ESB347`
- `ESB414`
- `ESB415`
- `ESB420`
- `FRFLD`
- `GDPUD_NU`
- `GRSVL`
- `JLIND`
- `MWD`
- `NAPA`
- `NAPA2`
- `PCWA3`
- `PINES`
- `PLMAS`
- `SBA029`
- `SBA036`
- `SCVWD`
- `SUISN`
- `TVAFB`
- `UPANG`
- `VLLJO`
- `WLDWD`

The 18 `60N_*` through `72_*` entries are the new urban rows added to
RDS outside the seed CSV reload (see "Coverage scorecard" above). The
remaining 41 entries match the original list from when this doc was
first generated.

### Agriculture (12)

- `63_NA5`
- `91_PA`
- `CULOS`
- `CVP`
- `GDPUD_NA`
- `LCCWD`
- `NIDBR_NA`
- `NIDDC_NA1`
- `NIDDC_NA2`
- `NIDDC_NA3`
- `PUTAH`
- `SIDSH`

### Refuge (1)

- `91_PR`

## Reconciling `has_gis_data`

The loader (`database/scripts/data_processing/load_du_geometries.py`)
sets `has_gis_data = TRUE` for every row it writes a polygon to. After
a successful load, `has_gis_data = True` should equal the gpkg
coverage column for each table:

| Entity table             | `has_gis_data = True` after load | Covered by gpkg |
| ------------------------ | -------------------------------: | --------------: |
| `du_urban_entity`        |                               86 |              86 |
| `du_agriculture_entity`  |                              132 |             132 |
| `du_refuge_entity`       |                               17 |              17 |

The pre-load snapshot of this table had `du_agriculture_entity` with
`has_gis_data = True` set on only 130 rows despite 132 having polygons
in the gpkg; the loader silently corrects this on its next run.

To verify after a load:

```sql
SELECT
    table_name,
    SUM(CASE WHEN has_gis_data THEN 1 ELSE 0 END)  AS has_gis_data_true,
    SUM(CASE WHEN geom IS NOT NULL THEN 1 ELSE 0 END) AS geom_not_null,
    SUM(CASE WHEN geom IS NOT NULL AND ST_IsValid(geom) THEN 1 ELSE 0 END)
        AS geom_valid
FROM (
    SELECT 'du_urban_entity'       AS table_name, has_gis_data, geom FROM du_urban_entity
    UNION ALL
    SELECT 'du_agriculture_entity', has_gis_data, geom FROM du_agriculture_entity
    UNION ALL
    SELECT 'du_refuge_entity',      has_gis_data, geom FROM du_refuge_entity
) all_du
GROUP BY table_name
ORDER BY table_name;
```

## How to regenerate this report

Run the loader in dry-run mode for the current numbers:

```
python database/scripts/data_processing/load_du_geometries.py --dry-run
```

It prints per-table `matched` and `missing in gpkg` counts and the
`du_id` lists they correspond to. The numbers in this doc should
shrink to zero over time as new polygons are added to
`database/seed_tables/03_GIS/du_4326.gpkg` (or a successor table).

## Wiring

- Schema: [`database/scripts/sql/.archive/56_add_du_geometry_columns.sql`](../database/scripts/sql/.archive/56_add_du_geometry_columns.sql) adds `geom_wkt TEXT`, `srid INTEGER`, `geom geometry(MultiPolygon, 4326)`, and `idx_<table>_geom USING GIST` to each of the three `du_*_entity` tables.
- Loader: [`database/scripts/data_processing/load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py) reads [`database/seed_tables/03_GIS/du_4326.gpkg`](../database/seed_tables/03_GIS/du_4326.gpkg), strips the GeoPackage GPB header from each `geom` blob, and writes the resulting WKB to whichever entity tables contain the `du_id` via `ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_GeomFromWKB(wkb, 4326)), 3))`. The `ST_MakeValid` wrap repairs ring self-intersections and other OGC validity violations that the upstream dissolve leaves in place; `ST_CollectionExtract(..., 3)` keeps only polygon parts; `ST_Multi` guarantees a `MultiPolygon` matches the column type. `geom_wkt` is materialized server-side from the same expression.
- Registry: [`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py) routes the lookup with `TIER_GEOMETRY_OVERRIDES` (mirroring the existing `TIER_ATTRIBUTE_OVERRIDES` pattern): `AG_REV` -> `du_agriculture_entity`, everything else -> `du_urban_entity`. The registry feeds the Mapbox tile-build pipeline; the API consumes the attribute side of the same registry but not the geometry side.
- API: [`api/coeqwal-api/routes/tier_endpoints.py`](../api/coeqwal-api/routes/tier_endpoints.py) at `/api/tiers/scenarios/{scenario_id}/locations` returns the per-location tier assignments without geometry. Missing DUs in the tile (per the gap counts above) surface as map features that exist in the API payload but cannot be highlighted; the website skips them.
