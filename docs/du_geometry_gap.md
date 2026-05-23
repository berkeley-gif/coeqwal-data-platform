# Demand-unit geometry coverage gap

Tracks which `DU_ID`s in `du_urban_entity`, `du_agriculture_entity`, and
`du_refuge_entity` are *not* covered by the demand-unit geopackage we ship
geometries from.

## Source of geometry

`reference/du_4326.gpkg`

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
| `du_urban_entity`        |        125 |              84 |  **41**  |
| `du_agriculture_entity`  |        144 |             132 |  **12**  |
| `du_refuge_entity`       |         18 |              17 |   **1**  |
| **Combined (distinct)**  |    **286** |         **232** |  **54**  |

Geopackage-only IDs (in `du_4326.gpkg` but not in any DB entity table; 3):
`07S_PA`, `50_NA`, `90_NA`. These are not currently loaded as
demand-unit entities and the geometry ingest will skip them.

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

## Missing `DU_ID`s (54)

These will be skipped at ingest time and logged. They need geometries
collected from the responsible agencies or a newer dissolve of the
source GIS layers before the geometry coverage of the registry can
reach 100%.

### Urban (41)

- `26N_NU513`
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

The entity-table `has_gis_data` flags do not line up exactly with the
gpkg coverage:

| Entity table             | `has_gis_data = True` | Covered by gpkg |
| ------------------------ | --------------------: | --------------: |
| `du_urban_entity`        |                    84 |              84 |
| `du_agriculture_entity`  |                   130 |             132 |
| `du_refuge_entity`       |                    17 |              17 |

The two extra agriculture rows covered by the gpkg but flagged
`has_gis_data = False` should be reviewed and the flag corrected when
the geometry load is run.

## How to regenerate this report

Run the loader in dry-run mode for the current numbers:

```
python database/scripts/data_processing/load_du_geometries.py --dry-run
```

It prints per-table `matched` and `missing in gpkg` counts and the
`du_id` lists they correspond to. The numbers in this doc should
shrink to zero over time as new polygons are added to
`reference/du_4326.gpkg` (or a successor table).

## Wiring

- Schema: [`database/scripts/sql/56_add_du_geometry_columns.sql`](../database/scripts/sql/56_add_du_geometry_columns.sql) adds `geom_wkt TEXT`, `srid INTEGER`, `geom geometry(MultiPolygon, 4326)`, and `idx_<table>_geom USING GIST` to each of the three `du_*_entity` tables.
- Loader: [`database/scripts/data_processing/load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py) reads `reference/du_4326.gpkg`, strips the GeoPackage GPB header from each `geom` blob, and writes the resulting WKB to whichever entity tables contain the `du_id` via `ST_GeomFromWKB(wkb, 4326)`. `geom_wkt` is materialized server-side from the resulting geometry.
- Registry: [`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py) routes the lookup with `TIER_GEOMETRY_OVERRIDES` (mirroring the existing `TIER_ATTRIBUTE_OVERRIDES` pattern): `AG_REV` -> `du_agriculture_entity`, everything else -> `du_urban_entity`.
- API: [`api/coeqwal-api/routes/tier_map_endpoints.py`](../api/coeqwal-api/routes/tier_map_endpoints.py) mirrors the registry in raw SQL when assembling GeoJSON FeatureCollections. There is no fallback to `network_gis` for missing DUs; they are dropped from the response and surface here.
