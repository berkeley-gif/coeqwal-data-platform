# Database geometry pattern

**Rule:** all PostGIS geometry lives in **dedicated geometry tables**. Entity
tables hold attributes only (`du_id`, acres, gw/sw, agency text, etc.).

Related: [`database/schema/COEQWAL_SCENARIOS_DB_ERD.md`](../database/schema/COEQWAL_SCENARIOS_DB_ERD.md),
[`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py)

---

## Canonical pattern

Every map layer follows the same shape as `reservoir` + `reservoir_entity`:

| Role | Table kind | Example |
|---|---|---|
| Attributes | `*_entity` or lookup row | `du_urban_entity`, `reservoir_entity` |
| Geometry | dedicated table keyed by stable id | `reservoir`, `wba`, `network_gis` |

Standard geometry column triple:

```
geom_wkt  TEXT
srid      INTEGER  (4326)
geom      geometry(..., 4326)
```

Plus `CREATE INDEX ... ON <table> USING GIST (geom)`.

### Geometry tables today (correct)

| Table | Geometry type | Key | Tier map |
|---|---|---|---|
| `network_gis` | `POINT` | `short_code` | Network nodes |
| `wba` | `POLYGON` | `wba_id` | Water budget areas |
| `reservoir` | `POLYGON` | `calsim_short_code` | Reservoir footprints |
| `compliance_station` | `POINT` | `station_code` | Delta compliance stations |

### Tier resolution (target)

| location_type | Attributes from | Geometry from |
|---|---|---|
| `network_node` | `network` | `network_gis` |
| `demand_unit` (CWS_DEL) | `du_urban_entity` | **`du_urban`** (planned) |
| `demand_unit` (AG_REV) | `du_agriculture_entity` | **`du_agriculture`** (planned) |
| `demand_unit` (refuge) | `du_refuge_entity` | **`du_refuge`** (planned) |
| `reservoir` | `reservoir_entity` | `reservoir` |
| `wba` | `wba` | `wba` |
| `compliance_station` | `compliance_station` | `compliance_station` |

Names in the **planned** row follow the `reservoir` precedent: a short
geometry table name keyed by the same id the entity table uses (`du_id` or
`calsim_short_code`).

---

## Drift to fix (action item)

Migration [`56_add_du_geometry_columns.sql`](../database/scripts/sql/56_add_du_geometry_columns.sql)
added `geom` / `geom_wkt` / `srid` directly onto:

- `du_urban_entity`
- `du_agriculture_entity`
- `du_refuge_entity`

That contradicts the dedicated-table rule. **`load_du_geometries.py` must not
be run on production.** Treat migration 56 as a mistaken spike to be reversed.

### Action checklist

1. **Design** three dedicated tables (or one `demand_unit_geometry` with a
   `du_class` discriminator - pick one approach and document it in the ERD):
   - Urban footprints keyed by `du_id`
   - Agriculture footprints keyed by `du_id`
   - Refuge footprints keyed by `du_id`
2. **SQL migration** create tables + GiST indexes (mirror `reservoir` DDL style).
3. **Retire migration 56** drop `geom`, `geom_wkt`, `srid` from the three
   entity tables once the dedicated tables exist and are loaded.
4. **Refactor** [`load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py)
   to write the dedicated geometry tables, not entity rows.
5. **Refactor** [`tier_location_entities.py`](../etl/common/tier_location_entities.py)
   `GeometryResolver.table` for `demand_unit` from `du_urban_entity` /
   `du_agriculture_entity` to the new geometry tables.
6. **CWS delivery:** when `cws_entity` lands, put PWS **points in a dedicated
   geometry table** (not on `cws_entity` attribute rows). Same rule as DU.
7. **Footprint policy** (dissolved gpkg vs multipart vs PWS union) is still
   open. See [`docs/du_polygon_mapping.md`](du_polygon_mapping.md). Policy
   affects loader logic, not whether geometry belongs on entity tables.

---

## `has_gis_data` on entity rows

Entity tables may keep `has_gis_data BOOLEAN` as a **catalog flag** ("we
expect a polygon in the geometry table for this id"). It is not a substitute
for a geometry column on the entity row.

---

## For new developers

- **Do** add a dedicated geometry table and point `tier_location_entities.py` at it.
- **Do not** add `geom` columns to `*_entity` tables.
- **Do not** load DU polygons until the dedicated-table migration exists.
