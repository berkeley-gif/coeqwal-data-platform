## Database geometry pattern (roadmap)

This is the **target** architecture for PostGIS geometry storage. It is
not the current implementation for demand units. Treat this doc as a
design reference for future refactor work.

Related: [`database/schema/COEQWAL_SCENARIOS_DB_ERD.md`](../database/schema/COEQWAL_SCENARIOS_DB_ERD.md),
[`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py)

---

## Rule

PostGIS geometry should live in **dedicated geometry tables**. Entity
tables would hold attributes only (`du_id`, acres, gw/sw, agency text,
etc.).

Every map layer would follow the same shape as `reservoir` +
`reservoir_entity`:

| Role | Table kind | Example (current) |
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

### Geometry tables that already follow this pattern

| Table | Geometry type | Key | Tier map |
|---|---|---|---|
| `network_gis` | `POINT` | `short_code` | Network nodes |
| `wba` | `POLYGON` | `wba_id` | Water budget areas |
| `reservoir` | `POLYGON` | `calsim_short_code` | Reservoir footprints |
| `compliance_station` | `POINT` | `station_code` | Delta compliance stations |

---

## Current state for demand-unit geometry

Migration [`56_add_du_geometry_columns.sql`](../database/scripts/sql/.archive/56_add_du_geometry_columns.sql)
added `geom` / `geom_wkt` / `srid` columns directly onto the three
demand-unit entity tables:

- `du_urban_entity`
- `du_agriculture_entity`
- `du_refuge_entity`

That migration is **applied to live RDS** and
[`load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py)
populates those columns from `database/seed_tables/03_GIS/du_4326.gpkg`.

This contradicts the dedicated-table rule, but it works and is not
hurting anything today. The refactor is on the roadmap (see
[`docs/statistics_roadmap.md`](statistics_roadmap.md)) but is not in
flight.

---

## Future refactor (roadmap, not started)

If a future developer picks this up, the rough shape is:

1. **Design** three dedicated tables (or one `demand_unit_geometry` with
   a `du_class` discriminator - pick one approach and document it in
   the ERD):
   - Urban footprints keyed by `du_id`
   - Agriculture footprints keyed by `du_id`
   - Refuge footprints keyed by `du_id`
2. **SQL migration** to create the tables + GiST indexes (mirror
   `reservoir` DDL style) and one to migrate data from the entity
   columns into the new tables.
3. **Refactor** [`load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py)
   to write the dedicated geometry tables, not entity rows.
4. **Refactor** [`tier_location_entities.py`](../etl/common/tier_location_entities.py)
   `GeometryResolver.table` for `demand_unit` from `du_urban_entity` /
   `du_agriculture_entity` to the new geometry tables. The resolver
   feeds the Mapbox tile-build pipeline; the API does not consume
   geometry from it (see geometry policy in `database/README.md`).
5. **Drop** `geom`, `geom_wkt`, `srid` from the three `du_*_entity`
   tables once the dedicated tables exist and are loaded (reverse 56).
6. **CWS delivery:** when `cws_entity` lands, put PWS **points in a
   dedicated geometry table** (not on `cws_entity` attribute rows).
   Same rule as DU.

Footprint policy (dissolved gpkg vs multipart vs PWS union) is a
separate open question. See [`docs/du_polygon_mapping.md`](du_polygon_mapping.md).
Policy affects loader logic, not whether geometry belongs on entity
tables.

---

## `has_gis_data` on entity rows

Entity tables already carry `has_gis_data BOOLEAN` as a **catalog flag**
("we expect a polygon for this id"). When the refactor happens, this
flag stays on the entity row. It would not be a substitute for a
geometry column on the entity row.
