# Database geometry pattern

Where map geometry lives in the COEQWAL schema, and why it looks inconsistent
at first glance.

Related: [`database/schema/COEQWAL_SCENARIOS_DB_ERD.md`](../database/schema/COEQWAL_SCENARIOS_DB_ERD.md),
[`etl/common/tier_location_entities.py`](../etl/common/tier_location_entities.py)

---

## The pattern (two tiers)

### Tier 1: Dedicated geometry tables

Geometry is the main reason the table exists. Entity **names** often live in a
separate `*_entity` table; geometry is keyed by a stable short code.

| Table | Geometry type | Key | Fed by | Tier map role |
|---|---|---|---|---|
| `network_gis` | `POINT` | `short_code` | Kart / geoschematic | Network node locations |
| `wba` | `POLYGON` | `wba_id` | `database/seed_tables/03_GIS/wba.csv` | Water budget areas |
| `reservoir` | `POLYGON` | `calsim_short_code` | NHD / seed GIS | Reservoir footprints (`SHSTA`, `OROVL`, `SLUIS`) |
| `compliance_station` | `POINT` | `station_code` | `compliance_stations.csv` | Delta compliance (`EM`, `JP`) |

Common column triple on these tables:

```
geom_wkt  TEXT
srid      INTEGER  (usually 4326)
geom      geometry(..., 4326)
```

Each has a GiST index on `geom`.

### Tier 2: Entity tables with optional footprint columns

CalSim **entity** tables hold attributes (acres, agency, gw/sw, etc.). Polygon
columns were added **later** for demand units only:

| Table | Geometry (migration 56) | Status |
|---|---|---|
| `du_urban_entity` | `geom`, `geom_wkt`, `srid` | Columns exist; **loading policy not approved** |
| `du_agriculture_entity` | same | same |
| `du_refuge_entity` | same | same |

These tables also have `has_gis_data BOOLEAN`, which predates the `geom`
columns and only means "we expect a polygon somewhere," not that `geom IS NOT NULL`.

### Planned (CWS delivery, not yet in DB)

| Table | Geometry type | Notes |
|---|---|---|
| `cws_entity` | `geometry(Point, 4326)` | One row per PWSID; see ERD draft |

---

## How tier maps resolve geometry

`tier_location` stores `(tier_code, location_type, location_id)` only. Display
name and geometry come from joins documented in
[`tier_location_entities.py`](../etl/common/tier_location_entities.py):

| location_type | Name from | Geometry from |
|---|---|---|
| `network_node` | `network` | `network_gis.geom` |
| `demand_unit` (CWS_DEL) | `du_urban_entity` | `du_urban_entity.geom` (when loaded) |
| `demand_unit` (AG_REV) | `du_agriculture_entity` | `du_agriculture_entity.geom` |
| `reservoir` | `reservoir_entity` | `reservoir.geom` (separate table) |
| `wba` | `wba` | `wba.geom` |
| `compliance_station` | `compliance_station` | `compliance_station.geom` |

**Reservoirs use Tier 1** (geometry table separate from `reservoir_entity`).
**Demand units use Tier 2** (geometry on the entity row). That split is the
main source of confusion.

---

## Why DU entity tables did not ship with `geom`

1. M&I/ag/refuge entity tables were created from **CSV attribute ingest**
   (`12_mi_statistics/01_create_du_urban_entity.sql`, etc.).
2. `has_gis_data` was added as a flag; polygons lived in the external Kart repo
   and `du_4326.gpkg`.
3. Migration [`56_add_du_geometry_columns.sql`](../database/scripts/sql/56_add_du_geometry_columns.sql)
   added `geom` columns later so footprints could be stored in RDS without a
   new table.
4. Whether **dissolved gpkg** footprints belong on those entity rows is still
   an open decision. See [`docs/du_polygon_mapping.md`](du_polygon_mapping.md).

---

## Rational target (for future developers)

When adding geometry for a new entity class, pick one home:

1. **Separate geometry table** when many entity rows share one footprint, or
   when geometry is maintained by a GIS pipeline distinct from attributes
   (reservoir model).
2. **Entity row columns** when there is exactly one footprint per entity row
   and attributes and geometry are loaded together (possible DU path).
3. **Neither** when a centroid or WBA clip is sufficient for the UI.

Do not load DU polygons to production until the team chooses among those options.
