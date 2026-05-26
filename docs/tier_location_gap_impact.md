# Tier location gap impact on reporting

What users see when `tier_location` references a `du_id` that is missing
from an entity table, or missing a polygon. Verified from code in May 2026.

Related: [`docs/du_geometry_gap.md`](du_geometry_gap.md),
[`docs/du_polygon_mapping.md`](du_polygon_mapping.md)

---

## Three gap types

| Gap type | Example | Entity row? | Polygon? |
|---|---|---|---|
| Missing attribute | CWS_DEL `ACFC` | No | No |
| Missing polygon only | CWS_DEL `AMADR` | Yes | No |
| Missing both | CWS_DEL cryptic codes before ingest | No | No |

---

## API behavior

### `/api/tiers/scenarios/{scenario}/locations?codes={tier}` (CWS_DEL, AG_REV)

Used by the frontend map for urban and agricultural demand units, and
by panels.

Source: [`api/coeqwal-api/routes/tier_endpoints.py`](../api/coeqwal-api/routes/tier_endpoints.py)

Returns every active row from `tier_location_result` for the scenario
and the requested tier codes. Does **not** check entity tables or
geometry. Tier level and value are always present when tier data
loaded successfully.

**User impact for all three gap types:** tier scores (heatmap colors,
tier level counts) include the location. The API response does not
signal that geometry or entity metadata is missing.

The API does not serve GeoJSON. Map polygons come from the Mapbox
`demand-units` vector tile keyed by `DU_ID`; rows with
`du_*_entity.geom IS NULL` simply have no matching tile feature and
render uncolored. See "Frontend behavior" below.

### Tier load-time warnings

[`etl/tier_data/scripts/load_all_tier_results.py`](../etl/tier_data/scripts/load_all_tier_results.py)
prints coverage warnings via `format_coverage_warnings()` when entity
attribute or geometry lookups fail. These are operator-facing, not
user-facing.

---

## Frontend behavior (CWS_DEL / AG_REV)

Source: coeqwal-website `apps/main/app/features/map/README.md` (How locations are resolved).

1. Frontend calls `/api/tiers/scenarios/{scenario}/locations?codes={tier}`.
2. Builds `tierColorMap` keyed by `location_id`.
3. `OutcomePolygonLayer` matches those ids against the Mapbox
   `demand-units` vector tileset via `idProperty: "DU_ID"`.

**Missing polygon in RDS:** if the Mapbox tileset still has a feature
for that `DU_ID`, the polygon may still render and receive tier coloring.
The RDS `geom` column and the Mapbox tileset are separate sources.

**Missing polygon in both RDS and Mapbox tileset:** the location id is
in `featureIds` but `OutcomePolygonLayer` finds no matching tile feature.
That polygon is not colored. Other locations still render. No error toast.

**Missing entity row:** same as missing polygon in RDS for map purposes.
Tier heatmap / summary counts still include the location via `/locations`.

Verified from [`OutcomePolygonLayer.tsx`](https://github.com/COEQWAL/coeqwal-website/blob/main/apps/main/app/features/map/visualizationLayers/components/OutcomePolygonLayer.tsx) in the coeqwal-website repo:
unmatched ids are filtered out of the Mapbox filter expression. The layer
does not crash.

---

## Impact summary

| Gap type | Tier scores in API / heatmap | Colored polygon on map | Operator warning at ETL load |
|---|---|---|---|
| Missing attribute row | Yes | Only if Mapbox tile exists | Yes (attribute missing) |
| Missing polygon (row exists) | Yes | Only if Mapbox tile exists | Yes (geometry missing) |
| Missing both | Yes | Only if Mapbox tile exists | Yes (both) |

Tier **reporting** (numeric tier levels, scenario comparisons, heatmaps)
is largely unaffected. **Map visualization** may show a gap (uncolored or
absent polygon) depending on whether Mapbox tiles cover the id.

---

## Decisions pending

Use this table when choosing accept vs fix for each Pattern C id in
[`docs/du_polygon_mapping.md`](du_polygon_mapping.md):

1. Is the location in the tier matrix focal set (`du_urban_group` `tier`
   group)?
2. Does the Mapbox `demand-units` tileset include the `DU_ID`?
3. Is the gap visible in user-facing demos for CWS_DEL?
