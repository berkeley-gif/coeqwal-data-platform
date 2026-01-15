# Tier map API

API for visualizing California water system tier performance on maps. Returns GeoJSON for Mapbox/Leaflet rendering.

## Base URL

```
https://api.coeqwal.org/api/tier-map
```

## Endpoints

### Get tier map data

**GET** `/{scenario}/{tier}`

Returns GeoJSON FeatureCollection for map visualization.

**Parameters:**
| Parameter | Example | Description |
|-----------|---------|-------------|
| `scenario` | `s0020` | Scenario identifier |
| `tier` | `RES_STOR` | Tier indicator code |

**Example:**
```bash
curl https://api.coeqwal.org/api/tier-map/s0020/RES_STOR
```

**Response:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Polygon", "coordinates": [...] },
      "properties": {
        "location_id": "SHSTA",
        "location_name": "Shasta",
        "location_type": "reservoir",
        "tier_level": 2,
        "tier_value": 1,
        "tier_color_class": "tier-2"
      }
    }
  ],
  "metadata": {
    "scenario": "s0020",
    "tier_code": "RES_STOR",
    "tier_name": "Reservoir storage",
    "feature_count": 8
  }
}
```

### List scenarios

**GET** `/scenarios`

Returns scenarios with tier map data available.

```json
{
  "scenarios": [
    { "scenario_code": "s0020", "tier_count": 8, "location_count": 205 }
  ],
  "total": 8
}
```

### List tiers

**GET** `/tiers?scenario_short_code={scenario}`

Returns available tier indicators. Optional scenario filter.

```json
{
  "tiers": [
    {
      "tier_code": "RES_STOR",
      "tier_name": "Reservoir storage",
      "tier_type": "multi_value",
      "tier_count": 4
    }
  ],
  "total": 9
}
```

### Get scenario summary

**GET** `/summary/{scenario}`

Returns all tiers for a scenario with location counts.

```json
{
  "scenario": "s0020",
  "tiers": [
    {
      "tier_code": "RES_STOR",
      "tier_name": "Reservoir storage",
      "location_count": 8,
      "tier_levels_used": 4
    }
  ],
  "total_tiers": 8
}
```

### Get tier locations (no geometry)

**GET** `/{scenario}/{tier}/locations`

Returns tier data without geometry. Use when frontend has geometries in Mapbox layers.

```json
{
  "scenario": "s0020",
  "tier_code": "CWS_DEL",
  "locations": [
    { "location_id": "26S_PU4", "tier_level": 2, "location_name": "..." }
  ]
}
```

## Tier indicators

| Code | Name | Geometry | Locations |
|------|------|----------|-----------|
| `RES_STOR` | Reservoir storage | Polygon | 8 reservoirs |
| `GW_STOR` | Groundwater storage | Polygon | 42 aquifers |
| `ENV_FLOWS` | Environmental flows | Point | 17 nodes |
| `DELTA_ECO` | Delta ecology | Polygon | 1 region |
| `FW_DELTA_USES` | Freshwater for Delta uses | Polygon | 1 region |
| `FW_EXP` | Freshwater for exports | Polygon | 1 region |
| `CWS_DEL` | Community water deliveries | — | 91 demand units |
| `AG_REV` | Agricultural revenue | — | 132 demand units |
| `WRC_SALMON_AB` | Salmon abundance | Polygon | 1 region |

**Note:** `CWS_DEL` and `AG_REV` use the `/locations` endpoint since geometries come from Mapbox layers.

## Tier levels

| Level | Meaning | Recommended color |
|-------|---------|-------------------|
| 1 | Optimal | `#2cc83b` (green) |
| 2 | Suboptimal | `#2064d4` (blue) |
| 3 | At-risk | `#f89740` (orange) |
| 4 | Critical | `#f96262` (red) |

## Frontend usage

### Mapbox GL JS

```javascript
// Fetch tier data
const response = await fetch(
  `https://api.coeqwal.org/api/tier-map/${scenario}/${tier}`
);
const geojson = await response.json();

// Add to map
map.addSource('tier-data', { type: 'geojson', data: geojson });

// Style by tier level
map.addLayer({
  id: 'tier-fill',
  type: 'fill',
  source: 'tier-data',
  paint: {
    'fill-color': [
      'match', ['get', 'tier_level'],
      1, '#2cc83b',
      2, '#2064d4',
      3, '#f89740',
      4, '#f96262',
      '#999999'
    ],
    'fill-opacity': 0.7
  }
});
```

### React example

```jsx
function TierMap({ scenario, tier }) {
  const [data, setData] = useState(null);

  useEffect(() => {
    fetch(`https://api.coeqwal.org/api/tier-map/${scenario}/${tier}`)
      .then(res => res.json())
      .then(setData);
  }, [scenario, tier]);

  if (!data) return <div>Loading...</div>;

  return (
    <MapboxMap
      source={{ type: 'geojson', data }}
      // ... render layers
    />
  );
}
```

## Error handling

| Status | Description |
|--------|-------------|
| 200 | Success |
| 404 | Invalid scenario/tier combination |
| 500 | Server error |

```json
{ "detail": "No tier data found for scenario 's0020' and tier 'INVALID'" }
```

## Performance

- Response time: 50-150ms typical
- Largest response: ~200KB (GW_STOR with 42 polygons)
- Most responses: <50KB
