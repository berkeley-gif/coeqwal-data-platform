# Tier Map Visualization API - Frontend Documentation

## ⚠️ IMPORTANT: Correct Endpoint
**DO NOT USE**: `https://api.coeqwal.org/api/tiers/scenarios/{id}/outcomes/{code}/locations` ❌

**USE THIS**: `https://api.coeqwal.org/api/tier-map/{scenario}/{tier}` ✅

---

## Base URL
```
https://api.coeqwal.org/api/tier-map
```

## Overview
This API provides GeoJSON data for visualizing California water system tier performance metrics on a map. Each tier represents a different aspect of water system health (storage, delivery, environmental flows, etc.).

## Outcome Name to Tier Code Mapping

Your frontend likely uses **outcome display names**. Map them to **API tier codes**:

| Frontend Name | API Tier Code |
|---------------|---------------|
| "Reservoir storage" | `RES_STOR` |
| "Groundwater storage" | `GW_STOR` |
| "Environmental flows" | `ENV_FLOWS` |
| "Agricultural revenue" | `AG_REV` |
| "Community water system deliveries" | `CWS_DEL` |
| "Delta ecology" | `DELTA_ECO` |
| "Freshwater for Delta uses" | `FW_DELTA_USES` |
| "Freshwater for Delta exports" | `FW_EXP` |
| "Salmon abundance" | `WRC_SALMON_AB` |

**Example:**
```javascript
const OUTCOME_TO_TIER = {
  "Environmental flows": "ENV_FLOWS",
  "Reservoir storage": "RES_STOR"
  // ... add all 9
};

const tierCode = OUTCOME_TO_TIER[outcomeName];
const url = `https://api.coeqwal.org/api/tier-map/s0011/${tierCode}`;
```

---

## Endpoints

### 1. Get Available Scenarios
```
GET /scenarios
```

Returns list of all available scenario configurations.

**Response:**
```json
{
  "scenarios": [
    {
      "scenario_code": "s0011",
      "tier_count": 7,
      "location_count": 73
    },
    {
      "scenario_code": "s0020",
      "tier_count": 7,
      "location_count": 73
    }
  ],
  "total": 2
}
```

---

### 2. Get Available Tiers
```
GET /tiers?scenario_short_code={scenario}
```

Returns list of all tier types. Optional: filter by scenario.

**Query Parameters:**
- `scenario_short_code` (optional): Filter tiers for specific scenario

**Response:**
```json
{
  "tiers": [
    {
      "tier_code": "RES_STOR",
      "tier_name": "Reservoir storage",
      "description": "Amount of water stored in California's major reservoir systems each spring",
      "tier_type": "multi_value",
      "tier_count": 4
    },
    {
      "tier_code": "DELTA_ECO",
      "tier_name": "Delta ecology",
      "description": "Ecological responses to flow, measured by direct indicators",
      "tier_type": "single_value",
      "tier_count": 1
    }
  ],
  "total": 9
}
```

**Tier Types:**
- `multi_value`: Location has multiple sub-locations with different tier levels (e.g., multiple reservoirs)
- `single_value`: Single geographic area with one tier level (e.g., Delta ecology)

---

### 3. Get Tier Map Data (Main Endpoint)
```
GET /{scenario_short_code}/{tier_short_code}
```

Returns GeoJSON FeatureCollection for visualizing a specific tier on a map.

**URL Parameters:**
- `scenario_short_code`: Scenario identifier (e.g., "s0011")
- `tier_short_code`: Tier identifier (e.g., "RES_STOR", "DELTA_ECO")

**Response:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[-121.5, 37.8], [-121.4, 37.8], ...]]
      },
      "properties": {
        "location_id": "SHSTA",
        "location_name": "Lake Shasta",
        "location_type": "reservoir",
        "location_type_display": "Reservoir",
        "tier_level": 2,
        "tier_value": 3500000,
        "display_order": 1,
        "tier_color_class": "tier-2"
      }
    }
  ],
  "metadata": {
    "scenario": "s0011",
    "tier_code": "RES_STOR",
    "tier_name": "Reservoir storage",
    "tier_type": "multi_value",
    "feature_count": 7,
    "location_types": ["reservoir"]
  }
}
```

**Geometry Types:**
- **Polygon**: Reservoirs, aquifers (WBAs), regions
- **Point**: Environmental flow locations, compliance stations

**Feature Properties:**
- `location_id`: Unique identifier for the location
- `location_name`: Human-readable name
- `location_type`: Backend type (`reservoir`, `wba`, `compliance_station`, `network_node`, `region`)
- `location_type_display`: Frontend-friendly label (`Reservoir`, `Aquifer`, `Compliance Station`, `Environmental Flow`, `Region`)
- `tier_level`: Performance level (1-4, where 1=best, 4=worst)
- `tier_value`: Raw numeric value (e.g., acre-feet for storage)
- `display_order`: Suggested ordering for UI lists
- `tier_color_class`: CSS class hint (optional, frontend can manage colors)

---

### 4. Get Scenario Summary
```
GET /summary/{scenario_short_code}
```

Returns summary of all tiers for a given scenario (useful for building tier selector UI).

**Response:**
```json
{
  "scenario": "s0011",
  "tiers": [
    {
      "tier_code": "RES_STOR",
      "tier_name": "Reservoir storage",
      "description": "Amount of water stored in California's major reservoir systems",
      "tier_type": "multi_value",
      "tier_count": 4,
      "location_count": 7,
      "tier_levels_used": 4
    }
  ],
  "total_tiers": 9
}
```

---

## Tier Performance Levels

Each location is assigned a tier level (1-4) representing performance:

| Tier Level | Color Recommendation | Meaning |
|------------|---------------------|---------|
| 1 | Green (#7b9d3f) | Optimal |
| 2 | Blue (#60aacb) | Suboptimal |
| 3 | Orange (#FFB347) | At-risk |
| 4 | Red (#CD5C5C) | Critical |

**Note:** Colors are recommendations. The API does not return colors; implement them on the frontend.

---

## Tier Types Reference

### Multi-Value Tiers (Multiple Locations)

1. **RES_STOR** (Reservoir Storage)
   - 7 major reservoirs with polygon geometries
   - Special case: San Luis Reservoir has two entries (SLUIS_CVP, SLUIS_SWP) but shares one polygon

2. **GW_STOR** (Groundwater Storage)
   - 4 aquifer regions with polygon geometries
   - Uses WBA (Water Budget Area) boundaries

3. **CWS_DEL** (Community Water System Deliveries)
   - 4 aquifer regions with polygon geometries

4. **AG_REV** (Agricultural Revenue)
   - 4 agricultural regions with polygon geometries

5. **ENV_FLOWS** (Environmental Flows)
   - 4 network node locations with point geometries

### Single-Value Tiers (One Location)

6. **DELTA_ECO** (Delta Ecology)
   - Single polygon for Sacramento-San Joaquin Delta (DETAW)

7. **FW_DELTA_USES** (Freshwater for Delta Uses)
   - 2 compliance station points in western Delta

8. **FW_EXP** (Freshwater for Delta Exports)
   - 2 compliance station points at export pumps

9. **WRC_SALMON_AB** (Salmon Abundance)
   - Single point at Sacramento River (SAC299 - Keswick)

---

## Usage Examples

### Example 1: Build Scenario Selector
```javascript
// Fetch available scenarios
const response = await fetch('https://api.coeqwal.org/api/tier-map/scenarios');
const { scenarios } = await response.json();

// Populate dropdown
scenarios.forEach(s => {
  addOption(`${s.scenario_code} (${s.tier_count} tiers, ${s.location_count} locations)`);
});
```

### Example 2: Build Tier Selector
```javascript
// Fetch tiers for selected scenario
const scenario = 's0011';
const response = await fetch(`https://api.coeqwal.org/api/tier-map/summary/${scenario}`);
const { tiers } = await response.json();

// Display tier options
tiers.forEach(tier => {
  addTierOption(tier.tier_code, tier.tier_name, tier.location_count);
});
```

### Example 3: Display Tier on Map (Mapbox)
```javascript
const scenario = 's0011';
const tier = 'RES_STOR';

// Fetch GeoJSON data
const response = await fetch(`https://api.coeqwal.org/api/tier-map/${scenario}/${tier}`);
const geojson = await response.json();

// Add to Mapbox
map.addSource('tier-data', {
  type: 'geojson',
  data: geojson
});

// Style based on tier_level
map.addLayer({
  id: 'tier-layer',
  type: 'fill',
  source: 'tier-data',
  paint: {
    'fill-color': [
      'match',
      ['get', 'tier_level'],
      1, '#7b9d3f',  // Green - Optimal
      2, '#60aacb',  // Blue - Suboptimal
      3, '#FFB347',  // Orange - At-risk
      4, '#CD5C5C',  // Red - Critical
      '#cccccc'      // Default gray
    ],
    'fill-opacity': 0.7
  }
});

// Add points for network nodes
map.addLayer({
  id: 'tier-points',
  type: 'circle',
  source: 'tier-data',
  filter: ['==', ['geometry-type'], 'Point'],
  paint: {
    'circle-radius': 8,
    'circle-color': [
      'match',
      ['get', 'tier_level'],
      1, '#7b9d3f',
      2, '#60aacb',
      3, '#FFB347',
      4, '#CD5C5C',
      '#cccccc'
    ]
  }
});

// Add popups
map.on('click', 'tier-layer', (e) => {
  const props = e.features[0].properties;
  new mapboxgl.Popup()
    .setLngLat(e.lngLat)
    .setHTML(`
      <h3>${props.location_name}</h3>
      <p><strong>Type:</strong> ${props.location_type_display}</p>
      <p><strong>Tier Level:</strong> ${props.tier_level}</p>
      <p><strong>Value:</strong> ${props.tier_value}</p>
    `)
    .addTo(map);
});
```

### Example 4: React Component
```jsx
import { useEffect, useState } from 'react';

function TierMap({ scenario, tier }) {
  const [geojson, setGeojson] = useState(null);
  
  useEffect(() => {
    fetch(`https://api.coeqwal.org/api/tier-map/${scenario}/${tier}`)
      .then(res => res.json())
      .then(data => setGeojson(data));
  }, [scenario, tier]);
  
  if (!geojson) return <div>Loading...</div>;
  
  return (
    <div>
      <h2>{geojson.metadata.tier_name}</h2>
      <p>{geojson.metadata.feature_count} locations</p>
      {/* Render map with geojson */}
    </div>
  );
}
```

---

## Special Cases

### San Luis Reservoir (SLUIS)
- **Issue**: San Luis has two tier values (CVP and SWP operations) but one physical reservoir
- **Solution**: Both `SLUIS_CVP` and `SLUIS_SWP` return the same polygon geometry
- **Frontend**: Decide how to visualize:
  - Option 1: Show two overlapping polygons with different colors
  - Option 2: Show one polygon with blended/averaged color
  - Option 3: Show one polygon with a split-color fill
  - Option 4: On click, show both tier values in popup

**Example Popup for SLUIS:**
```html
<h3>San Luis Reservoir</h3>
<p><strong>CVP:</strong> Tier 2 (3,500,000 AF)</p>
<p><strong>SWP:</strong> Tier 3 (2,800,000 AF)</p>
```

---

## Error Handling

### 404 - Not Found
```json
{
  "detail": "No tier data found for scenario 's0099' and tier 'FAKE_TIER'"
}
```

### 500 - Server Error
```json
{
  "detail": "Database error: [error message]"
}
```

**Recommended Error Handling:**
```javascript
try {
  const response = await fetch(`https://api.coeqwal.org/api/tier-map/${scenario}/${tier}`);
  if (!response.ok) {
    const error = await response.json();
    console.error('API Error:', error.detail);
    showErrorMessage('Unable to load tier data');
    return;
  }
  const geojson = await response.json();
  renderMap(geojson);
} catch (error) {
  console.error('Network Error:', error);
  showErrorMessage('Network error - please try again');
}
```

---

## Performance Notes

- All endpoints return data in < 500ms
- GeoJSON features are pre-computed (no on-the-fly geometry processing)
- Recommended: Cache scenario and tier lists, refresh only when user changes selection
- No pagination needed (max ~73 locations per tier)

---

## Questions or Issues?

Contact the backend team if you encounter:
- Missing geometries
- Incorrect tier levels
- Performance issues
- Need for additional metadata fields

