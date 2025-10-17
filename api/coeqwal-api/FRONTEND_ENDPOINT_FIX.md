# 🚨 Frontend Endpoint Correction

## Problem
Frontend is calling a **non-existent endpoint**:
```
❌ https://api.coeqwal.org/api/tiers/scenarios/{scenarioId}/outcomes/{outcomeCode}/locations
```

This endpoint does **not exist** in the API.

---

## Solution
Use the **correct tier map endpoint**:
```
✅ https://api.coeqwal.org/api/tier-map/{scenario}/{tier}
```

---

## Quick Fix for Frontend

### Before (Wrong ❌)
```javascript
const url = `https://api.coeqwal.org/api/tiers/scenarios/${scenarioId}/outcomes/${outcomeCode}/locations`;
```

### After (Correct ✅)
```javascript
const url = `https://api.coeqwal.org/api/tier-map/${scenarioId}/${tierCode}`;
```

---

## Endpoint Mapping

The frontend needs to map **outcome display names** to **tier codes**:

| Frontend Outcome Name | API Tier Code | Example URL |
|-----------------------|---------------|-------------|
| "Reservoir storage" | `RES_STOR` | `/api/tier-map/s0011/RES_STOR` |
| "Groundwater storage" | `GW_STOR` | `/api/tier-map/s0011/GW_STOR` |
| "Environmental flows" | `ENV_FLOWS` | `/api/tier-map/s0011/ENV_FLOWS` |
| "Agricultural revenue" | `AG_REV` | `/api/tier-map/s0011/AG_REV` |
| "Community water system deliveries" | `CWS_DEL` | `/api/tier-map/s0011/CWS_DEL` |
| "Delta ecology" | `DELTA_ECO` | `/api/tier-map/s0011/DELTA_ECO` |
| "Freshwater for Delta uses" | `FW_DELTA_USES` | `/api/tier-map/s0011/FW_DELTA_USES` |
| "Freshwater for Delta exports" | `FW_EXP` | `/api/tier-map/s0011/FW_EXP` |
| "Salmon abundance" | `WRC_SALMON_AB` | `/api/tier-map/s0011/WRC_SALMON_AB` |

---

## Complete API Reference

### Available Endpoints (Tier Map)

#### 1. **Get Map Data (Main Endpoint)**
```
GET /api/tier-map/{scenario}/{tier}
```

**Example:**
```bash
curl https://api.coeqwal.org/api/tier-map/s0011/ENV_FLOWS
```

**Response:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [-121.5, 38.5]
      },
      "properties": {
        "location_id": "C_HOOD",
        "location_name": "Hood",
        "location_type": "network_node",
        "location_type_display": "Environmental Flow",
        "tier_level": 2,
        "tier_value": 15000,
        "display_order": 1,
        "tier_color_class": "tier-2"
      }
    }
  ],
  "metadata": {
    "scenario": "s0011",
    "tier_code": "ENV_FLOWS",
    "tier_name": "Environmental flows",
    "tier_type": "multi_value",
    "feature_count": 4,
    "location_types": ["network_node"]
  }
}
```

---

#### 2. **Get Available Scenarios**
```
GET /api/tier-map/scenarios
```

**Response:**
```json
{
  "scenarios": [
    {
      "scenario_code": "s0011",
      "tier_count": 7,
      "location_count": 73
    }
  ],
  "total": 1
}
```

---

#### 3. **Get Available Tiers**
```
GET /api/tier-map/tiers
```

**Response:**
```json
{
  "tiers": [
    {
      "tier_code": "ENV_FLOWS",
      "tier_name": "Environmental flows",
      "description": "Water allocated to support ecosystem health",
      "tier_type": "multi_value",
      "tier_count": 4
    }
  ],
  "total": 9
}
```

---

#### 4. **Get Scenario Summary**
```
GET /api/tier-map/summary/{scenario}
```

**Response:**
```json
{
  "scenario": "s0011",
  "tiers": [
    {
      "tier_code": "ENV_FLOWS",
      "tier_name": "Environmental flows",
      "tier_type": "multi_value",
      "location_count": 4,
      "tier_levels_used": 4
    }
  ],
  "total_tiers": 9
}
```

---

## Frontend Code Example

### Fetch and Display Tier Data

```typescript
interface TierMapResponse {
  type: "FeatureCollection";
  features: Array<{
    type: "Feature";
    geometry: {
      type: "Point" | "Polygon";
      coordinates: number[] | number[][][];
    };
    properties: {
      location_id: string;
      location_name: string;
      location_type: string;
      location_type_display: string;
      tier_level: number; // 1-4
      tier_value: number;
      display_order: number;
      tier_color_class: string;
    };
  }>;
  metadata: {
    scenario: string;
    tier_code: string;
    tier_name: string;
    tier_type: "multi_value" | "single_value";
    feature_count: number;
    location_types: string[];
  };
}

// Mapping from frontend outcome names to API tier codes
const OUTCOME_TO_TIER_CODE: Record<string, string> = {
  "Reservoir storage": "RES_STOR",
  "Groundwater storage": "GW_STOR",
  "Environmental flows": "ENV_FLOWS",
  "Agricultural revenue": "AG_REV",
  "Community water system deliveries": "CWS_DEL",
  "Delta ecology": "DELTA_ECO",
  "Freshwater for Delta uses": "FW_DELTA_USES",
  "Freshwater for Delta exports": "FW_EXP",
  "Salmon abundance": "WRC_SALMON_AB"
};

async function fetchTierMapData(
  scenarioId: string, 
  outcomeName: string
): Promise<TierMapResponse> {
  // Get tier code from outcome name
  const tierCode = OUTCOME_TO_TIER_CODE[outcomeName];
  
  if (!tierCode) {
    throw new Error(`Unknown outcome: ${outcomeName}`);
  }
  
  // Correct endpoint!
  const url = `https://api.coeqwal.org/api/tier-map/${scenarioId}/${tierCode}`;
  
  const response = await fetch(url);
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(`API Error: ${error.detail}`);
  }
  
  return await response.json();
}

// Usage
async function handleOutcomeClick(scenarioId: string, outcomeName: string) {
  try {
    const data = await fetchTierMapData(scenarioId, outcomeName);
    
    // Add to map
    map.getSource('tier-data').setData(data);
    
    // Calculate bounds
    const bounds = calculateBounds(data.features);
    map.fitBounds(bounds, { padding: 50 });
    
    // Update legend
    updateLegend(data.metadata);
    
  } catch (error) {
    console.error('Failed to load tier data:', error);
    showErrorMessage('Unable to load map data');
  }
}
```

---

## Tier Color Mapping

```javascript
const TIER_COLORS = {
  1: '#7b9d3f',  // Green - Optimal
  2: '#60aacb',  // Blue - Suboptimal
  3: '#FFB347',  // Orange - At-risk
  4: '#CD5C5C'   // Red - Critical
};

// For Mapbox
function getTierColor(tierLevel: number): string {
  return TIER_COLORS[tierLevel] || '#cccccc';
}

// Mapbox paint property
map.addLayer({
  id: 'tier-polygons',
  type: 'fill',
  source: 'tier-data',
  filter: ['==', ['geometry-type'], 'Polygon'],
  paint: {
    'fill-color': [
      'match',
      ['get', 'tier_level'],
      1, '#7b9d3f',
      2, '#60aacb',
      3, '#FFB347',
      4, '#CD5C5C',
      '#cccccc'
    ],
    'fill-opacity': 0.7
  }
});

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
    ],
    'circle-stroke-color': '#fff',
    'circle-stroke-width': 2
  }
});
```

---

## Testing

### Test the Correct Endpoint
```bash
# Test ENV_FLOWS
curl https://api.coeqwal.org/api/tier-map/s0011/ENV_FLOWS | jq '.metadata'

# Expected output:
# {
#   "scenario": "s0011",
#   "tier_code": "ENV_FLOWS",
#   "tier_name": "Environmental flows",
#   "tier_type": "multi_value",
#   "feature_count": 4,
#   "location_types": ["network_node"]
# }

# Test all tiers
curl https://api.coeqwal.org/api/tier-map/tiers | jq '.tiers[].tier_code'

# Expected output:
# "RES_STOR"
# "GW_STOR"
# "ENV_FLOWS"
# "AG_REV"
# "CWS_DEL"
# "DELTA_ECO"
# "FW_DELTA_USES"
# "FW_EXP"
# "WRC_SALMON_AB"
```

---

## Summary of Changes Needed

1. **Change base path**: `/api/tiers/scenarios/.../outcomes/.../locations` → `/api/tier-map/{scenario}/{tier}`
2. **Add tier code mapping**: Create `OUTCOME_TO_TIER_CODE` constant
3. **Use GeoJSON**: The response is a GeoJSON FeatureCollection, not a custom format
4. **Handle geometry types**: Both Point and Polygon geometries are returned
5. **Use `tier_level` for colors**: The `tier_level` property (1-4) determines color

---

## Questions?

If you need help with:
- Specific tier code mappings
- Handling different geometry types
- Bounds calculation
- Color scheme adjustments

Contact the backend team!

