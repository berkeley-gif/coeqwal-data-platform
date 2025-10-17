# 🚀 Quick Start - Tier Map API

## The One Line Fix

**Change this:**
```javascript
❌ https://api.coeqwal.org/api/tiers/scenarios/${scenarioId}/outcomes/${outcomeCode}/locations
```

**To this:**
```javascript
✅ https://api.coeqwal.org/api/tier-map/${scenarioId}/${tierCode}
```

---

## Tier Code Mapping (Copy-Paste Ready)

```javascript
const OUTCOME_TO_TIER_CODE = {
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
```

---

## Complete Working Example

```javascript
// 1. Map outcome name to tier code
const tierCode = OUTCOME_TO_TIER_CODE[outcomeName];

// 2. Fetch GeoJSON from correct endpoint
const response = await fetch(
  `https://api.coeqwal.org/api/tier-map/${scenarioId}/${tierCode}`
);
const geojson = await response.json();

// 3. Add to map (Mapbox example)
map.getSource('tier-data').setData(geojson);

// 4. Style by tier level
map.setPaintProperty('tier-layer', 'fill-color', [
  'match',
  ['get', 'tier_level'],
  1, '#7b9d3f',  // Green
  2, '#60aacb',  // Blue
  3, '#FFB347',  // Orange
  4, '#CD5C5C',  // Red
  '#cccccc'
]);
```

---

## Response Format

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [-121.5, 38.5] },
      "properties": {
        "location_name": "Hood",
        "tier_level": 2,
        "tier_value": 15000
      }
    }
  ],
  "metadata": {
    "tier_name": "Environmental flows",
    "feature_count": 4
  }
}
```

---

## Test URLs

```bash
# All tiers list
https://api.coeqwal.org/api/tier-map/tiers

# Scenario summary
https://api.coeqwal.org/api/tier-map/summary/s0011

# Map data examples
https://api.coeqwal.org/api/tier-map/s0011/ENV_FLOWS
https://api.coeqwal.org/api/tier-map/s0011/RES_STOR
https://api.coeqwal.org/api/tier-map/s0011/DELTA_ECO
```

---

## That's It!

The API returns standard GeoJSON. Just plug it into your map library and style by `tier_level` (1-4).

📖 See `FRONTEND_API_DOCUMENTATION.md` for complete details.
🆘 See `FRONTEND_ENDPOINT_FIX.md` for troubleshooting.

