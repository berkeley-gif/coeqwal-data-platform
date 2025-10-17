# ✅ Tier Map API - Ready for Frontend Integration

**Date**: October 17, 2025  
**Status**: Production Ready ✅  
**Base URL**: https://api.coeqwal.org/api/tier-map

---

## 🚨 Critical: Your Frontend Has the Wrong Endpoint

### What Your Frontend Is Doing (Wrong ❌)
```javascript
const url = `https://api.coeqwal.org/api/tiers/scenarios/${scenarioId}/outcomes/${outcomeCode}/locations`;
```

**This endpoint DOES NOT EXIST.**

### What It Should Be Doing (Correct ✅)
```javascript
const url = `https://api.coeqwal.org/api/tier-map/${scenarioId}/${tierCode}`;
```

---

## 📋 Frontend Team Action Items

### 1. Update Base URL Path
- **Old**: `/api/tiers/scenarios/.../outcomes/.../locations`
- **New**: `/api/tier-map/{scenario}/{tier}`

### 2. Add Tier Code Mapping
The API uses short codes, not display names:

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

### 3. Expect GeoJSON Format
The API returns standard GeoJSON FeatureCollection:

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
    "scenario": "s0011",
    "tier_name": "Environmental flows",
    "feature_count": 4
  }
}
```

### 4. Style by `tier_level` Property
Each feature has `tier_level` (1-4) for color mapping:

```javascript
const TIER_COLORS = {
  1: '#7b9d3f',  // Green - Optimal
  2: '#60aacb',  // Blue - Suboptimal
  3: '#FFB347',  // Orange - At-risk
  4: '#CD5C5C'   // Red - Critical
};
```

---

## 📚 Documentation Files for Frontend

### Priority 1: Read These First
1. **`QUICK_START_FOR_FRONTEND.md`** - Copy-paste code snippets
2. **`FRONTEND_ENDPOINT_FIX.md`** - Detailed fix instructions with examples

### Priority 2: Complete Reference
3. **`FRONTEND_API_DOCUMENTATION.md`** - Full API specification

---

## 🧪 Test the API Now

### Test Available Tiers
```bash
curl https://api.coeqwal.org/api/tier-map/tiers
```

### Test Map Data (Environmental Flows)
```bash
curl https://api.coeqwal.org/api/tier-map/s0011/ENV_FLOWS | jq
```

### Expected Response Structure
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [...] },
      "properties": {
        "location_name": "...",
        "tier_level": 2,
        "tier_value": 15000,
        ...
      }
    }
  ],
  "metadata": {
    "scenario": "s0011",
    "tier_code": "ENV_FLOWS",
    "tier_name": "Environmental flows",
    "feature_count": 4
  }
}
```

---

## 🎯 What's Available

### Scenarios
- `s0011` - Scenario 11
- `s0020` - Scenario 20
- `s0021` - Scenario 21

### Tiers (9 Total)

#### Multi-Value Tiers (Multiple Locations)
1. **RES_STOR** - Reservoir storage (7 reservoirs with polygons)
2. **GW_STOR** - Groundwater storage (4 aquifers with polygons)
3. **ENV_FLOWS** - Environmental flows (4 network nodes with points)
4. **AG_REV** - Agricultural revenue (4 regions with polygons)
5. **CWS_DEL** - Community water deliveries (4 regions with polygons)

#### Single-Value Tiers (One Location)
6. **DELTA_ECO** - Delta ecology (1 Delta polygon)
7. **FW_DELTA_USES** - Freshwater for Delta uses (2 compliance stations)
8. **FW_EXP** - Freshwater for exports (2 compliance stations)
9. **WRC_SALMON_AB** - Salmon abundance (1 Sacramento River point)

---

## 🗺️ Geometry Types

The API returns two types of geometries:

### Polygons
- Reservoirs (RES_STOR)
- Aquifers/WBAs (GW_STOR, AG_REV, CWS_DEL)
- Delta region (DELTA_ECO)

### Points
- Environmental flow nodes (ENV_FLOWS)
- Compliance stations (FW_DELTA_USES, FW_EXP)
- Salmon monitoring (WRC_SALMON_AB)

Your map needs to handle both types.

---

## ⚠️ Special Case: San Luis Reservoir

**Issue**: San Luis has two tier values (CVP and SWP) but one physical polygon.

**API Behavior**: Both `SLUIS_CVP` and `SLUIS_SWP` return the same polygon geometry.

**Frontend Options**:
1. Show two overlapping polygons with different colors
2. Show one polygon with blended color
3. Show one polygon with split fill
4. Show both values in popup on click

---

## 🔧 Complete Working Code Example

```javascript
import mapboxgl from 'mapbox-gl';

// Tier code mapping
const OUTCOME_TO_TIER_CODE = {
  "Environmental flows": "ENV_FLOWS",
  "Reservoir storage": "RES_STOR",
  // ... (add all 9)
};

// Tier colors
const TIER_COLORS = {
  1: '#7b9d3f',  // Green
  2: '#60aacb',  // Blue
  3: '#FFB347',  // Orange
  4: '#CD5C5C'   // Red
};

// Fetch and display tier data
async function loadTierMap(scenarioId, outcomeName) {
  try {
    // 1. Map outcome to tier code
    const tierCode = OUTCOME_TO_TIER_CODE[outcomeName];
    if (!tierCode) {
      throw new Error(`Unknown outcome: ${outcomeName}`);
    }
    
    // 2. Fetch GeoJSON (CORRECT ENDPOINT!)
    const response = await fetch(
      `https://api.coeqwal.org/api/tier-map/${scenarioId}/${tierCode}`
    );
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail);
    }
    
    const geojson = await response.json();
    
    // 3. Add to map
    if (!map.getSource('tier-data')) {
      map.addSource('tier-data', {
        type: 'geojson',
        data: geojson
      });
      
      // Add polygon layer
      map.addLayer({
        id: 'tier-polygons',
        type: 'fill',
        source: 'tier-data',
        filter: ['==', ['geometry-type'], 'Polygon'],
        paint: {
          'fill-color': [
            'match',
            ['get', 'tier_level'],
            1, TIER_COLORS[1],
            2, TIER_COLORS[2],
            3, TIER_COLORS[3],
            4, TIER_COLORS[4],
            '#cccccc'
          ],
          'fill-opacity': 0.7
        }
      });
      
      // Add point layer
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
            1, TIER_COLORS[1],
            2, TIER_COLORS[2],
            3, TIER_COLORS[3],
            4, TIER_COLORS[4],
            '#cccccc'
          ],
          'circle-stroke-color': '#fff',
          'circle-stroke-width': 2
        }
      });
      
    } else {
      map.getSource('tier-data').setData(geojson);
    }
    
    // 4. Calculate and fit bounds
    const coordinates = [];
    geojson.features.forEach(feature => {
      if (feature.geometry.type === 'Point') {
        coordinates.push(feature.geometry.coordinates);
      } else if (feature.geometry.type === 'Polygon') {
        feature.geometry.coordinates[0].forEach(coord => {
          coordinates.push(coord);
        });
      }
    });
    
    const bounds = coordinates.reduce((bounds, coord) => {
      return bounds.extend(coord);
    }, new mapboxgl.LngLatBounds(coordinates[0], coordinates[0]));
    
    map.fitBounds(bounds, { padding: 50 });
    
    // 5. Update UI
    updateLegend(geojson.metadata);
    showSuccess(`Loaded ${geojson.metadata.feature_count} locations`);
    
  } catch (error) {
    console.error('Failed to load tier map:', error);
    showError('Unable to load map data');
  }
}

// Handle outcome click
document.querySelectorAll('.outcome-chart').forEach(chart => {
  chart.addEventListener('click', () => {
    const scenarioId = chart.dataset.scenario;
    const outcomeName = chart.dataset.outcome;
    loadTierMap(scenarioId, outcomeName);
  });
});
```

---

## ✅ Verification Checklist

Before deploying, verify:

- [ ] Using `/api/tier-map/` endpoint (not `/api/tiers/`)
- [ ] Mapping outcome names to tier codes
- [ ] Expecting GeoJSON FeatureCollection format
- [ ] Handling both Point and Polygon geometries
- [ ] Using `tier_level` property for colors (1-4)
- [ ] Calculating bounds from feature coordinates
- [ ] Handling errors (404, 500) gracefully

---

## 🆘 Troubleshooting

### Problem: 404 Not Found
**Cause**: Wrong endpoint or invalid tier code  
**Fix**: Check endpoint path and tier code mapping

### Problem: No data showing on map
**Cause**: GeoJSON not added to map source  
**Fix**: Verify `map.getSource('tier-data').setData(geojson)`

### Problem: Wrong colors
**Cause**: Using wrong property for color mapping  
**Fix**: Use `tier_level` (not `tier_value` or other property)

### Problem: Map not zooming
**Cause**: Bounds not calculated  
**Fix**: Extract coordinates from features and use `fitBounds()`

---

## 📞 Support

Questions? Contact backend team with:
- The exact URL you're calling
- The error message (if any)
- The expected vs actual behavior

---

## 🎉 Summary

**The API is ready!** Your frontend just needs to:
1. Change the endpoint URL
2. Add the tier code mapping
3. Expect GeoJSON format

The data is all there, correctly formatted, with geometries and tier levels ready for visualization. Just plug it in! 🚀

