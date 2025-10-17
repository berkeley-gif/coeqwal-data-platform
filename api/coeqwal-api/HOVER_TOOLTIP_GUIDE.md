# 🎯 Hover/Click Tooltip Information Guide

## Overview
This document details what additional information can be displayed when users hover over or click on tier map features (points and polygons).

---

## 📊 Currently Available Data (Already in API Response)

Every feature in the GeoJSON response already includes:

```json
{
  "properties": {
    "location_id": "AMR004",
    "location_name": "American River at I-80 Bridge",
    "location_type": "network_node",
    "location_type_display": "Environmental Flow",
    "tier_level": 3,
    "tier_value": 1,
    "display_order": 1,
    "tier_color_class": "tier-3"
  }
}
```

### Basic Tooltip (No Extra API Call Needed)

```javascript
// Simple hover tooltip - uses existing data
map.on('mouseenter', 'tier-layer', (e) => {
  const props = e.features[0].properties;
  
  popup.setHTML(`
    <div class="tier-tooltip">
      <h3>${props.location_name}</h3>
      <div class="tier-badge tier-${props.tier_level}">
        Tier ${props.tier_level}
      </div>
      <p class="location-type">${props.location_type_display}</p>
    </div>
  `);
});
```

---

## 🔍 Additional Data Available (Requires Extra API Calls)

For richer tooltips, you can fetch additional metadata based on `location_type` and `location_id`.

### 1. **Environmental Flows** (Network Nodes)

**Data Available:**
- River system name
- River mile
- Hydrologic region
- Stream code
- C2VSIM groundwater connection
- Nearest gage station
- Node type/subtype
- Description

**Additional API Endpoint Needed:**
```
GET /api/network/nodes/{location_id}/details
```

**Example Response:**
```json
{
  "short_code": "AMR004",
  "name": "American River at I-80 Bridge",
  "description": "American River monitoring location at Interstate 80",
  "type": "Instream Flow",
  "subtype": "Environmental",
  "river_system": "American River",
  "river_mile": 14.5,
  "hydrologic_region": "Sacramento River",
  "stream_code": "AMR",
  "nearest_gage": "USGS 11446500",
  "c2vsim_gw": "SAC_LOWER",
  "coordinates": {
    "latitude": 38.587420292,
    "longitude": -121.44651837
  }
}
```

**Rich Tooltip Example:**
```html
<div class="tier-tooltip-detailed">
  <h3>American River at I-80 Bridge</h3>
  <div class="tier-badge tier-3">Tier 3 - At-risk</div>
  
  <div class="details">
    <div class="detail-row">
      <span class="label">Type:</span>
      <span class="value">Environmental Flow</span>
    </div>
    <div class="detail-row">
      <span class="label">River System:</span>
      <span class="value">American River</span>
    </div>
    <div class="detail-row">
      <span class="label">River Mile:</span>
      <span class="value">14.5</span>
    </div>
    <div class="detail-row">
      <span class="label">Region:</span>
      <span class="value">Sacramento River</span>
    </div>
    <div class="detail-row">
      <span class="label">Nearest Gage:</span>
      <span class="value">USGS 11446500</span>
    </div>
  </div>
  
  <div class="tier-explanation">
    At-risk conditions: Flow levels below optimal for ecosystem health
  </div>
</div>
```

---

### 2. **Reservoir Storage** (Reservoirs)

**Data Available:**
- Reservoir full name
- Surface area (sq km)
- Elevation (m)
- GNIS ID (Geographic Names Information System)
- NHD Permanent ID (National Hydrography Dataset)
- Storage capacity (from tier_result)
- Current storage level

**Additional API Endpoint Needed:**
```
GET /api/reservoirs/{location_id}/details
```

**Example Response:**
```json
{
  "calsim_short_code": "SHSTA",
  "reservoir_name": "Shasta Lake",
  "area_sqkm": 121.4,
  "elevation_m": 324.0,
  "gnis_id": "253097",
  "nhd_permanent_id": "1234567890",
  "storage_capacity_af": 4552000,
  "current_storage_af": 3200000,
  "percent_capacity": 70.3,
  "data_source": "NHD"
}
```

**Rich Tooltip Example:**
```html
<div class="tier-tooltip-detailed">
  <h3>Shasta Lake</h3>
  <div class="tier-badge tier-2">Tier 2 - Suboptimal</div>
  
  <div class="storage-stats">
    <div class="storage-gauge">
      <div class="gauge-fill" style="width: 70.3%"></div>
    </div>
    <p class="storage-text">3.2M AF / 4.5M AF (70%)</p>
  </div>
  
  <div class="details">
    <div class="detail-row">
      <span class="label">Surface Area:</span>
      <span class="value">121.4 km²</span>
    </div>
    <div class="detail-row">
      <span class="label">Elevation:</span>
      <span class="value">324 m</span>
    </div>
  </div>
  
  <div class="tier-explanation">
    Suboptimal: Storage below ideal spring levels
  </div>
</div>
```

---

### 3. **Groundwater Storage** (Aquifers/WBAs)

**Data Available:**
- WBA (Water Budget Area) name
- Area (acres)
- Hydrologic region
- Comments/description
- Groundwater basin details

**Additional API Endpoint Needed:**
```
GET /api/wba/{location_id}/details
```

**Example Response:**
```json
{
  "wba_id": "06N",
  "wba_name": "Upper Sacramento Valley",
  "area_acres": 4200000,
  "hydrologic_region": "Sacramento River",
  "comments": "Major agricultural groundwater basin",
  "data_source": "CalSim_Geopackage",
  "basin_info": {
    "safe_yield_af": 500000,
    "overdraft_status": "Critical"
  }
}
```

**Rich Tooltip Example:**
```html
<div class="tier-tooltip-detailed">
  <h3>Upper Sacramento Valley</h3>
  <div class="tier-badge tier-3">Tier 3 - At-risk</div>
  
  <div class="details">
    <div class="detail-row">
      <span class="label">WBA ID:</span>
      <span class="value">06N</span>
    </div>
    <div class="detail-row">
      <span class="label">Area:</span>
      <span class="value">4.2M acres</span>
    </div>
    <div class="detail-row">
      <span class="label">Region:</span>
      <span class="value">Sacramento River</span>
    </div>
    <div class="detail-row">
      <span class="label">Status:</span>
      <span class="value critical">Critical Overdraft</span>
    </div>
  </div>
  
  <p class="description">
    Major agricultural groundwater basin with declining water levels
  </p>
</div>
```

---

### 4. **Compliance Stations** (Delta Uses/Exports)

**Data Available:**
- Station name
- Station code
- Coordinates
- Tier use (FW_DELTA_USES or FW_EXP)
- Notes

**Additional API Endpoint Needed:**
```
GET /api/compliance-stations/{location_id}/details
```

**Example Response:**
```json
{
  "station_code": "JP",
  "station_name": "Jones Pumping Plant",
  "latitude": 37.81,
  "longitude": -121.57,
  "tier_use": "FW_EXP",
  "data_source": "Manual Entry",
  "notes": "Primary CVP Delta export facility",
  "annual_capacity_af": 4600000
}
```

**Rich Tooltip Example:**
```html
<div class="tier-tooltip-detailed">
  <h3>Jones Pumping Plant</h3>
  <div class="tier-badge tier-2">Tier 2 - Suboptimal</div>
  
  <div class="details">
    <div class="detail-row">
      <span class="label">Station Code:</span>
      <span class="value">JP</span>
    </div>
    <div class="detail-row">
      <span class="label">Facility Type:</span>
      <span class="value">CVP Export Facility</span>
    </div>
    <div class="detail-row">
      <span class="label">Capacity:</span>
      <span class="value">4.6M AF/year</span>
    </div>
  </div>
  
  <p class="description">
    Primary CVP Delta export facility - water quality monitoring
  </p>
</div>
```

---

### 5. **Delta Ecology** (Regions)

**Data Available:**
- Region name
- Polygon geometry
- Ecological indicators
- Water quality metrics

**Additional API Endpoint Needed:**
```
GET /api/regions/{location_id}/ecological-status
```

**Example Response:**
```json
{
  "region_id": "DETAW",
  "region_name": "Sacramento-San Joaquin Delta",
  "area_acres": 738000,
  "ecological_metrics": {
    "sav_coverage_acres": 15000,
    "salinity_avg_ec": 0.8,
    "turbidity_ntu": 12.5,
    "x2_position_km": 81
  },
  "water_quality": {
    "dissolved_oxygen_mg_l": 7.2,
    "temperature_c": 18.5,
    "ph": 7.8
  }
}
```

**Rich Tooltip Example:**
```html
<div class="tier-tooltip-detailed">
  <h3>Sacramento-San Joaquin Delta</h3>
  <div class="tier-badge tier-2">Tier 2 - Suboptimal</div>
  
  <div class="ecological-indicators">
    <h4>Ecological Indicators</h4>
    <div class="detail-row">
      <span class="label">SAV Coverage:</span>
      <span class="value">15,000 acres</span>
    </div>
    <div class="detail-row">
      <span class="label">Salinity (EC):</span>
      <span class="value">0.8 dS/m</span>
    </div>
    <div class="detail-row">
      <span class="label">X2 Position:</span>
      <span class="value">81 km</span>
    </div>
  </div>
  
  <div class="tier-explanation">
    Suboptimal: Ecological conditions below optimal targets
  </div>
</div>
```

---

### 6. **Salmon Abundance** (Monitoring Location)

**Data Available:**
- Monitoring location name
- River system
- Population metrics
- Habitat quality

**Additional API Endpoint Needed:**
```
GET /api/salmon-monitoring/{location_id}/status
```

**Example Response:**
```json
{
  "location_code": "SAC299",
  "location_name": "Sacramento River at Keswick",
  "river_system": "Sacramento River",
  "river_mile": 302,
  "species": "Winter-run Chinook",
  "population_metrics": {
    "escapement_count": 8500,
    "target_escapement": 12000,
    "percent_of_target": 71,
    "trend_5yr": "declining"
  },
  "habitat_quality": {
    "water_temperature_c": 14.2,
    "flow_cfs": 5500,
    "spawning_gravel_quality": "fair"
  }
}
```

**Rich Tooltip Example:**
```html
<div class="tier-tooltip-detailed">
  <h3>Sacramento River at Keswick</h3>
  <div class="tier-badge tier-3">Tier 3 - At-risk</div>
  
  <div class="salmon-stats">
    <h4>Winter-run Chinook</h4>
    <div class="population-gauge">
      <div class="gauge-fill" style="width: 71%"></div>
    </div>
    <p class="population-text">
      8,500 / 12,000 escapement (71% of target)
    </p>
  </div>
  
  <div class="details">
    <div class="detail-row">
      <span class="label">5-Year Trend:</span>
      <span class="value declining">Declining</span>
    </div>
    <div class="detail-row">
      <span class="label">Water Temp:</span>
      <span class="value">14.2°C</span>
    </div>
    <div class="detail-row">
      <span class="label">Flow:</span>
      <span class="value">5,500 CFS</span>
    </div>
  </div>
  
  <div class="tier-explanation">
    At-risk: Population below recovery targets
  </div>
</div>
```

---

## 🎨 Recommended Tooltip Implementations

### Level 1: Simple Hover (No Extra API Calls)

**Use Case**: Quick reference while exploring the map

**Data Source**: Existing GeoJSON properties

**Example:**
```javascript
map.on('mouseenter', 'tier-layer', (e) => {
  const props = e.features[0].properties;
  const tierLabels = {
    1: 'Optimal',
    2: 'Suboptimal',
    3: 'At-risk',
    4: 'Critical'
  };
  
  popup.setHTML(`
    <div class="simple-tooltip">
      <strong>${props.location_name}</strong><br>
      <span class="tier tier-${props.tier_level}">
        Tier ${props.tier_level}: ${tierLabels[props.tier_level]}
      </span>
    </div>
  `);
});
```

---

### Level 2: Detailed Click (One Extra API Call)

**Use Case**: User wants more information about a specific location

**Data Source**: Additional API call based on `location_type` and `location_id`

**Example:**
```javascript
map.on('click', 'tier-layer', async (e) => {
  const props = e.features[0].properties;
  
  // Show loading state
  popup.setHTML('<div class="loading">Loading details...</div>');
  
  // Fetch additional data based on location type
  let detailsUrl;
  switch (props.location_type) {
    case 'network_node':
      detailsUrl = `/api/network/nodes/${props.location_id}/details`;
      break;
    case 'reservoir':
      detailsUrl = `/api/reservoirs/${props.location_id}/details`;
      break;
    case 'wba':
      detailsUrl = `/api/wba/${props.location_id}/details`;
      break;
    // ... other types
  }
  
  const response = await fetch(detailsUrl);
  const details = await response.json();
  
  // Render detailed popup
  popup.setHTML(renderDetailedTooltip(props, details));
});
```

---

### Level 3: Progressive Enhancement

**Use Case**: Show basic info immediately, load details in background

**Implementation:**
```javascript
map.on('click', 'tier-layer', async (e) => {
  const props = e.features[0].properties;
  
  // Show basic info immediately
  popup.setHTML(renderBasicTooltip(props));
  
  // Fetch and append additional details
  fetchDetails(props).then(details => {
    const currentHtml = popup.getHTML();
    popup.setHTML(currentHtml + renderAdditionalDetails(details));
  });
});
```

---

## 📈 Tier Context Information

Add explanatory text for each tier level to help users understand what the colors mean:

```javascript
const TIER_EXPLANATIONS = {
  1: {
    label: 'Optimal',
    color: '#7b9d3f',
    explanation: {
      'ENV_FLOWS': 'Flow levels meet or exceed environmental targets',
      'RES_STOR': 'Storage at or above ideal spring levels',
      'GW_STOR': 'Groundwater levels within sustainable range',
      'DELTA_ECO': 'Ecological indicators at healthy levels',
      'WRC_SALMON_AB': 'Salmon populations meeting recovery targets'
    }
  },
  2: {
    label: 'Suboptimal',
    color: '#60aacb',
    explanation: {
      'ENV_FLOWS': 'Flow levels below optimal but still functional',
      'RES_STOR': 'Storage below ideal but adequate for operations',
      'GW_STOR': 'Groundwater levels declining but not critical',
      'DELTA_ECO': 'Some ecological indicators showing stress',
      'WRC_SALMON_AB': 'Salmon populations below targets but viable'
    }
  },
  3: {
    label: 'At-risk',
    color: '#FFB347',
    explanation: {
      'ENV_FLOWS': 'Flow levels insufficient for ecosystem health',
      'RES_STOR': 'Storage significantly below ideal levels',
      'GW_STOR': 'Groundwater overdraft concerns',
      'DELTA_ECO': 'Multiple ecological stressors present',
      'WRC_SALMON_AB': 'Salmon populations at concerning levels'
    }
  },
  4: {
    label: 'Critical',
    color: '#CD5C5C',
    explanation: {
      'ENV_FLOWS': 'Critical shortage affecting ecosystem survival',
      'RES_STOR': 'Storage at critical low levels',
      'GW_STOR': 'Severe groundwater depletion',
      'DELTA_ECO': 'Severe ecological degradation',
      'WRC_SALMON_AB': 'Salmon populations at risk of collapse'
    }
  }
};

// Use in tooltip
function getTierExplanation(tierLevel, tierCode) {
  return TIER_EXPLANATIONS[tierLevel].explanation[tierCode] || 
         `Tier ${tierLevel} - ${TIER_EXPLANATIONS[tierLevel].label}`;
}
```

---

## 🔗 Recommended New API Endpoints

To support rich tooltips, consider implementing these endpoints:

### 1. Network Node Details
```
GET /api/network/nodes/{node_id}/details
```

### 2. Reservoir Details
```
GET /api/reservoirs/{reservoir_id}/details
```

### 3. WBA/Aquifer Details
```
GET /api/wba/{wba_id}/details
```

### 4. Compliance Station Details
```
GET /api/compliance-stations/{station_id}/details
```

### 5. Regional Ecological Status
```
GET /api/regions/{region_id}/ecological-status
```

### 6. Salmon Monitoring Details
```
GET /api/salmon-monitoring/{location_id}/status
```

---

## 💡 Implementation Priority

**Phase 1 (MVP)**: Simple hover tooltips using existing data
- Location name
- Tier level with color coding
- Location type

**Phase 2**: Add tier explanations
- Context-specific tier descriptions
- What the tier level means for this metric

**Phase 3**: Click for details
- Implement detail endpoints
- Show comprehensive location information
- Add charts/gauges for quantitative data

**Phase 4**: Real-time data
- Link to live monitoring data where available
- Show trends and historical context

---

## 📱 Responsive Design Considerations

### Mobile Tooltips
- Use bottom sheets instead of popups
- Larger touch targets
- Simplified detail view

### Desktop Tooltips
- Rich hover states
- Detailed click overlays
- Support for multi-select/compare

---

## 🎯 Summary

**Currently Available (No Extra Work)**:
- Location name
- Tier level (1-4)
- Location type
- Coordinates (from geometry)

**Recommended Next Steps**:
1. Start with simple hover tooltips using existing data
2. Add tier-specific explanations as static content
3. Implement detail endpoints for click interactions
4. Consider progressive enhancement for better UX

The data is in the database; you just need to decide how much detail to expose and create the appropriate API endpoints to access it.

