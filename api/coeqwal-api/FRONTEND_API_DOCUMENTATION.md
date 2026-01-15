# Frontend integration guide

Quick reference for integrating COEQWAL API into the frontend.

## API base URLs

| Purpose | URL |
|---------|-----|
| Main API | `https://api.coeqwal.org/api` |
| Download API | `https://x66ckhp067.execute-api.us-west-2.amazonaws.com/default` |
| Interactive docs | `https://api.coeqwal.org/docs` |

## Scenario data

### List scenarios
```javascript
const response = await fetch('https://api.coeqwal.org/api/scenarios');
const { scenarios } = await response.json();

// Filter for active scenarios
const activeScenarios = scenarios.filter(s => s.is_active);
```

### Scenario IDs
Current scenarios: `s0011`, `s0020`, `s0021`, `s0023`, `s0024`, `s0025`, `s0027`, `s0029`

## Tier data (for charts)

### Get all tiers for a scenario
```javascript
const scenario = 's0020';
const response = await fetch(
  `https://api.coeqwal.org/api/tiers/scenarios/${scenario}/tiers`
);
const data = await response.json();

// data.scenario_id, data.tiers[]
```

### Tier codes
| Display name | API code |
|--------------|----------|
| Reservoir storage | `RES_STOR` |
| Groundwater storage | `GW_STOR` |
| Environmental flows | `ENV_FLOWS` |
| Delta ecology | `DELTA_ECO` |
| Freshwater for Delta uses | `FW_DELTA_USES` |
| Freshwater for exports | `FW_EXP` |
| Community water deliveries | `CWS_DEL` |
| Agricultural revenue | `AG_REV` |
| Salmon abundance | `WRC_SALMON_AB` |

## Tier map (for map visualization)

### Get GeoJSON for map
```javascript
const scenario = 's0020';
const tier = 'RES_STOR';

const response = await fetch(
  `https://api.coeqwal.org/api/tier-map/${scenario}/${tier}`
);
const geojson = await response.json();

// Add to Mapbox
map.addSource('tier-data', { type: 'geojson', data: geojson });
```

### Tier colors
```javascript
const TIER_COLORS = {
  1: '#2cc83b', // green - optimal
  2: '#2064d4', // blue - suboptimal
  3: '#f89740', // orange - at-risk
  4: '#f96262', // red - critical
};

// Mapbox expression
['match', ['get', 'tier_level'],
  1, '#2cc83b',
  2, '#2064d4',
  3, '#f89740',
  4, '#f96262',
  '#999999'
]
```

## File downloads

### List available files
```javascript
const response = await fetch(
  'https://x66ckhp067.execute-api.us-west-2.amazonaws.com/default/scenario'
);
const { scenarios } = await response.json();

// scenarios[].files.zip, .output_csv, .sv_csv
```

### Download a file
```javascript
const scenario = 's0020';
const type = 'zip'; // or 'output', 'sv'

// Redirect to presigned URL
window.location.href = 
  `https://x66ckhp067.execute-api.us-west-2.amazonaws.com/default/download?scenario=${scenario}&type=${type}`;
```

**Note:** The download Lambda may have cold starts (5-10s first request). Implement retry logic:

```javascript
async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, { 
        signal: AbortSignal.timeout(15000) 
      });
      if (response.ok) return response.json();
    } catch (e) {
      if (i === retries - 1) throw e;
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
}
```

## Network data

### Spatial query
```javascript
const bbox = '-122.5,37.0,-121.0,38.5'; // minLng,minLat,maxLng,maxLat
const zoom = 10;

const response = await fetch(
  `https://api.coeqwal.org/api/nodes/spatial?bbox=${bbox}&zoom=${zoom}`
);
const { nodes } = await response.json();
```

### Network traversal
```javascript
const nodeId = 42;
const response = await fetch(
  `https://api.coeqwal.org/api/nodes/${nodeId}/network?direction=downstream`
);
const { nodes, arcs } = await response.json();
```

## Error handling

```javascript
try {
  const response = await fetch(url);
  if (!response.ok) {
    const error = await response.json();
    console.error('API error:', error.detail);
    return;
  }
  const data = await response.json();
} catch (e) {
  console.error('Network error:', e);
}
```

## TypeScript types

```typescript
interface Scenario {
  scenario_id: string;
  name: string;
  is_active: boolean;
}

interface TierResult {
  tier_short_code: string;
  tier_1_value: number | null;
  tier_2_value: number | null;
  tier_3_value: number | null;
  tier_4_value: number | null;
  single_tier_level: number | null;
}

interface TierMapFeature {
  type: 'Feature';
  geometry: GeoJSON.Geometry;
  properties: {
    location_id: string;
    location_name: string;
    tier_level: 1 | 2 | 3 | 4;
    tier_value: number;
  };
}
```
