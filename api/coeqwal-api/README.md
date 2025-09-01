# COEQWAL Network Topology API

FastAPI backend for COEQWAL

## Features

- **Spatial Data**: Serves GeoJSON data for 1,400+ nodes and 1,063+ arcs
- **Network Analysis**: Upstream/downstream traversal using PostgreSQL functions
- **Interactive**: Click handlers for nodes/arcs with rich metadata
- **Search**: Find network elements by name or code
- **CORS Ready**: Configured for Next.js frontend integration

## Quick Start

### 1. Install dependencies

```bash
cd api
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
# Set PostgreSQL connection string
export DATABASE_URL="postgresql://username:password@hostname:port/coeqwal_scenario"
```

### 3. Run the API

```bash
# Development
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Test the API

Visit: http://localhost:8000/docs for interactive API documentation

## API Endpoints

### Core Data
- `GET /api/nodes` - Get all network nodes with spatial data
- `GET /api/arcs` - Get all network arcs with spatial data
- `GET /api/search?q={query}` - Search nodes/arcs by name or code

### Network Analysis
- `GET /api/nodes/{node_id}/analysis` - Get upstream/downstream connections for a node
- `GET /api/arcs/{arc_id}/analysis` - Get connected nodes for an arc

### Utility
- `GET /api/health` - Health check
- `GET /` - API information

## Frontend integration

### Next.js Mapbox Example

```javascript
// Load all nodes and arcs
const loadNetworkData = async () => {
  const [nodesResponse, arcsResponse] = await Promise.all([
    fetch('http://localhost:8000/api/nodes'),
    fetch('http://localhost:8000/api/arcs')
  ]);
  
  const nodes = await nodesResponse.json();
  const arcs = await arcsResponse.json();
  
  // Add to Mapbox map
  map.addSource('nodes', {
    type: 'geojson',
    data: {
      type: 'FeatureCollection',
      features: nodes.map(node => ({
        type: 'Feature',
        geometry: node.geojson,
        properties: {
          id: node.id,
          name: node.name,
          type: node.node_type,
          ...node.attributes
        }
      }))
    }
  });
  
  map.addSource('arcs', {
    type: 'geojson', 
    data: {
      type: 'FeatureCollection',
      features: arcs.map(arc => ({
        type: 'Feature',
        geometry: arc.geojson,
        properties: {
          id: arc.id,
          name: arc.name,
          type: arc.arc_type,
          length: arc.shape_length,
          ...arc.attributes
        }
      }))
    }
  });
};

// Handle node/arc clicks
map.on('click', 'nodes', async (e) => {
  const nodeId = e.features[0].properties.id;
  
  // Get network analysis
  const response = await fetch(`http://localhost:8000/api/nodes/${nodeId}/analysis`);
  const analysis = await response.json();
  
  // Highlight upstream/downstream connections
  highlightConnections(analysis);
  
  // Show popup with details
  showPopup(e.lngLat, e.features[0].properties, analysis);
});
```

## Response examples

### Node Response
```json
{
  "id": 1,
  "short_code": "SAC232",
  "calsim_id": "SAC232", 
  "name": "Sacramento River",
  "node_type": "Channel - Stream",
  "latitude": 40.0829,
  "longitude": -122.1162,
  "hydrologic_region": "SAC",
  "geojson": {
    "type": "Point",
    "coordinates": [-122.11621022, 40.08286059]
  },
  "attributes": {
    "riv_mi": 231.83,
    "riv_name": "Sacramento River",
    "comment": "Antelope Creek Confluence"
  }
}
```

### Network analysis response
```json
{
  "origin_id": 1,
  "origin_type": "node",
  "upstream_nodes": [
    {
      "id": 2,
      "short_code": "SAC236", 
      "name": "Sacramento River",
      "element_type": "node",
      "distance": 1,
      "direction": "upstream"
    }
  ],
  "downstream_nodes": [
    {
      "id": 3,
      "short_code": "SAC228",
      "name": "Sacramento River", 
      "element_type": "node",
      "distance": 1,
      "direction": "downstream"
    }
  ],
  "connected_arcs": [
    {
      "id": 15,
      "short_code": "C_SAC232",
      "name": "Sacramento River",
      "element_type": "arc",
      "distance": 0,
      "direction": "outflow"
    }
  ]
}
```

## Configuration

Update `api/config.py` to customize:
- Database connection
- CORS origins
- Pagination limits
- Network traversal depth

## Deployment

### Docker
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Environment variables
- `DATABASE_URL`: PostgreSQL connection string
- `ALLOWED_ORIGINS`: Comma-separated list of frontend origins

## Database functions required

The API uses these PostgreSQL functions (configured in database):
- `get_upstream_nodes(node_id, max_depth)` 
- `get_downstream_nodes(node_id, max_depth)`
- `get_connected_arcs(node_id)`

## Performance notes

- Responses are paginated (default 100, max 1000)
- Spatial queries use PostGIS indexes
- Network analysis is limited to reasonable depths (3)
- Consider caching for production use
