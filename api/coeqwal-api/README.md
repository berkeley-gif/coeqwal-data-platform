# COEQWAL API

Production FastAPI backend




## Quick start
- **Base URL**: `https://api.coeqwal.org`
- **Documentation**: `https://api.coeqwal.org/docs`
- **Health check**: `https://api.coeqwal.org/api/health`





## **API endpoints**

### **Core data**
- `GET /api/nodes` - Network nodes with coordinates (up to 10,000 per request)
- `GET /api/arcs` - Network arcs with geometry (up to 10,000 per request)
- `GET /api/search?q={query}` - Search nodes/arcs by name or code

### **Network analysis**
- `GET /api/nodes/{id}/analysis` - Upstream/downstream connections for any node
- `GET /api/arcs/{id}/analysis` - Connected nodes for any arc

### **Monitoring**
- `GET /api/health` - Database connectivity and performance metrics
- `GET /docs` - Interactive API documentation

## **Mapbox integration**

### **Load network data**
```javascript
const API_URL = "https://api.coeqwal.org"

// Load all nodes and arcs for map visualization
const [nodesResponse, arcsResponse] = await Promise.all([
  fetch(`${API_URL}/api/nodes`),
  fetch(`${API_URL}/api/arcs`)
])

const nodes = await nodesResponse.json()
const arcs = await arcsResponse.json()

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
        region: node.hydrologic_region,
        ...node.attributes
      }
    }))
  }
})
```

### **Interactive network analysis**
```javascript
// Handle node clicks for network analysis
map.on('click', 'nodes', async (e) => {
  const nodeId = e.features[0].properties.id
  
  // Get upstream/downstream connections
  const response = await fetch(`${API_URL}/api/nodes/${nodeId}/analysis`)
  const analysis = await response.json()
  
  // Highlight connected elements
  highlightConnectedElements(analysis.upstream_nodes, analysis.downstream_nodes)
  
  // Show popup with data attributes
  showNetworkPopup(e.lngLat, e.features[0].properties, analysis)
});
```

## **Architecture**

### **Production infrastructure**
```
Internet → Route 53 (api.coeqwal.org) → Application Load Balancer → ECS Fargate → PostgreSQL RDS
```

### **Expected performance**
- **Response time**: 50-300ms for spatial queries
- **Concurrent users**: 50+ workshop participants supported
- **Database pool**: 5-50 connections with auto-scaling
- **Auto-scaling**: ECS tasks scale based on load
- **Zero cold starts**: Always-on containers

### **Security**
- **HTTPS/SSL**: TLS 1.3 encryption with wildcard certificate
- **VPC isolation**: All resources in private network
- **Security groups**: Controlled access between services
- **Database security**: Private subnets, restricted access

## **Database schema**

### **Network topology (Ring 1)**
- **network_node**: 1,402 California water system nodes
- **network_arc**: 1,061 river/canal connections
- **Spatial data**: PostGIS geometry with EPSG:4326 coordinates
- **Metadata**: CalSim attributes, operational data, versioning

### **Supporting tables**
- **network_node_type**: 29 node classifications
- **network_arc_type**: 26 arc classifications  
- **hydrologic_region**: 5 California water regions
- **Versioning system**: Developer tracking, version management

## **Development**

### **Local development**
```bash
# Install dependencies
pip install -r requirements.txt

# Set database connection
export DATABASE_URL="postgresql://user:pass@host:5432/coeqwal_scenario"

# Run locally
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### **Testing**
```bash
# Health check
curl http://localhost:8000/api/health

# Sample data
curl "http://localhost:8000/api/nodes?limit=5"
curl "http://localhost:8000/api/nodes/1/analysis"
```

## **Deployment**

### **Production deployment**
- **GitHub Actions**: Automated Docker builds on push to `main`
- **ECR**: Container registry for image storage
- **ECS Fargate**: Serverless container orchestration
- **Application Load Balancer**: Traffic distribution and SSL termination

### **Manual updates**
```bash
# Update code and deploy
git add .
git commit -m "Update API ..."
git push origin main

# Force ECS deployment (if needed)
# ECS Console → Update service → Force new deployment
```

## **Monitoring**

### **CloudWatch integration**
- **Application logs**: `/ecs/coeqwal-api`
- **Performance metrics**: Response times via `X-Process-Time` headers
- **Database monitoring**: Connection pool status
- **Load balancer metrics**: Request count, error rates

### **Health endpoints**
- **`/api/health`**: Database connectivity and pool status
- **`/api/performance/stats`**: Detailed performance metrics

## **Cost**

### **Monthly pperational cost estimate**
- **ECS Fargate**: ~$25-35 (2 tasks, 0.5 vCPU, 1GB each)
- **Application Load Balancer**: ~$20
- **Route 53**: ~$0.50
- **CloudWatch Logs**: ~$5
- **Total**: ~$50-60/month

### **Workshop scaling**
- **Auto-scaling**: Additional ECS tasks during high load
- **Cost**: +$15-25 for workshop hours
- **Performance**: Maintains <300ms response times





