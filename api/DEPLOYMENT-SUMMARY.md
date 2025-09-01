# COEQWAL API

## **All api files in `api/` directory**

### **Core application** (`api/coeqwal-api/`)
- `main.py` - FastAPI application with GB-scale optimizations
- `requirements.txt` - Python dependencies
- `config.py` - Configuration (10K query limits)
- `README.md` - API documentation
- `Dockerfile` - Development container

### **Production deployment** (`api/deployment/`)
- `Dockerfile.production` - Optimized production container
- `ecs-fargate.yml` - CloudFormation template for ECS
- `ecs-production-deployment.md` - Deployment guide

### **CI/CD pipeline**
- `.github/workflows/api.yml` - GitHub Actions workflow

## **Scalability features**

### **Query limits **
- **Nodes/Arcs**: 1,000 default → **10,000 maximum**
- **Variables**: Up to **50,000** per request
- **Time series**: Up to **100,000** data points
- **Bulk exports**: For GB-scale data

### **Infrastructure upgraded for performance (workshops)**
- **CPU**: 1 vCPU per container (was 0.5)
- **Memory**: 2GB per container (was 1GB)  
- **Instances**: 3 base instances (was 2)
- **Auto-scaling**: Up to 10+ for workshops

### **Endpoints**
- `/api/scenarios` - For scenario analysis
- `/api/variables` - For Ring 2/3 entity/variable data
- `/api/timeseries/{id}` - For time series data
- `/api/bulk/export` - For large data exports
- `/api/performance/stats` - Monitoring

## **Deployment process**

### **Push, and automagically:**
1. **GitHub Actions triggers** (API workflow actions are separate from ETL actions)
2. **Builds production Docker image** from `api/deployment/Dockerfile.production`
3. **Pushes to ECR** @ AWS
4. **Deploys ECS Fargate** using `api/deployment/ecs-fargate.yml`
5. **Creates load balancer** for 100+ concurrent users
6. **Tests deployment** automatically

## **Production API:**

### **Endpoints**
- `GET /api/nodes` - 1,400+ network nodes (up to 10K per request)
- `GET /api/arcs` - 1,063+ network arcs (up to 10K per request)
- `GET /api/nodes/{id}/analysis` - Network analysis
- `GET /api/search?q=sacramento` - Search functionality
- `GET /api/health` - Health monitoring
- `GET /docs` - Interactive API documentation

### **Endpoint plans:**
- `GET /api/scenarios` - Scenario data
- `GET /api/variables` - Variable data (50K per request)
- `GET /api/timeseries/{id}` - Time series (100K points)
- `GET /api/bulk/export` - GB-scale exports
- `GET /api/performance/stats` - Performance monitoring

## **Expected costs**

### **Base cost** (~$80-120/month)
- **ECS Fargate**: 2 instances × 1 vCPU × 2GB = ~$70-90/month
- **Load balancer**: ~$20/month
- **Data transfer**: ~$5-10/month

### **Workshop scaling** (+$30-50 for event hours)
- Auto-scales to 6-10 instances during workshops
- Returns to 2 instances after traffic drops

## ✅ **Ready to Deploy**

Everything is self-contained in the `api/` directory:

```bash
git add .
git commit -m "Add scalable FastAPI with self-contained deployment"
git push origin main
```

**This will:**
- ✅ **Only trigger API workflow** (not ETL)
- ✅ **Deploy production-ready FastAPI**
- ✅ **Handle 100+ workshop users**
- ✅ **Support 10K+ nodes per request**
- ✅ **Ready for GB-scale future data**
