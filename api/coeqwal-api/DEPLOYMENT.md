# COEQWAL API deployment guide

## **Directory structure **

```
coeqwal-backend/
├── etl/
│   └── coeqwal-etl/          # ETL pipeline
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── batch_entrypoint.sh
│       └── python-code/
└── api/
    └── coeqwal-api/          # API pipeline
        ├── main.py           # FastAPI app
        ├── requirements.txt  # Python dependencies
        ├── config.py         # Config
        ├── Dockerfile        # Development container
        └── README.md
```

## **Parallel CI/CD pipelines**

### **ETL pipeline**
```yaml
Trigger: etl/coeqwal-etl/** changes
Action: Build -> Push to ECR -> AWS Batch Jobs
```

### **API pipeline** (New)
```yaml
Trigger: api/coeqwal-api/** changes  
Action: Build -> Push to ECR -> Deploy to ECS Fargate
```

## **Deployment**

### **1. Set GitHub secrets**
Secrets added (in addition to ETL secrets):

| Secret | Value |
|--------|-------|
| `VPC_ID` | VPC where RDS lives
| `PRIVATE_SUBNET_IDS` | Private subnets (comma-separated)
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:pass@rds-endpoint:5432/coeqwal_scenario` |

### **2. Deploy**
```bash
# Commit and push to trigger deployment
git add .
git commit -m "Add FastAPI production deployment"
git push origin main
```

### **3. Monitor**
- GitHub Actions: **"Build and Deploy COEQWAL Network API"**
- Get API URL from deployment logs

## **Deployment:**
- ✅ **API URL**: `http://coeqwal-api-alb-123.us-west-2.elb.amazonaws.com`
- ✅ **Health check**: `{api-url}/api/health`
- ✅ **Docs**: `{api-url}/docs`
- ✅ **Nodes data**: `{api-url}/api/nodes` (1,400+ nodes with coordinates)
- ✅ **Arcs data**: `{api-url}/api/arcs` (1,063+ arcs with geometry)
- ✅ **Network analysis**: `{api-url}/api/nodes/1/analysis`

### **Performance expectations:**
- **Response time**: 50-300ms for spatial queries
- **Concurrent users**: Supports 100+ participants (workshop context)
- **Auto-scaling**: 2-10 ECS instances based on load
- **Always-on**: Zero cold starts

## 🧪 **Basic tests**

```bash
# Get API URL from CloudFormation outputs
API_URL=$(aws cloudformation describe-stacks \
  --stack-name coeqwal-network-api \
  --query 'Stacks[0].Outputs[?OutputKey==`LoadBalancerUrl`].OutputValue' \
  --output text)

# Test health
curl "$API_URL/api/health"

# Test nodes (first 5)
curl "$API_URL/api/nodes?limit=5" | jq '.[0]'

# Test workshop readiness
curl "$API_URL/api/workshop/status"
```

### **Load testing for workshops**
```bash
# Install apache bench
brew install httpd  # macOS
# or: sudo apt-get install apache2-utils  # Linux

# Test 100 concurrent users
ab -n 1000 -c 100 "$API_URL/api/nodes?limit=500"

# Expected results:
# - 95% requests < 500ms
# - 0% failed requests
# - Consistent performance
```

## **Troubleshooting**

### **Common issues:**

#### **1. Database connection failed**
```bash
# Check if DATABASE_URL secret is correct
# Test connection from local machine:
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM network_node;"
```

#### **2. VPC/Subnet issues**
```bash
# Verify subnets can reach RDS
aws ec2 describe-route-tables --filters "Name=association.subnet-id,Values=subnet-your-id"
```

#### **3. Security Group issues**
```bash
# Check if ECS can reach RDS on port 5432
aws ec2 describe-security-groups --group-ids sg-your-rds-security-group
```

## **Frontend integration**

```javascript
// Load network data
const nodes = await fetch(`${API_URL}/api/nodes`);
const arcs = await fetch(`${API_URL}/api/arcs`);

// Handle map clicks
const analysis = await fetch(`${API_URL}/api/nodes/${nodeId}/analysis`);
```

## **Monitoring**

### **CloudWatch metrics**
- **ECS service**: CPU, Memory, Task count
- **Load balancer**: Request count, Response time, Error rate
- **Database**: Connection count, Query performance

### **Custom metrics**
- **API response times**: Via `X-Process-Time` headers
- **Workshop readiness**: Via `/api/workshop/status`
- **Database pool**: Via `/api/health`
