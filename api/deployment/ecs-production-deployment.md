# Production ECS Deployment for COEQWAL FastAPI

## **Architecture for workshop-level performance**

```
Internet → ALB → ECS Fargate (2-10 instances) → RDS PostgreSQL
                      ↑
               Connection Pool (5-50 connections)
```

## **Deployment steps**

### 1. Create ECR repository
```bash
aws ecr create-repository --repository-name coeqwal-network-api --region us-west-2
```

### 2. Build and push Docker Image
```bash
cd api/

# Build optimized production image
docker build -f ../aws-deployment/Dockerfile.production -t coeqwal-network-api .

# Tag and push
ECR_URI="aws-account.dkr.ecr.us-west-2.amazonaws.com/coeqwal-network-api"
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $ECR_URI

docker tag coeqwal-network-api:latest $ECR_URI:latest
docker push $ECR_URI:latest
```

### 3. Deploy ECS service
```bash
# Deploy the CloudFormation stack
aws cloudformation deploy \
  --template-file ecs-fargate.yml \
  --stack-name coeqwal-network-api \
  --parameter-overrides \
    VpcId=vpc-your-existing-vpc \
    SubnetIds=subnet-your-private-1,subnet-your-private-2 \
    DatabaseUrl="postgresql://username:password@your-rds-endpoint:5432/coeqwal_scenario" \
    ImageUri=$ECR_URI:latest \
  --capabilities CAPABILITY_IAM
```

### 4. Get API URL
```bash
# Get the load balancer URL
aws cloudformation describe-stacks \
  --stack-name coeqwal-network-api \
  --query 'Stacks[0].Outputs[?OutputKey==`LoadBalancerUrl`].OutputValue' \
  --output text
```

## **Performance expectations**

### **Response times (Always-On)**
- **Node/Arc queries**: **50-150ms**
- **Network analysis**: **100-300ms**  
- **Complex spatial**: **200-500ms**
- **Workshop scaling**: **Instant** (no cold starts)

### **Scaling configuration**
```yaml
# Normal operation: 2 instances
MinCapacity: 2
MaxCapacity: 10

# Rule of thumb auto-scaling triggers:
# CPU > 70% -> scale up
# Requests > 1000/min -> scale up
# Memory > 80% -> scale up
```

### **Database connections**
- **Per instance**: 5-10 connections
- **Total pool**: 10-50 connections (well within RDS limits)
- **Connection reuse**

## **Costs**

### **Monthly costs**
- **Normal (2 instances)**: ~$60-80/month
- **Workshop scaling**: +$30-50 for event hours
- **Load balancer**: ~$20/month
- **Total**: ~$80-100/month base + workshop bursts

### **Cost vs performance**
- **App Runner**: Cheaper but 2-3s cold starts ❌
- **ECS Fargate**: More expensive but instant response ✅
- **Lambda**: Cheapest but cold starts + connection limits ❌

### **Workshop requirements** ✅
- **100 concurrent users** 5-8 ECS instances
- **Instant response** Always-warm containers
- **Heavy spatial queries** Dedicated CPU/memory
- **Demo reliability** No cold start

### **Normal usage** ✅  
- **Auto-scales down** 2 instances during low traffic
- **Cost efficient** Pay for what we use
- **High availability** Multi-AZ deployment

### **Integration** ✅
- **Same VPC** Direct RDS access
- **Existing security** Same security group

## 🔧 **Optimizations**

### **Database optimizations**
- **Connection pooling**: 5-50 connections per instance
- **Query optimization**: Spatial indexing, efficient joins
- **Response caching**: For frequently accessed data

### **API optimizations**  
- **GZip compression**: Faster data transfer
- **Bounding box queries**: Only load visible map areas
- **Batch endpoints**: Multiple operations in one request
- **Performance monitoring**: Track slow queries

### **Workshop optimizations**
- **Pre-warm endpoint**: `/api/workshop/status`
- **Bulk data loading**: Efficient GeoJSON responses
- **Connection monitoring**: Prevent database overload

## 🧪 **Load testing commands**

```bash
# Test 100 concurrent users
ab -n 1000 -c 100 "http://your-alb-url/api/nodes/optimized?limit=500"

# Test network analysis
ab -n 500 -c 50 "http://your-alb-url/api/network/analysis/node/1"

# Expected results:
# - 95% of requests < 500ms
# - No failed requests
# - Consistent performance
```
