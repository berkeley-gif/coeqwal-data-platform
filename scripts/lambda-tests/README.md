# Lambda Test Events

Test events for the `coeqwalPresignDownload` Lambda function.

## Test Events

| File | Description |
|------|-------------|
| `test-event-scenario-list.json` | GET /scenario - List all available scenarios |
| `test-event-download.json` | GET /download?scenario=s0020&type=zip - Get presigned URL |

## Testing in AWS Console

1. Go to **Lambda** → **coeqwalPresignDownload** (or your function name)
2. Click **Test** tab
3. Create a new test event, paste the JSON from one of these files
4. Click **Test** to run

The response and logs will show the actual error.

## Testing via AWS CLI

```bash
# List scenarios
aws lambda invoke \
  --function-name coeqwalPresignDownload \
  --cli-binary-format raw-in-base64-out \
  --payload file://scripts/lambda-tests/test-event-scenario-list.json \
  --log-type Tail \
  response.json \
  --query 'LogResult' --output text | base64 -d

# Check response
cat response.json | jq .

# Download test
aws lambda invoke \
  --function-name coeqwalPresignDownload \
  --cli-binary-format raw-in-base64-out \
  --payload file://scripts/lambda-tests/test-event-download.json \
  --log-type Tail \
  response.json \
  --query 'LogResult' --output text | base64 -d
```

## Common Issues

### 1. S3 Permission Error
```
AccessDenied: Access Denied
```
**Fix:** Add S3 permissions to the Lambda's IAM role:
```json
{
  "Effect": "Allow",
  "Action": ["s3:ListBucket", "s3:GetObject"],
  "Resource": [
    "arn:aws:s3:::coeqwal-model-run",
    "arn:aws:s3:::coeqwal-model-run/*"
  ]
}
```

### 2. Environment Variable Missing
```
Error: Bucket name is undefined
```
**Fix:** Set `COEQWAL_S3_BUCKET` environment variable to `coeqwal-model-run`

### 3. Path Not Found (404)
```
{"error": "Not Found"}
```
**Fix:** Check that API Gateway route matches the Lambda's expected paths (`/scenario`, `/download`)

### 4. Timeout
Lambda times out without response.
**Fix:** Increase Lambda timeout (recommend 30s for cold starts + S3 listing)

## Lambda Configuration Checklist

- [ ] Runtime: Node.js 20.x or 22.x
- [ ] Handler: `index.handler`
- [ ] Timeout: 30 seconds
- [ ] Memory: 256 MB minimum
- [ ] Environment variable: `COEQWAL_S3_BUCKET=coeqwal-model-run`
- [ ] IAM role has S3 read access to `coeqwal-model-run` bucket
