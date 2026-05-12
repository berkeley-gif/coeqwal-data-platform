# Lambda: presigned S3 downloads (`coeqwalPresignDownload`)

The download API for `https://api.coeqwal.org`. Lists available scenarios in S3 and presigns a short-lived GET so the website can serve direct downloads of the scenario ZIP and CSV outputs.

| AWS resource | Value |
|---|---|
| Function name | `coeqwalPresignDownload` |
| Runtime | Node.js 20.x or 22.x (uses built-in AWS SDK v3, no `node_modules` needed) |
| Trigger | API Gateway v2 (HTTP API, id `x66ckhp067`), routes `GET /scenario` and `GET /download` |
| Action | List scenario prefixes under `coeqwal-model-run/scenario/`, presign a GET for the requested file |
| Source | [`index.mjs`](index.mjs) (single file) |
| Test events | [`tests/`](tests/) |

## What it does

| Route | Behavior |
|---|---|
| `GET /scenario` | Walks `s3://coeqwal-model-run/scenario/<id>/` prefixes. Returns one row per scenario with the keys for the run ZIP, the CalSim output CSV, and the SV input CSV (each null if not present) |
| `GET /download?scenario=s0020&type=zip\|output\|sv` | Validates the scenario id and type, finds the matching key in S3, presigns a GET (15 min expiry) with `Content-Disposition: attachment`, and returns a 302 redirect to the presigned URL |

Both routes set permissive CORS so the frontend can call them from any origin.

## Deploy updates

Single `index.mjs`, no external deps. Same flow as the ETL trigger.

**Via AWS Console:**
1. AWS Console -> Lambda -> Functions -> `coeqwalPresignDownload`
2. Click the **Code** tab
3. Select all in the inline editor, paste the full contents of [`index.mjs`](index.mjs)
4. Click **Deploy**

**Via Cloud9 / CLI:**

```bash
cd ~/environment/coeqwal-backend/api/lambda/coeqwalPresignDownload
zip lambda.zip index.mjs
aws lambda update-function-code --function-name coeqwalPresignDownload --zip-file fileb://lambda.zip
rm lambda.zip
```

## Test events

| File | Route exercised |
|---|---|
| [`tests/test-event-scenario-list.json`](tests/test-event-scenario-list.json) | `GET /scenario`. Lists all available scenarios |
| [`tests/test-event-download.json`](tests/test-event-download.json) | `GET /download?scenario=s0020&type=zip`. Returns a presigned URL |

**In the AWS Console:** Lambda -> `coeqwalPresignDownload` -> Test tab. Create a new test event and paste the JSON from one of the files in `tests/`.

**Via AWS CLI:**

```bash
# List scenarios
aws lambda invoke \
  --function-name coeqwalPresignDownload \
  --cli-binary-format raw-in-base64-out \
  --payload file://api/lambda/coeqwalPresignDownload/tests/test-event-scenario-list.json \
  --log-type Tail \
  response.json \
  --query 'LogResult' --output text | base64 -d
cat response.json | jq .

# Download presign
aws lambda invoke \
  --function-name coeqwalPresignDownload \
  --cli-binary-format raw-in-base64-out \
  --payload file://api/lambda/coeqwalPresignDownload/tests/test-event-download.json \
  --log-type Tail \
  response.json \
  --query 'LogResult' --output text | base64 -d
```

## Lambda configuration checklist

- [ ] Runtime: Node.js 20.x or 22.x
- [ ] Handler: `index.handler`
- [ ] Timeout: 30 seconds
- [ ] Memory: 256 MB minimum
- [ ] Environment variable: `COEQWAL_S3_BUCKET=coeqwal-model-run`
- [ ] IAM role has S3 read access to `coeqwal-model-run` bucket

## Common issues

### S3 permission error
```
AccessDenied: Access Denied
```
Add S3 read to the Lambda's IAM role:
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

### Environment variable missing
```
Error: Bucket name is undefined
```
Set `COEQWAL_S3_BUCKET=coeqwal-model-run` on the function.

### Path not found (404)
```
{"error": "Not Found"}
```
Check the API Gateway route matches the Lambda's expected paths (`/scenario`, `/download`).

### Timeout
Increase the Lambda timeout. 30s is enough for the cold start plus the S3 listing.

## Related

- The FastAPI service that serves the rest of the API: [`../../coeqwal-api/`](../../coeqwal-api/)
- Frontend caller (separate repo, `coeqwal-website`): `apps/main/app/lib/api/fileDownloadApi.ts`
- AWS-side resource details (API Gateway id, IAM, costs): [`../../../docs/INFRASTRUCTURE.md`](../../../docs/INFRASTRUCTURE.md)
