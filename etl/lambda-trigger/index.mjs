// index.mjs
import {
  S3Client,
  CopyObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import { BatchClient, SubmitJobCommand } from '@aws-sdk/client-batch';
import { DynamoDBClient, UpdateItemCommand } from '@aws-sdk/client-dynamodb';

const REGION = process.env.AWS_REGION || 'us-west-2';

// --- Config ---
const BUCKET = process.env.COEQWAL_S3_BUCKET || 'coeqwal-model-run';
const JOB_QUEUE = process.env.COEQWAL_BATCH_QUEUE || 'coeqwal-dss-queue';
const JOB_DEFINITION = process.env.COEQWAL_BATCH_JOBDEF || 'coeqwal-dss-jobdef';
const DDB_TABLE = process.env.DDB_TABLE || ''; // optional; if blank, DDB status is skipped

const s3 = new S3Client({ region: REGION });
const batch = new BatchClient({ region: REGION });
const ddb = new DynamoDBClient({ region: REGION });

export async function handler(event) {
  console.log('📦 Incoming Event:', JSON.stringify(event, null, 2));

  const record = event?.Records?.[0];
  if (!record) {
    console.error('❌ No record found in event');
    return;
  }

  const sourceKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
  const bucket = record.s3.bucket.name || BUCKET;

  // Handle .zip uploads that are in ready/
  if (!sourceKey.toLowerCase().startsWith('ready/') || !sourceKey.toLowerCase().endsWith('.zip')) {
    console.log('ℹ️ Not a ZIP under ready/. Ignoring:', sourceKey);
    return;
  }

  const fileName = sourceKey.split('/').pop();
  const stem = fileName.replace(/\.zip$/i, '');
  const scenarioId = (fileName.split('_')[0] || '').toLowerCase(); // e.g., s0020

  if (!/^s\d{4}$/.test(scenarioId)) {
    console.warn('⚠️ Could not derive scenario id from file name:', fileName);
    return;
  }

  const zipDestKey = `scenario/${scenarioId}/run/${fileName}`;

  try {
    // --- Move ZIP to its final location ---
    console.log(`➡️ Copying ZIP ${bucket}/${sourceKey} -> ${zipDestKey}`);
    await s3.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${sourceKey}`,
      Key: zipDestKey,
    }));
    console.log('🗑️ Deleting original ZIP from ready/');
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: sourceKey }));

    // --- Try to find a peer csv in ready/ ---
    let validationCsvReadyKey = await findPeerCsv(bucket, stem, scenarioId);

    // If found, move csv to scenario/<id>/verify/
    let validationCsvFinalKey = '';
    if (validationCsvReadyKey) {
      const csvName = validationCsvReadyKey.split('/').pop();
      validationCsvFinalKey = `scenario/${scenarioId}/verify/${csvName}`;

      console.log(`➡️ Copying CSV ${bucket}/${validationCsvReadyKey} -> ${validationCsvFinalKey}`);
      await s3.send(new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${validationCsvReadyKey}`,
        Key: validationCsvFinalKey,
      }));
      console.log('🗑️ Deleting original CSV from ready/');
      await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: validationCsvReadyKey }));
    } else {
      console.log('ℹ️ No peer CSV found in ready/ for', scenarioId);
    }

    // --- Submit Batch job ---
    const jobName = `etl-${scenarioId}-${Date.now()}`;
    const environment = [
      { name: 'SCENARIO_ID', value: scenarioId },
      { name: 'ZIP_FILENAME', value: fileName },
      { name: 'ZIP_BUCKET', value: bucket },
      { name: 'ZIP_KEY', value: zipDestKey },
      // pass the reference csv if any; empty means "no validation"
      { name: 'VALIDATION_REF_CSV_KEY', value: validationCsvFinalKey || '' },
      // Add validation tolerances for enhanced validation
      { name: 'ABS_TOL', value: '1e-6' },
      { name: 'REL_TOL', value: '1e-6' },
      // can also pass OUTPUT_PREFIX / DDB_TABLE etc. here if to override defaults in the container
    ];

    console.log('🚀 Submitting Batch job:', { jobName, JOB_QUEUE, JOB_DEFINITION, environment });
    const submitRes = await batch.send(new SubmitJobCommand({
      jobName,
      jobQueue: JOB_QUEUE,
      jobDefinition: JOB_DEFINITION,
      // NOTE: using ECS-style overrides to match jobdef
      ecsPropertiesOverride: {
        taskProperties: [
          {
            containers: [
              {
                name: 'main',
                environment,
              },
            ],
          },
        ],
      },
    }));

    const jobId = submitRes.jobId;
    console.log(`✅ Job submitted: ${jobId}`);

    // Log validation status
    if (validationCsvFinalKey) {
      console.log(`🔍 Validation ENABLED - Reference CSV: ${validationCsvFinalKey}`);
    } else {
      console.log(`⚠️ Validation DISABLED - No reference CSV found`);
    }

    // --- Optional: write SUBMITTED state to DynamoDB ---
    if (DDB_TABLE) {
      try {
        console.log(`📝 Updating DynamoDB (${DDB_TABLE}) with SUBMITTED`);
        await ddb.send(new UpdateItemCommand({
          TableName: DDB_TABLE,
          Key: { scenario_id: { S: scenarioId } },
          UpdateExpression: 'SET #s = :s, zip_key = :z, job_id = :j, updated = :u, validation_enabled = :v',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: {
            ':s': { S: 'SUBMITTED' },
            ':z': { S: zipDestKey },
            ':j': { S: jobId || 'unknown' },
            ':u': { N: String(Math.floor(Date.now() / 1000)) },
            ':v': { BOOL: !!validationCsvFinalKey },
          },
        }));
      } catch (e) {
        console.warn('⚠️ DDB update failed (non-fatal):', e);
      }
    }
  } catch (err) {
    console.error('❌ Error in Lambda handler:', err);
    throw err;
  }
}

/**
 * Find a peer csv sitting in ready/ that matches the uploaded ZIP.
 * Preference order:
 *   1) ready/<zip_stem>.csv      (exact match)
 *   2) newest ready/*<scenarioId>*.csv (broader pattern)
 * Returns the key under ready/ (string) or '' if none.
 */
async function findPeerCsv(bucket, zipStem, scenarioId) {
  const exactKey = `ready/${zipStem}.csv`;
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: exactKey }));
    console.log('🔎 Found exact peer CSV:', exactKey);
    return exactKey;
  } catch {
    // ignore 404
  }

  // List any ready/*s####*.csv and pick the newest
  const prefix = `ready/`;
  let candidates = [];
  let ContinuationToken = undefined;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken,
    }));
    (res.Contents || []).forEach(obj => {
      if (obj.Key?.toLowerCase().endsWith('.csv') && obj.Key.includes(scenarioId)) {
        candidates.push({ key: obj.Key, last: obj.LastModified ? new Date(obj.LastModified).getTime() : 0 });
      }
    });
    ContinuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (ContinuationToken);

  if (candidates.length === 0) return '';

  candidates.sort((a, b) => b.last - a.last);
  console.log('🔎 Selected newest peer CSV:', candidates[0].key);
  return candidates[0].key;
}