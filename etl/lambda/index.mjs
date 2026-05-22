// index.mjs
//
// coeqwalEtlTrigger Lambda.
//
// Fires on S3 PUT under `coeqwal-model-run/ready/`. For each ZIP it:
//   1. Waits up to 60 seconds for `ingest_record.json` to arrive in the same
//      prefix (HEAD-and-retry every 5 seconds). Handles upload-order races
//      between the operator's promote step and the S3 ObjectCreated event.
//   2. If no ingest record shows up, infers a minimal one and writes it to
//      `scenario/<id>/ingest_record.json`. Marks `ingestion.path = "manual_inferred"`
//      so the Batch container knows the hashes were not validated upstream.
//   3. Skips Batch submission if an active job already exists for this scenario
//      (idempotency check across SUBMITTED/PENDING/RUNNABLE/STARTING/RUNNING).
//   4. Moves ZIP and trend CSV out of `ready/` into `scenario/<id>/...`, writes
//      (or moves) the ingest record to `scenario/<id>/ingest_record.json`, and
//      submits the Batch job with SCENARIO_ID, ZIP_KEY, INGEST_RECORD_KEY, and
//      optional VALIDATION_REF_CSV_KEY env vars.
//
// Designed to be additive. The legacy behavior (move ZIP, find peer CSV,
// submit Batch) is preserved. New behavior only adds upstream protection.

import {
  S3Client,
  CopyObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { BatchClient, SubmitJobCommand, ListJobsCommand } from '@aws-sdk/client-batch';

const REGION = process.env.AWS_REGION || 'us-west-2';

const BUCKET = process.env.COEQWAL_S3_BUCKET || 'coeqwal-model-run';
const JOB_QUEUE = process.env.COEQWAL_BATCH_QUEUE || 'coeqwal-dss-queue';
const JOB_DEFINITION = process.env.COEQWAL_BATCH_JOBDEF || 'coeqwal-dss-jobdef';

const INGEST_RECORD_GRACE_MS = parseInt(process.env.INGEST_RECORD_GRACE_MS || '60000', 10);
const INGEST_RECORD_POLL_MS = parseInt(process.env.INGEST_RECORD_POLL_MS || '5000', 10);

const INGEST_RECORD_BASENAME = 'ingest_record.json';

const s3 = new S3Client({ region: REGION });
const batch = new BatchClient({ region: REGION });

export async function handler(event) {
  console.log('Incoming event:', JSON.stringify(event, null, 2));

  const record = event?.Records?.[0];
  if (!record) {
    console.error('No record found in event');
    return;
  }

  const sourceKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
  const bucket = record.s3.bucket.name || BUCKET;

  if (!sourceKey.toLowerCase().startsWith('ready/') || !sourceKey.toLowerCase().endsWith('.zip')) {
    console.log('Not a ZIP under ready/. Ignoring:', sourceKey);
    return;
  }

  const fileName = sourceKey.split('/').pop();
  const stem = fileName.replace(/\.zip$/i, '');
  const scenarioId = (fileName.split('_')[0] || '').toLowerCase();
  const sourcePrefix = sourceKey.substring(0, sourceKey.lastIndexOf('/') + 1);

  if (!/^s\d{4}$/.test(scenarioId)) {
    console.warn('Could not derive scenario id from file name:', fileName);
    return;
  }

  // Idempotency: skip if a job is already in flight for this scenario
  try {
    const active = await findActiveJobForScenario(scenarioId);
    if (active) {
      console.log(`Idempotency skip: active Batch job already exists for ${scenarioId}: ${active.jobId} (${active.status})`);
      return;
    }
  } catch (err) {
    console.warn('Idempotency check failed (continuing anyway):', err?.message || err);
  }

  // The ZIP keeps living under `scenario/<id>/run/` because the presign
  // download API still serves it from there. The ingest record lives one
  // level up at `scenario/<id>/ingest_record.json` alongside the extract
  // record the Batch container will write.
  const zipDestKey = `scenario/${scenarioId}/run/${fileName}`;
  const ingestRecordDestKey = `scenario/${scenarioId}/${INGEST_RECORD_BASENAME}`;

  try {
    // Wait for ingest_record.json to land alongside the ZIP
    const ingestRecordReadyKey = await waitForIngestRecord(bucket, sourcePrefix);
    const ingestRecordFinalKey = ingestRecordDestKey;

    if (ingestRecordReadyKey) {
      console.log(`Ingest record arrived at ${ingestRecordReadyKey}; moving to ${ingestRecordDestKey}`);
      await s3.send(new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${ingestRecordReadyKey}`,
        Key: ingestRecordDestKey,
      }));
      await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: ingestRecordReadyKey }));
    } else {
      console.warn(`No ingest record after ${INGEST_RECORD_GRACE_MS}ms grace; inferring a minimal one for ${scenarioId}`);
      await writeInferredIngestRecord(bucket, ingestRecordDestKey, scenarioId, fileName, zipDestKey);
    }

    // Move the ZIP to its final location
    console.log(`Copying ZIP ${bucket}/${sourceKey} -> ${zipDestKey}`);
    await s3.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${sourceKey}`,
      Key: zipDestKey,
    }));
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: sourceKey }));

    // Find peer trend CSV in the same prefix and relocate it
    const validationCsvReadyKey = await findPeerCsv(bucket, stem, scenarioId, sourcePrefix);
    let validationCsvFinalKey = '';
    if (validationCsvReadyKey) {
      const csvName = validationCsvReadyKey.split('/').pop();
      validationCsvFinalKey = `scenario/${scenarioId}/verify/${csvName}`;
      console.log(`Copying CSV ${bucket}/${validationCsvReadyKey} -> ${validationCsvFinalKey}`);
      await s3.send(new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${validationCsvReadyKey}`,
        Key: validationCsvFinalKey,
      }));
      await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: validationCsvReadyKey }));
    } else {
      console.log('No peer CSV found in ready/ for', scenarioId);
    }

    // Clean up the source subfolder if any leftovers remain
    if (sourcePrefix !== 'ready/') {
      const remaining = await s3.send(new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: sourcePrefix,
      }));
      for (const obj of (remaining.Contents || [])) {
        console.log('Cleaning up leftover:', obj.Key);
        await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: obj.Key }));
      }
    }

    // Submit the Batch job
    const jobName = `etl-${scenarioId}-${Date.now()}`;
    const environment = [
      { name: 'SCENARIO_ID', value: scenarioId },
      { name: 'ZIP_FILENAME', value: fileName },
      { name: 'ZIP_BUCKET', value: bucket },
      { name: 'ZIP_KEY', value: zipDestKey },
      { name: 'INGEST_RECORD_KEY', value: ingestRecordFinalKey },
      { name: 'VALIDATION_REF_CSV_KEY', value: validationCsvFinalKey || '' },
      { name: 'ABS_TOL', value: '1e-6' },
      { name: 'REL_TOL', value: '1e-6' },
    ];

    console.log('Submitting Batch job:', { jobName, JOB_QUEUE, JOB_DEFINITION, environment });
    const submitRes = await batch.send(new SubmitJobCommand({
      jobName,
      jobQueue: JOB_QUEUE,
      jobDefinition: JOB_DEFINITION,
      ecsPropertiesOverride: {
        taskProperties: [
          {
            containers: [
              { name: 'main', environment },
            ],
          },
        ],
      },
    }));

    const jobId = submitRes.jobId;
    console.log(`Submitted Batch job ${jobId} for scenario ${scenarioId}`);
    console.log(`Ingest record: ${ingestRecordFinalKey}`);
    if (validationCsvFinalKey) {
      console.log(`Validation ENABLED. Reference CSV: ${validationCsvFinalKey}`);
    } else {
      console.log('Validation DISABLED. No reference CSV found');
    }

  } catch (err) {
    console.error('Error in Lambda handler:', err);
    throw err;
  }
}

/**
 * Wait for `ingest_record.json` to land alongside the triggering ZIP.
 * Returns the S3 key once it appears, or null after the grace window.
 *
 * The developer's promote step uploads three files in order:
 * ingest_record.json, then the trend CSV, then the ZIP. The Lambda
 * fires on the ZIP PUT (the last one), so the record is normally
 * already there. This wait covers the case where either of the
 * earlier PUTs is slightly delayed.
 *
 * Polls with HeadObject (an existence check that returns headers
 * only, no body) against `<sourcePrefix>ingest_record.json`, where
 * sourcePrefix is the directory the ZIP landed in (e.g. "ready/" or
 * "ready/s0020/"). Sleeps INGEST_RECORD_POLL_MS between attempts,
 * up to INGEST_RECORD_GRACE_MS total.
 */
async function waitForIngestRecord(bucket, sourcePrefix) {
  const recordKey = `${sourcePrefix}${INGEST_RECORD_BASENAME}`;
  const deadline = Date.now() + INGEST_RECORD_GRACE_MS;
  let attempts = 0;
  while (Date.now() < deadline) {
    attempts++;
    try {
      await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: recordKey }));
      console.log(`Ingest record found at ${recordKey} after ${attempts} attempt(s)`);
      return recordKey;
    } catch (err) {
      const code = err?.name || err?.$metadata?.httpStatusCode;
      if (code !== 'NotFound' && code !== 404 && code !== '404') {
        console.warn('Unexpected ingest record HEAD error (continuing to retry):', err?.message || err);
      }
      if (Date.now() + INGEST_RECORD_POLL_MS >= deadline) break;
      await new Promise(r => setTimeout(r, INGEST_RECORD_POLL_MS));
    }
  }
  return null;
}

/**
 * Write a minimal ingest record to S3 when none arrived. Marks the
 * ingestion path as `manual_inferred` so downstream consumers know the
 * file hashes were not verified at ingestion time.
 */
async function writeInferredIngestRecord(bucket, recordKey, scenarioId, zipFilename, zipKey) {
  const ingestRecord = {
    schema_version: 1,
    short_code: scenarioId,
    zip_basename: zipFilename,
    zip_key: zipKey,
    zip_sha256: '',
    expected_sv_filename: '',
    expected_dv_filename: '',
    expected_sv_path_in_zip: '',
    expected_dv_path_in_zip: '',
    sv_sha256: '',
    dv_sha256: '',
    trend_csv_basename: null,
    trend_csv_sha256: null,
    ingestion: {
      path: 'manual_inferred',
      script: 'lambda:coeqwalEtlTrigger',
      script_version: 'lambda-pass2b',
      operator: 'lambda',
      ingested_at_utc: new Date().toISOString(),
    },
    notes: 'Ingest record inferred by Lambda. No file hashes computed at ingestion. The Batch container is the only place hashes can be verified for this run.',
  };
  await s3.send(new PutObjectCommand({
    Bucket: bucket,
    Key: recordKey,
    Body: JSON.stringify(ingestRecord, null, 2),
    ContentType: 'application/json',
  }));
  console.log(`Inferred ingest record written to ${recordKey}`);
}

/**
 * Look for a Batch job that's already active for this scenario id. Active
 * means SUBMITTED, PENDING, RUNNABLE, STARTING, or RUNNING. Used to make the
 * Lambda idempotent against duplicate S3 events or operator retries.
 */
async function findActiveJobForScenario(scenarioId) {
  const activeStatuses = ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING'];
  const namePrefix = `etl-${scenarioId}-`;
  for (const status of activeStatuses) {
    let nextToken;
    do {
      const res = await batch.send(new ListJobsCommand({
        jobQueue: JOB_QUEUE,
        jobStatus: status,
        nextToken,
      }));
      const match = (res.jobSummaryList || []).find(j => (j.jobName || '').startsWith(namePrefix));
      if (match) return match;
      nextToken = res.nextToken;
    } while (nextToken);
  }
  return null;
}

/**
 * Find a peer csv (Trend Report) sitting alongside the uploaded ZIP. Searches in the given
 * prefix (e.g., "ready/" or "ready/s0020/").
 * Preference order:
 *   1) <prefix><zip_stem>.csv      (exact match)
 *   2) newest <prefix>*<scenarioId>*.csv (broader pattern)
 */
async function findPeerCsv(bucket, zipStem, scenarioId, prefix = 'ready/') {
  const exactKey = `${prefix}${zipStem}.csv`;
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: exactKey }));
    console.log('Found exact peer CSV:', exactKey);
    return exactKey;
  } catch {
    // ignore 404
  }

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
  console.log('Selected newest peer CSV:', candidates[0].key);
  return candidates[0].key;
}
