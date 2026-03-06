#!/usr/bin/env python3
"""
AWS Batch job submission script for DSS validation testing
Submits validation jobs directly to AWS Batch with reference CSV comparison
"""
import boto3
import json
import argparse
import sys
from datetime import datetime
import time

def submit_validation_job(
    zip_bucket, 
    zip_key, 
    reference_csv_key,
    job_queue="coeqwal-etl-queue",
    job_definition="coeqwal-etl-enhanced",
    abs_tol=1e-6,
    rel_tol=1e-6,
    dry_run=False
):
    """Submit a validation job to AWS Batch"""
    
    batch_client = boto3.client('batch')
    
    # Generate unique job name
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_name = f"validation-test-{timestamp}"
    
    # Job parameters
    parameters = {
        'ZIP_BUCKET': zip_bucket,
        'ZIP_KEY': zip_key,
        'VALIDATION_REF_CSV_KEY': reference_csv_key,
        'ABS_TOL': str(abs_tol),
        'REL_TOL': str(rel_tol)
    }
    
    print(f"🚀 Submitting validation job: {job_name}")
    print(f"   📦 DSS File: s3://{zip_bucket}/{zip_key}")
    print(f"   📄 Reference: s3://{zip_bucket}/{reference_csv_key}")
    print(f"   📊 Tolerances: abs={abs_tol}, rel={rel_tol}")
    print(f"   🎯 Queue: {job_queue}")
    print(f"   📋 Definition: {job_definition}")
    
    if dry_run:
        print("🔍 DRY RUN - Job not submitted")
        return None
    
    try:
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            parameters=parameters
        )
        
        job_id = response['jobId']
        print("✅ Job submitted successfully!")
        print(f"   🆔 Job ID: {job_id}")
        print(f"   📊 Monitor: https://console.aws.amazon.com/batch/home#jobs/detail/{job_id}")
        
        return job_id
        
    except Exception as e:
        print(f"❌ Failed to submit job: {e}")
        return None

def monitor_job(job_id, poll_interval=30):
    """Monitor job progress and return final status"""
    
    batch_client = boto3.client('batch')
    
    print(f"👀 Monitoring job {job_id}...")
    
    while True:
        try:
            response = batch_client.describe_jobs(jobs=[job_id])
            job = response['jobs'][0]
            status = job['jobStatus']
            
            print(f"   📊 Status: {status}")
            
            if status in ['SUCCEEDED', 'FAILED']:
                return status, job
            
            time.sleep(poll_interval)
            
        except Exception as e:
            print(f"❌ Error monitoring job: {e}")
            return 'ERROR', None

def get_validation_results(zip_bucket, scenario_id):
    """Download and display validation results from S3"""
    
    s3_client = boto3.client('s3')
    
    # Try to get validation summary
    summary_key = f"scenario/{scenario_id}/validation/{scenario_id}_validation_summary.json"
    
    try:
        print("📊 Downloading validation results...")
        response = s3_client.get_object(Bucket=zip_bucket, Key=summary_key)
        summary = json.loads(response['Body'].read())
        
        print("📄 VALIDATION RESULTS:")
        print(f"   Status: {summary['status']}")
        print(f"   Common columns: {summary['columns_common']}")
        print(f"   Date range: {summary['overlap_start']} to {summary['overlap_end']}")
        print(f"   Rows compared: {summary['rows_in_overlap']}")
        
        if summary['status'] != 'PASSED':
            print(f"   ⚠️  Mismatch columns: {summary['mismatch_columns']}")
            print(f"   ⚠️  Mismatch cells: {summary['mismatch_cells']}")
        
        # Show S3 locations
        mismatches_key = f"scenario/{scenario_id}/validation/{scenario_id}_validation_mismatches.csv"
        print("📁 Detailed reports:")
        print(f"   Summary: s3://{zip_bucket}/{summary_key}")
        print(f"   Mismatches: s3://{zip_bucket}/{mismatches_key}")
        
        return summary
        
    except Exception as e:
        print(f"❌ Could not retrieve validation results: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(
        description="Submit DSS validation job to AWS Batch"
    )
    parser.add_argument("--zip-bucket", required=True, 
                       help="S3 bucket containing DSS zip file")
    parser.add_argument("--zip-key", required=True,
                       help="S3 key for DSS zip file")
    parser.add_argument("--reference-csv-key", required=True,
                       help="S3 key for reference CSV (Java extraction)")
    parser.add_argument("--job-queue", default="coeqwal-etl-queue",
                       help="AWS Batch job queue")
    parser.add_argument("--job-definition", default="coeqwal-etl-enhanced",
                       help="AWS Batch job definition")
    parser.add_argument("--abs-tol", type=float, default=1e-6,
                       help="Absolute tolerance for validation")
    parser.add_argument("--rel-tol", type=float, default=1e-6,
                       help="Relative tolerance for validation")
    parser.add_argument("--monitor", action="store_true",
                       help="Monitor job until completion")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be submitted without actually submitting")
    
    args = parser.parse_args()
    
    # Submit job
    job_id = submit_validation_job(
        args.zip_bucket,
        args.zip_key,
        args.reference_csv_key,
        args.job_queue,
        args.job_definition,
        args.abs_tol,
        args.rel_tol,
        args.dry_run
    )
    
    if not job_id:
        return 1
    
    # Monitor if requested
    if args.monitor:
        status, job = monitor_job(job_id)
        
        if status == 'SUCCEEDED':
            # Try to extract scenario ID from job parameters or zip key
            scenario_id = args.zip_key.split('/')[-1].replace('.zip', '')
            get_validation_results(args.zip_bucket, scenario_id)
            print("✅ Validation job completed successfully!")
            return 0
        else:
            print(f"❌ Validation job failed with status: {status}")
            return 1
    
    print("🔄 Job submitted. Use --monitor flag to wait for completion.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
